//! Live hosted lifecycle. Consumers supply runtimes; the host owns capacity and retirement fencing.
mod suspension;
pub use suspension::ColdMatchResolver;

use crate::connection::{
    SNAPSHOT_CHANNEL_DEPTH, SnapshotPublication, committed_snapshot_publication,
    snapshot_publication,
};
use crate::runtime::RuntimeError;
use crate::{
    GameSimulation, MatchHost, MatchId, MatchRuntime, MatchStatus, PersistenceError, RecoveryImage,
    RuntimeRecoveryError, SnapshotCommitBatch, SnapshotPersistence,
};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex as TaskMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock, broadcast},
    task::JoinHandle,
    time::MissedTickBehavior,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveHostError {
    NotServing,
    AlreadyStarted,
    LifecycleFailed,
    Draining,
    AtCapacity,
    DuplicateMatch,
    UnknownMatch,
    InvalidTickRate,
    PlayerCapacityTooLarge,
    ReplayCaptureDisabled,
    NotCommitted,
    MatchOccupied,
    InvalidLifecycleTimeout,
    Suspension(PersistenceError),
    Recovery(RuntimeRecoveryError),
    Runtime(RuntimeError),
}
impl fmt::Display for LiveHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LiveHostError {}

pub struct LivePlacementFailure<S> {
    error: LiveHostError,
    id: MatchId,
    runtime: Box<MatchRuntime<S>>,
}
impl<S> LivePlacementFailure<S> {
    pub fn error(&self) -> &LiveHostError {
        &self.error
    }
    pub fn into_parts(self) -> (LiveHostError, MatchId, MatchRuntime<S>) {
        (self.error, self.id, *self.runtime)
    }
}
impl<S> fmt::Debug for LivePlacementFailure<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LivePlacementFailure")
            .field("error", &self.error)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
impl<S> fmt::Display for LivePlacementFailure<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
impl<S> std::error::Error for LivePlacementFailure<S> {}

pub(crate) struct HostedMatch<S> {
    pub(crate) runtime: Arc<Mutex<MatchRuntime<S>>>,
    pub(crate) snapshots: broadcast::Sender<SnapshotPublication>,
    pub(crate) shutdown: broadcast::Sender<()>,
    retired: AtomicBool,
    tick_task: TaskMutex<Option<JoinHandle<()>>>,
    lifecycle: Mutex<()>,
    pending_suspension: TaskMutex<Option<RecoveryImage>>,
    initial_replay_len: usize,
    committed_checkpoint: TaskMutex<Option<RecoveryImage>>,
}
impl<S> Drop for HostedMatch<S> {
    fn drop(&mut self) {
        if let Some(task) = self
            .tick_task
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            task.abort();
        }
    }
}
struct Inner<S> {
    matches: TaskMutex<BTreeMap<MatchId, Arc<HostedMatch<S>>>>,
    max_matches: usize,
    gate: Arc<RwLock<bool>>,
    serving: AtomicBool,
    claimed: AtomicBool,
    stop: broadcast::Sender<()>,
    persistence: Option<SnapshotPersistence>,
    cold_admission: Option<suspension::ColdAdmission<S>>,
}
/// Clonable management capability for trusted application servers, never a browser credential.
pub struct LiveMatchHost<S> {
    inner: Arc<Inner<S>>,
}
impl<S> Clone for LiveMatchHost<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
impl<S: GameSimulation> LiveMatchHost<S> {
    pub fn new(host: MatchHost<S>) -> Self {
        Self::build(host, None)
    }

    /// Enables durable snapshot publication. Initial runtime state must already
    /// have a durable recovery basis. The simulation must support canonical restore.
    pub fn with_persistence(
        host: MatchHost<S>,
        persistence: SnapshotPersistence,
    ) -> Result<Self, LiveHostError> {
        for status in host.statuses() {
            if host
                .runtime(&status.id)
                .is_none_or(|runtime| runtime.replay_log().is_none())
            {
                return Err(LiveHostError::ReplayCaptureDisabled);
            }
        }
        Ok(Self::build(host, Some(persistence)))
    }
    fn build(host: MatchHost<S>, persistence: Option<SnapshotPersistence>) -> Self {
        let max_matches = host.max_matches();
        let draining = host.is_draining();
        let matches = host
            .into_runtimes()
            .into_iter()
            .map(|(id, runtime)| (id, Arc::new(hosted(runtime))))
            .collect();
        let (stop, _) = broadcast::channel(1);
        Self {
            inner: Arc::new(Inner {
                matches: TaskMutex::new(matches),
                max_matches,
                gate: Arc::new(RwLock::new(draining)),
                serving: AtomicBool::new(false),
                claimed: AtomicBool::new(false),
                stop,
                persistence,
                cold_admission: None,
            }),
        }
    }
    pub(crate) fn has_persistence(&self) -> bool {
        self.inner.persistence.is_some()
    }

    /// Captures periodic checkpoint evidence without storage I/O. Durable hosts
    /// return only their last committed boundary, even while a newer tick awaits I/O.
    pub async fn checkpoint(&self, id: &MatchId) -> Result<RecoveryImage, LiveHostError> {
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        if self.has_persistence() {
            return hosted
                .committed_checkpoint
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .ok_or(LiveHostError::NotCommitted);
        }
        let mut runtime = hosted.runtime.lock().await;
        if hosted.retired.load(Ordering::Acquire) {
            return Err(LiveHostError::UnknownMatch);
        }
        runtime.checkpoint_replay().map_err(LiveHostError::Recovery)
    }

    pub fn is_serving(&self) -> bool {
        self.inner.serving.load(Ordering::Acquire)
    }
    pub fn max_matches(&self) -> usize {
        self.inner.max_matches
    }
    pub(crate) fn admission_gate(&self) -> Arc<RwLock<bool>> {
        Arc::clone(&self.inner.gate)
    }
    pub(crate) fn get(&self, id: &MatchId) -> Option<Arc<HostedMatch<S>>> {
        self.inner
            .matches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(id)
            .filter(|hosted| !hosted.retired.load(Ordering::Acquire))
            .cloned()
    }
    pub(crate) fn entries(&self) -> Vec<(MatchId, Arc<HostedMatch<S>>)> {
        self.inner
            .matches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(id, hosted)| (id.clone(), Arc::clone(hosted)))
            .collect()
    }
    pub async fn statuses(&self) -> Vec<MatchStatus> {
        let mut statuses = Vec::new();
        // Snapshot membership before awaiting independent runtime locks.
        let matches = self.entries();
        for (id, hosted) in matches.iter() {
            let runtime = hosted.runtime.lock().await;
            statuses.push(MatchStatus {
                id: id.clone(),
                draining: runtime.is_draining() || hosted.retired.load(Ordering::Acquire),
                frozen: runtime.is_frozen(),
                current_tick: runtime.current_tick(),
                active_players: runtime.active_count(),
                occupied_player_slots: runtime.slot_count(),
                max_players: runtime.max_players(),
            });
        }
        statuses
    }
    /// Read-only trusted query, also available before serving starts.
    /// Consumers can capture restored metadata, then perform storage I/O outside
    /// the runtime lock. Removed/closed matches still fail as unknown. Never
    /// retain private projections publicly.
    pub async fn inspect<R>(
        &self,
        id: &MatchId,
        query: impl FnOnce(&MatchRuntime<S>) -> R,
    ) -> Result<R, LiveHostError> {
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        let runtime = hosted.runtime.lock().await;
        if hosted.retired.load(Ordering::Acquire) {
            return Err(LiveHostError::UnknownMatch);
        }
        Ok(query(&runtime))
    }
    /// Submits trusted server data for the current connection after external I/O.
    /// Rechecks the epoch under the runtime lock; stale control handlers cannot
    /// mutate a replacement connection or a retired match.
    pub async fn submit_host_input(
        &self,
        id: &MatchId,
        player_id: crate::PlayerId,
        connection_epoch: u32,
        payload: &[u8],
    ) -> Result<(), LiveHostError> {
        let gate = self.inner.gate.read().await;
        if !self.is_serving() {
            return Err(LiveHostError::NotServing);
        }
        if *gate {
            return Err(LiveHostError::Draining);
        }
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        let mut runtime = hosted.runtime.lock().await;
        if hosted.retired.load(Ordering::Acquire) {
            return Err(LiveHostError::UnknownMatch);
        }
        runtime
            .validate_connection(player_id, connection_epoch)
            .map_err(LiveHostError::Runtime)?;
        runtime
            .submit_host_input(player_id, payload)
            .map_err(LiveHostError::Runtime)
    }

    /// Failed placement returns the original runtime intact. Routing and task start precede return.
    /// With persistence enabled, the caller must establish the initial durable recovery basis first.
    pub async fn place(
        &self,
        id: MatchId,
        runtime: MatchRuntime<S>,
    ) -> Result<(), LivePlacementFailure<S>> {
        let gate = self.inner.gate.read().await;
        let mut matches = self
            .inner
            .matches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let reason = if !self.inner.serving.load(Ordering::Acquire) {
            Some(LiveHostError::NotServing)
        } else if *gate {
            Some(LiveHostError::Draining)
        } else if matches.contains_key(&id) {
            Some(LiveHostError::DuplicateMatch)
        } else if matches.len() >= self.inner.max_matches {
            Some(LiveHostError::AtCapacity)
        } else if runtime.tick_hz() == 0 {
            Some(LiveHostError::InvalidTickRate)
        } else if runtime.max_players() > usize::from(u16::MAX) {
            Some(LiveHostError::PlayerCapacityTooLarge)
        } else if self.has_persistence() && runtime.replay_log().is_none() {
            Some(LiveHostError::ReplayCaptureDisabled)
        } else if runtime.is_draining() || runtime.is_frozen() {
            Some(LiveHostError::Draining)
        } else {
            None
        };
        if let Some(error) = reason {
            return Err(LivePlacementFailure {
                error,
                id,
                runtime: Box::new(runtime),
            });
        }
        let tick_hz = runtime.tick_hz();
        let hosted = Arc::new(hosted(runtime));
        start_tick(
            &id,
            &hosted,
            self.inner.stop.subscribe(),
            tick_hz,
            self.inner.persistence.clone(),
        );
        matches.insert(id, hosted);
        Ok(())
    }
    /// Immediate retirement fences even reconnectable/active sessions before releasing the match ID.
    pub async fn retire(&self, id: &MatchId) -> Result<(), LiveHostError> {
        // The owned task finishes fencing and releases capacity even if the caller cancels.
        let host = self.clone();
        let id = id.clone();
        tokio::spawn(async move { host.retire_inner(&id).await })
            .await
            .map_err(|_| LiveHostError::LifecycleFailed)?
    }
    async fn retire_inner(&self, id: &MatchId) -> Result<(), LiveHostError> {
        let gate = self.inner.gate.read().await;
        if *gate {
            return Err(LiveHostError::Draining);
        }
        if !self.inner.serving.load(Ordering::Acquire) {
            return Err(LiveHostError::NotServing);
        }
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        let _lifecycle = hosted.lifecycle.lock().await;
        if hosted.retired.swap(true, Ordering::AcqRel) {
            return Err(LiveHostError::UnknownMatch);
        }
        self.remove_hosted(id, &hosted).await;
        Ok(())
    }
    async fn remove_hosted(&self, id: &MatchId, hosted: &Arc<HostedMatch<S>>) {
        hosted.retired.store(true, Ordering::Release);
        hosted.runtime.lock().await.freeze_for_recovery();
        let _ = hosted.shutdown.send(());
        let task = hosted
            .tick_task
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.inner
            .matches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(id);
    }
    pub(crate) fn claim(&self) -> Result<(), LiveHostError> {
        self.inner
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| LiveHostError::AlreadyStarted)
    }
    pub(crate) async fn start(&self) {
        let matches = self.entries();
        for (id, hosted) in matches.iter() {
            let tick_hz = hosted.runtime.lock().await.tick_hz();
            start_tick(
                id,
                hosted,
                self.inner.stop.subscribe(),
                tick_hz,
                self.inner.persistence.clone(),
            );
        }
        self.inner.serving.store(true, Ordering::Release);
    }
    pub(crate) async fn begin_drain(&self) {
        let mut gate = self.inner.gate.write().await;
        *gate = true;
        for (_, hosted) in self.entries() {
            hosted.runtime.lock().await.begin_drain();
        }
    }
    /// Process admission state, including the interval while individual runtimes enter drain.
    /// Observations serialize with that transition instead of reporting partially drained membership.
    pub async fn is_draining(&self) -> bool {
        *self.inner.gate.read().await
    }
    pub(crate) fn close(&self) {
        let mut matches = self
            .inner
            .matches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        self.inner.serving.store(false, Ordering::Release);
        let _ = self.inner.stop.send(());
        for hosted in matches.values() {
            let _ = hosted.shutdown.send(());
        }
        matches.clear();
    }
    pub(crate) async fn stop(&self) {
        let entries = self.entries();
        self.close();
        for (_, hosted) in entries {
            let _ = hosted.shutdown.send(());
            let task = hosted
                .tick_task
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            if let Some(task) = task {
                task.abort();
                let _ = task.await;
            }
        }
    }
}
fn hosted<S: GameSimulation>(runtime: MatchRuntime<S>) -> HostedMatch<S> {
    let initial_replay_len = runtime.replay_log().map_or(0, |log| log.records().len());
    let (snapshots, _) = broadcast::channel(SNAPSHOT_CHANNEL_DEPTH);
    let (shutdown, _) = broadcast::channel(1);
    HostedMatch {
        runtime: Arc::new(Mutex::new(runtime)),
        snapshots,
        shutdown,
        retired: AtomicBool::new(false),
        tick_task: TaskMutex::new(None),
        lifecycle: Mutex::new(()),
        pending_suspension: TaskMutex::new(None),
        initial_replay_len,
        committed_checkpoint: TaskMutex::new(None),
    }
}
fn start_tick<S: GameSimulation>(
    id: &MatchId,
    hosted: &Arc<HostedMatch<S>>,
    mut stop: broadcast::Receiver<()>,
    tick_hz: u16,
    persistence: Option<SnapshotPersistence>,
) {
    let runtime = Arc::clone(&hosted.runtime);
    let snapshots = hosted.snapshots.clone();
    let id = id.clone();
    let owner = Arc::downgrade(hosted);
    let mut cursor = hosted.initial_replay_len;
    let task = tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_micros(1_000_000 / u64::from(tick_hz)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.recv() => return,
                _ = ticker.tick() => {
                    let mut runtime = runtime.lock().await;
                    if let Some(persistence) = &persistence {
                        if runtime.is_frozen() { continue; }
                        let committed = async {
                            let snapshot = runtime.advance_tick().map_err(|error| PersistenceError::new(error.to_string()))?;
                            let checkpoint = runtime.checkpoint_image(snapshot.clone()).map_err(|error| PersistenceError::new(error.to_string()))?;
                            let (journal, journal_bytes) = runtime.replay_suffix(cursor, persistence.max_batch_bytes).map_err(|error| PersistenceError::new(error.to_string()))?;
                            let checkpoint_bytes = checkpoint.encode().map_err(|error| PersistenceError::new(error.to_string()))?.len();
                            let remaining = persistence.max_batch_bytes.checked_sub(journal_bytes).and_then(|left| left.checked_sub(checkpoint_bytes))
                                .ok_or_else(|| PersistenceError::new("persistence batch exceeds configured byte limit"))?;
                            let publication = committed_snapshot_publication(&runtime, snapshot.clone(), checkpoint.sessions.sessions.iter().map(|session| session.player_id), remaining)
                                .map_err(PersistenceError::new)?;
                            let batch = SnapshotCommitBatch { match_id: id.clone(), snapshot, journal, checkpoint };
                            // Serial backpressure: synchronous simulation has returned.
                            // Keep mutation fenced while cancellation-aware storage runs.
                            let was_draining = runtime.is_draining();
                            // Pessimistically fence before await: cancellation or a
                            // callback panic cannot leave an unfenced, uncommitted writer.
                            runtime.fail_persistence();
                            persistence.commit(&batch).await?;
                            runtime.finish_persistence(was_draining);
                            runtime.install_checkpoint(&batch.checkpoint);
                            cursor = batch.checkpoint.replay.records().len();
                            let hosted = owner.upgrade().ok_or_else(|| PersistenceError::new("host was dropped during commit"))?;
                            *hosted.committed_checkpoint.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(batch.checkpoint);
                            Ok::<_, PersistenceError>((hosted.retired.load(Ordering::Acquire), publication))
                        }.await;
                        match committed {
                            Ok((false, publication)) => { let _ = snapshots.send(publication); },
                            Ok((true, _)) => return,
                            Err(error) => {
                                runtime.fail_persistence();
                                if let Some(hosted) = owner.upgrade() {
                                    let _ = hosted.shutdown.send(());
                                }
                                eprintln!("persistence_commit_failed match={id} tick={} error={error}", runtime.current_tick());
                                return;
                            }
                        }
                        continue;
                    }
                    let scope = runtime.snapshot_scope();
                    match runtime.advance_tick() {
                        Ok(snapshot) => match snapshot_publication(scope, snapshot) {
                            Ok(publication) => { let _ = snapshots.send(publication); },
                            Err(error) => eprintln!("snapshot encoding failed for match {id}: {error}"),
                        },
                        Err(RuntimeError::Frozen) => {},
                        Err(error) => eprintln!("authoritative tick failed for match {id}: {error}"),
                    }
                }
            }
        }
    });
    *hosted
        .tick_task
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = Some(task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation};

    fn id(value: &str) -> MatchId {
        MatchId::new(value).unwrap()
    }
    fn runtime() -> MatchRuntime<DemoSimulation> {
        MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS)
    }
    async fn live(capacity: usize) -> LiveMatchHost<DemoSimulation> {
        let host = LiveMatchHost::new(MatchHost::new(capacity).unwrap());
        host.claim().unwrap();
        host.start().await;
        host
    }
    #[tokio::test]
    async fn empty_host_places_ticks_retires_and_reuses_capacity() {
        let host = live(1).await;
        assert!(host.statuses().await.is_empty());
        host.place(id("alpha"), runtime()).await.unwrap();
        let old = host.get(&id("alpha")).unwrap();
        let mut snapshots = old.snapshots.subscribe();
        tokio::time::timeout(Duration::from_secs(1), snapshots.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            host.place(id("alpha"), runtime())
                .await
                .unwrap_err()
                .error(),
            &LiveHostError::DuplicateMatch
        );
        assert_eq!(
            host.place(id("beta"), runtime()).await.unwrap_err().error(),
            &LiveHostError::AtCapacity
        );
        host.retire(&id("alpha")).await.unwrap();
        assert!(host.get(&id("alpha")).is_none());
        assert!(old.runtime.lock().await.is_frozen());
        let tick = old.runtime.lock().await.current_tick();
        host.place(id("alpha"), runtime()).await.unwrap();
        let replacement = host.get(&id("alpha")).unwrap();
        assert!(!Arc::ptr_eq(&old.runtime, &replacement.runtime));
        let mut replacement_snapshots = replacement.snapshots.subscribe();
        tokio::time::timeout(Duration::from_secs(1), replacement_snapshots.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.runtime.lock().await.current_tick(), tick);
        host.stop().await;
    }
    #[tokio::test]
    async fn process_drain_observation_waits_for_the_complete_runtime_transition() {
        let host = live(1).await;
        assert!(!host.is_draining().await);
        host.place(id("alpha"), runtime()).await.unwrap();
        let hosted = host.get(&id("alpha")).unwrap();
        let runtime = hosted.runtime.lock().await;
        let draining = {
            let host = host.clone();
            tokio::spawn(async move { host.begin_drain().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.inner.gate.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !runtime.is_draining(),
            "runtime lock deliberately delays the process transition"
        );
        let observation = host.is_draining();
        tokio::pin!(observation);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(observation.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(runtime);
        assert!(observation.await);
        draining.await.unwrap();
        assert!(hosted.runtime.lock().await.is_draining());
        host.stop().await;
    }

    #[tokio::test]
    async fn concurrent_placement_obeys_capacity_and_drain_fences_mutations() {
        let host = live(1).await;
        let (alpha, beta) = tokio::join!(
            host.place(id("alpha"), runtime()),
            host.place(id("beta"), runtime())
        );
        assert_eq!(usize::from(alpha.is_ok()) + usize::from(beta.is_ok()), 1);
        assert_eq!(host.statuses().await.len(), 1);
        host.begin_drain().await;
        assert_eq!(
            host.place(id("gamma"), runtime())
                .await
                .unwrap_err()
                .error(),
            &LiveHostError::Draining
        );
        let match_id = host.statuses().await[0].id.clone();
        assert_eq!(host.retire(&match_id).await, Err(LiveHostError::Draining));
        host.stop().await;
        assert_eq!(host.claim(), Err(LiveHostError::AlreadyStarted));
    }
    #[tokio::test]
    async fn cancellation_does_not_strand_retirement_capacity() {
        let host = live(1).await;
        host.place(id("alpha"), runtime()).await.unwrap();
        let hosted = host.get(&id("alpha")).unwrap();
        let guard = hosted.runtime.lock().await;
        let retiring = {
            let host = host.clone();
            tokio::spawn(async move { host.retire(&id("alpha")).await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.get(&id("alpha")).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        retiring.abort();
        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !host.statuses().await.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        host.place(id("beta"), runtime()).await.unwrap();
        host.stop().await;
    }
    #[tokio::test]
    async fn hosted_runtimes_have_independent_locks() {
        let host = live(2).await;
        host.place(id("alpha"), runtime()).await.unwrap();
        host.place(id("beta"), runtime()).await.unwrap();
        let alpha = host.get(&id("alpha")).unwrap();
        let beta = host.get(&id("beta")).unwrap();
        {
            let _alpha = alpha.runtime.lock().await;
            let _beta = beta.runtime.try_lock().unwrap();
        }
        host.stop().await;
    }
}
