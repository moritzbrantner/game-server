//! Live hosted lifecycle. Consumers supply runtimes; the host owns capacity and retirement fencing.
use crate::connection::{SNAPSHOT_CHANNEL_DEPTH, SnapshotPublication, snapshot_publication};
use crate::runtime::RuntimeError;
use crate::{GameSimulation, MatchHost, MatchId, MatchRuntime, MatchStatus};
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
            }),
        }
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
    /// Read-only trusted query; do not perform blocking IO in the callback or retain private projections publicly.
    pub async fn inspect<R>(
        &self,
        id: &MatchId,
        query: impl FnOnce(&MatchRuntime<S>) -> R,
    ) -> Result<R, LiveHostError> {
        if !self.inner.serving.load(Ordering::Acquire) {
            return Err(LiveHostError::NotServing);
        }
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        let runtime = hosted.runtime.lock().await;
        if hosted.retired.load(Ordering::Acquire) {
            return Err(LiveHostError::UnknownMatch);
        }
        Ok(query(&runtime))
    }
    /// Failed placement returns the original runtime intact. Publication and task start precede return.
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
        start_tick(&id, &hosted, self.inner.stop.subscribe(), tick_hz);
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
        let hosted = {
            let matches = self
                .inner
                .matches
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let hosted = matches
                .get(id)
                .cloned()
                .ok_or(LiveHostError::UnknownMatch)?;
            if hosted.retired.swap(true, Ordering::AcqRel) {
                return Err(LiveHostError::UnknownMatch);
            }
            hosted
        };
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
        Ok(())
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
            start_tick(id, hosted, self.inner.stop.subscribe(), tick_hz);
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
fn hosted<S>(runtime: MatchRuntime<S>) -> HostedMatch<S> {
    let (snapshots, _) = broadcast::channel(SNAPSHOT_CHANNEL_DEPTH);
    let (shutdown, _) = broadcast::channel(1);
    HostedMatch {
        runtime: Arc::new(Mutex::new(runtime)),
        snapshots,
        shutdown,
        retired: AtomicBool::new(false),
        tick_task: TaskMutex::new(None),
    }
}
fn start_tick<S: GameSimulation>(
    id: &MatchId,
    hosted: &HostedMatch<S>,
    mut stop: broadcast::Receiver<()>,
    tick_hz: u16,
) {
    let runtime = Arc::clone(&hosted.runtime);
    let snapshots = hosted.snapshots.clone();
    let id = id.clone();
    let task = tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_micros(1_000_000 / u64::from(tick_hz)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.recv() => return,
                _ = ticker.tick() => {
                    let mut runtime = runtime.lock().await;
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
