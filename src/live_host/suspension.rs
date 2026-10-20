//! Optional cold admission and immutable empty-match eviction boundary.
use super::{LiveHostError, LiveMatchHost};
use crate::{GameSimulation, MatchId, PersistenceError, RecoveryImage, SnapshotCommitFuture};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// Restore only an authorized known match from durable evidence. Never substitute
/// a fresh runtime for a failed restore. The host serializes cold resolutions;
/// resident matches bypass this hook. Storage I/O must propagate cancellation.
pub trait ColdMatchResolver<S>: Send + Sync + 'static {
    fn resolve<'a>(
        &'a self,
        host: &'a LiveMatchHost<S>,
        id: &'a MatchId,
    ) -> SnapshotCommitFuture<'a>;
}

pub(super) struct ColdAdmission<S> {
    service: Arc<dyn ColdMatchResolver<S>>,
    timeout: Duration,
    serial: Mutex<()>,
}

impl<S: GameSimulation> LiveMatchHost<S> {
    /// Configure before sharing or serving the host. Existing hosts reject cold
    /// routes unless explicitly opted in. The deadline includes queueing time.
    pub fn with_cold_admission(
        mut self,
        service: Arc<dyn ColdMatchResolver<S>>,
        timeout: Duration,
    ) -> Result<Self, LiveHostError> {
        if timeout.is_zero() {
            return Err(LiveHostError::InvalidLifecycleTimeout);
        }
        let inner = Arc::get_mut(&mut self.inner).ok_or(LiveHostError::AlreadyStarted)?;
        if inner.claimed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LiveHostError::AlreadyStarted);
        }
        inner.cold_admission = Some(ColdAdmission {
            service,
            timeout,
            serial: Mutex::new(()),
        });
        Ok(self)
    }

    pub(crate) async fn resolve_match(&self, id: &MatchId) -> bool {
        if self.get(id).is_some() {
            return true;
        }
        if !self.is_serving() || self.is_draining().await {
            return false;
        }
        let Some(cold) = &self.inner.cold_admission else {
            return false;
        };
        let resolved = tokio::time::timeout(cold.timeout, async {
            let _serial = cold.serial.lock().await;
            if self.get(id).is_none() {
                cold.service.resolve(self, id).await?;
            }
            Ok::<_, PersistenceError>(())
        })
        .await;
        matches!(resolved, Ok(Ok(()))) && self.get(id).is_some()
    }

    /// Freeze an empty match and confirm its recoverability before eviction.
    /// Reconnectable slots count as occupied. The owned operation completes even
    /// if its caller disappears. A failed/timed-out confirmation retains the
    /// frozen resident writer and immutable image; retry this operation to finish.
    /// The callback runs outside the runtime lock and must not block the executor.
    pub async fn suspend_empty<F, Fut>(
        &self,
        id: &MatchId,
        timeout: Duration,
        confirm: F,
    ) -> Result<RecoveryImage, LiveHostError>
    where
        F: FnOnce(RecoveryImage) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), PersistenceError>> + Send + 'static,
    {
        if timeout.is_zero() {
            return Err(LiveHostError::InvalidLifecycleTimeout);
        }
        let host = self.clone();
        let id = id.clone();
        tokio::spawn(async move { host.suspend_inner(&id, timeout, confirm).await })
            .await
            .map_err(|_| LiveHostError::LifecycleFailed)?
    }

    async fn suspend_inner<F, Fut>(
        &self,
        id: &MatchId,
        timeout: Duration,
        confirm: F,
    ) -> Result<RecoveryImage, LiveHostError>
    where
        F: FnOnce(RecoveryImage) -> Fut + Send,
        Fut: Future<Output = Result<(), PersistenceError>> + Send,
    {
        let gate = self.inner.gate.read().await;
        if *gate {
            return Err(LiveHostError::Draining);
        }
        if !self.is_serving() {
            return Err(LiveHostError::NotServing);
        }
        let hosted = self.get(id).ok_or(LiveHostError::UnknownMatch)?;
        let _lifecycle = hosted.lifecycle.lock().await;
        if hosted.retired.load(std::sync::atomic::Ordering::Acquire) {
            return Err(LiveHostError::UnknownMatch);
        }
        let pending = hosted
            .pending_suspension
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let image = if let Some(image) = pending {
            image
        } else {
            let mut runtime = hosted.runtime.lock().await;
            if runtime.slot_count() != 0 {
                return Err(LiveHostError::MatchOccupied);
            }
            if runtime.is_draining() || runtime.is_frozen() {
                return Err(LiveHostError::Draining);
            }
            let image = runtime
                .checkpoint_replay()
                .map_err(LiveHostError::Recovery)?;
            runtime.freeze_for_recovery();
            *hosted
                .pending_suspension
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(image.clone());
            image
        };
        tokio::time::timeout(timeout, confirm(image.clone()))
            .await
            .map_err(|_| {
                LiveHostError::Suspension(PersistenceError::new(
                    "suspension confirmation timed out; match retained frozen",
                ))
            })?
            .map_err(LiveHostError::Suspension)?;
        self.remove_hosted(id, &hosted).await;
        Ok(image)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MatchHost, MatchRuntime, ReconnectToken, checkpoint_tests::CounterSimulation};
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn id() -> MatchId {
        MatchId::new("alpha").unwrap()
    }
    fn runtime() -> MatchRuntime<CounterSimulation> {
        MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 100_000)
    }
    async fn live() -> LiveMatchHost<CounterSimulation> {
        let host = LiveMatchHost::new(MatchHost::new(1).unwrap());
        host.claim().unwrap();
        host.start().await;
        host.place(id(), runtime()).await.unwrap();
        host
    }

    #[tokio::test]
    async fn suspension_refuses_active_and_reconnectable_slots() {
        let host = live().await;
        let hosted = host.get(&id()).unwrap();
        let lease = hosted
            .runtime
            .lock()
            .await
            .admit(ReconnectToken([1; 16]))
            .unwrap();
        for disconnected in [false, true] {
            if disconnected {
                assert!(
                    hosted
                        .runtime
                        .lock()
                        .await
                        .disconnect(lease.player_id, lease.connection_epoch)
                );
            }
            assert_eq!(
                host.suspend_empty(&id(), Duration::from_secs(1), |_| async {
                    panic!("occupied matches must never reach durability confirmation");
                })
                .await
                .unwrap_err(),
                LiveHostError::MatchOccupied
            );
            assert!(!hosted.runtime.lock().await.is_frozen());
        }
        host.stop().await;
    }

    #[tokio::test]
    async fn suspension_keeps_failed_confirmation_frozen_and_retries_the_same_cold_image() {
        let host = live().await;
        let captured = Arc::new(StdMutex::new(None));
        let saved = Arc::clone(&captured);
        let query = host.clone();
        let failure = host
            .suspend_empty(&id(), Duration::from_secs(1), move |image| async move {
                // This query proves storage confirmation does not hold the runtime lock.
                let (frozen, snapshot, admission) = query
                    .inspect(&id(), |runtime| {
                        (
                            runtime.is_frozen(),
                            runtime.snapshot().unwrap(),
                            runtime.is_draining(),
                        )
                    })
                    .await
                    .unwrap();
                assert!(frozen && admission);
                assert_eq!(snapshot.tick, image.current_tick);
                *saved.lock().unwrap() = Some((image, snapshot));
                Err(PersistenceError::new("object storage unavailable"))
            })
            .await;
        assert!(matches!(failure, Err(LiveHostError::Suspension(_))));
        let (expected, snapshot) = captured.lock().unwrap().clone().unwrap();
        let hosted = host.get(&id()).unwrap();
        assert_eq!(
            hosted.runtime.lock().await.current_tick(),
            expected.current_tick
        );
        assert!(
            hosted
                .runtime
                .lock()
                .await
                .admit(ReconnectToken([2; 16]))
                .is_err()
        );
        let check = expected.clone();
        let image = host
            .suspend_empty(&id(), Duration::from_secs(1), move |image| async move {
                assert_eq!(image, check);
                Ok(())
            })
            .await
            .unwrap();
        assert!(host.get(&id()).is_none());
        assert!(host.statuses().await.is_empty());
        let restored =
            MatchRuntime::restore_from_recovery(CounterSimulation::default(), image).unwrap();
        assert_eq!(restored.snapshot().unwrap(), snapshot);
        assert!(!restored.is_frozen());
        host.stop().await;
    }

    #[tokio::test]
    async fn suspension_finishes_its_verified_eviction_after_the_caller_is_cancelled() {
        let host = live().await;
        let worker = host.clone();
        let (started, capture) = tokio::sync::oneshot::channel();
        let (verified, confirmation) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            worker
                .suspend_empty(&id(), Duration::from_secs(1), move |image| async move {
                    started.send(image).unwrap();
                    confirmation
                        .await
                        .map_err(|cause| PersistenceError::new(cause.to_string()))
                })
                .await
        });
        let image = capture.await.unwrap();
        caller.abort();
        assert!(host.get(&id()).unwrap().runtime.lock().await.is_frozen());
        assert_eq!(
            host.get(&id()).unwrap().runtime.lock().await.current_tick(),
            image.current_tick
        );
        verified.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while host.get(&id()).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        host.stop().await;
    }

    struct Restore {
        image: RecoveryImage,
        calls: AtomicUsize,
    }
    impl ColdMatchResolver<CounterSimulation> for Restore {
        fn resolve<'a>(
            &'a self,
            host: &'a LiveMatchHost<CounterSimulation>,
            requested: &'a MatchId,
        ) -> SnapshotCommitFuture<'a> {
            Box::pin(async move {
                if requested != &id() {
                    return Err(PersistenceError::new("unknown durable match"));
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                let runtime = MatchRuntime::restore_from_recovery(
                    CounterSimulation::default(),
                    self.image.clone(),
                )
                .map_err(|cause| PersistenceError::new(cause.to_string()))?;
                assert_eq!(runtime.current_tick(), self.image.current_tick);
                host.place(requested.clone(), runtime)
                    .await
                    .map_err(|cause| PersistenceError::new(cause.to_string()))
            })
        }
    }
    #[tokio::test]
    async fn cold_admissions_restore_once_and_refuse_unknown_matches() {
        let mut original = runtime();
        for _ in 0..17 {
            original.advance_tick().unwrap();
        }
        let service = Arc::new(Restore {
            image: original.checkpoint_replay().unwrap(),
            calls: AtomicUsize::new(0),
        });
        let host = LiveMatchHost::new(MatchHost::new(1).unwrap())
            .with_cold_admission(service.clone(), Duration::from_secs(1))
            .unwrap();
        host.claim().unwrap();
        host.start().await;
        let match_id = id();
        let (first, second) =
            tokio::join!(host.resolve_match(&match_id), host.resolve_match(&match_id));
        assert!(first && second);
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);
        assert!(!host.resolve_match(&MatchId::new("unknown").unwrap()).await);
        assert_eq!(host.statuses().await.len(), 1);
        host.stop().await;
    }
}
