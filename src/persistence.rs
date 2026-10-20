//! Optional immutable commit boundary between authoritative ticks and publication.
use crate::{MatchId, RecoveryImage, ReplayLog, SimulationSnapshot};
use std::{error::Error, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistenceError(String);
impl PersistenceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl fmt::Display for PersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
impl Error for PersistenceError {}

/// A single tick's immutable state and newly accepted authoritative replay records.
/// The checkpoint also carries reconnect/session and per-lane sequence metadata.
/// Database fencing, grant deduplication and uncertain-commit resolution belong to the consumer.
#[derive(Debug)]
pub struct SnapshotCommitBatch {
    pub match_id: MatchId,
    pub snapshot: SimulationSnapshot,
    pub journal: ReplayLog,
    pub checkpoint: RecoveryImage,
}

pub type SnapshotCommitFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PersistenceError>> + Send + 'a>>;

pub trait SnapshotCommitter: Send + Sync + 'static {
    /// Returns success only after the required durable commit. No live runtime is
    /// exposed. The future must propagate cancellation to any owned I/O or workers;
    /// it is dropped on deadline or lifecycle cancellation. Never block the executor.
    fn commit<'a>(&'a self, batch: &'a SnapshotCommitBatch) -> SnapshotCommitFuture<'a>;
}

#[derive(Clone)]
pub struct SnapshotPersistence {
    pub(crate) service: Arc<dyn SnapshotCommitter>,
    pub(crate) timeout: Duration,
    pub(crate) max_batch_bytes: usize,
}
impl SnapshotPersistence {
    pub fn new(
        service: Arc<dyn SnapshotCommitter>,
        timeout: Duration,
        max_batch_bytes: usize,
    ) -> Result<Self, PersistenceError> {
        if max_batch_bytes == 0 {
            return Err(PersistenceError::new(
                "persistence byte limit must be nonzero",
            ));
        }
        if timeout.is_zero() {
            return Err(PersistenceError::new("persistence timeout must be nonzero"));
        }
        Ok(Self {
            service,
            timeout,
            max_batch_bytes,
        })
    }
    pub(crate) async fn commit(&self, batch: &SnapshotCommitBatch) -> Result<(), PersistenceError> {
        tokio::time::timeout(self.timeout, self.service.commit(batch))
            .await
            .map_err(|_| {
                PersistenceError::new("persistence commit timed out; outcome is uncertain")
            })?
    }
}
