use crate::{
    LiveMatchHost, MatchHost, MatchId, MatchRuntime, PersistenceError, ReconnectToken, ReplayLog,
    ReplayRecord, RuntimeRecoveryError, SnapshotCommitBatch, SnapshotCommitFuture,
    SnapshotCommitter, SnapshotPersistence, checkpoint_tests::CounterSimulation,
    connection::SnapshotPublication,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

type Reply = oneshot::Sender<Result<(), PersistenceError>>;
struct Pending {
    tick: u64,
    journal: ReplayLog,
    checkpoint: crate::RecoveryImage,
    reply: Reply,
}
struct ControlledCommit(mpsc::UnboundedSender<Pending>);
impl SnapshotCommitter for ControlledCommit {
    fn commit<'a>(&'a self, batch: &'a SnapshotCommitBatch) -> SnapshotCommitFuture<'a> {
        Box::pin(async move {
            assert_eq!(batch.snapshot.tick, batch.checkpoint.current_tick);
            let (reply, response) = oneshot::channel();
            self.0
                .send(Pending {
                    tick: batch.snapshot.tick,
                    journal: batch.journal.clone(),
                    checkpoint: batch.checkpoint.clone(),
                    reply,
                })
                .map_err(|_| PersistenceError::new("test receiver closed"))?;
            response
                .await
                .map_err(|_| PersistenceError::new("test commit abandoned"))?
        })
    }
}
fn host(
    timeout: Duration,
    limit: usize,
) -> (
    LiveMatchHost<CounterSimulation>,
    MatchId,
    mpsc::UnboundedReceiver<Pending>,
) {
    let mut runtime = MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 30);
    runtime.admit(ReconnectToken([1; 16])).unwrap();
    // A multi-player canonical payload exercises the compact digest journal path.
    runtime.admit(ReconnectToken([2; 16])).unwrap();
    let id = MatchId::new("durable").unwrap();
    let mut matches = MatchHost::new(1).unwrap();
    matches.insert(id.clone(), runtime).unwrap();
    let (requests, receiver) = mpsc::unbounded_channel();
    let persistence =
        SnapshotPersistence::new(Arc::new(ControlledCommit(requests)), timeout, limit).unwrap();
    (
        LiveMatchHost::with_persistence(matches, persistence).unwrap(),
        id,
        receiver,
    )
}
async fn next(receiver: &mut mpsc::UnboundedReceiver<Pending>) -> Pending {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn hosted_commit_gates_mutation_publication_and_checkpoint_visibility() {
    let (host, id, mut requests) = host(Duration::from_secs(2), 1_000_000);
    let hosted = host.get(&id).unwrap();
    let mut snapshots = hosted.snapshots.subscribe();
    host.claim().unwrap();
    host.start().await;
    let first = next(&mut requests).await;
    assert_eq!(first.tick, 1);
    assert!(matches!(
        snapshots.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        host.checkpoint(&id).await,
        Err(crate::LiveHostError::NotCommitted)
    ));
    let writer = host.clone();
    let route = id.clone();
    let mut command =
        tokio::spawn(async move { writer.submit_host_input(&route, 1, 1, &[9]).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut command)
            .await
            .is_err()
    );
    assert!(
        requests.try_recv().is_err(),
        "a pending commit prevents a later tick"
    );
    first.reply.send(Ok(())).unwrap();
    command.await.unwrap().unwrap();
    let publication = tokio::time::timeout(Duration::from_secs(2), snapshots.recv())
        .await
        .unwrap()
        .unwrap();
    let SnapshotPublication::CommittedPlayers(frames) = publication else {
        panic!("expected captured private frames");
    };
    let frame = crate::decode_snapshot(&frames[&1]).unwrap();
    assert_eq!(frame.tick, 1);
    assert_eq!(frame.payload, 1_u64.to_be_bytes());
    let second = next(&mut requests).await;
    assert_eq!(second.tick, 2);
    assert_eq!(
        second
            .journal
            .records()
            .iter()
            .filter(|record| matches!(record, ReplayRecord::HostInputApplied { .. }))
            .count(),
        1
    );
    assert!(
        !second
            .journal
            .records()
            .iter()
            .any(|record| matches!(record, ReplayRecord::Baseline { .. })),
        "the committed baseline is not re-journaled"
    );
    assert_eq!(
        host.checkpoint(&id).await.unwrap().current_tick,
        1,
        "a pending tick cannot replace the committed checkpoint"
    );
    assert!(matches!(
        second.journal.records().last(),
        Some(ReplayRecord::DigestCheckpoint { .. })
    ));
    let mut recovery = host.checkpoint(&id).await.unwrap();
    recovery.replay.append_journal(&second.journal).unwrap();
    recovery.current_tick = second.tick;
    recovery.sessions = second.checkpoint.sessions.clone();
    let mut unverified = recovery.clone();
    unverified.replay.append(ReplayRecord::HostInputApplied {
        tick: recovery.current_tick,
        player_id: 1,
        payload: vec![1],
    });
    assert!(matches!(
        unverified.encode(),
        Err(crate::RecoveryError::MissingFinalCheckpoint)
    ));
    let mut corrupt = recovery.clone();
    corrupt.replay.append(ReplayRecord::DigestCheckpoint {
        tick: recovery.current_tick,
        digest: [0; 32],
    });
    assert!(MatchRuntime::restore_from_recovery(CounterSimulation::default(), corrupt).is_err());
    let decoded = crate::RecoveryImage::decode(&recovery.encode().unwrap()).unwrap();
    let restored =
        MatchRuntime::restore_from_recovery(CounterSimulation::default(), decoded).unwrap();
    assert_eq!(
        restored.snapshot().unwrap(),
        second
            .checkpoint
            .replay
            .records()
            .first()
            .and_then(|record| {
                if let ReplayRecord::Baseline { baseline } = record {
                    Some(baseline.snapshot.clone())
                } else {
                    None
                }
            })
            .unwrap()
    );
    second.reply.send(Ok(())).unwrap();
    let third = next(&mut requests).await;
    assert_eq!(host.checkpoint(&id).await.unwrap().current_tick, 2);
    assert!(
        !third
            .journal
            .records()
            .iter()
            .any(|record| matches!(record, ReplayRecord::HostInputApplied { .. })),
        "accepted inputs are not repeated"
    );
    assert!(hosted.runtime.try_lock().is_err());
    // Stop aborts the owned tick task and drops the consumer's in-flight future.
    host.stop().await;
    assert!(third.reply.is_closed());
    assert_eq!(
        hosted.runtime.lock().await.recovery_image().unwrap_err(),
        RuntimeRecoveryError::PersistenceFailed
    );
}

#[tokio::test]
async fn failed_commit_freezes_writer_without_publishing_or_saving_uncommitted_state() {
    let (host, id, mut requests) = host(Duration::from_secs(2), 1_000_000);
    let hosted = host.get(&id).unwrap();
    let mut closed = hosted.shutdown.subscribe();
    let mut snapshots = hosted.snapshots.subscribe();
    host.claim().unwrap();
    host.start().await;
    let first = next(&mut requests).await;
    first
        .reply
        .send(Err(PersistenceError::new("uncertain database commit")))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), closed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(snapshots.try_recv().is_err());
    let mut runtime = hosted.runtime.lock().await;
    assert!(runtime.is_frozen());
    assert!(runtime.admit(ReconnectToken([2; 16])).is_err());
    assert_eq!(
        runtime.recovery_image().unwrap_err(),
        RuntimeRecoveryError::PersistenceFailed
    );
    runtime.resume_after_failed_recovery();
    assert!(runtime.is_frozen());
    drop(runtime);
    host.retire(&id).await.unwrap();
    assert!(
        host.statuses().await.is_empty(),
        "failed matches can be retired to release capacity"
    );
    host.stop().await;
}

#[tokio::test]
async fn deadline_cancels_commit_and_byte_limit_rejects_before_calling_storage() {
    let (host, id, mut requests) = host(Duration::from_millis(30), 1_000_000);
    let hosted = host.get(&id).unwrap();
    let mut closed = hosted.shutdown.subscribe();
    host.claim().unwrap();
    host.start().await;
    let first = next(&mut requests).await;
    tokio::time::timeout(Duration::from_secs(2), closed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(first.reply.is_closed());
    assert!(hosted.runtime.lock().await.is_frozen());
    host.stop().await;

    let (limited, id, mut requests) = self::host(Duration::from_secs(2), 16);
    let hosted = limited.get(&id).unwrap();
    let mut closed = hosted.shutdown.subscribe();
    limited.claim().unwrap();
    limited.start().await;
    tokio::time::timeout(Duration::from_secs(2), closed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(requests.try_recv().is_err());
    assert!(hosted.runtime.lock().await.is_frozen());
    limited.stop().await;
}

#[test]
fn invalid_journal_append_preserves_the_checkpoint_prefix() {
    let mut runtime = MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 30);
    runtime.advance_tick().unwrap();
    let checkpoint = runtime.checkpoint_replay().unwrap();
    let mut joined = checkpoint.replay.clone();
    assert!(joined.append_journal(&checkpoint.replay).is_err());
    assert_eq!(joined, checkpoint.replay);
    let mut backwards = ReplayLog::default();
    backwards.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    assert!(joined.append_journal(&backwards).is_err());
    assert_eq!(joined, checkpoint.replay);
}

#[test]
fn persistence_requires_explicit_limits_and_replay_capture() {
    let (requests, _) = mpsc::unbounded_channel();
    let service = Arc::new(ControlledCommit(requests));
    assert!(SnapshotPersistence::new(service.clone(), Duration::ZERO, 1000).is_err());
    assert!(SnapshotPersistence::new(service.clone(), Duration::from_secs(1), 0).is_err());
    let mut matches = MatchHost::new(1).unwrap();
    matches
        .insert(
            MatchId::new("uncaptured").unwrap(),
            MatchRuntime::new(CounterSimulation::default(), 10),
        )
        .unwrap();
    let persistence = SnapshotPersistence::new(service, Duration::from_secs(1), 1000).unwrap();
    assert!(matches!(
        LiveMatchHost::with_persistence(matches, persistence),
        Err(crate::LiveHostError::ReplayCaptureDisabled)
    ));
}
