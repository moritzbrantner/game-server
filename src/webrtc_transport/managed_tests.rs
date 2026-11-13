use super::*;
use crate::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

struct CommitTicks(Mutex<Vec<u64>>);
impl SnapshotCommitter for CommitTicks {
    fn commit<'a>(&'a self, batch: &'a SnapshotCommitBatch) -> SnapshotCommitFuture<'a> {
        Box::pin(async move {
            self.0.lock().unwrap().push(batch.snapshot.tick);
            Ok(())
        })
    }
}
struct ParticipantEcho;
impl WebRtcControlService for ParticipantEcho {
    fn handle(
        &self,
        participant: &str,
        id: &MatchId,
        context: ControlContext,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, ControlServiceError> {
        assert_eq!(participant, "00000002");
        assert_eq!(id.as_str(), "managed");
        assert_eq!(context.player_id, 2);
        assert!(context.connection_epoch > 0);
        Ok(payload.to_vec())
    }
}

struct ContentProbe {
    blocked: tokio::sync::Notify,
    canceled: std::sync::atomic::AtomicBool,
}
struct PendingContent<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for PendingContent<'_> {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
impl WebRtcContentService for ContentProbe {
    fn handle<'a>(
        &'a self,
        participant: &'a str,
        id: &'a MatchId,
        context: ControlContext,
        request: &'a [u8],
    ) -> WebRtcContentFuture<'a> {
        Box::pin(async move {
            assert_eq!(participant, "00000002");
            assert_eq!(id.as_str(), "managed");
            assert_eq!(context.player_id, 2);
            assert!(context.connection_epoch > 0);
            if request == b"refused" {
                return Err(ControlServiceError::new("unavailable"));
            }
            if request == b"pending" {
                let _pending = PendingContent(&self.canceled);
                self.blocked.notify_one();
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(vec![request[0]; MAX_CONTENT_BYTES])
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_attachment_keeps_one_committed_tick_owner_and_scoped_control() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let id = MatchId::new("managed").unwrap();
        let mut matches = MatchHost::new(1).unwrap();
        let mut runtime = MatchRuntime::new_with_replay_capture(
            crate::checkpoint_tests::CounterSimulation::default(),
            400,
        );
        let private = runtime.admit(ReconnectToken([7; 16])).unwrap();
        runtime.disconnect(private.player_id, private.connection_epoch);
        matches.insert(id.clone(), runtime).unwrap();
        let commits = Arc::new(CommitTicks(Mutex::new(Vec::new())));
        let persistence =
            SnapshotPersistence::new(commits.clone(), Duration::from_secs(1), 1024 * 1024).unwrap();
        let host = LiveMatchHost::with_persistence(matches, persistence).unwrap();
        host.claim().unwrap();
        host.start().await; // Existing managed lifecycle owns the only tick loop.
        let (lobby, guest, _signaling) = super::setup::test_support::lobbies().await;
        let content = Arc::new(ContentProbe {
            blocked: tokio::sync::Notify::new(),
            canceled: std::sync::atomic::AtomicBool::new(false),
        });
        let adapter = WebRtcHost::attach(
            lobby,
            host.clone(),
            id.clone(),
            Arc::new(ParticipantEcho),
            WebRtcHostConfig {
                bind_ip: "127.0.0.1".to_owned(),
                relay_only: false,
            },
        )
        .unwrap()
        .with_content_service(content.clone());
        let (stop, shutdown) = oneshot::channel();
        let (approved, approval) = oneshot::channel();
        let (requested, request) = oneshot::channel();
        let approval = Arc::new(tokio::sync::Mutex::new(Some((approval, requested))));
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let current = Arc::clone(&allowed);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(adapter.run_with_authorization(
            move |participant, proof| {
                let approval = Arc::clone(&approval);
                async move {
                    assert_eq!(participant, "00000002");
                    assert_eq!(proof, b"game proof");
                    if let Some((approval, requested)) = approval.lock().await.take() {
                        requested.send(()).unwrap();
                        approval.await.unwrap();
                    }
                    true
                }
            },
            move |_| current.load(std::sync::atomic::Ordering::Acquire),
            async {
                let _ = shutdown.await;
            },
        ));
        let mut guests = tokio::task::JoinSet::new();
        guests.spawn(WebRtcClient::connect_with_content_authorization(
            guest,
            id.clone(),
            "127.0.0.1",
            false,
            b"game proof".to_vec(),
        ));
        request.await.unwrap();
        assert_eq!(host.statuses().await[0].occupied_player_slots, 1);
        approved.send(()).unwrap();
        let mut client = guests.join_next().await.unwrap().unwrap().unwrap();
        assert_eq!(client.control(b"bound").await.unwrap().payload, b"bound");
        let snapshot = client.snapshot().await.unwrap();
        assert!(snapshot.tick > 0);
        assert_eq!(snapshot.payload.len(), 8); // Canonical payload contains both players.
        assert_eq!(
            client.command(1, &[3]).await.unwrap().outcome,
            ReliableCommandOutcome::Applied
        );
        let transfer = client.content_client().unwrap();
        let (bytes, ()) = tokio::join!(
            async {
                let mut received = Vec::new();
                for chunk in 0..16_u8 {
                    received.extend(transfer.request(&[chunk]).await.unwrap());
                }
                received
            },
            async {
                assert_eq!(
                    client.control(b"during content").await.unwrap().payload,
                    b"during content"
                );
                assert_eq!(
                    client.command(2, &[3]).await.unwrap().outcome,
                    ReliableCommandOutcome::Applied
                );
                assert!(client.snapshot().await.unwrap().tick > snapshot.tick);
                client.send_intent(3, &[3]).await.unwrap();
            }
        );
        assert_eq!(bytes.len(), 256 * 1024);
        assert!(
            bytes
                .as_chunks::<MAX_CONTENT_BYTES>()
                .0
                .iter()
                .enumerate()
                .all(|(index, chunk)| chunk.iter().all(|byte| usize::from(*byte) == index))
        );
        assert!(
            transfer
                .request(&vec![0; MAX_CONTENT_BYTES + 1])
                .await
                .is_err()
        );
        assert_eq!(
            transfer.request(b"refused").await,
            Err(WebRtcContentError::Refused)
        );
        assert_eq!(
            transfer.request(&[17]).await.unwrap(),
            vec![17; MAX_CONTENT_BYTES]
        );
        assert_eq!(
            client.control(b"after refusal").await.unwrap().payload,
            b"after refusal"
        );
        let old = *client.welcome();
        client.disconnect().await.unwrap();
        let mut client = client.reconnect().await.unwrap();
        assert!(transfer.request(b"old connection").await.is_err());
        assert_eq!(client.welcome().player_id, old.player_id);
        assert!(client.welcome().connection_epoch > old.connection_epoch);
        assert_eq!(
            client.control(b"resumed").await.unwrap().payload,
            b"resumed"
        );
        let pending_content = client.content_client().unwrap();
        let mut pending = tokio::task::JoinSet::new();
        pending.spawn(async move { pending_content.request(b"pending").await });
        content.blocked.notified().await;
        // The runtime lock remains available while game-owned content I/O waits.
        assert_eq!(
            client.control(b"while blocked").await.unwrap().payload,
            b"while blocked"
        );
        allowed.store(false, std::sync::atomic::Ordering::Release);
        loop {
            if host.statuses().await[0].active_players == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending.join_next().await.unwrap().unwrap().is_err());
        assert!(content.canceled.load(std::sync::atomic::Ordering::Acquire));
        client.close().await.unwrap();
        stop.send(()).unwrap();
        tasks.join_next().await.unwrap().unwrap().unwrap();
        assert!(host.is_serving());
        assert!(
            !host
                .inspect(&id, |runtime| runtime.is_frozen())
                .await
                .unwrap()
        );
        // Every advanced tick must have passed through the managed commit path.
        // A second standalone WebRTC ticker would create a gap here.
        host.inspect(&id, |runtime| {
            let ticks = commits.0.lock().unwrap();
            assert_eq!(ticks.len() as u64, runtime.current_tick());
            assert!(ticks.iter().copied().eq(1..=runtime.current_tick()));
        })
        .await
        .unwrap();
        let checkpoint = host.checkpoint(&id).await.unwrap();
        let restored = MatchRuntime::restore_from_recovery(
            crate::checkpoint_tests::CounterSimulation::default(),
            checkpoint,
        )
        .unwrap();
        assert!(restored.current_tick() >= snapshot.tick);
        assert_eq!(restored.slot_count(), 2);
        host.stop().await;
    })
    .await
    .expect("bounded managed-host attachment probe");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn content_requires_mutual_opt_in_without_changing_gameplay() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for host_content in [false, true] {
            let id = MatchId::new("optional").unwrap();
            let mut matches = MatchHost::new(1).unwrap();
            matches
                .insert(
                    id.clone(),
                    MatchRuntime::new(crate::checkpoint_tests::CounterSimulation::default(), 400),
                )
                .unwrap();
            let host = LiveMatchHost::new(matches);
            host.claim().unwrap();
            host.start().await;
            let (lobby, guest, _signaling) = super::setup::test_support::lobbies().await;
            let mut adapter = WebRtcHost::attach(
                lobby,
                host.clone(),
                id.clone(),
                Arc::new(ParticipantEcho),
                WebRtcHostConfig {
                    bind_ip: "127.0.0.1".to_owned(),
                    relay_only: false,
                },
            )
            .unwrap();
            if host_content {
                adapter = adapter.with_content_service(Arc::new(ContentProbe {
                    blocked: tokio::sync::Notify::new(),
                    canceled: std::sync::atomic::AtomicBool::new(false),
                }));
            }
            let (stop, shutdown) = oneshot::channel();
            let mut tasks = tokio::task::JoinSet::new();
            tasks.spawn(adapter.run(|_| true, async {
                let _ = shutdown.await;
            }));
            let mut client = if host_content {
                WebRtcClient::connect(guest, id, "127.0.0.1", false)
                    .await
                    .unwrap()
            } else {
                WebRtcClient::connect_with_content_authorization(
                    guest,
                    id,
                    "127.0.0.1",
                    false,
                    Vec::new(),
                )
                .await
                .unwrap()
            };
            assert!(client.content_client().is_none());
            assert!(client.snapshot().await.unwrap().tick > 0);
            assert_eq!(
                client.command(1, &[3]).await.unwrap().outcome,
                ReliableCommandOutcome::Applied
            );
            client.close().await.unwrap();
            stop.send(()).unwrap();
            tasks.join_next().await.unwrap().unwrap().unwrap();
            host.stop().await;
        }
    })
    .await
    .expect("bounded optional content compatibility probe");
}
