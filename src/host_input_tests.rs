use crate::protocol::MAX_COMMAND_PAYLOAD_BYTES;
use crate::replay::{ReplayLog, ReplayRecord};
use crate::*;

#[derive(Default)]
struct AdmissionSimulation {
    tick: u64,
    state: Vec<u8>,
}
impl GameSimulation for AdmissionSimulation {
    fn tick_hz(&self) -> u16 {
        20
    }
    fn max_players(&self) -> usize {
        2
    }
    fn current_tick(&self) -> u64 {
        self.tick
    }
    fn add_player(&mut self, _: PlayerId) -> Result<(), SimulationError> {
        Ok(())
    }
    fn remove_player(&mut self, _: PlayerId) -> bool {
        true
    }
    fn apply_command(
        &mut self,
        _: PlayerId,
        _: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        self.state.extend_from_slice(payload);
        Ok(())
    }
    fn apply_host_input(&mut self, _: PlayerId, payload: &[u8]) -> Result<(), SimulationError> {
        if payload == b"reject" {
            return Err(SimulationError::new("rejected admission"));
        }
        self.state.extend_from_slice(payload);
        Ok(())
    }
    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        self.tick += 1;
        Ok(())
    }
    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        Ok(SimulationSnapshot::new(self.tick, self.state.clone()))
    }
}
fn runtime() -> MatchRuntime<AdmissionSimulation> {
    MatchRuntime::new_with_replay_capture(AdmissionSimulation::default(), 5)
}

#[test]
fn host_input_replays_and_recovers_in_command_order_without_consuming_sequences() {
    let mut runtime = runtime();
    let lease = runtime.admit(ReconnectToken([1; 16])).unwrap();
    runtime
        .submit_command(lease.player_id, lease.connection_epoch, 1, b"before")
        .unwrap();
    runtime
        .submit_host_input(lease.player_id, b"character")
        .unwrap();
    runtime
        .submit_command(lease.player_id, lease.connection_epoch, 2, b"after")
        .unwrap();
    let snapshot = runtime.advance_tick().unwrap();
    assert_eq!(snapshot.payload, b"beforecharacterafter");
    runtime.freeze_for_recovery();
    let image = runtime.recovery_image().unwrap();
    let encoded = image.encode().unwrap();
    let image = RecoveryImage::decode(&encoded).unwrap();
    let restored =
        MatchRuntime::restore_from_recovery(AdmissionSimulation::default(), image).unwrap();
    assert_eq!(restored.snapshot().unwrap(), snapshot);
}

#[test]
fn rejected_oversized_and_retired_host_inputs_do_not_change_replay() {
    let mut runtime = runtime();
    let lease = runtime.admit(ReconnectToken([2; 16])).unwrap();
    let before = runtime.replay_log().unwrap().clone();
    assert!(
        runtime
            .submit_host_input(lease.player_id, b"reject")
            .is_err()
    );
    assert!(
        runtime
            .submit_host_input(lease.player_id, &vec![0; MAX_COMMAND_PAYLOAD_BYTES + 1])
            .is_err()
    );
    assert!(runtime.submit_host_input(999, b"missing").is_err());
    assert_eq!(runtime.replay_log().unwrap(), &before);
    runtime.begin_drain();
    assert_eq!(
        runtime.submit_host_input(lease.player_id, b"late"),
        Err(RuntimeError::Draining)
    );
    runtime.freeze_for_recovery();
    assert_eq!(
        runtime.submit_host_input(lease.player_id, b"late"),
        Err(RuntimeError::Frozen)
    );
}

#[test]
fn host_input_codec_is_versioned_bounded_and_private() {
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::HostInputApplied {
        tick: 0,
        player_id: 1,
        payload: b"private-character-data".to_vec(),
    });
    let bytes = log.encode().unwrap();
    assert_eq!(bytes[4], 4);
    assert_eq!(ReplayLog::decode(&bytes).unwrap(), log);
    assert!(!format!("{log:?}").contains("private-character-data"));
    let mut legacy = bytes.clone();
    legacy[4] = 3;
    assert!(ReplayLog::decode(&legacy).is_err());
    assert!(ReplayLog::decode(&bytes[..bytes.len() - 1]).is_err());
    let mut inactive = ReplayLog::default();
    inactive.append(ReplayRecord::HostInputApplied {
        tick: 0,
        player_id: 1,
        payload: vec![],
    });
    assert!(verify_replay(AdmissionSimulation::default(), &inactive).is_err());
    let mut oversized = ReplayLog::default();
    oversized.append(ReplayRecord::HostInputApplied {
        tick: 0,
        player_id: 1,
        payload: vec![0; MAX_COMMAND_PAYLOAD_BYTES + 1],
    });
    assert!(oversized.encode().is_err());
}

#[tokio::test]
async fn live_host_input_checks_epoch_freeze_drain_and_retirement() {
    let mut runtime = runtime();
    let lease = runtime.admit(ReconnectToken([3; 16])).unwrap();
    let id = MatchId::new("admission").unwrap();
    let mut matches = MatchHost::new(1).unwrap();
    matches.insert(id.clone(), runtime).unwrap();
    let host = LiveMatchHost::new(matches);
    assert!(
        host.submit_host_input(&id, lease.player_id, lease.connection_epoch, b"offline")
            .await
            .is_err()
    );
    host.start().await;
    assert!(
        host.submit_host_input(&id, lease.player_id, lease.connection_epoch + 1, b"stale")
            .await
            .is_err()
    );
    host.submit_host_input(&id, lease.player_id, lease.connection_epoch, b"character")
        .await
        .unwrap();
    assert_eq!(
        host.inspect(&id, |runtime| runtime.snapshot().unwrap().payload)
            .await
            .unwrap(),
        b"character"
    );
    let hosted = host.get(&id).unwrap();
    hosted.runtime.lock().await.freeze_for_recovery();
    assert_eq!(
        host.submit_host_input(&id, lease.player_id, lease.connection_epoch, b"frozen")
            .await,
        Err(LiveHostError::Runtime(RuntimeError::Frozen))
    );
    hosted.runtime.lock().await.resume_after_failed_recovery();
    host.retire(&id).await.unwrap();
    assert_eq!(
        host.submit_host_input(&id, lease.player_id, lease.connection_epoch, b"retired")
            .await,
        Err(LiveHostError::UnknownMatch)
    );
    host.begin_drain().await;
    assert_eq!(
        host.submit_host_input(&id, lease.player_id, lease.connection_epoch, b"drained")
            .await,
        Err(LiveHostError::Draining)
    );
    host.stop().await;
}

#[test]
fn host_input_default_rejects_and_removed_player_cannot_receive_data() {
    let mut default = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 5);
    let lease = default.admit(ReconnectToken([4; 16])).unwrap();
    let before = default.snapshot().unwrap();
    assert!(
        default
            .submit_host_input(lease.player_id, b"unsupported")
            .is_err()
    );
    assert_eq!(default.snapshot().unwrap(), before);
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::PlayerRemoved {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::HostInputApplied {
        tick: 0,
        player_id: 1,
        payload: b"late".to_vec(),
    });
    assert!(verify_replay(AdmissionSimulation::default(), &log).is_err());
}
