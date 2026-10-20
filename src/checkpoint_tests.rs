use crate::{
    CommandLane, CommandOutcome, GameSimulation, MatchRuntime, ReconnectToken, RecoveryImage,
    ReplayError, ReplayLog, ReplayRecord, RuntimeError, SimulationError, SimulationSnapshot,
    verify_replay,
};
use std::collections::BTreeMap;

#[derive(Default)]
struct CounterSimulation {
    tick: u64,
    players: BTreeMap<u32, u64>,
}
impl GameSimulation for CounterSimulation {
    fn tick_hz(&self) -> u16 {
        20
    }
    fn max_players(&self) -> usize {
        8
    }
    fn current_tick(&self) -> u64 {
        self.tick
    }
    fn add_player(&mut self, id: u32) -> Result<(), SimulationError> {
        if self.players.insert(id, 0).is_some() {
            return Err(SimulationError::new("duplicate player"));
        }
        Ok(())
    }
    fn remove_player(&mut self, id: u32) -> bool {
        self.players.remove(&id).is_some()
    }
    fn apply_command(&mut self, id: u32, _: u32, payload: &[u8]) -> Result<(), SimulationError> {
        let [delta] = payload else {
            return Err(SimulationError::new("invalid counter input"));
        };
        *self
            .players
            .get_mut(&id)
            .ok_or_else(|| SimulationError::new("unknown counter player"))? += u64::from(*delta);
        Ok(())
    }
    fn apply_host_input(&mut self, id: u32, payload: &[u8]) -> Result<(), SimulationError> {
        self.apply_command(id, 0, payload)
    }
    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        self.tick += 1;
        for value in self.players.values_mut() {
            *value += 1;
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        let mut payload = self.tick.to_be_bytes().to_vec();
        for (id, value) in &self.players {
            payload.extend_from_slice(&id.to_be_bytes());
            payload.extend_from_slice(&value.to_be_bytes());
        }
        Ok(SimulationSnapshot::new(self.tick, payload))
    }
    fn restore_checkpoint(&mut self, snapshot: &SimulationSnapshot) -> Result<(), SimulationError> {
        let bytes = &snapshot.payload;
        if bytes.len() < 8 || !(bytes.len() - 8).is_multiple_of(12) {
            return Err(SimulationError::new("invalid counter checkpoint"));
        }
        let tick = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let mut players = BTreeMap::new();
        for entry in bytes[8..].as_chunks::<12>().0 {
            let id = u32::from_be_bytes(entry[..4].try_into().unwrap());
            let value = u64::from_be_bytes(entry[4..].try_into().unwrap());
            if id == 0 || players.insert(id, value).is_some() {
                return Err(SimulationError::new("invalid counter identity"));
            }
        }
        *self = Self { tick, players };
        Ok(())
    }
}

#[test]
fn checkpoint_restores_only_its_suffix_with_sessions_lanes_and_trusted_inputs() {
    let mut runtime = MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 30);
    let retired = runtime.admit(ReconnectToken([1; 16])).unwrap();
    runtime.disconnect(retired.player_id, retired.connection_epoch);
    for _ in 0..32 {
        runtime.advance_tick().unwrap();
    }
    let lease = runtime.admit(ReconnectToken([2; 16])).unwrap();
    assert_eq!(lease.player_id, 2);
    runtime
        .submit_lane_command(
            lease.player_id,
            lease.connection_epoch,
            CommandLane::Datagram,
            7,
            &[3],
        )
        .unwrap();
    runtime
        .submit_lane_command(
            lease.player_id,
            lease.connection_epoch,
            CommandLane::Reliable,
            11,
            &[4],
        )
        .unwrap();
    runtime.submit_host_input(lease.player_id, &[5]).unwrap();
    runtime.advance_tick().unwrap();
    assert!(runtime.replay_log().unwrap().records().len() > 30);
    let image = runtime.checkpoint_replay().unwrap();
    assert_eq!(runtime.replay_log().unwrap().records().len(), 1);
    assert_eq!(image.replay.encode().unwrap()[4], 5);
    let image = RecoveryImage::decode(&image.encode().unwrap()).unwrap();
    let mut duplicate = image.replay.clone();
    duplicate.append(ReplayRecord::CommandApplied {
        tick: image.current_tick,
        player_id: 2,
        lane: CommandLane::Reliable,
        sequence: 11,
        payload: vec![99],
    });
    assert!(matches!(
        verify_replay(CounterSimulation::default(), &duplicate),
        Err(ReplayError::NonIncreasingSequence {
            previous: 11,
            actual: 11,
            ..
        })
    ));

    assert_eq!(image.last_lane_sequences()[&(2, CommandLane::Datagram)], 7);
    assert_eq!(image.last_lane_sequences()[&(2, CommandLane::Reliable)], 11);
    let mut restored =
        MatchRuntime::restore_from_recovery(CounterSimulation::default(), image).unwrap();
    assert_eq!(restored.snapshot().unwrap(), runtime.snapshot().unwrap());
    let resumed = restored
        .reconnect(ReconnectToken([2; 16]), ReconnectToken([3; 16]))
        .unwrap();
    assert_eq!(resumed.player_id, 2);
    assert_eq!(resumed.connection_epoch, lease.connection_epoch + 1);
    assert!(matches!(
        restored.submit_command(2, lease.connection_epoch, 8, &[99]),
        Err(RuntimeError::StaleConnection)
    ));
    for (lane, sequence) in [(CommandLane::Datagram, 7), (CommandLane::Reliable, 11)] {
        assert_eq!(
            restored
                .submit_lane_command(2, resumed.connection_epoch, lane, sequence, &[99])
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
    }
    for (host, epoch) in [
        (&mut runtime, lease.connection_epoch),
        (&mut restored, resumed.connection_epoch),
    ] {
        host.submit_lane_command(2, epoch, CommandLane::Reliable, 12, &[6])
            .unwrap();
        host.submit_host_input(2, &[7]).unwrap();
        for _ in 0..3 {
            host.advance_tick().unwrap();
        }
    }
    assert_eq!(restored.snapshot().unwrap(), runtime.snapshot().unwrap());
    let replay = ReplayLog::decode(&runtime.replay_log().unwrap().encode().unwrap()).unwrap();
    assert!(matches!(replay.records()[0], ReplayRecord::Baseline { .. }));
    assert_eq!(
        verify_replay(CounterSimulation::default(), &replay)
            .unwrap()
            .final_snapshot,
        runtime.snapshot().unwrap()
    );
    runtime.freeze_for_recovery();
    let recovery = runtime.recovery_image().unwrap();
    let mut again =
        MatchRuntime::restore_from_recovery(CounterSimulation::default(), recovery).unwrap();
    assert_eq!(again.snapshot().unwrap(), restored.snapshot().unwrap());
    assert_eq!(again.admit(ReconnectToken([4; 16])).unwrap().player_id, 3);
}

#[test]
fn baseline_keeps_retired_identity_ceiling_even_with_no_live_players() {
    let mut runtime = MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 0);
    let lease = runtime.admit(ReconnectToken([1; 16])).unwrap();
    runtime.disconnect(lease.player_id, lease.connection_epoch);
    runtime.advance_tick().unwrap();
    let image = runtime.checkpoint_replay().unwrap();
    let mut restored =
        MatchRuntime::restore_from_recovery(CounterSimulation::default(), image.clone()).unwrap();
    assert_eq!(
        restored.admit(ReconnectToken([2; 16])).unwrap().player_id,
        2
    );
    let mut reused = image.replay;
    reused.append(ReplayRecord::PlayerAdmitted {
        tick: 1,
        player_id: 1,
    });
    assert!(matches!(
        verify_replay(CounterSimulation::default(), &reused),
        Err(ReplayError::NonIncreasingPlayerId { .. })
    ));
}

#[test]
fn malformed_or_repeated_baselines_and_mismatched_restore_fail_closed() {
    let mut runtime = MatchRuntime::new_with_replay_capture(CounterSimulation::default(), 10);
    runtime.admit(ReconnectToken([1; 16])).unwrap();
    let image = runtime.checkpoint_replay().unwrap();
    let ReplayRecord::Baseline { baseline } = &image.replay.records()[0] else {
        panic!("missing baseline");
    };
    let mut malformed = baseline.clone();
    malformed.players.push(malformed.players[0].clone());
    let mut bad = ReplayLog::default();
    bad.append(ReplayRecord::Baseline {
        baseline: malformed,
    });
    assert!(matches!(bad.encode(), Err(ReplayError::InvalidBaseline(_))));
    let mut stale = image.replay.clone();
    stale.append(ReplayRecord::CommandApplied {
        tick: baseline.snapshot.tick,
        player_id: 1,
        lane: CommandLane::Datagram,
        sequence: 0,
        payload: vec![1],
    });
    assert!(matches!(
        verify_replay(CounterSimulation::default(), &stale),
        Err(ReplayError::InvalidSequence)
    ));
    let mut corrupt = image.replay.encode().unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(matches!(
        ReplayLog::decode(&corrupt),
        Err(ReplayError::InvalidCheckpointHash { .. })
    ));
    assert!(ReplayLog::decode(&corrupt[..corrupt.len() - 1]).is_err());
    let mut repeated = image.replay.clone();
    repeated.append(image.replay.records()[0].clone());
    assert!(matches!(
        repeated.encode(),
        Err(ReplayError::InvalidBaseline(_))
    ));
    assert!(matches!(
        verify_replay(CounterSimulation::default(), &repeated),
        Err(ReplayError::InvalidBaseline(_))
    ));
    let mut repeated_bytes = image.replay.encode().unwrap();
    repeated_bytes.extend_from_slice(&image.replay.encode().unwrap()[5..]);
    assert!(matches!(
        ReplayLog::decode(&repeated_bytes),
        Err(ReplayError::InvalidBaseline(_))
    ));
    let mut legacy = image.replay.encode().unwrap();
    legacy[4] = 4;
    assert!(matches!(
        ReplayLog::decode(&legacy),
        Err(ReplayError::UnknownRecordKind(_))
    ));
    let mut wrong_state = baseline.clone();
    wrong_state.snapshot = SimulationSnapshot::new(7, wrong_state.snapshot.payload);
    let mut wrong = ReplayLog::default();
    wrong.append(ReplayRecord::Baseline {
        baseline: wrong_state,
    });
    assert!(matches!(
        verify_replay(CounterSimulation::default(), &wrong),
        Err(ReplayError::InvalidBaseline(_))
    ));
}

#[test]
fn default_simulations_reject_checkpoint_restore_without_affecting_legacy_replay() {
    use crate::world::DemoSimulation;
    let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
    runtime.advance_tick().unwrap();
    let image = runtime.checkpoint_replay().unwrap();
    assert!(matches!(
        verify_replay(DemoSimulation::new(), &image.replay),
        Err(ReplayError::Simulation(_))
    ));
    let mut legacy = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
    legacy.advance_tick().unwrap();
    assert!(verify_replay(DemoSimulation::new(), legacy.replay_log().unwrap()).is_ok());
    assert_ne!(legacy.replay_log().unwrap().encode().unwrap()[4], 5);
}
