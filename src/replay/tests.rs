use super::codec::{
    DIGEST_CHECKPOINT_KIND, DIGEST_REPLAY_FORMAT_VERSION, LANE_COMMAND_KIND,
    LANE_REPLAY_FORMAT_VERSION, LEGACY_REPLAY_FORMAT_VERSION, PLAYER_BODY_BYTES,
    RECORD_HEADER_BYTES, RELIABLE_LANE_BYTE, REPLAY_HEADER_BYTES,
};
use super::verify::MAX_VERIFIER_TICK_GAP;
use super::*;
use crate::protocol::CommandLane;
use crate::simulation::{GameSimulation, SimulationSnapshot};
use crate::world::{DemoSimulation, encode_demo_command};

#[test]
fn digest_checkpoints_round_trip_and_verify_each_tick() {
    let mut source = DemoSimulation::new();
    let mut log = ReplayLog::default();
    for _ in 0..100 {
        source.advance_tick().unwrap();
        let snapshot = source.snapshot().unwrap();
        log.append(ReplayRecord::DigestCheckpoint {
            tick: snapshot.tick,
            digest: canonical_digest(&snapshot),
        });
    }
    let bytes = log.encode().unwrap();
    assert_eq!(bytes.len(), REPLAY_HEADER_BYTES + 100 * 45);
    assert_eq!(bytes[4], DIGEST_REPLAY_FORMAT_VERSION);
    let decoded = ReplayLog::decode(&bytes).unwrap();
    assert_eq!(decoded, log);
    let result = verify_replay(DemoSimulation::new(), &decoded).unwrap();
    assert_eq!(result.checkpoints_verified, 100);
    assert_eq!(result.final_snapshot, source.snapshot().unwrap());
    let mut tampered = bytes.clone();
    tampered[REPLAY_HEADER_BYTES + RECORD_HEADER_BYTES] ^= 1;
    assert_eq!(
        verify_replay(
            DemoSimulation::new(),
            &ReplayLog::decode(&tampered).unwrap()
        ),
        Err(ReplayError::CheckpointDigestMismatch(1))
    );
    let mut legacy = bytes;
    legacy[4] = LEGACY_REPLAY_FORMAT_VERSION;
    assert_eq!(
        ReplayLog::decode(&legacy),
        Err(ReplayError::UnknownRecordKind(DIGEST_CHECKPOINT_KIND))
    );
}

#[test]
fn lane_command_records_are_canonical_and_version_three_only() {
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 1,
        lane: CommandLane::Reliable,
        sequence: 4,
        payload: vec![0xee],
    });
    let encoded = log.encode().unwrap();
    assert_eq!(encoded[4], LANE_REPLAY_FORMAT_VERSION);
    let command = REPLAY_HEADER_BYTES + RECORD_HEADER_BYTES + PLAYER_BODY_BYTES;
    assert_eq!(encoded[command], LANE_COMMAND_KIND);
    let lane_byte = command + RECORD_HEADER_BYTES + 4;
    assert_eq!(encoded[lane_byte], RELIABLE_LANE_BYTE);
    assert_eq!(ReplayLog::decode(&encoded).unwrap(), log);

    // Datagram commands always use kind 2, so a datagram lane byte in a lane
    // record is not canonical; unknown lanes fail closed as well.
    for lane in [0, 2, u8::MAX] {
        let mut tampered = encoded.clone();
        tampered[lane_byte] = lane;
        assert_eq!(
            ReplayLog::decode(&tampered),
            Err(ReplayError::UnknownCommandLane(lane))
        );
    }
}

#[test]
fn replay_log_binary_round_trip_is_exact() {
    let payload = vec![1, 2, 3];
    let snapshot = SimulationSnapshot::new(7, payload.clone());
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 1,
        lane: CommandLane::Datagram,
        sequence: 1,
        payload,
    });
    log.append(ReplayRecord::Checkpoint { snapshot });

    let encoded = log.encode().unwrap();
    assert_eq!(ReplayLog::decode(&encoded).unwrap(), log);
}

#[test]
fn decoder_rejects_tampered_checkpoint_hash() {
    let mut log = ReplayLog::default();
    let snapshot = SimulationSnapshot::new(1, vec![0]);
    log.append(ReplayRecord::Checkpoint { snapshot });
    let mut encoded = log.encode().unwrap();
    let hash_offset = REPLAY_HEADER_BYTES + RECORD_HEADER_BYTES;
    encoded[hash_offset] ^= 0xff;
    assert!(matches!(
        ReplayLog::decode(&encoded),
        Err(ReplayError::InvalidCheckpointHash { .. })
    ));
}

#[test]
fn verifier_reconstructs_demo_simulation() {
    let command = encode_demo_command(1, -1).unwrap().to_vec();
    let mut source = DemoSimulation::new();
    source.add_player(1).unwrap();
    source.apply_command(1, 1, &command).unwrap();
    source.advance_tick().unwrap();
    let first = source.snapshot().unwrap();
    source.apply_command(1, 2, &command).unwrap();
    source.advance_tick().unwrap();
    let second = source.snapshot().unwrap();

    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    log.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 1,
        lane: CommandLane::Datagram,
        sequence: 1,
        payload: command.clone(),
    });
    log.append(ReplayRecord::Checkpoint { snapshot: first });
    log.append(ReplayRecord::CommandApplied {
        tick: 1,
        player_id: 1,
        lane: CommandLane::Datagram,
        sequence: 2,
        payload: command,
    });
    log.append(ReplayRecord::Checkpoint {
        snapshot: second.clone(),
    });

    let verification = verify_replay(DemoSimulation::new(), &log).unwrap();
    assert_eq!(verification.records_verified, 5);
    assert_eq!(verification.checkpoints_verified, 2);
    assert_eq!(verification.final_snapshot, second);
}

#[test]
fn verifier_rejects_implausible_tick_gap_before_advancing() {
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: u64::MAX,
        player_id: 1,
    });

    assert_eq!(
        verify_replay(DemoSimulation::new(), &log),
        Err(ReplayError::TickGapTooLarge {
            from_tick: 0,
            to_tick: u64::MAX,
            maximum: MAX_VERIFIER_TICK_GAP,
        })
    );
}
#[test]
fn verification_and_recovery_reject_impossible_command_sequences() {
    use crate::{MatchRuntime, ReconnectToken};
    let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
    let lease = runtime.admit(ReconnectToken([1; 16])).unwrap();
    let command = encode_demo_command(1, 0).unwrap();
    runtime
        .submit_command(lease.player_id, lease.connection_epoch, 2, &command)
        .unwrap();
    runtime
        .submit_command(lease.player_id, lease.connection_epoch, 3, &command)
        .unwrap();
    runtime.advance_tick().unwrap();
    runtime.freeze_for_recovery();
    let valid = runtime.recovery_image().unwrap();
    assert!(verify_replay(DemoSimulation::new(), &valid.replay).is_ok());

    for sequence in [1, 2] {
        let mut image = valid.clone();
        // Same payload and tick: even the final snapshot hash remains valid.
        // Only the runtime's sequence invariant distinguishes this from history.
        image.replay.records.insert(
            2,
            ReplayRecord::CommandApplied {
                tick: 0,
                player_id: lease.player_id,
                lane: CommandLane::Datagram,
                sequence,
                payload: command.to_vec(),
            },
        );
        let encoded = image.encode().unwrap();
        let decoded = crate::RecoveryImage::decode(&encoded).unwrap();
        let expected = ReplayError::NonIncreasingSequence {
            player_id: lease.player_id,
            lane: CommandLane::Datagram,
            previous: 2,
            actual: sequence,
        };
        assert_eq!(
            verify_replay(DemoSimulation::new(), &decoded.replay).unwrap_err(),
            expected
        );
        assert_eq!(
            MatchRuntime::restore_from_recovery(DemoSimulation::new(), decoded).unwrap_err(),
            crate::RuntimeRecoveryError::Recovery(crate::RecoveryError::Replay(expected))
        );
    }
}

#[test]
fn verification_and_recovery_reject_reused_player_identity() {
    use crate::{MatchRuntime, ReconnectToken};
    let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
    let lease = runtime.admit(ReconnectToken([1; 16])).unwrap();
    runtime.freeze_for_recovery();
    let mut image = runtime.recovery_image().unwrap();
    image.replay.records.insert(
        1,
        ReplayRecord::PlayerRemoved {
            tick: 0,
            player_id: lease.player_id,
        },
    );
    image.replay.records.insert(
        2,
        ReplayRecord::PlayerAdmitted {
            tick: 0,
            player_id: lease.player_id,
        },
    );
    assert!(
        verify_replay(DemoSimulation::new(), &image.replay).is_err(),
        "verifier accepted reuse of a retired player ID"
    );
    assert!(
        MatchRuntime::restore_from_recovery(DemoSimulation::new(), image).is_err(),
        "recovery accepted reuse of a retired player ID"
    );
}
#[test]
fn replay_encoding_preserves_the_version_one_wire_fixture() {
    let mut log = ReplayLog::default();
    log.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 7,
    });
    log.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 7,
        lane: CommandLane::Datagram,
        sequence: 9,
        payload: vec![0xaa],
    });
    log.append(ReplayRecord::Checkpoint {
        snapshot: SimulationSnapshot::new(1, vec![0xbb]),
    });
    log.append(ReplayRecord::PlayerRemoved {
        tick: 1,
        player_id: 7,
    });
    const FIXTURE_HEX: &str = "475352500101000000000000000000000004000000070200000000000000000000000d000000070000000900000001aa0400000000000000010000000d35594afc4eab92da00000001bb0300000000000000010000000400000007";
    let expected: Vec<u8> = FIXTURE_HEX
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    assert_eq!(log.encode().unwrap(), expected);
    assert_eq!(ReplayLog::decode(&expected).unwrap(), log);
}
#[test]
fn replay_enforces_identity_before_delegating_to_simulation() {
    let mut zero = ReplayLog::default();
    zero.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 0,
    });
    assert_eq!(
        verify_replay(DemoSimulation::new(), &zero).unwrap_err(),
        ReplayError::NonIncreasingPlayerId {
            previous: 0,
            actual: 0
        }
    );

    let mut retired = ReplayLog::default();
    retired.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    retired.append(ReplayRecord::PlayerRemoved {
        tick: 0,
        player_id: 1,
    });
    retired.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 1,
        lane: CommandLane::Datagram,
        sequence: 1,
        payload: encode_demo_command(1, 0).unwrap().to_vec(),
    });
    assert_eq!(
        verify_replay(DemoSimulation::new(), &retired).unwrap_err(),
        ReplayError::UnknownCommandPlayer(1)
    );
}

#[test]
fn replay_allows_gaps_in_allocated_ids_and_command_sequences() {
    let mut log = ReplayLog::default();
    for player_id in [3, 8] {
        log.append(ReplayRecord::PlayerAdmitted { tick: 0, player_id });
        for sequence in [5, 9] {
            log.append(ReplayRecord::CommandApplied {
                tick: 0,
                player_id,
                lane: CommandLane::Datagram,
                sequence,
                payload: encode_demo_command(1, 0).unwrap().to_vec(),
            });
        }
    }
    assert_eq!(
        verify_replay(DemoSimulation::new(), &log)
            .unwrap()
            .records_verified,
        6
    );
}
