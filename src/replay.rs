use crate::protocol::{
    MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, PlayerId, snapshot_hash,
};
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot};
use std::collections::BTreeMap;
use std::fmt;

pub const REPLAY_FORMAT_VERSION: u8 = 1;
const REPLAY_MAGIC: &[u8; 4] = b"GSRP";
const REPLAY_HEADER_BYTES: usize = REPLAY_MAGIC.len() + 1;
const RECORD_HEADER_BYTES: usize = 1 + 8 + 4;
const ADMISSION_KIND: u8 = 1;
const COMMAND_KIND: u8 = 2;
const REMOVAL_KIND: u8 = 3;
const CHECKPOINT_KIND: u8 = 4;
const PLAYER_BODY_BYTES: usize = 4;
const COMMAND_FIXED_BODY_BYTES: usize = 4 + 4 + 4;
const CHECKPOINT_FIXED_BODY_BYTES: usize = 8 + 4;
const MAX_REPLAY_BODY_BYTES: usize = CHECKPOINT_FIXED_BODY_BYTES + MAX_SNAPSHOT_PAYLOAD_BYTES;
const MAX_VERIFIER_TICK_GAP: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayRecord {
    PlayerAdmitted {
        tick: u64,
        player_id: PlayerId,
    },
    CommandApplied {
        tick: u64,
        player_id: PlayerId,
        sequence: u32,
        payload: Vec<u8>,
    },
    PlayerRemoved {
        tick: u64,
        player_id: PlayerId,
    },
    Checkpoint {
        snapshot: SimulationSnapshot,
    },
}

impl ReplayRecord {
    pub fn tick(&self) -> u64 {
        match self {
            Self::PlayerAdmitted { tick, .. }
            | Self::CommandApplied { tick, .. }
            | Self::PlayerRemoved { tick, .. } => *tick,
            Self::Checkpoint { snapshot } => snapshot.tick,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplayLog {
    records: Vec<ReplayRecord>,
}

impl ReplayLog {
    pub fn records(&self) -> &[ReplayRecord] {
        &self.records
    }

    pub fn encode(&self) -> Result<Vec<u8>, ReplayError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(REPLAY_MAGIC);
        bytes.push(REPLAY_FORMAT_VERSION);
        for record in &self.records {
            encode_record(record, &mut bytes)?;
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplayError> {
        if bytes.len() < REPLAY_HEADER_BYTES {
            return Err(ReplayError::Truncated);
        }
        if &bytes[..REPLAY_MAGIC.len()] != REPLAY_MAGIC {
            return Err(ReplayError::InvalidMagic);
        }
        let version = bytes[REPLAY_MAGIC.len()];
        if version != REPLAY_FORMAT_VERSION {
            return Err(ReplayError::UnsupportedVersion(version));
        }

        let mut offset = REPLAY_HEADER_BYTES;
        let mut records = Vec::new();
        let mut previous_tick = None;
        while offset < bytes.len() {
            let remaining = bytes.len() - offset;
            if remaining < RECORD_HEADER_BYTES {
                return Err(ReplayError::Truncated);
            }
            let kind = bytes[offset];
            let tick = u64::from_be_bytes(
                bytes[offset + 1..offset + 9]
                    .try_into()
                    .expect("checked replay record header"),
            );
            let body_len = usize::try_from(u32::from_be_bytes(
                bytes[offset + 9..offset + 13]
                    .try_into()
                    .expect("checked replay record header"),
            ))
            .expect("u32 fits usize on supported targets");
            if body_len > MAX_REPLAY_BODY_BYTES {
                return Err(ReplayError::RecordTooLarge(body_len));
            }
            offset += RECORD_HEADER_BYTES;
            let body_end = offset.checked_add(body_len).ok_or(ReplayError::Truncated)?;
            if body_end > bytes.len() {
                return Err(ReplayError::Truncated);
            }
            if previous_tick.is_some_and(|previous| tick < previous) {
                return Err(ReplayError::NonMonotonicTick {
                    previous: previous_tick.expect("checked previous tick"),
                    actual: tick,
                });
            }
            records.push(decode_record(kind, tick, &bytes[offset..body_end])?);
            previous_tick = Some(tick);
            offset = body_end;
        }

        Ok(Self { records })
    }

    pub(crate) fn append(&mut self, record: ReplayRecord) {
        debug_assert!(
            self.records
                .last()
                .is_none_or(|previous| record.tick() >= previous.tick()),
            "replay records must be append-only in authoritative tick order"
        );
        self.records.push(record);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayVerification {
    pub records_verified: usize,
    pub checkpoints_verified: usize,
    pub final_snapshot: SimulationSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownRecordKind(u8),
    Truncated,
    RecordTooLarge(usize),
    InvalidRecordLength {
        kind: u8,
        expected: usize,
        actual: usize,
    },
    InvalidSequence,
    NonIncreasingPlayerId {
        previous: PlayerId,
        actual: PlayerId,
    },
    UnknownCommandPlayer(PlayerId),
    NonIncreasingSequence {
        player_id: PlayerId,
        previous: u32,
        actual: u32,
    },
    PayloadTooLarge {
        maximum: usize,
        actual: usize,
    },
    InvalidCheckpointHash {
        tick: u64,
        expected: u64,
        actual: u64,
    },
    NonMonotonicTick {
        previous: u64,
        actual: u64,
    },
    TickGapTooLarge {
        from_tick: u64,
        to_tick: u64,
        maximum: u64,
    },
    ReplayStartsBeforeSimulation {
        simulation_tick: u64,
        record_tick: u64,
    },
    MissingPlayerOnRemoval(PlayerId),
    CheckpointMismatch {
        tick: u64,
        expected_hash: u64,
        actual_hash: u64,
    },
    CheckpointPayloadMismatch(u64),
    Simulation(SimulationError),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid replay log magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported replay format version {version}")
            }
            Self::UnknownRecordKind(kind) => write!(formatter, "unknown replay record kind {kind}"),
            Self::Truncated => write!(formatter, "truncated replay log"),
            Self::RecordTooLarge(size) => {
                write!(formatter, "replay record body is too large: {size}")
            }
            Self::InvalidRecordLength {
                kind,
                expected,
                actual,
            } => write!(
                formatter,
                "replay record kind {kind} expected {expected} body bytes, received {actual}"
            ),
            Self::InvalidSequence => write!(formatter, "replay command sequence must be non-zero"),
            Self::NonIncreasingPlayerId { previous, actual } => write!(
                formatter,
                "replay player ID {actual} must be greater than previously admitted ID {previous}"
            ),
            Self::UnknownCommandPlayer(player_id) => write!(
                formatter,
                "replay command addressed inactive player {player_id}"
            ),
            Self::NonIncreasingSequence {
                player_id,
                previous,
                actual,
            } => write!(
                formatter,
                "replay command sequence {actual} for player {player_id} must exceed {previous}"
            ),
            Self::PayloadTooLarge { maximum, actual } => write!(
                formatter,
                "replay payload size {actual} exceeds maximum {maximum}"
            ),
            Self::InvalidCheckpointHash {
                tick,
                expected,
                actual,
            } => write!(
                formatter,
                "replay checkpoint {tick} hash mismatch: expected {expected:#018x}, got {actual:#018x}"
            ),
            Self::NonMonotonicTick { previous, actual } => write!(
                formatter,
                "replay tick moved backward from {previous} to {actual}"
            ),
            Self::TickGapTooLarge {
                from_tick,
                to_tick,
                maximum,
            } => write!(
                formatter,
                "replay tick gap from {from_tick} to {to_tick} exceeds maximum {maximum}"
            ),
            Self::ReplayStartsBeforeSimulation {
                simulation_tick,
                record_tick,
            } => write!(
                formatter,
                "replay record tick {record_tick} precedes simulation tick {simulation_tick}"
            ),
            Self::MissingPlayerOnRemoval(player_id) => {
                write!(
                    formatter,
                    "replay attempted to remove missing player {player_id}"
                )
            }
            Self::CheckpointMismatch {
                tick,
                expected_hash,
                actual_hash,
            } => write!(
                formatter,
                "replay diverged at checkpoint {tick}: expected {expected_hash:#018x}, got {actual_hash:#018x}"
            ),
            Self::CheckpointPayloadMismatch(tick) => {
                write!(formatter, "replay payload diverged at checkpoint {tick}")
            }
            Self::Simulation(error) => write!(formatter, "replay simulation failed: {error}"),
        }
    }
}

impl std::error::Error for ReplayError {}

impl From<SimulationError> for ReplayError {
    fn from(error: SimulationError) -> Self {
        Self::Simulation(error)
    }
}

pub fn verify_replay<S: GameSimulation>(
    simulation: S,
    log: &ReplayLog,
) -> Result<ReplayVerification, ReplayError> {
    let (simulation, checkpoints_verified) = replay_into(simulation, log)?;
    Ok(ReplayVerification {
        records_verified: log.records().len(),
        checkpoints_verified,
        final_snapshot: simulation.snapshot()?,
    })
}

// Verification and recovery must interpret the same authoritative history.
// Runtime-owned identity and sequence rules cannot be delegated to game logic.
pub(crate) fn replay_into<S: GameSimulation>(
    mut simulation: S,
    log: &ReplayLog,
) -> Result<(S, usize), ReplayError> {
    let mut last_admitted_player_id = 0;
    let mut sequences = BTreeMap::new();
    let mut previous_tick = None;
    let mut checkpoints_verified = 0_usize;

    for record in log.records() {
        let record_tick = record.tick();
        if previous_tick.is_some_and(|previous| record_tick < previous) {
            return Err(ReplayError::NonMonotonicTick {
                previous: previous_tick.expect("checked previous tick"),
                actual: record_tick,
            });
        }
        let simulation_tick = simulation.current_tick();
        if record_tick < simulation_tick {
            return Err(ReplayError::ReplayStartsBeforeSimulation {
                simulation_tick,
                record_tick,
            });
        }
        let tick_gap = record_tick - simulation_tick;
        if tick_gap > MAX_VERIFIER_TICK_GAP {
            return Err(ReplayError::TickGapTooLarge {
                from_tick: simulation_tick,
                to_tick: record_tick,
                maximum: MAX_VERIFIER_TICK_GAP,
            });
        }
        if tick_gap == 1 {
            simulation.advance_tick()?;
        }

        match record {
            ReplayRecord::PlayerAdmitted { player_id, .. } => {
                if *player_id <= last_admitted_player_id {
                    return Err(ReplayError::NonIncreasingPlayerId {
                        previous: last_admitted_player_id,
                        actual: *player_id,
                    });
                }
                simulation.add_player(*player_id)?;
                sequences.insert(*player_id, 0);
                last_admitted_player_id = *player_id;
            }
            ReplayRecord::CommandApplied {
                player_id,
                sequence,
                payload,
                ..
            } => {
                let previous = sequences
                    .get_mut(player_id)
                    .ok_or(ReplayError::UnknownCommandPlayer(*player_id))?;
                if *sequence == 0 {
                    return Err(ReplayError::InvalidSequence);
                }
                if *sequence <= *previous {
                    return Err(ReplayError::NonIncreasingSequence {
                        player_id: *player_id,
                        previous: *previous,
                        actual: *sequence,
                    });
                }
                simulation.apply_command(*player_id, *sequence, payload)?;
                *previous = *sequence;
            }
            ReplayRecord::PlayerRemoved { player_id, .. } => {
                if sequences.remove(player_id).is_none() || !simulation.remove_player(*player_id) {
                    return Err(ReplayError::MissingPlayerOnRemoval(*player_id));
                }
            }
            ReplayRecord::Checkpoint { snapshot } => {
                let actual = simulation.snapshot()?;
                if actual.state_hash != snapshot.state_hash {
                    return Err(ReplayError::CheckpointMismatch {
                        tick: snapshot.tick,
                        expected_hash: snapshot.state_hash,
                        actual_hash: actual.state_hash,
                    });
                }
                if actual.payload != snapshot.payload {
                    return Err(ReplayError::CheckpointPayloadMismatch(snapshot.tick));
                }
                checkpoints_verified += 1;
            }
        }
        previous_tick = Some(record_tick);
    }

    Ok((simulation, checkpoints_verified))
}

fn record_body_len(record: &ReplayRecord) -> Result<usize, ReplayError> {
    match record {
        ReplayRecord::PlayerAdmitted { .. } | ReplayRecord::PlayerRemoved { .. } => {
            Ok(PLAYER_BODY_BYTES)
        }
        ReplayRecord::CommandApplied {
            sequence, payload, ..
        } => {
            if *sequence == 0 {
                return Err(ReplayError::InvalidSequence);
            }
            if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload.len(),
                });
            }
            Ok(COMMAND_FIXED_BODY_BYTES + payload.len())
        }
        ReplayRecord::Checkpoint { snapshot } => {
            if snapshot.payload.len() > MAX_SNAPSHOT_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                    actual: snapshot.payload.len(),
                });
            }
            let expected = snapshot_hash(snapshot.tick, &snapshot.payload);
            if snapshot.state_hash != expected {
                return Err(ReplayError::InvalidCheckpointHash {
                    tick: snapshot.tick,
                    expected,
                    actual: snapshot.state_hash,
                });
            }
            Ok(CHECKPOINT_FIXED_BODY_BYTES + snapshot.payload.len())
        }
    }
}

fn encode_record(record: &ReplayRecord, output: &mut Vec<u8>) -> Result<(), ReplayError> {
    let body_len = record_body_len(record)?;
    let kind = match record {
        ReplayRecord::PlayerAdmitted { .. } => ADMISSION_KIND,
        ReplayRecord::CommandApplied { .. } => COMMAND_KIND,
        ReplayRecord::PlayerRemoved { .. } => REMOVAL_KIND,
        ReplayRecord::Checkpoint { .. } => CHECKPOINT_KIND,
    };
    output.reserve(RECORD_HEADER_BYTES + body_len);
    output.push(kind);
    output.extend_from_slice(&record.tick().to_be_bytes());
    output.extend_from_slice(
        &u32::try_from(body_len)
            .expect("bounded replay body length")
            .to_be_bytes(),
    );
    match record {
        ReplayRecord::PlayerAdmitted { player_id, .. }
        | ReplayRecord::PlayerRemoved { player_id, .. } => {
            output.extend_from_slice(&player_id.to_be_bytes());
        }
        ReplayRecord::CommandApplied {
            player_id,
            sequence,
            payload,
            ..
        } => {
            output.extend_from_slice(&player_id.to_be_bytes());
            output.extend_from_slice(&sequence.to_be_bytes());
            output.extend_from_slice(
                &u32::try_from(payload.len())
                    .expect("bounded command payload length")
                    .to_be_bytes(),
            );
            output.extend_from_slice(payload);
        }
        ReplayRecord::Checkpoint { snapshot } => {
            output.extend_from_slice(&snapshot.state_hash.to_be_bytes());
            output.extend_from_slice(
                &u32::try_from(snapshot.payload.len())
                    .expect("bounded snapshot payload length")
                    .to_be_bytes(),
            );
            output.extend_from_slice(&snapshot.payload);
        }
    }
    Ok(())
}

fn decode_record(kind: u8, tick: u64, body: &[u8]) -> Result<ReplayRecord, ReplayError> {
    match kind {
        ADMISSION_KIND => {
            require_body_length(kind, body, PLAYER_BODY_BYTES)?;
            Ok(ReplayRecord::PlayerAdmitted {
                tick,
                player_id: u32::from_be_bytes(body.try_into().expect("checked player body")),
            })
        }
        COMMAND_KIND => {
            if body.len() < COMMAND_FIXED_BODY_BYTES {
                return Err(ReplayError::InvalidRecordLength {
                    kind,
                    expected: COMMAND_FIXED_BODY_BYTES,
                    actual: body.len(),
                });
            }
            let player_id =
                u32::from_be_bytes(body[0..4].try_into().expect("checked command body"));
            let sequence = u32::from_be_bytes(body[4..8].try_into().expect("checked command body"));
            if sequence == 0 {
                return Err(ReplayError::InvalidSequence);
            }
            let payload_len = usize::try_from(u32::from_be_bytes(
                body[8..12].try_into().expect("checked command body"),
            ))
            .expect("u32 fits usize on supported targets");
            if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload_len,
                });
            }
            require_body_length(kind, body, COMMAND_FIXED_BODY_BYTES + payload_len)?;
            Ok(ReplayRecord::CommandApplied {
                tick,
                player_id,
                sequence,
                payload: body[COMMAND_FIXED_BODY_BYTES..].to_vec(),
            })
        }
        REMOVAL_KIND => {
            require_body_length(kind, body, PLAYER_BODY_BYTES)?;
            Ok(ReplayRecord::PlayerRemoved {
                tick,
                player_id: u32::from_be_bytes(body.try_into().expect("checked player body")),
            })
        }
        CHECKPOINT_KIND => {
            if body.len() < CHECKPOINT_FIXED_BODY_BYTES {
                return Err(ReplayError::InvalidRecordLength {
                    kind,
                    expected: CHECKPOINT_FIXED_BODY_BYTES,
                    actual: body.len(),
                });
            }
            let state_hash =
                u64::from_be_bytes(body[0..8].try_into().expect("checked checkpoint body"));
            let payload_len = usize::try_from(u32::from_be_bytes(
                body[8..12].try_into().expect("checked checkpoint body"),
            ))
            .expect("u32 fits usize on supported targets");
            if payload_len > MAX_SNAPSHOT_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                    actual: payload_len,
                });
            }
            require_body_length(kind, body, CHECKPOINT_FIXED_BODY_BYTES + payload_len)?;
            let payload = body[CHECKPOINT_FIXED_BODY_BYTES..].to_vec();
            let expected_hash = snapshot_hash(tick, &payload);
            if state_hash != expected_hash {
                return Err(ReplayError::InvalidCheckpointHash {
                    tick,
                    expected: expected_hash,
                    actual: state_hash,
                });
            }
            Ok(ReplayRecord::Checkpoint {
                snapshot: SimulationSnapshot {
                    tick,
                    state_hash,
                    payload,
                },
            })
        }
        _ => Err(ReplayError::UnknownRecordKind(kind)),
    }
}

fn require_body_length(kind: u8, body: &[u8], expected: usize) -> Result<(), ReplayError> {
    if body.len() != expected {
        return Err(ReplayError::InvalidRecordLength {
            kind,
            expected,
            actual: body.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::{DemoSimulation, encode_demo_command};

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
            sequence: 1,
            payload: command.clone(),
        });
        log.append(ReplayRecord::Checkpoint { snapshot: first });
        log.append(ReplayRecord::CommandApplied {
            tick: 1,
            player_id: 1,
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
                    sequence,
                    payload: command.to_vec(),
                },
            );
            let encoded = image.encode().unwrap();
            let decoded = crate::RecoveryImage::decode(&encoded).unwrap();
            let expected = ReplayError::NonIncreasingSequence {
                player_id: lease.player_id,
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
}
