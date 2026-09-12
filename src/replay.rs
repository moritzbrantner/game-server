use crate::protocol::{
    MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, PlayerId, snapshot_hash,
};
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot};
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
    mut simulation: S,
    log: &ReplayLog,
) -> Result<ReplayVerification, ReplayError> {
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
        while simulation.current_tick() < record_tick {
            simulation.advance_tick()?;
        }

        match record {
            ReplayRecord::PlayerAdmitted { player_id, .. } => {
                simulation.add_player(*player_id)?;
            }
            ReplayRecord::CommandApplied {
                player_id,
                sequence,
                payload,
                ..
            } => {
                simulation.apply_command(*player_id, *sequence, payload)?;
            }
            ReplayRecord::PlayerRemoved { player_id, .. } => {
                if !simulation.remove_player(*player_id) {
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

    Ok(ReplayVerification {
        records_verified: log.records().len(),
        checkpoints_verified,
        final_snapshot: simulation.snapshot()?,
    })
}

fn encode_record(record: &ReplayRecord, output: &mut Vec<u8>) -> Result<(), ReplayError> {
    let (kind, tick, body) = match record {
        ReplayRecord::PlayerAdmitted { tick, player_id } => {
            (ADMISSION_KIND, *tick, player_id.to_be_bytes().to_vec())
        }
        ReplayRecord::CommandApplied {
            tick,
            player_id,
            sequence,
            payload,
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
            let mut body = Vec::with_capacity(COMMAND_FIXED_BODY_BYTES + payload.len());
            body.extend_from_slice(&player_id.to_be_bytes());
            body.extend_from_slice(&sequence.to_be_bytes());
            body.extend_from_slice(
                &u32::try_from(payload.len())
                    .expect("bounded command payload length")
                    .to_be_bytes(),
            );
            body.extend_from_slice(payload);
            (COMMAND_KIND, *tick, body)
        }
        ReplayRecord::PlayerRemoved { tick, player_id } => {
            (REMOVAL_KIND, *tick, player_id.to_be_bytes().to_vec())
        }
        ReplayRecord::Checkpoint { snapshot } => {
            if snapshot.payload.len() > MAX_SNAPSHOT_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                    actual: snapshot.payload.len(),
                });
            }
            let expected_hash = snapshot_hash(snapshot.tick, &snapshot.payload);
            if snapshot.state_hash != expected_hash {
                return Err(ReplayError::InvalidCheckpointHash {
                    tick: snapshot.tick,
                    expected: expected_hash,
                    actual: snapshot.state_hash,
                });
            }
            let mut body = Vec::with_capacity(CHECKPOINT_FIXED_BODY_BYTES + snapshot.payload.len());
            body.extend_from_slice(&snapshot.state_hash.to_be_bytes());
            body.extend_from_slice(
                &u32::try_from(snapshot.payload.len())
                    .expect("bounded snapshot payload length")
                    .to_be_bytes(),
            );
            body.extend_from_slice(&snapshot.payload);
            (CHECKPOINT_KIND, snapshot.tick, body)
        }
    };

    output.push(kind);
    output.extend_from_slice(&tick.to_be_bytes());
    output.extend_from_slice(
        &u32::try_from(body.len())
            .map_err(|_| ReplayError::RecordTooLarge(body.len()))?
            .to_be_bytes(),
    );
    output.extend_from_slice(&body);
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
}
