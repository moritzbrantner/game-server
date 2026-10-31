//! The replay binary format: framing, record kinds, and version selection.
use super::{ReplayError, ReplayRecord};
use crate::protocol::{
    CommandLane, MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, snapshot_hash,
};
use crate::replay_baseline::{MAX_BASELINE_BODY_BYTES, ReplayBaseline};
use crate::simulation::SimulationSnapshot;

/// Newest replay format: version 5 adds an opt-in canonical recovery baseline.
///
/// Encoding picks the oldest version that can carry a log, so datagram-only logs
/// stay byte-identical to their version 1 (no digests) or version 2 encoding.
pub const REPLAY_FORMAT_VERSION: u8 = 5;
const HOST_INPUT_REPLAY_FORMAT_VERSION: u8 = 4;
pub(super) const LANE_REPLAY_FORMAT_VERSION: u8 = 3;
pub(super) const DIGEST_REPLAY_FORMAT_VERSION: u8 = 2;
pub(super) const LEGACY_REPLAY_FORMAT_VERSION: u8 = 1;
const REPLAY_MAGIC: &[u8; 4] = b"GSRP";
pub(super) const REPLAY_HEADER_BYTES: usize = REPLAY_MAGIC.len() + 1;
pub(super) const RECORD_HEADER_BYTES: usize = 1 + 8 + 4;
const ADMISSION_KIND: u8 = 1;
const COMMAND_KIND: u8 = 2;
const REMOVAL_KIND: u8 = 3;
const CHECKPOINT_KIND: u8 = 4;
pub(super) const DIGEST_CHECKPOINT_KIND: u8 = 5;
/// Version 3: a command on a non-datagram lane. Datagram commands keep kind 2.
pub(super) const LANE_COMMAND_KIND: u8 = 6;
const HOST_INPUT_KIND: u8 = 7;
const BASELINE_KIND: u8 = 8;
const HOST_INPUT_FIXED_BODY_BYTES: usize = 4 + 4;
pub(super) const RELIABLE_LANE_BYTE: u8 = 1;
const DIGEST_BODY_BYTES: usize = 32;
pub(crate) const FULL_CHECKPOINT_PAYLOAD_THRESHOLD: usize =
    DIGEST_BODY_BYTES - CHECKPOINT_FIXED_BODY_BYTES;
pub(super) const PLAYER_BODY_BYTES: usize = 4;
const COMMAND_FIXED_BODY_BYTES: usize = 4 + 4 + 4;
const LANE_COMMAND_FIXED_BODY_BYTES: usize = 4 + 1 + 4 + 4;
const CHECKPOINT_FIXED_BODY_BYTES: usize = 8 + 4;
const MAX_REPLAY_BODY_BYTES: usize = CHECKPOINT_FIXED_BODY_BYTES + MAX_SNAPSHOT_PAYLOAD_BYTES;

pub(super) fn encode_records(records: &[ReplayRecord]) -> Result<Vec<u8>, ReplayError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(REPLAY_MAGIC);
    let has_lane_commands = records.iter().any(|record| {
        matches!(
            record,
            ReplayRecord::CommandApplied { lane, .. } if *lane != CommandLane::Datagram
        )
    });
    let has_digests = records
        .iter()
        .any(|record| matches!(record, ReplayRecord::DigestCheckpoint { .. }));
    bytes.push(
        if matches!(records.first(), Some(ReplayRecord::Baseline { .. })) {
            REPLAY_FORMAT_VERSION
        } else if records
            .iter()
            .any(|record| matches!(record, ReplayRecord::HostInputApplied { .. }))
        {
            HOST_INPUT_REPLAY_FORMAT_VERSION
        } else if has_lane_commands {
            LANE_REPLAY_FORMAT_VERSION
        } else if has_digests {
            DIGEST_REPLAY_FORMAT_VERSION
        } else {
            LEGACY_REPLAY_FORMAT_VERSION
        },
    );
    for (index, record) in records.iter().enumerate() {
        if index != 0 && matches!(record, ReplayRecord::Baseline { .. }) {
            return Err(ReplayError::InvalidBaseline("baseline must be first"));
        }
        encode_record(record, &mut bytes)?;
    }
    Ok(bytes)
}

pub(super) fn decode_records(bytes: &[u8]) -> Result<Vec<ReplayRecord>, ReplayError> {
    if bytes.len() < REPLAY_HEADER_BYTES {
        return Err(ReplayError::Truncated);
    }
    if &bytes[..REPLAY_MAGIC.len()] != REPLAY_MAGIC {
        return Err(ReplayError::InvalidMagic);
    }
    let version = bytes[REPLAY_MAGIC.len()];
    if !(LEGACY_REPLAY_FORMAT_VERSION..=REPLAY_FORMAT_VERSION).contains(&version) {
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
        if (version < DIGEST_REPLAY_FORMAT_VERSION && kind == DIGEST_CHECKPOINT_KIND)
            || (version < LANE_REPLAY_FORMAT_VERSION && kind == LANE_COMMAND_KIND)
            || (version < HOST_INPUT_REPLAY_FORMAT_VERSION && kind == HOST_INPUT_KIND)
            || (version < REPLAY_FORMAT_VERSION && kind == BASELINE_KIND)
        {
            return Err(ReplayError::UnknownRecordKind(kind));
        }
        let tick = u64::from_be_bytes(field(bytes, offset + 1)?);
        let body_len = u32::from_be_bytes(field(bytes, offset + 9)?);
        let body_len =
            usize::try_from(body_len).map_err(|_| ReplayError::RecordTooLarge(usize::MAX))?;
        if kind == BASELINE_KIND && !records.is_empty() {
            return Err(ReplayError::InvalidBaseline("baseline must be first"));
        }
        let maximum = if kind == BASELINE_KIND {
            MAX_BASELINE_BODY_BYTES
        } else {
            MAX_REPLAY_BODY_BYTES
        };
        if body_len > maximum {
            return Err(ReplayError::RecordTooLarge(body_len));
        }
        offset += RECORD_HEADER_BYTES;
        let body_end = offset.checked_add(body_len).ok_or(ReplayError::Truncated)?;
        if body_end > bytes.len() {
            return Err(ReplayError::Truncated);
        }
        if let Some(previous) = previous_tick.filter(|previous| tick < *previous) {
            return Err(ReplayError::NonMonotonicTick {
                previous,
                actual: tick,
            });
        }
        records.push(decode_record(kind, tick, &bytes[offset..body_end])?);
        previous_tick = Some(tick);
        offset = body_end;
    }

    Ok(records)
}

pub(super) fn record_body_len(record: &ReplayRecord) -> Result<usize, ReplayError> {
    match record {
        ReplayRecord::Baseline { baseline } => baseline.body_len(),
        ReplayRecord::HostInputApplied { payload, .. } => {
            if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload.len(),
                });
            }
            Ok(HOST_INPUT_FIXED_BODY_BYTES + payload.len())
        }

        ReplayRecord::DigestCheckpoint { .. } => Ok(DIGEST_BODY_BYTES),
        ReplayRecord::PlayerAdmitted { .. } | ReplayRecord::PlayerRemoved { .. } => {
            Ok(PLAYER_BODY_BYTES)
        }
        ReplayRecord::CommandApplied {
            lane,
            sequence,
            payload,
            ..
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
            Ok(match lane {
                CommandLane::Datagram => COMMAND_FIXED_BODY_BYTES,
                CommandLane::Reliable => LANE_COMMAND_FIXED_BODY_BYTES,
            } + payload.len())
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
    let encoded_body_len =
        u32::try_from(body_len).map_err(|_| ReplayError::RecordTooLarge(body_len))?;
    let kind = match record {
        ReplayRecord::Baseline { .. } => BASELINE_KIND,
        ReplayRecord::PlayerAdmitted { .. } => ADMISSION_KIND,
        ReplayRecord::CommandApplied {
            lane: CommandLane::Datagram,
            ..
        } => COMMAND_KIND,
        ReplayRecord::CommandApplied {
            lane: CommandLane::Reliable,
            ..
        } => LANE_COMMAND_KIND,
        ReplayRecord::HostInputApplied { .. } => HOST_INPUT_KIND,
        ReplayRecord::PlayerRemoved { .. } => REMOVAL_KIND,
        ReplayRecord::Checkpoint { .. } => CHECKPOINT_KIND,
        ReplayRecord::DigestCheckpoint { .. } => DIGEST_CHECKPOINT_KIND,
    };
    output.reserve(RECORD_HEADER_BYTES + body_len);
    output.push(kind);
    output.extend_from_slice(&record.tick().to_be_bytes());
    output.extend_from_slice(&encoded_body_len.to_be_bytes());
    match record {
        ReplayRecord::Baseline { baseline } => baseline.encode_body(output),
        ReplayRecord::HostInputApplied {
            player_id, payload, ..
        } => {
            output.extend_from_slice(&player_id.to_be_bytes());
            output.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            output.extend_from_slice(payload);
        }
        ReplayRecord::DigestCheckpoint { digest, .. } => output.extend_from_slice(digest),
        ReplayRecord::PlayerAdmitted { player_id, .. }
        | ReplayRecord::PlayerRemoved { player_id, .. } => {
            output.extend_from_slice(&player_id.to_be_bytes());
        }
        ReplayRecord::CommandApplied {
            player_id,
            lane,
            sequence,
            payload,
            ..
        } => {
            output.extend_from_slice(&player_id.to_be_bytes());
            if *lane == CommandLane::Reliable {
                output.push(RELIABLE_LANE_BYTE);
            }
            output.extend_from_slice(&sequence.to_be_bytes());
            output.extend_from_slice(
                &u32::try_from(payload.len())
                    .map_err(|_| ReplayError::PayloadTooLarge {
                        maximum: MAX_COMMAND_PAYLOAD_BYTES,
                        actual: payload.len(),
                    })?
                    .to_be_bytes(),
            );
            output.extend_from_slice(payload);
        }
        ReplayRecord::Checkpoint { snapshot } => {
            output.extend_from_slice(&snapshot.state_hash.to_be_bytes());
            output.extend_from_slice(
                &u32::try_from(snapshot.payload.len())
                    .map_err(|_| ReplayError::PayloadTooLarge {
                        maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                        actual: snapshot.payload.len(),
                    })?
                    .to_be_bytes(),
            );
            output.extend_from_slice(&snapshot.payload);
        }
    }
    Ok(())
}

/// Reads `N` bytes at `offset`; callers have already checked the length.
fn field<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], ReplayError> {
    bytes
        .get(offset..offset + N)
        .and_then(|field| field.try_into().ok())
        .ok_or(ReplayError::Truncated)
}

fn decode_record(kind: u8, tick: u64, body: &[u8]) -> Result<ReplayRecord, ReplayError> {
    match kind {
        BASELINE_KIND => Ok(ReplayRecord::Baseline {
            baseline: ReplayBaseline::decode(tick, body)?,
        }),
        DIGEST_CHECKPOINT_KIND => {
            require_body_length(kind, body, DIGEST_BODY_BYTES)?;
            Ok(ReplayRecord::DigestCheckpoint {
                tick,
                digest: field(body, 0)?,
            })
        }
        ADMISSION_KIND => {
            require_body_length(kind, body, PLAYER_BODY_BYTES)?;
            Ok(ReplayRecord::PlayerAdmitted {
                tick,
                player_id: u32::from_be_bytes(field(body, 0)?),
            })
        }
        COMMAND_KIND | LANE_COMMAND_KIND => {
            let (fixed, lane) = if kind == COMMAND_KIND {
                (COMMAND_FIXED_BODY_BYTES, CommandLane::Datagram)
            } else {
                (LANE_COMMAND_FIXED_BODY_BYTES, CommandLane::Reliable)
            };
            if body.len() < fixed {
                return Err(ReplayError::InvalidRecordLength {
                    kind,
                    expected: fixed,
                    actual: body.len(),
                });
            }
            let player_id = u32::from_be_bytes(field(body, 0)?);
            // Lane records name their lane explicitly; datagram commands always use
            // kind 2, so only the reliable lane byte is canonical here.
            let offset = if kind == LANE_COMMAND_KIND {
                if body[4] != RELIABLE_LANE_BYTE {
                    return Err(ReplayError::UnknownCommandLane(body[4]));
                }
                5
            } else {
                4
            };
            let sequence = u32::from_be_bytes(field(body, offset)?);
            if sequence == 0 {
                return Err(ReplayError::InvalidSequence);
            }
            let payload_len = usize::try_from(u32::from_be_bytes(field(body, offset + 4)?))
                .map_err(|_| ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: usize::MAX,
                })?;
            if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload_len,
                });
            }
            require_body_length(kind, body, fixed + payload_len)?;
            Ok(ReplayRecord::CommandApplied {
                tick,
                player_id,
                lane,
                sequence,
                payload: body[fixed..].to_vec(),
            })
        }
        HOST_INPUT_KIND => {
            if body.len() < HOST_INPUT_FIXED_BODY_BYTES {
                return Err(ReplayError::InvalidRecordLength {
                    kind,
                    expected: HOST_INPUT_FIXED_BODY_BYTES,
                    actual: body.len(),
                });
            }
            let payload_len = u32::from_be_bytes(field(body, 4)?) as usize;
            if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ReplayError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload_len,
                });
            }
            require_body_length(kind, body, HOST_INPUT_FIXED_BODY_BYTES + payload_len)?;
            Ok(ReplayRecord::HostInputApplied {
                tick,
                player_id: u32::from_be_bytes(field(body, 0)?),
                payload: body[HOST_INPUT_FIXED_BODY_BYTES..].to_vec(),
            })
        }
        REMOVAL_KIND => {
            require_body_length(kind, body, PLAYER_BODY_BYTES)?;
            Ok(ReplayRecord::PlayerRemoved {
                tick,
                player_id: u32::from_be_bytes(field(body, 0)?),
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
            let state_hash = u64::from_be_bytes(field(body, 0)?);
            let payload_len =
                usize::try_from(u32::from_be_bytes(field(body, 8)?)).map_err(|_| {
                    ReplayError::PayloadTooLarge {
                        maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                        actual: usize::MAX,
                    }
                })?;
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
