//! The in-memory replay history: records and the append-only log that holds them.
use super::ReplayError;
use super::codec::{
    RECORD_HEADER_BYTES, REPLAY_HEADER_BYTES, decode_records, encode_records, record_body_len,
};
use crate::protocol::{CommandLane, PlayerId};
use crate::replay_baseline::ReplayBaseline;
use crate::simulation::SimulationSnapshot;
use std::fmt;

#[derive(Clone, Eq, PartialEq)]
pub enum ReplayRecord {
    Baseline {
        baseline: ReplayBaseline,
    },
    PlayerAdmitted {
        tick: u64,
        player_id: PlayerId,
    },
    CommandApplied {
        tick: u64,
        player_id: PlayerId,
        lane: CommandLane,
        sequence: u32,
        payload: Vec<u8>,
    },
    HostInputApplied {
        tick: u64,
        player_id: PlayerId,
        payload: Vec<u8>,
    },
    PlayerRemoved {
        tick: u64,
        player_id: PlayerId,
    },
    Checkpoint {
        snapshot: SimulationSnapshot,
    },
    DigestCheckpoint {
        tick: u64,
        digest: [u8; 32],
    },
}

impl ReplayRecord {
    pub fn tick(&self) -> u64 {
        match self {
            Self::Baseline { baseline } => baseline.snapshot.tick,
            Self::PlayerAdmitted { tick, .. }
            | Self::CommandApplied { tick, .. }
            | Self::HostInputApplied { tick, .. }
            | Self::PlayerRemoved { tick, .. }
            | Self::DigestCheckpoint { tick, .. } => *tick,
            Self::Checkpoint { snapshot } => snapshot.tick,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplayLog {
    pub(super) records: Vec<ReplayRecord>,
}

impl ReplayLog {
    pub fn records(&self) -> &[ReplayRecord] {
        &self.records
    }

    pub fn encode(&self) -> Result<Vec<u8>, ReplayError> {
        encode_records(&self.records)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplayError> {
        Ok(Self {
            records: decode_records(bytes)?,
        })
    }

    /// Appends a committed journal segment without reinterpreting gameplay.
    /// Checks framing and tick order before mutation. Restore/verify still enforce
    /// player identities and lane sequences through the shared interpreter.
    pub fn append_journal(&mut self, suffix: &ReplayLog) -> Result<(), ReplayError> {
        let mut previous_tick = self.records.last().map(ReplayRecord::tick);
        for (index, record) in suffix.records.iter().enumerate() {
            if matches!(record, ReplayRecord::Baseline { .. })
                && (!self.records.is_empty() || index != 0)
            {
                return Err(ReplayError::InvalidBaseline("baseline must be first"));
            }
            record_body_len(record)?;
            if let Some(previous) = previous_tick.filter(|previous| record.tick() < *previous) {
                return Err(ReplayError::NonMonotonicTick {
                    previous,
                    actual: record.tick(),
                });
            }
            previous_tick = Some(record.tick());
        }
        self.records.extend(suffix.records.iter().cloned());
        Ok(())
    }

    pub(crate) fn suffix(&self, from: usize, maximum: usize) -> Result<(Self, usize), ReplayError> {
        let records = self
            .records
            .get(from..)
            .ok_or(ReplayError::InvalidBaseline(
                "journal cursor exceeds replay",
            ))?;
        let mut byte_length = REPLAY_HEADER_BYTES;
        for record in records {
            byte_length = byte_length
                .checked_add(RECORD_HEADER_BYTES + record_body_len(record)?)
                .ok_or(ReplayError::RecordTooLarge(usize::MAX))?;
            if byte_length > maximum {
                return Err(ReplayError::PayloadTooLarge {
                    maximum,
                    actual: byte_length,
                });
            }
        }
        Ok((
            Self {
                records: records.to_vec(),
            },
            byte_length,
        ))
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

impl fmt::Debug for ReplayRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Baseline { baseline } => f
                .debug_struct("Baseline")
                .field("baseline", baseline)
                .finish(),
            Self::CommandApplied {
                tick,
                player_id,
                lane,
                sequence,
                payload,
            } => f
                .debug_struct("CommandApplied")
                .field("tick", tick)
                .field("player_id", player_id)
                .field("lane", lane)
                .field("sequence", sequence)
                .field("payload_bytes", &payload.len())
                .finish(),
            Self::HostInputApplied {
                tick,
                player_id,
                payload,
            } => f
                .debug_struct("HostInputApplied")
                .field("tick", tick)
                .field("player_id", player_id)
                .field("payload_bytes", &payload.len())
                .finish(),
            Self::PlayerAdmitted { tick, player_id } => f
                .debug_struct("PlayerAdmitted")
                .field("tick", tick)
                .field("player_id", player_id)
                .finish(),
            Self::PlayerRemoved { tick, player_id } => f
                .debug_struct("PlayerRemoved")
                .field("tick", tick)
                .field("player_id", player_id)
                .finish(),
            Self::Checkpoint { snapshot } => f
                .debug_struct("Checkpoint")
                .field("snapshot", snapshot)
                .finish(),
            Self::DigestCheckpoint { tick, .. } => f
                .debug_struct("DigestCheckpoint")
                .field("tick", tick)
                .finish_non_exhaustive(),
        }
    }
}
