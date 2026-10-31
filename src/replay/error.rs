use crate::protocol::{CommandLane, PlayerId};
use crate::simulation::SimulationError;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    InvalidMagic,
    InvalidBaseline(&'static str),
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
        lane: CommandLane,
        previous: u32,
        actual: u32,
    },
    UnknownCommandLane(u8),
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
    CheckpointDigestMismatch(u64),
    Simulation(SimulationError),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseline(reason) => write!(formatter, "invalid replay baseline: {reason}"),
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
                lane,
                previous,
                actual,
            } => write!(
                formatter,
                "replay {lane:?} command sequence {actual} for player {player_id} must exceed {previous}"
            ),
            Self::UnknownCommandLane(lane) => {
                write!(formatter, "replay command record has unknown lane {lane}")
            }
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
            Self::CheckpointDigestMismatch(tick) => {
                write!(formatter, "replay digest diverged at checkpoint {tick}")
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
