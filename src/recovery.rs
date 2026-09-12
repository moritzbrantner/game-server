use crate::protocol::{PlayerId, RECONNECT_TOKEN_BYTES};
use crate::replay::{ReplayError, ReplayLog, ReplayRecord};
use crate::session::{
    RecoverableSession, ReconnectToken, SessionRecoveryError, SessionRecoverySnapshot,
};
use crate::simulation::{GameSimulation, SimulationError};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const RECOVERY_FORMAT_VERSION: u8 = 1;
pub const MAX_RECOVERY_IMAGE_BYTES: usize = 256 * 1024 * 1024;
const RECOVERY_MAGIC: &[u8; 4] = b"GSRC";
const HEADER_BYTES: usize = 4 + 1 + 8 + 8 + 4 + 2 + 4;
const SESSION_BYTES: usize = 4 + 4 + 8 + RECONNECT_TOKEN_BYTES;
const MAX_REPLAY_TICK_GAP: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryImage {
    pub current_tick: u64,
    pub reconnect_grace_ticks: u64,
    pub replay: ReplayLog,
    pub sessions: SessionRecoverySnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    InvalidMagic,
    UnsupportedVersion(u8),
    Truncated,
    TrailingBytes(usize),
    ImageTooLarge(usize),
    ReplayTooLarge(usize),
    SessionCountTooLarge(usize),
    ReplayTickMismatch {
        expected: u64,
        actual: Option<u64>,
    },
    ReplayPlayerSetMismatch,
    ReplayTickGapTooLarge {
        from_tick: u64,
        to_tick: u64,
    },
    ReplayStartsBeforeSimulation {
        simulation_tick: u64,
        record_tick: u64,
    },
    MissingPlayerOnRemoval(PlayerId),
    Replay(ReplayError),
    Session(SessionRecoveryError),
    Simulation(SimulationError),
    Io(String),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid recovery image magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported recovery image version {version}")
            }
            Self::Truncated => write!(formatter, "truncated recovery image"),
            Self::TrailingBytes(count) => {
                write!(formatter, "recovery image has {count} trailing bytes")
            }
            Self::ImageTooLarge(size) => {
                write!(formatter, "recovery image size {size} exceeds limit")
            }
            Self::ReplayTooLarge(size) => write!(formatter, "replay payload is too large: {size}"),
            Self::SessionCountTooLarge(count) => {
                write!(formatter, "recovery session count is too large: {count}")
            }
            Self::ReplayTickMismatch { expected, actual } => write!(
                formatter,
                "recovery replay final tick {:?} does not match image tick {expected}",
                actual
            ),
            Self::ReplayPlayerSetMismatch => {
                write!(formatter, "recovery replay players do not match session snapshot")
            }
            Self::ReplayTickGapTooLarge { from_tick, to_tick } => write!(
                formatter,
                "recovery replay tick gap from {from_tick} to {to_tick} exceeds {MAX_REPLAY_TICK_GAP}"
            ),
            Self::ReplayStartsBeforeSimulation {
                simulation_tick,
                record_tick,
            } => write!(
                formatter,
                "recovery replay record tick {record_tick} precedes simulation tick {simulation_tick}"
            ),
            Self::MissingPlayerOnRemoval(player_id) => write!(
                formatter,
                "recovery replay attempted to remove missing player {player_id}"
            ),
            Self::Replay(error) => write!(formatter, "replay error: {error}"),
            Self::Session(error) => write!(formatter, "session recovery error: {error}"),
            Self::Simulation(error) => write!(formatter, "simulation recovery error: {error}"),
            Self::Io(error) => write!(formatter, "recovery I/O error: {error}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<ReplayError> for RecoveryError {
    fn from(error: ReplayError) -> Self {
        Self::Replay(error)
    }
}

impl From<SessionRecoveryError> for RecoveryError {
    fn from(error: SessionRecoveryError) -> Self {
        Self::Session(error)
    }
}

impl From<SimulationError> for RecoveryError {
    fn from(error: SimulationError) -> Self {
        Self::Simulation(error)
    }
}

impl RecoveryImage {
    pub fn encode(&self) -> Result<Vec<u8>, RecoveryError> {
        self.validate_structure()?;
        let replay = self.replay.encode()?;
        if replay.len() > u32::MAX as usize {
            return Err(RecoveryError::ReplayTooLarge(replay.len()));
        }
        if self.sessions.sessions.len() > usize::from(u16::MAX) {
            return Err(RecoveryError::SessionCountTooLarge(
                self.sessions.sessions.len(),
            ));
        }
        let expected_size = HEADER_BYTES
            .checked_add(
                SESSION_BYTES
                    .checked_mul(self.sessions.sessions.len())
                    .ok_or(RecoveryError::ImageTooLarge(usize::MAX))?,
            )
            .and_then(|size| size.checked_add(replay.len()))
            .ok_or(RecoveryError::ImageTooLarge(usize::MAX))?;
        if expected_size > MAX_RECOVERY_IMAGE_BYTES {
            return Err(RecoveryError::ImageTooLarge(expected_size));
        }

        let mut bytes = Vec::with_capacity(expected_size);
        bytes.extend_from_slice(RECOVERY_MAGIC);
        bytes.push(RECOVERY_FORMAT_VERSION);
        bytes.extend_from_slice(&self.current_tick.to_be_bytes());
        bytes.extend_from_slice(&self.reconnect_grace_ticks.to_be_bytes());
        bytes.extend_from_slice(&self.sessions.next_player_id.to_be_bytes());
        bytes.extend_from_slice(
            &u16::try_from(self.sessions.sessions.len())
                .expect("bounded recovery session count")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(replay.len())
                .expect("bounded recovery replay length")
                .to_be_bytes(),
        );
        for session in &self.sessions.sessions {
            bytes.extend_from_slice(&session.player_id.to_be_bytes());
            bytes.extend_from_slice(&session.connection_epoch.to_be_bytes());
            bytes.extend_from_slice(&session.remaining_grace_ticks.to_be_bytes());
            bytes.extend_from_slice(&session.reconnect_token.0);
        }
        bytes.extend_from_slice(&replay);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryError> {
        if bytes.len() > MAX_RECOVERY_IMAGE_BYTES {
            return Err(RecoveryError::ImageTooLarge(bytes.len()));
        }
        if bytes.len() < HEADER_BYTES {
            return Err(RecoveryError::Truncated);
        }
        if &bytes[..RECOVERY_MAGIC.len()] != RECOVERY_MAGIC {
            return Err(RecoveryError::InvalidMagic);
        }
        let version = bytes[4];
        if version != RECOVERY_FORMAT_VERSION {
            return Err(RecoveryError::UnsupportedVersion(version));
        }
        let current_tick = read_u64(bytes, 5)?;
        let reconnect_grace_ticks = read_u64(bytes, 13)?;
        let next_player_id = read_u32(bytes, 21)?;
        let session_count = usize::from(read_u16(bytes, 25)?);
        let replay_len = usize::try_from(read_u32(bytes, 27)?)
            .expect("u32 replay length fits supported usize");
        let sessions_len = SESSION_BYTES
            .checked_mul(session_count)
            .ok_or(RecoveryError::ImageTooLarge(usize::MAX))?;
        let replay_offset = HEADER_BYTES
            .checked_add(sessions_len)
            .ok_or(RecoveryError::ImageTooLarge(usize::MAX))?;
        let expected_len = replay_offset
            .checked_add(replay_len)
            .ok_or(RecoveryError::ImageTooLarge(usize::MAX))?;
        if expected_len > bytes.len() {
            return Err(RecoveryError::Truncated);
        }
        if expected_len < bytes.len() {
            return Err(RecoveryError::TrailingBytes(bytes.len() - expected_len));
        }

        let mut sessions = Vec::with_capacity(session_count);
        let mut offset = HEADER_BYTES;
        for _ in 0..session_count {
            let player_id = read_u32(bytes, offset)?;
            let connection_epoch = read_u32(bytes, offset + 4)?;
            let remaining_grace_ticks = read_u64(bytes, offset + 8)?;
            let token_start = offset + 16;
            let token_end = token_start + RECONNECT_TOKEN_BYTES;
            let reconnect_token = ReconnectToken(
                bytes[token_start..token_end]
                    .try_into()
                    .expect("checked recovery session length"),
            );
            sessions.push(RecoverableSession {
                player_id,
                reconnect_token,
                connection_epoch,
                remaining_grace_ticks,
            });
            offset += SESSION_BYTES;
        }

        let image = Self {
            current_tick,
            reconnect_grace_ticks,
            replay: ReplayLog::decode(&bytes[replay_offset..expected_len])?,
            sessions: SessionRecoverySnapshot {
                next_player_id,
                sessions,
            },
        };
        image.validate_structure()?;
        Ok(image)
    }

    pub fn write_atomic(&self, path: &Path) -> Result<(), RecoveryError> {
        let bytes = self.encode()?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io_error)?;
        let temp_path = recovery_temp_path(path);
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let write_result = (|| -> Result<(), RecoveryError> {
            let mut file = options.open(&temp_path).map_err(io_error)?;
            file.write_all(&bytes).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            fs::rename(&temp_path, path).map_err(io_error)?;
            sync_parent_directory(parent)?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_result
    }

    pub fn read_file(path: &Path) -> Result<Self, RecoveryError> {
        let metadata = fs::metadata(path).map_err(io_error)?;
        let size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
        if size > MAX_RECOVERY_IMAGE_BYTES {
            return Err(RecoveryError::ImageTooLarge(size));
        }
        let mut file = File::open(path).map_err(io_error)?;
        let mut bytes = Vec::with_capacity(size);
        file.read_to_end(&mut bytes).map_err(io_error)?;
        Self::decode(&bytes)
    }

    pub fn last_sequences(&self) -> BTreeMap<PlayerId, u32> {
        let current_players = self
            .sessions
            .sessions
            .iter()
            .map(|session| session.player_id)
            .collect::<BTreeSet<_>>();
        let mut sequences = current_players
            .iter()
            .map(|player_id| (*player_id, 0_u32))
            .collect::<BTreeMap<_, _>>();
        for record in self.replay.records() {
            if let ReplayRecord::CommandApplied {
                player_id,
                sequence,
                ..
            } = record
                && current_players.contains(player_id)
            {
                sequences
                    .entry(*player_id)
                    .and_modify(|current| *current = (*current).max(*sequence));
            }
        }
        sequences
    }

    pub(crate) fn restore_simulation<S: GameSimulation>(
        &self,
        mut simulation: S,
    ) -> Result<S, RecoveryError> {
        let mut previous_tick = None;
        for record in self.replay.records() {
            let record_tick = record.tick();
            if previous_tick.is_some_and(|previous| record_tick < previous) {
                return Err(RecoveryError::Replay(ReplayError::NonMonotonicTick {
                    previous: previous_tick.expect("checked previous recovery tick"),
                    actual: record_tick,
                }));
            }
            let simulation_tick = simulation.current_tick();
            if record_tick < simulation_tick {
                return Err(RecoveryError::ReplayStartsBeforeSimulation {
                    simulation_tick,
                    record_tick,
                });
            }
            let gap = record_tick - simulation_tick;
            if gap > MAX_REPLAY_TICK_GAP {
                return Err(RecoveryError::ReplayTickGapTooLarge {
                    from_tick: simulation_tick,
                    to_tick: record_tick,
                });
            }
            if gap == 1 {
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
                        return Err(RecoveryError::MissingPlayerOnRemoval(*player_id));
                    }
                }
                ReplayRecord::Checkpoint { snapshot } => {
                    let actual = simulation.snapshot()?;
                    if actual.state_hash != snapshot.state_hash {
                        return Err(RecoveryError::Replay(ReplayError::CheckpointMismatch {
                            tick: snapshot.tick,
                            expected_hash: snapshot.state_hash,
                            actual_hash: actual.state_hash,
                        }));
                    }
                    if actual.payload != snapshot.payload {
                        return Err(RecoveryError::Replay(
                            ReplayError::CheckpointPayloadMismatch(snapshot.tick),
                        ));
                    }
                }
            }
            previous_tick = Some(record_tick);
        }

        if simulation.current_tick() != self.current_tick {
            return Err(RecoveryError::ReplayTickMismatch {
                expected: self.current_tick,
                actual: self.replay.records().last().map(ReplayRecord::tick),
            });
        }
        Ok(simulation)
    }

    fn validate_structure(&self) -> Result<(), RecoveryError> {
        let final_tick = self.replay.records().last().map(ReplayRecord::tick);
        if final_tick != Some(self.current_tick) {
            return Err(RecoveryError::ReplayTickMismatch {
                expected: self.current_tick,
                actual: final_tick,
            });
        }

        let mut live_players = BTreeSet::new();
        for record in self.replay.records() {
            match record {
                ReplayRecord::PlayerAdmitted { player_id, .. } => {
                    live_players.insert(*player_id);
                }
                ReplayRecord::PlayerRemoved { player_id, .. } => {
                    live_players.remove(player_id);
                }
                ReplayRecord::CommandApplied { .. } | ReplayRecord::Checkpoint { .. } => {}
            }
        }
        let session_players = self
            .sessions
            .sessions
            .iter()
            .map(|session| session.player_id)
            .collect::<BTreeSet<_>>();
        if live_players != session_players {
            return Err(RecoveryError::ReplayPlayerSetMismatch);
        }
        Ok(())
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, RecoveryError> {
    let end = offset.checked_add(2).ok_or(RecoveryError::Truncated)?;
    Ok(u16::from_be_bytes(
        bytes
            .get(offset..end)
            .ok_or(RecoveryError::Truncated)?
            .try_into()
            .expect("checked u16 recovery range"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, RecoveryError> {
    let end = offset.checked_add(4).ok_or(RecoveryError::Truncated)?;
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..end)
            .ok_or(RecoveryError::Truncated)?
            .try_into()
            .expect("checked u32 recovery range"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, RecoveryError> {
    let end = offset.checked_add(8).ok_or(RecoveryError::Truncated)?;
    Ok(u64::from_be_bytes(
        bytes
            .get(offset..end)
            .ok_or(RecoveryError::Truncated)?
            .try_into()
            .expect("checked u64 recovery range"),
    ))
}

fn recovery_temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("recovery");
    path.with_file_name(format!(".{file_name}.tmp"))
}

fn sync_parent_directory(parent: &Path) -> Result<(), RecoveryError> {
    #[cfg(unix)]
    {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(io_error)?;
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> RecoveryError {
    RecoveryError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::ReplayRecord;
    use crate::simulation::SimulationSnapshot;
    use crate::world::{DemoSimulation, encode_demo_command};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn image() -> RecoveryImage {
        let command = encode_demo_command(1, 0).unwrap().to_vec();
        let mut simulation = DemoSimulation::new();
        simulation.add_player(1).unwrap();
        simulation.apply_command(1, 1, &command).unwrap();
        simulation.advance_tick().unwrap();
        let checkpoint = simulation.snapshot().unwrap();

        let mut replay = ReplayLog::default();
        replay.append(ReplayRecord::PlayerAdmitted {
            tick: 0,
            player_id: 1,
        });
        replay.append(ReplayRecord::CommandApplied {
            tick: 0,
            player_id: 1,
            sequence: 1,
            payload: command,
        });
        replay.append(ReplayRecord::Checkpoint {
            snapshot: checkpoint,
        });

        RecoveryImage {
            current_tick: 1,
            reconnect_grace_ticks: 600,
            replay,
            sessions: SessionRecoverySnapshot {
                next_player_id: 2,
                sessions: vec![RecoverableSession {
                    player_id: 1,
                    reconnect_token: ReconnectToken([7; RECONNECT_TOKEN_BYTES]),
                    connection_epoch: 2,
                    remaining_grace_ticks: 600,
                }],
            },
        }
    }

    #[test]
    fn recovery_image_round_trip_is_exact() {
        let expected = image();
        let encoded = expected.encode().unwrap();
        assert_eq!(RecoveryImage::decode(&encoded).unwrap(), expected);
    }

    #[test]
    fn recovery_reconstructs_simulation_and_sequences() {
        let image = image();
        let simulation = image.restore_simulation(DemoSimulation::new()).unwrap();
        assert_eq!(simulation.current_tick(), 1);
        assert_eq!(simulation.snapshot().unwrap().state_hash, image.replay.records().last().and_then(|record| match record {
            ReplayRecord::Checkpoint { snapshot } => Some(snapshot.state_hash),
            _ => None,
        }).unwrap());
        assert_eq!(image.last_sequences().get(&1), Some(&1));
    }

    #[test]
    fn atomic_file_round_trip_replaces_previous_image() {
        let path = unique_test_path();
        let first = image();
        first.write_atomic(&path).unwrap();
        assert_eq!(RecoveryImage::read_file(&path).unwrap(), first);

        let mut second = first.clone();
        second.reconnect_grace_ticks = 300;
        second.sessions.sessions[0].remaining_grace_ticks = 300;
        second.write_atomic(&path).unwrap();
        assert_eq!(RecoveryImage::read_file(&path).unwrap(), second);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn corrupted_replay_fails_closed() {
        let mut encoded = image().encode().unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(RecoveryImage::decode(&encoded).is_err());
    }

    fn unique_test_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "game-server-recovery-{}-{nonce}.bin",
            std::process::id()
        ))
    }
}
