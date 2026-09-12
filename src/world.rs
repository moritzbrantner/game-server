use crate::protocol::PlayerId;
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot};
use std::collections::BTreeMap;
use std::fmt;

pub const DEMO_TICK_HZ: u16 = 20;
pub const DEMO_MAX_PLAYERS: usize = 16;
pub const WORLD_LIMIT: i16 = 10_000;
pub const STEP_UNITS: i16 = 64;
const DEMO_COMMAND_BYTES: usize = 2;
const DEMO_PLAYER_BYTES: usize = 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DemoSnapshotPlayer {
    pub player_id: PlayerId,
    pub x: i16,
    pub y: i16,
    pub last_applied_sequence: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorldError {
    InvalidPlayerId,
    DuplicatePlayer(PlayerId),
    PlayerCapacity,
    UnknownPlayer(PlayerId),
    InvalidCommandLength(usize),
    InvalidAxis { horizontal: i8, vertical: i8 },
    InvalidSnapshotLength,
    TickExhausted,
}

impl fmt::Display for WorldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlayerId => write!(formatter, "player id zero is reserved"),
            Self::DuplicatePlayer(player_id) => {
                write!(formatter, "player {player_id} already exists")
            }
            Self::PlayerCapacity => write!(formatter, "demo world has reached player capacity"),
            Self::UnknownPlayer(player_id) => write!(formatter, "unknown player {player_id}"),
            Self::InvalidCommandLength(actual) => {
                write!(formatter, "demo command must be 2 bytes, received {actual}")
            }
            Self::InvalidAxis {
                horizontal,
                vertical,
            } => write!(
                formatter,
                "demo axes must each be between -1 and 1, got ({horizontal}, {vertical})"
            ),
            Self::InvalidSnapshotLength => {
                write!(formatter, "invalid demo snapshot payload length")
            }
            Self::TickExhausted => write!(formatter, "tick counter is exhausted"),
        }
    }
}

impl std::error::Error for WorldError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PlayerRecord {
    x: i16,
    y: i16,
    horizontal: i8,
    vertical: i8,
    last_received_sequence: u32,
    last_applied_sequence: u32,
}

#[derive(Clone, Debug, Default)]
struct DemoWorld {
    tick: u64,
    players: BTreeMap<PlayerId, PlayerRecord>,
}

impl DemoWorld {
    fn add_player(&mut self, player_id: PlayerId) -> Result<(), WorldError> {
        if player_id == 0 {
            return Err(WorldError::InvalidPlayerId);
        }
        if self.players.contains_key(&player_id) {
            return Err(WorldError::DuplicatePlayer(player_id));
        }
        if self.players.len() >= DEMO_MAX_PLAYERS {
            return Err(WorldError::PlayerCapacity);
        }
        let (x, y) = spawn_position(player_id);
        self.players.insert(
            player_id,
            PlayerRecord {
                x,
                y,
                horizontal: 0,
                vertical: 0,
                last_received_sequence: 0,
                last_applied_sequence: 0,
            },
        );
        Ok(())
    }

    fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.players.remove(&player_id).is_some()
    }

    fn apply_command(
        &mut self,
        player_id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), WorldError> {
        if payload.len() != DEMO_COMMAND_BYTES {
            return Err(WorldError::InvalidCommandLength(payload.len()));
        }
        let horizontal = payload[0] as i8;
        let vertical = payload[1] as i8;
        if !(-1..=1).contains(&horizontal) || !(-1..=1).contains(&vertical) {
            return Err(WorldError::InvalidAxis {
                horizontal,
                vertical,
            });
        }
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or(WorldError::UnknownPlayer(player_id))?;
        player.horizontal = horizontal;
        player.vertical = vertical;
        player.last_received_sequence = sequence;
        Ok(())
    }

    fn advance_tick(&mut self) -> Result<(), WorldError> {
        self.tick = self.tick.checked_add(1).ok_or(WorldError::TickExhausted)?;
        for player in self.players.values_mut() {
            let next_x = player.x as i32 + i32::from(player.horizontal) * i32::from(STEP_UNITS);
            let next_y = player.y as i32 + i32::from(player.vertical) * i32::from(STEP_UNITS);
            player.x = next_x.clamp(i32::from(-WORLD_LIMIT), i32::from(WORLD_LIMIT)) as i16;
            player.y = next_y.clamp(i32::from(-WORLD_LIMIT), i32::from(WORLD_LIMIT)) as i16;
            player.last_applied_sequence = player.last_received_sequence;
        }
        Ok(())
    }

    fn snapshot_payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(1 + self.players.len() * DEMO_PLAYER_BYTES);
        payload.push(self.players.len() as u8);
        for (&player_id, player) in &self.players {
            payload.extend_from_slice(&player_id.to_be_bytes());
            payload.extend_from_slice(&player.x.to_be_bytes());
            payload.extend_from_slice(&player.y.to_be_bytes());
            payload.extend_from_slice(&player.last_applied_sequence.to_be_bytes());
        }
        payload
    }
}

#[derive(Clone, Debug, Default)]
pub struct DemoSimulation {
    world: DemoWorld,
}

impl DemoSimulation {
    pub fn new() -> Self {
        Self::default()
    }
}

impl GameSimulation for DemoSimulation {
    fn tick_hz(&self) -> u16 {
        DEMO_TICK_HZ
    }

    fn max_players(&self) -> usize {
        DEMO_MAX_PLAYERS
    }

    fn current_tick(&self) -> u64 {
        self.world.tick
    }

    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError> {
        self.world
            .add_player(player_id)
            .map_err(|error| SimulationError::new(error.to_string()))
    }

    fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.world.remove_player(player_id)
    }

    fn apply_command(
        &mut self,
        player_id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        self.world
            .apply_command(player_id, sequence, payload)
            .map_err(|error| SimulationError::new(error.to_string()))
    }

    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        self.world
            .advance_tick()
            .map_err(|error| SimulationError::new(error.to_string()))
    }

    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        Ok(SimulationSnapshot::new(
            self.world.tick,
            self.world.snapshot_payload(),
        ))
    }
}

pub fn encode_demo_command(horizontal: i8, vertical: i8) -> Result<[u8; 2], WorldError> {
    if !(-1..=1).contains(&horizontal) || !(-1..=1).contains(&vertical) {
        return Err(WorldError::InvalidAxis {
            horizontal,
            vertical,
        });
    }
    Ok([horizontal as u8, vertical as u8])
}

pub fn decode_demo_snapshot(payload: &[u8]) -> Result<Vec<DemoSnapshotPlayer>, WorldError> {
    let Some((&count, remainder)) = payload.split_first() else {
        return Err(WorldError::InvalidSnapshotLength);
    };
    let count = usize::from(count);
    if count > DEMO_MAX_PLAYERS || remainder.len() != count * DEMO_PLAYER_BYTES {
        return Err(WorldError::InvalidSnapshotLength);
    }
    let mut players = Vec::with_capacity(count);
    for index in 0..count {
        let offset = index * DEMO_PLAYER_BYTES;
        players.push(DemoSnapshotPlayer {
            player_id: u32::from_be_bytes(
                remainder[offset..offset + 4]
                    .try_into()
                    .expect("checked demo snapshot length"),
            ),
            x: i16::from_be_bytes(
                remainder[offset + 4..offset + 6]
                    .try_into()
                    .expect("checked demo snapshot length"),
            ),
            y: i16::from_be_bytes(
                remainder[offset + 6..offset + 8]
                    .try_into()
                    .expect("checked demo snapshot length"),
            ),
            last_applied_sequence: u32::from_be_bytes(
                remainder[offset + 8..offset + 12]
                    .try_into()
                    .expect("checked demo snapshot length"),
            ),
        });
    }
    Ok(players)
}

fn spawn_position(player_id: PlayerId) -> (i16, i16) {
    let lane = i32::try_from(player_id % 8).expect("bounded lane");
    let row = i32::try_from((player_id / 8) % 8).expect("bounded row");
    ((lane * 256 - 896) as i16, (row * 256 - 896) as i16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_simulation_is_deterministic() {
        fn run() -> SimulationSnapshot {
            let mut simulation = DemoSimulation::new();
            simulation.add_player(1).unwrap();
            simulation.add_player(2).unwrap();
            simulation
                .apply_command(1, 1, &encode_demo_command(1, 0).unwrap())
                .unwrap();
            simulation
                .apply_command(2, 1, &encode_demo_command(0, -1).unwrap())
                .unwrap();
            for _ in 0..10 {
                simulation.advance_tick().unwrap();
            }
            simulation.snapshot().unwrap()
        }
        assert_eq!(run(), run());
    }

    #[test]
    fn reference_snapshot_round_trip_is_exact() {
        let mut simulation = DemoSimulation::new();
        simulation.add_player(1).unwrap();
        simulation
            .apply_command(1, 7, &encode_demo_command(1, -1).unwrap())
            .unwrap();
        simulation.advance_tick().unwrap();
        let snapshot = simulation.snapshot().unwrap();
        let players = decode_demo_snapshot(&snapshot.payload).unwrap();
        assert_eq!(players.len(), 1);
        assert_eq!(players[0].player_id, 1);
        assert_eq!(players[0].last_applied_sequence, 7);
    }
}
