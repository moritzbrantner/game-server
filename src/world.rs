use crate::protocol::{InputCommand, MAX_PLAYERS, PlayerId, Snapshot, SnapshotPlayer, snapshot_hash, validate_input};
use std::collections::BTreeMap;
use std::fmt;

pub const TICK_HZ: u16 = 20;
pub const WORLD_LIMIT: i16 = 10_000;
pub const STEP_UNITS: i16 = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    Accepted,
    IgnoredStale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorldError {
    InvalidPlayerId,
    DuplicatePlayer(PlayerId),
    PlayerCapacity,
    UnknownPlayer(PlayerId),
    InvalidInput(crate::protocol::ProtocolError),
    TickExhausted,
}

impl fmt::Display for WorldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlayerId => write!(formatter, "player id zero is reserved"),
            Self::DuplicatePlayer(player_id) => write!(formatter, "player {player_id} already exists"),
            Self::PlayerCapacity => write!(formatter, "world has reached player capacity"),
            Self::UnknownPlayer(player_id) => write!(formatter, "unknown player {player_id}"),
            Self::InvalidInput(error) => write!(formatter, "invalid input: {error}"),
            Self::TickExhausted => write!(formatter, "tick counter is exhausted"),
        }
    }
}

impl std::error::Error for WorldError {}

impl From<crate::protocol::ProtocolError> for WorldError {
    fn from(error: crate::protocol::ProtocolError) -> Self {
        Self::InvalidInput(error)
    }
}

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
pub struct DemoWorld {
    tick: u64,
    players: BTreeMap<PlayerId, PlayerRecord>,
}

impl DemoWorld {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tick(&self) -> u64 {
        self.tick
    }

    pub fn player_count(&self) -> usize {
        self.players.len()
    }

    pub fn add_player(&mut self, player_id: PlayerId) -> Result<SnapshotPlayer, WorldError> {
        if player_id == 0 {
            return Err(WorldError::InvalidPlayerId);
        }
        if self.players.contains_key(&player_id) {
            return Err(WorldError::DuplicatePlayer(player_id));
        }
        if self.players.len() >= MAX_PLAYERS {
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
        Ok(SnapshotPlayer {
            player_id,
            x,
            y,
            last_applied_sequence: 0,
        })
    }

    pub fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.players.remove(&player_id).is_some()
    }

    pub fn submit_input(
        &mut self,
        player_id: PlayerId,
        input: InputCommand,
    ) -> Result<SubmitOutcome, WorldError> {
        validate_input(input)?;
        let player = self
            .players
            .get_mut(&player_id)
            .ok_or(WorldError::UnknownPlayer(player_id))?;
        if input.sequence <= player.last_received_sequence {
            return Ok(SubmitOutcome::IgnoredStale);
        }
        player.horizontal = input.horizontal;
        player.vertical = input.vertical;
        player.last_received_sequence = input.sequence;
        Ok(SubmitOutcome::Accepted)
    }

    pub fn advance_tick(&mut self) -> Result<Snapshot, WorldError> {
        self.tick = self.tick.checked_add(1).ok_or(WorldError::TickExhausted)?;
        for player in self.players.values_mut() {
            let next_x = player.x as i32 + i32::from(player.horizontal) * i32::from(STEP_UNITS);
            let next_y = player.y as i32 + i32::from(player.vertical) * i32::from(STEP_UNITS);
            player.x = next_x.clamp(i32::from(-WORLD_LIMIT), i32::from(WORLD_LIMIT)) as i16;
            player.y = next_y.clamp(i32::from(-WORLD_LIMIT), i32::from(WORLD_LIMIT)) as i16;
            player.last_applied_sequence = player.last_received_sequence;
        }
        Ok(self.snapshot())
    }

    pub fn snapshot(&self) -> Snapshot {
        let players = self
            .players
            .iter()
            .map(|(&player_id, player)| SnapshotPlayer {
                player_id,
                x: player.x,
                y: player.y,
                last_applied_sequence: player.last_applied_sequence,
            })
            .collect::<Vec<_>>();
        Snapshot {
            tick: self.tick,
            state_hash: snapshot_hash(self.tick, &players),
            players,
        }
    }
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
    fn duplicate_and_stale_inputs_are_deterministic() {
        let mut world = DemoWorld::new();
        world.add_player(1).unwrap();
        let input = InputCommand {
            sequence: 1,
            horizontal: 1,
            vertical: 0,
        };
        assert_eq!(world.submit_input(1, input).unwrap(), SubmitOutcome::Accepted);
        assert_eq!(
            world.submit_input(1, input).unwrap(),
            SubmitOutcome::IgnoredStale
        );
        let snapshot = world.advance_tick().unwrap();
        assert_eq!(snapshot.players[0].last_applied_sequence, 1);
    }

    #[test]
    fn replaying_the_same_inputs_produces_the_same_hash() {
        fn run() -> Snapshot {
            let mut world = DemoWorld::new();
            world.add_player(1).unwrap();
            world.add_player(2).unwrap();
            world
                .submit_input(
                    1,
                    InputCommand {
                        sequence: 1,
                        horizontal: 1,
                        vertical: 0,
                    },
                )
                .unwrap();
            world
                .submit_input(
                    2,
                    InputCommand {
                        sequence: 1,
                        horizontal: 0,
                        vertical: -1,
                    },
                )
                .unwrap();
            for _ in 0..10 {
                world.advance_tick().unwrap();
            }
            world.snapshot()
        }

        assert_eq!(run(), run());
    }

    #[test]
    fn capacity_is_bounded() {
        let mut world = DemoWorld::new();
        for player_id in 1..=MAX_PLAYERS as u32 {
            world.add_player(player_id).unwrap();
        }
        assert_eq!(world.add_player(99), Err(WorldError::PlayerCapacity));
    }
}
