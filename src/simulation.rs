use crate::PlayerId;
use crate::protocol::snapshot_hash;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationError {
    message: String,
}

impl SimulationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SimulationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationSnapshot {
    pub tick: u64,
    pub state_hash: u64,
    pub payload: Vec<u8>,
}

impl SimulationSnapshot {
    pub fn new(tick: u64, payload: Vec<u8>) -> Self {
        Self {
            tick,
            state_hash: snapshot_hash(tick, &payload),
            payload,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotScope {
    Shared,
    PlayerScoped,
}

pub trait GameSimulation: Send + 'static {
    fn tick_hz(&self) -> u16;
    fn max_players(&self) -> usize;
    fn current_tick(&self) -> u64;
    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError>;
    fn remove_player(&mut self, player_id: PlayerId) -> bool;
    fn apply_command(
        &mut self,
        player_id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError>;
    fn advance_tick(&mut self) -> Result<(), SimulationError>;

    fn snapshot_scope(&self) -> SnapshotScope {
        SnapshotScope::Shared
    }

    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError>;

    fn snapshot_for(&self, _player_id: PlayerId) -> Result<SimulationSnapshot, SimulationError> {
        Err(SimulationError::new(
            "player-scoped snapshots require an explicit per-player projection",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct PlayerScopedWithoutProjection {
        tick: u64,
    }

    impl GameSimulation for PlayerScopedWithoutProjection {
        fn tick_hz(&self) -> u16 {
            20
        }

        fn max_players(&self) -> usize {
            1
        }

        fn current_tick(&self) -> u64 {
            self.tick
        }

        fn add_player(&mut self, _player_id: PlayerId) -> Result<(), SimulationError> {
            Ok(())
        }

        fn remove_player(&mut self, _player_id: PlayerId) -> bool {
            true
        }

        fn apply_command(
            &mut self,
            _player_id: PlayerId,
            _sequence: u32,
            _payload: &[u8],
        ) -> Result<(), SimulationError> {
            Ok(())
        }

        fn advance_tick(&mut self) -> Result<(), SimulationError> {
            self.tick += 1;
            Ok(())
        }

        fn snapshot_scope(&self) -> SnapshotScope {
            SnapshotScope::PlayerScoped
        }

        fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
            Ok(SimulationSnapshot::new(
                self.tick,
                b"canonical-private-state".to_vec(),
            ))
        }
    }

    #[test]
    fn player_scoped_default_projection_fails_closed() {
        let simulation = PlayerScopedWithoutProjection::default();

        let error = simulation.snapshot_for(1).unwrap_err();

        assert_eq!(
            error.to_string(),
            "player-scoped snapshots require an explicit per-player projection"
        );
    }
}
