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
    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError>;
}
