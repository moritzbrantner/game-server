use crate::session::{ReconnectToken, SessionError, SessionLease, SessionRegistry};
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot};
use crate::PlayerId;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    Applied,
    IgnoredStale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Session(SessionError),
    Simulation(SimulationError),
    StaleConnection,
    InvalidSequence,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => write!(formatter, "session error: {error}"),
            Self::Simulation(error) => write!(formatter, "simulation error: {error}"),
            Self::StaleConnection => write!(formatter, "connection no longer owns the player slot"),
            Self::InvalidSequence => write!(formatter, "command sequence must be non-zero"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<SessionError> for RuntimeError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

impl From<SimulationError> for RuntimeError {
    fn from(error: SimulationError) -> Self {
        Self::Simulation(error)
    }
}

#[derive(Clone, Debug)]
pub struct MatchRuntime<S> {
    simulation: S,
    sessions: SessionRegistry,
    last_sequences: BTreeMap<PlayerId, u32>,
}

impl<S: GameSimulation> MatchRuntime<S> {
    pub fn new(simulation: S, reconnect_grace_ticks: u64) -> Self {
        let max_players = simulation.max_players();
        Self {
            simulation,
            sessions: SessionRegistry::new(max_players, reconnect_grace_ticks),
            last_sequences: BTreeMap::new(),
        }
    }

    pub fn tick_hz(&self) -> u16 {
        self.simulation.tick_hz()
    }

    pub fn max_players(&self) -> usize {
        self.simulation.max_players()
    }

    pub fn current_tick(&self) -> u64 {
        self.simulation.current_tick()
    }

    pub fn slot_count(&self) -> usize {
        self.sessions.slot_count()
    }

    pub fn active_count(&self) -> usize {
        self.sessions.active_count()
    }

    pub fn admit(&mut self, token: ReconnectToken) -> Result<SessionLease, RuntimeError> {
        let lease = self.sessions.admit(token)?;
        if let Err(error) = self.simulation.add_player(lease.player_id) {
            self.sessions.remove_slot(lease.player_id);
            return Err(error.into());
        }
        self.last_sequences.insert(lease.player_id, 0);
        Ok(lease)
    }

    pub fn reconnect(
        &mut self,
        previous_token: ReconnectToken,
        replacement_token: ReconnectToken,
    ) -> Result<SessionLease, RuntimeError> {
        Ok(self.sessions.reconnect(
            previous_token,
            replacement_token,
            self.current_tick(),
        )?)
    }

    pub fn disconnect(&mut self, player_id: PlayerId, connection_epoch: u32) -> bool {
        self.sessions
            .disconnect(player_id, connection_epoch, self.current_tick())
    }

    pub fn submit_command(
        &mut self,
        player_id: PlayerId,
        connection_epoch: u32,
        sequence: u32,
        payload: &[u8],
    ) -> Result<CommandOutcome, RuntimeError> {
        if sequence == 0 {
            return Err(RuntimeError::InvalidSequence);
        }
        if !self
            .sessions
            .owns_connection(player_id, connection_epoch)
        {
            return Err(RuntimeError::StaleConnection);
        }
        let last_sequence = self
            .last_sequences
            .get_mut(&player_id)
            .ok_or(RuntimeError::StaleConnection)?;
        if sequence <= *last_sequence {
            return Ok(CommandOutcome::IgnoredStale);
        }
        self.simulation
            .apply_command(player_id, sequence, payload)?;
        *last_sequence = sequence;
        Ok(CommandOutcome::Applied)
    }

    pub fn advance_tick(&mut self) -> Result<SimulationSnapshot, RuntimeError> {
        let expired = self.sessions.expire(self.current_tick());
        for player_id in expired {
            self.simulation.remove_player(player_id);
            self.last_sequences.remove(&player_id);
        }
        self.simulation.advance_tick()?;
        Ok(self.simulation.snapshot()?)
    }

    pub fn snapshot(&self) -> Result<SimulationSnapshot, RuntimeError> {
        Ok(self.simulation.snapshot()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::SimulationSnapshot;

    #[derive(Clone, Debug, Default)]
    struct FakeSimulation {
        tick: u64,
        players: Vec<PlayerId>,
        commands: Vec<(PlayerId, u32, Vec<u8>)>,
    }

    impl GameSimulation for FakeSimulation {
        fn tick_hz(&self) -> u16 {
            20
        }

        fn max_players(&self) -> usize {
            2
        }

        fn current_tick(&self) -> u64 {
            self.tick
        }

        fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError> {
            self.players.push(player_id);
            Ok(())
        }

        fn remove_player(&mut self, player_id: PlayerId) -> bool {
            let before = self.players.len();
            self.players.retain(|candidate| *candidate != player_id);
            self.players.len() != before
        }

        fn apply_command(
            &mut self,
            player_id: PlayerId,
            sequence: u32,
            payload: &[u8],
        ) -> Result<(), SimulationError> {
            self.commands.push((player_id, sequence, payload.to_vec()));
            Ok(())
        }

        fn advance_tick(&mut self) -> Result<(), SimulationError> {
            self.tick += 1;
            Ok(())
        }

        fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
            Ok(SimulationSnapshot::new(self.tick, vec![self.players.len() as u8]))
        }
    }

    fn token(value: u8) -> ReconnectToken {
        ReconnectToken([value; crate::RECONNECT_TOKEN_BYTES])
    }

    #[test]
    fn runtime_owns_command_sequencing() {
        let mut runtime = MatchRuntime::new(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, b"a")
                .unwrap(),
            CommandOutcome::Applied
        );
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, b"duplicate")
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
    }

    #[test]
    fn stale_connection_cannot_submit_after_reconnect() {
        let mut runtime = MatchRuntime::new(FakeSimulation::default(), 10);
        let first = runtime.admit(token(1)).unwrap();
        assert!(runtime.disconnect(first.player_id, first.connection_epoch));
        let second = runtime.reconnect(token(1), token(2)).unwrap();
        assert_eq!(
            runtime.submit_command(first.player_id, first.connection_epoch, 1, b"stale"),
            Err(RuntimeError::StaleConnection)
        );
        assert_eq!(
            runtime
                .submit_command(second.player_id, second.connection_epoch, 1, b"fresh")
                .unwrap(),
            CommandOutcome::Applied
        );
    }
}
