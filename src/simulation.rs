use crate::PlayerId;
use crate::protocol::{MAX_COMMAND_REJECTION_PAYLOAD_BYTES, ProtocolError, snapshot_hash};
use std::fmt;

#[derive(Clone, Eq, PartialEq)]
pub struct SimulationError {
    message: String,
    rejection: Option<Vec<u8>>,
}

impl SimulationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            rejection: None,
        }
    }
}

impl SimulationError {
    /// A recoverable private rejection. The simulation must leave canonical state unchanged.
    /// Diagnostics deliberately omit the payload; runtime sequence/replay admission still fails.
    pub fn command_rejected(payload: Vec<u8>) -> Result<Self, ProtocolError> {
        if payload.len() > MAX_COMMAND_REJECTION_PAYLOAD_BYTES {
            return Err(ProtocolError::PayloadTooLarge {
                maximum: MAX_COMMAND_REJECTION_PAYLOAD_BYTES,
                actual: payload.len(),
            });
        }
        Ok(Self {
            message: "command rejected".into(),
            rejection: Some(payload),
        })
    }
    pub fn command_rejection(&self) -> Option<&[u8]> {
        self.rejection.as_deref()
    }
}
impl fmt::Debug for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SimulationError")
            .field("message", &self.message)
            .field("recoverable", &self.rejection.is_some())
            .finish()
    }
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SimulationError {}

#[derive(Clone, Eq, PartialEq)]
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

/// Runtime-owned facts for a player-facing projection, borrowed under its lock.
/// No credentials, epochs, or mutable session access cross this boundary.
#[derive(Clone, Copy)]
pub struct PlayerSnapshotContext<'a> {
    pub(crate) sessions: &'a crate::session::SessionRegistry,
}

impl PlayerSnapshotContext<'_> {
    /// Missing and grace-disconnected identities are both offline.
    pub fn is_connected(&self, player_id: PlayerId) -> bool {
        self.sessions.is_connected(player_id)
    }
}

pub trait GameSimulation: Send + 'static {
    fn tick_hz(&self) -> u16;
    fn max_players(&self) -> usize;
    fn current_tick(&self) -> u64;
    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError>;
    fn remove_player(&mut self, player_id: PlayerId) -> bool;

    /// Invalidate presentation-only state after an accepted disconnect or reconnect.
    /// This notification must not alter canonical snapshots or gameplay. It is
    /// intentionally infallible and is not included in the deterministic replay.
    fn connection_changed(&mut self, _player_id: PlayerId) {}

    fn try_remove_player(&mut self, player_id: PlayerId) -> Result<bool, SimulationError> {
        Ok(self.remove_player(player_id))
    }

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
    /// Presentation-only context; canonical replay snapshots remain independent.
    /// Implementations retain responsibility for recipient validation and private-state scoping.
    fn snapshot_for_with_context(
        &self,
        player_id: PlayerId,
        _context: PlayerSnapshotContext<'_>,
    ) -> Result<SimulationSnapshot, SimulationError> {
        self.snapshot_for(player_id)
    }
}

impl fmt::Debug for SimulationSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimulationSnapshot")
            .field("tick", &self.tick)
            .field("state_hash", &self.state_hash)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recoverable_command_errors_bound_private_payloads_and_omit_them_from_diagnostics() {
        let error = SimulationError::command_rejected(b"private-reason".to_vec()).unwrap();
        assert_eq!(
            error.command_rejection(),
            Some(b"private-reason".as_slice())
        );
        assert!(!format!("{error:?} {error}").contains("private-reason"));
        assert!(SimulationError::new("fatal").command_rejection().is_none());
        assert!(
            SimulationError::command_rejected(vec![0; MAX_COMMAND_REJECTION_PAYLOAD_BYTES + 1])
                .is_err()
        );
    }

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
