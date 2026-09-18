use crate::PlayerId;
use crate::protocol::MAX_COMMAND_PAYLOAD_BYTES;
use crate::recovery::{RecoveryError, RecoveryImage};
use crate::replay::{ReplayLog, ReplayRecord};
use crate::session::{ReconnectToken, SessionError, SessionLease, SessionRegistry};
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot, SnapshotScope};
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
    CommandPayloadTooLarge { maximum: usize, actual: usize },
    Draining,
    Frozen,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => write!(formatter, "session error: {error}"),
            Self::Simulation(error) => write!(formatter, "simulation error: {error}"),
            Self::StaleConnection => write!(formatter, "connection no longer owns the player slot"),
            Self::InvalidSequence => write!(formatter, "command sequence must be non-zero"),
            Self::CommandPayloadTooLarge { maximum, actual } => write!(
                formatter,
                "command payload size {actual} exceeds maximum {maximum}"
            ),
            Self::Draining => write!(formatter, "match is draining and rejects new admissions"),
            Self::Frozen => write!(formatter, "match is frozen for recovery"),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeRecoveryError {
    ReplayCaptureDisabled,
    RuntimeNotFrozen,
    Recovery(RecoveryError),
}

impl fmt::Display for RuntimeRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplayCaptureDisabled => {
                write!(formatter, "runtime recovery requires replay capture")
            }
            Self::RuntimeNotFrozen => write!(formatter, "runtime must be frozen before recovery"),
            Self::Recovery(error) => write!(formatter, "recovery error: {error}"),
        }
    }
}

impl std::error::Error for RuntimeRecoveryError {}

impl From<RecoveryError> for RuntimeRecoveryError {
    fn from(error: RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

#[derive(Clone, Debug)]
pub struct MatchRuntime<S> {
    simulation: S,
    sessions: SessionRegistry,
    last_sequences: BTreeMap<PlayerId, u32>,
    replay: Option<ReplayLog>,
    reconnect_grace_ticks: u64,
    draining: bool,
    frozen: bool,
}

impl<S: GameSimulation> MatchRuntime<S> {
    pub fn new(simulation: S, reconnect_grace_ticks: u64) -> Self {
        Self::build(simulation, reconnect_grace_ticks, false)
    }

    pub fn new_with_replay_capture(simulation: S, reconnect_grace_ticks: u64) -> Self {
        Self::build(simulation, reconnect_grace_ticks, true)
    }

    pub fn restore_from_recovery(
        simulation: S,
        image: RecoveryImage,
    ) -> Result<Self, RuntimeRecoveryError> {
        let _ = image.encode()?;
        let max_players = simulation.max_players();
        let simulation = image.restore_simulation(simulation)?;
        let sessions = SessionRegistry::restore(
            max_players,
            image.reconnect_grace_ticks,
            image.current_tick,
            &image.sessions,
        )
        .map_err(RecoveryError::from)?;
        let last_sequences = image.last_sequences();
        Ok(Self {
            simulation,
            sessions,
            last_sequences,
            replay: Some(image.replay),
            reconnect_grace_ticks: image.reconnect_grace_ticks,
            draining: false,
            frozen: false,
        })
    }

    fn build(simulation: S, reconnect_grace_ticks: u64, capture_replay: bool) -> Self {
        let max_players = simulation.max_players();
        Self {
            simulation,
            sessions: SessionRegistry::new(max_players, reconnect_grace_ticks),
            last_sequences: BTreeMap::new(),
            replay: capture_replay.then(ReplayLog::default),
            reconnect_grace_ticks,
            draining: false,
            frozen: false,
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

    pub fn snapshot_scope(&self) -> SnapshotScope {
        self.simulation.snapshot_scope()
    }

    pub fn slot_count(&self) -> usize {
        self.sessions.slot_count()
    }

    pub fn active_count(&self) -> usize {
        self.sessions.active_count()
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub fn replay_log(&self) -> Option<&ReplayLog> {
        self.replay.as_ref()
    }

    pub fn begin_drain(&mut self) {
        self.draining = true;
    }

    pub fn freeze_for_recovery(&mut self) {
        self.draining = true;
        self.frozen = true;
    }

    pub fn resume_after_failed_recovery(&mut self) {
        self.frozen = false;
        self.draining = false;
    }

    pub fn recovery_image(&self) -> Result<RecoveryImage, RuntimeRecoveryError> {
        if !self.frozen {
            return Err(RuntimeRecoveryError::RuntimeNotFrozen);
        }
        let mut replay = self
            .replay
            .clone()
            .ok_or(RuntimeRecoveryError::ReplayCaptureDisabled)?;
        let snapshot = self.simulation.snapshot().map_err(RecoveryError::from)?;
        let checkpoint_matches = matches!(
            replay.records().last(),
            Some(ReplayRecord::Checkpoint { snapshot: previous }) if previous == &snapshot
        );
        if !checkpoint_matches {
            replay.append(ReplayRecord::Checkpoint {
                snapshot: snapshot.clone(),
            });
        }
        let image = RecoveryImage {
            current_tick: snapshot.tick,
            reconnect_grace_ticks: self.reconnect_grace_ticks,
            replay,
            sessions: self.sessions.recovery_snapshot(snapshot.tick),
        };
        let _ = image.encode()?;
        Ok(image)
    }

    pub fn admit(&mut self, token: ReconnectToken) -> Result<SessionLease, RuntimeError> {
        if self.frozen {
            return Err(RuntimeError::Frozen);
        }
        if self.draining {
            return Err(RuntimeError::Draining);
        }
        let lease = self.sessions.admit(token)?;
        if let Err(error) = self.simulation.add_player(lease.player_id) {
            self.sessions.remove_slot(lease.player_id);
            return Err(error.into());
        }
        self.last_sequences.insert(lease.player_id, 0);
        self.record(ReplayRecord::PlayerAdmitted {
            tick: self.current_tick(),
            player_id: lease.player_id,
        });
        Ok(lease)
    }

    pub fn reconnect(
        &mut self,
        previous_token: ReconnectToken,
        replacement_token: ReconnectToken,
    ) -> Result<SessionLease, RuntimeError> {
        if self.frozen {
            return Err(RuntimeError::Frozen);
        }
        let current_tick = self.current_tick();
        Ok(self
            .sessions
            .reconnect(previous_token, replacement_token, current_tick)?)
    }

    pub fn disconnect(&mut self, player_id: PlayerId, connection_epoch: u32) -> bool {
        if self.frozen {
            return false;
        }
        let current_tick = self.current_tick();
        self.sessions
            .disconnect(player_id, connection_epoch, current_tick)
    }

    pub(crate) fn abort_admission(&mut self, player_id: PlayerId, connection_epoch: u32) -> bool {
        if self.frozen || !self.sessions.owns_connection(player_id, connection_epoch) {
            return false;
        }
        if !self.simulation.remove_player(player_id) {
            return false;
        }
        if !self.sessions.remove_slot(player_id) {
            return false;
        }
        self.last_sequences.remove(&player_id);
        self.record(ReplayRecord::PlayerRemoved {
            tick: self.current_tick(),
            player_id,
        });
        true
    }

    pub fn submit_command(
        &mut self,
        player_id: PlayerId,
        connection_epoch: u32,
        sequence: u32,
        payload: &[u8],
    ) -> Result<CommandOutcome, RuntimeError> {
        if self.frozen {
            return Err(RuntimeError::Frozen);
        }
        if sequence == 0 {
            return Err(RuntimeError::InvalidSequence);
        }
        if !self.sessions.owns_connection(player_id, connection_epoch) {
            return Err(RuntimeError::StaleConnection);
        }
        let last_sequence = self
            .last_sequences
            .get(&player_id)
            .copied()
            .ok_or(RuntimeError::StaleConnection)?;
        if sequence <= last_sequence {
            return Ok(CommandOutcome::IgnoredStale);
        }
        if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
            return Err(RuntimeError::CommandPayloadTooLarge {
                maximum: MAX_COMMAND_PAYLOAD_BYTES,
                actual: payload.len(),
            });
        }
        self.simulation
            .apply_command(player_id, sequence, payload)?;
        self.last_sequences.insert(player_id, sequence);
        self.record(ReplayRecord::CommandApplied {
            tick: self.current_tick(),
            player_id,
            sequence,
            payload: payload.to_vec(),
        });
        Ok(CommandOutcome::Applied)
    }

    pub fn advance_tick(&mut self) -> Result<SimulationSnapshot, RuntimeError> {
        if self.frozen {
            return Err(RuntimeError::Frozen);
        }
        let current_tick = self.current_tick();
        let expired = self.sessions.expire(current_tick);
        for player_id in expired {
            self.simulation.remove_player(player_id);
            self.last_sequences.remove(&player_id);
            self.record(ReplayRecord::PlayerRemoved {
                tick: current_tick,
                player_id,
            });
        }
        self.simulation.advance_tick()?;
        let snapshot = self.simulation.snapshot()?;
        self.record(ReplayRecord::Checkpoint {
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    pub fn snapshot(&self) -> Result<SimulationSnapshot, RuntimeError> {
        Ok(self.simulation.snapshot()?)
    }

    pub fn snapshot_for(&self, player_id: PlayerId) -> Result<SimulationSnapshot, RuntimeError> {
        Ok(self.simulation.snapshot_for(player_id)?)
    }

    fn record(&mut self, record: ReplayRecord) {
        if let Some(replay) = &mut self.replay {
            replay.append(record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{ReplayRecord, verify_replay};
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
            let mut payload = vec![self.players.len() as u8];
            for (player_id, sequence, command) in &self.commands {
                payload.extend_from_slice(&player_id.to_be_bytes());
                payload.extend_from_slice(&sequence.to_be_bytes());
                payload.extend_from_slice(command);
            }
            Ok(SimulationSnapshot::new(self.tick, payload))
        }
    }

    fn token(value: u8) -> ReconnectToken {
        ReconnectToken([value; crate::RECONNECT_TOKEN_BYTES])
    }

    #[derive(Clone, Debug, Default)]
    struct PrivateSimulation {
        tick: u64,
        secrets: BTreeMap<PlayerId, u8>,
    }

    impl GameSimulation for PrivateSimulation {
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
            self.secrets.insert(player_id, 0);
            Ok(())
        }

        fn remove_player(&mut self, player_id: PlayerId) -> bool {
            self.secrets.remove(&player_id).is_some()
        }

        fn apply_command(
            &mut self,
            player_id: PlayerId,
            _sequence: u32,
            payload: &[u8],
        ) -> Result<(), SimulationError> {
            let secret = payload
                .first()
                .copied()
                .ok_or_else(|| SimulationError::new("private command must carry one byte"))?;
            let slot = self
                .secrets
                .get_mut(&player_id)
                .ok_or_else(|| SimulationError::new("unknown private player"))?;
            *slot = secret;
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
                self.secrets.values().copied().collect(),
            ))
        }

        fn snapshot_for(&self, player_id: PlayerId) -> Result<SimulationSnapshot, SimulationError> {
            let secret = self
                .secrets
                .get(&player_id)
                .copied()
                .ok_or_else(|| SimulationError::new("unknown private player"))?;
            Ok(SimulationSnapshot::new(self.tick, vec![secret]))
        }
    }

    #[test]
    fn player_scoped_snapshots_keep_canonical_state_off_the_client_projection() {
        let mut runtime = MatchRuntime::new(PrivateSimulation::default(), 10);
        let first = runtime.admit(token(1)).unwrap();
        let second = runtime.admit(token(2)).unwrap();

        runtime
            .submit_command(first.player_id, first.connection_epoch, 1, &[11])
            .unwrap();
        runtime
            .submit_command(second.player_id, second.connection_epoch, 1, &[22])
            .unwrap();

        assert_eq!(runtime.snapshot_scope(), SnapshotScope::PlayerScoped);
        assert_eq!(runtime.snapshot().unwrap().payload, vec![11, 22]);
        assert_eq!(
            runtime.snapshot_for(first.player_id).unwrap().payload,
            vec![11]
        );
        assert_eq!(
            runtime.snapshot_for(second.player_id).unwrap().payload,
            vec![22]
        );
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
        let oversized_stale = vec![0_u8; MAX_COMMAND_PAYLOAD_BYTES + 1];
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, &oversized_stale,)
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
    }

    #[test]
    fn oversized_new_command_is_rejected_before_simulation_or_replay_mutation() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();
        let oversized = vec![0_u8; MAX_COMMAND_PAYLOAD_BYTES + 1];

        assert_eq!(
            runtime.submit_command(lease.player_id, lease.connection_epoch, 1, &oversized,),
            Err(RuntimeError::CommandPayloadTooLarge {
                maximum: MAX_COMMAND_PAYLOAD_BYTES,
                actual: MAX_COMMAND_PAYLOAD_BYTES + 1,
            })
        );
        assert!(runtime.simulation.commands.is_empty());
        let replay = runtime.replay_log().unwrap();
        assert_eq!(replay.records().len(), 1);
        assert!(replay.encode().is_ok());
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

    #[test]
    fn replay_capture_records_only_applied_commands_and_checkpoints() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, b"accepted")
                .unwrap(),
            CommandOutcome::Applied
        );
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, b"stale")
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
        let expected = runtime.advance_tick().unwrap();

        let replay = runtime.replay_log().unwrap();
        assert_eq!(replay.records().len(), 3);
        assert!(matches!(
            replay.records()[0],
            ReplayRecord::PlayerAdmitted { player_id: 1, .. }
        ));
        assert!(matches!(
            replay.records()[1],
            ReplayRecord::CommandApplied {
                player_id: 1,
                sequence: 1,
                ..
            }
        ));
        assert_eq!(
            replay.records()[2],
            ReplayRecord::Checkpoint {
                snapshot: expected.clone()
            }
        );

        let verification = verify_replay(FakeSimulation::default(), replay).unwrap();
        assert_eq!(verification.checkpoints_verified, 1);
        assert_eq!(verification.final_snapshot, expected);
    }

    #[test]
    fn replay_capture_records_expired_player_before_next_tick() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 1);
        let lease = runtime.admit(token(1)).unwrap();
        assert!(runtime.disconnect(lease.player_id, lease.connection_epoch));
        runtime.advance_tick().unwrap();
        runtime.advance_tick().unwrap();
        runtime.advance_tick().unwrap();

        assert!(
            runtime
                .replay_log()
                .unwrap()
                .records()
                .iter()
                .any(|record| {
                    matches!(
                        record,
                        ReplayRecord::PlayerRemoved {
                            tick: 2,
                            player_id: 1
                        }
                    )
                })
        );
        verify_replay(FakeSimulation::default(), runtime.replay_log().unwrap()).unwrap();
    }

    #[test]
    fn draining_blocks_new_admission_but_still_allows_reconnect() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let first = runtime.admit(token(1)).unwrap();
        assert!(runtime.disconnect(first.player_id, first.connection_epoch));
        runtime.begin_drain();
        assert_eq!(runtime.admit(token(2)), Err(RuntimeError::Draining));
        assert_eq!(
            runtime.reconnect(token(1), token(3)).unwrap().player_id,
            first.player_id
        );
    }

    #[test]
    fn aborted_admission_releases_capacity_and_keeps_replay_valid() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();

        assert!(runtime.abort_admission(lease.player_id, lease.connection_epoch));
        assert_eq!(runtime.slot_count(), 0);
        assert_eq!(runtime.active_count(), 0);
        assert!(runtime.simulation.players.is_empty());

        runtime.advance_tick().unwrap();
        verify_replay(FakeSimulation::default(), runtime.replay_log().unwrap()).unwrap();
    }

    #[test]
    fn failed_recovery_can_resume_runtime_without_state_loss() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();
        runtime.freeze_for_recovery();
        runtime.resume_after_failed_recovery();
        assert!(!runtime.is_draining());
        assert!(!runtime.is_frozen());
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, b"resume")
                .unwrap(),
            CommandOutcome::Applied
        );
    }

    #[test]
    fn frozen_runtime_round_trips_through_recovery() {
        let mut runtime = MatchRuntime::new_with_replay_capture(FakeSimulation::default(), 10);
        let lease = runtime.admit(token(1)).unwrap();
        runtime
            .submit_command(lease.player_id, lease.connection_epoch, 1, b"move")
            .unwrap();
        let expected = runtime.advance_tick().unwrap();
        runtime.freeze_for_recovery();
        assert_eq!(
            runtime.submit_command(lease.player_id, lease.connection_epoch, 2, b"late"),
            Err(RuntimeError::Frozen)
        );
        assert_eq!(runtime.advance_tick(), Err(RuntimeError::Frozen));

        let image = runtime.recovery_image().unwrap();
        let mut restored =
            MatchRuntime::restore_from_recovery(FakeSimulation::default(), image).unwrap();
        assert_eq!(restored.snapshot().unwrap(), expected);
        assert_eq!(restored.active_count(), 0);
        let reconnected = restored.reconnect(token(1), token(2)).unwrap();
        assert_eq!(reconnected.player_id, lease.player_id);
        assert_eq!(reconnected.connection_epoch, lease.connection_epoch + 1);
        assert_eq!(
            restored
                .submit_command(
                    reconnected.player_id,
                    reconnected.connection_epoch,
                    1,
                    b"stale"
                )
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
        assert_eq!(
            restored
                .submit_command(
                    reconnected.player_id,
                    reconnected.connection_epoch,
                    2,
                    b"new"
                )
                .unwrap(),
            CommandOutcome::Applied
        );
    }
}
