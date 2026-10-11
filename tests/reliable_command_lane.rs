//! Acceptance contract for the reliable ordered command lane (moritzbrantner/game-server#43).
//!
//! These tests use only the public crate API. The lane adds this surface beside the
//! unchanged datagram path:
//!
//! ```text
//! // crate root (re-exported from `protocol`)
//! #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
//! pub enum CommandLane { Datagram, Reliable }          // exhaustive, not #[non_exhaustive]
//!
//! // runtime: `submit_command` stays the Datagram lane
//! impl<S: GameSimulation> MatchRuntime<S> {
//!     pub fn submit_lane_command(&mut self, player_id: PlayerId, connection_epoch: u32,
//!         lane: CommandLane, sequence: u32, payload: &[u8])
//!         -> Result<CommandOutcome, RuntimeError>;
//! }
//! // A command at or below the lane's watermark returns Ok(CommandOutcome::IgnoredStale)
//! // on either lane; the transport acknowledges that as `Duplicate` on the reliable lane.
//!
//! // simulation hook; the runtime and replay always call it, with the lane
//! trait GameSimulation {
//!     fn apply_lane_command(&mut self, player_id: PlayerId, lane: CommandLane,
//!         sequence: u32, payload: &[u8]) -> Result<(), SimulationError> {
//!         self.apply_command(player_id, sequence, payload)
//!     }
//! }
//!
//! // replay
//! ReplayRecord::CommandApplied { tick, player_id, lane: CommandLane, sequence, payload }
//! REPLAY_FORMAT_VERSION == 3; version 1 and 2 encodings decode as CommandLane::Datagram.
//!
//! // protocol (crate root)
//! pub const COMMAND_LANE_FORMAT_VERSION: u8;
//! pub const COMMAND_LANE_HEADER_BYTES: usize;
//! pub const MAX_COMMAND_LANES_PER_CONNECTION: usize;   // == 1
//! pub fn encode_command_lane_header() -> [u8; COMMAND_LANE_HEADER_BYTES];
//! pub fn decode_command_lane_header(bytes: &[u8]) -> Result<(), ProtocolError>;
//! pub const RELIABLE_COMMAND_ACK_BYTES: usize;          // fixed-size ack frame
//! #[derive(Clone, Copy, Debug, Eq, PartialEq)]
//! pub enum ReliableCommandOutcome { Applied, Duplicate, Rejected }
//! #[derive(Clone, Copy, Debug, Eq, PartialEq)]
//! pub struct ReliableCommandAck { pub sequence: u32, pub outcome: ReliableCommandOutcome }
//! pub fn encode_reliable_command_ack(ack: ReliableCommandAck) -> Result<Vec<u8>, ProtocolError>;
//! pub fn decode_reliable_command_ack(bytes: &[u8]) -> Result<ReliableCommandAck, ProtocolError>;
//!
//! // browser contract: new BrowserProtocolContract fields
//! command_lane_format_version: u8, command_lane_header_bytes: usize,
//! reliable_command_ack_bytes: usize, max_command_lanes_per_connection: usize
//! ```
//!
//! Transport-level acceptance (one lane per connection, ack frames over a real
//! WebTransport session, stalled lanes, datagram loss) lives in
//! `src/command_lane_acceptance.rs` because it needs the crate-internal connection
//! harness that the existing real-network tests use.

use game_server::{
    BROWSER_PROTOCOL_CONTRACT, COMMAND_LANE_FORMAT_VERSION, COMMAND_LANE_HEADER_BYTES, CommandLane,
    CommandOutcome, GameSimulation, MAX_COMMAND_LANES_PER_CONNECTION, MAX_COMMAND_PAYLOAD_BYTES,
    MatchRuntime, PlayerId, ProtocolError, RECONNECT_TOKEN_BYTES, RELIABLE_COMMAND_ACK_BYTES,
    REPLAY_FORMAT_VERSION, ReconnectToken, RecoveryImage, ReliableCommandAck,
    ReliableCommandOutcome, ReplayLog, ReplayRecord, RuntimeError, SimulationError,
    SimulationSnapshot, decode_command, decode_command_lane_header, decode_command_rejection,
    decode_control_request, decode_reliable_command_ack, encode_command,
    encode_command_lane_header, encode_command_rejection, encode_control_request,
    encode_reliable_command_ack, verify_replay,
};
use std::collections::BTreeSet;

type Applied = (PlayerId, CommandLane, u32, Vec<u8>);

/// Deterministic simulation that records the lane of every applied command in its
/// canonical snapshot, so replay and recovery prove that the lane is carried through.
///
/// Payload rules: `b"fatal"` is a fatal simulation error, `b"needs-unlock"` is a
/// recoverable private rejection until that player applied `b"unlock"`.
#[derive(Clone, Debug, Default)]
struct LaneSimulation {
    tick: u64,
    players: BTreeSet<PlayerId>,
    unlocked: BTreeSet<PlayerId>,
    applied: Vec<Applied>,
}

impl GameSimulation for LaneSimulation {
    fn tick_hz(&self) -> u16 {
        20
    }

    fn max_players(&self) -> usize {
        4
    }

    fn current_tick(&self) -> u64 {
        self.tick
    }

    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError> {
        if self.players.insert(player_id) {
            Ok(())
        } else {
            Err(SimulationError::new("duplicate player"))
        }
    }

    fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.unlocked.remove(&player_id);
        self.players.remove(&player_id)
    }

    fn apply_command(
        &mut self,
        player_id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        // Only the datagram lane may reach the lane-less entry point.
        self.apply_lane_command(player_id, CommandLane::Datagram, sequence, payload)
    }

    fn apply_lane_command(
        &mut self,
        player_id: PlayerId,
        lane: CommandLane,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        if !self.players.contains(&player_id) {
            return Err(SimulationError::new("unknown player"));
        }
        match payload {
            b"fatal" => return Err(SimulationError::new("fatal command")),
            b"needs-unlock" if !self.unlocked.contains(&player_id) => {
                return Err(SimulationError::command_rejected(b"locked".to_vec()).unwrap());
            }
            b"unlock" => {
                self.unlocked.insert(player_id);
            }
            _ => {}
        }
        self.applied
            .push((player_id, lane, sequence, payload.to_vec()));
        Ok(())
    }

    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        self.tick += 1;
        Ok(())
    }

    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        Ok(SimulationSnapshot::new(self.tick, encode_state(self)))
    }
}

fn lane_byte(lane: CommandLane) -> u8 {
    match lane {
        CommandLane::Datagram => 0,
        CommandLane::Reliable => 1,
    }
}

fn encode_state(simulation: &LaneSimulation) -> Vec<u8> {
    let mut bytes = Vec::new();
    for set in [&simulation.players, &simulation.unlocked] {
        bytes.push(u8::try_from(set.len()).unwrap());
        for player_id in set {
            bytes.extend_from_slice(&player_id.to_be_bytes());
        }
    }
    bytes.extend_from_slice(
        &u16::try_from(simulation.applied.len())
            .unwrap()
            .to_be_bytes(),
    );
    for (player_id, lane, sequence, payload) in &simulation.applied {
        bytes.extend_from_slice(&player_id.to_be_bytes());
        bytes.push(lane_byte(*lane));
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(payload);
    }
    bytes
}

/// Decodes the applied-command list from a `LaneSimulation` canonical snapshot.
fn applied_commands(snapshot: &SimulationSnapshot) -> Vec<Applied> {
    let bytes = &snapshot.payload;
    let mut offset = 0;
    for _ in 0..2 {
        let count = usize::from(bytes[offset]);
        offset += 1 + 4 * count;
    }
    let count = usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
    offset += 2;
    let mut applied = Vec::with_capacity(count);
    for _ in 0..count {
        let player_id = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let lane = match bytes[offset + 4] {
            0 => CommandLane::Datagram,
            1 => CommandLane::Reliable,
            other => panic!("unknown lane byte {other}"),
        };
        let sequence = u32::from_be_bytes(bytes[offset + 5..offset + 9].try_into().unwrap());
        let len = usize::from(u16::from_be_bytes([bytes[offset + 9], bytes[offset + 10]]));
        offset += 11;
        applied.push((
            player_id,
            lane,
            sequence,
            bytes[offset..offset + len].to_vec(),
        ));
        offset += len;
    }
    assert_eq!(offset, bytes.len(), "trailing snapshot bytes");
    applied
}

fn applied_of<S: GameSimulation>(runtime: &MatchRuntime<S>) -> Vec<Applied> {
    applied_commands(&runtime.snapshot().unwrap())
}

fn token(value: u8) -> ReconnectToken {
    ReconnectToken([value; RECONNECT_TOKEN_BYTES])
}

fn replayed_commands(log: &ReplayLog) -> Vec<(CommandLane, u32, Vec<u8>)> {
    log.records()
        .iter()
        .filter_map(|record| match record {
            ReplayRecord::CommandApplied {
                lane,
                sequence,
                payload,
                ..
            } => Some((*lane, *sequence, payload.clone())),
            _ => None,
        })
        .collect()
}

const DATAGRAM: CommandLane = CommandLane::Datagram;
const RELIABLE: CommandLane = CommandLane::Reliable;

// ---------------------------------------------------------------------------
// Runtime: lanes, watermarks, duplicates, rejection
// ---------------------------------------------------------------------------

#[test]
fn reliable_command_overtaken_by_later_datagram_commands_is_still_applied() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);

    // The client used one counter: one-shot 7 on the reliable lane, held state
    // 8..=10 on datagrams. The datagrams arrive first.
    for sequence in 8..=10 {
        assert_eq!(
            runtime.submit_lane_command(player, epoch, DATAGRAM, sequence, b"held"),
            Ok(CommandOutcome::Applied)
        );
    }
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 7, b"one-shot"),
        Ok(CommandOutcome::Applied),
        "a reliable command must not be made stale by higher datagram sequences"
    );
    // The datagram lane itself is unchanged: lower datagram sequences stay stale.
    assert_eq!(
        runtime.submit_lane_command(player, epoch, DATAGRAM, 7, b"late-held"),
        Ok(CommandOutcome::IgnoredStale)
    );

    let applied = applied_of(&runtime);
    assert_eq!(
        applied.last(),
        Some(&(player, RELIABLE, 7, b"one-shot".to_vec())),
        "the simulation must observe the reliable lane"
    );
    assert_eq!(applied.len(), 4);

    runtime.advance_tick().unwrap();
    let verification = verify_replay(LaneSimulation::default(), runtime.replay_log().unwrap())
        .expect("interleaved lane sequences are valid history");
    assert_eq!(verification.final_snapshot, runtime.snapshot().unwrap());
}

#[test]
fn submit_command_is_the_unchanged_datagram_lane() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);

    assert_eq!(
        runtime.submit_command(player, epoch, 3, b"a"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, DATAGRAM, 3, b"b"),
        Ok(CommandOutcome::IgnoredStale),
        "submit_command and the Datagram lane share one watermark"
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, DATAGRAM, 4, b"c"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_command(player, epoch, 4, b"d"),
        Ok(CommandOutcome::IgnoredStale)
    );
    assert_eq!(
        applied_of(&runtime),
        vec![
            (player, DATAGRAM, 3, b"a".to_vec()),
            (player, DATAGRAM, 4, b"c".to_vec()),
        ]
    );
    assert_eq!(
        replayed_commands(runtime.replay_log().unwrap()),
        vec![(DATAGRAM, 3, b"a".to_vec()), (DATAGRAM, 4, b"c".to_vec())]
    );
}

#[test]
fn lane_watermarks_are_independent_per_player() {
    let mut runtime = MatchRuntime::new(LaneSimulation::default(), 10);
    let first = runtime.admit(token(1)).unwrap();
    let second = runtime.admit(token(2)).unwrap();
    let (p1, e1) = (first.player_id, first.connection_epoch);
    let (p2, e2) = (second.player_id, second.connection_epoch);

    let steps: [(PlayerId, u32, CommandLane, u32, CommandOutcome); 11] = [
        (p1, e1, DATAGRAM, 10, CommandOutcome::Applied),
        (p1, e1, RELIABLE, 1, CommandOutcome::Applied),
        (p1, e1, DATAGRAM, 5, CommandOutcome::IgnoredStale),
        (p1, e1, RELIABLE, 2, CommandOutcome::Applied),
        (p1, e1, RELIABLE, 2, CommandOutcome::IgnoredStale),
        (p1, e1, RELIABLE, 1, CommandOutcome::IgnoredStale),
        (p1, e1, DATAGRAM, 11, CommandOutcome::Applied),
        (p1, e1, RELIABLE, 20, CommandOutcome::Applied),
        (p1, e1, DATAGRAM, 12, CommandOutcome::Applied),
        // Another player's reliable watermark is untouched by player 1.
        (p2, e2, RELIABLE, 1, CommandOutcome::Applied),
        (p2, e2, DATAGRAM, 1, CommandOutcome::Applied),
    ];
    for (player, epoch, lane, sequence, expected) in steps {
        assert_eq!(
            runtime.submit_lane_command(player, epoch, lane, sequence, b"x"),
            Ok(expected),
            "player {player} {lane:?} sequence {sequence}"
        );
    }
    let applied: Vec<_> = applied_of(&runtime)
        .into_iter()
        .map(|(player, lane, sequence, _)| (player, lane, sequence))
        .collect();
    assert_eq!(
        applied,
        vec![
            (p1, DATAGRAM, 10),
            (p1, RELIABLE, 1),
            (p1, RELIABLE, 2),
            (p1, DATAGRAM, 11),
            (p1, RELIABLE, 20),
            (p1, DATAGRAM, 12),
            (p2, RELIABLE, 1),
            (p2, DATAGRAM, 1),
        ]
    );
}

#[test]
fn reliable_resend_after_reconnect_is_applied_once_and_reported_duplicate() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let first = runtime.admit(token(1)).unwrap();
    let player = first.player_id;
    assert_eq!(
        runtime.submit_lane_command(player, first.connection_epoch, RELIABLE, 1, b"attack"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(player, first.connection_epoch, RELIABLE, 2, b"interact"),
        Ok(CommandOutcome::Applied)
    );
    runtime.advance_tick().unwrap();
    // The ack for sequence 2 was lost with the connection.
    assert!(runtime.disconnect(player, first.connection_epoch));
    runtime.advance_tick().unwrap();
    let second = runtime.reconnect(token(1), token(2)).unwrap();
    assert_eq!(second.player_id, player);

    // The old connection is fenced on the reliable lane, too.
    assert_eq!(
        runtime.submit_lane_command(player, first.connection_epoch, RELIABLE, 3, b"stale"),
        Err(RuntimeError::StaleConnection)
    );
    // The client re-sends every unacknowledged command on the new connection.
    assert_eq!(
        runtime.submit_lane_command(player, second.connection_epoch, RELIABLE, 2, b"interact"),
        Ok(CommandOutcome::IgnoredStale),
        "the reliable watermark must survive a reconnect within the grace window"
    );
    assert_eq!(
        runtime.submit_lane_command(player, second.connection_epoch, RELIABLE, 1, b"attack"),
        Ok(CommandOutcome::IgnoredStale)
    );
    assert_eq!(
        runtime.submit_lane_command(player, second.connection_epoch, RELIABLE, 3, b"swap"),
        Ok(CommandOutcome::Applied)
    );

    let reliable: Vec<_> = applied_of(&runtime)
        .into_iter()
        .filter(|(_, lane, _, _)| *lane == RELIABLE)
        .map(|(_, _, sequence, payload)| (sequence, payload))
        .collect();
    assert_eq!(
        reliable,
        vec![
            (1, b"attack".to_vec()),
            (2, b"interact".to_vec()),
            (3, b"swap".to_vec()),
        ],
        "each reliable command is applied exactly once"
    );
    runtime.advance_tick().unwrap();
    verify_replay(LaneSimulation::default(), runtime.replay_log().unwrap()).unwrap();
}

#[test]
fn rejected_reliable_command_consumes_neither_sequence_nor_replay_admission() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);
    let before_snapshot = runtime.snapshot().unwrap();
    let before_records = runtime.replay_log().unwrap().records().len();

    let error = runtime
        .submit_lane_command(player, epoch, RELIABLE, 1, b"needs-unlock")
        .unwrap_err();
    match &error {
        RuntimeError::Simulation(error) => {
            assert_eq!(error.command_rejection(), Some(b"locked".as_slice()));
        }
        other => panic!("expected a recoverable simulation rejection, got {other:?}"),
    }
    assert_eq!(runtime.snapshot().unwrap(), before_snapshot);
    assert_eq!(
        runtime.replay_log().unwrap().records().len(),
        before_records,
        "a rejection must not be admitted to replay"
    );

    // The rejection does not move the reliable watermark: once the simulation stops
    // rejecting, the same sequence is applied.
    assert_eq!(
        runtime.submit_lane_command(player, epoch, DATAGRAM, 1, b"unlock"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 1, b"needs-unlock"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 1, b"needs-unlock"),
        Ok(CommandOutcome::IgnoredStale)
    );
    assert_eq!(
        replayed_commands(runtime.replay_log().unwrap()),
        vec![
            (DATAGRAM, 1, b"unlock".to_vec()),
            (RELIABLE, 1, b"needs-unlock".to_vec()),
        ]
    );

    // A fatal simulation error stays an error and consumes nothing either.
    assert!(matches!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 2, b"fatal"),
        Err(RuntimeError::Simulation(error)) if error.command_rejection().is_none()
    ));
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 2, b"after-fatal"),
        Ok(CommandOutcome::Applied)
    );
}

#[test]
fn reliable_lane_enforces_runtime_bounds_without_consuming_the_sequence() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);

    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 0, b"zero"),
        Err(RuntimeError::InvalidSequence)
    );
    let oversized = vec![0_u8; MAX_COMMAND_PAYLOAD_BYTES + 1];
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 1, &oversized),
        Err(RuntimeError::CommandPayloadTooLarge {
            maximum: MAX_COMMAND_PAYLOAD_BYTES,
            actual: MAX_COMMAND_PAYLOAD_BYTES + 1,
        })
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch + 1, RELIABLE, 1, b"wrong-epoch"),
        Err(RuntimeError::StaleConnection)
    );
    assert_eq!(
        runtime.submit_lane_command(player + 7, epoch, RELIABLE, 1, b"unknown"),
        Err(RuntimeError::StaleConnection)
    );
    assert!(applied_of(&runtime).is_empty());
    let max_payload = vec![1_u8; MAX_COMMAND_PAYLOAD_BYTES];
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 1, &max_payload),
        Ok(CommandOutcome::Applied)
    );

    runtime.freeze_for_recovery();
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 2, b"frozen"),
        Err(RuntimeError::Frozen)
    );
    runtime.resume_after_failed_recovery();
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 2, b"resumed"),
        Ok(CommandOutcome::Applied)
    );
}

#[test]
fn reliable_watermark_is_dropped_only_with_the_removed_player() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 1);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 9, b"before-removal"),
        Ok(CommandOutcome::Applied)
    );
    assert!(runtime.disconnect(player, epoch));
    for _ in 0..3 {
        runtime.advance_tick().unwrap();
    }
    assert!(
        runtime
            .replay_log()
            .unwrap()
            .records()
            .iter()
            .any(|record| matches!(record, ReplayRecord::PlayerRemoved { player_id, .. } if *player_id == player)),
        "the grace window expired and removed the player"
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 10, b"after-removal"),
        Err(RuntimeError::StaleConnection)
    );
    assert_eq!(
        runtime
            .reconnect(token(1), token(2))
            .map(|lease| lease.player_id),
        Err(RuntimeError::Session(
            game_server::SessionError::UnknownToken
        ))
    );

    // A fresh admission starts both lanes at watermark zero.
    let fresh = runtime.admit(token(3)).unwrap();
    assert_ne!(fresh.player_id, player);
    assert_eq!(
        runtime.submit_lane_command(fresh.player_id, fresh.connection_epoch, RELIABLE, 1, b"r"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(fresh.player_id, fresh.connection_epoch, DATAGRAM, 1, b"d"),
        Ok(CommandOutcome::Applied)
    );
    runtime.advance_tick().unwrap();
    verify_replay(LaneSimulation::default(), runtime.replay_log().unwrap()).unwrap();
}

/// A simulation written before the lane existed: it implements only `apply_command`.
#[derive(Clone, Debug, Default)]
struct LanelessSimulation {
    tick: u64,
    players: BTreeSet<PlayerId>,
    commands: Vec<(PlayerId, u32, Vec<u8>)>,
}

impl GameSimulation for LanelessSimulation {
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
        self.players.insert(player_id);
        Ok(())
    }

    fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.players.remove(&player_id)
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
        let mut payload = Vec::new();
        for (player_id, sequence, command) in &self.commands {
            payload.extend_from_slice(&player_id.to_be_bytes());
            payload.extend_from_slice(&sequence.to_be_bytes());
            payload.extend_from_slice(command);
        }
        Ok(SimulationSnapshot::new(self.tick, payload))
    }
}

#[test]
fn default_lane_hook_delegates_to_apply_command_for_existing_games() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LanelessSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);
    assert_eq!(
        runtime.submit_lane_command(player, epoch, RELIABLE, 1, b"r"),
        Ok(CommandOutcome::Applied)
    );
    assert_eq!(
        runtime.submit_lane_command(player, epoch, DATAGRAM, 1, b"d"),
        Ok(CommandOutcome::Applied)
    );
    let mut expected = Vec::new();
    for (sequence, command) in [(1_u32, b"r"), (1, b"d")] {
        expected.extend_from_slice(&player.to_be_bytes());
        expected.extend_from_slice(&sequence.to_be_bytes());
        expected.extend_from_slice(command);
    }
    assert_eq!(runtime.snapshot().unwrap().payload, expected);
    runtime.advance_tick().unwrap();
    assert_eq!(
        verify_replay(LanelessSimulation::default(), runtime.replay_log().unwrap())
            .unwrap()
            .final_snapshot,
        runtime.snapshot().unwrap()
    );
}

// ---------------------------------------------------------------------------
// Replay and recovery
// ---------------------------------------------------------------------------

/// Builds a mixed-lane history with interleaving, duplicates, a rejection, ticks and
/// a reconnect. Returns the runtime with replay capture enabled.
fn mixed_lane_runtime() -> (MatchRuntime<LaneSimulation>, PlayerId) {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let other = runtime.admit(token(5)).unwrap();
    let player = lease.player_id;
    let epoch = lease.connection_epoch;
    runtime
        .submit_lane_command(player, epoch, DATAGRAM, 5, b"held-5")
        .unwrap();
    runtime
        .submit_lane_command(player, epoch, RELIABLE, 1, b"shot-1")
        .unwrap();
    runtime
        .submit_lane_command(player, epoch, RELIABLE, 2, b"needs-unlock")
        .unwrap_err();
    runtime.advance_tick().unwrap();
    runtime
        .submit_lane_command(player, epoch, DATAGRAM, 7, b"unlock")
        .unwrap();
    runtime
        .submit_lane_command(player, epoch, RELIABLE, 2, b"needs-unlock")
        .unwrap();
    runtime
        .submit_lane_command(other.player_id, other.connection_epoch, RELIABLE, 4, b"o-4")
        .unwrap();
    runtime.advance_tick().unwrap();
    assert!(runtime.disconnect(player, epoch));
    runtime.advance_tick().unwrap();
    let resumed = runtime.reconnect(token(1), token(2)).unwrap();
    assert_eq!(
        runtime.submit_lane_command(
            player,
            resumed.connection_epoch,
            RELIABLE,
            2,
            b"needs-unlock"
        ),
        Ok(CommandOutcome::IgnoredStale)
    );
    runtime
        .submit_lane_command(player, resumed.connection_epoch, RELIABLE, 3, b"shot-3")
        .unwrap();
    runtime
        .submit_lane_command(player, resumed.connection_epoch, DATAGRAM, 8, b"held-8")
        .unwrap();
    runtime.advance_tick().unwrap();
    (runtime, player)
}

#[test]
fn mixed_lane_history_round_trips_through_replay_encoding_and_verification() {
    let (runtime, _) = mixed_lane_runtime();
    let log = runtime.replay_log().unwrap();
    assert_eq!(
        replayed_commands(log),
        vec![
            (DATAGRAM, 5, b"held-5".to_vec()),
            (RELIABLE, 1, b"shot-1".to_vec()),
            (DATAGRAM, 7, b"unlock".to_vec()),
            (RELIABLE, 2, b"needs-unlock".to_vec()),
            (RELIABLE, 4, b"o-4".to_vec()),
            (RELIABLE, 3, b"shot-3".to_vec()),
            (DATAGRAM, 8, b"held-8".to_vec()),
        ]
    );

    assert_eq!(REPLAY_FORMAT_VERSION, 3);
    let encoded = log.encode().unwrap();
    assert_eq!(&encoded[..4], b"GSRP");
    assert_eq!(
        encoded[4], REPLAY_FORMAT_VERSION,
        "a log carrying reliable-lane commands uses the lane-aware replay version"
    );
    let decoded = ReplayLog::decode(&encoded).unwrap();
    assert_eq!(&decoded, log);

    let verification = verify_replay(LaneSimulation::default(), &decoded).unwrap();
    assert_eq!(verification.final_snapshot, runtime.snapshot().unwrap());
    // The lane reached the simulation during verification, not only the record.
    assert!(
        applied_commands(&verification.final_snapshot)
            .iter()
            .any(|(_, lane, sequence, _)| *lane == RELIABLE && *sequence == 3)
    );

    // Pre-lane versions cannot carry a reliable record: downgrading the version
    // byte must fail closed rather than silently reinterpreting the lane.
    for legacy in [1_u8, 2] {
        let mut downgraded = encoded.clone();
        downgraded[4] = legacy;
        assert!(
            ReplayLog::decode(&downgraded).is_err(),
            "version {legacy} must reject lane-aware records"
        );
    }
}

#[test]
fn recovery_restores_both_lane_watermarks() {
    let (mut runtime, player) = mixed_lane_runtime();
    // Watermarks for `player`: datagram 8, reliable 3.
    let expected = runtime.snapshot().unwrap();
    runtime.freeze_for_recovery();
    let bytes = runtime.recovery_image().unwrap().encode().unwrap();
    let mut restored = MatchRuntime::restore_from_recovery(
        LaneSimulation::default(),
        RecoveryImage::decode(&bytes).unwrap(),
    )
    .unwrap();
    assert_eq!(restored.snapshot().unwrap(), expected);
    let lease = restored.reconnect(token(2), token(3)).unwrap();
    assert_eq!(lease.player_id, player);
    let epoch = lease.connection_epoch;

    let steps: [(CommandLane, u32, CommandOutcome); 7] = [
        (RELIABLE, 3, CommandOutcome::IgnoredStale),
        (RELIABLE, 1, CommandOutcome::IgnoredStale),
        (DATAGRAM, 8, CommandOutcome::IgnoredStale),
        // Discriminates the lanes: 4 is above the reliable watermark (3) but below
        // the datagram watermark (8).
        (RELIABLE, 4, CommandOutcome::Applied),
        (DATAGRAM, 4, CommandOutcome::IgnoredStale),
        (DATAGRAM, 9, CommandOutcome::Applied),
        (RELIABLE, 5, CommandOutcome::Applied),
    ];
    for (lane, sequence, outcome) in steps {
        assert_eq!(
            restored.submit_lane_command(player, epoch, lane, sequence, b"after-recovery"),
            Ok(outcome),
            "{lane:?} sequence {sequence} after recovery"
        );
    }
    restored.advance_tick().unwrap();
    assert_eq!(
        verify_replay(LaneSimulation::default(), restored.replay_log().unwrap())
            .unwrap()
            .final_snapshot,
        restored.snapshot().unwrap()
    );
}

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

/// Version 1 replay bytes from origin/main (`replay_encoding_preserves_the_version_one_wire_fixture`):
/// admit player 7, command sequence 9 payload `aa`, checkpoint tick 1 payload `bb`, remove 7.
const VERSION_ONE_FIXTURE_HEX: &str = "475352500101000000000000000000000004000000070200000000000000000000000d000000070000000900000001aa0400000000000000010000000d35594afc4eab92da00000001bb0300000000000000010000000400000007";

/// The pre-lane record framing: kind, big-endian u64 tick, big-endian u32 body length, body.
fn legacy_record(output: &mut Vec<u8>, kind: u8, tick: u64, body: &[u8]) {
    output.push(kind);
    output.extend_from_slice(&tick.to_be_bytes());
    output.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
    output.extend_from_slice(body);
}

#[test]
fn pre_lane_replay_encodings_decode_as_datagram_commands() {
    let version_one = hex(VERSION_ONE_FIXTURE_HEX);
    let decoded = ReplayLog::decode(&version_one).unwrap();
    assert_eq!(
        decoded.records()[1],
        ReplayRecord::CommandApplied {
            tick: 0,
            player_id: 7,
            lane: CommandLane::Datagram,
            sequence: 9,
            payload: vec![0xaa],
        }
    );
    assert_eq!(
        decoded.encode().unwrap(),
        version_one,
        "datagram-only logs without digests keep the version-one encoding"
    );

    // Version 2 (digest checkpoints) with a pre-lane command record.
    let mut version_two = b"GSRP".to_vec();
    version_two.push(2);
    legacy_record(&mut version_two, 1, 0, &7_u32.to_be_bytes());
    let mut command = Vec::new();
    command.extend_from_slice(&7_u32.to_be_bytes());
    command.extend_from_slice(&11_u32.to_be_bytes());
    command.extend_from_slice(&2_u32.to_be_bytes());
    command.extend_from_slice(&[0xc0, 0xde]);
    legacy_record(&mut version_two, 2, 0, &command);
    legacy_record(&mut version_two, 5, 1, &[0x5a; 32]);
    let decoded = ReplayLog::decode(&version_two).unwrap();
    assert_eq!(
        decoded.records(),
        &[
            ReplayRecord::PlayerAdmitted {
                tick: 0,
                player_id: 7
            },
            ReplayRecord::CommandApplied {
                tick: 0,
                player_id: 7,
                lane: CommandLane::Datagram,
                sequence: 11,
                payload: vec![0xc0, 0xde],
            },
            ReplayRecord::DigestCheckpoint {
                tick: 1,
                digest: [0x5a; 32]
            },
        ]
    );
    assert_eq!(
        decoded.encode().unwrap(),
        version_two,
        "datagram-only logs with digests keep the version-two encoding"
    );
}

#[test]
fn verification_accepts_interleaved_lanes_with_equal_and_crossing_sequences() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);
    for (lane, sequence) in [
        (DATAGRAM, 5),
        (RELIABLE, 5),
        (DATAGRAM, 6),
        (RELIABLE, 1_000),
        (DATAGRAM, 7),
    ] {
        assert_eq!(
            runtime.submit_lane_command(player, epoch, lane, sequence, b"x"),
            Ok(CommandOutcome::Applied)
        );
    }
    runtime.advance_tick().unwrap();
    let encoded = runtime.replay_log().unwrap().encode().unwrap();
    let verification = verify_replay(
        LaneSimulation::default(),
        &ReplayLog::decode(&encoded).unwrap(),
    )
    .unwrap();
    assert_eq!(verification.final_snapshot, runtime.snapshot().unwrap());
}

// ---------------------------------------------------------------------------
// Protocol: lane header and ack frame
// ---------------------------------------------------------------------------

#[test]
fn lane_header_is_exact_versioned_and_distinct_from_control() {
    let header = encode_command_lane_header();
    assert_eq!(header.len(), COMMAND_LANE_HEADER_BYTES);
    const { assert!(COMMAND_LANE_HEADER_BYTES >= 4) };
    assert_ne!(
        &header[..4],
        b"GSCT",
        "must not collide with the control magic"
    );
    assert_eq!(decode_command_lane_header(&header), Ok(()));

    for length in 0..header.len() {
        assert!(
            decode_command_lane_header(&header[..length]).is_err(),
            "truncated header of {length} bytes accepted"
        );
    }
    let mut trailing = header.to_vec();
    trailing.push(0);
    assert!(decode_command_lane_header(&trailing).is_err());
    // The header is a constant: any single-byte change, including the format
    // version, must be rejected.
    for index in 0..header.len() {
        for value in 0..=u8::MAX {
            if value == header[index] {
                continue;
            }
            let mut mutated = header;
            mutated[index] = value;
            assert!(
                decode_command_lane_header(&mutated).is_err(),
                "mutated header byte {index} = {value:#04x} accepted"
            );
        }
    }

    // Control and lane streams are told apart by their first bytes.
    let control = encode_control_request(b"ping").unwrap();
    assert!(
        decode_command_lane_header(&control[..COMMAND_LANE_HEADER_BYTES.min(control.len())])
            .is_err()
    );
    assert!(decode_control_request(&header).is_err());
    assert!(decode_command(&header).is_err());
}

#[test]
fn reliable_command_ack_round_trips_every_outcome_at_a_fixed_size() {
    let outcomes = [
        ReliableCommandOutcome::Applied,
        ReliableCommandOutcome::Duplicate,
        ReliableCommandOutcome::Rejected,
    ];
    let mut encodings = BTreeSet::new();
    for outcome in outcomes {
        for sequence in [1, 2, 0x0102_0304, u32::MAX] {
            let ack = ReliableCommandAck { sequence, outcome };
            let bytes = encode_reliable_command_ack(ack).unwrap();
            assert_eq!(bytes.len(), RELIABLE_COMMAND_ACK_BYTES);
            assert_eq!(decode_reliable_command_ack(&bytes), Ok(ack));
            assert!(encodings.insert(bytes), "two acks share one encoding");
        }
    }
    assert_eq!(
        encode_reliable_command_ack(ReliableCommandAck {
            sequence: 0,
            outcome: ReliableCommandOutcome::Applied,
        }),
        Err(ProtocolError::InvalidSequence)
    );
}

#[test]
fn reliable_command_ack_decoding_fails_closed() {
    let ack = ReliableCommandAck {
        sequence: 0x0a0b_0c0d,
        outcome: ReliableCommandOutcome::Duplicate,
    };
    let bytes = encode_reliable_command_ack(ack).unwrap();

    for length in 0..bytes.len() {
        assert!(
            decode_reliable_command_ack(&bytes[..length]).is_err(),
            "truncated ack of {length} bytes accepted"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_reliable_command_ack(&trailing).is_err());

    // Decoding is canonical: a mutated frame is either rejected or is exactly the
    // encoding of the ack it decodes to. Unknown outcomes, versions, kinds and a zero
    // sequence are therefore all rejected.
    for index in 0..bytes.len() {
        for value in 0..=u8::MAX {
            let mut mutated = bytes.clone();
            mutated[index] = value;
            if let Ok(decoded) = decode_reliable_command_ack(&mutated) {
                assert_ne!(decoded.sequence, 0);
                assert_eq!(
                    encode_reliable_command_ack(decoded).unwrap(),
                    mutated,
                    "non-canonical ack accepted at byte {index} = {value:#04x}"
                );
            }
        }
    }

    // Other game frames are not acks, and an ack is not another game frame.
    assert!(decode_reliable_command_ack(&encode_command(1, b"").unwrap()).is_err());
    assert!(decode_reliable_command_ack(&encode_command_rejection(1, b"").unwrap()).is_err());
    assert!(decode_command(&bytes).is_err());
    assert!(decode_command_rejection(&bytes).is_err());
}

#[test]
fn browser_contract_publishes_the_command_lane_bounds() {
    assert_eq!(MAX_COMMAND_LANES_PER_CONNECTION, 1);
    let contract = BROWSER_PROTOCOL_CONTRACT;
    assert_eq!(
        contract.command_lane_format_version,
        COMMAND_LANE_FORMAT_VERSION
    );
    assert_eq!(
        contract.command_lane_header_bytes,
        COMMAND_LANE_HEADER_BYTES
    );
    assert_eq!(
        contract.reliable_command_ack_bytes,
        RELIABLE_COMMAND_ACK_BYTES
    );
    assert_eq!(
        contract.max_command_lanes_per_connection,
        MAX_COMMAND_LANES_PER_CONNECTION
    );
    // Reliable-lane frames are the existing command frames with the existing bound.
    assert_eq!(
        contract.max_command_payload_bytes,
        MAX_COMMAND_PAYLOAD_BYTES
    );
}
