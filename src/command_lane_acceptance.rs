//! Real-network and crate-internal acceptance for the reliable command lane (#43).
//!
//! The public API contract is documented at the top of `tests/reliable_command_lane.rs`.
//! These tests drive a real loopback WebTransport session through the same
//! `ConnectionState`/`handle_connection` harness as the existing control-stream and
//! command-rejection acceptance in `connection.rs`.
//!
//! Wire contract exercised here:
//! - The client opens one bidirectional stream and writes `encode_command_lane_header()`
//!   first. Control streams keep their `GSCT` frames and remain usable beside the lane.
//! - It then writes ordinary `encode_command` frames, in order. The server answers
//!   each complete frame, in order, with one `RELIABLE_COMMAND_ACK_BYTES` ack frame:
//!   `Applied`, `Duplicate` (sequence at or below the reliable watermark) or
//!   `Rejected` (a recoverable `SimulationError::command_rejected`, ADR-0005 semantics:
//!   neither sequence nor replay admission is consumed).
//! - A second lane on one connection, a malformed frame, a declared payload above
//!   `MAX_COMMAND_PAYLOAD_BYTES`, a zero sequence, a partial frame followed by FIN and
//!   a fatal simulation error close the whole connection with the protocol close code
//!   (`CLOSE_PROTOCOL`, 1), exactly like a malformed datagram command.
//! - A stalled lane holds no runtime lock and never blocks ticks, snapshots or
//!   datagram ingestion.
//!
//! The loopback harness (`TestServer`, `LossyRelay`, `connect`) is shared with
//! `welcome_handshake_acceptance` (#45).

use crate::connection::{
    AdmissionRequest, ConnectionState, SNAPSHOT_CHANNEL_DEPTH, handle_connection,
    snapshot_publication,
};
use crate::{
    CommandLane, GameSimulation, MAX_COMMAND_PAYLOAD_BYTES, MatchRuntime, PROTOCOL_VERSION,
    PlayerId, RELIABLE_COMMAND_ACK_BYTES, ReconnectToken, RecoveryError, RecoveryImage,
    RejectControlService, ReliableCommandAck, ReliableCommandOutcome, ReplayError, ReplayLog,
    ReplayRecord, RuntimeRecoveryError, SimulationError, SimulationSnapshot, SnapshotReassembler,
    SnapshotScope, WELCOME_BYTES, Welcome, decode_control_response, decode_reliable_command_ack,
    decode_welcome, encode_command, encode_command_lane_header, encode_command_rejection,
    encode_control_request, verify_replay,
};
use std::collections::{BTreeSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use wtransport::endpoint::endpoint_side::Client;
use wtransport::error::ConnectionError;
use wtransport::tls::Sha256Digest;
use wtransport::{
    ClientConfig, Connection, Endpoint, Identity, RecvStream, SendStream, ServerConfig, VarInt,
};

/// `connection::CLOSE_PROTOCOL`.
const PROTOCOL_CLOSE_CODE: u64 = 1;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(40);
const TICK_INTERVAL: Duration = Duration::from_millis(20);

pub(crate) type Applied = (PlayerId, CommandLane, u32, Vec<u8>);

/// Records the lane of every applied command in canonical state.
/// `b"fatal"` is fatal; `b"needs-unlock"` is a private rejection until `b"unlock"`.
#[derive(Clone, Debug, Default)]
pub(crate) struct LaneSimulation {
    tick: u64,
    players: BTreeSet<PlayerId>,
    unlocked: BTreeSet<PlayerId>,
    applied: Vec<Applied>,
}

impl GameSimulation for LaneSimulation {
    fn tick_hz(&self) -> u16 {
        50
    }

    fn max_players(&self) -> usize {
        16
    }

    fn current_tick(&self) -> u64 {
        self.tick
    }

    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError> {
        self.players.insert(player_id);
        Ok(())
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
        let mut bytes = Vec::new();
        for set in [&self.players, &self.unlocked] {
            bytes.push(u8::try_from(set.len()).unwrap());
            for player_id in set {
                bytes.extend_from_slice(&player_id.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&u16::try_from(self.applied.len()).unwrap().to_be_bytes());
        for (player_id, lane, sequence, payload) in &self.applied {
            bytes.extend_from_slice(&player_id.to_be_bytes());
            bytes.push(match lane {
                CommandLane::Datagram => 0,
                CommandLane::Reliable => 1,
            });
            bytes.extend_from_slice(&sequence.to_be_bytes());
            bytes.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
            bytes.extend_from_slice(payload);
        }
        Ok(SimulationSnapshot::new(self.tick, bytes))
    }
}

fn applied_commands(snapshot: &SimulationSnapshot) -> Vec<Applied> {
    let bytes = &snapshot.payload;
    let mut offset = 0;
    for _ in 0..2 {
        offset += 1 + 4 * usize::from(bytes[offset]);
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
    applied
}

pub(crate) struct TestServer {
    pub(crate) state: ConnectionState<LaneSimulation>,
    pub(crate) port: u16,
    pub(crate) certificate_hash: Sha256Digest,
    pub(crate) admissions: Arc<std::sync::Mutex<VecDeque<AdmissionRequest>>>,
    /// Each accepted session waits for one permit before `handle_connection` runs.
    welcome_gate: Arc<Semaphore>,
    tasks: JoinSet<()>,
}

impl TestServer {
    /// Serves `handle_connection` on loopback and ticks/publishes snapshots every 20 ms.
    pub(crate) async fn start() -> Self {
        Self::start_with_gate(Semaphore::MAX_PERMITS).await
    }

    /// Like `start`, but every accepted WebTransport session waits for
    /// `release_welcome` before admission and the welcome. A test can then change
    /// the network path between session establishment and the welcome.
    pub(crate) async fn start_gated() -> Self {
        Self::start_with_gate(0).await
    }

    /// Lets one waiting (or the next) accepted session proceed to admission.
    pub(crate) fn release_welcome(&self) {
        self.welcome_gate.add_permits(1);
    }

    async fn start_with_gate(permits: usize) -> Self {
        let identity = Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
        let certificate_hash = identity.certificate_chain().as_slice()[0].hash();
        let endpoint = Endpoint::server(
            ServerConfig::builder()
                .with_bind_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .with_identity(identity)
                .build(),
        )
        .unwrap();
        let port = endpoint.local_addr().unwrap().port();
        let (snapshots, _) = broadcast::channel(SNAPSHOT_CHANNEL_DEPTH);
        let (shutdown, _) = broadcast::channel(1);
        let state = ConnectionState {
            runtime: Arc::new(Mutex::new(MatchRuntime::new_with_replay_capture(
                LaneSimulation::default(),
                10_000,
            ))),
            control: Arc::new(RejectControlService),
            control_handlers: Arc::new(Semaphore::new(1)),
            snapshots,
            shutdown,
            admission_gate: None,
        };
        let admissions = Arc::new(std::sync::Mutex::new(VecDeque::new()));
        let welcome_gate = Arc::new(Semaphore::new(permits));
        let mut tasks = JoinSet::new();

        let accepting = state.clone();
        let queue = Arc::clone(&admissions);
        let gate = Arc::clone(&welcome_gate);
        tasks.spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                let Ok(request) = endpoint.accept().await.await else {
                    continue;
                };
                let Ok(connection) = request.accept().await else {
                    continue;
                };
                let admission = queue
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(AdmissionRequest::New);
                let state = accepting.clone();
                let gate = Arc::clone(&gate);
                handlers.spawn(async move {
                    gate.acquire().await.unwrap().forget();
                    if let Err(error) = handle_connection(connection, state, admission).await {
                        eprintln!("test connection ended with error: {error}");
                    }
                });
            }
        });

        let ticking = state.clone();
        tasks.spawn(async move {
            let mut ticker = tokio::time::interval(TICK_INTERVAL);
            loop {
                ticker.tick().await;
                let snapshot = match ticking.runtime.lock().await.advance_tick() {
                    Ok(snapshot) => snapshot,
                    Err(_) => return,
                };
                let publication = snapshot_publication(SnapshotScope::Shared, snapshot).unwrap();
                let _ = ticking.snapshots.send(publication);
            }
        });

        Self {
            state,
            port,
            certificate_hash,
            admissions,
            welcome_gate,
            tasks,
        }
    }

    pub(crate) async fn applied(&self) -> Vec<Applied> {
        applied_commands(&self.state.runtime.lock().await.snapshot().unwrap())
    }

    pub(crate) async fn wait_for_applied(
        &self,
        predicate: impl Fn(&[Applied]) -> bool,
    ) -> Vec<Applied> {
        loop {
            let applied = self.applied().await;
            if predicate(&applied) {
                return applied;
            }
            sleep(Duration::from_millis(5)).await;
        }
    }

    async fn command_records(&self) -> Vec<(PlayerId, CommandLane, u32)> {
        self.state
            .runtime
            .lock()
            .await
            .replay_log()
            .unwrap()
            .records()
            .iter()
            .filter_map(|record| match record {
                ReplayRecord::CommandApplied {
                    player_id,
                    lane,
                    sequence,
                    ..
                } => Some((*player_id, *lane, *sequence)),
                _ => None,
            })
            .collect()
    }

    async fn assert_replay_verifies(&self) {
        let runtime = self.state.runtime.lock().await;
        assert_eq!(
            verify_replay(LaneSimulation::default(), runtime.replay_log().unwrap())
                .unwrap()
                .final_snapshot,
            runtime.snapshot().unwrap()
        );
    }

    pub(crate) async fn shutdown(mut self) {
        self.tasks.shutdown().await;
    }
}

struct TestClient {
    _endpoint: Endpoint<Client>,
    connection: Connection,
    welcome: Welcome,
}

async fn connect(port: u16, certificate_hash: &Sha256Digest) -> TestClient {
    let (endpoint, connection) = connect_session(port, certificate_hash).await;
    let welcome = read_welcome(&connection)
        .await
        .expect("welcome timed out")
        .expect("welcome stream failed");
    TestClient {
        _endpoint: endpoint,
        connection,
        welcome,
    }
}

/// Establishes the WebTransport session without reading the welcome.
async fn connect_session(
    port: u16,
    certificate_hash: &Sha256Digest,
) -> (Endpoint<Client>, Connection) {
    try_connect_session(port, certificate_hash, None)
        .await
        .unwrap_or_else(|error| panic!("connect failed: {error}"))
}

/// Like `connect_session`, but returns why the session could not be established,
/// for example because the server refused the admission while it was opening.
/// `keep_alive` sets the client's QUIC keep-alive interval.
pub(crate) async fn try_connect_session(
    port: u16,
    certificate_hash: &Sha256Digest,
    keep_alive: Option<Duration>,
) -> Result<(Endpoint<Client>, Connection), String> {
    let endpoint = Endpoint::client(
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([certificate_hash.clone()])
            .keep_alive_interval(keep_alive)
            .build(),
    )
    .unwrap();
    let connection = timeout(
        IO_TIMEOUT,
        endpoint.connect(format!("https://127.0.0.1:{port}/game")),
    )
    .await
    .expect("connect timed out")
    .map_err(|error| error.to_string())?;
    Ok((endpoint, connection))
}

/// Reads the welcome within `IO_TIMEOUT`. The inner error says why no complete
/// welcome arrived, for example because the server closed the session instead.
pub(crate) async fn read_welcome(
    connection: &Connection,
) -> Result<Result<Welcome, String>, tokio::time::error::Elapsed> {
    timeout(IO_TIMEOUT, async {
        let mut stream = connection
            .accept_uni()
            .await
            .map_err(|error| format!("no welcome stream: {error}"))?;
        let mut welcome = [0_u8; WELCOME_BYTES];
        stream
            .read_exact(&mut welcome)
            .await
            .map_err(|error| format!("incomplete welcome: {error}"))?;
        decode_welcome(&welcome).map_err(|error| error.to_string())
    })
    .await
}

async fn open_bi(connection: &Connection) -> (SendStream, RecvStream) {
    connection.open_bi().await.unwrap().await.unwrap()
}

async fn open_lane(connection: &Connection) -> (SendStream, RecvStream) {
    let (mut send, recv) = open_bi(connection).await;
    send.write_all(&encode_command_lane_header()).await.unwrap();
    (send, recv)
}

async fn send_reliable(send: &mut SendStream, sequence: u32, payload: &[u8]) {
    send.write_all(&encode_command(sequence, payload).unwrap())
        .await
        .unwrap();
}

async fn read_ack(recv: &mut RecvStream) -> ReliableCommandAck {
    let mut bytes = [0_u8; RELIABLE_COMMAND_ACK_BYTES];
    timeout(IO_TIMEOUT, recv.read_exact(&mut bytes))
        .await
        .expect("reliable command ack timed out")
        .unwrap();
    decode_reliable_command_ack(&bytes).unwrap()
}

pub(crate) fn ack(sequence: u32, outcome: ReliableCommandOutcome) -> ReliableCommandAck {
    ReliableCommandAck { sequence, outcome }
}

async fn expect_protocol_close(connection: &Connection, case: &str) {
    let error = timeout(IO_TIMEOUT, connection.closed())
        .await
        .unwrap_or_else(|_| panic!("{case}: server must close the connection"));
    match error {
        ConnectionError::ApplicationClosed(close) => assert_eq!(
            close.code().into_inner(),
            PROTOCOL_CLOSE_CODE,
            "{case}: expected the protocol close code"
        ),
        other => panic!("{case}: expected an application close, got {other:?}"),
    }
}

fn token(value: u8) -> ReconnectToken {
    ReconnectToken([value; crate::RECONNECT_TOKEN_BYTES])
}

pub(crate) const DATAGRAM: CommandLane = CommandLane::Datagram;
pub(crate) const RELIABLE: CommandLane = CommandLane::Reliable;
pub(crate) const APPLIED: ReliableCommandOutcome = ReliableCommandOutcome::Applied;
const DUPLICATE: ReliableCommandOutcome = ReliableCommandOutcome::Duplicate;
const REJECTED: ReliableCommandOutcome = ReliableCommandOutcome::Rejected;

#[tokio::test]
async fn reliable_lane_acknowledges_applied_duplicate_and_rejected_commands_over_webtransport() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let client = connect(server.port, &server.certificate_hash).await;
        let player = client.welcome.player_id;

        // A held-state datagram with a much higher sequence arrives first.
        client
            .connection
            .send_datagram(encode_command(50, b"held").unwrap())
            .unwrap();
        server
            .wait_for_applied(|applied| applied.iter().any(|(_, _, sequence, _)| *sequence == 50))
            .await;

        let (mut send, mut recv) = open_lane(&client.connection).await;
        send_reliable(&mut send, 1, b"one-shot").await;
        assert_eq!(
            read_ack(&mut recv).await,
            ack(1, APPLIED),
            "an overtaken reliable command is still applied"
        );
        send_reliable(&mut send, 1, b"one-shot").await;
        assert_eq!(read_ack(&mut recv).await, ack(1, DUPLICATE));

        let records_before_rejection = server.command_records().await;
        send_reliable(&mut send, 2, b"needs-unlock").await;
        assert_eq!(read_ack(&mut recv).await, ack(2, REJECTED));
        assert_eq!(
            server.command_records().await,
            records_before_rejection,
            "a rejection is not admitted to replay"
        );

        client
            .connection
            .send_datagram(encode_command(51, b"unlock").unwrap())
            .unwrap();
        server
            .wait_for_applied(|applied| {
                applied
                    .iter()
                    .any(|(_, _, _, payload)| payload == b"unlock")
            })
            .await;
        // The rejected sequence was not consumed.
        send_reliable(&mut send, 2, b"needs-unlock").await;
        assert_eq!(read_ack(&mut recv).await, ack(2, APPLIED));
        send_reliable(&mut send, 1, b"late-resend").await;
        assert_eq!(read_ack(&mut recv).await, ack(1, DUPLICATE));

        // Several frames written back to back are acknowledged in order.
        let mut pipelined = Vec::new();
        for sequence in 3..=5 {
            pipelined.extend_from_slice(&encode_command(sequence, b"burst").unwrap());
        }
        send.write_all(&pipelined).await.unwrap();
        for sequence in 3..=5 {
            assert_eq!(read_ack(&mut recv).await, ack(sequence, APPLIED));
        }

        let reliable: Vec<_> = server
            .applied()
            .await
            .into_iter()
            .filter(|(owner, lane, _, _)| *owner == player && *lane == RELIABLE)
            .map(|(_, _, sequence, payload)| (sequence, payload))
            .collect();
        assert_eq!(
            reliable,
            vec![
                (1, b"one-shot".to_vec()),
                (2, b"needs-unlock".to_vec()),
                (3, b"burst".to_vec()),
                (4, b"burst".to_vec()),
                (5, b"burst".to_vec()),
            ]
        );
        assert!(
            server
                .state
                .runtime
                .lock()
                .await
                .validate_connection(player, client.welcome.connection_epoch)
                .is_ok(),
            "duplicates and rejections keep the connection open"
        );
        server.assert_replay_verifies().await;

        // Finishing the lane at a frame boundary is a clean end, not a protocol error.
        send.finish().await.unwrap();
        assert_eq!(
            timeout(IO_TIMEOUT, recv.read(&mut [0_u8; 1]))
                .await
                .expect("lane must end after a clean finish")
                .unwrap(),
            None
        );
        assert!(
            timeout(Duration::from_millis(200), client.connection.closed())
                .await
                .is_err(),
            "a clean lane finish must not close the connection"
        );
        client.connection.close(VarInt::from_u32(0), b"done");
        server.shutdown().await;
    })
    .await
    .expect("reliable lane acknowledgement acceptance timed out");
}

#[tokio::test]
async fn reliable_resend_after_reconnect_is_applied_once_and_acknowledged_duplicate() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let first = connect(server.port, &server.certificate_hash).await;
        let player = first.welcome.player_id;

        let (mut send, mut recv) = open_lane(&first.connection).await;
        send_reliable(&mut send, 1, b"attack").await;
        assert_eq!(read_ack(&mut recv).await, ack(1, APPLIED));
        // Sequence 2 reaches the authority, but its ack is lost with the connection.
        send_reliable(&mut send, 2, b"interact").await;
        server
            .wait_for_applied(|applied| {
                applied
                    .iter()
                    .any(|(_, _, _, payload)| payload == b"interact")
            })
            .await;
        first
            .connection
            .close(VarInt::from_u32(0), b"network change");
        loop {
            if server.state.runtime.lock().await.active_count() == 0 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }

        server
            .admissions
            .lock()
            .unwrap()
            .push_back(AdmissionRequest::Reconnect(ReconnectToken(
                first.welcome.reconnect_token,
            )));
        let second = connect(server.port, &server.certificate_hash).await;
        assert_eq!(second.welcome.player_id, player);
        assert!(second.welcome.connection_epoch > first.welcome.connection_epoch);

        // The client re-sends everything it has no ack for, then continues.
        let (mut send, mut recv) = open_lane(&second.connection).await;
        send_reliable(&mut send, 2, b"interact").await;
        assert_eq!(read_ack(&mut recv).await, ack(2, DUPLICATE));
        send_reliable(&mut send, 3, b"swap").await;
        assert_eq!(read_ack(&mut recv).await, ack(3, APPLIED));

        let records = server.command_records().await;
        for sequence in 1..=3 {
            assert_eq!(
                records
                    .iter()
                    .filter(|record| **record == (player, RELIABLE, sequence))
                    .count(),
                1,
                "reliable sequence {sequence} must be applied exactly once"
            );
        }
        server.assert_replay_verifies().await;
        second.connection.close(VarInt::from_u32(0), b"done");
        server.shutdown().await;
    })
    .await
    .expect("reliable reconnect acceptance timed out");
}

#[tokio::test]
async fn second_command_lane_closes_the_connection_with_a_protocol_error() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let client = connect(server.port, &server.certificate_hash).await;

        let (mut lane_send, mut lane_recv) = open_lane(&client.connection).await;
        send_reliable(&mut lane_send, 1, b"first-lane").await;
        assert_eq!(read_ack(&mut lane_recv).await, ack(1, APPLIED));

        // Reliable control still works beside the open lane.
        let (mut control_send, mut control_recv) = open_bi(&client.connection).await;
        control_send
            .write_all(&encode_control_request(b"ping").unwrap())
            .await
            .unwrap();
        control_send.finish().await.unwrap();
        let mut response = Vec::new();
        let mut buffer = [0_u8; 64];
        while let Some(read) = timeout(IO_TIMEOUT, control_recv.read(&mut buffer))
            .await
            .expect("control response timed out")
            .unwrap()
        {
            response.extend_from_slice(&buffer[..read]);
        }
        let response = decode_control_response(&response).unwrap();
        assert!(
            !response.accepted,
            "RejectControlService answers through the control path"
        );

        let _second_lane = open_lane(&client.connection).await;
        expect_protocol_close(&client.connection, "second lane").await;
        server.shutdown().await;
    })
    .await
    .expect("second lane acceptance timed out");
}

#[tokio::test]
async fn malformed_oversized_truncated_and_fatal_lane_input_fails_closed() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;

        let mut unsupported_version = encode_command(1, b"x").unwrap();
        unsupported_version[0] = PROTOCOL_VERSION.wrapping_add(1);
        let mut zero_sequence = encode_command(1, b"").unwrap();
        zero_sequence[2..6].copy_from_slice(&0_u32.to_be_bytes());
        // Only the 8-byte header is written: the server must fail on the declared length
        // instead of waiting for a payload that would exceed the bound.
        let mut oversized_header = encode_command(1, b"").unwrap();
        oversized_header[6..8].copy_from_slice(
            &u16::try_from(MAX_COMMAND_PAYLOAD_BYTES + 1)
                .unwrap()
                .to_be_bytes(),
        );
        let complete = encode_command(1, b"abc").unwrap();
        let truncated = complete[..complete.len() - 1].to_vec();

        let cases: [(&str, Vec<u8>, bool); 6] = [
            (
                "wrong frame kind",
                encode_command_rejection(1, b"x").unwrap(),
                false,
            ),
            ("unsupported version", unsupported_version, false),
            ("zero sequence", zero_sequence, false),
            ("oversized declared payload", oversized_header, false),
            ("partial frame then finish", truncated, true),
            (
                "fatal simulation error",
                encode_command(1, b"fatal").unwrap(),
                false,
            ),
        ];
        for (case, bytes, finish) in cases {
            let client = connect(server.port, &server.certificate_hash).await;
            let player = client.welcome.player_id;
            let (mut send, _recv) = open_lane(&client.connection).await;
            send.write_all(&bytes).await.unwrap();
            if finish {
                send.finish().await.unwrap();
            }
            expect_protocol_close(&client.connection, case).await;
            assert!(
                !server
                    .applied()
                    .await
                    .iter()
                    .any(|(owner, _, _, _)| *owner == player),
                "{case}: nothing may be applied"
            );
        }
        server.assert_replay_verifies().await;
        server.shutdown().await;
    })
    .await
    .expect("fail-closed lane acceptance timed out");
}

#[tokio::test]
async fn stalled_lane_does_not_block_ticks_snapshots_or_datagram_commands() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let client = connect(server.port, &server.certificate_hash).await;
        let player = client.welcome.player_id;

        // A lane stalled in the middle of a frame header ...
        let (mut stalled, _stalled_recv) = open_lane(&client.connection).await;
        let frame = encode_command(1, b"stalled").unwrap();
        stalled.write_all(&frame[..3]).await.unwrap();
        // ... and a bidirectional stream stalled before it could be classified.
        let (mut unclassified, _unclassified_recv) = open_bi(&client.connection).await;
        unclassified.write_all(&[0x47]).await.unwrap();

        let mut reassembler = SnapshotReassembler::new();
        let mut ticks = Vec::new();
        timeout(Duration::from_secs(3), async {
            while ticks.len() < 5 {
                let datagram = client.connection.receive_datagram().await.unwrap();
                if let Some(snapshot) = reassembler.accept(&datagram).unwrap() {
                    ticks.push(snapshot.tick);
                }
            }
        })
        .await
        .expect("snapshots must keep flowing while a lane is stalled");
        assert!(ticks.windows(2).all(|pair| pair[0] < pair[1]));

        client
            .connection
            .send_datagram(encode_command(1, b"realtime").unwrap())
            .unwrap();
        timeout(
            Duration::from_secs(3),
            server.wait_for_applied(|applied| {
                applied.contains(&(player, DATAGRAM, 1, b"realtime".to_vec()))
            }),
        )
        .await
        .expect("datagram commands must keep applying while a lane is stalled");
        assert!(
            !server
                .applied()
                .await
                .iter()
                .any(|(_, _, _, payload)| payload == b"stalled")
        );
        client.connection.close(VarInt::from_u32(0), b"done");
        server.shutdown().await;
    })
    .await
    .expect("stalled lane acceptance timed out");
}

/// A UDP relay between client and server that can drop every packet in both
/// directions. QUIC retransmits lost stream data after the outage; datagrams lost
/// during the outage are gone for good. `upstream_outage` and `downstream_outage`
/// drop only client-to-server or only server-to-client packets.
pub(crate) struct LossyRelay {
    pub(crate) port: u16,
    pub(crate) outage: Arc<AtomicBool>,
    pub(crate) upstream_outage: Arc<AtomicBool>,
    pub(crate) downstream_outage: Arc<AtomicBool>,
    pub(crate) dropped: Arc<AtomicU64>,
    pub(crate) tasks: JoinSet<()>,
}

impl LossyRelay {
    pub(crate) async fn start(server_port: u16) -> Self {
        let client_side = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
        let server_side = Arc::new(UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap());
        server_side
            .connect((Ipv4Addr::LOCALHOST, server_port))
            .await
            .unwrap();
        let port = client_side.local_addr().unwrap().port();
        let outage = Arc::new(AtomicBool::new(false));
        let upstream_outage = Arc::new(AtomicBool::new(false));
        let downstream_outage = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicU64::new(0));
        let peer = Arc::new(std::sync::Mutex::new(None::<SocketAddr>));
        let mut tasks = JoinSet::new();

        {
            let (client_side, server_side) = (Arc::clone(&client_side), Arc::clone(&server_side));
            let (outage, dropped, peer) =
                (Arc::clone(&outage), Arc::clone(&dropped), Arc::clone(&peer));
            let directional = Arc::clone(&upstream_outage);
            tasks.spawn(async move {
                let mut buffer = vec![0_u8; 65_535];
                loop {
                    let Ok((read, from)) = client_side.recv_from(&mut buffer).await else {
                        continue;
                    };
                    *peer.lock().unwrap() = Some(from);
                    if outage.load(Ordering::SeqCst) || directional.load(Ordering::SeqCst) {
                        dropped.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    let _ = server_side.send(&buffer[..read]).await;
                }
            });
        }
        {
            let (outage, dropped, peer) =
                (Arc::clone(&outage), Arc::clone(&dropped), Arc::clone(&peer));
            let directional = Arc::clone(&downstream_outage);
            tasks.spawn(async move {
                let mut buffer = vec![0_u8; 65_535];
                loop {
                    let Ok(read) = server_side.recv(&mut buffer).await else {
                        continue;
                    };
                    if outage.load(Ordering::SeqCst) || directional.load(Ordering::SeqCst) {
                        dropped.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    let destination = *peer.lock().unwrap();
                    if let Some(destination) = destination {
                        let _ = client_side.send_to(&buffer[..read], destination).await;
                    }
                }
            });
        }
        Self {
            port,
            outage,
            upstream_outage,
            downstream_outage,
            dropped,
            tasks,
        }
    }
}

#[tokio::test]
async fn reliable_command_reaches_the_authority_while_datagrams_are_dropped() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let mut relay = LossyRelay::start(server.port).await;
        let client = connect(relay.port, &server.certificate_hash).await;
        let player = client.welcome.player_id;

        let (mut send, mut recv) = open_lane(&client.connection).await;
        send_reliable(&mut send, 1, b"warmup").await;
        assert_eq!(read_ack(&mut recv).await, ack(1, APPLIED));

        // Total packet loss: the one-shot goes out on the lane, a datagram command
        // goes out beside it, and both are lost on the wire.
        relay.outage.store(true, Ordering::SeqCst);
        send_reliable(&mut send, 2, b"one-shot").await;
        client
            .connection
            .send_datagram(encode_command(10, b"lost-datagram").unwrap())
            .unwrap();
        sleep(Duration::from_millis(300)).await;
        assert!(relay.dropped.load(Ordering::SeqCst) > 0);
        relay.outage.store(false, Ordering::SeqCst);

        assert_eq!(
            read_ack(&mut recv).await,
            ack(2, APPLIED),
            "the lane must deliver the one-shot after the loss"
        );
        let applied = server.applied().await;
        assert!(applied.contains(&(player, RELIABLE, 2, b"one-shot".to_vec())));
        assert!(
            !applied
                .iter()
                .any(|(_, _, _, payload)| payload == b"lost-datagram"),
            "the dropped datagram must not have reached the authority"
        );

        // Datagrams keep their best-effort behavior after the outage.
        client
            .connection
            .send_datagram(encode_command(11, b"after-outage").unwrap())
            .unwrap();
        timeout(
            Duration::from_secs(5),
            server.wait_for_applied(|applied| {
                applied.contains(&(player, DATAGRAM, 11, b"after-outage".to_vec()))
            }),
        )
        .await
        .expect("datagram commands must resume after the outage");
        server.assert_replay_verifies().await;
        client.connection.close(VarInt::from_u32(0), b"done");
        relay.tasks.shutdown().await;
        server.shutdown().await;
    })
    .await
    .expect("lossy reliable lane acceptance timed out");
}

/// `connection::send_command_rejection` bounds delivery to two seconds.
const COMMAND_REJECTION_DELIVERY_BOUND: Duration = Duration::from_secs(2);

/// A rejection is delivered once written and finished locally (ADR-0009), so an
/// outage that hides the client's ACK for longer than the delivery bound neither
/// stalls nor closes the connection.
#[tokio::test]
async fn command_rejection_does_not_wait_for_the_peer_ack() {
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start().await;
        let mut relay = LossyRelay::start(server.port).await;
        let client = connect(relay.port, &server.certificate_hash).await;
        let player = client.welcome.player_id;

        // The server receives the command, but nothing it sends reaches the client,
        // so it never sees an ACK for the rejection stream.
        relay.downstream_outage.store(true, Ordering::SeqCst);
        client
            .connection
            .send_datagram(encode_command(1, b"needs-unlock").unwrap())
            .unwrap();
        sleep(COMMAND_REJECTION_DELIVERY_BOUND + Duration::from_secs(1)).await;
        assert!(relay.dropped.load(Ordering::SeqCst) > 0);
        relay.downstream_outage.store(false, Ordering::SeqCst);

        let rejection = timeout(IO_TIMEOUT, async {
            let mut stream = client.connection.accept_uni().await.unwrap();
            let mut frame = [0_u8; 8 + b"locked".len()];
            stream.read_exact(&mut frame).await.unwrap();
            crate::decode_command_rejection(&frame).unwrap()
        })
        .await
        .expect("the rejection must reach the client once the path returns");
        assert_eq!(
            rejection,
            crate::CommandRejectionFrame {
                sequence: 1,
                payload: b"locked".to_vec()
            }
        );

        client
            .connection
            .send_datagram(encode_command(2, b"after-outage").unwrap())
            .unwrap();
        timeout(
            Duration::from_secs(5),
            server.wait_for_applied(|applied| {
                applied.contains(&(player, DATAGRAM, 2, b"after-outage".to_vec()))
            }),
        )
        .await
        .expect("the connection must stay usable after a delayed rejection ACK");
        client.connection.close(VarInt::from_u32(0), b"done");
        relay.tasks.shutdown().await;
        server.shutdown().await;
    })
    .await
    .expect("command rejection outage acceptance timed out");
}

#[test]
fn verification_and_recovery_reject_non_monotonic_sequences_within_one_lane() {
    let mut runtime = MatchRuntime::new_with_replay_capture(LaneSimulation::default(), 10);
    let lease = runtime.admit(token(1)).unwrap();
    let (player, epoch) = (lease.player_id, lease.connection_epoch);
    for (lane, sequence) in [(DATAGRAM, 5), (RELIABLE, 5), (RELIABLE, 6), (DATAGRAM, 9)] {
        runtime
            .submit_lane_command(player, epoch, lane, sequence, b"x")
            .unwrap();
    }
    runtime.advance_tick().unwrap();
    runtime.freeze_for_recovery();
    let valid = runtime.recovery_image().unwrap();
    verify_replay(LaneSimulation::default(), &valid.replay).unwrap();
    MatchRuntime::restore_from_recovery(LaneSimulation::default(), valid.clone()).unwrap();

    let last_command = valid
        .replay
        .records()
        .iter()
        .rposition(|record| matches!(record, ReplayRecord::CommandApplied { .. }))
        .unwrap();
    // (lane, injected sequence, that lane's previous watermark)
    for (lane, sequence, previous) in [
        (RELIABLE, 6, 6),
        (RELIABLE, 3, 6),
        (DATAGRAM, 9, 9),
        (DATAGRAM, 6, 9),
    ] {
        let mut tampered = ReplayLog::default();
        for (index, record) in valid.replay.records().iter().enumerate() {
            tampered.append(record.clone());
            if index == last_command {
                tampered.append(ReplayRecord::CommandApplied {
                    tick: record.tick(),
                    player_id: player,
                    lane,
                    sequence,
                    payload: b"x".to_vec(),
                });
            }
        }
        let is_expected = |error: &ReplayError| {
            matches!(
                error,
                ReplayError::NonIncreasingSequence {
                    player_id,
                    previous: found_previous,
                    actual,
                    ..
                } if *player_id == player && *found_previous == previous && *actual == sequence
            )
        };
        let error = verify_replay(LaneSimulation::default(), &tampered).unwrap_err();
        assert!(is_expected(&error), "{lane:?} {sequence}: got {error:?}");

        let mut image = valid.clone();
        image.replay = tampered;
        let decoded = RecoveryImage::decode(&image.encode().unwrap()).unwrap();
        match MatchRuntime::restore_from_recovery(LaneSimulation::default(), decoded) {
            Err(RuntimeRecoveryError::Recovery(RecoveryError::Replay(error))) => {
                assert!(is_expected(&error), "{lane:?} {sequence}: got {error:?}");
            }
            Err(other) => panic!("{lane:?} {sequence}: unexpected recovery error {other:?}"),
            Ok(_) => panic!("{lane:?} {sequence}: recovery accepted impossible history"),
        }
    }

    // A zero sequence is invalid on the reliable lane as well.
    let mut zero = ReplayLog::default();
    zero.append(ReplayRecord::PlayerAdmitted {
        tick: 0,
        player_id: 1,
    });
    zero.append(ReplayRecord::CommandApplied {
        tick: 0,
        player_id: 1,
        lane: RELIABLE,
        sequence: 0,
        payload: Vec::new(),
    });
    assert_eq!(
        verify_replay(LaneSimulation::default(), &zero).unwrap_err(),
        ReplayError::InvalidSequence
    );
}
