use crate::control::{
    CONTROL_HEADER_BYTES, ControlService, MAX_CONTROL_PAYLOAD_BYTES, RejectControlService,
    decode_control_request, encode_control_response,
};
use crate::protocol::{
    PlayerId, RECONNECT_TOKEN_BYTES, SnapshotFrame, Welcome, decode_command, encode_snapshot,
    encode_welcome,
};
use crate::recovery::RecoveryImage;
use crate::runtime::{MatchRuntime, RuntimeError};
use crate::session::{ReconnectToken, SessionLease};
use crate::simulation::GameSimulation;
use ring::rand::{SecureRandom, SystemRandom};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::MissedTickBehavior;
use wtransport::{Connection, Endpoint, Identity, RecvStream, SendStream, ServerConfig, VarInt};

const CLOSE_PROTOCOL: u32 = 1;
const CLOSE_DATAGRAM: u32 = 2;
const CLOSE_RUNTIME: u32 = 3;
const CLOSE_SERVER: u32 = 4;
const SNAPSHOT_CHANNEL_DEPTH: usize = 1;
const WELCOME_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_STREAM_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_CONTROL_STREAMS: usize = 4;

#[derive(Clone, Debug)]
pub struct WebTransportConfig {
    pub port: u16,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
    pub session_path: String,
    pub recovery_path: Option<PathBuf>,
    pub drain_grace: Duration,
}

impl WebTransportConfig {
    pub fn reconnect_path(&self, token: ReconnectToken) -> String {
        format!("{}/reconnect/{}", self.session_path, token.encode_hex())
    }
}

#[derive(Debug)]
pub enum TransportError {
    Identity(String),
    Endpoint(String),
    Recovery(String),
    InvalidTickRate,
    PlayerCapacityTooLarge(usize),
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(formatter, "TLS identity error: {error}"),
            Self::Endpoint(error) => write!(formatter, "WebTransport endpoint error: {error}"),
            Self::Recovery(error) => write!(formatter, "recovery error: {error}"),
            Self::InvalidTickRate => write!(formatter, "simulation tick rate must be non-zero"),
            Self::PlayerCapacityTooLarge(capacity) => {
                write!(formatter, "player capacity {capacity} exceeds wire limit")
            }
        }
    }
}

impl Error for TransportError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionRequest {
    New,
    Reconnect(ReconnectToken),
}

struct ServerState<S> {
    runtime: Arc<Mutex<MatchRuntime<S>>>,
    control: Arc<dyn ControlService>,
    snapshots: broadcast::Sender<Vec<u8>>,
    shutdown: broadcast::Sender<()>,
}

impl<S> Clone for ServerState<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            control: Arc::clone(&self.control),
            snapshots: self.snapshots.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

pub async fn serve<S>(
    simulation: S,
    reconnect_grace_ticks: u64,
    config: WebTransportConfig,
) -> Result<(), TransportError>
where
    S: GameSimulation,
{
    serve_with_control(
        simulation,
        RejectControlService,
        reconnect_grace_ticks,
        config,
    )
    .await
}

pub async fn serve_with_control<S, C>(
    simulation: S,
    control: C,
    reconnect_grace_ticks: u64,
    config: WebTransportConfig,
) -> Result<(), TransportError>
where
    S: GameSimulation,
    C: ControlService,
{
    let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);
    let result = serve_with_control_and_shutdown(
        simulation,
        control,
        reconnect_grace_ticks,
        config,
        shutdown_receiver,
    )
    .await;
    drop(shutdown_sender);
    result
}

pub async fn serve_with_shutdown<S>(
    simulation: S,
    reconnect_grace_ticks: u64,
    config: WebTransportConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), TransportError>
where
    S: GameSimulation,
{
    serve_with_control_and_shutdown(
        simulation,
        RejectControlService,
        reconnect_grace_ticks,
        config,
        shutdown_requests,
    )
    .await
}

pub async fn serve_with_control_and_shutdown<S, C>(
    simulation: S,
    control: C,
    reconnect_grace_ticks: u64,
    config: WebTransportConfig,
    mut shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), TransportError>
where
    S: GameSimulation,
    C: ControlService,
{
    if simulation.tick_hz() == 0 {
        return Err(TransportError::InvalidTickRate);
    }
    if simulation.max_players() > usize::from(u16::MAX) {
        return Err(TransportError::PlayerCapacityTooLarge(
            simulation.max_players(),
        ));
    }

    let identity = Identity::load_pemfiles(&config.certificate_pem, &config.private_key_pem)
        .await
        .map_err(|error| TransportError::Identity(error.to_string()))?;
    let server_config = ServerConfig::builder()
        .with_bind_default(config.port)
        .with_identity(identity)
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .build();
    let endpoint = Endpoint::server(server_config)
        .map_err(|error| TransportError::Endpoint(error.to_string()))?;

    let runtime = build_runtime(
        simulation,
        reconnect_grace_ticks,
        config.recovery_path.as_deref(),
    )?;
    let tick_hz = runtime.tick_hz();
    let (snapshots, _) = broadcast::channel::<Vec<u8>>(SNAPSHOT_CHANNEL_DEPTH);
    let (shutdown, _) = broadcast::channel::<()>(1);
    let state = ServerState {
        runtime: Arc::new(Mutex::new(runtime)),
        control: Arc::new(control),
        snapshots,
        shutdown,
    };
    let tick_task = spawn_tick_loop(state.clone(), tick_hz);

    let spawn_incoming = |incoming: wtransport::endpoint::IncomingSession| {
        let state = state.clone();
        let session_path = config.session_path.clone();
        tokio::spawn(async move {
            let request = match incoming.await {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("WebTransport negotiation failed: {error}");
                    return;
                }
            };
            let Some(admission) = parse_admission_request(request.path(), &session_path) else {
                let _ = request.not_found().await;
                return;
            };
            let connection = match request.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("WebTransport acceptance failed: {error}");
                    return;
                }
            };
            if let Err(error) = handle_connection(connection, state, admission).await {
                eprintln!("game session failed: {error}");
            }
        });
    };

    let mut shutdown_channel_open = true;
    loop {
        tokio::select! {
            incoming = endpoint.accept() => spawn_incoming(incoming),
            shutdown = shutdown_requests.recv(), if shutdown_channel_open => {
                let Some(()) = shutdown else {
                    shutdown_channel_open = false;
                    continue;
                };

                state.runtime.lock().await.begin_drain();
                let drain_deadline = tokio::time::sleep(config.drain_grace);
                tokio::pin!(drain_deadline);
                loop {
                    tokio::select! {
                        _ = &mut drain_deadline => break,
                        incoming = endpoint.accept() => spawn_incoming(incoming),
                    }
                }

                match persist_graceful_recovery(&state, config.recovery_path.as_deref()).await {
                    Ok(()) => {
                        let _ = state.shutdown.send(());
                        stop_tick_loop(tick_task).await;
                        return Ok(());
                    }
                    Err(error) => {
                        eprintln!("graceful shutdown aborted because recovery persistence failed: {error}");
                    }
                }
            }
        }
    }
}

fn build_runtime<S: GameSimulation>(
    simulation: S,
    reconnect_grace_ticks: u64,
    recovery_path: Option<&Path>,
) -> Result<MatchRuntime<S>, TransportError> {
    let Some(path) = recovery_path else {
        return Ok(MatchRuntime::new(simulation, reconnect_grace_ticks));
    };

    match fs::metadata(path) {
        Ok(_) => {
            let image = RecoveryImage::read_file(path)
                .map_err(|error| TransportError::Recovery(error.to_string()))?;
            let runtime = MatchRuntime::restore_from_recovery(simulation, image)
                .map_err(|error| TransportError::Recovery(error.to_string()))?;
            consume_recovery_file(path)?;
            Ok(runtime)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(
            MatchRuntime::new_with_replay_capture(simulation, reconnect_grace_ticks),
        ),
        Err(error) => Err(TransportError::Recovery(error.to_string())),
    }
}

async fn persist_graceful_recovery<S: GameSimulation>(
    state: &ServerState<S>,
    recovery_path: Option<&Path>,
) -> Result<(), String> {
    let image = {
        let mut runtime = state.runtime.lock().await;
        runtime.freeze_for_recovery();
        match recovery_path {
            Some(_) => match runtime.recovery_image() {
                Ok(image) => Some(image),
                Err(error) => {
                    runtime.resume_after_failed_recovery();
                    return Err(error.to_string());
                }
            },
            None => None,
        }
    };

    let result = match (recovery_path, image) {
        (Some(path), Some(image)) => {
            let path = path.to_path_buf();
            match spawn_blocking(move || image.write_atomic(&path)).await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(error) => Err(format!("recovery persistence task failed: {error}")),
            }
        }
        (None, None) => Ok(()),
        _ => Err("recovery persistence state mismatch".to_owned()),
    };

    if result.is_err() {
        state.runtime.lock().await.resume_after_failed_recovery();
    }
    result
}

fn consume_recovery_file(path: &Path) -> Result<(), TransportError> {
    fs::remove_file(path).map_err(|error| TransportError::Recovery(error.to_string()))?;
    #[cfg(unix)]
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| TransportError::Recovery(error.to_string()))?;
    }
    Ok(())
}

fn spawn_tick_loop<S: GameSimulation>(state: ServerState<S>, tick_hz: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_micros(1_000_000_u64 / u64::from(tick_hz)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let snapshot = match state.runtime.lock().await.advance_tick() {
                Ok(snapshot) => snapshot,
                Err(RuntimeError::Frozen) => continue,
                Err(error) => {
                    eprintln!("authoritative tick failed: {error}");
                    continue;
                }
            };
            let frame = SnapshotFrame {
                tick: snapshot.tick,
                state_hash: snapshot.state_hash,
                payload: snapshot.payload,
            };
            match encode_snapshot(&frame) {
                Ok(encoded) => {
                    let _ = state.snapshots.send(encoded);
                }
                Err(error) => eprintln!("snapshot encoding failed: {error}"),
            }
        }
    })
}

async fn stop_tick_loop(tick_task: JoinHandle<()>) {
    tick_task.abort();
    let _ = tick_task.await;
}

async fn handle_connection<S: GameSimulation>(
    connection: Connection,
    state: ServerState<S>,
    admission: AdmissionRequest,
) -> Result<(), String> {
    let max_datagram_size = match connection.max_datagram_size() {
        Some(max_datagram_size) => max_datagram_size,
        None => {
            close(
                &connection,
                CLOSE_DATAGRAM,
                "WebTransport datagrams are required",
            );
            return Ok(());
        }
    };

    let replacement_token = generate_reconnect_token()?;
    let lease = {
        let mut runtime = state.runtime.lock().await;
        match admission {
            AdmissionRequest::New => runtime.admit(replacement_token),
            AdmissionRequest::Reconnect(previous_token) => {
                runtime.reconnect(previous_token, replacement_token)
            }
        }
    };
    let lease = match lease {
        Ok(lease) => lease,
        Err(error) => {
            close(&connection, CLOSE_RUNTIME, &error.to_string());
            return Ok(());
        }
    };

    if let Err(error) = send_welcome(&connection, lease, &state).await {
        let cleanup = {
            let mut runtime = state.runtime.lock().await;
            rollback_failed_welcome(&mut runtime, admission, lease)
        };
        if let Err(cleanup_error) = cleanup {
            eprintln!("failed to roll back incomplete welcome: {cleanup_error}");
        }
        close(&connection, CLOSE_RUNTIME, &error);
        return Ok(());
    }

    let result = run_established_connection(&connection, lease, max_datagram_size, &state).await;
    state
        .runtime
        .lock()
        .await
        .disconnect(lease.player_id, lease.connection_epoch);
    result
}

async fn send_welcome<S: GameSimulation>(
    connection: &Connection,
    lease: SessionLease,
    state: &ServerState<S>,
) -> Result<(), String> {
    let (tick_hz, max_players, current_tick) = {
        let runtime = state.runtime.lock().await;
        (
            runtime.tick_hz(),
            u16::try_from(runtime.max_players())
                .map_err(|_| "player capacity exceeds wire limit")?,
            runtime.current_tick(),
        )
    };
    let welcome = encode_welcome(Welcome {
        player_id: lease.player_id,
        tick_hz,
        max_players,
        current_tick,
        connection_epoch: lease.connection_epoch,
        reconnect_token: lease.reconnect_token.0,
        reconnect_grace_ticks: lease.reconnect_grace_ticks,
    });
    let handshake = async {
        let opening = connection
            .open_uni()
            .await
            .map_err(|error| error.to_string())?;
        let mut welcome_stream = opening.await.map_err(|error| error.to_string())?;
        welcome_stream
            .write_all(&welcome)
            .await
            .map_err(|error| error.to_string())?;
        welcome_stream
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    };

    tokio::select! {
        result = tokio::time::timeout(WELCOME_HANDSHAKE_TIMEOUT, handshake) => {
            match result {
                Ok(result) => result,
                Err(_) => Err("welcome handshake timed out".to_owned()),
            }
        }
        _ = connection.closed() => Err("connection closed before welcome completed".to_owned()),
    }
}

fn rollback_failed_welcome<S: GameSimulation>(
    runtime: &mut MatchRuntime<S>,
    admission: AdmissionRequest,
    lease: SessionLease,
) -> Result<(), String> {
    if runtime.is_frozen() {
        return Err("runtime froze before welcome rollback".to_owned());
    }

    match admission {
        AdmissionRequest::New => runtime
            .abort_admission(lease.player_id, lease.connection_epoch)
            .then_some(())
            .ok_or_else(|| "failed to release incomplete new admission".to_owned()),
        AdmissionRequest::Reconnect(previous_token) => {
            if !runtime.disconnect(lease.player_id, lease.connection_epoch) {
                return Err("welcome rollback no longer owns the connection epoch".to_owned());
            }
            let restored = runtime
                .reconnect(lease.reconnect_token, previous_token)
                .map_err(|error| format!("failed to restore previous reconnect token: {error}"))?;
            if !runtime.disconnect(restored.player_id, restored.connection_epoch) {
                return Err("failed to return restored reconnect token to grace state".to_owned());
            }
            Ok(())
        }
    }
}

async fn run_established_connection<S: GameSimulation>(
    connection: &Connection,
    lease: SessionLease,
    max_datagram_size: usize,
    state: &ServerState<S>,
) -> Result<(), String> {
    let mut snapshots = state.snapshots.subscribe();
    let mut shutdown = state.shutdown.subscribe();
    let control_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_STREAMS));
    loop {
        tokio::select! {
            datagram = connection.receive_datagram() => {
                match datagram {
                    Ok(datagram) => match decode_command(datagram.as_ref()) {
                        Ok(command) => {
                            match state.runtime.lock().await.submit_command(
                                lease.player_id,
                                lease.connection_epoch,
                                command.sequence,
                                &command.payload,
                            ) {
                                Ok(_) | Err(RuntimeError::Frozen) => {}
                                Err(error) => {
                                    close(connection, CLOSE_PROTOCOL, &error.to_string());
                                    return Ok(());
                                }
                            }
                        }
                        Err(error) => {
                            close(connection, CLOSE_PROTOCOL, &error.to_string());
                            return Ok(());
                        }
                    },
                    Err(_) => return Ok(()),
                }
            }
            control_stream = connection.accept_bi() => {
                let (send_stream, recv_stream) = match control_stream {
                    Ok(streams) => streams,
                    Err(_) => return Ok(()),
                };
                let permit = match Arc::clone(&control_permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        eprintln!("reliable control stream rejected: concurrency limit reached");
                        continue;
                    }
                };
                let control = Arc::clone(&state.control);
                let player_id = lease.player_id;
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) =
                        run_control_stream(send_stream, recv_stream, player_id, control).await
                    {
                        eprintln!("reliable control stream failed: {error}");
                    }
                });
            }
            snapshot = snapshots.recv() => {
                match snapshot {
                    Ok(snapshot) => {
                        if snapshot.len() > max_datagram_size {
                            close(connection, CLOSE_DATAGRAM, "snapshot exceeds negotiated datagram budget");
                            return Ok(());
                        }
                        let _ = connection.send_datagram(snapshot);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        close(connection, CLOSE_SERVER, "snapshot source closed");
                        return Ok(());
                    }
                }
            }
            _ = shutdown.recv() => {
                close(connection, CLOSE_SERVER, "server shutting down");
                return Ok(());
            }
            _ = connection.closed() => return Ok(()),
        }
    }
}

async fn run_control_stream(
    mut send_stream: SendStream,
    mut recv_stream: RecvStream,
    player_id: PlayerId,
    control: Arc<dyn ControlService>,
) -> Result<(), String> {
    let exchange = async {
        let mut header = [0_u8; CONTROL_HEADER_BYTES];
        recv_stream
            .read_exact(&mut header)
            .await
            .map_err(|error| error.to_string())?;
        let payload_len = usize::from(u16::from_be_bytes([header[6], header[7]]));
        if payload_len > MAX_CONTROL_PAYLOAD_BYTES {
            return Err(format!(
                "declared reliable-control payload {payload_len} exceeds maximum {MAX_CONTROL_PAYLOAD_BYTES}"
            ));
        }

        let mut frame = Vec::with_capacity(CONTROL_HEADER_BYTES + payload_len);
        frame.extend_from_slice(&header);
        frame.resize(CONTROL_HEADER_BYTES + payload_len, 0);
        if payload_len > 0 {
            recv_stream
                .read_exact(&mut frame[CONTROL_HEADER_BYTES..])
                .await
                .map_err(|error| error.to_string())?;
        }
        let request = decode_control_request(&frame).map_err(|error| error.to_string())?;

        let service = Arc::clone(&control);
        let payload = request.payload;
        let handled = spawn_blocking(move || service.handle(player_id, &payload))
            .await
            .map_err(|error| format!("reliable-control handler task failed: {error}"))?;
        let response = match handled {
            Ok(payload) => match encode_control_response(true, &payload) {
                Ok(response) => response,
                Err(error) => {
                    eprintln!("reliable-control response rejected: {error}");
                    encode_control_response(false, b"")
                        .expect("empty reliable-control rejection is always encodable")
                }
            },
            Err(error) => {
                eprintln!("reliable-control request rejected: {error}");
                encode_control_response(false, b"")
                    .expect("empty reliable-control rejection is always encodable")
            }
        };

        send_stream
            .write_all(&response)
            .await
            .map_err(|error| error.to_string())?;
        send_stream
            .finish()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    };

    tokio::time::timeout(CONTROL_STREAM_TIMEOUT, exchange)
        .await
        .map_err(|_| "reliable control stream timed out".to_owned())?
}

fn parse_admission_request(path: &str, session_path: &str) -> Option<AdmissionRequest> {
    if path == session_path {
        return Some(AdmissionRequest::New);
    }
    let prefix = format!("{session_path}/reconnect/");
    let token = path.strip_prefix(&prefix)?;
    ReconnectToken::decode_hex(token).map(AdmissionRequest::Reconnect)
}

fn generate_reconnect_token() -> Result<ReconnectToken, String> {
    let mut bytes = [0_u8; RECONNECT_TOKEN_BYTES];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "secure reconnect-token generation failed".to_owned())?;
    Ok(ReconnectToken(bytes))
}

fn close(connection: &Connection, code: u32, reason: &str) {
    connection.close(VarInt::from_u32(code), reason.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::DemoSimulation;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_new_and_reconnect_paths() {
        assert_eq!(
            parse_admission_request("/match/one", "/match/one"),
            Some(AdmissionRequest::New)
        );
        let token = ReconnectToken([0xab; RECONNECT_TOKEN_BYTES]);
        assert_eq!(
            parse_admission_request(
                &format!("/match/one/reconnect/{}", token.encode_hex()),
                "/match/one"
            ),
            Some(AdmissionRequest::Reconnect(token))
        );
        assert_eq!(
            parse_admission_request("/match/one/reconnect/not-valid", "/match/one"),
            None
        );
    }

    #[test]
    fn failed_new_welcome_releases_unusable_slot() {
        let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();

        rollback_failed_welcome(&mut runtime, AdmissionRequest::New, lease).unwrap();

        assert_eq!(runtime.slot_count(), 0);
        assert_eq!(runtime.active_count(), 0);
    }

    #[test]
    fn failed_reconnect_welcome_restores_the_client_known_token() {
        let previous_token = ReconnectToken([1; RECONNECT_TOKEN_BYTES]);
        let replacement_token = ReconnectToken([2; RECONNECT_TOKEN_BYTES]);
        let next_token = ReconnectToken([3; RECONNECT_TOKEN_BYTES]);
        let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
        let original = runtime.admit(previous_token).unwrap();
        assert!(runtime.disconnect(original.player_id, original.connection_epoch));
        let failed = runtime
            .reconnect(previous_token, replacement_token)
            .unwrap();

        rollback_failed_welcome(
            &mut runtime,
            AdmissionRequest::Reconnect(previous_token),
            failed,
        )
        .unwrap();

        let recovered = runtime.reconnect(previous_token, next_token).unwrap();
        assert_eq!(recovered.player_id, original.player_id);
        assert!(
            runtime
                .reconnect(replacement_token, previous_token)
                .is_err()
        );
    }

    #[test]
    fn recovery_startup_consumes_valid_image_and_rejects_corruption() {
        let path = unique_test_path();
        let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        runtime.advance_tick().unwrap();
        runtime.freeze_for_recovery();
        runtime
            .recovery_image()
            .unwrap()
            .write_atomic(&path)
            .unwrap();

        let restored = build_runtime(DemoSimulation::new(), 10, Some(&path)).unwrap();
        assert_eq!(restored.slot_count(), 1);
        assert!(!path.exists());
        assert_eq!(restored.current_tick(), 1);
        assert_eq!(restored.active_count(), 0);
        assert_eq!(lease.player_id, 1);

        fs::write(&path, b"not a recovery image").unwrap();
        assert!(matches!(
            build_runtime(DemoSimulation::new(), 10, Some(&path)),
            Err(TransportError::Recovery(_))
        ));
        assert!(path.exists());
        let _ = fs::remove_file(path);
    }

    fn unique_test_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "game-server-transport-recovery-{}-{nonce}.bin",
            std::process::id()
        ))
    }
}
