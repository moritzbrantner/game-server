use crate::browser::{BrowserAdmission, BrowserRoutePrefix, BrowserSessionRoute};
use crate::control::{
    CONTROL_HEADER_BYTES, ControlContext, MAX_CONTROL_PAYLOAD_BYTES, MatchControlService,
    RejectMatchControlService, decode_control_request, encode_control_response,
};
use crate::host::{MatchHost, MatchId};
use crate::host_recovery::{MatchHostRecoveryPlan, consume_recovery_bundle, write_recovery_bundle};
use crate::protocol::{
    RECONNECT_TOKEN_BYTES, SnapshotFrame, Welcome, decode_command, encode_snapshot, encode_welcome,
};
use crate::recovery::RecoveryImage;
use crate::runtime::{MatchRuntime, RuntimeError};
use crate::session::{ReconnectToken, SessionLease};
use crate::simulation::GameSimulation;
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast, mpsc, oneshot};
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
const MAX_CONCURRENT_CONTROL_HANDLERS: usize = 64;

#[derive(Clone, Debug)]
pub struct MatchHostWebTransportConfig {
    pub port: u16,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
    pub route_prefix: BrowserRoutePrefix,
    pub drain_grace: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchHostTransportError {
    Identity(String),
    Endpoint(String),
    Recovery(String),
    EmptyHost,
    HostAlreadyDraining,
    InvalidTickRate(MatchId),
    PlayerCapacityTooLarge { match_id: MatchId, capacity: usize },
}

impl fmt::Display for MatchHostTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(formatter, "TLS identity error: {error}"),
            Self::Endpoint(error) => write!(formatter, "WebTransport endpoint error: {error}"),
            Self::Recovery(error) => write!(formatter, "host recovery error: {error}"),
            Self::EmptyHost => write!(formatter, "match host must contain at least one match"),
            Self::HostAlreadyDraining => {
                write!(
                    formatter,
                    "match host cannot start transport while already draining"
                )
            }
            Self::InvalidTickRate(match_id) => {
                write!(
                    formatter,
                    "match {match_id} has a zero simulation tick rate"
                )
            }
            Self::PlayerCapacityTooLarge { match_id, capacity } => write!(
                formatter,
                "match {match_id} player capacity {capacity} exceeds wire limit"
            ),
        }
    }
}

impl Error for MatchHostTransportError {}

struct HostedMatch<S> {
    runtime: Mutex<MatchRuntime<S>>,
    snapshots: broadcast::Sender<Vec<u8>>,
}

struct HostedServerState<S> {
    matches: Arc<BTreeMap<MatchId, HostedMatch<S>>>,
    admission_gate: Arc<RwLock<bool>>,
    control: Arc<dyn MatchControlService>,
    control_handlers: Arc<Semaphore>,
    shutdown: broadcast::Sender<()>,
}

impl<S> Clone for HostedServerState<S> {
    fn clone(&self) -> Self {
        Self {
            matches: Arc::clone(&self.matches),
            admission_gate: Arc::clone(&self.admission_gate),
            control: Arc::clone(&self.control),
            control_handlers: Arc::clone(&self.control_handlers),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<S: GameSimulation> HostedServerState<S> {
    async fn with_runtime<R>(
        &self,
        match_id: &MatchId,
        operation: impl FnOnce(&MatchRuntime<S>) -> R,
    ) -> Option<R> {
        let hosted = self.matches.get(match_id)?;
        let runtime = hosted.runtime.lock().await;
        Some(operation(&runtime))
    }

    async fn with_runtime_mut<R>(
        &self,
        match_id: &MatchId,
        operation: impl FnOnce(&mut MatchRuntime<S>) -> R,
    ) -> Option<R> {
        let hosted = self.matches.get(match_id)?;
        let mut runtime = hosted.runtime.lock().await;
        Some(operation(&mut runtime))
    }

    fn snapshots(&self, match_id: &MatchId) -> Option<broadcast::Receiver<Vec<u8>>> {
        self.matches
            .get(match_id)
            .map(|hosted| hosted.snapshots.subscribe())
    }

    async fn begin_process_drain(&self) {
        let mut draining = self.admission_gate.write().await;
        *draining = true;
        for hosted in self.matches.values() {
            hosted.runtime.lock().await.begin_drain();
        }
    }
}

pub async fn serve_match_host<S>(
    host: MatchHost<S>,
    config: MatchHostWebTransportConfig,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
{
    serve_match_host_with_control(host, RejectMatchControlService, config).await
}

pub async fn serve_match_host_with_control<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);
    let result =
        serve_match_host_with_control_and_shutdown(host, control, config, shutdown_receiver).await;
    drop(shutdown_sender);
    result
}

pub async fn serve_match_host_with_shutdown<S>(
    host: MatchHost<S>,
    config: MatchHostWebTransportConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
{
    serve_match_host_with_control_and_shutdown(
        host,
        RejectMatchControlService,
        config,
        shutdown_requests,
    )
    .await
}

pub async fn serve_match_host_with_control_and_shutdown<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_match_host_with_control_and_shutdown_notifying_ready(
        host,
        control,
        config,
        shutdown_requests,
        None,
        None,
    )
    .await
}

pub(crate) async fn serve_match_host_with_control_and_shutdown_notifying_ready<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    mut shutdown_requests: mpsc::Receiver<()>,
    ready: Option<oneshot::Sender<()>>,
    recovery: Option<MatchHostRecoveryPlan>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let match_tick_rates = validate_host(&host)?;
    let identity = Identity::load_pemfiles(&config.certificate_pem, &config.private_key_pem)
        .await
        .map_err(|error| MatchHostTransportError::Identity(error.to_string()))?;
    let server_config = ServerConfig::builder()
        .with_bind_default(config.port)
        .with_identity(identity)
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .build();
    let endpoint = Endpoint::server(server_config)
        .map_err(|error| MatchHostTransportError::Endpoint(error.to_string()))?;

    if recovery.as_ref().is_some_and(|plan| plan.consume_on_start) {
        let directory = recovery
            .as_ref()
            .expect("checked recovery plan")
            .directory
            .clone();
        spawn_blocking(move || consume_recovery_bundle(&directory))
            .await
            .map_err(|error| {
                MatchHostTransportError::Recovery(format!(
                    "recovery consumption task failed: {error}"
                ))
            })?
            .map_err(|error| MatchHostTransportError::Recovery(error.to_string()))?;
    }

    let (shutdown, _) = broadcast::channel::<()>(1);
    let state = HostedServerState {
        matches: Arc::new(isolate_hosted_runtimes(host)),
        admission_gate: Arc::new(RwLock::new(false)),
        control: Arc::new(control),
        control_handlers: Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_HANDLERS)),
        shutdown,
    };
    let tick_tasks = spawn_tick_loops(state.clone(), &match_tick_rates);
    let route_prefix = config.route_prefix.clone();
    if let Some(ready) = ready {
        let _ = ready.send(());
    }

    let spawn_incoming = |incoming: wtransport::endpoint::IncomingSession| {
        let state = state.clone();
        let route_prefix = route_prefix.clone();
        tokio::spawn(async move {
            let request = match incoming.await {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("WebTransport negotiation failed: {error}");
                    return;
                }
            };
            let route = match route_prefix.parse(request.path()) {
                Ok(Some(route)) if state.matches.contains_key(&route.match_id) => route,
                Ok(Some(_)) | Ok(None) | Err(_) => {
                    let _ = request.not_found().await;
                    return;
                }
            };
            let connection = match request.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("WebTransport acceptance failed: {error}");
                    return;
                }
            };
            if let Err(error) = handle_connection(connection, state, route).await {
                eprintln!("hosted game session failed: {error}");
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

                state.begin_process_drain().await;
                let drain_deadline = tokio::time::sleep(config.drain_grace);
                tokio::pin!(drain_deadline);
                loop {
                    tokio::select! {
                        _ = &mut drain_deadline => break,
                        incoming = endpoint.accept() => spawn_incoming(incoming),
                    }
                }

                if let Some(plan) = &recovery
                    && let Err(error) = persist_host_recovery(&state, &plan.directory).await
                {
                    eprintln!(
                        "graceful hosted shutdown aborted because recovery persistence failed: {error}"
                    );
                    continue;
                }

                let _ = state.shutdown.send(());
                stop_tick_loops(tick_tasks).await;
                return Ok(());
            }
        }
    }
}

async fn persist_host_recovery<S: GameSimulation>(
    state: &HostedServerState<S>,
    directory: &Path,
) -> Result<(), String> {
    let mut images = BTreeMap::<MatchId, RecoveryImage>::new();
    for (id, hosted) in state.matches.iter() {
        let image = {
            let mut runtime = hosted.runtime.lock().await;
            runtime.freeze_for_recovery();
            runtime.recovery_image()
        };
        match image {
            Ok(image) => {
                images.insert(id.clone(), image);
            }
            Err(error) => {
                resume_host_after_failed_recovery(state).await;
                return Err(format!("match {id}: {error}"));
            }
        }
    }

    let directory = directory.to_path_buf();
    let result = match spawn_blocking(move || write_recovery_bundle(&directory, &images)).await {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(error) => Err(format!("hosted recovery persistence task failed: {error}")),
    };
    if result.is_err() {
        resume_host_after_failed_recovery(state).await;
    }
    result
}

async fn resume_host_after_failed_recovery<S: GameSimulation>(state: &HostedServerState<S>) {
    for hosted in state.matches.values() {
        hosted.runtime.lock().await.resume_after_failed_recovery();
    }
}

fn validate_host<S: GameSimulation>(
    host: &MatchHost<S>,
) -> Result<Vec<(MatchId, u16)>, MatchHostTransportError> {
    if host.is_empty() {
        return Err(MatchHostTransportError::EmptyHost);
    }
    if host.is_draining() {
        return Err(MatchHostTransportError::HostAlreadyDraining);
    }

    host.statuses()
        .into_iter()
        .map(|status| {
            let runtime = host
                .runtime(&status.id)
                .expect("host status must reference an existing runtime");
            let tick_hz = runtime.tick_hz();
            if tick_hz == 0 {
                return Err(MatchHostTransportError::InvalidTickRate(status.id));
            }
            if runtime.max_players() > usize::from(u16::MAX) {
                return Err(MatchHostTransportError::PlayerCapacityTooLarge {
                    match_id: status.id,
                    capacity: runtime.max_players(),
                });
            }
            Ok((status.id, tick_hz))
        })
        .collect()
}

fn isolate_hosted_runtimes<S: GameSimulation>(
    host: MatchHost<S>,
) -> BTreeMap<MatchId, HostedMatch<S>> {
    host.into_runtimes()
        .into_iter()
        .map(|(match_id, runtime)| {
            let (snapshots, _) = broadcast::channel::<Vec<u8>>(SNAPSHOT_CHANNEL_DEPTH);
            (
                match_id,
                HostedMatch {
                    runtime: Mutex::new(runtime),
                    snapshots,
                },
            )
        })
        .collect()
}

fn spawn_tick_loops<S: GameSimulation>(
    state: HostedServerState<S>,
    match_tick_rates: &[(MatchId, u16)],
) -> Vec<JoinHandle<()>> {
    match_tick_rates
        .iter()
        .map(|(match_id, tick_hz)| {
            let state = state.clone();
            let match_id = match_id.clone();
            let tick_hz = *tick_hz;
            let snapshots = state
                .matches
                .get(&match_id)
                .expect("validated match must have a snapshot channel")
                .snapshots
                .clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_micros(
                    1_000_000_u64 / u64::from(tick_hz),
                ));
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    let snapshot = match state
                        .with_runtime_mut(&match_id, |runtime| runtime.advance_tick())
                        .await
                    {
                        Some(Ok(snapshot)) => snapshot,
                        Some(Err(RuntimeError::Frozen)) => continue,
                        Some(Err(error)) => {
                            eprintln!("authoritative tick failed for match {match_id}: {error}");
                            continue;
                        }
                        None => return,
                    };
                    let frame = SnapshotFrame {
                        tick: snapshot.tick,
                        state_hash: snapshot.state_hash,
                        payload: snapshot.payload,
                    };
                    match encode_snapshot(&frame) {
                        Ok(encoded) => {
                            let _ = snapshots.send(encoded);
                        }
                        Err(error) => {
                            eprintln!("snapshot encoding failed for match {match_id}: {error}")
                        }
                    }
                }
            })
        })
        .collect()
}

async fn stop_tick_loops(tick_tasks: Vec<JoinHandle<()>>) {
    for task in &tick_tasks {
        task.abort();
    }
    for task in tick_tasks {
        let _ = task.await;
    }
}

async fn handle_connection<S: GameSimulation>(
    connection: Connection,
    state: HostedServerState<S>,
    route: BrowserSessionRoute,
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
    let lease = match route.admission.clone() {
        BrowserAdmission::New => {
            let draining = state.admission_gate.read().await;
            if *draining {
                close(
                    &connection,
                    CLOSE_RUNTIME,
                    &RuntimeError::Draining.to_string(),
                );
                return Ok(());
            }
            state
                .with_runtime_mut(&route.match_id, |runtime| runtime.admit(replacement_token))
                .await
                .ok_or_else(|| "match disappeared before admission".to_owned())?
        }
        BrowserAdmission::Reconnect(previous_token) => state
            .with_runtime_mut(&route.match_id, |runtime| {
                runtime.reconnect(previous_token, replacement_token)
            })
            .await
            .ok_or_else(|| "match disappeared before reconnect".to_owned())?,
    };
    let lease = match lease {
        Ok(lease) => lease,
        Err(error) => {
            close(&connection, CLOSE_RUNTIME, &error.to_string());
            return Ok(());
        }
    };

    if let Err(error) = send_welcome(&connection, lease, &state, &route.match_id).await {
        let cleanup = state
            .with_runtime_mut(&route.match_id, |runtime| {
                rollback_failed_welcome(runtime, route.admission, lease)
            })
            .await;
        if let Some(Err(cleanup_error)) = cleanup {
            eprintln!("failed to roll back incomplete hosted welcome: {cleanup_error}");
        }
        close(&connection, CLOSE_RUNTIME, &error);
        return Ok(());
    }

    let snapshots = state
        .snapshots(&route.match_id)
        .ok_or_else(|| "match snapshot channel disappeared".to_owned())?;
    let result = run_established_connection(
        &connection,
        lease,
        max_datagram_size,
        snapshots,
        &state,
        route.match_id.clone(),
    )
    .await;
    let _ = state
        .with_runtime_mut(&route.match_id, |runtime| {
            runtime.disconnect(lease.player_id, lease.connection_epoch)
        })
        .await;
    result
}

async fn send_welcome<S: GameSimulation>(
    connection: &Connection,
    lease: SessionLease,
    state: &HostedServerState<S>,
    match_id: &MatchId,
) -> Result<(), String> {
    let (tick_hz, max_players, current_tick) = state
        .with_runtime(match_id, |runtime| {
            (
                runtime.tick_hz(),
                runtime.max_players(),
                runtime.current_tick(),
            )
        })
        .await
        .ok_or_else(|| "match disappeared before welcome".to_owned())?;
    let max_players =
        u16::try_from(max_players).map_err(|_| "player capacity exceeds wire limit")?;
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
    admission: BrowserAdmission,
    lease: SessionLease,
) -> Result<(), String> {
    if runtime.is_frozen() {
        return Err("runtime froze before welcome rollback".to_owned());
    }

    match admission {
        BrowserAdmission::New => runtime
            .abort_admission(lease.player_id, lease.connection_epoch)
            .then_some(())
            .ok_or_else(|| "failed to release incomplete new admission".to_owned()),
        BrowserAdmission::Reconnect(previous_token) => {
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
    mut snapshots: broadcast::Receiver<Vec<u8>>,
    state: &HostedServerState<S>,
    match_id: MatchId,
) -> Result<(), String> {
    let mut shutdown = state.shutdown.subscribe();
    let control_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_STREAMS));
    loop {
        tokio::select! {
            datagram = connection.receive_datagram() => {
                match datagram {
                    Ok(datagram) => match decode_command(datagram.as_ref()) {
                        Ok(command) => {
                            match state
                                .with_runtime_mut(&match_id, |runtime| {
                                    runtime.submit_command(
                                        lease.player_id,
                                        lease.connection_epoch,
                                        command.sequence,
                                        &command.payload,
                                    )
                                })
                                .await
                            {
                                Some(Ok(_)) | Some(Err(RuntimeError::Frozen)) => {}
                                Some(Err(error)) => {
                                    close(connection, CLOSE_PROTOCOL, &error.to_string());
                                    return Ok(());
                                }
                                None => {
                                    close(connection, CLOSE_SERVER, "match is no longer hosted");
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
                let control_handlers = Arc::clone(&state.control_handlers);
                let context = ControlContext {
                    player_id: lease.player_id,
                    connection_epoch: lease.connection_epoch,
                };
                let control_match_id = match_id.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = run_control_stream(
                        send_stream,
                        recv_stream,
                        control_match_id,
                        context,
                        control,
                        control_handlers,
                    )
                    .await
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
    match_id: MatchId,
    context: ControlContext,
    control: Arc<dyn MatchControlService>,
    control_handlers: Arc<Semaphore>,
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
        let mut trailing = [0_u8; 1];
        match recv_stream
            .read(&mut trailing)
            .await
            .map_err(|error| error.to_string())?
        {
            None => {}
            Some(count) => {
                return Err(format!(
                    "reliable-control request has {count} trailing byte(s)"
                ));
            }
        }

        let handler_permit = control_handlers
            .acquire_owned()
            .await
            .map_err(|_| "reliable-control handler capacity closed".to_owned())?;
        let service = Arc::clone(&control);
        let payload = request.payload;
        let handled = spawn_blocking(move || {
            let _handler_permit = handler_permit;
            service.handle(&match_id, context, &payload)
        })
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
    use crate::{DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation};

    fn host_with(ids: &[&str]) -> MatchHost<DemoSimulation> {
        let mut host = MatchHost::new(ids.len()).unwrap();
        for value in ids {
            host.insert(
                MatchId::new(*value).unwrap(),
                MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS),
            )
            .unwrap();
        }
        host
    }

    #[test]
    fn host_validation_binds_one_tick_loop_per_match() {
        let host = host_with(&["alpha", "beta"]);
        let tick_rates = validate_host(&host).unwrap();
        assert_eq!(tick_rates.len(), 2);
        assert_eq!(tick_rates[0].0.as_str(), "alpha");
        assert_eq!(tick_rates[1].0.as_str(), "beta");
        assert!(tick_rates.iter().all(|(_, tick_hz)| *tick_hz > 0));
    }

    #[test]
    fn hosted_runtimes_have_independent_locks() {
        let matches = isolate_hosted_runtimes(host_with(&["alpha", "beta"]));
        let alpha = matches.get(&MatchId::new("alpha").unwrap()).unwrap();
        let beta = matches.get(&MatchId::new("beta").unwrap()).unwrap();

        let _alpha_guard = alpha.runtime.try_lock().unwrap();
        let _beta_guard = beta.runtime.try_lock().unwrap();
    }

    #[test]
    fn empty_or_pre_draining_hosts_fail_closed() {
        let empty = MatchHost::<DemoSimulation>::new(1).unwrap();
        assert_eq!(
            validate_host(&empty),
            Err(MatchHostTransportError::EmptyHost)
        );

        let mut draining = host_with(&["alpha"]);
        draining.begin_drain();
        assert_eq!(
            validate_host(&draining),
            Err(MatchHostTransportError::HostAlreadyDraining)
        );
    }
}
