use crate::control::{
    CONTROL_HEADER_BYTES, ControlContext, ControlService, ControlServiceError,
    MAX_CONTROL_PAYLOAD_BYTES, decode_control_request, encode_control_response,
};
use crate::protocol::{
    ProtocolError, RECONNECT_TOKEN_BYTES, SnapshotFrame, Welcome, decode_command, encode_snapshot,
    encode_snapshot_fragments, encode_welcome, snapshot_frame_tick,
};
use crate::runtime::{MatchRuntime, RuntimeError};
use crate::session::{ReconnectToken, SessionLease};
use crate::simulation::{GameSimulation, SimulationSnapshot, SnapshotScope};
use ring::rand::{SecureRandom, SystemRandom};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast};
use tokio::task::JoinSet;
use wtransport::error::SendDatagramError;
use wtransport::{Connection, RecvStream, SendStream, VarInt};

const CLOSE_PROTOCOL: u32 = 1;
const CLOSE_DATAGRAM: u32 = 2;
const CLOSE_RUNTIME: u32 = 3;
const CLOSE_SERVER: u32 = 4;
pub(crate) const SNAPSHOT_CHANNEL_DEPTH: usize = 1;
const WELCOME_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_STREAM_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_CONTROL_STREAMS: usize = 4;
pub(crate) const MAX_CONCURRENT_CONTROL_HANDLERS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionRequest {
    New,
    Reconnect(ReconnectToken),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotPublication {
    Shared(Arc<[u8]>),
    PlayerScoped,
}

pub(crate) fn snapshot_publication(
    scope: SnapshotScope,
    snapshot: SimulationSnapshot,
) -> Result<SnapshotPublication, String> {
    match scope {
        SnapshotScope::Shared => encode_simulation_snapshot(snapshot)
            .map(|bytes| SnapshotPublication::Shared(bytes.into())),
        SnapshotScope::PlayerScoped => Ok(SnapshotPublication::PlayerScoped),
    }
}

fn encode_simulation_snapshot(snapshot: SimulationSnapshot) -> Result<Vec<u8>, String> {
    encode_snapshot(&SnapshotFrame {
        tick: snapshot.tick,
        state_hash: snapshot.state_hash,
        payload: snapshot.payload,
    })
    .map_err(|error| error.to_string())
}

pub(crate) struct ConnectionState<S> {
    pub(crate) runtime: Arc<Mutex<MatchRuntime<S>>>,
    pub(crate) control: Arc<dyn ControlService>,
    pub(crate) control_handlers: Arc<Semaphore>,
    pub(crate) snapshots: broadcast::Sender<SnapshotPublication>,
    pub(crate) admission_gate: Option<Arc<RwLock<bool>>>,
    pub(crate) shutdown: broadcast::Sender<()>,
}

impl<S> Clone for ConnectionState<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            control: Arc::clone(&self.control),
            control_handlers: Arc::clone(&self.control_handlers),
            snapshots: self.snapshots.clone(),
            shutdown: self.shutdown.clone(),
            admission_gate: self.admission_gate.clone(),
        }
    }
}

pub(crate) async fn handle_connection<S: GameSimulation>(
    connection: Connection,
    state: ConnectionState<S>,
    admission: AdmissionRequest,
) -> Result<(), String> {
    if connection.max_datagram_size().is_none() {
        close(
            &connection,
            CLOSE_DATAGRAM,
            "WebTransport datagrams are required",
        );
        return Ok(());
    }

    let replacement_token = generate_reconnect_token()?;
    // Keep hosted process drain atomic with new admission, without holding the
    // gate during the welcome handshake or established connection.
    let admission_guard = match (&state.admission_gate, admission) {
        (Some(gate), AdmissionRequest::New) => Some(gate.read().await),
        _ => None,
    };
    if admission_guard.as_deref().copied().unwrap_or(false) {
        close(
            &connection,
            CLOSE_RUNTIME,
            &RuntimeError::Draining.to_string(),
        );
        return Ok(());
    }
    let lease = {
        let mut runtime = state.runtime.lock().await;
        match admission {
            AdmissionRequest::New => runtime.admit(replacement_token),
            AdmissionRequest::Reconnect(previous_token) => {
                runtime.reconnect(previous_token, replacement_token)
            }
        }
    };
    drop(admission_guard);
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

    let result = run_established_connection(&connection, lease, &state).await;
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
    state: &ConnectionState<S>,
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
            .map_err(|error| error.to_string())?
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
    state: &ConnectionState<S>,
) -> Result<(), String> {
    let mut snapshots = state.snapshots.subscribe();
    let mut snapshot_sender = SnapshotSender::default();
    let mut shutdown = state.shutdown.subscribe();
    let control_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_STREAMS));
    let mut control_tasks = JoinSet::new();
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
                let runtime = Arc::clone(&state.runtime);
                let control = Arc::clone(&state.control);
                let control_handlers = Arc::clone(&state.control_handlers);
                let context = ControlContext {
                    player_id: lease.player_id,
                    connection_epoch: lease.connection_epoch,
                };
                control_tasks.spawn(async move {
                    let _permit = permit;
                    run_control_stream(
                        send_stream,
                        recv_stream,
                        context,
                        runtime,
                        control,
                        control_handlers,
                    ).await
                });
            }
            completed = control_tasks.join_next(), if !control_tasks.is_empty() => {
                match completed {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => eprintln!("reliable control stream failed: {error}"),
                    Some(Err(error)) => eprintln!("reliable control task failed: {error}"),
                }
            }
            snapshot = snapshots.recv() => {
                match snapshot {
                    Ok(publication) => {
                        let runtime = state.runtime.lock().await;
                        let snapshot = match snapshot_for_connection(&runtime, lease, &publication) {
                            Ok(snapshot) => snapshot,
                            Err(error) => {
                                close(connection, CLOSE_RUNTIME, &error);
                                return Ok(());
                            }
                        };
                        match snapshot_sender.send(connection, &snapshot) {
                            Ok(_) => {}
                            Err(SnapshotSendError::DatagramsUnavailable) => {
                                close(connection, CLOSE_DATAGRAM, "WebTransport datagrams are required");
                                return Ok(());
                            }
                            Err(SnapshotSendError::Unsendable(error)) => {
                                close(
                                    connection,
                                    CLOSE_DATAGRAM,
                                    &format!("snapshot exceeds datagram budget: {error}"),
                                );
                                return Ok(());
                            }
                        }
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

async fn run_control_stream<S: GameSimulation>(
    mut send_stream: SendStream,
    mut recv_stream: RecvStream,
    context: ControlContext,
    runtime: Arc<Mutex<MatchRuntime<S>>>,
    control: Arc<dyn ControlService>,
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

        let handled =
            dispatch_control(runtime, context, control, control_handlers, request.payload).await?;
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

fn snapshot_for_connection<'a, S: GameSimulation>(
    runtime: &MatchRuntime<S>,
    lease: SessionLease,
    publication: &'a SnapshotPublication,
) -> Result<Cow<'a, [u8]>, String> {
    runtime
        .validate_connection(lease.player_id, lease.connection_epoch)
        .map_err(|error| error.to_string())?;
    match publication {
        SnapshotPublication::Shared(snapshot) => Ok(Cow::Borrowed(snapshot)),
        SnapshotPublication::PlayerScoped => encode_simulation_snapshot(
            runtime
                .snapshot_for(lease.player_id)
                .map_err(|error| error.to_string())?,
        )
        .map(Cow::Owned),
    }
}

/// The datagram operations snapshot delivery uses, so tests can model a path
/// without a live connection.
trait DatagramPath {
    /// Current datagram budget, or `None` when the peer does not accept
    /// datagrams. It follows the path MTU estimate, so it can shrink during a
    /// connection, for example after black-hole detection or migration.
    fn max_datagram_size(&self) -> Option<usize>;
    fn send_datagram(&self, datagram: &[u8]) -> Result<(), SendDatagramError>;
}

impl DatagramPath for Connection {
    fn max_datagram_size(&self) -> Option<usize> {
        Connection::max_datagram_size(self)
    }

    fn send_datagram(&self, datagram: &[u8]) -> Result<(), SendDatagramError> {
        Connection::send_datagram(self, datagram)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotDelivery {
    /// The frame fit the budget and was enqueued as one snapshot datagram.
    Whole,
    /// The frame was split and every fragment was enqueued.
    Fragmented,
    /// The path refused a datagram, for example because its budget shrank
    /// after it was read. The rest of the snapshot was not sent; the next
    /// snapshot reads the budget again.
    Abandoned,
    /// Nothing was sent: this connection already sent a frame for this tick or a newer one.
    NotNewer,
}

#[derive(Debug, Eq, PartialEq)]
enum SnapshotSendError {
    /// The peer no longer accepts datagrams.
    DatagramsUnavailable,
    /// The frame cannot be carried within the fragment bound at the current budget.
    Unsendable(ProtocolError),
}

/// Snapshot delivery state of one connection.
#[derive(Debug, Default)]
struct SnapshotSender {
    /// Tick of the newest frame this connection started to send.
    newest_sent_tick: Option<u64>,
}

impl SnapshotSender {
    /// Sends `frame` unless this connection already sent a frame for its tick
    /// or a newer one.
    ///
    /// A player-scoped projection is built when the connection handles a
    /// publication, not when the tick is published. A connection that falls
    /// one publication behind therefore builds the current tick twice, with
    /// different bytes when a command was applied in between. Fragments are
    /// identified by tick, so sending both frames would mix their fragments
    /// on the client. Clients drop a frame that is not newer anyway.
    fn send(
        &mut self,
        path: &impl DatagramPath,
        frame: &[u8],
    ) -> Result<SnapshotDelivery, SnapshotSendError> {
        let tick = snapshot_frame_tick(frame).map_err(SnapshotSendError::Unsendable)?;
        if self.newest_sent_tick.is_some_and(|newest| tick <= newest) {
            return Ok(SnapshotDelivery::NotNewer);
        }
        let delivery = send_snapshot(path, frame)?;
        self.newest_sent_tick = Some(tick);
        Ok(delivery)
    }
}

/// Enqueues one encoded snapshot frame, fragmenting it when it exceeds the
/// path's current datagram budget. Fails without sending anything when the
/// frame would need more than the maximum number of fragments.
fn send_snapshot(
    path: &impl DatagramPath,
    frame: &[u8],
) -> Result<SnapshotDelivery, SnapshotSendError> {
    let max_datagram_size = path
        .max_datagram_size()
        .ok_or(SnapshotSendError::DatagramsUnavailable)?;
    if frame.len() <= max_datagram_size {
        return Ok(match path.send_datagram(frame) {
            Ok(()) => SnapshotDelivery::Whole,
            Err(_) => SnapshotDelivery::Abandoned,
        });
    }
    let fragments = encode_snapshot_fragments(frame, max_datagram_size)
        .map_err(SnapshotSendError::Unsendable)?;
    for fragment in &fragments {
        if path.send_datagram(fragment).is_err() {
            // A snapshot missing any fragment cannot complete.
            return Ok(SnapshotDelivery::Abandoned);
        }
    }
    Ok(SnapshotDelivery::Fragmented)
}

async fn dispatch_control<S: GameSimulation>(
    runtime: Arc<Mutex<MatchRuntime<S>>>,
    context: ControlContext,
    control: Arc<dyn ControlService>,
    control_handlers: Arc<Semaphore>,
    payload: Vec<u8>,
) -> Result<Result<Vec<u8>, ControlServiceError>, String> {
    let handler_permit = control_handlers
        .acquire_owned()
        .await
        .map_err(|_| "reliable-control handler capacity closed".to_owned())?;
    // Dropping the exchange aborts work still queued in the blocking pool.
    // Already-running handlers retain their permits until they actually exit.
    let mut handler = JoinSet::new();
    handler.spawn_blocking(move || {
        let _handler_permit = handler_permit;
        runtime
            .blocking_lock()
            .validate_connection(context.player_id, context.connection_epoch)
            .map_err(|error| error.to_string())?;
        // External work must not hold the authoritative runtime lock. Handlers
        // still use the epoch to fence external effects committed after dispatch.
        Ok(control.handle(context, &payload))
    });
    handler
        .join_next()
        .await
        .expect("one control handler was spawned")
        .map_err(|error| format!("reliable-control handler task failed: {error}"))?
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
    use tokio::task::spawn_blocking;

    #[test]
    fn player_scoped_publication_never_broadcasts_canonical_payload() {
        let snapshot = SimulationSnapshot::new(7, b"canonical-private-state".to_vec());

        assert_eq!(
            snapshot_publication(SnapshotScope::PlayerScoped, snapshot).unwrap(),
            SnapshotPublication::PlayerScoped
        );
    }

    #[test]
    fn shared_publication_keeps_the_single_encode_fast_path() {
        let snapshot = SimulationSnapshot::new(7, b"shared-state".to_vec());

        assert!(matches!(
            snapshot_publication(SnapshotScope::Shared, snapshot).unwrap(),
            SnapshotPublication::Shared(_)
        ));
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
    #[derive(Default)]
    struct CountingControl(std::sync::atomic::AtomicUsize);

    impl ControlService for CountingControl {
        fn handle(
            &self,
            _context: ControlContext,
            payload: &[u8],
        ) -> Result<Vec<u8>, ControlServiceError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(payload.to_vec())
        }
    }

    #[tokio::test]
    async fn queued_control_rechecks_epoch_after_capacity_becomes_available() {
        let mut runtime = MatchRuntime::new(DemoSimulation::new(), 10);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let runtime = Arc::new(Mutex::new(runtime));
        let control = Arc::new(CountingControl::default());
        let capacity = Arc::new(Semaphore::new(0));
        let pending = dispatch_control(
            Arc::clone(&runtime),
            ControlContext {
                player_id: lease.player_id,
                connection_epoch: lease.connection_epoch,
            },
            control.clone(),
            Arc::clone(&capacity),
            b"old request".to_vec(),
        );
        tokio::pin!(pending);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;

        let replacement = {
            let mut runtime = runtime.lock().await;
            assert!(runtime.disconnect(lease.player_id, lease.connection_epoch));
            runtime
                .reconnect(
                    lease.reconnect_token,
                    ReconnectToken([2; RECONNECT_TOKEN_BYTES]),
                )
                .unwrap()
        };
        capacity.add_permits(1);
        assert!(
            pending.await.is_err(),
            "a queued request from the old epoch executed"
        );
        assert_eq!(control.0.load(std::sync::atomic::Ordering::SeqCst), 0);

        let response = dispatch_control(
            runtime,
            ControlContext {
                player_id: replacement.player_id,
                connection_epoch: replacement.connection_epoch,
            },
            control.clone(),
            capacity,
            b"current request".to_vec(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response, b"current request");
        assert_eq!(control.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn disconnected_and_replaced_leases_cannot_receive_shared_snapshots() {
        let mut runtime = MatchRuntime::new(DemoSimulation::new(), 10);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let snapshot = runtime.advance_tick().unwrap();
        let publication = snapshot_publication(runtime.snapshot_scope(), snapshot).unwrap();
        let expected = snapshot_for_connection(&runtime, lease, &publication).unwrap();
        assert!(runtime.disconnect(lease.player_id, lease.connection_epoch));
        assert!(snapshot_for_connection(&runtime, lease, &publication).is_err());
        let replacement = runtime
            .reconnect(
                lease.reconnect_token,
                ReconnectToken([2; RECONNECT_TOKEN_BYTES]),
            )
            .unwrap();
        assert!(snapshot_for_connection(&runtime, lease, &publication).is_err());
        assert_eq!(
            snapshot_for_connection(&runtime, replacement, &publication).unwrap(),
            expected
        );
    }
    #[test]
    fn cancelled_control_does_not_execute_from_blocking_queue() {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        executor.block_on(async {
            let (release, held) = tokio::sync::oneshot::channel::<()>();
            let blocker = spawn_blocking(move || held.blocking_recv().unwrap());
            let mut runtime = MatchRuntime::new(DemoSimulation::new(), 10);
            let lease = runtime
                .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
                .unwrap();
            let control = Arc::new(CountingControl::default());
            let capacity = Arc::new(Semaphore::new(1));
            {
                let pending = dispatch_control(
                    Arc::new(Mutex::new(runtime)),
                    ControlContext {
                        player_id: lease.player_id,
                        connection_epoch: lease.connection_epoch,
                    },
                    control.clone(),
                    Arc::clone(&capacity),
                    vec![],
                );
                tokio::pin!(pending);
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
            }
            release.send(()).unwrap();
            blocker.await.unwrap();
            // A sentinel completes after the queued blocking work is processed.
            spawn_blocking(|| ()).await.unwrap();
            assert_eq!(control.0.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(capacity.available_permits(), 1);
        });
    }
    struct PrivateSimulation {
        tick: u64,
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
        fn add_player(
            &mut self,
            _player_id: crate::PlayerId,
        ) -> Result<(), crate::SimulationError> {
            Ok(())
        }
        fn remove_player(&mut self, _player_id: crate::PlayerId) -> bool {
            true
        }
        fn apply_command(
            &mut self,
            _player_id: crate::PlayerId,
            _sequence: u32,
            _payload: &[u8],
        ) -> Result<(), crate::SimulationError> {
            Ok(())
        }
        fn advance_tick(&mut self) -> Result<(), crate::SimulationError> {
            self.tick += 1;
            Ok(())
        }
        fn snapshot_scope(&self) -> SnapshotScope {
            SnapshotScope::PlayerScoped
        }
        fn snapshot(&self) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(
                self.tick,
                b"canonical-private-state".to_vec(),
            ))
        }
        fn snapshot_for(
            &self,
            player_id: crate::PlayerId,
        ) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(
                self.tick,
                player_id.to_be_bytes().to_vec(),
            ))
        }
    }

    #[test]
    fn private_publication_projects_only_for_current_connections() {
        let mut runtime = MatchRuntime::new(PrivateSimulation { tick: 0 }, 10);
        let first = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let second = runtime
            .admit(ReconnectToken([2; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let canonical = runtime.advance_tick().unwrap();
        let publication = snapshot_publication(runtime.snapshot_scope(), canonical).unwrap();
        assert_eq!(publication, SnapshotPublication::PlayerScoped);
        for lease in [first, second] {
            let frame = snapshot_for_connection(&runtime, lease, &publication).unwrap();
            let visible = crate::decode_snapshot(&frame).unwrap();
            assert_eq!(visible.tick, 1);
            assert_eq!(visible.payload, lease.player_id.to_be_bytes());
        }
        assert!(runtime.disconnect(first.player_id, first.connection_epoch));
        assert!(snapshot_for_connection(&runtime, first, &publication).is_err());
        let replacement = runtime
            .reconnect(
                first.reconnect_token,
                ReconnectToken([3; RECONNECT_TOKEN_BYTES]),
            )
            .unwrap();
        assert!(snapshot_for_connection(&runtime, first, &publication).is_err());
        assert!(snapshot_for_connection(&runtime, replacement, &publication).is_ok());
    }

    struct BlockingControl {
        entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }

    impl ControlService for BlockingControl {
        fn handle(
            &self,
            _context: ControlContext,
            _payload: &[u8],
        ) -> Result<Vec<u8>, ControlServiceError> {
            self.entered
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .blocking_recv()
                .unwrap();
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn running_control_retains_capacity_without_blocking_authoritative_ticks() {
        let mut runtime = MatchRuntime::new(DemoSimulation::new(), 10);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let runtime = Arc::new(Mutex::new(runtime));
        let capacity = Arc::new(Semaphore::new(1));
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        {
            let pending = dispatch_control(
                Arc::clone(&runtime),
                ControlContext {
                    player_id: lease.player_id,
                    connection_epoch: lease.connection_epoch,
                },
                Arc::new(BlockingControl {
                    entered: std::sync::Mutex::new(Some(entered)),
                    release: std::sync::Mutex::new(Some(held)),
                }),
                Arc::clone(&capacity),
                vec![],
            );
            tokio::pin!(pending);
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            started.await.unwrap();
        }
        // The exchange is cancelled, but the synchronous handler is still running.
        assert_eq!(capacity.available_permits(), 0);
        assert_eq!(runtime.try_lock().unwrap().advance_tick().unwrap().tick, 1);
        release.send(()).unwrap();
        let _released = capacity.acquire().await.unwrap();
    }

    const LARGE_PROJECTION_BYTES: usize = 12_000;

    fn large_projection(player_id: crate::PlayerId, tick: u64) -> Vec<u8> {
        (0..LARGE_PROJECTION_BYTES)
            .map(|index| (index as u64 ^ tick ^ u64::from(player_id)).to_le_bytes()[0])
            .collect()
    }

    struct LargeProjectionSimulation {
        tick: u64,
    }

    impl GameSimulation for LargeProjectionSimulation {
        fn tick_hz(&self) -> u16 {
            50
        }
        fn max_players(&self) -> usize {
            1
        }
        fn current_tick(&self) -> u64 {
            self.tick
        }
        fn add_player(
            &mut self,
            _player_id: crate::PlayerId,
        ) -> Result<(), crate::SimulationError> {
            Ok(())
        }
        fn remove_player(&mut self, _player_id: crate::PlayerId) -> bool {
            true
        }
        fn apply_command(
            &mut self,
            _player_id: crate::PlayerId,
            _sequence: u32,
            _payload: &[u8],
        ) -> Result<(), crate::SimulationError> {
            Ok(())
        }
        fn advance_tick(&mut self) -> Result<(), crate::SimulationError> {
            self.tick += 1;
            Ok(())
        }
        fn snapshot_scope(&self) -> SnapshotScope {
            SnapshotScope::PlayerScoped
        }
        fn snapshot(&self) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(self.tick, b"canonical".to_vec()))
        }
        fn snapshot_for(
            &self,
            player_id: crate::PlayerId,
        ) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(
                self.tick,
                large_projection(player_id, self.tick),
            ))
        }
    }

    #[tokio::test]
    async fn snapshot_larger_than_the_datagram_budget_reaches_a_webtransport_client() {
        use crate::control::RejectControlService;
        use crate::protocol::{WELCOME_BYTES, decode_welcome};
        use crate::reassembly::SnapshotReassembler;
        use std::net::{Ipv4Addr, SocketAddr};
        use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};

        let identity = Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
        let certificate_hash = identity.certificate_chain().as_slice()[0].hash();
        let server = Endpoint::server(
            ServerConfig::builder()
                .with_bind_address(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .with_identity(identity)
                .build(),
        )
        .unwrap();
        let port = server.local_addr().unwrap().port();
        let (snapshots, _) = broadcast::channel(SNAPSHOT_CHANNEL_DEPTH);
        let (shutdown, _) = broadcast::channel(1);
        let state = ConnectionState {
            runtime: Arc::new(Mutex::new(MatchRuntime::new(
                LargeProjectionSimulation { tick: 0 },
                10,
            ))),
            control: Arc::new(RejectControlService),
            control_handlers: Arc::new(Semaphore::new(1)),
            snapshots,
            admission_gate: None,
            shutdown,
        };
        let mut server_tasks = JoinSet::new();
        let connection_state = state.clone();
        server_tasks.spawn(async move {
            let request = server.accept().await.await.unwrap();
            let connection = request.accept().await.unwrap();
            handle_connection(connection, connection_state, AdmissionRequest::New)
                .await
                .unwrap();
        });
        server_tasks.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(20));
            loop {
                ticker.tick().await;
                let snapshot = state.runtime.lock().await.advance_tick().unwrap();
                let publication =
                    snapshot_publication(SnapshotScope::PlayerScoped, snapshot).unwrap();
                let _ = state.snapshots.send(publication);
            }
        });

        let client = Endpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_server_certificate_hashes([certificate_hash])
                .build(),
        )
        .unwrap();
        let (welcome, snapshot, budget, stats) =
            tokio::time::timeout(Duration::from_secs(20), async {
                let connection = client
                    .connect(format!("https://127.0.0.1:{port}/game"))
                    .await
                    .unwrap();
                let mut welcome_stream = connection.accept_uni().await.unwrap();
                let mut welcome = [0_u8; WELCOME_BYTES];
                welcome_stream.read_exact(&mut welcome).await.unwrap();
                let welcome = decode_welcome(&welcome).unwrap();

                let mut reassembler = SnapshotReassembler::new();
                let snapshot = loop {
                    let datagram = connection.receive_datagram().await.unwrap();
                    if let Some(snapshot) = reassembler.accept(&datagram).unwrap() {
                        break snapshot;
                    }
                };
                let budget = connection.max_datagram_size().unwrap();
                connection.close(VarInt::from_u32(0), b"done");
                (welcome, snapshot, budget, reassembler.stats())
            })
            .await
            .expect("a fragmented snapshot must reach the client");
        server_tasks.shutdown().await;

        assert!(
            LARGE_PROJECTION_BYTES > 4 * budget,
            "projection must exceed the datagram budget ({budget} bytes)"
        );
        assert_eq!(
            snapshot.payload,
            large_projection(welcome.player_id, snapshot.tick)
        );
        assert_eq!(stats.whole_snapshots, 0);
        assert_eq!(stats.reassembled_snapshots, 1);
        assert!(stats.buffered_fragments >= 5);
    }

    /// A datagram path that records what it enqueues and, like quinn, refuses
    /// datagrams above its current budget.
    struct RecordingPath {
        /// Budget returned by `max_datagram_size`.
        reported: std::cell::Cell<Option<usize>>,
        /// Budget enforced by `send_datagram`; differs from `reported` only to
        /// model a budget that shrinks between reading it and sending.
        enforced: std::cell::Cell<Option<usize>>,
        sent: std::cell::RefCell<Vec<Vec<u8>>>,
    }

    impl RecordingPath {
        fn new(budget: usize) -> Self {
            Self {
                reported: std::cell::Cell::new(Some(budget)),
                enforced: std::cell::Cell::new(Some(budget)),
                sent: std::cell::RefCell::new(Vec::new()),
            }
        }

        fn set_budget(&self, budget: Option<usize>) {
            self.reported.set(budget);
            self.enforced.set(budget);
        }

        fn take_sent(&self) -> Vec<Vec<u8>> {
            self.sent.take()
        }
    }

    impl DatagramPath for RecordingPath {
        fn max_datagram_size(&self) -> Option<usize> {
            self.reported.get()
        }

        fn send_datagram(&self, datagram: &[u8]) -> Result<(), SendDatagramError> {
            match self.enforced.get() {
                None => Err(SendDatagramError::UnsupportedByPeer),
                Some(budget) if datagram.len() > budget => Err(SendDatagramError::TooLarge),
                Some(_) => {
                    self.sent.borrow_mut().push(datagram.to_vec());
                    Ok(())
                }
            }
        }
    }

    fn encoded_frame(tick: u64, payload_len: usize) -> Vec<u8> {
        encode_simulation_snapshot(SimulationSnapshot::new(
            tick,
            (0..payload_len).map(|index| index as u8).collect(),
        ))
        .unwrap()
    }

    #[test]
    fn a_frame_at_the_datagram_budget_is_sent_as_one_unchanged_snapshot_datagram() {
        let frame = encoded_frame(3, 1_000);
        let path = RecordingPath::new(frame.len());

        assert_eq!(send_snapshot(&path, &frame), Ok(SnapshotDelivery::Whole));

        // Version-3 clients without fragment support decode this datagram as before.
        let sent = path.take_sent();
        assert_eq!(sent, std::slice::from_ref(&frame));
        assert_eq!(
            crate::decode_snapshot(&sent[0]).unwrap(),
            crate::decode_snapshot(&frame).unwrap()
        );
        let mut reassembler = crate::SnapshotReassembler::new();
        assert!(reassembler.accept(&sent[0]).unwrap().is_some());
        assert_eq!(reassembler.stats().whole_snapshots, 1);
        assert_eq!(reassembler.stats().reassembled_snapshots, 0);
    }

    #[test]
    fn a_frame_one_byte_over_the_datagram_budget_is_fragmented() {
        let frame = encoded_frame(3, 1_000);
        let budget = frame.len() - 1;
        let path = RecordingPath::new(budget);

        assert_eq!(
            send_snapshot(&path, &frame),
            Ok(SnapshotDelivery::Fragmented)
        );

        let sent = path.take_sent();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().all(|datagram| datagram.len() <= budget));
        assert!(crate::decode_snapshot(&sent[0]).is_err());
        let mut reassembler = crate::SnapshotReassembler::new();
        assert_eq!(reassembler.accept(&sent[0]).unwrap(), None);
        assert_eq!(
            reassembler.accept(&sent[1]).unwrap().unwrap(),
            crate::decode_snapshot(&frame).unwrap()
        );
        assert_eq!(reassembler.stats().whole_snapshots, 0);
        assert_eq!(reassembler.stats().reassembled_snapshots, 1);
    }

    fn reassemble(datagrams: &[Vec<u8>]) -> Vec<SnapshotFrame> {
        let mut reassembler = crate::SnapshotReassembler::new();
        datagrams
            .iter()
            .filter_map(|datagram| reassembler.accept(datagram).unwrap())
            .collect()
    }

    #[test]
    fn snapshot_fragments_follow_a_datagram_budget_that_shrinks_between_ticks() {
        let path = RecordingPath::new(1_400);
        let first = encoded_frame(1, 6_000);
        assert_eq!(
            send_snapshot(&path, &first),
            Ok(SnapshotDelivery::Fragmented)
        );
        let sent = path.take_sent();
        assert!(sent.iter().any(|datagram| datagram.len() > 1_150));
        assert_eq!(reassemble(&sent), [crate::decode_snapshot(&first).unwrap()]);

        // Black-hole detection or migration drops the path MTU after admission.
        path.set_budget(Some(1_150));
        let second = encoded_frame(2, 6_000);
        assert_eq!(
            send_snapshot(&path, &second),
            Ok(SnapshotDelivery::Fragmented)
        );
        let sent = path.take_sent();
        assert!(sent.iter().all(|datagram| datagram.len() <= 1_150));
        assert_eq!(
            reassemble(&sent),
            [crate::decode_snapshot(&second).unwrap()]
        );
    }

    #[test]
    fn a_budget_that_shrinks_while_sending_abandons_the_rest_of_the_snapshot() {
        let path = RecordingPath::new(1_400);
        path.enforced.set(Some(1_150));

        assert_eq!(
            send_snapshot(&path, &encoded_frame(1, 6_000)),
            Ok(SnapshotDelivery::Abandoned)
        );
        assert!(path.take_sent().is_empty());

        // The next snapshot reads the smaller budget and arrives whole.
        path.reported.set(Some(1_150));
        let next = encoded_frame(2, 6_000);
        assert_eq!(
            send_snapshot(&path, &next),
            Ok(SnapshotDelivery::Fragmented)
        );
        assert_eq!(
            reassemble(&path.take_sent()),
            [crate::decode_snapshot(&next).unwrap()]
        );
    }

    #[test]
    fn snapshots_fail_closed_without_datagrams_or_within_the_fragment_bound() {
        let path = RecordingPath::new(1_200);
        path.set_budget(None);
        assert_eq!(
            send_snapshot(&path, &encoded_frame(1, 10)),
            Err(SnapshotSendError::DatagramsUnavailable)
        );

        let budget = crate::MIN_FRAGMENTED_DATAGRAM_BYTES - 1;
        path.set_budget(Some(budget));
        assert!(matches!(
            send_snapshot(&path, &encoded_frame(2, crate::MAX_SNAPSHOT_PAYLOAD_BYTES)),
            Err(SnapshotSendError::Unsendable(
                ProtocolError::TooManyFragments { .. }
            ))
        ));
        assert!(path.take_sent().is_empty());
    }

    /// Player-scoped simulation whose projection changes with every applied
    /// command, without advancing the tick.
    struct CommandEchoSimulation {
        tick: u64,
        applied_commands: u8,
    }

    impl GameSimulation for CommandEchoSimulation {
        fn tick_hz(&self) -> u16 {
            20
        }
        fn max_players(&self) -> usize {
            1
        }
        fn current_tick(&self) -> u64 {
            self.tick
        }
        fn add_player(
            &mut self,
            _player_id: crate::PlayerId,
        ) -> Result<(), crate::SimulationError> {
            Ok(())
        }
        fn remove_player(&mut self, _player_id: crate::PlayerId) -> bool {
            true
        }
        fn apply_command(
            &mut self,
            _player_id: crate::PlayerId,
            _sequence: u32,
            _payload: &[u8],
        ) -> Result<(), crate::SimulationError> {
            self.applied_commands += 1;
            Ok(())
        }
        fn advance_tick(&mut self) -> Result<(), crate::SimulationError> {
            self.tick += 1;
            Ok(())
        }
        fn snapshot_scope(&self) -> SnapshotScope {
            SnapshotScope::PlayerScoped
        }
        fn snapshot(&self) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(self.tick, b"canonical".to_vec()))
        }
        fn snapshot_for(
            &self,
            _player_id: crate::PlayerId,
        ) -> Result<SimulationSnapshot, crate::SimulationError> {
            Ok(SimulationSnapshot::new(
                self.tick,
                vec![self.applied_commands; 3_000],
            ))
        }
    }

    #[test]
    fn a_connection_sends_at_most_one_frame_per_tick() {
        let mut runtime = MatchRuntime::new(
            CommandEchoSimulation {
                tick: 0,
                applied_commands: 0,
            },
            10,
        );
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        let path = RecordingPath::new(1_100);
        let mut sender = SnapshotSender::default();

        // The connection is one publication behind: the runtime already
        // advanced to tick 1 and published twice when the task catches up.
        runtime.advance_tick().unwrap();
        let publication = SnapshotPublication::PlayerScoped;
        let first = snapshot_for_connection(&runtime, lease, &publication)
            .unwrap()
            .into_owned();
        assert_eq!(sender.send(&path, &first), Ok(SnapshotDelivery::Fragmented));
        // A command applied before the queued publication changes the projection
        // without advancing the tick.
        runtime
            .submit_command(lease.player_id, lease.connection_epoch, 1, b"move")
            .unwrap();
        let changed = snapshot_for_connection(&runtime, lease, &publication)
            .unwrap()
            .into_owned();
        assert_ne!(changed, first);
        assert_eq!(
            crate::decode_snapshot(&changed).unwrap().tick,
            crate::decode_snapshot(&first).unwrap().tick
        );
        assert_eq!(sender.send(&path, &changed), Ok(SnapshotDelivery::NotNewer));

        // Every fragment of tick 1 comes from one frame, so losing one fragment
        // loses only that snapshot instead of corrupting a mixed reassembly.
        let mut sent = path.take_sent();
        assert_eq!(sent.len(), 3);
        sent.remove(1);
        assert!(reassemble(&sent).is_empty());

        runtime.advance_tick().unwrap();
        let next = snapshot_for_connection(&runtime, lease, &publication)
            .unwrap()
            .into_owned();
        assert_eq!(sender.send(&path, &next), Ok(SnapshotDelivery::Fragmented));
        assert_eq!(
            reassemble(&path.take_sent()),
            [crate::decode_snapshot(&next).unwrap()]
        );
        // An older frame is never sent after a newer one.
        assert_eq!(sender.send(&path, &first), Ok(SnapshotDelivery::NotNewer));
        assert!(path.take_sent().is_empty());
    }

    #[test]
    fn shared_snapshot_fanout_reuses_payload_storage() {
        let payload = vec![42; 1024];
        let publication = snapshot_publication(
            SnapshotScope::Shared,
            SimulationSnapshot::new(7, payload.clone()),
        )
        .unwrap();
        let (sender, _) = broadcast::channel(1);
        let mut first = sender.subscribe();
        let mut second = sender.subscribe();
        sender.send(publication).unwrap();
        let SnapshotPublication::Shared(first) = first.try_recv().unwrap() else {
            panic!("expected shared snapshot")
        };
        let SnapshotPublication::Shared(second) = second.try_recv().unwrap() else {
            panic!("expected shared snapshot")
        };
        assert!(
            Arc::ptr_eq(&first, &second),
            "fan-out copied the encoded payload"
        );
        drop(sender);
        let decoded = crate::decode_snapshot(&second).unwrap();
        assert_eq!(decoded.tick, 7);
        assert_eq!(decoded.payload, payload);
    }
}
