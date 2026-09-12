use crate::protocol::{
    RECONNECT_TOKEN_BYTES, SnapshotFrame, Welcome, decode_command, encode_snapshot, encode_welcome,
};
use crate::runtime::MatchRuntime;
use crate::session::{ReconnectToken, SessionLease};
use crate::simulation::GameSimulation;
use ring::rand::{SecureRandom, SystemRandom};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tokio::time::MissedTickBehavior;
use wtransport::{Connection, Endpoint, Identity, ServerConfig, VarInt};

const CLOSE_PROTOCOL: u32 = 1;
const CLOSE_DATAGRAM: u32 = 2;
const CLOSE_RUNTIME: u32 = 3;
const CLOSE_SERVER: u32 = 4;
const SNAPSHOT_CHANNEL_DEPTH: usize = 1;

#[derive(Clone, Debug)]
pub struct WebTransportConfig {
    pub port: u16,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
    pub session_path: String,
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
    InvalidTickRate,
    PlayerCapacityTooLarge(usize),
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(formatter, "TLS identity error: {error}"),
            Self::Endpoint(error) => write!(formatter, "WebTransport endpoint error: {error}"),
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
    snapshots: broadcast::Sender<Vec<u8>>,
}

impl<S> Clone for ServerState<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            snapshots: self.snapshots.clone(),
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

    let tick_hz = simulation.tick_hz();
    let (snapshots, _) = broadcast::channel::<Vec<u8>>(SNAPSHOT_CHANNEL_DEPTH);
    let state = ServerState {
        runtime: Arc::new(Mutex::new(MatchRuntime::new(
            simulation,
            reconnect_grace_ticks,
        ))),
        snapshots,
    };
    spawn_tick_loop(state.clone(), tick_hz);

    loop {
        let incoming = endpoint.accept().await;
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
    }
}

fn spawn_tick_loop<S: GameSimulation>(state: ServerState<S>, tick_hz: u16) {
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_micros(1_000_000_u64 / u64::from(tick_hz)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let snapshot = match state.runtime.lock().await.advance_tick() {
                Ok(snapshot) => snapshot,
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
    });
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

    let result = run_admitted_connection(&connection, lease, max_datagram_size, &state).await;
    state
        .runtime
        .lock()
        .await
        .disconnect(lease.player_id, lease.connection_epoch);
    result
}

async fn run_admitted_connection<S: GameSimulation>(
    connection: &Connection,
    lease: SessionLease,
    max_datagram_size: usize,
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

    let mut snapshots = state.snapshots.subscribe();
    loop {
        tokio::select! {
            datagram = connection.receive_datagram() => {
                match datagram {
                    Ok(datagram) => match decode_command(datagram.as_ref()) {
                        Ok(command) => {
                            if let Err(error) = state.runtime.lock().await.submit_command(
                                lease.player_id,
                                lease.connection_epoch,
                                command.sequence,
                                &command.payload,
                            ) {
                                close(connection, CLOSE_PROTOCOL, &error.to_string());
                                return Ok(());
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
            _ = connection.closed() => return Ok(()),
        }
    }
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
}
