use game_server::{
    DemoWorld, MAX_PLAYERS, RECONNECT_TOKEN_BYTES, ReconnectToken, SessionLease, SessionRegistry,
    TICK_HZ, Welcome, decode_input, encode_snapshot, encode_welcome,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};
use tokio::time::MissedTickBehavior;
use wtransport::{Connection, Endpoint, Identity, ServerConfig, VarInt};

const DEFAULT_PORT: u16 = 4433;
const SESSION_PATH: &str = "/game";
const RECONNECT_PATH_PREFIX: &str = "/game/reconnect/";
const CLOSE_PROTOCOL: u32 = 1;
const CLOSE_CAPACITY: u32 = 2;
const CLOSE_DATAGRAM: u32 = 3;
const CLOSE_SERVER: u32 = 4;
const CLOSE_SESSION: u32 = 5;
const SNAPSHOT_CHANNEL_DEPTH: usize = 1;
const MAX_SNAPSHOT_BYTES: usize = 19 + MAX_PLAYERS * 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionRequest {
    New,
    Reconnect(ReconnectToken),
}

#[derive(Clone)]
struct ServerState {
    world: Arc<Mutex<DemoWorld>>,
    sessions: Arc<Mutex<SessionRegistry>>,
    snapshots: broadcast::Sender<Vec<u8>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = env::var("GAME_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let certificate = env::var("GAME_SERVER_CERT_PEM").unwrap_or_else(|_| "cert.pem".to_owned());
    let private_key = env::var("GAME_SERVER_KEY_PEM").unwrap_or_else(|_| "key.pem".to_owned());

    let identity = Identity::load_pemfiles(&certificate, &private_key).await?;
    let config = ServerConfig::builder()
        .with_bind_default(port)
        .with_identity(identity)
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .build();
    let endpoint = Endpoint::server(config)?;

    let (snapshots, _) = broadcast::channel::<Vec<u8>>(SNAPSHOT_CHANNEL_DEPTH);
    let state = ServerState {
        world: Arc::new(Mutex::new(DemoWorld::new())),
        sessions: Arc::new(Mutex::new(SessionRegistry::default())),
        snapshots,
    };
    spawn_tick_loop(state.clone());

    eprintln!("game-server listening on https://localhost:{port}{SESSION_PATH}");

    loop {
        let incoming = endpoint.accept().await;
        let state = state.clone();
        tokio::spawn(async move {
            let request = match incoming.await {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("WebTransport negotiation failed: {error}");
                    return;
                }
            };
            let Some(admission) = parse_admission_request(request.path()) else {
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

fn spawn_tick_loop(state: ServerState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(1_000 / u64::from(TICK_HZ)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let current_tick = state.world.lock().await.tick();
            let expired = state.sessions.lock().await.expire(current_tick);
            let encoded = {
                let mut world = state.world.lock().await;
                for player_id in expired {
                    world.remove_player(player_id);
                }
                match world.advance_tick() {
                    Ok(snapshot) => match encode_snapshot(&snapshot) {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            eprintln!("snapshot encoding failed: {error}");
                            continue;
                        }
                    },
                    Err(error) => {
                        eprintln!("authoritative tick failed: {error}");
                        continue;
                    }
                }
            };
            let _ = state.snapshots.send(encoded);
        }
    });
}

async fn handle_connection(
    connection: Connection,
    state: ServerState,
    admission: AdmissionRequest,
) -> Result<(), String> {
    let max_datagram_size = match connection.max_datagram_size() {
        Some(max_datagram_size) if max_datagram_size >= MAX_SNAPSHOT_BYTES => max_datagram_size,
        Some(max_datagram_size) => {
            close(
                &connection,
                CLOSE_DATAGRAM,
                &format!("datagram budget {max_datagram_size} is below {MAX_SNAPSHOT_BYTES}"),
            );
            return Ok(());
        }
        None => {
            close(
                &connection,
                CLOSE_DATAGRAM,
                "WebTransport datagrams are required",
            );
            return Ok(());
        }
    };

    let current_tick = state.world.lock().await.tick();
    let replacement_token = generate_reconnect_token()?;
    let lease = {
        let mut sessions = state.sessions.lock().await;
        match admission {
            AdmissionRequest::New => sessions.admit(replacement_token),
            AdmissionRequest::Reconnect(previous_token) => {
                sessions.reconnect(previous_token, replacement_token, current_tick)
            }
        }
    };
    let lease = match lease {
        Ok(lease) => lease,
        Err(error) => {
            close(&connection, CLOSE_SESSION, &error.to_string());
            return Ok(());
        }
    };

    if admission == AdmissionRequest::New {
        let add_result = state.world.lock().await.add_player(lease.player_id);
        if let Err(error) = add_result {
            state.sessions.lock().await.remove_slot(lease.player_id);
            close(&connection, CLOSE_CAPACITY, &error.to_string());
            return Ok(());
        }
    }

    let result =
        run_admitted_connection(&connection, lease, current_tick, max_datagram_size, &state).await;
    let disconnect_tick = state.world.lock().await.tick();
    state.sessions.lock().await.disconnect(
        lease.player_id,
        lease.connection_epoch,
        disconnect_tick,
    );
    result
}

async fn run_admitted_connection(
    connection: &Connection,
    lease: SessionLease,
    current_tick: u64,
    max_datagram_size: usize,
    state: &ServerState,
) -> Result<(), String> {
    let welcome = encode_welcome(Welcome {
        player_id: lease.player_id,
        tick_hz: TICK_HZ,
        max_players: MAX_PLAYERS as u8,
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
                    Ok(datagram) => match decode_input(datagram.as_ref()) {
                        Ok(input) => {
                            if let Err(error) = state.world.lock().await.submit_input(lease.player_id, input) {
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

fn parse_admission_request(path: &str) -> Option<AdmissionRequest> {
    if path == SESSION_PATH {
        return Some(AdmissionRequest::New);
    }
    let token = path.strip_prefix(RECONNECT_PATH_PREFIX)?;
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
            parse_admission_request("/game"),
            Some(AdmissionRequest::New)
        );
        let token = ReconnectToken([0xab; RECONNECT_TOKEN_BYTES]);
        assert_eq!(
            parse_admission_request(&format!("/game/reconnect/{}", token.encode_hex())),
            Some(AdmissionRequest::Reconnect(token))
        );
        assert_eq!(parse_admission_request("/game/reconnect/not-valid"), None);
        assert_eq!(parse_admission_request("/other"), None);
    }
}
