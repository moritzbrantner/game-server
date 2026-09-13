use game_server::{
    BrowserRoutePrefix, ControlContext, ControlService, ControlServiceError,
    DEFAULT_HOST_STATUS_PORT, DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation, MatchControlService,
    MatchHost, MatchHostRecoveryConfig, MatchHostStatusConfig, MatchHostWebTransportConfig, MatchId,
    MatchRuntime, WebTransportConfig, prepare_match_host_for_recovery,
    serve_match_host_with_status_and_control_and_shutdown,
    serve_prepared_match_host_with_status_and_control_and_shutdown, serve_with_control_and_shutdown,
};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

const DEFAULT_PORT: u16 = 4433;
const DEFAULT_DRAIN_GRACE_MS: u64 = 500;

#[derive(Clone, Copy, Debug, Default)]
struct DemoControlService;

impl ControlService for DemoControlService {
    fn handle(
        &self,
        _context: ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError> {
        handle_demo_control(payload)
    }
}

impl MatchControlService for DemoControlService {
    fn handle(
        &self,
        match_id: &MatchId,
        _context: ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError> {
        if payload == b"match-id" {
            return Ok(match_id.as_str().as_bytes().to_vec());
        }
        handle_demo_control(payload)
    }
}

fn handle_demo_control(payload: &[u8]) -> Result<Vec<u8>, ControlServiceError> {
    match payload {
        b"ping" => Ok(b"pong".to_vec()),
        b"reject" => Err(ControlServiceError::new("demo control request rejected")),
        _ => Err(ControlServiceError::new("unsupported demo control request")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = env::var("GAME_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let status_port = env::var("GAME_SERVER_STATUS_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_HOST_STATUS_PORT);
    let certificate_pem =
        PathBuf::from(env::var("GAME_SERVER_CERT_PEM").unwrap_or_else(|_| "cert.pem".to_owned()));
    let private_key_pem =
        PathBuf::from(env::var("GAME_SERVER_KEY_PEM").unwrap_or_else(|_| "key.pem".to_owned()));
    let session_path = env::var("GAME_SERVER_SESSION_PATH").unwrap_or_else(|_| "/game".to_owned());
    let hosted_match_ids = read_hosted_match_ids()?;
    let single_match_id = read_single_match_id()?;
    if hosted_match_ids.is_some() && single_match_id.is_some() {
        return Err("GAME_SERVER_MATCH_IDS and GAME_SERVER_MATCH_ID are mutually exclusive".into());
    }
    let recovery_path = env::var("GAME_SERVER_RECOVERY_PATH")
        .ok()
        .map(PathBuf::from);
    let recovery_directory = env::var("GAME_SERVER_RECOVERY_DIR")
        .ok()
        .map(PathBuf::from);
    if recovery_path.is_some() && recovery_directory.is_some() {
        return Err(
            "GAME_SERVER_RECOVERY_PATH and GAME_SERVER_RECOVERY_DIR are mutually exclusive".into(),
        );
    }
    let drain_grace_ms = env::var("GAME_SERVER_DRAIN_GRACE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DRAIN_GRACE_MS);
    let drain_grace = Duration::from_millis(drain_grace_ms);

    let (shutdown_sender, shutdown_receiver) = mpsc::channel(4);
    install_shutdown_forwarder(shutdown_sender)?;

    if let Some(match_ids) = hosted_match_ids {
        if recovery_path.is_some() {
            return Err(
                "GAME_SERVER_RECOVERY_PATH is single-match only; use GAME_SERVER_RECOVERY_DIR with GAME_SERVER_MATCH_IDS"
                    .into(),
            );
        }
        let route_prefix = BrowserRoutePrefix::new(session_path)?;
        let transport_config = MatchHostWebTransportConfig {
            port,
            certificate_pem,
            private_key_pem,
            route_prefix,
            drain_grace,
        };
        let status_config = MatchHostStatusConfig { port: status_port };

        if let Some(directory) = recovery_directory {
            let max_matches = match_ids.len();
            let matches = match_ids
                .into_iter()
                .map(|match_id| (match_id, DemoSimulation::new()))
                .collect();
            let prepared = prepare_match_host_for_recovery(
                matches,
                max_matches,
                DEFAULT_RECONNECT_GRACE_TICKS,
                MatchHostRecoveryConfig { directory },
            )
            .await?;
            serve_prepared_match_host_with_status_and_control_and_shutdown(
                prepared,
                DemoControlService,
                transport_config,
                status_config,
                shutdown_receiver,
            )
            .await?;
        } else {
            let mut host = MatchHost::new(match_ids.len())?;
            for match_id in match_ids {
                host.insert(
                    match_id,
                    MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS),
                )?;
            }
            serve_match_host_with_status_and_control_and_shutdown(
                host,
                DemoControlService,
                transport_config,
                status_config,
                shutdown_receiver,
            )
            .await?;
        }
    } else {
        if recovery_directory.is_some() {
            return Err(
                "GAME_SERVER_RECOVERY_DIR requires GAME_SERVER_MATCH_IDS; use GAME_SERVER_RECOVERY_PATH for single-match serving"
                    .into(),
            );
        }
        let session_path = match single_match_id {
            Some(match_id) => {
                let route_prefix = BrowserRoutePrefix::new(session_path)?;
                route_prefix.match_path(&match_id)
            }
            None => session_path,
        };
        serve_with_control_and_shutdown(
            DemoSimulation::new(),
            DemoControlService,
            DEFAULT_RECONNECT_GRACE_TICKS,
            WebTransportConfig {
                port,
                certificate_pem,
                private_key_pem,
                session_path,
                recovery_path,
                drain_grace,
            },
            shutdown_receiver,
        )
        .await?;
    }
    Ok(())
}

fn read_single_match_id() -> Result<Option<MatchId>, Box<dyn Error>> {
    match env::var("GAME_SERVER_MATCH_ID") {
        Ok(value) => Ok(Some(MatchId::new(value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_hosted_match_ids() -> Result<Option<Vec<MatchId>>, Box<dyn Error>> {
    let value = match env::var("GAME_SERVER_MATCH_IDS") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if value.is_empty() {
        return Err("GAME_SERVER_MATCH_IDS must not be empty".into());
    }
    let match_ids = value
        .split(',')
        .map(MatchId::new)
        .collect::<Result<Vec<_>, _>>()?;
    if match_ids.is_empty() {
        return Err("GAME_SERVER_MATCH_IDS must contain at least one match".into());
    }
    Ok(Some(match_ids))
}

#[cfg(unix)]
fn install_shutdown_forwarder(sender: mpsc::Sender<()>) -> Result<(), Box<dyn Error>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        loop {
            let signal_received = tokio::select! {
                result = tokio::signal::ctrl_c() => result.is_ok(),
                received = terminate.recv() => received.is_some(),
            };
            if !signal_received || sender.send(()).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}

#[cfg(not(unix))]
fn install_shutdown_forwarder(sender: mpsc::Sender<()>) -> Result<(), Box<dyn Error>> {
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() || sender.send(()).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}
