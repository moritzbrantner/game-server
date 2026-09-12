use game_server::{
    DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation, WebTransportConfig, serve_with_shutdown,
};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

const DEFAULT_PORT: u16 = 4433;
const DEFAULT_DRAIN_GRACE_MS: u64 = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = env::var("GAME_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let certificate_pem =
        PathBuf::from(env::var("GAME_SERVER_CERT_PEM").unwrap_or_else(|_| "cert.pem".to_owned()));
    let private_key_pem =
        PathBuf::from(env::var("GAME_SERVER_KEY_PEM").unwrap_or_else(|_| "key.pem".to_owned()));
    let session_path = env::var("GAME_SERVER_SESSION_PATH").unwrap_or_else(|_| "/game".to_owned());
    let recovery_path = env::var("GAME_SERVER_RECOVERY_PATH").ok().map(PathBuf::from);
    let drain_grace_ms = env::var("GAME_SERVER_DRAIN_GRACE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DRAIN_GRACE_MS);

    let (shutdown_sender, shutdown_receiver) = mpsc::channel(4);
    install_shutdown_forwarder(shutdown_sender)?;

    serve_with_shutdown(
        DemoSimulation::new(),
        DEFAULT_RECONNECT_GRACE_TICKS,
        WebTransportConfig {
            port,
            certificate_pem,
            private_key_pem,
            session_path,
            recovery_path,
            drain_grace: Duration::from_millis(drain_grace_ms),
        },
        shutdown_receiver,
    )
    .await?;
    Ok(())
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
