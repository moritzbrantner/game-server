use game_server::{DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation, WebTransportConfig, serve};
use std::env;
use std::error::Error;
use std::path::PathBuf;

const DEFAULT_PORT: u16 = 4433;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = env::var("GAME_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let certificate_pem = PathBuf::from(
        env::var("GAME_SERVER_CERT_PEM").unwrap_or_else(|_| "cert.pem".to_owned()),
    );
    let private_key_pem = PathBuf::from(
        env::var("GAME_SERVER_KEY_PEM").unwrap_or_else(|_| "key.pem".to_owned()),
    );
    let session_path = env::var("GAME_SERVER_SESSION_PATH").unwrap_or_else(|_| "/game".to_owned());

    serve(
        DemoSimulation::new(),
        DEFAULT_RECONNECT_GRACE_TICKS,
        WebTransportConfig {
            port,
            certificate_pem,
            private_key_pem,
            session_path,
        },
    )
    .await?;
    Ok(())
}
