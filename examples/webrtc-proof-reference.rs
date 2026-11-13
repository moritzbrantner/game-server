//! The WebRTC probe's semantic trace through the unchanged WebTransport adapter.
use game_server::*;
use ring::rand::{SecureRandom, SystemRandom};
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::timeout;
use wtransport::{ClientConfig, Connection, Endpoint, Identity, VarInt};

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        // Only these files, in the directory this probe created, are disposable.
        let _ = std::fs::remove_file(self.0.join("certificate.pem"));
        let _ = std::fs::remove_file(self.0.join("key.pem"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

async fn welcome(connection: &Connection) -> Result<Welcome, Box<dyn std::error::Error>> {
    let mut stream = connection.accept_uni().await?;
    let mut bytes = [0_u8; WELCOME_BYTES];
    stream.read_exact(&mut bytes).await?;
    Ok(decode_welcome(&bytes)?)
}

async fn command(
    send: &mut wtransport::SendStream,
    recv: &mut wtransport::RecvStream,
    sequence: u32,
    expected: ReliableCommandOutcome,
) -> Result<(), Box<dyn std::error::Error>> {
    send.write_all(&encode_command(sequence, &[0, 0])?).await?;
    let mut ack = [0; RELIABLE_COMMAND_ACK_BYTES];
    recv.read_exact(&mut ack).await?;
    assert_eq!(
        decode_reliable_command_ack(&ack)?,
        ReliableCommandAck {
            sequence,
            outcome: expected
        }
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    timeout(Duration::from_secs(15), run()).await?
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let player_id: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "2".to_owned())
        .parse()?;
    if !(1..=16).contains(&player_id) {
        return Err("player id must be 1..=16".into());
    }
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let scratch = Scratch(std::env::temp_dir().join(format!(
        "game-server-webrtc-reference-{}-{nonce}",
        std::process::id()
    )));
    std::fs::create_dir(&scratch.0)?;
    let identity = Identity::self_signed(["localhost", "127.0.0.1"])?;
    let hash = identity.certificate_chain().as_slice()[0].hash();
    let certificate = scratch.0.join("certificate.pem");
    let key = scratch.0.join("key.pem");
    identity
        .certificate_chain()
        .store_pemfile(&certificate)
        .await?;
    identity.private_key().store_secret_pemfile(&key).await?;
    let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::default(), 400);
    for _ in 1..player_id {
        let mut token = [0; 16];
        SystemRandom::new()
            .fill(&mut token)
            .map_err(|_| "randomness unavailable")?;
        runtime.admit(ReconnectToken(token))?;
    }
    let id = MatchId::new("webrtc-proof")?;
    let mut matches = MatchHost::new(1)?;
    matches
        .insert(id.clone(), runtime)
        .map_err(|_| "reference match placement failed")?;
    let host = LiveMatchHost::new(matches);
    let reservation = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let port = reservation.local_addr()?.port();
    drop(reservation);
    let prefix = BrowserRoutePrefix::new("/game")?;
    let (stop, shutdown) = tokio::sync::mpsc::channel(1);
    let serving = host.clone();
    let mut tasks = JoinSet::new();
    tasks.spawn(serve_live_match_host_with_control_and_shutdown(
        serving,
        RejectMatchControlService,
        MatchHostWebTransportConfig {
            port,
            certificate_pem: certificate,
            private_key_pem: key,
            route_prefix: prefix,
            allowed_origins: None,
            drain_grace: Duration::ZERO,
        },
        shutdown,
    ));
    while !host.is_serving() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let endpoint = Endpoint::client(
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([hash])
            .build(),
    )?;
    let url = format!("https://127.0.0.1:{port}/game/matches/webrtc-proof");
    let connection = endpoint.connect(&url).await?;
    let first = welcome(&connection).await?;
    assert_eq!(first.player_id, player_id);
    for _ in 0..3 {
        connection.send_datagram(encode_command(1, &[0, 0])?)?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut intent_snapshots = SnapshotReassembler::new();
    loop {
        let bytes = connection.receive_datagram().await?;
        if let Some(snapshot) = intent_snapshots.accept(&bytes)?
            && decode_demo_snapshot(&snapshot.payload)?
                .iter()
                .any(|player| player.player_id == player_id && player.last_applied_sequence == 1)
        {
            break;
        }
    }
    let (mut send, mut recv) = connection.open_bi().await?.await?;
    send.write_all(&encode_command_lane_header()).await?;
    command(&mut send, &mut recv, 1, ReliableCommandOutcome::Applied).await?;
    command(&mut send, &mut recv, 1, ReliableCommandOutcome::Duplicate).await?;
    connection.close(VarInt::from_u32(0), b"reconnect");
    drop(send);
    drop(recv);
    while host.inspect(&id, MatchRuntime::active_count).await? != (player_id - 1) as usize {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let reconnect = endpoint
        .connect(format!(
            "{url}/reconnect/{}",
            ReconnectToken(first.reconnect_token).encode_hex()
        ))
        .await?;
    let second = welcome(&reconnect).await?;
    assert_eq!(second.player_id, player_id);
    assert!(second.connection_epoch > first.connection_epoch);
    let (mut send, mut recv) = reconnect.open_bi().await?.await?;
    send.write_all(&encode_command_lane_header()).await?;
    command(&mut send, &mut recv, 1, ReliableCommandOutcome::Duplicate).await?;
    command(&mut send, &mut recv, 2, ReliableCommandOutcome::Applied).await?;
    let mut reassembler = SnapshotReassembler::new();
    let snapshot = loop {
        let datagram = reconnect.receive_datagram().await?;
        if let Some(snapshot) = reassembler.accept(&datagram)?
            && decode_demo_snapshot(&snapshot.payload)?
                .iter()
                .any(|player| player.player_id == player_id && player.last_applied_sequence == 2)
        {
            break snapshot;
        }
    };
    let record = snapshot.payload[1..]
        .as_chunks::<12>()
        .0
        .iter()
        .find(|record| record[..4] == player_id.to_be_bytes())
        .ok_or("missing addressed player")?;
    let mut scoped = vec![1];
    scoped.extend_from_slice(record);
    println!(
        "{}",
        serde_json::json!({"transport":"WebTransport","playerId":player_id,"connectionEpoch":second.connection_epoch,"orderedCommands":true,"fencedReconnect":true,"semanticPayload":scoped,"sharedSemanticPayload":snapshot.payload})
    );
    let replay = host
        .inspect(&id, |runtime| {
            runtime
                .replay_log()
                .expect("reference capture enabled")
                .clone()
        })
        .await?;
    verify_replay(DemoSimulation::default(), &replay)?;
    reconnect.close(VarInt::from_u32(0), b"done");
    stop.send(()).await?;
    tasks
        .join_next()
        .await
        .ok_or("missing reference server")???;
    Ok(())
}
