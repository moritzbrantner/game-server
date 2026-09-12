use game_server::protocol::WELCOME_BYTES;
use game_server::{
    ReconnectToken, decode_demo_snapshot, decode_snapshot, decode_welcome, encode_command,
    encode_demo_command,
};
use std::env;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use wtransport::tls::{Sha256Digest, Sha256DigestFmt};
use wtransport::{ClientConfig, Endpoint};

const MIN_ACCEPTED_SNAPSHOTS: u64 = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let url = args
        .first()
        .cloned()
        .unwrap_or_else(|| "https://127.0.0.1:4433/game".to_owned());
    let certificate_hash = args
        .get(1)
        .ok_or("certificate SHA-256 fingerprint is required")?;
    let command_count = parse_or(&args, 2, 120_u32)?;
    let interval_ms = parse_or(&args, 3, 15_u64)?;
    let observe_ms = parse_or(&args, 4, 3_000_u64)?;
    let retransmit_count = parse_or(&args, 5, 20_u32)?;
    let start_sequence = parse_or(&args, 6, 1_u32)?;
    if command_count == 0 || start_sequence == 0 {
        return Err("command count and start sequence must be non-zero".into());
    }
    let final_sequence = start_sequence
        .checked_add(command_count - 1)
        .ok_or("command sequence range overflow")?;

    let digest = Sha256Digest::from_str_fmt(certificate_hash, Sha256DigestFmt::DottedHex)?;
    let client_config = ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([digest])
        .keep_alive_interval(Some(Duration::from_secs(1)))
        .build();
    let endpoint = Endpoint::client(client_config)?;

    let connect_started = Instant::now();
    let connection = timeout(Duration::from_secs(8), endpoint.connect(url.as_str())).await??;
    let connect_ms = connect_started.elapsed().as_secs_f64() * 1_000.0;

    let mut welcome_stream = timeout(Duration::from_secs(5), connection.accept_uni()).await??;
    let mut welcome_bytes = [0_u8; WELCOME_BYTES];
    timeout(
        Duration::from_secs(5),
        welcome_stream.read_exact(&mut welcome_bytes),
    )
    .await??;
    let welcome = decode_welcome(&welcome_bytes)?;
    let reconnect_token = ReconnectToken(welcome.reconnect_token).encode_hex();

    let payload = encode_demo_command(1, 0)?;
    for offset in 0..command_count {
        let sequence = start_sequence
            .checked_add(offset)
            .ok_or("command sequence overflow")?;
        let command = encode_command(sequence, &payload)?;
        connection.send_datagram(command)?;
        if interval_ms > 0 {
            sleep(Duration::from_millis(interval_ms)).await;
        }
    }

    let final_command = encode_command(final_sequence, &payload)?;
    for _ in 0..retransmit_count {
        connection.send_datagram(&final_command)?;
        sleep(Duration::from_millis(interval_ms.max(1))).await;
    }

    let deadline = Instant::now() + Duration::from_millis(observe_ms);
    let mut received_snapshots = 0_u64;
    let mut accepted_snapshots = 0_u64;
    let mut stale_snapshots = 0_u64;
    let mut invalid_snapshots = 0_u64;
    let mut first_tick = None;
    let mut last_tick = None;
    let mut final_applied_sequence = 0_u32;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_millis(250));
        let datagram = match timeout(wait, connection.receive_datagram()).await {
            Ok(Ok(datagram)) => datagram,
            Ok(Err(_)) => break,
            Err(_) => continue,
        };
        received_snapshots += 1;
        let snapshot = match decode_snapshot(datagram.as_ref()) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                invalid_snapshots += 1;
                continue;
            }
        };
        if last_tick.is_some_and(|tick| snapshot.tick <= tick) {
            stale_snapshots += 1;
            continue;
        }
        first_tick.get_or_insert(snapshot.tick);
        last_tick = Some(snapshot.tick);
        accepted_snapshots += 1;
        let players = decode_demo_snapshot(&snapshot.payload)?;
        if let Some(player) = players
            .iter()
            .find(|player| player.player_id == welcome.player_id)
        {
            final_applied_sequence = final_applied_sequence.max(player.last_applied_sequence);
        }
        if final_applied_sequence == final_sequence && accepted_snapshots >= MIN_ACCEPTED_SNAPSHOTS {
            break;
        }
    }

    let progressed = match (first_tick, last_tick) {
        (Some(first), Some(last)) => last > first,
        _ => false,
    };
    let expectations_hold = received_snapshots >= MIN_ACCEPTED_SNAPSHOTS
        && accepted_snapshots >= MIN_ACCEPTED_SNAPSHOTS
        && invalid_snapshots == 0
        && progressed
        && final_applied_sequence == final_sequence;
    let rtt_ms = connection.rtt().as_secs_f64() * 1_000.0;

    println!(
        "{{\"mode\":\"webtransport-netem-client\",\"connected\":true,\"connectMs\":{connect_ms:.3},\"rttMs\":{rtt_ms:.3},\"playerId\":{},\"connectionEpoch\":{},\"reconnectToken\":\"{}\",\"welcomeTick\":{},\"sentCommands\":{},\"firstSentSequence\":{},\"finalSentSequence\":{},\"finalAppliedSequence\":{},\"receivedSnapshots\":{},\"acceptedSnapshots\":{},\"staleSnapshots\":{},\"invalidSnapshots\":{},\"firstAcceptedTick\":{},\"lastAcceptedTick\":{},\"expectationsHold\":{}}}",
        welcome.player_id,
        welcome.connection_epoch,
        reconnect_token,
        welcome.current_tick,
        command_count,
        start_sequence,
        final_sequence,
        final_applied_sequence,
        received_snapshots,
        accepted_snapshots,
        stale_snapshots,
        invalid_snapshots,
        optional_number(first_tick),
        optional_number(last_tick),
        expectations_hold,
    );

    if expectations_hold {
        Ok(())
    } else {
        Err("WebTransport impairment expectations did not hold".into())
    }
}

fn parse_or<T>(args: &[String], index: usize, default: T) -> Result<T, T::Err>
where
    T: std::str::FromStr,
{
    match args.get(index) {
        Some(value) => value.parse(),
        None => Ok(default),
    }
}

fn optional_number(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |number| number.to_string())
}
