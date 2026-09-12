use game_server::protocol::WELCOME_BYTES;
use game_server::{
    CONTROL_FORMAT_VERSION, CONTROL_HEADER_BYTES, MAX_CONTROL_PAYLOAD_BYTES,
    decode_control_response, decode_demo_snapshot, decode_snapshot, decode_welcome, encode_command,
    encode_control_request, encode_demo_command,
};
use std::env;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use wtransport::tls::{Sha256Digest, Sha256DigestFmt};
use wtransport::{ClientConfig, Connection, Endpoint, RecvStream, SendStream};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const STREAM_REJECTION_TIMEOUT: Duration = Duration::from_secs(2);
const STALLED_STREAM_TIMEOUT: Duration = Duration::from_secs(7);
const DATAGRAM_PROGRESS_TIMEOUT: Duration = Duration::from_secs(2);
const CONCURRENCY_SETTLE: Duration = Duration::from_millis(150);
const CONTROL_REQUEST_KIND: u8 = 1;

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

    let digest = Sha256Digest::from_str_fmt(certificate_hash, Sha256DigestFmt::DottedHex)?;
    let client_config = ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([digest])
        .keep_alive_interval(Some(Duration::from_secs(1)))
        .build();
    let endpoint = Endpoint::client(client_config)?;
    let connection = timeout(IO_TIMEOUT, endpoint.connect(url.as_str())).await??;

    let mut welcome_stream = timeout(IO_TIMEOUT, connection.accept_uni()).await??;
    let mut welcome_bytes = [0_u8; WELCOME_BYTES];
    timeout(IO_TIMEOUT, welcome_stream.read_exact(&mut welcome_bytes)).await??;
    let welcome = decode_welcome(&welcome_bytes)?;

    let accepted = control_exchange(&connection, b"ping").await?;
    let accepted_round_trip = accepted.accepted && accepted.payload == b"pong";

    let rejected = control_exchange(&connection, b"reject").await?;
    let service_rejection = !rejected.accepted && rejected.payload.is_empty();

    let malformed_rejected = expect_stream_rejected(&connection, b"bad").await?;
    let oversized_rejected = expect_stream_rejected(&connection, &oversized_header()).await?;
    let mut trailing_frame = encode_control_request(b"ping")?;
    trailing_frame.push(0);
    let trailing_rejected = expect_stream_rejected(&connection, &trailing_frame).await?;

    let post_rejection = control_exchange(&connection, b"ping").await?;
    let connection_remained_usable = post_rejection.accepted && post_rejection.payload == b"pong";

    let (mut stalled_send, mut stalled_recv) = open_bi(&connection).await?;
    stalled_send.write_all(&[0]).await?;
    let datagram_progress_while_control_stalled =
        observe_datagram_progress(&connection, welcome.player_id).await?;
    let stalled_stream_timed_out = timeout(
        STALLED_STREAM_TIMEOUT,
        read_one_byte_or_close(&mut stalled_recv),
    )
    .await
    .unwrap_or_default();
    drop(stalled_send);

    let concurrency_bound_observed = observe_concurrency_bound(&connection).await?;

    let expectations_hold = accepted_round_trip
        && service_rejection
        && malformed_rejected
        && oversized_rejected
        && trailing_rejected
        && connection_remained_usable
        && datagram_progress_while_control_stalled
        && stalled_stream_timed_out
        && concurrency_bound_observed;

    println!(
        "{{\"mode\":\"webtransport-control-client\",\"playerId\":{},\"acceptedRoundTrip\":{},\"serviceRejection\":{},\"malformedRejected\":{},\"oversizedRejected\":{},\"trailingRejected\":{},\"connectionRemainedUsable\":{},\"datagramProgressWhileControlStalled\":{},\"stalledStreamTimedOut\":{},\"concurrencyBoundObserved\":{},\"expectationsHold\":{}}}",
        welcome.player_id,
        accepted_round_trip,
        service_rejection,
        malformed_rejected,
        oversized_rejected,
        trailing_rejected,
        connection_remained_usable,
        datagram_progress_while_control_stalled,
        stalled_stream_timed_out,
        concurrency_bound_observed,
        expectations_hold,
    );

    if expectations_hold {
        Ok(())
    } else {
        Err("reliable-control acceptance expectations did not hold".into())
    }
}

async fn control_exchange(
    connection: &Connection,
    payload: &[u8],
) -> Result<game_server::ControlResponse, Box<dyn Error>> {
    let request = encode_control_request(payload)?;
    let (mut send_stream, mut recv_stream) = open_bi(connection).await?;
    timeout(IO_TIMEOUT, send_stream.write_all(&request)).await??;
    timeout(IO_TIMEOUT, send_stream.finish()).await??;
    read_control_response(&mut recv_stream).await
}

async fn read_control_response(
    recv_stream: &mut RecvStream,
) -> Result<game_server::ControlResponse, Box<dyn Error>> {
    let mut header = [0_u8; CONTROL_HEADER_BYTES];
    timeout(IO_TIMEOUT, recv_stream.read_exact(&mut header)).await??;
    let payload_len = usize::from(u16::from_be_bytes([header[6], header[7]]));
    if payload_len > MAX_CONTROL_PAYLOAD_BYTES {
        return Err("server returned an oversized reliable-control response".into());
    }
    let mut frame = Vec::with_capacity(CONTROL_HEADER_BYTES + payload_len);
    frame.extend_from_slice(&header);
    frame.resize(CONTROL_HEADER_BYTES + payload_len, 0);
    if payload_len > 0 {
        timeout(
            IO_TIMEOUT,
            recv_stream.read_exact(&mut frame[CONTROL_HEADER_BYTES..]),
        )
        .await??;
    }
    Ok(decode_control_response(&frame)?)
}

async fn expect_stream_rejected(
    connection: &Connection,
    frame: &[u8],
) -> Result<bool, Box<dyn Error>> {
    let (mut send_stream, mut recv_stream) = open_bi(connection).await?;
    match timeout(IO_TIMEOUT, send_stream.write_all(frame)).await {
        Err(_) => return Ok(false),
        Ok(Err(_)) => return Ok(true),
        Ok(Ok(())) => {}
    }
    match timeout(IO_TIMEOUT, send_stream.finish()).await {
        Err(_) => return Ok(false),
        Ok(Err(_)) => return Ok(true),
        Ok(Ok(())) => {}
    }
    Ok(timeout(
        STREAM_REJECTION_TIMEOUT,
        read_one_byte_or_close(&mut recv_stream),
    )
    .await
    .unwrap_or_default())
}

async fn observe_datagram_progress(
    connection: &Connection,
    player_id: u32,
) -> Result<bool, Box<dyn Error>> {
    let payload = encode_demo_command(1, 0)?;
    connection.send_datagram(encode_command(1, &payload)?)?;

    let deadline = Instant::now() + DATAGRAM_PROGRESS_TIMEOUT;
    let mut first_tick = None;
    let mut last_tick = None;
    let mut applied = false;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_millis(250));
        let datagram = match timeout(wait, connection.receive_datagram()).await {
            Ok(Ok(datagram)) => datagram,
            Ok(Err(_)) => return Ok(false),
            Err(_) => continue,
        };
        let snapshot = match decode_snapshot(datagram.as_ref()) {
            Ok(snapshot) => snapshot,
            Err(_) => continue,
        };
        first_tick.get_or_insert(snapshot.tick);
        last_tick = Some(snapshot.tick);
        let players = decode_demo_snapshot(&snapshot.payload)?;
        if players
            .iter()
            .any(|player| player.player_id == player_id && player.last_applied_sequence >= 1)
        {
            applied = true;
        }
        if applied && first_tick.is_some_and(|first| snapshot.tick > first) {
            return Ok(true);
        }
    }

    Ok(applied && matches!((first_tick, last_tick), (Some(first), Some(last)) if last > first))
}

async fn observe_concurrency_bound(connection: &Connection) -> Result<bool, Box<dyn Error>> {
    let mut held = Vec::new();
    for _ in 0..4 {
        let (mut send_stream, recv_stream) = open_bi(connection).await?;
        send_stream.write_all(&[0]).await?;
        held.push((send_stream, recv_stream));
    }
    sleep(CONCURRENCY_SETTLE).await;

    let (mut overflow_send, mut overflow_recv) = open_bi(connection).await?;
    match timeout(IO_TIMEOUT, overflow_send.write_all(&[0])).await {
        Err(_) => return Ok(false),
        Ok(Err(_)) => return Ok(true),
        Ok(Ok(())) => {}
    }
    let rejected = timeout(
        STREAM_REJECTION_TIMEOUT,
        read_one_byte_or_close(&mut overflow_recv),
    )
    .await
    .unwrap_or_default();
    drop(overflow_send);
    drop(held);
    Ok(rejected)
}

async fn open_bi(connection: &Connection) -> Result<(SendStream, RecvStream), Box<dyn Error>> {
    let opening = timeout(IO_TIMEOUT, connection.open_bi()).await??;
    Ok(timeout(IO_TIMEOUT, opening).await??)
}

async fn read_one_byte_or_close(recv_stream: &mut RecvStream) -> bool {
    let mut byte = [0_u8; 1];
    recv_stream.read_exact(&mut byte).await.is_err()
}

fn oversized_header() -> [u8; CONTROL_HEADER_BYTES] {
    let payload_len = u16::try_from(MAX_CONTROL_PAYLOAD_BYTES + 1)
        .expect("reliable-control maximum stays below u16::MAX")
        .to_be_bytes();
    [
        b'G',
        b'S',
        b'C',
        b'T',
        CONTROL_FORMAT_VERSION,
        CONTROL_REQUEST_KIND,
        payload_len[0],
        payload_len[1],
    ]
}
