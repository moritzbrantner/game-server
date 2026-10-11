//! Acceptance contract for the welcome handshake under a path outage (#45).
//!
//! wtransport's `SendStream::finish()` first queues the FIN locally (quinn's
//! synchronous `finish`) and then awaits `stopped()`, which resolves only when the
//! peer has acknowledged every byte, or when the peer sends STOP_SENDING. Awaiting
//! it inside the welcome handshake ties admission to the client's stream state:
//!
//! - During an outage right after the welcome, the server never enters the
//!   established connection loop. Once the outage outlasts
//!   `WELCOME_HANDSHAKE_TIMEOUT` (5 s), the server rolls back a welcome the client
//!   already holds, so the client's reconnect token is refused.
//! - A client that reads exactly `WELCOME_BYTES` and drops the stream before the FIN
//!   arrives sends STOP_SENDING. That turns into `stream stopped (code: 0)` and a
//!   rollback even after a short outage. The repository's own clients read the
//!   welcome this way.
//!
//! Contract, consistent with ADR-0001 (welcome rollback must never invalidate a
//! token the client may know) and ADR-0008 (commands are delivered once the path
//! returns):
//!
//! - **Delivery.** The welcome counts as delivered once the whole frame is written
//!   and finished locally. Peer acknowledgement, and a STOP_SENDING after the frame,
//!   belong to the transport, not to admission. The handshake bound still applies
//!   to opening and writing.
//! - **Short outage** (below the handshake timeout) right after the welcome: once
//!   the path returns, the session is usable on the same connection. Datagram and
//!   lane commands apply and snapshots arrive.
//! - **Long outage** (above the handshake timeout, below the idle timeout): once
//!   the path returns, the client learns the outcome within `RECOVERY_BOUND`. The
//!   session is either usable on the same connection, or the connection visibly
//!   ends and the reconnect token from the welcome the client received resumes
//!   the same player within the grace window. A silent session, or a rollback that
//!   invalidates the received token, violates the contract.
//! - **Rollback.** Rollback is reserved for a welcome the client cannot hold: one
//!   that was never completely written. A new admission then releases its slot and
//!   a reconnect restores the previous token (`rollback_failed_welcome`, unit tests
//!   in `connection.rs`). A client that never received a welcome holds no token;
//!   after an outage it either gets the welcome late or sees the connection end,
//!   and a fresh admission then works with no ghost player.
//! - **No leak.** After every case exactly the expected players hold slots, all of
//!   them active, and the simulation knows no other player.
//!
//! Determinism: the gated `TestServer` holds each accepted session before admission.
//! The test cuts the client-to-server direction, then releases the welcome, so no
//! client ACK for the welcome can reach the server. As soon as the client has read
//! the welcome, the server-to-client direction is cut as well (a total outage).
//! Clients use a 1 s keep-alive like `control-client` and `netem-client`, so a
//! client with nothing to send still learns that the server dropped its connection.

use crate::command_lane_acceptance::{
    APPLIED, Applied, DATAGRAM, LossyRelay, RELIABLE, TestServer, ack, read_welcome,
    try_connect_session,
};
use crate::connection::AdmissionRequest;
use crate::{
    PlayerId, RELIABLE_COMMAND_ACK_BYTES, ReconnectToken, ReliableCommandAck, SnapshotReassembler,
    WELCOME_BYTES, Welcome, decode_reliable_command_ack, decode_welcome, encode_command,
    encode_command_lane_header,
};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout};
use wtransport::endpoint::endpoint_side::Client;
use wtransport::error::ConnectionError;
use wtransport::tls::Sha256Digest;
use wtransport::{Connection, Endpoint, RecvStream, SendStream, VarInt};

/// `connection::WELCOME_HANDSHAKE_TIMEOUT`.
const WELCOME_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Well below the handshake timeout.
const SHORT_OUTAGE: Duration = Duration::from_millis(1_500);
/// Above the handshake timeout and well below the 30 s QUIC idle timeout.
const LONG_OUTAGE: Duration = Duration::from_millis(6_500);
/// Once the path returns, the client must learn the outcome within this bound.
const RECOVERY_BOUND: Duration = Duration::from_secs(5);
/// Whole-test bound; the slowest case holds two long outages.
const TEST_TIMEOUT: Duration = Duration::from_secs(25);
/// Bound for a check on a session that is already known to be usable.
const USABLE_BOUND: Duration = Duration::from_secs(3);

const _: () = assert!(SHORT_OUTAGE.as_millis() < WELCOME_HANDSHAKE_TIMEOUT.as_millis());
const _: () = assert!(LONG_OUTAGE.as_millis() > WELCOME_HANDSHAKE_TIMEOUT.as_millis() + 1_000);

/// How the client consumes the welcome stream.
#[derive(Clone, Copy, Debug)]
enum WelcomeRead {
    /// Read exactly `WELCOME_BYTES` and drop the stream, like `control-client`,
    /// `netem-client` and the shared harness. If the FIN has not arrived yet, the
    /// drop sends STOP_SENDING.
    ExactFrame,
    /// Read the welcome and then the FIN, so the client never stops the stream.
    ToFin,
}

/// Real clients (`control-client`, `netem-client`) keep the path alive, so a client
/// learns about a connection the server has dropped even when it has nothing to send.
const CLIENT_KEEP_ALIVE: Option<Duration> = Some(Duration::from_secs(1));

async fn connect_session(
    port: u16,
    certificate_hash: &Sha256Digest,
) -> (Endpoint<Client>, Connection) {
    try_connect_session(port, certificate_hash, CLIENT_KEEP_ALIVE)
        .await
        .unwrap_or_else(|error| panic!("connect failed: {error}"))
}

struct StalledClient {
    _endpoint: Endpoint<Client>,
    connection: Connection,
    welcome: Welcome,
}

async fn read_welcome_to_fin(connection: &Connection) -> Welcome {
    timeout(RECOVERY_BOUND, async {
        let mut stream = connection.accept_uni().await.unwrap();
        let mut bytes = [0_u8; WELCOME_BYTES];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(
            stream.read(&mut [0_u8; 1]).await.unwrap(),
            None,
            "the welcome stream carries exactly one frame"
        );
        decode_welcome(&bytes).unwrap()
    })
    .await
    .expect("the welcome and its FIN must still reach the client")
}

/// Establishes a session through `relay` whose welcome the client receives but
/// cannot acknowledge, then holds a total outage for `outage` and restores the path.
async fn welcome_then_total_outage(
    server: &TestServer,
    relay: &LossyRelay,
    outage: Duration,
    mode: WelcomeRead,
) -> StalledClient {
    let (endpoint, connection) = connect_session(relay.port, &server.certificate_hash).await;
    relay.upstream_outage.store(true, Ordering::SeqCst);
    server.release_welcome();
    let welcome = match mode {
        WelcomeRead::ExactFrame => read_welcome(&connection)
            .await
            .expect("the welcome must still reach the client")
            .expect("the welcome stream failed"),
        WelcomeRead::ToFin => read_welcome_to_fin(&connection).await,
    };
    relay.downstream_outage.store(true, Ordering::SeqCst);
    sleep(outage).await;
    assert!(relay.dropped.load(Ordering::SeqCst) > 0);
    {
        let runtime = server.state.runtime.lock().await;
        // Diagnostic only: a rolled-back welcome shows up here as zero slots.
        eprintln!(
            "after a {outage:?} outage ({mode:?}): {} slot(s), {} active",
            runtime.slot_count(),
            runtime.active_count()
        );
    }
    relay.upstream_outage.store(false, Ordering::SeqCst);
    relay.downstream_outage.store(false, Ordering::SeqCst);
    StalledClient {
        _endpoint: endpoint,
        connection,
        welcome,
    }
}

#[derive(Debug)]
enum AfterOutage {
    /// The reliable lane acknowledged a command sent after the outage. The lane
    /// streams are kept so the lane stays open for the rest of the test.
    Usable(ReliableCommandAck, SendStream, RecvStream),
    /// The connection ended visibly.
    Ended(ConnectionError),
}

/// Sends one reliable lane command after the outage and waits for its ack or for
/// the connection to end. Panics when neither happens within `RECOVERY_BOUND`.
async fn probe_after_outage(connection: &Connection, sequence: u32, case: &str) -> AfterOutage {
    let attempt = async {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|error| error.to_string())?
            .await
            .map_err(|error| error.to_string())?;
        send.write_all(&encode_command_lane_header())
            .await
            .map_err(|error| error.to_string())?;
        send.write_all(&encode_command(sequence, b"after-outage").unwrap())
            .await
            .map_err(|error| error.to_string())?;
        let mut bytes = [0_u8; RELIABLE_COMMAND_ACK_BYTES];
        recv.read_exact(&mut bytes)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>((decode_reliable_command_ack(&bytes).unwrap(), send, recv))
    };
    let outcome = timeout(RECOVERY_BOUND, async {
        tokio::select! {
            ack = attempt => match ack {
                Ok((ack, send, recv)) => AfterOutage::Usable(ack, send, recv),
                Err(_) => AfterOutage::Ended(connection.closed().await),
            },
            error = connection.closed() => AfterOutage::Ended(error),
        }
    })
    .await;
    outcome.unwrap_or_else(|_| {
        panic!(
            "{case}: {RECOVERY_BOUND:?} after the path returned, the session is neither usable \
             nor visibly ended. The server is still waiting for the peer ACK of the welcome, \
             or it rolled the welcome back without the client noticing (#45)"
        )
    })
}

/// Proves that the session on `connection` is fully usable: a datagram command
/// applies, snapshots newer than the welcome arrive, and the lease is current.
async fn assert_session_usable(
    server: &TestServer,
    connection: &Connection,
    welcome: &Welcome,
    datagram_sequence: u32,
    case: &str,
) {
    let player = welcome.player_id;
    let expected: Applied = (
        player,
        DATAGRAM,
        datagram_sequence,
        b"datagram-after-outage".to_vec(),
    );
    timeout(USABLE_BOUND, async {
        // Datagrams are best effort, so resend until one is applied.
        loop {
            connection
                .send_datagram(encode_command(datagram_sequence, b"datagram-after-outage").unwrap())
                .unwrap();
            if timeout(
                Duration::from_millis(200),
                server.wait_for_applied(|applied| applied.contains(&expected)),
            )
            .await
            .is_ok()
            {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{case}: a datagram command sent after the outage must apply"));

    let mut reassembler = SnapshotReassembler::new();
    timeout(USABLE_BOUND, async {
        loop {
            let datagram = connection.receive_datagram().await.unwrap();
            if let Some(snapshot) = reassembler.accept(&datagram).unwrap()
                && snapshot.tick > welcome.current_tick
            {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{case}: snapshots must arrive after the outage"));

    assert!(
        server
            .state
            .runtime
            .lock()
            .await
            .validate_connection(player, welcome.connection_epoch)
            .is_ok(),
        "{case}: the welcomed connection epoch must own the session"
    );
}

/// Players known to the simulation, decoded from the canonical `LaneSimulation` snapshot.
async fn simulation_players(server: &TestServer) -> Vec<PlayerId> {
    let snapshot = server.state.runtime.lock().await.snapshot().unwrap();
    let bytes = &snapshot.payload;
    (0..usize::from(bytes[0]))
        .map(|index| u32::from_be_bytes(bytes[1 + 4 * index..5 + 4 * index].try_into().unwrap()))
        .collect()
}

/// Exactly `players` hold slots, all of them active, and the simulation has no ghost.
async fn assert_no_leak(server: &TestServer, players: &[PlayerId], case: &str) {
    {
        let runtime = server.state.runtime.lock().await;
        assert_eq!(runtime.slot_count(), players.len(), "{case}: slot count");
        assert_eq!(
            runtime.active_count(),
            players.len(),
            "{case}: active count"
        );
    }
    assert_eq!(
        simulation_players(server).await,
        players,
        "{case}: the simulation must know exactly the connected players"
    );
}

async fn wait_until_inactive(server: &TestServer, case: &str) {
    timeout(Duration::from_secs(2), async {
        while server.state.runtime.lock().await.active_count() != 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{case}: the server must release the ended connection"));
}

/// Reconnects directly to the server with `token`, which the client received in
/// its latest welcome, and requires the same player to resume.
async fn reconnect_with_received_token(
    server: &TestServer,
    token: [u8; crate::RECONNECT_TOKEN_BYTES],
    player: PlayerId,
    case: &str,
) -> (Endpoint<Client>, Connection, Welcome) {
    server
        .admissions
        .lock()
        .unwrap()
        .push_back(AdmissionRequest::Reconnect(ReconnectToken(token)));
    server.release_welcome();
    let refused = |error: String| -> ! {
        panic!(
            "{case}: the reconnect token from the welcome the client received must stay \
             valid within grace, but the reconnect was refused ({error}). The welcome was \
             rolled back although the client already held it (#45)"
        )
    };
    let (endpoint, connection) =
        try_connect_session(server.port, &server.certificate_hash, CLIENT_KEEP_ALIVE)
            .await
            .unwrap_or_else(|error| refused(error));
    let welcome = read_welcome(&connection)
        .await
        .unwrap_or_else(|_| panic!("{case}: reconnect welcome timed out"))
        .unwrap_or_else(|error| refused(error));
    assert_eq!(welcome.player_id, player, "{case}: the same player resumes");
    (endpoint, connection, welcome)
}

/// Shared body of the long-outage cases: the client already holds `stalled.welcome`.
///
/// Whichever way the original connection recovers, the token from the received
/// welcome must then resume the same player. A usable original connection is
/// closed explicitly so the reconnect is always exercised.
async fn assert_long_outage_recovers(server: &TestServer, stalled: StalledClient, case: &str) {
    let player = stalled.welcome.player_id;
    let restored_at = Instant::now();
    // Datagram sequence for the session that resumes with the received token. The
    // datagram watermark survives reconnect, so it must exceed any earlier one.
    let resumed_datagram_sequence = match probe_after_outage(&stalled.connection, 1, case).await {
        AfterOutage::Usable(acknowledgement, lane_send, lane_recv) => {
            assert_eq!(acknowledgement, ack(1, APPLIED), "{case}");
            assert_session_usable(server, &stalled.connection, &stalled.welcome, 1, case).await;
            assert_no_leak(server, &[player], case).await;
            drop((lane_send, lane_recv));
            stalled
                .connection
                .close(VarInt::from_u32(0), b"exercise the received token");
            2
        }
        AfterOutage::Ended(error) => {
            eprintln!(
                "{case}: connection ended {:?} after the path returned: {error}",
                restored_at.elapsed()
            );
            1
        }
    };
    wait_until_inactive(server, case).await;
    {
        let runtime = server.state.runtime.lock().await;
        assert_eq!(
            runtime.slot_count(),
            1,
            "{case}: the player keeps its slot in grace"
        );
    }
    let (_endpoint, connection, welcome) =
        reconnect_with_received_token(server, stalled.welcome.reconnect_token, player, case).await;
    assert!(welcome.connection_epoch > stalled.welcome.connection_epoch);
    assert_session_usable(
        server,
        &connection,
        &welcome,
        resumed_datagram_sequence,
        case,
    )
    .await;
    assert_no_leak(server, &[player], case).await;
    connection.close(VarInt::from_u32(0), b"done");
}

#[tokio::test]
async fn short_outage_right_after_the_welcome_leaves_a_usable_session() {
    timeout(TEST_TIMEOUT, async {
        for mode in [WelcomeRead::ToFin, WelcomeRead::ExactFrame] {
            let case = format!("short outage after a new welcome ({mode:?})");
            let case = case.as_str();
            let server = TestServer::start_gated().await;
            let mut relay = LossyRelay::start(server.port).await;
            let stalled = welcome_then_total_outage(&server, &relay, SHORT_OUTAGE, mode).await;
            let player = stalled.welcome.player_id;

            let _lane = match probe_after_outage(&stalled.connection, 1, case).await {
                AfterOutage::Usable(acknowledgement, lane_send, lane_recv) => {
                    assert_eq!(acknowledgement, ack(1, APPLIED), "{case}");
                    (lane_send, lane_recv)
                }
                AfterOutage::Ended(error) => panic!(
                    "{case}: an outage shorter than the welcome handshake timeout must not end \
                     the session, got: {error}. A client may stop the welcome stream once it \
                     holds the frame; neither its ACK nor its STOP_SENDING decides delivery (#45)"
                ),
            };
            assert!(server.applied().await.contains(&(
                player,
                RELIABLE,
                1,
                b"after-outage".to_vec()
            )));
            assert_session_usable(&server, &stalled.connection, &stalled.welcome, 1, case).await;
            assert_no_leak(&server, &[player], case).await;
            stalled.connection.close(VarInt::from_u32(0), b"done");
            relay.tasks.shutdown().await;
            server.shutdown().await;
        }
    })
    .await
    .expect("short outage acceptance timed out");
}

#[tokio::test]
async fn long_outage_after_a_new_welcome_recovers_with_the_received_token() {
    timeout(TEST_TIMEOUT, async {
        for mode in [WelcomeRead::ToFin, WelcomeRead::ExactFrame] {
            let case = format!("long outage after a new welcome ({mode:?})");
            let server = TestServer::start_gated().await;
            let mut relay = LossyRelay::start(server.port).await;
            let stalled = welcome_then_total_outage(&server, &relay, LONG_OUTAGE, mode).await;
            assert_long_outage_recovers(&server, stalled, &case).await;
            relay.tasks.shutdown().await;
            server.shutdown().await;
        }
    })
    .await
    .expect("long outage acceptance timed out");
}

#[tokio::test]
async fn long_outage_after_a_reconnect_welcome_recovers_with_the_latest_token() {
    let case = "long outage after a reconnect welcome";
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start_gated().await;
        let mut relay = LossyRelay::start(server.port).await;

        // A first session on a healthy path, then a clean disconnect into grace.
        server.release_welcome();
        let (_first_endpoint, first, first_welcome) = {
            let (endpoint, connection) =
                connect_session(relay.port, &server.certificate_hash).await;
            let welcome = read_welcome(&connection).await.unwrap().unwrap();
            (endpoint, connection, welcome)
        };
        first.close(VarInt::from_u32(0), b"network change");
        wait_until_inactive(&server, case).await;

        // The reconnect welcome carries a replacement token; its ACK never arrives.
        server
            .admissions
            .lock()
            .unwrap()
            .push_back(AdmissionRequest::Reconnect(ReconnectToken(
                first_welcome.reconnect_token,
            )));
        let stalled =
            welcome_then_total_outage(&server, &relay, LONG_OUTAGE, WelcomeRead::ExactFrame).await;
        assert_eq!(stalled.welcome.player_id, first_welcome.player_id);
        assert_ne!(
            stalled.welcome.reconnect_token, first_welcome.reconnect_token,
            "a reconnect welcome rotates the token"
        );
        assert_long_outage_recovers(&server, stalled, case).await;
        relay.tasks.shutdown().await;
        server.shutdown().await;
    })
    .await
    .expect("reconnect long outage acceptance timed out");
}

#[tokio::test]
async fn outage_before_the_welcome_arrives_ends_usable_or_rolled_back_without_a_ghost() {
    let case = "long outage before the welcome reached the client";
    timeout(TEST_TIMEOUT, async {
        let server = TestServer::start_gated().await;
        let mut relay = LossyRelay::start(server.port).await;
        let (_endpoint, connection) = connect_session(relay.port, &server.certificate_hash).await;
        relay.outage.store(true, Ordering::SeqCst);
        server.release_welcome();
        sleep(LONG_OUTAGE).await;
        relay.outage.store(false, Ordering::SeqCst);

        let welcome = timeout(RECOVERY_BOUND, async {
            tokio::select! {
                welcome = read_welcome(&connection) => welcome.ok().and_then(Result::ok),
                _ = connection.closed() => None,
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{case}: {RECOVERY_BOUND:?} after the path returned, the client has neither \
                 a welcome nor a visibly ended connection (#45)"
            )
        });
        match welcome {
            // Delivered late: the session must be usable.
            Some(welcome) => {
                assert_session_usable(&server, &connection, &welcome, 1, case).await;
                assert_no_leak(&server, &[welcome.player_id], case).await;
            }
            // Never delivered and rolled back: a fresh admission works, with no ghost.
            None => {
                timeout(Duration::from_secs(2), async {
                    while server.state.runtime.lock().await.slot_count() != 0 {
                        sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("{case}: the rolled-back slot must be released"));
                server.release_welcome();
                let (_fresh_endpoint, fresh) =
                    connect_session(server.port, &server.certificate_hash).await;
                let welcome = read_welcome(&fresh).await.unwrap().unwrap();
                assert_session_usable(&server, &fresh, &welcome, 1, case).await;
                assert_no_leak(&server, &[welcome.player_id], case).await;
                fresh.close(VarInt::from_u32(0), b"done");
            }
        }
        relay.tasks.shutdown().await;
        server.shutdown().await;
    })
    .await
    .expect("outage before welcome acceptance timed out");
}
