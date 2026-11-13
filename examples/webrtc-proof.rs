//! Explicit real-network probe; never runs in the hermetic Cargo test gate.
//! host <bind-ip> | client <bind-ip> <lobby-id> [relay]
use game_server::webrtc_transport::{
    Lobby, SetupConfig, WebRtcClient, WebRtcHost, WebRtcHostConfig,
};
use game_server::{DemoSimulation, MatchId, MatchRuntime, ReliableCommandOutcome};
use game_server::{GameSimulation, PlayerId, SimulationError, SimulationSnapshot, SnapshotScope};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Default)]
struct TickMetrics {
    ticks: AtomicU64,
    nanos: AtomicU64,
    max_nanos: AtomicU64,
}
struct ProofSimulation {
    demo: DemoSimulation,
    scoped: bool,
    timing: Arc<TickMetrics>,
}
impl ProofSimulation {
    fn new(scoped: bool, timing: Arc<TickMetrics>) -> Self {
        Self {
            demo: DemoSimulation::default(),
            scoped,
            timing,
        }
    }
}
impl GameSimulation for ProofSimulation {
    fn tick_hz(&self) -> u16 {
        self.demo.tick_hz()
    }
    fn max_players(&self) -> usize {
        self.demo.max_players()
    }
    fn current_tick(&self) -> u64 {
        self.demo.current_tick()
    }
    fn add_player(&mut self, id: PlayerId) -> Result<(), SimulationError> {
        self.demo.add_player(id)
    }
    fn remove_player(&mut self, id: PlayerId) -> bool {
        self.demo.remove_player(id)
    }
    fn apply_command(
        &mut self,
        id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        self.demo.apply_command(id, sequence, payload)
    }
    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        let start = Instant::now();
        let result = self.demo.advance_tick();
        let nanos = start.elapsed().as_nanos() as u64;
        self.timing.ticks.fetch_add(1, Ordering::Relaxed);
        self.timing.nanos.fetch_add(nanos, Ordering::Relaxed);
        self.timing.max_nanos.fetch_max(nanos, Ordering::Relaxed);
        result
    }
    fn snapshot_scope(&self) -> SnapshotScope {
        if self.scoped {
            SnapshotScope::PlayerScoped
        } else {
            SnapshotScope::Shared
        }
    }
    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        self.demo.snapshot()
    }
    fn snapshot_for(&self, id: PlayerId) -> Result<SimulationSnapshot, SimulationError> {
        let snapshot = self.demo.snapshot()?;
        let player = snapshot.payload[1..]
            .chunks_exact(12)
            .find(|bytes| bytes[..4] == id.to_be_bytes())
            .ok_or_else(|| SimulationError::new("unknown projection recipient"))?;
        let mut payload = vec![1];
        payload.extend_from_slice(player);
        Ok(SimulationSnapshot::new(snapshot.tick, payload))
    }
}

struct Echo;
impl game_server::ControlService for Echo {
    fn handle(
        &self,
        _: game_server::ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, game_server::ControlServiceError> {
        Ok(payload.to_vec())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let mode = args
        .first()
        .map(String::as_str)
        .ok_or("host or client required")?;
    let bind_ip = args.get(1).ok_or("native interface IP required")?;
    let setup = SetupConfig::new("https://moenarch.com", "https://moenarch.com")?;
    let match_id = MatchId::new("webrtc-proof")?;
    match mode {
        "host" => {
            let lobby = Lobby::create(setup, 16).await?;
            let scoped = args.get(2).is_some_and(|value| value == "scoped");
            let timing = Arc::new(TickMetrics::default());
            let mut runtime = MatchRuntime::new_with_replay_capture(
                ProofSimulation::new(scoped, Arc::clone(&timing)),
                400,
            );
            let mut private = [0_u8; 16];
            use ring::rand::SecureRandom;
            ring::rand::SystemRandom::new()
                .fill(&mut private)
                .map_err(|_| "probe randomness failed")?;
            // An independently admitted session makes accidental shared publication
            // visible when the second player's transport asks for a private projection.
            runtime.admit(game_server::ReconnectToken(private))?;
            let runtime = Arc::new(Mutex::new(runtime));
            let host = WebRtcHost::new(
                lobby,
                match_id,
                Arc::clone(&runtime),
                Arc::new(Echo),
                WebRtcHostConfig {
                    bind_ip: bind_ip.to_owned(),
                    relay_only: false,
                },
            )?;
            println!(
                "{}",
                serde_json::json!({"lobbyId":host.lobby_id(),"matchId":"webrtc-proof","ready":true,"scoped":scoped})
            );
            // This opt-in probe explicitly authorizes its ephemeral lobby's guests.
            host.run(|_| true, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
            let runtime = runtime.lock().await;
            let replay = runtime.replay_log().ok_or("missing replay")?;
            game_server::verify_replay(
                ProofSimulation::new(scoped, Arc::new(TickMetrics::default())),
                replay,
            )?;
            let ticks = timing.ticks.load(Ordering::Relaxed);
            let rss = std::fs::read_to_string("/proc/self/status")?
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .unwrap_or("VmRSS unavailable")
                .to_owned();
            println!(
                "{}",
                serde_json::json!({"shutdown":true,"replayVerified":true,"tick":runtime.current_tick(),"authoritativeTickSamples":ticks,"meanTickNanos":timing.nanos.load(Ordering::Relaxed)/ticks.max(1),"maxTickNanos":timing.max_nanos.load(Ordering::Relaxed),"singleMatchProcessRss":rss})
            );
        }
        "client" => {
            let lobby_id = args.get(2).ok_or("public lobby id required")?;
            let relay = args.get(3).is_some_and(|value| value == "relay");
            let started = Instant::now();
            let lobby = Lobby::join(setup, lobby_id).await?;
            let mut client = WebRtcClient::connect(lobby, match_id, bind_ip, relay).await?;
            let setup_ms = started.elapsed().as_secs_f64() * 1000.0;
            let player = client.welcome().player_id;
            let epoch = client.welcome().connection_epoch;
            let payload = game_server::encode_demo_command(0, 0)?;
            client.send_intent(1, &payload).await?;
            let ack = client.command(1, &payload).await?;
            assert_eq!(ack.outcome, ReliableCommandOutcome::Applied);
            let duplicate = client.command(1, &payload).await?;
            assert_eq!(duplicate.outcome, ReliableCommandOutcome::Duplicate);
            let snapshot =
                tokio::time::timeout(Duration::from_secs(5), client.snapshot()).await??;
            assert!(
                game_server::decode_demo_snapshot(&snapshot.payload)?
                    .iter()
                    .any(|entry| entry.player_id == player)
            );
            let started = Instant::now();
            let mut client = client.reconnect().await?;
            let reconnect_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(client.welcome().player_id, player);
            assert!(client.welcome().connection_epoch > epoch);
            assert_eq!(
                client.command(1, &payload).await?.outcome,
                ReliableCommandOutcome::Duplicate
            );
            assert_eq!(
                client.command(2, &payload).await?.outcome,
                ReliableCommandOutcome::Applied
            );
            let mut rtts = Vec::new();
            for _ in 0..10 {
                let start = Instant::now();
                assert_eq!(client.control(b"latency").await?.payload, b"latency");
                rtts.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            let jitter = rtts
                .windows(2)
                .map(|pair| (pair[1] - pair[0]).abs())
                .sum::<f64>()
                / 9.0;
            let final_snapshot = loop {
                let snapshot =
                    tokio::time::timeout(Duration::from_secs(5), client.snapshot()).await??;
                if game_server::decode_demo_snapshot(&snapshot.payload)?
                    .iter()
                    .any(|entry| entry.player_id == player && entry.last_applied_sequence == 2)
                {
                    break snapshot;
                }
            };
            println!(
                "{}",
                serde_json::json!({"setupMs":setup_ms,"reconnectMs":reconnect_ms,"playerId":player,"connectionEpoch":client.welcome().connection_epoch,"forcedRelay":relay,"orderedCommands":true,"snapshots":true,"fencedReconnect":true,"path":client.metrics().await,"controlRttSamplesMs":rtts,"rttMeanAbsoluteDeltaMs":jitter,"semanticPayload":final_snapshot.payload})
            );
            client.close().await?;
        }
        _ => return Err("unknown probe mode".into()),
    }
    Ok(())
}
