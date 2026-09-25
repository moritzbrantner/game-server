use game_server::{
    CommandOutcome, ExternalSimulationAdapter, ExternalSimulationBridge, ExternalSimulationError,
    MatchRuntime, RECONNECT_TOKEN_BYTES, ReconnectToken, verify_replay,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const FIXTURE_TEXT: &str = include_str!("../fixtures/card-game-template/uno-replay-v1.txt");

#[derive(Clone, Debug)]
struct FixturePlayer {
    server_player_id: u32,
    game_player_id: String,
}

#[derive(Clone, Debug)]
struct FixtureCommand {
    player_id: u32,
    sequence: u32,
    payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct FixtureExchange {
    operation: String,
    request: Vec<u8>,
    response: Vec<u8>,
}

#[derive(Clone, Debug)]
struct ConvergenceFixture {
    schema: u8,
    source_repository: String,
    source_fixture: String,
    source_contract: String,
    protocol_version: u8,
    match_id: String,
    tick_hz: u16,
    players: Vec<FixturePlayer>,
    commands: Vec<FixtureCommand>,
    expected_final_tick: u64,
    expected_replay_fingerprint: u64,
    expected_accepted_move_count: usize,
    expected_winner: String,
    exchanges: Vec<FixtureExchange>,
}

#[derive(Clone, Debug)]
struct TranscriptBridge {
    exchanges: Arc<Mutex<VecDeque<FixtureExchange>>>,
}

impl TranscriptBridge {
    fn new(exchanges: Vec<FixtureExchange>) -> Self {
        Self {
            exchanges: Arc::new(Mutex::new(exchanges.into())),
        }
    }

    fn remaining(&self) -> usize {
        self.exchanges.lock().expect("fixture bridge lock").len()
    }
}

impl ExternalSimulationBridge for TranscriptBridge {
    fn exchange(&self, request: &[u8]) -> Result<Vec<u8>, ExternalSimulationError> {
        let mut exchanges = self
            .exchanges
            .lock()
            .map_err(|_| ExternalSimulationError::bridge("fixture bridge lock poisoned"))?;
        let expected = exchanges
            .pop_front()
            .ok_or_else(|| ExternalSimulationError::bridge("fixture transcript exhausted"))?;

        if expected.request != request {
            return Err(ExternalSimulationError::bridge(format!(
                "{} request mismatch: expected {}, got {}",
                expected.operation,
                encode_hex(&expected.request),
                encode_hex(request)
            )));
        }

        Ok(expected.response)
    }
}

fn parse_fixture(input: &str) -> ConvergenceFixture {
    let mut schema = None;
    let mut source_repository = None;
    let mut source_fixture = None;
    let mut source_contract = None;
    let mut protocol_version = None;
    let mut match_id = None;
    let mut tick_hz = None;
    let mut players = Vec::new();
    let mut commands = Vec::new();
    let mut expected_final_tick = None;
    let mut expected_replay_fingerprint = None;
    let mut expected_accepted_move_count = None;
    let mut expected_winner = None;
    let mut exchanges = Vec::new();

    for line in input.lines().filter(|line| !line.is_empty()) {
        let (key, value) = line.split_once('=').expect("fixture line must contain '='");
        match key {
            "schema" => schema = Some(value.parse().expect("valid fixture schema")),
            "source_repository" => source_repository = Some(value.to_owned()),
            "source_fixture" => source_fixture = Some(value.to_owned()),
            "source_contract" => source_contract = Some(value.to_owned()),
            "protocol_version" => {
                protocol_version = Some(value.parse().expect("valid protocol version"))
            }
            "match_id" => match_id = Some(value.to_owned()),
            "tick_hz" => tick_hz = Some(value.parse().expect("valid tick rate")),
            "player" => {
                let (server_player_id, game_player_id) =
                    value.split_once(':').expect("valid player mapping");
                players.push(FixturePlayer {
                    server_player_id: server_player_id.parse().expect("valid server player ID"),
                    game_player_id: game_player_id.to_owned(),
                });
            }
            "command" => {
                let mut parts = value.splitn(3, ':');
                commands.push(FixtureCommand {
                    player_id: parts
                        .next()
                        .expect("command player ID")
                        .parse()
                        .expect("valid command player ID"),
                    sequence: parts
                        .next()
                        .expect("command sequence")
                        .parse()
                        .expect("valid command sequence"),
                    payload: decode_hex(parts.next().expect("command payload")),
                });
            }
            "expected_final_tick" => {
                expected_final_tick = Some(value.parse().expect("valid final tick"))
            }
            "expected_replay_fingerprint" => {
                expected_replay_fingerprint =
                    Some(u64::from_str_radix(value, 16).expect("valid replay fingerprint"))
            }
            "expected_accepted_move_count" => {
                expected_accepted_move_count =
                    Some(value.parse().expect("valid accepted move count"))
            }
            "expected_winner" => expected_winner = Some(value.to_owned()),
            "exchange" => {
                let mut parts = value.splitn(3, ':');
                exchanges.push(FixtureExchange {
                    operation: parts.next().expect("exchange operation").to_owned(),
                    request: decode_hex(parts.next().expect("exchange request")),
                    response: decode_hex(parts.next().expect("exchange response")),
                });
            }
            other => panic!("unknown convergence fixture field {other}"),
        }
    }

    ConvergenceFixture {
        schema: schema.expect("fixture schema"),
        source_repository: source_repository.expect("source repository"),
        source_fixture: source_fixture.expect("source fixture"),
        source_contract: source_contract.expect("source contract"),
        protocol_version: protocol_version.expect("protocol version"),
        match_id: match_id.expect("match ID"),
        tick_hz: tick_hz.expect("tick rate"),
        players,
        commands,
        expected_final_tick: expected_final_tick.expect("final tick"),
        expected_replay_fingerprint: expected_replay_fingerprint.expect("replay fingerprint"),
        expected_accepted_move_count: expected_accepted_move_count.expect("accepted move count"),
        expected_winner: expected_winner.expect("winner"),
        exchanges,
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex bytes must be paired");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                .expect("valid hex byte")
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn run_through_match_runtime(
    fixture: &ConvergenceFixture,
) -> (game_server::ReplayLog, game_server::SimulationSnapshot) {
    let bridge = TranscriptBridge::new(fixture.exchanges.clone());
    let observer = bridge.clone();
    let simulation = ExternalSimulationAdapter::connect(bridge).expect("fixture must describe");
    let descriptor = simulation.descriptor();

    assert_eq!(descriptor.tick_hz, fixture.tick_hz);

    let mut runtime = MatchRuntime::new_with_replay_capture(simulation, 10);
    assert_eq!(runtime.tick_hz(), fixture.tick_hz);
    assert_eq!(runtime.max_players(), fixture.players.len());

    let mut leases = BTreeMap::new();
    for player in &fixture.players {
        let token_byte = u8::try_from(player.server_player_id).expect("small fixture player ID");
        let lease = runtime
            .admit(ReconnectToken([token_byte; RECONNECT_TOKEN_BYTES]))
            .expect("fixture admission must succeed");
        assert_eq!(lease.player_id, player.server_player_id);
        leases.insert(player.server_player_id, lease);
    }

    for command in &fixture.commands {
        let lease = leases
            .get(&command.player_id)
            .expect("command player must be admitted");
        assert_eq!(
            runtime
                .submit_command(
                    command.player_id,
                    lease.connection_epoch,
                    command.sequence,
                    &command.payload,
                )
                .expect("fixture command must be accepted"),
            CommandOutcome::Applied
        );
    }

    let checkpoint = runtime.advance_tick().expect("fixture tick must advance");
    assert_eq!(checkpoint.tick, fixture.expected_final_tick);
    assert_eq!(
        checkpoint.state_hash,
        fixture.expected_replay_fingerprint,
        "game-server checkpoint diverged from the TypeScript replay fingerprint"
    );

    let final_snapshot = runtime.snapshot().expect("fixture final snapshot");
    assert_eq!(final_snapshot, checkpoint);
    assert_eq!(observer.remaining(), 0, "live fixture transcript not fully consumed");

    let replay = runtime
        .replay_log()
        .expect("fixture replay capture enabled")
        .clone();
    (replay, final_snapshot)
}

#[test]
fn card_game_template_uno_replay_converges_through_game_server() {
    let fixture = parse_fixture(FIXTURE_TEXT);

    assert_eq!(fixture.schema, 1);
    assert_eq!(fixture.source_repository, "moritzbrantner/card-game-template");
    assert_eq!(fixture.source_fixture, "uno/replay-summary");
    assert_eq!(
        fixture.source_contract,
        "game-server/external-simulation-v1"
    );
    assert_eq!(fixture.protocol_version, 1);
    assert_eq!(fixture.match_id, "game-server/uno-replay-v1");
    assert_eq!(fixture.expected_accepted_move_count, fixture.commands.len());
    assert_eq!(fixture.expected_winner, "p1");
    assert_eq!(
        fixture
            .players
            .iter()
            .map(|player| (player.server_player_id, player.game_player_id.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "p1"), (2, "p2")]
    );

    let (replay, live_snapshot) = run_through_match_runtime(&fixture);

    let replay_bridge = TranscriptBridge::new(fixture.exchanges.clone());
    let replay_observer = replay_bridge.clone();
    let replay_simulation =
        ExternalSimulationAdapter::connect(replay_bridge).expect("replay fixture must describe");
    let verification = verify_replay(replay_simulation, &replay)
        .expect("game-server replay must reconstruct through the TypeScript fixture");

    assert_eq!(verification.final_snapshot, live_snapshot);
    assert_eq!(
        verification.final_snapshot.state_hash,
        fixture.expected_replay_fingerprint
    );
    assert_eq!(
        replay_observer.remaining(),
        0,
        "replay fixture transcript not fully consumed"
    );
}
