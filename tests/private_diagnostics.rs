use game_server::{
    CommandFrame, CommandRejectionFrame, ControlRequest, ControlResponse, MatchRuntime,
    ReconnectToken, RecoverableSession, ReplayRecord, SessionLease, SessionRecoverySnapshot,
    SimulationSnapshot, SnapshotDatagram, SnapshotFragment, SnapshotFrame, Welcome,
};

#[test]
fn opaque_payloads_and_credentials_are_redacted_at_nested_diagnostic_boundaries() {
    let secret = b"private-rack-and-command-marker".to_vec();
    let token = ReconnectToken([0xab; 16]);
    let snapshot = SimulationSnapshot::new(7, secret.clone());
    let cases = [
        format!(
            "{:?}",
            CommandFrame {
                sequence: 3,
                payload: secret.clone()
            }
        ),
        format!(
            "{:?}",
            CommandRejectionFrame {
                sequence: 3,
                payload: secret.clone()
            }
        ),
        format!(
            "{:?}",
            SnapshotFrame {
                tick: 7,
                state_hash: 9,
                payload: secret.clone()
            }
        ),
        format!(
            "{:?}",
            SnapshotDatagram::Fragment(SnapshotFragment {
                tick: 7,
                index: 0,
                count: 1,
                chunk: &secret
            })
        ),
        format!(
            "{:?}",
            ControlRequest {
                payload: secret.clone()
            }
        ),
        format!(
            "{:?}",
            ControlResponse {
                accepted: false,
                payload: secret.clone()
            }
        ),
        format!("{snapshot:?}"),
        format!(
            "{:?}",
            ReplayRecord::Checkpoint {
                snapshot: snapshot.clone()
            }
        ),
        format!(
            "{:?}",
            ReplayRecord::CommandApplied {
                tick: 7,
                player_id: 1,
                sequence: 3,
                payload: secret.clone()
            }
        ),
        format!("{token:?}"),
        format!(
            "{:?}",
            SessionLease {
                player_id: 1,
                connection_epoch: 2,
                reconnect_token: token,
                reconnect_grace_ticks: 600
            }
        ),
        format!(
            "{:?}",
            SessionRecoverySnapshot {
                next_player_id: 2,
                sessions: vec![RecoverableSession {
                    player_id: 1,
                    reconnect_token: token,
                    connection_epoch: 2,
                    remaining_grace_ticks: 600,
                }]
            }
        ),
        format!(
            "{:?}",
            Welcome {
                player_id: 1,
                tick_hz: 20,
                max_players: 4,
                current_tick: 7,
                connection_epoch: 2,
                reconnect_token: token.0,
                reconnect_grace_ticks: 600
            }
        ),
    ];
    for diagnostic in cases {
        assert!(!diagnostic.contains(&format!("{secret:?}")));
        assert!(!diagnostic.contains(&format!("{:?}", token.0)));
        assert!(!diagnostic.contains(&token.encode_hex()));
        assert!(!diagnostic.contains("private-rack-and-command-marker"));
    }
    assert_eq!(ReconnectToken::decode_hex(&token.encode_hex()), Some(token));
    assert_eq!(snapshot.payload, secret);
}

#[test]
fn a_real_runtime_recovery_image_does_not_print_its_reconnect_capability() {
    let token = ReconnectToken([0xab; 16]);
    let mut runtime =
        MatchRuntime::new_with_replay_capture(game_server::DemoSimulation::new(), 600);
    runtime.admit(token).unwrap();
    runtime.freeze_for_recovery();
    let image = runtime.recovery_image().unwrap();
    let diagnostic = format!("{image:#?}");
    assert!(!diagnostic.contains(&format!("{:?}", token.0)));
    assert!(!diagnostic.contains(&token.encode_hex()));
    assert!(diagnostic.contains("[redacted]"));
    assert_eq!(
        game_server::RecoveryImage::decode(&image.encode().unwrap()).unwrap(),
        image
    );
}
