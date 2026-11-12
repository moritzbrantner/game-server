//! A real consumer's replay: the shooter's scripted deathmatch (moenarch-games/shooter#7).
//!
//! `tests/fixtures/shooter/deathmatch-v1.replay` is 20 s of a 60 Hz match the shooter hosted
//! through `MatchRuntime::new_with_replay_capture`: six slots of seeded bots, two scripted people
//! who join one second apart and send one datagram command every tick at 100 ms input latency,
//! and one who disconnects and is removed after the reconnect grace. Every tick ends in a
//! digest checkpoint (the shooter's snapshot is larger than a full checkpoint). The shooter
//! verifies the same log against its own simulation; this test holds game-server's side: the
//! codec reads the log, writes it back byte-identically, and the history has the shape the
//! runtime promises (one checkpoint per tick in order, admissions before commands, per-player
//! sequences strictly increasing, nothing from a removed player after its removal).
//!
//! `deathmatch-v1.txt` is the shooter's manifest. To regenerate both from the shooter repository:
//!
//! ```sh
//! cargo run --release -p shooter-server --bin shooter-replay -- record --ticks 1200 -o dm.shrp
//! cargo run --release -p shooter-server --bin shooter-replay -- \
//!     promote dm.shrp --target game-server --to 1200 -o deathmatch-v1.replay
//! ```

use game_server::{CommandLane, ReplayLog, ReplayRecord};
use std::collections::{BTreeMap, BTreeSet};

const REPLAY: &[u8] = include_bytes!("fixtures/shooter/deathmatch-v1.replay");
const MANIFEST: &str = include_str!("fixtures/shooter/deathmatch-v1.txt");

fn expected(key: &str) -> usize {
    MANIFEST
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("{key} in the manifest"))
        .parse()
        .unwrap()
}

#[test]
fn the_shooter_deathmatch_replay_round_trips_through_the_codec() {
    assert_eq!(REPLAY.len(), expected("replay_bytes"));
    let log = ReplayLog::decode(REPLAY).unwrap();
    assert_eq!(log.records().len(), expected("expected_records"));
    assert_eq!(log.encode().unwrap(), REPLAY);
}

#[test]
fn the_shooter_deathmatch_history_has_the_runtime_shape() {
    let log = ReplayLog::decode(REPLAY).unwrap();
    let (mut admissions, mut commands, mut removals, mut checkpoints) = (0, 0, 0, 0);
    let mut admitted = BTreeSet::new();
    let mut removed = BTreeSet::new();
    let mut sequences = BTreeMap::new();
    let mut last_checkpoint = 0;
    for record in log.records() {
        match record {
            ReplayRecord::PlayerAdmitted { player_id, tick } => {
                assert_eq!(*tick, last_checkpoint);
                assert!(admitted.insert(*player_id));
                admissions += 1;
            }
            ReplayRecord::CommandApplied {
                tick,
                player_id,
                lane,
                sequence,
                payload,
            } => {
                assert_eq!(*tick, last_checkpoint);
                assert_eq!(*lane, CommandLane::Datagram);
                assert!(admitted.contains(player_id) && !removed.contains(player_id));
                assert!(!payload.is_empty());
                let previous = sequences.insert(*player_id, *sequence).unwrap_or(0);
                assert!(*sequence > previous, "sequence {sequence} after {previous}");
                commands += 1;
            }
            ReplayRecord::PlayerRemoved { player_id, tick } => {
                assert_eq!(*tick, last_checkpoint);
                assert!(admitted.contains(player_id) && removed.insert(*player_id));
                removals += 1;
            }
            ReplayRecord::DigestCheckpoint { tick, .. } => {
                assert_eq!(*tick, last_checkpoint + 1);
                last_checkpoint = *tick;
                checkpoints += 1;
            }
            other => panic!("unexpected record {other:?}"),
        }
    }
    assert_eq!(admissions, expected("expected_admissions"));
    assert_eq!(commands, expected("expected_commands"));
    assert_eq!(removals, expected("expected_removals"));
    assert_eq!(checkpoints, expected("expected_digest_checkpoints"));
    assert_eq!(last_checkpoint, expected("expected_final_tick") as u64);
}
