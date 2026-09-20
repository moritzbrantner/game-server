//! Opt-in, release-only measurements of real runtime paths. See benchmarks/README.md.
use crate::connection::{SnapshotPublication, snapshot_publication};
use crate::{
    DemoSimulation, GameSimulation, MatchRuntime, PlayerId, ReconnectToken, SimulationError,
    SimulationSnapshot, SnapshotScope, encode_demo_command, verify_replay,
};
use std::hint::black_box;
use std::time::Instant;
use tokio::sync::broadcast;

const SAMPLES: usize = 31;
const WARMUP_BATCHES: usize = 5;

fn measure(name: &str, iterations: usize, mut operation: impl FnMut()) {
    for _ in 0..WARMUP_BATCHES {
        for _ in 0..iterations {
            operation();
        }
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..iterations {
            operation();
        }
        samples.push(started.elapsed().as_nanos() as f64 / iterations as f64);
    }
    println!(
        "BENCHMARK {{\"name\":\"{name}\",\"unit\":\"ns/op\",\"direction\":\"lower\",\"iterationsPerSample\":{iterations},\"warmupBatches\":{WARMUP_BATCHES},\"samples\":{samples:?}}}"
    );
}

struct PayloadSimulation {
    tick: u64,
}

impl GameSimulation for PayloadSimulation {
    fn tick_hz(&self) -> u16 {
        20
    }
    fn max_players(&self) -> usize {
        1
    }
    fn current_tick(&self) -> u64 {
        self.tick
    }
    fn add_player(&mut self, _player_id: PlayerId) -> Result<(), SimulationError> {
        Ok(())
    }
    fn remove_player(&mut self, _player_id: PlayerId) -> bool {
        true
    }
    fn apply_command(
        &mut self,
        _player_id: PlayerId,
        _sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        black_box(payload);
        Ok(())
    }
    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        self.tick += 1;
        Ok(())
    }
    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        Ok(SimulationSnapshot::new(self.tick, vec![42; 65_535]))
    }
}

#[test]
#[ignore = "opt-in benchmark: python3 benchmarks/run.py --output target/benchmarks/current.json"]
#[allow(
    clippy::assertions_on_constants,
    reason = "Ignored benchmarks must compile in debug builds but refuse to run without --release"
)]
fn benchmark_hot_paths() {
    assert!(!cfg!(debug_assertions), "benchmarks require --release");
    let mut runtime = MatchRuntime::new(PayloadSimulation { tick: 0 }, 10);
    let lease = runtime.admit(ReconnectToken([1; 16])).unwrap();
    let payload = vec![7; 1024];
    let mut sequence = 0;
    measure("live_command_1024b_no_replay", 10_000, || {
        sequence += 1;
        black_box(
            runtime
                .submit_command(
                    lease.player_id,
                    lease.connection_epoch,
                    sequence,
                    black_box(&payload),
                )
                .unwrap(),
        );
    });
    measure("live_tick_65535b_no_replay", 128, || {
        black_box(runtime.advance_tick().unwrap());
    });

    let publication = snapshot_publication(
        SnapshotScope::Shared,
        SimulationSnapshot::new(7, vec![42; 1024]),
    )
    .unwrap();
    let (sender, _) = broadcast::channel::<SnapshotPublication>(1);
    let mut receivers: Vec<_> = (0..64).map(|_| sender.subscribe()).collect();
    measure("shared_snapshot_fanout_64x1024b", 512, || {
        sender.send(black_box(&publication).clone()).unwrap();
        for receiver in &mut receivers {
            black_box(receiver.try_recv().unwrap());
        }
    });

    let mut runtime = MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 10);
    let leases: Vec<_> = (1..=16)
        .map(|id| runtime.admit(ReconnectToken([id; 16])).unwrap())
        .collect();
    let command = encode_demo_command(1, -1).unwrap();
    for sequence in 1..=256 {
        for lease in &leases {
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, sequence, &command)
                .unwrap();
        }
        runtime.advance_tick().unwrap();
    }
    runtime.freeze_for_recovery();
    let image = runtime.recovery_image().unwrap();
    measure("replay_encode_256ticks_16players", 32, || {
        black_box(image.replay.encode().unwrap());
    });
    measure("replay_verify_256ticks_16players", 16, || {
        black_box(verify_replay(DemoSimulation::new(), black_box(&image.replay)).unwrap());
    });
    // Includes owned-image cloning, required by the public restore interface.
    measure("recovery_restore_256ticks_16players", 8, || {
        black_box(
            MatchRuntime::restore_from_recovery(DemoSimulation::new(), black_box(&image).clone())
                .unwrap(),
        );
    });
}
