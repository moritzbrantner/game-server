# Runtime benchmarks

Run from any directory with Python 3, the pinned Rust toolchain, and dependencies already fetched:

```sh
python3 /path/to/game-server/benchmarks/run.py --output /path/to/game-server/target/benchmarks/current.json
```

From the repository root, compare against the accepted local-machine baseline:

```sh
python3 benchmarks/run.py --output target/benchmarks/current.json --compare benchmarks/baselines/after-runtime-audit.json --cpu 7
```

The runner builds the ignored release-mode Rust benchmark through Cargo with `--locked --offline`, then runs the exact test executable reported by Cargo's JSON output. Build directories are isolated by source digest so comparisons between checkouts cannot reuse each other's executable. It uses the actual runtime, broadcast publication type, replay encoder/interpreter, and recovery entry point. No benchmarking dependency or production-facing benchmark interface is added.

## Workloads and interpretation

Every scenario measures nanoseconds per operation; lower is better. Each has five warmup batches and 31 measured batches. Receipts retain all per-batch means, their median, and nearest-rank p95. Setup is outside measurement unless stated below.

| Scenario | One measured operation | Operations per batch |
| --- | --- | ---: |
| `live_command_1024b_no_replay` | Submit a fresh 1 KiB command to a minimal simulation with replay capture disabled | 10,000 |
| `live_tick_65535b_no_replay` | Advance one tick and construct/hash a maximum-sized canonical snapshot without replay capture | 128 |
| `shared_snapshot_fanout_64x1024b` | Publish one pre-encoded 1 KiB shared snapshot and receive it on 64 broadcast subscribers | 512 |
| `replay_encode_256ticks_16players` | Encode 256 ticks, 16 players, and one command per player per tick | 32 |
| `replay_verify_256ticks_16players` | Verify that complete replay against a fresh demo simulation | 16 |
| `recovery_restore_256ticks_16players` | Clone an owned recovery image and restore it through `MatchRuntime` | 8 |

The payload simulation isolates runtime overhead. Maximum-sized snapshots stress copying; they are not a claim about negotiated network datagram capacity. Fan-out excludes encoding and network I/O. These are local microbenchmarks, not throughput, latency, or concurrency guarantees for a deployed game.

## Comparing and tracing changes

Receipts include Git HEAD, a digest of the complete working source (including untracked Rust files), a digest covering both the Rust benchmark and Python runner, the executable digest, the build command and test arguments, toolchain versions, compiler overrides, lockfile digest, and CPU/platform/governor information. Source changes during a run invalidate the receipt. The optional `--convention-source-revision` records the resolved policy revision without making the benchmark depend on external policy tooling.

Comparison refuses mismatched environments, harnesses, scenario sets, or sampling parameters. A regression requires both more than 20% median slowdown and more than the absolute noise allowance: 5 ns for command ingestion and 25 ns for other scenarios. Limits are stored with each baseline. Exit status is 0 for a passing comparison, 1 for a regression, and 2 for an invalid comparison or failed execution.

Use a quiet machine and compare equivalent builds on the same hardware. `--cpu` pins the timed executable, not compilation, to an allowed Linux CPU; the selected CPU is part of the environment fingerprint. The checked-in comparison uses CPU 7. Matching fingerprints do not guarantee identical load, thermal state, or CPU frequency. Receipts record system load before and after measurement. Do not retry a failed benchmark until it happens to pass. Investigate variance or record an intentional trade-off. Measurements remain opt-in; CI runs deterministic tests of the comparator rather than enforcing this machine's timing baseline on arbitrary hosted runners.

[before-runtime-audit.json](baselines/before-runtime-audit.json) measures the source after the connection-lifecycle refactor and before this runtime/replay audit. [after-runtime-audit.json](baselines/after-runtime-audit.json) measures the checked candidate. Both use the same final benchmark harness. Since the work is uncommitted, they share a Git HEAD but have distinct source digests. The before measurement was reproduced in an isolated checkout of the saved source, without reverting the active workspace.

Correctness evidence accompanies the timings: regression tests reproduce ID exhaustion, impossible replay sequences and identity reuse, and unchecked recovery tails; additional tests protect valid gaps, replay-on/off equivalence, exact wire bytes, and shared payload storage. Existing network acceptance covers both serving modes and both restart paths.

## Recorded comparison

These are batch-median measurements from the two versioned receipts, using the same CPU affinity, environment, and harness. Changes are relative to the before measurement.

| Operation | Before, ns/op | After, ns/op | Change |
| --- | ---: | ---: | ---: |
| Command ingestion without replay | 34.8 | 8.1 | −76.8% |
| Maximum-sized snapshot tick | 66,253.4 | 64,415.5 | −2.8% |
| Shared snapshot fan-out | 4,140.2 | 813.4 | −80.4% |
| Replay encoding | 163,024.7 | 106,797.9 | −34.5% |
| Replay verification | 149,129.5 | 175,238.6 | +17.5% |
| Recovery restoration | 601,586.1 | 580,595.4 | −3.5% |

Replay verification is intentionally more expensive because it now enforces identity and sequence invariants. All medians in this controlled comparison are within the committed regression limits. The small tick/recovery differences should not be treated as established speedups; the verification p95 also remains noisy. The clearest improvements are the large reductions in command ingestion, broadcast fan-out, and encoding work.

The `*-unpinned.json` receipts preserve the earlier failed comparison, made during heavy CPU contention. It flagged replay verification and recovery regressions. Diagnostics observed a load average around 31 on 16 allowed CPUs, and the subsequent per-CPU sample showed 86–100% utilization. That finding prompted one comparison with both versions pinned to the same CPU; it did not change the regression thresholds. The older receipts use the earlier runner fingerprint and are retained for investigation, not mixed into the final comparison.
