# Agent Instructions

Reusable server-authoritative multiplayer runtime: match/session lifecycle, deterministic ticks and command ingestion, authoritative snapshots, replay/recovery, WebTransport transport, reliable control and multi-match process hosting. Games supply the simulation; consumers (`mmorpg`, `arcania`, `arpg`, `battle-royale`) pin this crate by git rev, and `card-game-template` is the planned turn-based consumer.

## Read first

Read this file, `README.md`, `ROADMAP.md` and the relevant `docs/adr/` for every task. Authority, determinism, compatibility and Done means below apply to every scope.

## Layout

| Path | Role |
| --- | --- |
| `src/runtime.rs`, `src/session.rs`, `src/simulation.rs` | Authoritative match runtime, sessions/reconnect, the `GameSimulation` seam |
| `src/protocol.rs`, `src/reassembly.rs`, `src/browser.rs` | Versioned command/snapshot wire format, client snapshot reassembly, browser route contract |
| `src/connection.rs`, `src/transport.rs`, `src/host_transport.rs`, `src/control.rs` | Shared WebTransport connection module, single-match and hosted listeners, reliable control |
| `src/host.rs`, `src/host_status.rs`, `src/host_recovery.rs` | Multi-match `MatchHost`, read-only status surface, hosted recovery bundles |
| `src/replay.rs`, `src/recovery.rs` | Replay capture/verification and graceful restart recovery |
| `src/external_simulation.rs` | Versioned bridge for deterministic non-Rust simulations |
| `src/physics.rs` | Optional `physics-engine` adapter (`physics` feature) |
| `src/bin/` | Demo server and probe clients used by the experiments |
| `netem/`, `control/`, `hosting/`, `recovery/` | Real-network acceptance experiments run by CI |
| `benchmarks/` | Opt-in benchmark runner and baselines; CI tests only the comparator |

## Commands

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo build --locked --release --all-features
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s benchmarks -p 'test_*.py'
cargo build --locked --bin game-server --bin netem-client --bin control-client
sudo bash netem/webtransport-experiment.sh     # packet impairment (network namespaces, needs root)
bash control/reliable-control-experiment.sh
bash hosting/match-dispatch-experiment.sh
bash recovery/restart-experiment.sh
bash recovery/hosted-restart-experiment.sh
python3 benchmarks/run.py --output target/benchmarks/current.json --compare benchmarks/baselines/after-runtime-audit.json --cpu 7   # opt-in, not in CI
```

## Authority

- `game-server` owns session identity, admission, command sequencing, tick scheduling, snapshot publication, replay, recovery, reconnect and process hosting.
- Games own their rules through `GameSimulation` (or `ExternalSimulationAdapter`). Do not move game rules, visibility policy or hidden-information decisions into transport or control code.
- `physics-engine` owns physics; integrate it through the adapter, never reimplement it.
- Reliable control never receives `GameSimulation`; authoritative mutation stays on the deterministic command/tick path.
- Accounts, matchmaking, rankings, persistent-world databases, fleet scheduling and provider provisioning stay out of this crate (see `ROADMAP.md`).

## Determinism and compatibility

- Stale or duplicate command sequences, malformed frames, version mismatches and corrupt recovery evidence fail closed.
- Canonical snapshots stay suitable for replay/recovery; player-scoped projections never replace them.
- Wire, replay, recovery, external-simulation, browser-route and host-status formats are versioned. A change to one either stays additive under its current version (as ADR 0004 did) or bumps it, and the README/ADR says which.
- Consumers pin this crate by rev. Public API changes that break a consumer are named in the PR description so the consumer can adapt when it bumps its pin.
- Performance changes need benchmark receipts; never gate correctness on wall-clock timings.

## Execution scope

These rules govern how work is sliced and when expensive checks run. They never relax Authority, Determinism and compatibility or Done means.

- **One task = one branch = one PR.** A task is a tracking issue or a `ROADMAP.md` slice. Deliver its complete declared scope on one branch, including code, tests, experiments and docs, in small commits. Do not split a task into new issues or follow-up PRs on your own; if it cannot land as one PR, stop and propose the split on the issue instead.
- **Stay inside the task.** Do not start tooling, CI, pin-refresh, maintenance or benchmark-baseline work unless the task cannot be completed without it. Note unrelated findings in one line of the PR description; do not open issues for them.
- **No new ratchets unless the task asks for one.** Do not add budgets, baselines or gates on your own initiative. Existing ones stay; when a task legitimately moves one, update it in the same PR.
- **One format bump per task.** Settle wire, replay, recovery and contract version changes before implementing; a task bumps each version at most once.
- **Validate in tiers.** While iterating, run the focused tests for the touched modules. GitHub Actions (`validate.yml`) is the full gate: format, Clippy, tests, benchmark comparator tests, release build and the netem, reliable-control, hosted-dispatch and restart-recovery experiments. Before pushing, run locally only what CI does not cover: the benchmark comparison for performance-motivated changes, and building the requesting consumer against the branch when the task exists for a consumer. A red CI check blocks merge; fix it rather than re-proving it locally.
- **Codex reviews the PR.** Codex reviews automatically when a PR is opened or marked ready, so open it only once the branch is complete. Address or explicitly answer every Codex finding before merge; after substantial fixes, comment `@codex review` for another pass.
- **Decide and continue.** When a task leaves a design choice open, pick the simplest option consistent with this file, record it in the PR description (or an ADR in `docs/adr/` when consequential) and keep going.
- **Short PR descriptions.** At most about 15 lines: what changed, format/compatibility changes, one line naming the checks that ran, and anything not verified.

Tasks arrive as GitHub issues in the format, labels and pickup rules of `docs/AGENT_TASKS.md`; implement only `spec:ready` issues labeled for you. Claude Opus runs the loop with the `/agent-loop` skill (`.claude/skills/agent-loop/`).

## Done means

- Format, Clippy, tests and the release build pass with `--all-features`, and the CI experiments stay green.
- Transport, session, hosting or recovery behavior changes are covered by unit tests and, where the behavior is network-visible, by the matching experiment script.
- Format or contract changes update the README section and the relevant ADR, and state whether the version was bumped.
- `ROADMAP.md` is updated when a roadmap slice completes.
