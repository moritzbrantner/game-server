# Roadmap

## Milestone A — foundation — completed

- [x] Extract the deterministic authoritative kernel proven in `server-lab`.
- [x] Keep protocol encoding/versioning independent from transport.
- [x] Add a WebTransport/HTTP/3 adapter without moving game authority into transport code.
- [x] Add fail-closed CI for formatting, linting, tests, and release build.

## Milestone B — session runtime — completed

- [x] Add match/session admission and connection ownership.
- [x] Add reconnect tokens with bounded expiry and one active connection per player slot.
- [x] Add a general reliable control-message channel distinct from realtime game-command and latest-state datagrams.
- [x] Make disconnect and reconnect lifecycle deterministic and testable.

The reliable welcome stream carries admission/reconnect metadata. Realtime game commands and authoritative snapshots deliberately remain datagrams. General transactional/session control uses separate versioned request/response frames over bidirectional WebTransport streams with a 4 KiB payload ceiling, a five-second stream timeout, and at most four concurrent control exchanges per connection. Control handlers receive the authenticated player ID plus the current connection epoch as a fencing token, but not `GameSimulation`, so delayed external side effects can reject stale reconnect epochs without turning this path into a second game-authority channel. Synchronous handler executions are additionally capped at 64 per server instance; their permits remain held until the handler actually exits even after a transport timeout, preventing detached blocking work from growing without bound. Real-network acceptance verifies successful and rejected exchanges, malformed/oversized/trailing fail-closed behavior, timeout/concurrency bounds, and continued command/snapshot progress while a control stream is stalled.

## Milestone C — pluggable simulation — completed

- [x] Replace the demo movement world with a `GameSimulation` contract supplied by games.
- [x] Keep the runtime authoritative over scheduling, identity, sequencing, and snapshot publication.
- [x] Provide a reference deterministic simulation for tests/examples only.

## Milestone D — physics integration — completed first adapter

- [x] Add an adapter boundary for `physics-engine` rather than duplicating collision/physics logic.
- [x] Pin integration to an explicit `physics-engine` revision and keep it optional for games without physics.
- [x] Add deterministic integration evidence around one authoritative tick -> one physics step and replay fingerprints.

## Milestone E — reliability

### Slice E1 — packet-level WebTransport resilience — completed

- [x] Exercise the real WebTransport/HTTP/3 runtime through isolated Linux network namespaces and kernel `tc netem` qdiscs.
- [x] Verify the reliable welcome stream survives normal packet impairment.
- [x] Verify realtime datagram loss/reordering cannot roll accepted authoritative snapshots backward.
- [x] Verify idempotent retransmission of the newest command converges to the newest authoritative sequence.
- [x] Verify a short 100% packet-loss outage can recover on the same QUIC session before idle expiry.
- [x] Keep exact timing/loss values as measurement evidence rather than benchmark assertions.

The acceptance harness exercises baseline traffic, sustained delay/jitter/loss/reordering/rate impairment, and a 450 ms total-loss window with real kernel qdiscs. The transient-outage run retained connection epoch 1 and converged final sent command sequence 200 to authoritative applied sequence 200 after the link recovered. `tc -s qdisc` drop/requeue counters are retained as evidence; exact latency and packet-loss counts are not benchmark claims.

### Slice E2 — replay and restore — completed

- [x] Add append-only replay logs for successful player admission/removal, applied commands, and authoritative snapshot checkpoints.
- [x] Add deterministic replay verification against a fresh game simulation.
- [x] Add graceful draining/shutdown and restart-safe match recovery semantics.

Replay capture is opt-in so ordinary matches do not accumulate unbounded evidence in memory. Captured records have a versioned deterministic binary format, rejected/stale commands are deliberately omitted, and every captured authoritative tick includes a snapshot checkpoint. The verifier replays lifecycle changes and accepted commands in authoritative tick order and fails on the first snapshot hash or payload divergence.

Graceful recovery freezes authoritative mutation before producing a bounded recovery image, atomically persists verified replay plus reconnect-session state, and restores live slots as disconnected/reconnectable after restart. Recovery-file reads and writes are bounded, startup fails closed on malformed evidence, successfully restored images are consumed only after the WebTransport endpoint is ready, and failed persistence resumes the live runtime rather than exiting with undurable state. This remains graceful restart recovery, not arbitrary crash journaling; durable per-command persistence still requires an explicit storage-failure transaction policy.

## Milestone F — process hosting — completed core

- [x] Host multiple matches per process with bounded match count and existing per-match player capacity.
- [x] Publish a versioned browser route/protocol contract and explicit match-addressed WebTransport path for single-runtime serving.
- [x] Route one WebTransport listener across `MatchHost` entries with isolated admission, reconnect, commands, snapshots, ticks, and reliable control.
- [x] Expose externally served process/match draining and health/ready state.
- [x] Add explicit per-match recovery semantics for process-hosted matches.
- [x] Keep cross-process orchestration out of the core until a real deployment needs it.

The in-process `MatchHost` uses deterministic URL-safe match IDs, bounded placement, explicit per-match/process draining, and safe removal only after active and reconnectable slots are gone. Host and match status expose lifecycle/capacity facts and derive readiness rather than storing mutable readiness state.

The browser-routing contract establishes `/game/matches/<match-id>` plus reconnect addressing and binds that route version to the existing command/snapshot and reliable-control versions. The hosted transport uses that parsed ID to select the authoritative `MatchHost` runtime on one listener. Each match owns an independent tick loop and latest-snapshot channel; player IDs and command watermarks can overlap safely because transport lookup remains match-scoped. Hosted reliable control additionally receives the validated match ID so external side effects are not ambiguous across matches.

The process-host status contract is a separate read-only HTTP surface with process and match `healthz`, `readyz`, and `status` routes. Health remains live through intentional drain, readiness turns `503` before the existing drain grace window, and status reports immutable process/match capacity plus drain/frozen facts without copying game rules or mutable gameplay counters. Service readiness is intentionally distinct from placement capacity, so a fully populated host remains ready to serve its existing matches. Real-network acceptance verifies the status surface alongside isolated multi-match WebTransport routing and observes the not-ready/draining transition after SIGTERM.

Hosted recovery uses a versioned bundle directory containing an exact deterministic match manifest and one existing `RecoveryImage` per `MatchId`. Startup validates every configured match and every bundle entry before restoring anything, then consumes the complete bundle only after the WebTransport endpoint binds. Graceful shutdown drains admissions, freezes every runtime, builds all per-match images, and commits the complete directory with one rename. If image creation or persistence fails, runtimes are unfrozen while the process remains drained so existing sessions/reconnects can continue and shutdown can be retried without admitting new nondurable state. Real-network acceptance proves independent alpha/beta sequence and reconnect continuity and verifies one corrupted match image rejects the entire bundle without partial consumption or fresh-state substitution.

The region/process-placement experiments in `server-lab` remain evidence for this milestone, not code to copy wholesale. `game-server` now exposes the process facts a separate fleet scheduler can consume; scheduler policy, provider provisioning, and cross-process orchestration remain outside the core until a concrete deployment requires them.

## Milestone G — turn-based browser integration

### Slice G1 — player-scoped snapshots — completed

- [x] Keep shared-state simulations on the existing single-encode broadcast fast path.
- [x] Add an explicit player-scoped projection boundary for simulations with private state.
- [x] Keep canonical replay/recovery snapshots separate from player-visible transport snapshots.
- [x] Apply the same visibility behavior to single-match and process-hosted WebTransport serving.

`GameSimulation::snapshot()` remains the canonical deterministic state used for replay and recovery. Hidden-information games opt into `SnapshotScope::PlayerScoped` and provide `snapshot_for(player_id)`; the transport then broadcasts only an update signal and encodes the authenticated player's projection per connection. Transport code therefore never becomes an authority for hand visibility or other game-specific secrecy rules.

### Next slices

- [ ] Add the deterministic external-simulation adapter boundary needed to host non-Rust game logic without copying rules into this repository.
- [ ] Add a cross-repository fixture with `card-game-template` proving that local and server-hosted accepted moves converge to the same deterministic replay fingerprint.

## Deliberately out of scope

Accounts, matchmaking/rankings, game-specific inventory/combat rules, persistent-world databases, provider provisioning, and a custom physics engine are not owned here.
