# Roadmap

## Milestone A — foundation — completed

- [x] Extract the deterministic authoritative kernel proven in `server-lab`.
- [x] Keep protocol encoding/versioning independent from transport.
- [x] Add a WebTransport/HTTP/3 adapter without moving game authority into transport code.
- [x] Add fail-closed CI for formatting, linting, tests, and release build.

## Milestone B — session runtime — mostly integrated

- [x] Add match/session admission and connection ownership.
- [x] Add reconnect tokens with bounded expiry and one active connection per player slot.
- [ ] Add a general reliable control-message channel distinct from realtime game-command and latest-state datagrams.
- [x] Make disconnect and reconnect lifecycle deterministic and testable.

The existing reliable welcome stream carries admission/reconnect metadata. Realtime game commands and authoritative snapshots deliberately use datagrams. The remaining item is a reusable reliable control path for future transactional/session messages; it must not turn realtime game commands into a reliable queue.

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

### Slice E2 — replay and restore

- [x] Add append-only replay logs for successful player admission/removal, applied commands, and authoritative snapshot checkpoints.
- [x] Add deterministic replay verification against a fresh game simulation.
- [ ] Add graceful draining/shutdown and restart-safe match recovery semantics.

Replay capture is opt-in so ordinary matches do not accumulate unbounded evidence in memory. Captured records have a versioned deterministic binary format, rejected/stale commands are deliberately omitted, and every captured authoritative tick includes a snapshot checkpoint. The verifier replays lifecycle changes and accepted commands in authoritative tick order and fails on the first snapshot hash or payload divergence.

Durable file/object storage is intentionally not hidden inside `MatchRuntime` yet. Making journal persistence part of restart safety requires an explicit policy for I/O failure versus authoritative mutation; that belongs with the remaining recovery slice rather than making game simulation depend on an arbitrary storage backend.

## Milestone F — process hosting

- [ ] Host multiple matches per process with bounded per-match resources.
- [ ] Add placement, draining, and health/ready state.
- [ ] Keep cross-process orchestration out of the core until a real deployment needs it.

The region/process-placement experiments in `server-lab` are evidence for this milestone, not code to copy wholesale. `game-server` should first expose truthful per-process capacity, health, and drain state; a separate fleet scheduler can consume those facts later.

## Deliberately out of scope

Accounts, matchmaking/rankings, game-specific inventory/combat rules, persistent-world databases, provider provisioning, and a custom physics engine are not owned here.
