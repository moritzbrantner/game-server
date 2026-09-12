# Roadmap

## Milestone A — foundation

- [ ] Extract the deterministic authoritative kernel proven in `server-lab`.
- [ ] Keep protocol encoding/versioning independent from transport.
- [ ] Add a WebTransport/HTTP/3 adapter without moving game authority into transport code.
- [ ] Add fail-closed CI for formatting, linting, tests, and release build.

## Milestone B — session runtime

- [ ] Add match/session admission and connection ownership.
- [ ] Add reconnect tokens with bounded expiry and one active connection per player slot.
- [ ] Separate reliable command/control messages from latest-state datagrams.
- [ ] Make disconnect and reconnect lifecycle deterministic and testable.

## Milestone C — pluggable simulation

- [ ] Replace the demo movement world with a `GameSimulation` contract supplied by games.
- [ ] Keep the runtime authoritative over scheduling, identity, sequencing, and snapshot publication.
- [ ] Provide a reference deterministic simulation for tests/examples only.

## Milestone D — physics integration

- [ ] Add an adapter boundary for `physics-engine` rather than duplicating collision/physics logic.
- [ ] Pin integration to an explicit `physics-engine` revision and keep it optional for games without physics.
- [ ] Add deterministic integration evidence around tick ownership and replay.

## Milestone E — reliability

- [ ] Add append-only replay logs for admitted commands and authoritative snapshot checkpoints.
- [ ] Add deterministic restore/replay verification.
- [ ] Add graceful draining/shutdown and restart-safe match recovery semantics.

## Milestone F — process hosting

- [ ] Host multiple matches per process with bounded per-match resources.
- [ ] Add placement, draining, and health/ready state.
- [ ] Keep cross-process orchestration out of the core until a real deployment needs it.

## Deliberately out of scope

Accounts, matchmaking/rankings, game-specific inventory/combat rules, persistent-world databases, and a custom physics engine are not owned here.
