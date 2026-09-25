# ADR 0003: Keep external simulations below runtime authority

## Status

Accepted.

## Context

Some games that should use game-server already own their deterministic rules outside Rust. card-game-template is the immediate case: its turn-based rules are TypeScript and should not be copied into this repository merely to satisfy the GameSimulation trait.

The server still has to remain authoritative for session identity, connection fencing, command sequencing, tick scheduling, replay capture, recovery, and snapshot publication. Treating a JavaScript process or another runtime as a second match server would split those responsibilities and make replay semantics ambiguous.

## Decision

Expose a versioned external simulation request/response contract and adapt it into GameSimulation through ExternalSimulationAdapter.

The external side may:

- describe its fixed tick rate, player capacity, current tick, and snapshot visibility scope;
- add or remove the player ID selected by game-server;
- apply the player ID, sequence, and payload already accepted by game-server;
- advance exactly one authoritative simulation tick;
- return the canonical replay/recovery snapshot;
- return an authenticated player's projection when the simulation declares player-scoped snapshots.

The external side may not allocate identities, admit sessions, reject stale sequences on behalf of the runtime, schedule ticks, manage reconnect tokens or epochs, append replay records, persist recovery images, or decide transport publication.

The protocol is bounded and transport-neutral. The bridge owns how bytes cross into another runtime. This repository does not add a Node, JavaScript, FFI, subprocess, or RPC dependency to the authoritative core.

Every response is matched to the requested operation. Snapshot payloads carry their tick and deterministic state hash. The adapter rejects malformed frames, unknown versions, response-operation mismatches, invalid hashes, and tick drift. A successful advance must move from N to N + 1.

GameSimulation retains remove_player for compatibility with existing Rust consumers and adds try_remove_player with a fallible default. Runtime expiry, failed-admission cleanup, and replay interpretation use the fallible method. Session expiry is inspected before mutation so a failed external removal does not discard the reconnect/session record.

## Consequences

Existing Rust GameSimulation implementations require no source change.

Non-Rust rule engines can be hosted without copying their rules into game-server, but they must implement the external byte contract and provide a bridge with bounded failure behavior.

Replay verification uses the same adapter path as live execution, so an external simulation must deterministically reconstruct the canonical snapshot from the same admitted players, accepted commands, and ticks.

This decision does not select the process/VM mechanism for card-game-template. The next roadmap slice owns the cross-repository fixture and can choose the narrowest bridge that matches that repository without changing the authority boundary.
