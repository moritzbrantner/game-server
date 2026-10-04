# Replay retains runtime authority

Status: accepted

## Problem

Live command sequencing and player identity allocation belong to the runtime. The replay verifier and recovery restorer independently replayed operations straight into game logic, bypassing those rules. Duplicate commands and reused retired player IDs could pass checkpoint comparison because they can produce the same final payload. Recovery also accepted state mutations after its last checkpoint.

Session ID exhaustion exposed the same identity invariant: a failed admission changed the next ID to zero, allowing subsequent calls to wrap back to previously allocated IDs.

## Decision

Use one replay interpreter for verification and restoration. It tracks the highest admitted ID and current players' command watermarks. IDs and sequences may skip values, but cannot move backward or repeat. A command for a removed or never-admitted player is rejected before invoking game logic. Recovery additionally requires the final record to be a checkpoint at the recovery tick.

Compute the next session ID with checked arithmetic before changing registry state. Exhaustion consistently rejects admissions and preserves existing sessions and reconnect capabilities.

## Compatibility and cost

Valid replay and recovery wire formats are unchanged; a fixed version-one replay fixture verifies byte compatibility. New typed replay errors identify impossible histories, and recovery has a missing-final-checkpoint error. Previously accepted invalid evidence now fails closed.

The interpreter performs additional identity and sequence checks during replay. Benchmarks report that validation cost explicitly. Live command and snapshot paths avoid constructing replay payload copies when capture is disabled, shared broadcast receivers reuse immutable encoded storage, and replay encoding writes records directly into its output buffer. These optimizations preserve runtime behavior and wire bytes.

See [benchmark evidence](../../benchmarks/README.md) for measurements, workload definitions, and repeatable commands.

## Compact tick checkpoints

Version-two logs can retain a SHA-256 digest instead of the full canonical payload at each tick. The domain-separated digest covers the tick, state hash, payload length, and exact canonical payload. Verification and recovery still advance and check every tick using the same interpreter; identity, command sequence, and maximum tick gap rules are unchanged. Digests are private recovery evidence and never become player snapshots.

The runtime selects digest checkpoints when the canonical payload exceeds 20 bytes, where the 45-byte digest record is smaller than a full checkpoint. Recovery always appends a full final checkpoint. Logs containing only existing record kinds continue to encode as version one, preserving the fixed byte fixture; the decoder accepts both versions and rejects digest records under version one.

Tick history now grows by at most 45 encoded bytes per tick, independent of canonical payload size. This bounds six hours at 20 Hz to 19,440,000 tick-record bytes, plus commands, session events, and the full final checkpoint. History still grows with match duration and authoritative events; consumers must enforce their own match lifetime. The existing recovery image size limit remains enforced.

## Runtime presence in player projections

`GameSimulation::snapshot_for_with_context` receives a borrowed `PlayerSnapshotContext` under the runtime lock. Its only query is whether a player currently owns a connected session. Grace-disconnected and missing identities are offline. The context exposes neither tokens nor epochs and permits no mutation. The default delegates to the existing player-scoped projection, preserving existing consumers and its fail-closed default.

This context is presentation evidence, excluded from canonical snapshots and replay. After recovery all restored slots are disconnected until successfully reconnected; projections query that current authority rather than replaying past connection state. Game adapters continue to validate recipients and scope private payloads.
