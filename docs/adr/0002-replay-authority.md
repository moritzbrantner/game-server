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
