# Canonical checkpoints start replay suffixes

Status: accepted

## Problem

Graceful recovery replays from a fresh simulation. Periodic external checkpoints cannot bound that replay history: retaining digest checkpoints reduces payload bytes but still needs every prior input and tick. MMORPG #40 needs immutable canonical checkpoints plus short ordered journals without implementing its own replay interpreter.

## Decision

A simulation may implement `GameSimulation::restore_checkpoint`. The default rejects restoration; existing games continue using their original fresh-start replay path. The callback validates game schema/content and restores the exact canonical state. It runs on a fresh, unserved simulation and performs no storage I/O. The shared interpreter compares the resulting snapshot, including tick and hash, before applying later records.

`MatchRuntime::checkpoint_replay` captures a canonical snapshot and the runtime's current player/lane watermarks, highest allocated identity and reconnect session snapshot. It returns an immutable `RecoveryImage` and starts a new in-memory replay suffix at this baseline. Its caller serializes capture with runtime mutation, stores the image off tick and retains the previous durable checkpoint/journal until publication commits. Capturing or compacting a replay is not a storage acknowledgement, an ownership lease or permission to discard durable recovery evidence.

Replay version 5 adds one initial baseline record. It is valid only at the beginning, with sorted nonzero player IDs bounded by the highest allocated ID and the existing snapshot payload limit. The interpreter restores the canonical state, seeds identity/sequence validation and then uses the unchanged accepted-input/tick interpreter. Retired IDs cannot be reused, and duplicate/stale lane commands remain invalid. Trusted host inputs retain their existing semantics. Reconnect credentials and epochs stay in the recovery session section, outside the replay baseline.

## Compatibility and boundaries

Logs without baselines retain byte-identical version 1–4 encoding, including version 4 for trusted host inputs. The outer recovery image and client wire formats do not change. Consumers that match every `ReplayRecord` variant must handle the new `Baseline` variant when adopting this revision.

A baseline is authoritative recovery input, not an integrity signature or a player projection. Storage composition must authenticate/fence its source and protect the complete serialized checkpoint and journal against corruption. This feature supplies no database, asynchronous publication barrier, economic grant policy or object-store implementation. A consumer must opt into the simulation restore callback before using checkpoint capture for cold recovery.

Tests discard the old runtime, restore a nonzero-tick checkpoint and replay only its suffix through both command lanes and trusted inputs. They verify reconnect epochs, stale sequences, retired identities, legacy replay, invalid order/hash/length, repeated baselines and mismatched/unsupported restoration.
