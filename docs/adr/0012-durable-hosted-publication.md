# Durable commits gate hosted publication

Status: accepted

## Problem

A simulation can award progression before the generic tick scheduler publishes a snapshot. An application cannot insert its storage commit by observing the runtime afterward: clients may already have seen the result. Player-scoped publication also currently reprojects live state when a connection handles the tick signal, so simply delaying that signal can expose a newer, uncommitted command.

## Decision

`LiveMatchHost::with_persistence` opts into a `SnapshotPersistence` service with an explicit deadline and combined encoded batch/publication byte limit. Replay capture and a simulation-owned canonical restore implementation are required. The application establishes the initial durable recovery basis before enabling serving, including for later placement.

At each tick the host captures canonical state, session/lane metadata, the new accepted replay records and the exact shared or player-scoped frames. It then awaits `SnapshotCommitter::commit` outside the synchronous simulation step. One commit is in flight per match. The runtime mutex intentionally remains held for serial backpressure: no later authoritative mutation can overtake the pending commit, and no database I/O is introduced into `GameSimulation` callbacks. Other matches keep independent runtimes.

The consumer receives only immutable data. It owns database leases/fencing, grant IDs, deduplication, atomic character/journal commits, retained-journal limits and resolution of uncertain transaction outcomes. Its async future must propagate cancellation to I/O and owned workers and must never block the executor or call the live runtime. Deadline or host-task cancellation drops that future. The runtime is pessimistically fenced before awaiting, so cancellation or a callback panic cannot leave a mutable uncommitted writer.

Only success releases the captured frames. Player-scoped delivery selects those immutable frames and revalidates connection fencing; it never projects later mutable state. A newly admitted player skips a frame captured before its admission. After success the in-memory replay is compacted to the committed canonical baseline, and `LiveMatchHost::checkpoint` exposes that last committed image without waiting for a newer in-flight commit. Uploading a checkpoint is not required on every tick: applications can continue durable PostgreSQL journaling while an object store is unavailable, within their configured retention bounds.

A failed/uncertain commit or exceeded limit freezes mutation, closes established connections and publishes no proposed outcome. New admission fails, and generic graceful-recovery retry cannot unfreeze the unresolved writer or save its uncommitted state. The application must retire/recover it from committed evidence; failed matches remain inspectable and can be explicitly retired to release capacity.

## Acknowledgements and recovery

The existing reliable command `Accepted` acknowledgement still means accepted into the in-memory ordered runtime, not economically durable completion. Durable outcomes are acknowledged through committed snapshots. A game that returns an economic outcome through reliable control must itself honour the same required commit before replying. No client wire format changes.

Durable publication cannot be combined with the generic graceful file-recovery bundle: that path could persist a newer uncommitted state. This combination is rejected before endpoint binding. Applications restore their committed checkpoint and ordered journal through the existing recovery interpreter, then construct the durable live host. `ReplayLog::append_journal` validates segment framing/tick order without implementing game rules; full restore still validates player identities and lane sequences. An image anchored by a canonical baseline may end at a digest-verified tick, so a large simulation's compact journal can be restored without requiring another object upload. Unanchored legacy images retain the full final-checkpoint requirement, and mutations after the final verified tick remain invalid.

Non-durable hosts keep their existing tick/publication behavior. Their explicit `checkpoint` method captures a baseline without making a durability promise. Replay and recovery formats remain those introduced in the previous decision.

## Evidence

Controlled async commits prove that pending storage prevents publication and subsequent mutation, checkpoint capture returns the prior committed tick, replay suffixes recover the same continuation, failed/deadline-limited commits freeze authority, configured byte limits reject oversized work before storage, and lifecycle cancellation drops the owned commit future. The connection projection seam proves later mutations do not change captured private frames and stale epochs cannot receive them. Existing transport experiments verify compatibility of the default host path.
