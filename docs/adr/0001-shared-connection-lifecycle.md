# Shared connection lifecycle

Status: accepted

## Problem

The single-match and hosted WebTransport listeners independently implemented welcome handshakes, rollback, command handling, snapshot visibility, control framing, timeouts, and concurrency limits. Changes to one copy could silently leave the other with different session semantics.

The duplicated implementations also left lifecycle gaps:

- Snapshot delivery accepted a player ID without checking whether its connection epoch still owned the session.
- A control request could wait for handler capacity, survive disconnect/reconnect, and execute with the old epoch.
- Dropping a timed-out control exchange detached its blocking task, including work that had not started yet.
- Dropping a serving future detached its connection and tick tasks, allowing them to retain authoritative state.

Active reconnect tokens were already rejected by the session registry. The snapshot change strengthens the delivery seam; it does not change that reconnect rule or claim that an active connection could previously be replaced over the network.

## Decision

Keep one private `connection` module shared by both listeners. It owns admission, welcome and rollback, established connection processing, snapshot projection and encoding, control framing, and exchange limits. Hosted routing binds a runtime and a match-specific control adapter once. The host continues to own process draining, match placement, tick scheduling, and recovery persistence.

The runtime supplies one connection-ownership check, reused by commands, snapshot delivery, and control dispatch. Snapshot authorization, projection, and synchronous datagram enqueue occur under the same runtime lock. Shared snapshots retain their once-per-tick encoding; private canonical payloads never enter the broadcast channel.

Use Tokio task sets for server-owned connection/tick tasks, connection-owned exchanges, and exchange-owned blocking work. Completed connection and control tasks are reaped. Canceling an owner aborts pending children, and normal server shutdown awaits cancellation of its tasks.

Control dispatch holds the global capacity permit inside the blocking closure and checks the epoch there, immediately before invoking the supplied handler. The runtime lock is released before external handler code runs. This preserves authoritative tick progress and prevents already-running handlers from escaping the global capacity limit after cancellation.

## Consequences

Both hosting modes use the same session behavior and regression tests. Public serving interfaces, wire formats, and recovery formats remain unchanged. No dependency is added.

Synchronous handlers already running cannot be forcibly stopped. External effects committed after dispatch still require epoch fencing by the supplied handler or downstream system. Graceful recovery remains an explicit shutdown operation; canceling a serving future is resource cleanup, not a request to persist state.

Regression coverage includes stale shared/private snapshot leases, reconnect while a control request waits for capacity, cancellation while a handler waits in the blocking pool, capacity retention and tick progress during a running handler, welcome rollback, and release of both single-match and hosted tick tasks. The existing network experiments cover both listeners and their recovery paths.
