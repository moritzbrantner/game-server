# Snapshot fragmentation

Status: accepted

## Problem

The connection module sent each encoded snapshot frame as one WebTransport datagram. When a frame was larger than the negotiated datagram budget, about 1.2 KB on typical paths, it closed the connection with `snapshot exceeds negotiated datagram budget`. Games with player-scoped projections of several kilobytes, such as the MMORPG starter zone, therefore lost the session as soon as a projection grew. The 64 KiB snapshot payload ceiling was far above what one datagram carries on ordinary paths.

## Decision

Keep snapshots on unreliable datagrams and split a frame that does not fit into fragment datagrams.

- A frame that fits the budget is still sent unchanged as one datagram. The shared-snapshot fast path and existing frames are unchanged.
- A larger frame is cut into chunks. Each fragment datagram carries protocol version `3`, frame kind `4`, the snapshot tick, the fragment index and count, the chunk length, and the chunk. Concatenating the chunks in index order yields the exact bytes of the normal snapshot frame. Clients verify the reassembled frame with `decode_snapshot`, including the state hash, so fragments add no second integrity scheme.
- One snapshot uses at most 64 fragments. Any budget of at least 1,039 bytes carries the largest legal frame, 65,555 bytes, within that bound. A snapshot that would need more fragments closes the connection through the existing datagram-budget path; the server never sends part of a snapshot.
- The server computes fragments per connection from the budget read at admission, and enqueues them synchronously under the runtime lock, like whole snapshots.
- `SnapshotReassembler` is the client entry point. It accepts whole snapshots and fragments, delivers strictly increasing ticks, and ignores stale datagrams and duplicate fragments. It keeps at most four incomplete snapshots and 131,110 chunk bytes, and drops the oldest incomplete snapshot first when a bound is reached. It also drops an incomplete snapshot that stores no fragment during 256 datagrams, so incomplete snapshots at ticks no delivery supersedes cannot fill the bounds for the rest of the connection. Idle time is counted in datagrams, not wall-clock time, so the behavior stays deterministic. Malformed or inconsistent fragments return errors without panicking or disturbing other buffered snapshots.

The protocol version stays at `3`. The fragment kind is additive: the server only sends it where it previously closed the connection. Existing version-3 clients, including browser clients that hard-code version `3`, keep working for snapshots within the budget. A version bump would have broken every such client without giving them large snapshots either.

## Alternatives considered

- **Reliable stream for large snapshots.** A stream retransmits stale state and adds head-of-line blocking to a latest-state channel. Datagram loss is already the accepted failure mode for snapshots.
- **Protocol version bump.** Rejected because it breaks clients that never receive a fragment. See above.
- **Forward error correction or retransmission of lost fragments.** More moving parts than the current evidence justifies. The next tick replaces a lost snapshot.
- **Game-side projection splitting.** It would push transport limits into every game's snapshot format and duplicate this logic per game.

## Consequences

Snapshots above the budget now reach clients. Losing any one fragment loses that snapshot, so the chance of losing a snapshot rises with its fragment count. Games should still bound their projections; fragmentation removes the size cliff but not the cost of large snapshots.

Rust clients must feed datagrams through `SnapshotReassembler` or `decode_snapshot_datagram`; the in-repository probes do. Browser clients that need snapshots above the budget must implement the fragment layout documented in the README. `BROWSER_PROTOCOL_CONTRACT.max_snapshot_fragments` publishes the bound.

Evidence: unit tests cover exact wire bytes, chunk boundaries, the 64-fragment limit, the minimum budget for the largest frame, loss, reordering, duplicates, stale ticks, the pending and byte bounds, idle expiry of abandoned far-future snapshots, malicious headers, forged chunks, and tick mismatches. A loopback WebTransport test sends a 12,000-byte player-scoped projection through the real connection module and reassembles it on the client.
