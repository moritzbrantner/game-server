# Reliable ordered command lane

Status: accepted

Requested by moritzbrantner/game-server#43 for moritzbrantner/arpg#143.

## Problem

Game commands reached the authority only as WebTransport datagrams under one per-player sequence watermark. A lost datagram was never sent again, and a higher-sequence datagram that arrived first made a lower one `IgnoredStale`. That is right for held state, where only the latest input matters. It is wrong for one-shot commands such as an attack or an interaction, which must arrive exactly once. Reliable control streams cannot carry them, because control services have no `GameSimulation` access by design.

## Decision

Add a second command lane beside the unchanged datagram lane. `CommandLane::{Datagram, Reliable}` names the path a command took.

**Runtime.** `MatchRuntime::submit_lane_command` keeps one watermark per (player, lane). `submit_command` is the datagram lane. A sequence at or below its lane's watermark returns `IgnoredStale` and is not applied again. Sequence zero, an oversized payload, a stale epoch and a frozen runtime are errors on both lanes. No error, including an ADR-0005 `command_rejected`, consumes the sequence or replay admission. Both watermarks survive disconnect and reconnect within the grace window. They are dropped only when the player is removed, so a fresh admission starts both at zero.

**Simulation.** The runtime and replay call `GameSimulation::apply_lane_command(player_id, lane, sequence, payload)`. Its default delegates to `apply_command`, so existing games compile and behave unchanged. A game that cares about the lane overrides the new method.

**Wire.** A client opens at most one bidirectional stream per connection as its command lane. It writes the six-byte header `GSCL`, the lane format version (`COMMAND_LANE_FORMAT_VERSION`, 1) and the game protocol version (3). Then it writes ordinary protocol-v3 command frames in order. The server answers every complete frame, in order, with a seven-byte ack: protocol version, kind `6`, big-endian `u32` sequence, and an outcome byte (`1` applied, `2` duplicate, `3` rejected). Decoding is exact and canonical.

The server tells a lane from a control stream by its first four bytes. `GSCT` and any other prefix keep the existing control path with its permits and its five-second deadline, which still starts when the stream is accepted. Classification uses its own bound of four pending streams per connection. It takes no control permit and holds no runtime lock, and it shares the control deadline, so a stream that never sends four bytes is dropped.

**Ack semantics.**

- `Applied`: the command was applied and admitted to replay.
- `Duplicate`: the sequence was at or below the reliable watermark, so it was applied earlier, possibly on a previous connection.
- `Rejected`: the simulation privately rejected the command. The rejection is final for that attempt, and the sequence was not consumed, so the client may send the same sequence again once the game would accept it.

The ack is the only rejection signal on the lane. The server does not also send the ADR-0005 kind-5 payload, because kind-5 frames carry no lane, and a sequence present on both lanes would be ambiguous. Games that need a reason can expose it in a player-scoped projection. A later lane format version can add a rejection payload.

**Fail closed.** These lane faults close the whole connection with the protocol close code (1), exactly like a malformed datagram command:

- a wrong header;
- a frame with the wrong version or kind;
- sequence zero;
- a declared payload above `MAX_COMMAND_PAYLOAD_BYTES`, which is rejected before its payload is read;
- a partial frame followed by FIN or a stream reset;
- a frame that started but did not finish within ten seconds;
- an ack write failure;
- a fatal simulation error;
- a second lane on the same connection, including after a clean finish (`MAX_COMMAND_LANES_PER_CONNECTION` is 1).

A FIN at a frame boundary ends the lane cleanly. The server then finishes its ack stream and keeps the connection open.

**Frozen runtime.** A command that reaches a frozen runtime is not acknowledged. The connection closes with the runtime close code (3). The client keeps the command unacknowledged and sends it again after it reconnects to the recovered match, or to the same match if recovery failed and the runtime resumed. Silently dropping the command and continuing would let a later sequence raise the watermark, and the re-sent command would then count as a duplicate.

**Isolation.** The lane runs as its own task owned by the connection. When the connection ends, the task is cancelled. The lane holds the runtime lock only for the synchronous submission, never across I/O. A stalled lane, a slow partial frame, or a client that never reads acks only stops that lane. QUIC flow control applies the backpressure. Ticks, snapshots, datagram ingestion and control continue.

**Replay and recovery.** `ReplayRecord::CommandApplied` carries the lane. `REPLAY_FORMAT_VERSION` is 3, which adds record kind `6` for reliable-lane commands with an explicit lane byte. Datagram commands keep kind `2`. Encoding picks the oldest version that can carry the log, so datagram-only logs stay byte-identical to their version 1 or version 2 encoding. Versions 1 and 2 decode commands as `Datagram` and reject kind `6`, so downgrading a lane-aware log fails instead of reinterpreting it. Verification and recovery enforce strictly increasing sequences per (player, lane), and `NonIncreasingSequence` names the lane. A recovery image restores both watermarks from its replay (`RecoveryImage::last_lane_sequences`). The recovery image format itself is unchanged.

**Browser contract.** `BROWSER_PROTOCOL_CONTRACT` publishes the lane format version, the header and ack sizes, and the lane limit.

## Consequences

One-shot commands are delivered exactly once while the player keeps its slot, including across reconnects within the grace window. Clients keep unacknowledged lane commands and send them again in order on the new lane. Duplicates are acknowledged and not applied. Held state stays on datagrams with unchanged latest-wins behaviour. Clients may use one sequence counter for both lanes or one counter per lane.

Reliable ordering is per lane, not across lanes. A reliable command and a datagram command can reach the simulation in either order. Replay records the order that actually happened.

Delivery is not guaranteed after the player is removed or when the process fails without graceful recovery. This is still graceful restart recovery, not per-command crash journaling. `ExternalSimulationAdapter` does not forward the lane yet, so external simulations receive lane commands through the default `apply_command` path.

Tests cover lane-independent watermarks, overtaken reliable commands, duplicates after reconnect, rejection without consumption, removal resetting watermarks, mixed-lane replay and recovery round trips, legacy replay fixtures, per-lane monotonic verification, and exact codecs. Real loopback WebTransport tests cover acks, the second lane, malformed, oversized, truncated and fatal input, stalled lanes beside realtime progress, and delivery while every packet is dropped. In-memory lane tests cover the partial-frame timeout, unread acks without holding the lock, and the frozen runtime.

Shared convention source revision: `46d8793bb3034326561f876dcc67dbaa5aa1e432`.
