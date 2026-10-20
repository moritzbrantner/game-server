# game-server

Reusable server-authoritative multiplayer runtime extracted from the proven `server-lab` authority/WebTransport experiments.

## Ownership

`game-server` owns reusable authoritative session runtime concerns:

- server-owned match/session identity and lifecycle;
- deterministic tick scheduling and command ingestion;
- authoritative snapshots and replay evidence;
- WebTransport/HTTP/3 transport adapters;
- reconnect/resume semantics and graceful recovery;
- multi-match process hosting and draining.

It does **not** own game-specific rules, matchmaking/accounts/rankings, or physics algorithms. Games supply deterministic simulation logic. Physics is delegated to `physics-engine` through an adapter boundary.

Single-match and hosted listeners share one internal connection module for admission, welcome rollback, command ingestion (datagrams and the reliable command lane), snapshot delivery, and reliable control. Hosted routing binds the match identity before entering that module. Server tasks own their connection and tick tasks; each connection owns its control exchanges, so canceling the owner also cancels pending work. See [the connection lifecycle decision](docs/adr/0001-shared-connection-lifecycle.md).

The welcome, command rejections and control responses count as delivered once the server has written and finished them locally. The server does not wait for the client's ACK, and a client that reads exactly one frame and drops the stream is fine. The five-second welcome bound covers opening and writing. Rollback applies only to a welcome that was never completely written, so the token from a received welcome stays valid: after an outage the session is usable on the same connection, or the connection ends and that token (the latest one, after a reconnect) resumes the same player within grace. See [the local stream finish decision](docs/adr/0009-local-stream-finish.md).

## Browser integration contract

`browser` exposes the versioned browser-facing boundary without creating a second gameplay protocol. `BROWSER_PROTOCOL_CONTRACT` binds the route version to the existing command/snapshot protocol version, reliable-control format version, reconnect-token size, payload ceilings, snapshot fragment bound, and reliable command lane format (lane format version, header and ack sizes, one lane per connection) so browser clients can pin one explicit compatibility surface.

Hosted browser deployments can set `MatchHostWebTransportConfig::allowed_origins` to `Some(BrowserOriginAllowlist::new(["https://board.example"])?)`. The allowlist requires canonical HTTP(S) origins and rejects missing/nonmatching headers before admission or reconnect fencing. `None` permits native/local clients without an Origin. This is a browser boundary, not authentication; see [the origin decision](docs/adr/0007-browser-origin-allowlists.md).

Use `BrowserRoutePrefix` plus a validated `MatchId` to address a match. With a `/game` base path and match ID `uno_01`, the canonical paths are:

```text
/game/matches/uno_01
/game/matches/uno_01/reconnect/<32-hex-character-token>
```

`BrowserRoutePrefix::parse` fails closed for malformed owned routes and returns no match for unrelated paths. Match IDs keep the existing URL-safe 64-byte bound, and reconnect tokens continue to use the same rotating capability already enforced by the session runtime.

The single-match demo mode opts into this addressing when `GAME_SERVER_MATCH_ID` is set. `GAME_SERVER_SESSION_PATH` then means the browser route base path rather than the full session path. If `GAME_SERVER_MATCH_ID` is absent, the old exact-session-path behavior remains available for existing experiments.

For process-hosted matches, `serve_match_host*` owns one WebTransport listener and dispatches each canonical match/reconnect route to the matching `MatchHost` runtime. Unknown or malformed routes fail closed before admission. `GAME_SERVER_MATCH_IDS=alpha,beta` enables that mode in the demo server and serves `/game/matches/alpha` and `/game/matches/beta` from the same listener.

## Snapshot visibility

`GameSimulation::snapshot()` is the canonical authoritative snapshot used by replay and recovery. Simulations with fully shared state keep the default `SnapshotScope::Shared`; the transport encodes that snapshot once per tick and, when the frame fits a connection's datagram budget, sends that same datagram to every connection. A shared frame above the budget is fragmented separately for each connection, from that connection's current budget, while its connection task holds the runtime lock. Connections can then receive different datagrams, and the copy and allocation cost grows with connection count and frame size (see [Snapshot fragmentation](#snapshot-fragmentation)).

Games with private state must opt into `SnapshotScope::PlayerScoped` and implement `snapshot_for(player_id)`. The default projection fails closed instead of falling back to the canonical snapshot. In that mode the transport publishes only an update signal, then asks the authoritative runtime for the addressed player's projection before encoding a datagram. Canonical snapshot bytes therefore never enter the connection broadcast channel. The projected snapshot carries its own hash over the player-visible payload, while replay and recovery continue to verify the canonical full-state snapshot.

Both shared and player-scoped delivery validate the current connection epoch under the runtime lock before sending. A disconnected or replaced lease cannot receive a snapshot through the connection module.

`GameSimulation::connection_changed(player_id)` lets Rust consumers invalidate ephemeral presentation state, such as tentative placements, after an accepted disconnect or reconnect. Stale epochs and invalid reconnect tokens produce no notification. The default is a no-op; implementations must keep canonical snapshots and gameplay unchanged. These notifications are intentionally absent from replay. Recovery restores sessions offline, and the first accepted reconnect notifies the simulation before it can serve that connection.

This boundary is intended for hidden-information games such as card games. It keeps visibility policy in the supplied game simulation rather than duplicating game rules in WebTransport handlers.

## Snapshot fragmentation

A connection sends each encoded snapshot frame as one WebTransport datagram when it fits the connection's current datagram budget. A larger frame is split into snapshot fragment datagrams instead of closing the connection. The budget is read again for every snapshot because it follows the QUIC path MTU estimate, which can shrink after admission, for example after black-hole detection or connection migration. If the path still refuses a fragment, the rest of that snapshot is not sent and the next snapshot uses the new budget. A connection sends at most one frame per tick and never a tick older than one it already sent. A player-scoped projection is built when the connection handles a publication, so a connection that falls behind could otherwise build the same tick twice with different bytes. The fragments of one tick therefore always belong to one frame. Concatenating the chunks of one tick in index order gives the exact bytes of the normal snapshot frame, so the reassembled frame is verified by `decode_snapshot`, including its state hash.

Fragment datagrams use frame kind `4` within protocol version 3. Integers are big-endian:

| Bytes | Field |
| --- | --- |
| 0 | Protocol version, `3` |
| 1 | Frame kind, `4` |
| 2..10 | Snapshot tick, `u64` |
| 10 | Fragment index, below the fragment count |
| 11 | Fragment count, `1..=64` |
| 12..14 | Chunk length, `u16`, non-zero; the datagram length must match exactly |
| 14.. | Chunk bytes |

One snapshot uses at most `MAX_SNAPSHOT_FRAGMENTS` (64) fragments. Every fragment except the last carries a full chunk. Any datagram budget of at least `MIN_FRAGMENTED_DATAGRAM_BYTES` (1,039 bytes) carries the largest legal snapshot frame (65,555 bytes). WebTransport budgets derived from QUIC's 1,200-byte minimum packet size are normally above that bound. If a snapshot would still need more fragments, for example because a peer advertised a smaller datagram limit, the connection closes with the existing datagram-budget error instead of sending part of a snapshot.

Rust clients pass every received datagram to `SnapshotReassembler::accept` and use one reassembler per connection. It returns whole snapshots directly and fragmented snapshots once every fragment has arrived and the frame verifies. Delivered ticks strictly increase. Stale datagrams, duplicate fragments, and fragments of superseded ticks are ignored. The reassembler keeps at most four incomplete snapshots and 131,110 chunk bytes; when either bound is reached, the oldest incomplete snapshot is dropped. An incomplete snapshot that stores no new fragment during 256 datagrams (`SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS`) is dropped as well, so abandoned fragments at any tick, including far-future ticks, cannot hold the bounds. Malformed or inconsistent datagrams return an error and leave the reassembler usable. `SnapshotReassemblyStats` exposes deterministic counters for each outcome. Clients that manage their own buffering can classify a datagram with `decode_snapshot_datagram`.

Losing one fragment loses that snapshot only; the next snapshot replaces it. The chance of losing a snapshot grows with its fragment count, so games should still keep player-scoped projections compact. Fragmentation removes the size cliff; it does not make large snapshots free.

The fragment kind is additive. Command, snapshot, and welcome frames are unchanged, and servers send fragments only for snapshots that previously closed the connection. Version-3 clients therefore keep working for snapshots within the budget. Browser clients that need larger snapshots implement the layout above; `BROWSER_PROTOCOL_CONTRACT.max_snapshot_fragments` publishes the bound. See [the snapshot fragmentation decision](docs/adr/0004-snapshot-fragmentation.md).

## External simulation boundary

`ExternalSimulationAdapter` lets a deterministic simulation implemented outside Rust participate in the existing `GameSimulation` authority boundary. The external side receives only simulation operations: describe, add/remove player, apply an already-sequenced command, advance exactly one tick, and produce canonical or player-scoped snapshots. It does not admit sessions, allocate player IDs, choose command ordering, schedule ticks, manage reconnects, or write replay/recovery evidence.

The byte contract is versioned independently as `EXTERNAL_SIMULATION_PROTOCOL_VERSION`. Requests and responses carry an operation code and use bounded big-endian fields. Command and snapshot payload ceilings are the same as the normal game protocol; remote error payloads are capped at 1 KiB. Snapshot responses include their tick and state hash, which the Rust adapter verifies before exposing them to the runtime. `EXTERNAL_SIMULATION_PROTOCOL_CONTRACT` publishes the current version and bounds for bridge implementations.

The adapter caches only immutable descriptor data and the runtime-observed tick. Every advance request carries the exact target tick, so a bridge that applied a tick but lost its response can return that already-completed target on retry instead of advancing twice. A successful external tick must advance by exactly one from the runtime's last observed tick. Snapshot reads must report that same tick. Mismatched versions, operations, lengths, hashes, response shapes, or ticks fail closed.

Existing Rust simulations keep the infallible `remove_player` compatibility method. Runtime, replay, and recovery now use `try_remove_player`, whose default delegates to `remove_player`; external simulations override it so bridge failures cannot be mistaken for a successful session expiry.

See [the external simulation authority decision](docs/adr/0003-external-simulation-authority.md).

## Reliable control

Realtime game commands and latest authoritative snapshots use WebTransport datagrams; one-shot game commands can use the [reliable command lane](#reliable-command-lane). Transactional/session control uses an independent versioned request/response frame over bidirectional WebTransport streams that start with `GSCT`. Each payload is bounded to 4 KiB, each exchange is time-bounded to five seconds, and each connection can have at most four control exchanges in flight.

Single-match applications opt in with `serve_with_control` or `serve_with_control_and_shutdown` and provide a `ControlService`. Hosted applications use `serve_match_host_with_control*` and provide a `MatchControlService`, which receives the validated `MatchId` in addition to the authenticated player ID and connection epoch. That preserves fencing identity even when different matches both allocate player ID 1. Neither control service has access to `GameSimulation`; authoritative game mutation therefore remains on the deterministic command/tick path.

Control handlers run off the async runtime. A server instance admits at most 64 handler executions at once, and that permit remains occupied until the synchronous handler actually exits even if its WebTransport exchange has already timed out. This prevents repeated transport timeouts from creating an unbounded tail of detached blocking work.

Handler dispatch rechecks the connection epoch after waiting for capacity and after entering the blocking pool. Closing a connection cancels its pending exchanges; canceling an exchange also cancels handlers still queued in the blocking pool. A synchronous handler that has already started cannot be interrupted. It runs without holding the simulation lock and remains responsible for fencing delayed external side effects using its control context.

The acceptance probes cover successful and rejected control exchanges, malformed, oversized, and trailing-byte fail-closed handling, stalled-stream timeout, the per-connection concurrency cap, continued realtime command/snapshot progress while a control stream is stalled, and match-scoped control routing through one hosted listener.

## Reliable command lane

Datagram commands stay best-effort and latest-wins, which suits held input. One-shot commands that must arrive exactly once use the reliable command lane instead. It is an ordered, acknowledged path into the same deterministic command/tick path. The runtime keeps an independent per-player watermark for each `CommandLane` (`Datagram`, `Reliable`), so later datagram commands never make a reliable command stale. `MatchRuntime::submit_lane_command` takes the lane, and `submit_command` stays the datagram lane. Simulations receive the lane through `GameSimulation::apply_lane_command`, whose default delegates to `apply_command`.

A client opens one bidirectional stream per connection and writes the header from `encode_command_lane_header()`: `GSCL`, lane format version `1`, and protocol version `3`. Then it writes the existing `encode_command` frames in order. The server answers each frame, in order, with one fixed `RELIABLE_COMMAND_ACK_BYTES` (7-byte) ack that `decode_reliable_command_ack` reads. The ack bytes are the protocol version, kind `6`, a big-endian `u32` sequence, and an outcome:

- `Applied`: the command was applied and admitted to replay.
- `Duplicate`: the sequence is at or below the reliable watermark, so it was already applied, possibly on an earlier connection. It is not applied again.
- `Rejected`: the simulation returned an ADR-0005 `command_rejected`. Neither the sequence nor replay admission is consumed, so the client may send the same sequence again later. The ack is the only rejection signal on the lane; the private kind-5 payload is not sent for lane commands.

The reliable watermark survives disconnect and reconnect within the grace window. It resets only when the player is removed. After a reconnect, a client opens a new lane and sends every unacknowledged command again in order; each command is applied exactly once.

The lane fails closed. These faults close the connection with protocol close code `1`:

- a wrong header, version, or kind;
- sequence zero;
- a declared payload above `MAX_COMMAND_PAYLOAD_BYTES`;
- a partial frame followed by FIN;
- a frame that does not complete within ten seconds of its first byte;
- a fatal simulation error;
- a second lane on one connection.

A FIN at a frame boundary only ends the lane. A command that reaches a frozen runtime is not acknowledged; the connection closes with code `3`, and the client sends the command again after it reconnects. The lane is a task owned by the connection. It holds the runtime lock only for the synchronous submission and never while it waits for I/O. A stalled lane, or a client that never reads its acks, therefore never blocks ticks, snapshots, datagram ingestion, or control. Streams are classified by their first four bytes without taking a control permit, under the existing five-second control deadline.

See [the reliable command lane decision](docs/adr/0008-reliable-command-lane.md).

## Trusted host inputs

`MatchRuntime::submit_host_input(player_id, payload)` applies trusted server data
through `GameSimulation::apply_host_input` in order with accepted commands, before
the next tick. The callback rejects by default, must be deterministic, and must
leave state unchanged on error. Perform storage I/O before submitting; replay and
recovery use the captured bytes without reading external storage again. Payloads
use `MAX_COMMAND_PAYLOAD_BYTES`; host inputs do not consume client sequences.

While serving, trusted application code uses
`LiveMatchHost::submit_host_input(match_id, player_id, connection_epoch, payload)`.
It rechecks connection ownership under the runtime lock and rejects drained,
frozen, retired or missing matches/players. No client wire route exposes this API.
Games still own validation, admission policy, durable leases and idempotency IDs.

Logs containing host inputs encode as replay version 4 (record kind 7); existing
logs retain their version 1–3 encoding. Older readers reject version 4. Recovery
images retain their outer format and embed the versioned replay log. Host input
payloads are private recovery data and omitted from debug output.

## Multi-match host

`MatchHost` owns a bounded set of homogeneous `MatchRuntime` instances inside one process. Match IDs are URL-safe ASCII identifiers capped at 64 bytes, iteration is deterministic, and placement fails closed on duplicate IDs, process drain, or configured match capacity. Failed placement returns a `PlacementFailure` containing the original ID and runtime intact, so authoritative state is never discarded merely because placement must be retried elsewhere. Existing per-match player capacity remains owned by each simulation/runtime rather than being duplicated in the host.

`serve_match_host*` routes admission, reconnect, commands, authoritative snapshots, and reliable control to the addressed hosted runtime. Each match has an independent snapshot channel and tick loop; player/session numbering and command watermarks stay isolated by `MatchId`. The real-network acceptance starts two matches on one listener, proves both independently allocate player ID 1, verifies different command sequences converge only in their addressed match, checks match-scoped control identity, and rejects an unknown match route.

For applications that create matches while serving, construct `LiveMatchHost::new(MatchHost::new(max_matches)?)`, retain a clone as the trusted management handle, and pass it to `serve_live_match_host_with_control_and_shutdown` or `serve_live_match_host_with_status_and_control_and_shutdown`. The live variants accept an empty host. `is_serving()` becomes true only after TLS and endpoint startup; placement before that point fails with `NotServing`. An empty live process is then ready to accept placement. Placement capacity is reported separately, so a full process can still be ready to serve its existing matches. Each status request observes current membership; retired IDs disappear from match status routes.

The consumer constructs the simulation and runtime, chooses the public match ID, and owns any game-specific creation policy, idempotency, and expiration policy. `place(id, runtime).await` serializes duplicate-ID, capacity, and process-drain checks inside the host and starts an independent tick task before returning. A failed placement returns `LivePlacementFailure`; `into_parts()` retains the supplied runtime intact. The trusted `inspect` callback can read a runtime under its lock, but must not block or publish private canonical data. Neither management capability is a browser credential, and gameplay changes still enter through the normal sequenced command path.

`retire(&id).await` immediately removes the route from admission, freezes the old runtime to fence commands and reconnects, closes active sessions, and stops its tick task before freeing the capacity slot. Repeated or unknown retirement returns `UnknownMatch`. Caller cancellation does not abandon retirement. An ID can be reused only after retirement finishes; the replacement has a fresh runtime and rejects the old runtime's reconnect tokens. Retirement discards that match's state rather than saving a recovery image. Placement and retirement return `Draining` once process drain begins. Trusted application retry paths can query `is_draining().await`; this observation serializes with process drain, rather than inferring it from individual match flags. A serving handle has one transport owner and cannot be served again after shutdown; dropping that owner stops its tasks even if an application retains a management clone.

Draining is explicit at both match and process level. Process shutdown marks the full host draining before the grace window, so new admissions fail while existing reconnects can still use their addressed runtime.

Hosted serving can additionally expose a read-only HTTP status surface through `serve_match_host_with_status_and_control_and_shutdown`. The demo server enables it for `GAME_SERVER_MATCH_IDS` mode on `GAME_SERVER_STATUS_PORT` (default `8080`). The contract is versioned by `HOST_STATUS_CONTRACT_VERSION` and exposes:

```text
GET /healthz
GET /readyz
GET /status
GET /matches/<match-id>/healthz
GET /matches/<match-id>/readyz
GET /matches/<match-id>/status
```

Health is process liveness and remains `200` during an intentional drain. Readiness means the hosted process or addressed runtime is available to serve gameplay rather than whether another match can be placed; a host already at its configured match count can therefore still be ready. Process and match readiness return `503` once draining begins, while `/status` reports the drain flag and current host membership and match capacity facts. Unknown match routes and mutating methods fail closed. The status surface deliberately does not duplicate game rules, transport admission, or mutable gameplay counters.

## Durable hosted publication

For application-owned durable storage, opt into `LiveMatchHost::with_persistence(host, SnapshotPersistence::new(service, timeout, max_batch_bytes)?)`. `SnapshotCommitter` receives an immutable tick snapshot, new replay journal records and a canonical recovery checkpoint. Its cancellable async future returns success only after the required commit. Replay capture and canonical checkpoint restore must be enabled, and the application's initial recovery basis must already be durable.

The scheduler pauses that match's mutation while storage commits, then publishes the captured frames. Player-scoped connections never reproject newer uncommitted state. Deadline, commit failure or an exceeded configured byte limit freezes the writer and closes its connections; recovery retry cannot unfreeze it. Other matches keep their independent ticks. Call `LiveMatchHost::checkpoint` to obtain the latest committed image for a periodic object upload; it remains available while a newer tick is waiting on storage. Append committed journal segments through `ReplayLog::append_journal` when reconstructing recovery evidence.

This service supplies ordering/publication, not a database, zone lease or economy implementation. The existing reliable command `Accepted` ACK is in-memory acceptance; economically durable outcomes appear in committed snapshots. Control handlers returning economic results must commit before replying. Durable hosts use application-owned recovery and cannot also enable generic graceful file-recovery bundles. See [durable publication](docs/adr/0012-durable-hosted-publication.md).

## Graceful recovery

Set `GAME_SERVER_RECOVERY_PATH` to enable replay-backed graceful restart recovery for the single-match transport. On SIGTERM or Ctrl-C the runtime drains, freezes authoritative mutation, writes a bounded recovery image atomically, and then closes established sessions. On the next successful endpoint startup the image is verified, authoritative state and command watermarks are reconstructed, and saved sessions become disconnected/reconnectable before the consumed image is removed.

Recovery persistence is intentionally fail-closed: malformed evidence prevents startup, failed shutdown persistence resumes the live runtime, recovery I/O does not hold the runtime mutex, and the reliable welcome handshake is time-bounded so a peer cannot retain capacity indefinitely. The bound covers writing the welcome, not the client's acknowledgement of it; a welcome that was completely written is never rolled back. This is graceful restart recovery rather than per-command crash journaling.

Replay verification and recovery share one interpreter that enforces increasing player identities and strictly increasing command sequences per player and command lane. Replay format version 3 adds lane-aware command records. Logs without reliable-lane commands still encode byte-identically as version 1, or as version 2 when they contain digest checkpoints, and older logs decode as datagram commands. Recovery restores both lane watermarks. Retired identities cannot be reused, commands require a live player, and recovery images must end with an authoritative checkpoint. These checks enforce runtime rules even when the supplied simulation would accept an impossible history. See [the replay authority decision](docs/adr/0002-replay-authority.md).

For a simulation that supports canonical cold restore, implement `GameSimulation::restore_checkpoint` and call `MatchRuntime::checkpoint_replay` at an application-selected tick boundary. It returns an immutable recovery image and replaces the in-memory replay prefix with a canonical baseline, retaining active player/lane watermarks and the retired identity ceiling. Recovery restores that checkpoint and interprets only subsequent records through the existing replay engine; sessions still resume with fenced connection epochs. The callback defaults to rejection, so existing games must explicitly opt in.

Capture performs no storage I/O and proves no durability: retain the previous durable checkpoint/journal until the new image and its reference have committed. A database/object-store adapter owns publication ordering, bounded journal retention and economic acknowledgement. Version 5 is selected only for logs containing a baseline; ordinary logs retain their version 1–4 bytes. Client wire and outer recovery-image versions are unchanged. See [canonical replay baselines](docs/adr/0011-canonical-replay-baselines.md).

For process-hosted matches, use `prepare_match_host_for_recovery` plus `serve_prepared_match_host_with_status_and_control_and_shutdown`. The demo server enables this path with `GAME_SERVER_MATCH_IDS` and `GAME_SERVER_RECOVERY_DIR`; `GAME_SERVER_RECOVERY_PATH` remains single-match only.

A hosted recovery directory is a versioned bundle containing an exact sorted match manifest and one existing `RecoveryImage` per `MatchId`:

```text
host.recovery/
  manifest
  alpha.recovery
  beta.recovery
```

The complete configured match set must match the manifest exactly. Startup reads and validates every per-match image off the async runtime and fails closed if any image, manifest entry, or directory entry is missing, extra, malformed, or incompatible. The validated bundle is consumed only after the WebTransport endpoint has bound successfully, so a bind/TLS failure does not discard restart evidence.

Dynamic applications use `prepare_live_match_host_for_recovery(fresh_ids, factory, max_matches, reconnect_grace_ticks, config)` and `serve_prepared_live_match_host_with_status_and_control_and_shutdown`. `PreparedLiveMatchHost::host()` supplies the management clone before serving. On a fresh start, the factory constructs the supplied initial IDs, which may be empty. On recovery, the bounded, sorted manifest supplies the complete ID set to the factory; fresh/default IDs are ignored so retired matches are not recreated. The consumer must reconstruct each ID's original rules/configuration. Replay and sessions are restored through the same verification path as static hosting. Empty bundles are valid and preserve empty membership; over-capacity or malformed manifests fail before the factory is called. Bundle consumption still happens only after endpoint binding succeeds.

On graceful hosted shutdown, new admissions drain first. After the grace window every runtime is frozen, all per-match recovery images are produced, and the complete bundle is staged in a sibling temporary directory before one directory rename publishes it. If image creation or persistence fails, all frozen runtimes are resumed while the host remains drained/unready; existing sessions, reconnects, and deterministic ticks can therefore continue while a later shutdown signal retries persistence, without admitting new nondurable match state.

The hosted restart acceptance proves independent reconnect tokens, connection epochs, command watermarks, and simulation continuity across two matches. It also corrupts only one match image and verifies that startup rejects the complete bundle without consuming the healthy match or silently replacing the failed match with fresh authoritative state.

See `ROADMAP.md` for the extraction plan and ownership boundaries.

## Performance evidence

The opt-in [benchmark suite](benchmarks/README.md) measures live commands, ticks, shared snapshot fan-out, replay encoding/verification, and recovery. Versioned before/after receipts retain raw samples and source/environment fingerprints. Ordinary tests remain independent of timing thresholds.
