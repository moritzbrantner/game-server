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

## Reliable control

Realtime game commands and latest authoritative snapshots use WebTransport datagrams. Transactional/session control uses an independent versioned request/response frame over bidirectional WebTransport streams. Each payload is bounded to 4 KiB, each exchange is time-bounded to five seconds, and each connection can have at most four control exchanges in flight.

Applications opt in with `serve_with_control` or `serve_with_control_and_shutdown` and provide a `ControlService`. Each request receives a `ControlContext` containing the authenticated player ID and current connection epoch, so services that perform external side effects can fence delayed work after a reconnect. The service has no access to `GameSimulation`; authoritative game mutation therefore remains on the deterministic command/tick path. The default `serve` and `serve_with_shutdown` entry points reject control requests.

Control handlers run off the async runtime. A server instance admits at most 64 handler executions at once, and that permit remains occupied until the synchronous handler actually exits even if its WebTransport exchange has already timed out. This prevents repeated transport timeouts from creating an unbounded tail of detached blocking work.

The real-network acceptance probe covers successful and rejected control exchanges, malformed and oversized fail-closed handling, stalled-stream timeout, the per-connection concurrency cap, and continued realtime command/snapshot progress while a control stream is stalled.

## Graceful recovery

Set `GAME_SERVER_RECOVERY_PATH` to enable replay-backed graceful restart recovery. On SIGTERM or Ctrl-C the demo host drains, freezes authoritative mutation, writes a bounded recovery image atomically, and then closes established sessions. On the next successful endpoint startup the image is verified, authoritative state and command watermarks are reconstructed, and saved sessions become disconnected/reconnectable before the consumed image is removed.

Recovery persistence is intentionally fail-closed: malformed evidence prevents startup, failed shutdown persistence resumes the live runtime, recovery I/O does not hold the runtime mutex, and the reliable welcome handshake is time-bounded so a peer cannot retain capacity indefinitely. This is graceful restart recovery rather than per-command crash journaling.

See `ROADMAP.md` for the extraction plan and remaining process-hosting work.
