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

## Browser integration contract

`browser` exposes the versioned browser-facing boundary without creating a second gameplay protocol. `BROWSER_PROTOCOL_CONTRACT` binds the route version to the existing command/snapshot protocol version, reliable-control format version, reconnect-token size, and payload ceilings so browser clients can pin one explicit compatibility surface.

Use `BrowserRoutePrefix` plus a validated `MatchId` to address a match. With a `/game` base path and match ID `uno_01`, the canonical paths are:

```text
/game/matches/uno_01
/game/matches/uno_01/reconnect/<32-hex-character-token>
```

`BrowserRoutePrefix::parse` fails closed for malformed owned routes and returns no match for unrelated paths. Match IDs keep the existing URL-safe 64-byte bound, and reconnect tokens continue to use the same rotating capability already enforced by the session runtime.

The demo server opts into this addressing when `GAME_SERVER_MATCH_ID` is set. `GAME_SERVER_SESSION_PATH` then means the browser route base path rather than the full session path. If `GAME_SERVER_MATCH_ID` is absent, the old exact-session-path behavior remains available for existing experiments.

This slice gives browser games a stable explicit match URL without moving game rules or authority into routing. A later process-hosting slice still needs to route one listener across multiple `MatchHost` entries rather than binding one server process invocation to one runtime.

## Reliable control

Realtime game commands and latest authoritative snapshots use WebTransport datagrams. Transactional/session control uses an independent versioned request/response frame over bidirectional WebTransport streams. Each payload is bounded to 4 KiB, each exchange is time-bounded to five seconds, and each connection can have at most four control exchanges in flight.

Applications opt in with `serve_with_control` or `serve_with_control_and_shutdown` and provide a `ControlService`. Each request receives a `ControlContext` containing the authenticated player ID and current connection epoch, so services that perform external side effects can fence delayed work after a reconnect. The service has no access to `GameSimulation`; authoritative game mutation therefore remains on the deterministic command/tick path. The default `serve` and `serve_with_shutdown` entry points reject control requests.

Control handlers run off the async runtime. A server instance admits at most 64 handler executions at once, and that permit remains occupied until the synchronous handler actually exits even if its WebTransport exchange has already timed out. This prevents repeated transport timeouts from creating an unbounded tail of detached blocking work.

The real-network acceptance probe covers successful and rejected control exchanges, malformed, oversized, and trailing-byte fail-closed handling, stalled-stream timeout, the per-connection concurrency cap, and continued realtime command/snapshot progress while a control stream is stalled.

## Multi-match host

`MatchHost` owns a bounded set of homogeneous `MatchRuntime` instances inside one process. Match IDs are URL-safe ASCII identifiers capped at 64 bytes, iteration is deterministic, and placement fails closed on duplicate IDs, process drain, or configured match capacity. Failed placement returns a `PlacementFailure` containing the original ID and runtime intact, so authoritative state is never discarded merely because placement must be retried elsewhere. Existing per-match player capacity remains owned by each simulation/runtime rather than being duplicated in the host.

Draining is explicit at both match and process level. Removing a match requires it to be draining and to have no active or reconnectable player slots; the removed runtime is returned to the caller rather than silently discarded. Mutable runtime operations also reassert any pre-existing match/process drain before returning. `HostStatus` and per-match status expose capacity and lifecycle facts and derive readiness from those facts instead of storing a second mutable ready flag. Network routing across hosted matches and externally served health/readiness endpoints remain the next process-hosting slice.

## Graceful recovery

Set `GAME_SERVER_RECOVERY_PATH` to enable replay-backed graceful restart recovery. On SIGTERM or Ctrl-C the demo host drains, freezes authoritative mutation, writes a bounded recovery image atomically, and then closes established sessions. On the next successful endpoint startup the image is verified, authoritative state and command watermarks are reconstructed, and saved sessions become disconnected/reconnectable before the consumed image is removed.

Recovery persistence is intentionally fail-closed: malformed evidence prevents startup, failed shutdown persistence resumes the live runtime, recovery I/O does not hold the runtime mutex, and the reliable welcome handshake is time-bounded so a peer cannot retain capacity indefinitely. This is graceful restart recovery rather than per-command crash journaling.

See `ROADMAP.md` for the extraction plan and remaining process-hosting work.
