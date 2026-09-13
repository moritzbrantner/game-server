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

The single-match demo mode opts into this addressing when `GAME_SERVER_MATCH_ID` is set. `GAME_SERVER_SESSION_PATH` then means the browser route base path rather than the full session path. If `GAME_SERVER_MATCH_ID` is absent, the old exact-session-path behavior remains available for existing experiments.

For process-hosted matches, `serve_match_host*` owns one WebTransport listener and dispatches each canonical match/reconnect route to the matching `MatchHost` runtime. Unknown or malformed routes fail closed before admission. `GAME_SERVER_MATCH_IDS=alpha,beta` enables that mode in the demo server and serves `/game/matches/alpha` and `/game/matches/beta` from the same listener.

## Reliable control

Realtime game commands and latest authoritative snapshots use WebTransport datagrams. Transactional/session control uses an independent versioned request/response frame over bidirectional WebTransport streams. Each payload is bounded to 4 KiB, each exchange is time-bounded to five seconds, and each connection can have at most four control exchanges in flight.

Single-match applications opt in with `serve_with_control` or `serve_with_control_and_shutdown` and provide a `ControlService`. Hosted applications use `serve_match_host_with_control*` and provide a `MatchControlService`, which receives the validated `MatchId` in addition to the authenticated player ID and connection epoch. That preserves fencing identity even when different matches both allocate player ID 1. Neither control service has access to `GameSimulation`; authoritative game mutation therefore remains on the deterministic command/tick path.

Control handlers run off the async runtime. A server instance admits at most 64 handler executions at once, and that permit remains occupied until the synchronous handler actually exits even if its WebTransport exchange has already timed out. This prevents repeated transport timeouts from creating an unbounded tail of detached blocking work.

The acceptance probes cover successful and rejected control exchanges, malformed, oversized, and trailing-byte fail-closed handling, stalled-stream timeout, the per-connection concurrency cap, continued realtime command/snapshot progress while a control stream is stalled, and match-scoped control routing through one hosted listener.

## Multi-match host

`MatchHost` owns a bounded set of homogeneous `MatchRuntime` instances inside one process. Match IDs are URL-safe ASCII identifiers capped at 64 bytes, iteration is deterministic, and placement fails closed on duplicate IDs, process drain, or configured match capacity. Failed placement returns a `PlacementFailure` containing the original ID and runtime intact, so authoritative state is never discarded merely because placement must be retried elsewhere. Existing per-match player capacity remains owned by each simulation/runtime rather than being duplicated in the host.

`serve_match_host*` now routes admission, reconnect, commands, authoritative snapshots, and reliable control to the addressed hosted runtime. Each match has an independent snapshot channel and tick loop; player/session numbering and command watermarks stay isolated by `MatchId`. The real-network acceptance starts two matches on one listener, proves both independently allocate player ID 1, verifies different command sequences converge only in their addressed match, checks match-scoped control identity, and rejects an unknown match route.

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

Health is process liveness and remains `200` during an intentional drain. Readiness means the hosted process or addressed runtime is available to serve gameplay rather than whether another match can be placed; a host already at its configured match count can therefore still be ready. Process and match readiness return `503` once draining begins, while `/status` reports the drain flag and immutable host/match capacity facts. Unknown match routes and mutating methods fail closed. The status surface deliberately does not duplicate game rules, transport admission, or mutable gameplay counters.

## Graceful recovery

Set `GAME_SERVER_RECOVERY_PATH` to enable replay-backed graceful restart recovery for the single-match transport. On SIGTERM or Ctrl-C the runtime drains, freezes authoritative mutation, writes a bounded recovery image atomically, and then closes established sessions. On the next successful endpoint startup the image is verified, authoritative state and command watermarks are reconstructed, and saved sessions become disconnected/reconnectable before the consumed image is removed.

Recovery persistence is intentionally fail-closed: malformed evidence prevents startup, failed shutdown persistence resumes the live runtime, recovery I/O does not hold the runtime mutex, and the reliable welcome handshake is time-bounded so a peer cannot retain capacity indefinitely. This is graceful restart recovery rather than per-command crash journaling.

Hosted transport intentionally rejects `GAME_SERVER_RECOVERY_PATH` for now rather than pretending one recovery file can represent multiple independent matches. Per-match hosted recovery remains the next process-hosting boundary.

See `ROADMAP.md` for the extraction plan and remaining process-hosting work.
