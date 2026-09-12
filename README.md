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

## Graceful recovery

Set `GAME_SERVER_RECOVERY_PATH` to enable replay-backed graceful restart recovery. On SIGTERM or Ctrl-C the demo host drains, freezes authoritative mutation, writes a bounded recovery image atomically, and then closes established sessions. On the next successful endpoint startup the image is verified, authoritative state and command watermarks are reconstructed, and saved sessions become disconnected/reconnectable before the consumed image is removed.

Recovery persistence is intentionally fail-closed: malformed evidence prevents startup, failed shutdown persistence resumes the live runtime, recovery I/O does not hold the runtime mutex, and the reliable welcome handshake is time-bounded so a peer cannot retain capacity indefinitely. This is graceful restart recovery rather than per-command crash journaling.

See `ROADMAP.md` for the extraction plan and remaining process-hosting work.
