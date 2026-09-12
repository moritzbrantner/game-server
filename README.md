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

See `ROADMAP.md` for the extraction plan.
