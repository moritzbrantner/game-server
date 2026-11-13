# Home-hosted WebRTC transport alongside dedicated WebTransport

Status: accepted architectural direction (owner choice A, 2026-11-10); opt-in implementation and bounded proof delivered in #86.

Implementation update (#86): the opt-in native/browser adapter and bounded direct,
TURN and restricted-network proof are described in [WebRTC](../WEBRTC.md) and
[the channel/admission mapping](0015-webrtc-session-channels.md). Dedicated
WebTransport remains the default; MMORPG consumer stages remain separately owned.

## Decision

Add an **optional WebRTC DataChannel transport adapter** to the existing `game-server` authoritative session/match runtime for games hosted on a player's own computer. Keep the existing WebTransport/HTTP/3 adapter for dedicated, publicly reachable servers; do not replace its proven path.

The `multiplayer-setup-service` remains the short-lived rendezvous/control plane for browser/native peers: lobby admission, bounded SDP/ICE signaling, and authenticated short-lived TURN credentials. Connection establishment prefers direct ICE candidates and falls back to TURN when direct connectivity fails. A custom public WebTransport reverse relay is **not** the selected home-hosting foundation.

The adapter must reuse the same game-server command sequencing, tick scheduling, player-scoped snapshots, match identity, admission/reconnect fencing, reliable command semantics, replay and recovery. Transport-specific framing and backpressure may differ, but they must not create a second game protocol or gameplay authority. A plausible mapping is an unordered, limited-retransmission DataChannel for latest-state/intent traffic and ordered reliable DataChannels for one-shot/control traffic; validate the actual ordering, size and recovery behavior before fixing that mapping in a public API.

## Authority and security boundaries

- `game-server` owns transport admission, session identity, command delivery, authoritative publication and reconnect; a supplied game simulation owns rules and game state. Game-owned storage owns durable worlds and characters.
- `multiplayer-setup-service` owns ephemeral lobby membership, invitation admission, signaling and TURN issuance. It does not relay ordinary game messages, run the simulation, store durable characters or carry bulk asset bytes.
- A public invite or lobby ID is never a reusable gameplay/participant credential. Admission to the authoritative server requires a separate game/host-scoped authorization, including freshness and revocation checks appropriate to the owning game.
- Participant capability tokens, reconnect tokens and TURN credentials never appear in invitations, QR codes, logs or diagnostics. Setup participant capabilities retain their existing in-memory-only rules.
- Optional asset distribution uses the existing independently bounded, manifest-verified peer-content path. A bulk content connection must never starve gameplay.
- Guest identity and persistent world saves are game-owned. For MMORPG, the previously approved product policy is that a self-hosted world is playable only while its host runs, saved progress survives restart, and characters do not automatically transfer between different hosted worlds.

## First bounded implementation proof

1. Drive one existing deterministic test simulation from a native Rust host over WebRTC without changing its game rules or authoritative runtime.
2. Connect real browser clients through an actual setup-service lobby and its existing signaling/short-lived TURN interface. Establish a direct candidate path and separately force a working TURN-relayed path across different networks, including restrictive NAT.
3. Exercise welcome/admission, latest-state/intent delivery, ordered one-shot commands, shared and player-scoped snapshots, disconnect/reconnect with a fenced connection epoch, and shutdown. Compare the same semantic command trace against the existing WebTransport adapter.
4. Record setup time, RTT, jitter, packet loss, reconnect behavior, relay bytes, per-match memory and representative authoritative tick cost. A passing local signaling demo alone is not internet-hosting acceptance.
5. Keep the new adapter opt-in until its actual browser/native acceptance works. Existing dedicated WebTransport consumers retain their behavior and protocol compatibility.

## Separate follow-up decisions

The native host's unattended registration/heartbeat lifecycle, precise authoritative admission-ticket exchange, relay budgets, player-facing launch workflow, and content-serving integration remain separately owned implementation choices. Do not infer them from this transport decision or create a second lobby/account/game-state service.

Related boundaries: [shared connection lifecycle](0001-shared-connection-lifecycle.md), [reliable command lane](0008-reliable-command-lane.md), [durable hosted publication](0012-durable-hosted-publication.md), [empty-match suspension](0013-empty-match-suspension.md), and [peer content distribution](https://github.com/moritzbrantner/multiplayer-setup-service/blob/main/docs/peer-content-distribution.md).
