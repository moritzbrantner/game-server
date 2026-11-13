# Native WebRTC session and admission mapping

Status: accepted implementation choice for #86; opt-in under ADR 0014.

## Decision

Use the external `webrtc` 0.21 provider for native peers. Each connection locally
creates three negotiated DataChannels: 0 is unordered with zero retransmissions
for existing intent/snapshot datagrams; 1 is ordered and reliable for admission,
welcome, private command rejections and one-at-a-time `GSCT` control exchanges;
2 is ordered and reliable for the existing `GSCL` command header, command frames
and acknowledgement frames. Reject additional in-band channels. Snapshot messages
use the existing fragmentation/reassembly contract with a 1,200-byte budget.

Native hosts create and heartbeat an existing setup-service lobby. Renew only
within that lobby's existing lifetime cap; expiry or loss of host signaling stops
serving. No separate host registry, account service or public gameplay relay is
introduced. Native unattended lifetime extensions remain setup-service work.

The supplied game authorizes the authenticated signaling participant before peer
creation and again immediately before runtime admission. The host sends a random,
single-use 256-bit ticket to that participant over targeted WSS, bound to its
configured match, lobby and negotiated DTLS connection. The ticket exchange has a
20-second deadline. The session channel starts with the host's `GSAD` plus version
byte `1`; the guest waits for it, then sends the ticket and, for reconnect, the
existing 16-byte rotating reconnect token. Only then may `MatchRuntime` admit or
resume a player and send the unchanged welcome.

The server-first readiness marker gives the initial ticket a causal predecessor
after the native handler opens. The original live Chromium probe could drain its
send buffer while the native provider reported zero received channel messages;
waiting for the readiness marker resolved that admission/reconnect failure.
There is no retry of an unacknowledged admission or arbitrary startup delay.

## Consequences

The existing runtime, tick loop, snapshot projection/fencing, command-lane
interpreter, control dispatch and replay/recovery formats remain authoritative.
Reliable control has one outstanding exchange per session channel because its
existing wire frame has no request ID. A native control timeout closes the peer,
so a delayed response cannot be attributed to a subsequent operation.

Provider send queues are bounded; snapshot and held-input sends do not await
reliable-channel capacity. Connection and handler task sets retain their existing
cancellation ownership. Provider reactors stop when their final owner drops.
Invites expose only public match/lobby IDs; capabilities, SDP, tickets, reconnect
tokens and TURN credentials never enter diagnostics. Games retain guest policy,
durable storage and invitations. Bulk assets use their independently bounded path.
