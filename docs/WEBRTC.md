# Opt-in WebRTC transport

Enable `webrtc-transport` to use `webrtc_transport::{SetupConfig, Lobby,
WebRtcHost, WebRtcHostConfig, WebRtcClient}`. It remains opt-in alongside the
dedicated WebTransport transport. [ADR 0014](adr/0014-home-hosted-webrtc-transport.md)
defines its ownership; [ADR 0015](adr/0015-webrtc-session-channels.md) defines the
native/browser channel and admission contract.

Create `SetupConfig` with the deployment's HTTPS origin and an allowed browser
origin. HTTP is accepted only for loopback. `Lobby::create` registers the native
host through `POST /lobbies`; guests use `Lobby::join`. Both retain capabilities
only in memory and obtain TURN credentials through the authenticated existing
endpoint. Use an actual local interface address in the WebRTC bind configuration.

Construct `WebRtcHost` with its validated `MatchId`, the game's existing
`Arc<Mutex<MatchRuntime<S>>>`, control service and bind configuration. `run` requires
a cheap, current game-authorization callback for the authenticated participant;
membership or possession of a public invite must not stand in for that policy.
The callback is checked before issuing a ticket and before admitting a player.
The probe intentionally permits its ephemeral test lobby's participants.

### Managed host attachment

For `PreparedLiveMatchHost` consumers, retain `prepared.host()` and start the
existing managed serving/recovery future. For an owner-only local listener, set
`MatchHostWebTransportConfig::bind_ip` to
`Some(std::net::Ipv4Addr::LOCALHOST.into())` before starting that future. This
keeps native QUIC admission on loopback while WebRTC uses game approval; an
Origin allowlist alone does not restrict native QUIC peers. Dedicated servers
retain their dual-stack wildcard bind with `bind_ip: None`.

Once `host.is_serving()` is true, use
`WebRtcHost::attach(lobby, host.clone(), match_id, control, config)` and keep its
`run_with_authorization` future alive beside that owner. `attach` rejects an
unstarted host or unknown match. It retains the existing private runtime and
snapshot source; it never starts another tick loop or claims host ownership.
Committed publications, player projections, command sequencing and recovery
therefore pass through the original managed lifecycle.

The attachment's `control` implements `WebRtcControlService::handle(participant_id,
match_id, context, payload)`. The participant comes from authenticated signaling;
`context` contains the runtime-assigned player and connection epoch and retains
the ordinary control dispatch fencing. Games bind that participant to their own
approved character/entitlement before protected operations and recheck revocation
when performing delayed external side effects. Do not expose a trusted LAN
arbitrary-character claim handler as public guest authorization.

Guests can use `WebRtcClient::connect_with_authorization(lobby, match_id, bind_ip,
relay_only, proof)` with at most `MAX_AUTHORIZATION_BYTES` (1,024) opaque bytes.
The optional offer field `authorization` carries this game-owned proof; missing
means empty for existing clients. Proofs stay in memory, including same-participant
reconnect, and must not enter diagnostics or invitations. Proof interpretation,
identity and durable entitlement storage belong to the game.

`run_with_authorization(authorize, revalidate, shutdown)` takes an asynchronous
`authorize(participant_id: String, proof: Vec<u8>) -> bool` and a cheap synchronous
`revalidate(&str) -> bool`. Up to 32 approvals can wait, one per participant, each
bounded to 60 seconds while signaling and heartbeat continue. This permits host
approval before a ticket is issued. Revalidation runs before ticket creation,
again at runtime admission, and during an established peer's lifetime (idle bound
100 ms). Revocation closes the peer and disconnects its current lease. Clients
bound the entire connection/reconnection attempt to 100 seconds.

Stopping the attachment closes only its WebRTC peers and leaves the managed host
serving. Managed drain, match retirement or owner shutdown ends the attachment;
the original owner retains freeze/save/recovery responsibilities. Dedicated
WebTransport connections keep their original control service and lifecycle. The
standalone `WebRtcHost::new` / synchronous `run` path remains available and owns
its tick loop, drain and freeze as before. Gameplay/control wire layouts are
unchanged; the authorization field extends only the setup signaling offer.

The host heartbeats every 20 seconds and renews near expiry only when its existing
setup lifetime cap allows it. Signaling expiry/disconnect ends hosting; ordinary
shutdown drains, releases connections and freezes the runtime for the owning game
to save through its existing recovery/storage path. Game applications own durable
saves, registration lifetime policy, launch UI and invitations. Games retain native launcher and missing-asset cache/import behavior.

`WebRtcClient` uses the same welcome, intent, snapshot and reliable-command codecs
as WebTransport. Retain the welcome token in memory; `reconnect` preserves player
identity and reliable-lane watermarks while rotating the token/epoch.
`disconnect(&self)` closes transport while retaining those reconnect capabilities;
`close(self)` consumes them. Control is
serialized on its reliable channel; after a control timeout reconnect before
issuing another exchange. Private command rejections can be read through
`command_rejection` or drained with `pop_command_rejection` after control reads.
`metrics` returns selected-path counters without addresses or credentials.

## Optional peer content

A host opts in with `.with_content_service(Arc<dyn WebRtcContentService>)`.
The asynchronous `handle(participant_id, match_id, ControlContext, request)` receives
opaque game-owned bytes only after admission. The participant, match, player and
connection epoch are the same identities as scoped control. Games own content
entitlements, manifests, immutable hashes, file lookup and verified cache/import.
Use asynchronous I/O and cancellation-safe futures; detached work and unbounded
blocking jobs are outside the service contract.

Native guests opt in with `WebRtcClient::connect_with_content_authorization`,
using the same arguments as `connect_with_authorization`. Offers and answers must
both advertise `contentVersion: 1`; missing/unknown capabilities leave channel 3
absent. Existing native/browser peers and dedicated listeners retain their lanes.
`client.content_client()` returns `None` without mutual support, otherwise an
independent cloneable handle whose `request(&bytes).await` exchanges one chunk.
Keep gameplay receives and sends running while that handle awaits content.
Reconnect negotiates again; old handles never migrate to the replacement epoch.

Requests and responses each contain at most `MAX_CONTENT_BYTES` (16 KiB).
There is one outstanding exchange across handle clones, one handler per peer and
four active handlers per host. Busy callers fail without queuing work; host handler
saturation returns a content refusal while keeping gameplay connected. Each chunk
waits at least 10 ms before lookup; exchanges and handlers have five-second bounds.
The existing provider limits each channel's send buffer to 64 KiB and bounds its
receive event queues. Responses use nonblocking enqueue so content cannot wait on
the authoritative loop; small paced chunks bound shared SCTP contention while
snapshots, intents and control continue on their original channels. This is not a
bandwidth reservation under arbitrary network congestion.

Current authorization, runtime drain and session fencing are checked before lookup
and again under the runtime lock at nonblocking response publication. File/network
I/O never holds that lock. Revocation, disconnect, retirement and shutdown close
the peer and cancel its content tasks. The game must also recheck mutable content
entitlements before delayed protected side effects. A game-owned service refusal returns `WebRtcContentError::Refused` and preserves
the peer and lane. A timeout or transport/protocol failure closes the peer. Canceling a request future disables its content handle; reconnect
before more content requests so delayed bytes cannot become a later response.
Setup signaling and the website never carry content bytes.

## Channel contract

All channels are negotiated out of band on both endpoints.

| ID | Label | Delivery | Frames |
| --- | --- | --- | --- |
| 0 | `game-server.realtime.v1` | unordered, `maxRetransmits: 0` | Existing commands and whole/fragmented snapshots |
| 1 | `game-server.session.v1` | ordered, reliable | `GSAD` + byte `1`, then 32-byte ticket (+ 16-byte reconnect token), existing welcome/rejections/GSCT |
| 2 | `game-server.commands.v1` | ordered, reliable | Existing GSCL header, commands and acknowledgements |
| 3 (optional) | `game-server.content.v1` | ordered, reliable | Opaque request (≤16 KiB); response status byte `1` + opaque bytes (≤16 KiB), or byte `0` for refusal |

No canonical player-scoped snapshot enters broadcast delivery. Snapshot
authorization, projection and provider enqueue use the same runtime lock.
Committed player projections, when supplied, retain their existing semantics.

## Explicit real-network probes

These probes deliberately contact the existing `https://moenarch.com` setup
deployment and its configured `turn.moenarch.com` relay. They are not part of the
hermetic Cargo gate. No tokens, credentials or SDP are printed.

```sh
cargo build --examples --features webrtc-transport --locked
target/debug/examples/webrtc-proof host <native-interface-ip> [scoped]
target/debug/examples/webrtc-proof client <guest-interface-ip> <public-lobby-id> [relay]
python3 examples/webrtc-proof-browser.py <public-lobby-id> [--scoped] [--relay]
target/debug/examples/webrtc-proof-reference <player-id>
./examples/webrtc-restricted.sh <public-lobby-id> <turn-ipv4> [relay] [loss-percent]
```

The browser script needs Playwright and its Chromium installation and runs from
the allowed deployment origin. Run the host and guest on different real networks.
`relay` explicitly forces relay-only ICE. The restricted script additionally runs
the native guest in a disposable Docker network namespace with an OUTPUT-drop
firewall allowing only DNS, HTTPS/WSS and UDP to the supplied TURN server's port
3478. Omitting `relay` tests normal direct-first ICE falling back under that actual
restriction. NET_ADMIN applies only to that container; host/VPS firewalls and
shared services are unchanged. It prints the actual packet counters afterward.
An optional nonzero loss percentage applies kernel netem only to outbound UDP
traffic to that TURN endpoint and prints the actual sent/drop counters. The
restricted recipe requires a Linux binary and Docker; the recorded platform is
Linux x86_64.

The deterministic demo uses zero held input to keep its semantic payload
independent of wall-clock tick alignment. Every probe checks welcome, intent,
ordered Applied/Duplicate acknowledgements, snapshots and fenced reconnect.
The scoped host also admits an independent first session: the addressed guest
must receive only its own player. The WebTransport reference executes the same
trace through the existing hosted listener and verifies replay. Compare its
`semanticPayload` (or `sharedSemanticPayload` in shared mode) with the network
probe's bytes, with matching player IDs. Ctrl-C on the host verifies replay and
reports a single-match process RSS plus observed authoritative tick timings.

## Recorded bounded proof

The implementation PR linked from [#86](https://github.com/moritzbrantner/game-server/issues/86)
records the checked revision and completed gates. The following
observations are specific to the deterministic demo, not MMORPG workload claims.

| Topology/path | Setup | Reconnect | Observations |
| --- | --- | --- | --- |
| Home Chromium 156 → VPS native, direct | 1,470.3 ms | 643.0 ms | selected `srflx`/`host`; ICE RTT 10 ms; 20 snapshot ticks, zero gaps; scoped player 2 only |
| Home native host → VPS native guest | 1,305.3 ms | 1,092.7 ms | scoped player 2 only; ICE RTT 8.56 ms; ordered commands, observed intent and reconnect |
| Home restricted native → VPS native, forced TURN | 4,829.9 ms | 4,689.4 ms | selected relay; ICE RTT 10.13 ms; selected-pair bytes sent 2,950 / received 3,858 |
| Same restriction, normal direct-first ICE | 4,686.5 ms | 4,537.7 ms | `forcedRelay=false`, selected relay; ICE RTT 10.66 ms; 106 firewall-dropped packets / 14,197 bytes |
| Home Chromium → VPS native, forced TURN/shared | 646.3 ms | 431.8 ms | selected `relay`/`host`; ICE RTT 10 ms; 20 snapshot ticks, zero gaps; shared bytes match WebTransport |
| Restricted native, forced TURN + configured 5% UDP loss | 5,955.7 ms | 4,300.9 ms | actual queue: 162 sent + 13 dropped, 7.43% observed one-way loss; trace/reconnect/control passed |

All rows completed ordered commands, duplicate suppression, control and reconnect
with epoch rotation. Scoped payloads for players 2, 3 and 4 matched the corresponding
real WebTransport reference traces byte for byte. The latest single-match native
run used 20,408 KiB process RSS. Its 1,199 simulation steps averaged 2,171 ns,
maximum 18,816 ns; the separate two-player neutral workload, with replay capture,
averaged 2,548 ns per `MatchRuntime::advance_tick` over 1,000 ticks. These are process
RSS and unoptimized informational timings, not retained heap or a production budget.
The controlled-loss run's control RTT samples included 1,251 ms; its mean absolute
RTT delta was 267.95 ms, versus 0.48 ms in the browser forced-TURN run. Selected-pair
bytes in the controlled-loss run were 3,811 sent / 6,703 received.

Snapshot tick gaps, provider discard counters, firewall drops and kernel queue
loss are distinct observations. The netem counters measure controlled outbound
TURN UDP loss; they do not establish natural Internet loss or billed TURN bandwidth.
