# Server stream frames count as delivered once finished locally

Status: accepted

Requested by moritzbrantner/game-server#45.

## Problem

In wtransport 0.7.2, `SendStream::finish()` queues the FIN and then awaits `stopped()`. That resolves only once the peer has acknowledged every byte, or once the peer sends STOP_SENDING. The server awaited it in three places:

- **Welcome.** During a path outage right after the welcome, the server never entered the established connection loop. Datagrams, the command lane and control were not served. Once the outage outlasted the five-second handshake bound, the server rolled back a welcome the client already held, so the client's reconnect token was refused. A client that reads exactly `WELCOME_BYTES` and drops the stream before the FIN arrives sends STOP_SENDING. That also turned into a rollback, even without an outage. The repository's own clients read the welcome this way.
- **Command rejection.** Delivery runs inside the connection loop with a two-second bound. An outage that hid the client's ACK for two seconds closed the connection with "command rejection delivery failed", and the loop stalled until then.
- **Control response.** The exchange held its control-stream permit until the ACK and logged a failure for a response that had been written, or that the client had read and stopped.

None of these frames depends on the peer's ACK. The outcome is fixed before the frame is written.

## Decision

A server-written frame on a unidirectional stream, or on the response half of a control stream, counts as delivered once it is completely written and finished locally. `connection::finish_locally` calls quinn's synchronous `SendStream::finish` through wtransport's `quinn` feature, which only exposes the existing quinn stream and adds no dependency. QUIC keeps retransmitting the finished stream until the peer acknowledges it or the connection ends, also after the handle is dropped. Dropping a quinn stream finishes an unfinished stream and never resets a finished one.

- **Welcome.** The five-second bound covers opening the stream, writing the frame and the local finish. Rollback (`rollback_failed_welcome`) applies only to a welcome that was never completely written. A partly written welcome cannot be decoded, so the client holds no token from it. This keeps ADR-0001's rule that rollback never invalidates a token the client may know. The handshake's select is biased toward the completed write, so a shutdown or connection close that becomes ready in the same poll does not roll back a written welcome. After a completely written welcome, the session is established. A later outage is handled by the connection like any other: the session stays usable once the path returns, or the connection ends (for example on the QUIC idle timeout) and the token from the received welcome resumes the same player within grace. For a reconnect welcome, the latest written token is the live one.
- **Command rejection.** The two-second bound covers opening and writing the kind-5 frame. A client that never accepts streams still exhausts its stream limit, so ADR-0005's protection against a non-reading peer holds.
- **Control response.** The control deadline covers reading, dispatch, writing and the local finish.
- **Command lane.** Acks are written with stream flow control and never waited on. On a client FIN, the lane calls `AsyncWrite::shutdown`, which for a WebTransport stream is quinn's synchronous finish. No change.

Client-side code (`control-client`, `netem-client` and test harnesses) may still await `finish()` where it intentionally probes the peer's stream state. That is not server admission.

## Consequences

The server no longer learns whether the client acknowledged a welcome, rejection or control response. Liveness after the frame comes from the connection: QUIC keep-alive and the idle timeout end a dead connection, and the session then enters the normal reconnect grace window. No wire, replay or recovery format changes.

Regression coverage: `welcome_handshake_acceptance` (short and long outages after new and reconnect welcomes, both welcome read modes, an outage before the welcome) and `command_rejection_does_not_wait_for_the_peer_ack` in `command_lane_acceptance`.
