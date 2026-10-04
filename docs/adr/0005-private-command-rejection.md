# Private recoverable command rejection

A simulation can reject a validly authenticated gameplay attempt through `SimulationError::command_rejected`. The immutable opaque payload is bounded to 1,024 bytes; Debug/Display omit it. Ordinary simulation errors and protocol/session errors retain their connection-closing behavior. Simulations must leave canonical state unchanged when returning any error.

The runtime still returns an error and consumes neither command sequence nor replay admission. The connection sends only a protocol-v3 kind-5 reliable unidirectional frame to that requesting connection: version and kind bytes, big-endian u32 sequence, big-endian u16 payload length, and payload. This is a rejection, never success acknowledgement or canonical state. The browser contract exposes the payload bound; snapshots, welcome, commands and control retain their existing formats.

Delivery holds no runtime lock, permits one in-flight rejection per connection, and ends on shutdown, connection close or a two-second timeout. A non-reading peer cannot retain delivery work indefinitely. Payload interpretation belongs to the consuming game; the transport never broadcasts or logs it. Reliable-control responses are separate.

Real WebTransport coverage verifies recipient isolation, unchanged canonical state/replay, retry with the same rejected sequence, continued valid commands, and ordinary fatal-error closure. Codec and diagnostic tests verify bounds, sequence correlation, distinct frame kinds and payload privacy.

Shared convention sourceRevision: `46d8793bb3034326561f876dcc67dbaa5aa1e432`.
