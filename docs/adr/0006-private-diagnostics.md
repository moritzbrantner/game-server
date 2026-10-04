# Private runtime diagnostics

Accepted for the Scrabble private-state audit (moritzbrantner/scrabble#27).

Reconnect capabilities and opaque consumer payloads are private values even when they sit in a public native type. Derived `Debug` exposed them directly or through nested session/recovery records. Redact the token in `ReconnectToken` and `Welcome`; report only metadata and byte lengths for command/rejection/snapshot/control frames, fragments, simulation snapshots, and replay records. Nested `Debug` implementations inherit those boundaries.

This is a diagnostic change. Equality, explicit field access, wire encoding, replay hashes and bytes, and recovery persistence remain unchanged. The native layer cannot sanitize arbitrary application-authored error messages or deliberately printed raw fields. Consumers must provide safe error text and treat explicit payload/token encoders and replay/recovery artifacts as private interfaces. No protocol version changes.

Tests use private sentinels across the direct and nested types and a real runtime recovery image, proving redaction and unchanged recovery encoding/decoding. Full all-feature native tests and Clippy remain the gate.

Shared convention sourceRevision: `46d8793bb3034326561f876dcc67dbaa5aa1e432`.
