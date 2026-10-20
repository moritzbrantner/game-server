# Trusted server inputs in the replay stream

Status: accepted for game-server #54, enabling MMORPG #30.

Durable character admission requires storage I/O outside the tick followed by a
deterministic state transition. A client command cannot safely represent trusted
storage data, and rereading storage during replay would change historical inputs.

Add a default-rejecting simulation callback and a runtime operation that validates
player membership and payload size, applies the input, then records accepted bytes
at the current tick. Commands and host inputs share lock/record order; client lane
watermarks remain independent. Simulation rejection must be atomic, as for commands.
Games own host-input schemas, durable fencing and idempotency; the runtime owns
membership, ordering and replay. No client wire message changes.

The live-host management capability checks the process gate, retirement and current
connection epoch under the runtime lock before submission. Delayed control handlers
must pass their original epoch so a replaced connection fails closed. Direct runtime
callers are trusted and responsible for any external lease fencing.

Replay version 4 adds kind 7 (player ID, payload length, bytes). Existing logs retain
their oldest compatible encoding. Verification requires an admitted, unremoved
player and invokes the same callback; recovery uses the same interpreter. Existing
recovery envelopes embed this versioned log without an outer format change.
