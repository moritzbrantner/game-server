# Empty-match suspension and cold hosted admission

Status: accepted for the opt-in reusable boundary; MMORPG owns zone policy and storage.

## Decision

`LiveMatchHost::suspend_empty` checks occupied slots under the same runtime mutex used by admission, including disconnected players still within reconnect grace. A nonempty match is refused. A healthy empty match captures its canonical recovery image and freezes admission and ticks atomically. Replay capture and the simulation's existing canonical restore capability are required for a useful cold recovery path.

An owned task then asks the supplied asynchronous callback to confirm that the immutable image is recoverable, with an explicit deadline and no runtime mutex held. Consumers own storage, fencing epochs, journal/checkpoint publication and verification. Only a successful confirmation releases the runtime and host capacity. A failed, timed-out or panicking confirmation retains the frozen resident writer and captured image; a later `suspend_empty` retry uses that image. The operation continues if its caller is canceled. No speculative unfreeze or new writer resolves an uncertain storage outcome. Explicit retirement remains available to the trusted lifecycle owner; it is not proof of durable suspension.

A per-match lifecycle lock serializes suspension and explicit retirement. Process draining fences management changes through the existing host gate. No format or gameplay protocol changes are introduced.

`with_cold_admission` configures an optional `ColdMatchResolver` before sharing/serving the host. The WebTransport path first checks the configured Origin and parses the canonical hosted route. Resident matches bypass the resolver. Missing matches may invoke the resolver under one serial cold-resolution lock, with a deadline covering queueing and callback execution. The callback restores only a known authorized match from durable evidence and places it using existing host capacity/fencing rules. A second waiter rechecks residence instead of creating another runtime. Unknown, failed or timed-out resolution refuses admission; the host never creates a fresh fallback match. Existing session admission still owns token, player and connection fencing.

## Consumer boundary

The game supplies its empty grace policy, lifecycle observations, safe durable checkpoint/journal reference and cold-restoration adapter. Default suspension freezes simulation time; any bounded offline progression is a separately declared deterministic game transition using explicit input. This runtime hook neither reads a gameplay clock nor supplies an offline scheduler. Optional prewarming may call the same consumer restoration path; it is not a correctness dependency. Persistence-enabled placement still requires an established initial durable recovery basis.

## Evidence and limits

Focused tests exercise occupied/reconnectable refusal, failed confirmation retaining the frozen image, retry and exact cold snapshot restoration, caller cancellation, serialized cold resolution, unknown-match refusal and real WebTransport welcome/admission of two clients into one cold-restored runtime. Database/object-store integration and a game's explicit resume-input contract are consumer responsibilities, not claims of this change.
