# Testing conventions

## TEST-001 — Test location follows dependency scope

- Place a test at the lowest source-tree directory containing all production code it covers.

## TEST-003 — Keep test scope separate from test kind

- Use location for coverage scope and independent names or metadata for execution kind.

## TEST-004 — Test authorization as a decision matrix

- Cover relevant authentication, role, relationship, and context combinations, including denial cases.
- Assert denial causes neither protected disclosure nor side effects.

## TEST-005 — Behavior changes require executable evidence

- Add or update the smallest automated test that would fail without a behavior change or bug fix.
- For a bug fix, first add the test that reproduces the bug and fails, then fix it.

## TEST-006 — Prefer stable public behavior seams

- Test through the highest practical stable interface that exercises the real behavior.
- Callers and tests should normally cross the same seam; avoid coupling tests to private structure when a public seam can prove the behavior.
- Maintain an executable verification path through important public behavior and into application-owned decision logic where narrower evidence materially improves determinism, coverage of owned rules, or fault localization.
- Add a lower-level test when an important owned rule cannot be exercised reliably through the higher interface, or when narrower evidence materially improves deterministic isolation or diagnosis; do not add it merely to mirror implementation structure.
- Stop at trusted library or framework behavior unless the repository owns an adapter contract at that boundary; do not mirror every function or call-graph edge merely to manufacture coverage.

## TEST-007 — Infer testing strategy from the repository before inventing one

- Reuse the repository's established test layers, commands, fixtures, and public seams.
- Treat test architecture as a design choice only when the existing structure does not provide a safe answer.
- Do not make specs repeat generic testing doctrine that is already encoded here or in repository-local rules.

## TEST-008 — Keep behavior change and structural cleanup distinct

- For approved behavior changes, establish the failing evidence before the production change and return it to green.
- Perform behavior-preserving refactoring only from a green baseline.
- If structural cleanup reveals a test that is insensitive, misleading, or validates the wrong behavior, stop before rewriting the test when doing so could conceal a product or contract decision.

## TEST-009 — Keep the deterministic test gate hermetic

- Unit and ordinary integration tests must not depend on the public internet or an uncontrolled external service.
- Use local fixtures, fakes, containers, or versioned inputs for the deterministic gate.
- Real external systems belong in explicitly classified acceptance, compatibility, or canary tiers and are not part of the fast deterministic gate unless a repository deliberately promotes them.

## TEST-010 — Build minimal deterministic fixtures

- Tests create only the state they need through small reusable builders or factories rather than a giant shared mutable seed database.
- Control IDs, clocks, randomness, and ordering when they affect assertions or diagnostics.
- Version larger reference datasets separately only when the dataset itself is part of the behavior under test.

## TEST-011 — Use snapshots only for stable structured output

- Prefer explicit semantic assertions for ordinary business behavior.
- Snapshot or golden tests are appropriate for genuinely structured/stable outputs such as serialized formats, compiler output, rendering trees, or generated text.
- Snapshot updates are deliberate reviewable changes; never regenerate snapshots automatically merely to make a failing test green.

## TEST-012 — Do not retry a deterministic gate until it turns green

- A required deterministic test failing once is a failed gate.
- Retries may collect diagnostic evidence but must not convert a failed deterministic result into success.
- Known flaky tests may move temporarily into an explicit, machine-readable quarantine tier that remains visible but does not redefine green.

## TEST-013 — Prefer risk-based differential coverage over a global percentage target

- Changed behavior requires executable evidence; use changed-line or changed-branch coverage where tooling can measure it reliably.
- Apply stronger deterministic coverage expectations to high-risk code such as authorization, persistence, parsers, protocols, migrations, billing, security-sensitive logic, and concurrency.
- Do not optimize tests merely to raise a repository-wide coverage percentage.

## TEST-014 — Tests do not depend on execution order

- Tests must be runnable individually and must not rely on state left by another test.
- Make tests parallel-safe where practical; suites that genuinely require exclusive resources may explicitly opt into serialization.
- Randomized-order or parallel stress runs may be used in broader diagnostic tiers to expose hidden coupling.

## TEST-015 — Run expensive correctness analyzers in risk-appropriate tiers

- Keep the fast development gate focused on normal compiler, linter, and test evidence.
- Full/nightly/security tiers run applicable ecosystem-native race detectors, sanitizers, Miri, thread analyzers, or equivalent deeper correctness tools.
- Changes involving concurrency, `unsafe`, native memory, or similarly high-risk code may promote the relevant analyzer into the affected-change gate.

## TEST-016 — Allocate dynamic ports for disposable processes

- Temporary test servers and parallel disposable services ask the OS for an available port rather than assuming a fixed port is free.
- Stable development services may use documented fixed ports when humans or external tooling genuinely require predictable addresses.

## TEST-017 — Choose database-test isolation by the semantics under test

- Transaction rollback is preferred when it faithfully represents behavior and provides cheap isolation.
- Tests involving commits, transaction boundaries, concurrency, migrations, connection behavior, or persistence across sessions use isolated schemas/databases/containers instead of a wrapping transaction that would change the behavior being tested.

## TEST-019 — Verify browser-specific risk at the browser boundary

- Use real-browser verification when the changed behavior materially depends on browser semantics such as layout, scrolling, pointer geometry, media, browser APIs, or navigation that a lower layer cannot prove reliably.
- Do not require a browser test when the affected risk is fully established by a cheaper stable layer; TEST-005 and TEST-009 remain the source of truth for durable executable evidence and deterministic-gate isolation.

## TEST-020 — Prefer semantic browser targets

- Browser automation targets stable user-facing semantics such as roles with accessible names, labels, and visible text before implementation-shaped selectors.
- Use deliberate test identifiers only when no stable user-facing semantic target exists; prefer them over DOM shape, CSS/XPath, or coordinates.
- Coordinate or image targeting is acceptable for surfaces without an adequate semantic tree, such as canvas or maps, but should be isolated to the interaction that actually requires it.

## TEST-021 — Keep browser test doubles protocol-faithful

- A browser failure that crosses a mocked or stubbed network boundary is not sufficient evidence of a product defect until the double preserves the production protocol semantics that materially affect the behavior.
- Match relevant request methods, statuses, headers, bodies, and stateful or streaming behavior such as redirects, cookies or authentication, CORS or cache handling, byte ranges and partial-content responses, downloads, SSE, or WebSockets.
- Prefer a deterministic real local service when it is cheap; otherwise use the smallest protocol-faithful double. Do not change product code merely to compensate for an unrealistic browser fixture.

## TEST-022 — Differentially verify alternative execution mechanics

- Apply `PRINCIPLE-010` for the semantic-equivalence and state-transition requirements when production mechanics differ from a simple reference.
- Use one deterministic differential/property/sequence harness to exercise both paths through comparable stable seams; compare observable results rather than private implementation structure.
- Keep the oracle independent enough that it does not reuse the optimization machinery whose correctness it is intended to check.
- Keep cheap differential/oracle cases in the ordinary deterministic test gate; place larger generated, stress, or exhaustive cases in an explicitly broader tier rather than omitting the evidence.
- Do not create a duplicate implementation merely for test symmetry when the semantics are trivial, the repository does not own them, or a trusted external/reference contract already provides a suitable oracle.
