# Reproducibility conventions

## REP-001 — Green verification introduces no new warnings

- New and cleaned repositories should treat compiler and linter warnings as failures.
- Existing warning debt may use an explicit temporary baseline, but changes must not add warnings and the baseline may only shrink.
- Prefer a narrow suppression with a reason over disabling a warning category globally.

## REP-002 — Mechanize stable preferences before instructing agents

- Prefer ecosystem-native compiler options, formatters, linters, analyzers, schemas, tests, and configuration over prose instructions for mechanically decidable policy.
- Keep policy in conventions and mechanics in deterministic tools; do not invent a bespoke checker when an established native tool already expresses the rule well.

## REP-003 — Generated outputs are disposable local state by default

- Do not commit generated artifacts when source inputs plus a deterministic generator can reproduce them.
- Generation must be deterministic and idempotent. Expensive generators may reuse a valid local result based on declared inputs and generator/tool versions.
- Clean verification and CI may regenerate required outputs from source. Commit generated output only when the generated file itself is intentionally part of a distribution or public contract.

## REP-004 — Observable output has stable ordering

- Define deterministic ordering for values that are serialized, persisted, hashed, snapshotted, compared, or emitted through user- or machine-visible interfaces.
- Internal sets and maps may remain unordered when order has no observable meaning.

## REP-005 — Control time, randomness, and generated identities where behavior depends on them

- Put clocks, randomness, and ID generation behind a controllable seam only when behavior depends on them.
- Tests use fixed clocks and fixed or seeded randomness/identities by default; use real implementations when their behavior is specifically under test.

## REP-006 — Keep machine time and locale semantics explicit

- Represent instants and machine-readable timestamps in UTC unless the domain explicitly requires another zone.
- Parsing, serialization, and tests use explicit invariant formats, time zones, and locales.
- Convert to a user's locale, currency, number format, or time zone at deliberate presentation boundaries.

## REP-007 — Compare floating-point results by their semantic contract

- Exact comparison is appropriate when exactness is part of the contract or a value is merely carried through unchanged.
- Computed floating-point results use an explicit domain-appropriate tolerance; do not hide different accuracy requirements behind one global epsilon.

## REP-008 — Keep toolchains current

- Repositories may use current stable toolchain channels or native version declarations. Do not require an exact fleet-wide version, canonical baseline, or repeated CI check of the resolved toolchain identity.
- When a toolchain update exposes an incompatibility, fix the affected repository. Pin a version narrowly when a specific compatibility or release contract requires it.

## REP-009 — Unused implementation code is not part of a green baseline

- Unused private code, imports, variables, parameters, and unreachable implementation paths should fail deterministic verification where the language can detect them.
- Public library seams, callbacks, FFI, generated bindings, and feature-gated code may use an explicit language/tooling-supported annotation or suppression when the unused shape is intentional.
- Do not globally disable unused-code diagnostics to accommodate one intentional seam.

## REP-010 — Handle closed variants exhaustively and unknown external values explicitly

- Closed enums, variants, and discriminated unions owned by the application should be handled exhaustively so adding a new case creates a deterministic failure at affected callers.
- External protocols and deserialization boundaries may use an explicit unknown/fallback case when forward compatibility requires it.

## REP-011 — Keep repository text portable

- Source, configuration, documentation, scripts, fixtures, and machine-readable text use UTF-8 and LF by default.
- Use an explicit narrow exception only when a platform or external tool requires another encoding or line ending.
- Prefer `.editorconfig` and native formatters to make the rule automatic rather than relying on editor settings.

## REP-012 — Separate verification from mutation

- Commands used as gates such as `check`, `verify`, format-check, lint, tests, CI, and release verification do not modify tracked source, lockfiles, generated outputs, or configuration.
- Mutating operations such as formatting writes, autofixes, dependency updates, code generation, and migration generation use separate explicit commands.
- Green means the submitted state was valid, not that verification silently repaired it.

## REP-013 — Use semantic paths internally and canonical paths at observable boundaries

- Use the language/runtime path APIs rather than manual path-string concatenation.
- Machine-readable output, manifests, hashes, snapshots, diagnostics, and cross-platform comparisons use stable repository-relative `/`-separated paths.
- Do not make a command depend on the caller's current working directory unless that is an explicit part of its contract.

## REP-014 — Keep disposable output in declared disposable locations

- Temporary files use OS temporary storage or an explicit ignored workspace directory.
- Build, cache, and generated outputs use conventional declared ignored locations rather than being scattered through source directories.
- A supposedly non-mutating verification command must not leave unexpected repository artifacts behind.

## REP-015 — Treat caches as transparent optimizations

- Correctness must not depend on a warm cache; important verification can succeed from a cold cache.
- Cache identity includes every relevant input and tool version needed to prevent stale results from masquerading as valid work.
- Invalid or corrupt caches are safely disposable, while agents should reuse valid caches instead of repeatedly downloading or rebuilding the same inputs.

## REP-016 — Separate dependency acquisition from deterministic build work

- Restore/install/fetch phases may use the network to acquire declared dependencies and external inputs.
- Once those inputs are present, compilation, tests, packaging, generation, and deterministic verification do not unexpectedly reach the public network.
- Hidden downloads are dependency acquisition and should be made explicit rather than buried inside a build step.

## REP-017 — Control ambient environment at deterministic boundaries

- Ordinary interactive development may inherit the user's shell environment.
- Hermetic tests, generators, builds, packaging, and release steps explicitly control environment values that can alter behavior, such as locale, time zone, proxies, feature flags, credentials, paths, and compiler settings.
- Unknown ambient variables must not silently change deterministic output.

## REP-018 — Keep build artifacts reproducible from declared inputs

- Artifact contents do not accidentally depend on wall-clock time, host name, user name, absolute checkout path, random build IDs, or other ambient machine state.
- When build metadata is part of the product contract, derive it from declared reproducible inputs such as the source revision or release metadata.

## REP-019 — Validate configuration against real contracts

- Use official or ecosystem-native schemas and validators when a configuration format has an established contract.
- Repository-owned machine-readable configuration gains deterministic structural validation once other tooling consumes its shape.
- Do not invent schemas for incidental data files merely because they use JSON, TOML, or YAML.

## REP-020 — Normalize Unicode where byte identity represents textual identity

- Use an explicit Unicode normalization policy for identifiers, generated filenames, canonical serialized forms, hashes, search/deduplication keys, or cross-system equality when canonically equivalent text must compare the same.
- Do not silently normalize ordinary human-facing text when the distinction could be meaningful to the domain.

## REP-021 — Reconcile deterministic mutations instead of repeating work

- A deterministic mutating operation inspects current state, derives the desired state, compares them, applies only the required delta, and verifies the resulting state.
- Reapplying the same operation to an already-satisfied state must be a verified no-op: no unnecessary file rewrites, installs, fetches, rebuilds, or other side effects merely to rediscover the same result.
- Machine-readable mutation results should distinguish at least `changed`, `unchanged`, and `conflict`, and expose created, changed, removed, skipped/no-op, or verified subjects when that detail is useful to callers.
- Tests for deterministic mutators should apply the same operation twice and assert that the second application performs zero writes or equivalent reconciliation work while preserving the verified final state.
- Unrelated input changes must not invalidate derived work. Relevant input changes should trigger only the smallest safe affected reconciliation.
- Use input fingerprints or incremental state only for expensive derived work where the identity includes every relevant input and tool version. Do not add memoization to cheap pure algorithms merely because they are deterministic.

## REP-022 — Deterministic systems replay from declared inputs

- When deterministic replay is part of a system's contract, the replay identity includes the authoritative initial state, ordered external inputs or events, seed or random stream, relevant configuration, and implementation/content identity needed to interpret them.
- Replaying the same declared inputs must reproduce the same authoritative observable state or canonical fingerprint.
- Wall-clock time, thread scheduling, unordered iteration, ambient machine state, rendering cadence, and non-authoritative simulations must not silently alter authoritative replay results.
- When nondeterminism is intentional, isolate and record it at an explicit boundary rather than allowing it to leak into otherwise deterministic state transitions.
- A replay verifier should identify the first divergent authoritative step or fingerprint when practical instead of reporting only a final mismatch.
