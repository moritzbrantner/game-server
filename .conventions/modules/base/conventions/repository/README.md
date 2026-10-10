# Repository conventions

## REPO-001 — Repository structure encodes agent-relevant relationships

- Prefer layouts whose relationships are mechanically derivable from paths, hierarchy, naming, or local metadata.

## REPO-002 — More specific conventions override broader conventions

- On conflict, use the narrowest applicable rule; non-conflicting broader rules remain in force.
- Precedence: repository rule → deepest technology scope → parent scopes → general convention → principle.
- A deliberate repository-local exception names the overridden shared rule and explains why; it does not require a shared-policy change before local work can proceed.

## REPO-009 — Use conventional roots for durable agent-authored project knowledge

Unless a repository explicitly overrides them, use:

- `CONTEXT.md` for the concise domain glossary and project-level domain overview;
- `docs/domain/` for richer domain-first hierarchical knowledge;
- `docs/adr/` for consequential architectural decisions;
- `docs/specs/` for canonical human-readable implementation specs;
- `docs/reviews/` for durable review history when review persistence is enabled.

These are repository-layout defaults, not requirements that every repository create every directory. Create a durable artifact only when the corresponding knowledge exists. Runtime agent state, ticket queues, run evidence, and derived catalog/profile resolution do not belong in these roots.

## REPO-010 — Keep TODOs machine-discoverable and actionable

- Developer-authored TODOs are legitimate follow-up work that agents may later enumerate, implement, or turn into issues.
- Use `TODO: <actionable description>` when no issue exists and `TODO(#123): <actionable description>` when the work is already tracked.
- A linked issue is optional; vague markers such as `TODO fix this` or unexplained `FIXME` comments are not an acceptable durable backlog.

## REPO-011 — Verify only meaningful file permissions

- Directly executable scripts and tools have the executable bit when the target platform uses it; ordinary source/config files should not accidentally become executable.
- Sensitive private-key material created locally uses restrictive permissions appropriate to the platform.
- Do not normalize permission metadata that has no portable semantic meaning.

## REPO-012 — Treat symlinks as explicit filesystem boundaries

- Deterministic traversal does not recursively follow symlinks by default.
- When symlinks are intentionally supported, targets stay inside the declared repository/workspace boundary unless the command explicitly permits external targets.
- Hashing, copying, cleanup, and generation distinguish the link from its target rather than silently traversing it.

## REPO-013 — Keep path casing portable

- Imports and references use the exact on-disk path casing.
- A repository must not contain case-colliding tracked entries such as `User.ts` and `user.ts`.
- Perform case-only renames explicitly through Git rather than relying on the host filesystem's case sensitivity.

## REPO-014 — Public agent tools expose a stable machine discovery document

- A public tool intentionally designed for coding-agent consumption exposes a versioned machine discovery document at a stable published URL, preferably `agent-tool.json` at the tool's Pages root.
- The discovery document identifies the tool, describes remotely consumable operations and their transport, and states which operations remain authoritative local commands.
- Prefer real static JSON endpoints when the operation is read-only metadata or committed evidence. Label browser-executed JSON views distinctly; do not present static hosting as a server-side API.
- Keep limitations explicit, especially authentication, privacy, execution, mutation, CORS, rate-limit, or completeness boundaries.
- This rule does not require private/internal repositories, ordinary libraries, or tools without a meaningful remote agent operation to become public or acquire a Pages façade.

## REPO-015 — Prefer vertical growth before creating another repository

- Before creating a new repository or lab, identify the capability, ownership boundary, runtime constraint, or validation need that cannot be expressed coherently in an existing repository.
- Prefer a deeper slice in an existing repository when the proposed work reuses the same domain, teaching surface, runtime, or maintenance owner and does not need an independently versioned contract.
- A new repository is justified when separation materially improves ownership, release cadence, security/privacy boundary, dependency isolation, platform/runtime isolation, or reusable public contract—not merely because the topic can be named separately.
- For a new lab, require at least one demonstrable interaction, measurement, algorithm, or platform behavior that the existing labs do not already teach. A renamed collection of explanations or duplicate controls is insufficient.
- Record the nearest existing repository considered and why extending it was rejected. Keep this rationale concise and reviewable; it is a scope decision, not a permanent architectural commitment.
- Do not block small experiments from being tested locally or on a branch. The higher bar applies when promoting an experiment into a durable repository with its own maintenance, CI, Pages, dependency, and automation surface.

## REPO-016 — Public repositories provide a useful GitHub Pages surface

- Public repositories should expose a maintained GitHub Pages site unless the repository records an explicit reason to opt out.
- Pages is a product surface, not a decorative README mirror. Prefer the smallest page that helps a visitor understand, evaluate, or directly experience what the repository does.
- Applications and visual tools should publish a usable application, playground, or representative interactive slice when static hosting permits it.
- Libraries should explain their public purpose, core concepts, examples, important trade-offs, and evidence such as benchmarks. When the real implementation can reasonably execute in a browser, prefer a live demo over a JavaScript reimplementation.
- Rust libraries may use WebAssembly adapters to expose existing crate behavior. Keep algorithm and domain ownership in the normal Rust crates; the WASM layer should stay thin so browser and native behavior cannot silently drift.
- Backend and service repositories should explain architecture, domain boundaries, API behavior, data flow, operational constraints, and representative workflows. A browser client, deterministic simulation, or fixture-driven demonstration is useful when it exercises real shared contracts, but do not distort server architecture merely to make it browser-runnable.
- CLI and tooling repositories should show the workflow, commands, configuration, generated evidence, or representative reports; interactive visualization is optional when explanation is the more faithful interface.
- Do not force WASM or browser compatibility through filesystem, networking, threading, database, GPU, or platform abstractions that would make the production code less coherent. A truthful static explanation is better than a fake demo.
- Pages deployments should reuse the repository landscape's shared deployment workflow when applicable and publish the same validated build artifact or a deterministically derived static artifact.
- Treat the Pages surface as part of the repository's public contract: keep examples accurate, responsive, accessible, and aligned with current capabilities rather than promising unimplemented behavior.

## REPO-017 — Public claims must not exceed verified capability

- README text, Pages surfaces, generated reports, release notes, PR descriptions, benchmark summaries, and demos must distinguish verified capability from experimental, advisory, simulated, or planned behavior when the distinction matters.
- Do not turn skipped, unavailable, incomparable, partial, or non-authoritative evidence into a success claim.
- Qualify claims to the boundary actually proved: target, workload, platform, precision, protocol behavior, authority level, or other material limitation.
- Prefer a narrower accurate claim over a broader claim that relies on inference beyond the evidence.

## REPO-018 — Track the current shared convention authority

- Consumer repositories select applicable `coding-agent-conventions` modules but do not pin the shared convention repository to a commit, tag, or historical snapshot.
- Resolve selected modules from the current shared convention authority before implementation and validation. Local managed files may be used as a cache, but they must not make an older policy revision authoritative.
- If a current convention exposes an incompatibility or validation failure, repair the consumer or record a narrow repository-local exception with the reason; do not freeze the consumer on an older convention revision.

## REPO-019 — Update documentation with the behavior it describes

- A change that alters documented behavior, commands, boundaries, or vocabulary updates the affected `README.md`, `CONTEXT.md`, `AGENTS.md`, or `docs/` in the same change.

## REPO-020 — Record consequential decisions with the change

- Implementation does not require a separate planning issue first.
- When a change makes a consequential architectural decision (new boundary, dependency, persistence or protocol shape, or a trade-off that is hard to reverse), add an ADR under `docs/adr/` in the same change.

## REPO-021 — Converge maintained repositories on canonical workflow profiles

- Maintained repositories that use GitHub Actions select a canonical workflow profile from the shared `reusable-workflows` profile catalog rather than independently inventing top-level GitHub workflow topology.
- For those GitHub Actions repositories, enable only the profile roles the repository actually needs; prefer jobs and matrices inside canonical caller workflows over adding another top-level workflow file.
- Any workflow outside the selected profile is an explicit repository-local exception with a concrete reason.
- Repository-owned commands and deterministic tooling continue to own validation semantics; the shared workflow profile owns only GitHub workflow topology, canonical caller roles, and reusable hosted mechanics.

