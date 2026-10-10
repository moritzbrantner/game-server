# Codebase design conventions

Use these terms consistently when reasoning about code structure. They describe engineering structure, not product-domain hierarchy.

- **Module**: anything with an interface and an implementation, at any scale.
- **Interface**: everything a caller must know to use a module correctly, including invariants, errors, ordering, configuration, and relevant performance characteristics.
- **Implementation**: the code hidden behind a module's interface.
- **Seam**: a place where behavior can vary without editing the caller at that place.
- **Adapter**: a concrete implementation that fills a role at a seam.
- **Depth**: the leverage a module provides through its interface: useful behavior hidden behind a comparatively small surface.
- **Leverage**: capability callers receive without duplicating the underlying complexity.
- **Locality**: keeping related change, knowledge, bugs, and verification concentrated rather than scattered across callers.

## DESIGN-001 — Prefer deep modules over pass-through layers

- Prefer a small, stable interface that hides meaningful behavior.
- Do not add layers that merely rename or forward another interface without concentrating complexity.
- Use the deletion test: if removing a module merely spreads the same complexity across callers, the module is earning its place; if complexity disappears, the layer was probably shallow.

## DESIGN-002 — Treat seam placement as a design decision

- Introduce a seam where behavior actually varies or where a stable public testing/calling surface is valuable.
- One hypothetical adapter is not enough reason for an abstraction by itself; prefer evidence of real variation.
- Internal seams may exist for implementation composition without becoming part of the module's public interface.
- Names, interfaces, ownership, and immediate dependencies should make the seam locally understandable; coordinator modules are valid when coordination is their actual responsibility.

## DESIGN-003 — Make the interface the natural verification surface

- Design modules so callers and tests can exercise important behavior through the same stable interface.
- Accept dependencies at appropriate seams rather than constructing replaceable external dependencies deep inside behavior.
- Prefer returning explicit results over hiding important outcomes in incidental side effects.

## DESIGN-004 — Optimize for locality and leverage, not line-count ratios

- Judge depth by what callers gain and what maintainers can change locally, not by implementation-lines divided by interface-lines.
- Prefer small, cohesive source files arranged in a meaningful hierarchy: their paths should communicate what they belong to and which neighboring behavior is related.
- A deep module may span several private implementation files. A file split does not by itself justify a new public API, service, adapter, package, or repository.
- Judge file decomposition by navigability and the relationships it makes explicit. Smaller files do not necessarily reduce the reasoning scope; do not impose a universal line-count limit or split cohesive code merely to satisfy one.
- Avoid speculative generality: do not widen an interface for hypothetical future callers.

## DESIGN-005 — Resolve contradictory structural rules at the correct level

- Apply `REPO-002` to convention precedence and repository-local exceptions.
- Reorganizing implementation files must preserve the declared architectural invariants. A smaller code module is not permission to introduce an undocumented ownership or dependency exception.
- When evidence shows that a shared rule is generally too absolute, qualify it at its owning scope rather than accumulating contradictory copies. Keep genuinely repository-specific exceptions local.

## DESIGN-006 — Give each semantic decision one authoritative owner

- A domain fact, state transition, invariant, algorithmic decision, or canonical representation has one authoritative owner at a time.
- Renderers, UI projections, browser or WASM adapters, simulations, reporting tools, caches, and convenience APIs may consume or transform authoritative state but must not silently redefine its semantics.
- A second implementation of the same semantics requires an explicit reason and a parity or differential-verification contract appropriate to the risk.
- Keep non-authoritative approximations explicitly labeled and prevent them from feeding authoritative state unless the ownership boundary deliberately changes.
- When authority moves, make the cutover explicit and remove or demote the former competing source of truth.
