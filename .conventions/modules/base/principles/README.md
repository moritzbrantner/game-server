# Principles

## PRINCIPLE-001 — Prefer determinism over inference

- Prefer executable checks, deterministic mappings, explicit baselines, and structured ownership over semantic inference.

## PRINCIPLE-002 — Structure should encode agent-relevant information

- Use paths, hierarchy, names, and local instructions to communicate scope, ownership, relevance, and dependencies.

## PRINCIPLE-003 — Validate progressively

- Run the narrowest, cheapest affected checks first; expand only after they pass.
- Re-run invalidated lower layers after a production-code change.
- During an implementation loop, use focused checks for fast feedback; run the repository-owned full gate as completion evidence rather than after every edit.
- Do not make a worker repeat a full gate that the coordinating layer will immediately and independently run again unless the worker needs that full result to continue reasoning.

## PRINCIPLE-004 — Make completion observable

- Completion is defined by repository-owned, independently repeatable gates—not agent confidence.

## PRINCIPLE-005 — Document decisions, not defaults

- Document consequential choices agents cannot reliably infer.
- Prefer tooling over prose for deterministic behavior.

## PRINCIPLE-006 — Escalate complexity only when the workload requires it

- Treat direct human-to-agent work as a first-class execution mode.
- Add reusable skills when a procedure should be shared; add a loop when iteration should be automated; add tasks or orchestration only when coordination, dependency management, concurrency, durable control state, or multi-worker ownership justify them.
- Higher-level execution layers may compose lower-level capabilities, but lower-level capabilities must not require higher-level machinery merely because it exists.
- Prefer escalation from a simple invocation over configuration that makes a large framework tolerate simple work.
- Treat model inference as a scarce execution resource: obtain deterministic evidence first and invoke stronger or more stateful agent execution only when cheaper layers cannot resolve the task.
- Escalate when the current layer no longer produces useful evidence; preserve enough context for the next layer to continue the investigation.
- Reuse safe stable environments and existing context. Deterministic mechanics belong in `coding-tooling`; durable execution history belongs to the caller or orchestrator, not ordinary direct-task prerequisites.
- Reusable execution/escalation and evidence-handoff procedures belong in [`coding-agent-skills`](https://github.com/moritzbrantner/coding-agent-skills/blob/main/docs/execution-escalation.md).

## PRINCIPLE-007 — Keep capabilities replaceable; internalize with evidence

- External libraries, services, processes, and hosted infrastructure are valid bootstrap implementations; avoid unnecessary domain coupling to a particular implementation.
- Internalize only the capability actually consumed, and only when evidence supports a concrete benefit such as fewer expensive boundaries, lower latency or resource use, a smaller dependency surface, stronger determinism or portability, or useful cross-project reuse.
- Prefer staged replacement: external implementation → explicit capability contract → native candidate → differential/parity validation → representative benchmarks → real consumer → optional default switch or removal.
- Reimplementation is not justified by implementation cost alone; retain mature external implementations when a replacement has no demonstrated advantage.
- See also: [ADR 0001 — Capability internalization](../docs/adr/0001-capability-internalization.md).

## PRINCIPLE-008 — Compute and validate once; reuse the trusted representation

- At a semantic or trust boundary, parse, normalize, probe, or otherwise compute the required representation once, validate it there, and pass the resulting trusted typed object downstream.
- Do not repeatedly re-read raw input, re-parse equivalent bytes, re-run equivalent validation, or recompute the same derived structure merely because another stage needs the same already-proven facts.
- Keep invalidation explicit: when any deterministic input that gives the object its meaning changes, discard or rebuild the affected trusted object and its derived structures before reuse.
- Recompute when a consumer genuinely needs different evidence, when independent verification is itself part of the contract, or when reuse would blur ownership or freshness semantics.
- Prefer ownership-preserving handles, references, immutable snapshots, or other cheap sharing mechanisms over repeated conversion and cloning where the language/runtime supports them.
- Do not extract a fleet-wide shared type merely to implement this principle. Keep the validated representation with its semantic owner and extract a reusable contract only after multiple real consumers demonstrate the same meaning and lifecycle.

## PRINCIPLE-009 — Spend interface space on the user's work

- In an interactive product, prominent screen space must support a current user decision, action, or state; product self-description is not a default UI function.
- For editors, games, maps, labs, media tools, and other workspace-oriented products, treat the primary work surface as the product: give the canvas, timeline, world, document, visualization, or manipulated objects the dominant usable area.
- Prefer manipulating represented objects directly when the domain has a natural spatial or temporal interaction. Inspectors, forms, menus, and command surfaces should supplement direct work rather than replace it.
- Assume returning users already know which product they opened. Do not repeatedly explain the product's purpose, architecture, implementation model, capability claims, privacy posture, or source provenance on ordinary workflow screens unless that information changes an immediate choice or consent decision.
- Prefer direct access to the actual choices, content, and controls over an introductory block, section preamble, badge row, or call to action that only restates or points at content already visible on the same screen.
- Remove redundant visible hierarchy when the content names itself. Preserve required accessibility semantics without forcing explanatory chrome into the visual layout.
- Keep explanatory material in onboarding, help, documentation, about surfaces, or a deliberate details affordance when users may need it.

## PRINCIPLE-010 — Prove optimized mechanics against simple semantics

- When owned behavior has a simple, independently understandable reference formulation, keep it as a development/test oracle when production mechanics become retained, incremental, cached, indexed, reordered, parallel, or otherwise harder to reason about directly.
- Compare production mechanics with the reference over representative deterministic inputs and state transitions, including mutations that exercise invalidation and reuse; matching one steady-state output is not sufficient evidence for an incremental architecture.
- Treat the reference as semantic authority, not as the required runtime architecture. Production code may use a substantially different execution strategy once equivalence is established.
- Keep semantic equivalence and execution cost as separate claims: reference parity proves correctness, while cost models, deterministic work counters, and representative benchmarks prove that the production architecture avoids unintended recomputation or materialization.
