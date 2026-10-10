# Agent conventions

## AGENT-003 — Separate execution from orchestration

- Keep the development loop independent of its local, CI, or hosted orchestration adapter.

## AGENT-004 — The harness defines completion

- The harness owns the completion gates; agents propose and repair changes.

## AGENT-005 — Integration is its own workspace

- Combine and validate independently produced changes in a dedicated integration workspace.

## AGENT-006 — Prefer mechanical discovery before semantic search

- Derive relationships from paths, names, metadata, or indexes before searching semantically.

## AGENT-009 — Delegate one bounded capability per implementation run

- Give each delegated implementation run one independently verifiable capability slice.
- Do not invent missing task data or widen the assigned scope.
- Only one active implementation run may own an overlapping path or behavioral scope.
- Report undeclared prerequisites, drift, overlap, or inconsistent delegated inputs to the delegating caller for replanning.
- Distinguish completing a partial slice from satisfying the broader convention.

## AGENT-010 — Apply progressive composition to agent execution

- Resolve execution-layer choices to `PRINCIPLE-006` and progressive verification to `PRINCIPLE-003`; AGENT-010 is the agent-category pointer and adds no second copy of those policies.
- Apply the reusable execution/escalation procedure in `coding-agent-skills`; deterministic mechanics remain owned by `coding-tooling` and durable history by the caller.

## AGENT-011 — Classify failures before repairing them

- Classify a failing signal before changing production behavior: candidate regression, pre-existing repository defect, repository-owned test or harness defect, environment or infrastructure failure, or unavailable/incomparable evidence.
- Repair the layer that owns the failure. Do not change production code merely to satisfy an unrealistic fixture, broken harness, unavailable dependency, or unrelated pre-existing failure.
- Preserve the evidence used for classification and revisit the classification when new evidence contradicts it.
- When ownership cannot yet be established, improve the diagnostic evidence before widening the patch.
