---
name: Agent task
about: One PR-sized task for a coding agent (see docs/AGENT_TASKS.md)
title: "<Area> <slice>: <what consumers or the runtime gain>"
labels: ["agent-task", "spec:draft"]
---

Serves <`ROADMAP.md` slice, or consumer request such as moritzbrantner/mmorpg#N>. Intended implementer: **<Opus|Sol|Sonnet>**. Start after: <#N or "nothing">. One branch (`agent/<topic>`), one PR; follows the `AGENTS.md` **Execution scope** rules.

## Goal

<Two or three sentences: what consumers or operators can do afterwards.>

## Decisions already made (do not reopen)

- **Behavior and numbers:** <…>
- **Formats:** <frame kinds/fields, version bumps or why the change stays additive; or "no format change">
- **Compatibility:** <existing clients, recovery images, replays and consumer pins>
- **Left to the implementer:** <explicitly delegated choices, recorded in the PR>

## Acceptance

- <unit tests and the experiment script for network-visible behavior>
- <benchmark comparison / building the requesting consumer against the branch, when relevant>
- CI green and every Codex review finding addressed or answered.

## Expected changes

- <modules, experiment scripts, README section, ADR, ROADMAP.md>

## Out of scope

- <…>
- Game rules owned by consumers.
- Tooling, CI, pin-refresh and benchmark-baseline work.

## Parallel work

- <open tasks touching the same modules or formats, or "none">
