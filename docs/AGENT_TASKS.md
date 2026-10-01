# Agent tasks

How work reaches the coding agents. A task is one GitHub issue that one agent turns into one PR (see `AGENTS.md`, Execution scope). Anyone may draft an issue, including a person, a chat assistant or an agent working in a consumer repository. An issue becomes implementable only once it is `spec:ready`.

## Roles

| Agent | Does |
| --- | --- |
| Claude Opus | Runs the loop (`/agent-loop`). Turns drafts into ready specs, writes new specs from `ROADMAP.md` and consumer requests, reviews PRs against their spec and merges them. Implements critical-path and cross-cutting runtime work itself (`agent:opus`): anything a consumer is blocked on, authority boundaries, wire/replay/recovery formats. |
| ChatGPT Sol | Implements narrow, technically deep `agent:sol` tasks via the Codex `implementer-loop` skill. The spec should settle architecture, authority, formats and scope so Sol can spend depth on correctness rather than redesigning adjacent systems. Runs occasionally, separately from `/agent-loop`, through a backlog of up to three tasks that nothing else waits on. |
| Claude Sonnet | Implements `agent:sonnet` tasks: docs, README/ADR updates, experiment-script and probe-client follow-ups, mechanical refactors. |
| GitHub Actions | The full deterministic gate on every PR (`validate.yml`: format, Clippy, tests, release build and the real-network experiments). |
| Codex review | Reviews each PR automatically when it is opened or marked ready; `@codex review` re-triggers it. |

## Labels

- `agent-task`: every task issue.
- `spec:draft`: written but not yet checked against the code. Do not implement.
- `spec:ready`: checked and implementable.
- `spec:needs-input`: blocked on a question for the owner, asked in a comment.
- `agent:opus`, `agent:sol`, `agent:sonnet`: the intended implementer.
- `in-progress`: an implementer has started; the PR will reference the issue.

## Picking up a task (implementers)

When asked to "pick up work", take the oldest open issue labeled `spec:ready` plus your `agent:*` label that has no `in-progress` label and whose "Start after" dependencies are merged. Add `in-progress`, branch `agent/<topic>` (or the branch the issue names) and follow the issue and `AGENTS.md`. Open the PR only when the branch is complete, with `Closes #N`. Never implement `spec:draft` or `spec:needs-input` issues. If the spec turns out to be wrong or impossible, comment on the issue, replace `spec:ready` with `spec:needs-input`, remove `in-progress` and stop; do not silently re-scope it.

## Implementer loop

An implementer run (Codex: the `implementer-loop` skill in `.agents/skills/`; Sonnet: dispatched by `/agent-loop`) takes exactly one action, in this priority order, then reports and exits.

1. **Fix your own open PR.** A PR of yours (its issue carries your `agent:*` label) needs work when:
   - a CI check failed;
   - a Codex review finding is neither fixed nor answered;
   - the loop driver posted a "changes needed" comment newer than your last push.
   
   Fix it on the same branch, push, and reply to each finding. After substantial fixes, comment `@codex review`. After three failed attempts on the same failure, comment what blocks you on the PR and stop touching it.
2. **Otherwise, wait if your PR is still in review.** If a PR of yours is open and only waiting on CI, Codex or the loop driver's merge, do nothing. One task in flight per implementer.
3. **Otherwise, start the next task** per "Picking up a task". Work in a fresh worktree from `origin/main`. Commit in small steps. Run the focused checks plus what the issue lists that CI does not run. Push, then open the PR with `Closes #N`. Wait for CI and the first Codex review, and handle them as in step 1 within the same run.
4. **Otherwise, exit.** Do not invent work: no new issues, no tooling, CI, pin or cleanup tasks.

An implementer never merges, never edits issue bodies, never writes specs and never changes a `spec:*` label except to replace `spec:ready` with `spec:needs-input` when the spec is wrong. That last case always comes with a comment explaining why and removal of `in-progress`.

## Writing an issue

**Title:** `<Area> <slice>: <what consumers or the runtime gain>`, for example `Turn-based integration G3: card-game-template replay-fingerprint fixture`.

**Sizing:**
- One PR. Big enough to deliver a whole roadmap slice or consumer request, small enough that one agent finishes it in one session.
- Split only along a real seam (for example the runtime change first, the experiment or docs follow-up after it merges).
- At most one bump per format version (wire protocol, reliable control, replay, recovery image/bundle, external simulation, browser route, host status) per task.
- Pick the implementer by the table above: ambiguous, cross-cutting, consumer-blocking or format/authority work → `agent:opus`; narrow but technically deep work with settled decisions, strong deterministic acceptance and no downstream waiters → `agent:sol`; docs and mechanical follow-ups → `agent:sonnet`.
- For `agent:sol`, keep breadth narrow even when implementation depth is high: pin the important decisions, name explicit out-of-scope boundaries, and do not rely on the implementer to decompose or redesign neighboring systems.

**Body:** use these sections in this order (the "Agent task" issue template has them):

1. **Header line:** roadmap slice or consumer request it serves (for example `moritzbrantner/mmorpg#18`), implementer, branch name, `Start after #N` if it depends on another task.
2. **Goal:** two or three sentences on the observable result for consumers or operators.
3. **Decisions already made (do not reopen):**
   - behavior, bounds and numbers (tables welcome);
   - exact format changes: frame kinds, fields, version bumps or why the change stays additive;
   - compatibility behavior for existing clients, recovery images, replays and consumer pins;
   - deliberate simplifications.
   
   Anything left open says so explicitly ("implementer decides X; record it in the PR").
4. **Acceptance:** concrete unit tests and, for network-visible behavior, the experiment script that proves it. Name the checks CI does not run: a benchmark comparison for performance-motivated work, and building the requesting consumer against the branch. Always end with "CI green and every Codex finding addressed".
5. **Expected changes:** modules, experiment scripts and docs (README section, ADR, `ROADMAP.md`) likely touched.
6. **Out of scope:** what a thorough implementer might otherwise add. Always includes tooling, CI, pin and benchmark-baseline work, and game rules that belong to consumers.
7. **Parallel work:** open tasks touching the same modules or formats, and how to stay out of their way.

**Quality bar for `spec:ready`:**
- Consistent with `AGENTS.md` (authority, determinism and compatibility).
- No unresolved design question that would change a format or an authority boundary.
- Acceptance checks can be verified from the PR.
- Matches the current code: version constants, frame kinds, module names and bounds are checked on `main`.

## Drafting with a chat assistant

To hash out an issue in a chat (e.g. ChatGPT) and have it filed, paste this into the chat:

> You are helping me specify a task for the `moritzbrantner/game-server` repository. Before proposing anything, read `AGENTS.md`, `docs/AGENT_TASKS.md`, `README.md`, `ROADMAP.md` and the ADRs in `docs/adr/` relevant to the topic. Discuss the task with me first: challenge scope that is too large for one PR, ask about decisions that would change a versioned format or an authority boundary, and propose concrete numbers. When I say "file it", create a GitHub issue in `moritzbrantner/game-server` with the title and body sections exactly as in `docs/AGENT_TASKS.md` "Writing an issue", and the labels `agent-task`, `spec:draft` and the `agent:*` label we agreed on. Never label it `spec:ready`; Claude checks drafts against the code first. If you cannot create issues, output the title and the body as a Markdown code block instead.

If the chat cannot create issues, open a new issue with the "Agent task" template and paste the body. The next `/agent-loop` run checks the draft against the code, completes or corrects it, and flips it to `spec:ready` (or asks its questions under `spec:needs-input`).

Agents in consumer repositories (for example `mmorpg`) that need a runtime change file it here the same way as a `spec:draft`, linking their own issue or PR. The loop treats such consumer requests as the highest-priority work.
