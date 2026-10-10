# Reliability conventions

## REL-001 — Handle errors specifically inside the system and broadly only at deliberate boundaries

- Internal code handles expected failures it can meaningfully recover from or translate; otherwise propagate them.
- Broad catches belong at explicit process, request, worker, or job boundaries where they intentionally log, translate, or recover.
- Empty catches and silently discarded errors are forbidden; intentionally ignored errors must be explicit in code.

## REL-002 — Bound external operations

- Network calls, database operations, queues, RPC, subprocesses, and similar cross-process operations use an explicit finite timeout or cancellation bound.
- Long-running operations may explicitly opt out when unbounded duration is genuinely part of their contract.
- Do not add artificial timeouts to ordinary in-process computation.

## REL-003 — Retry only failures classified as transient and safe to repeat

- Retries are bounded, use a defined backoff strategy, and continue to respect timeout and cancellation.
- Non-idempotent operations require an idempotency mechanism or an explicit proof that retrying cannot duplicate effects.
- Never use retries merely to turn an unexplained deterministic failure green.

## REL-004 — Give scarce resources an obvious owner and deterministic cleanup

- Files, sockets, database connections, subprocesses, locks, streams, subscriptions, GPU/audio handles, and similar resources must have an obvious lifetime owner.
- Prefer language-native scoped cleanup such as RAII, `using`/`await using`, guards, or equivalent mechanisms.
- Do not rely on garbage collection or finalizers for timely release of external or scarce resources.

## REL-005 — Propagate cancellation from lifecycle boundaries through blocking work

- Requests, jobs, CLI operations, workers, database calls, network I/O, and subprocesses propagate cancellation when their surrounding lifecycle supports it.
- Small in-memory async helpers do not need cancellation parameters merely for uniformity.

## REL-006 — Detached asynchronous work requires explicit ownership

- Async work is awaited or returned by default.
- Fire-and-forget work is allowed only through an explicit abstraction that owns lifetime, cancellation, failure handling, and observability.
- Do not leave promises, tasks, or futures unobserved.

## REL-007 — Make shared mutable state and synchronization explicit

- Prefer clear ownership, immutable transfer, or message passing when they keep concurrency simpler.
- Shared mutable state is allowed when it is simpler or performance-relevant, but its synchronization and ownership boundary must be locally understandable.
- Mutable global state is limited to explicit process-wide infrastructure such as tracing, metrics, immutable configuration snapshots, or carefully synchronized caches; domain state should have an explicit owner.

## REL-008 — Use structured logging where logs are operational data

- Services and applications use stable event names and structured fields for identifiers, durations, statuses, error categories, and other queryable values.
- Do not hide operationally important values only inside interpolated message strings.
- Local human-oriented CLI and development scripts may use simpler text output.

## REL-009 — Make public compatibility an executable concern

- Versioned/public APIs, schemas, persisted formats, CLI contracts, package exports, and protocol messages should have deterministic compatibility checks where established tooling can provide them.
- Breaking changes are allowed when intentional, but must be explicitly classified through versioning, migration, adapter, or deliberate compatibility-baseline changes rather than occurring accidentally.

## REL-010 — Give CLIs stable exit and stream semantics

- Successful commands return exit code `0`; failures return a non-zero exit code.
- Machine-readable modes write structured results to stdout and keep diagnostics/progress on stderr so callers can compose them reliably.
- Specialized exit codes are allowed when a CLI genuinely needs them, but do not invent a landscape-wide bespoke taxonomy.

## REL-011 — Replace durable machine-managed state atomically

- State/config manifests, generated indexes, cache metadata, downloaded artifacts, and similar machine-managed files use temporary output plus validation and atomic replacement where the filesystem supports it.
- Do not leave a durable state file partially written when a process is interrupted.
- Ordinary source editing does not require a bespoke atomic-write abstraction.
