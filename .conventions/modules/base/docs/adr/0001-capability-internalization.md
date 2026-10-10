# ADR 0001 — Capability internalization

- Status: Accepted
- Scope: coding-agent landscape and repositories that adopt `PRINCIPLE-007`

## Context

External libraries, Docker Compose services, subprocesses, hosted APIs, language bridges, and other general-purpose systems are often the fastest way to obtain a capability. They are not automatically architectural debt.

At the same time, mature general-purpose implementations often carry compatibility behavior, process and network boundaries, serialization, features a consumer does not use, or abstractions required for workloads that are broader than the consuming application. Coding agents reduce the cost of implementing and evaluating specialized alternatives, which makes selective reimplementation a practical experiment rather than an all-or-nothing rewrite.

The landscape therefore needs a durable rule for when to keep an external implementation, when to introduce an implementation-independent boundary, and when a smaller native implementation is justified.

## Decision

Treat important functionality as a capability whose implementation may change over time.

Use external implementations freely while they are the best current trade-off. Avoid coupling domain behavior unnecessarily to a vendor, service protocol, package-specific model, or process boundary when a narrow capability boundary can express what the application actually consumes.

When there is a concrete reason to investigate internalization, prefer this progression:

```text
external implementation
        ↓
explicit capability contract + external adapter
        ↓
specialized candidate implementation
        ↓
differential/parity validation against the reference
        ↓
representative performance/resource evaluation
        ↓
validation in at least one real consumer
        ↓
keep external | keep both | make candidate default
        ↓
remove the external implementation only when no longer needed
```

A failed replacement experiment is an acceptable outcome. Evidence that the existing implementation is already the better trade-off should terminate the experiment rather than trigger further rewriting.

## What counts as evidence

A replacement decision should normally have evidence in three categories:

1. **Correctness** — differential, property, fuzz, fixture, protocol, or other parity evidence against an accepted reference or specification.
2. **Performance and operational cost** — representative latency, throughput, CPU, memory, allocation, binary/package size, startup, I/O, serialization, IPC/network, or other workload-relevant measurements.
3. **Usefulness** — at least one real consumer whose actual required capability subset is served by the candidate.

Not every metric applies to every candidate. The workload defines the relevant measurements.

## Capability scope

Prefer internalizing the smallest useful capability, not recreating an upstream product wholesale.

For example, an application that uses a search service for tokenization, an inverted index, BM25 ranking, filters, prefix search, and Top-K retrieval should first evaluate those capabilities. It should not begin by rebuilding clustering, administration APIs, scripting, replication, or unrelated compatibility surfaces.

Likewise, an application using a cache service only for local TTL key/value data and counters may evaluate an in-process implementation without claiming to replace the complete external cache product.

## Native kernels

Successful specialized implementations should be extracted into small reusable foundations when multiple consumers benefit from them.

Prefer a landscape of focused crates and packages over a monolithic `utils` dependency. A consumer that needs an indexed heap should not have to pull in search, media, geometry, or storage implementations.

Rust is a preferred implementation language for reusable compute-heavy or systems-oriented native kernels when it provides meaningful portability, performance, safety, or cross-project reuse. It is not mandatory when another implementation better serves the workload.

Likely kernel families include algorithms and collections, graph and ordering structures, indexing and search primitives, geometry and spatial structures, parsers, codecs, scheduling, caching, streaming/statistical structures, media primitives, storage helpers, and interoperability adapters.

## Candidate triggers

A capability is worth investigating when evidence or requirements suggest one or more of:

- an expensive network, subprocess, FFI, WASM, serialization, or other runtime boundary;
- significant latency, CPU, memory, allocation, startup, or package/binary cost;
- a large dependency surface for a small consumed subset;
- portability, offline, embedding, determinism, or deployment constraints;
- a capability that would be reused by multiple repositories;
- a specialized workload for which the general-purpose implementation cannot provide an important property.

The mere availability of cheap generated code is not a trigger.

## Guardrails

Do not replace mature infrastructure or security-critical implementations merely to reduce dependency count.

Whole-database engines, cryptographic primitives, TLS stacks, distributed consensus systems, and similarly reliability- or security-sensitive systems require a substantially higher evidence bar. Internalizing a narrow capability around such systems can still be appropriate without attempting to reproduce the system itself.

Do not make every dependency pass through a speculative abstraction. Introduce a capability boundary when the implementation may realistically vary, the boundary reduces unwanted coupling, or an internalization experiment is being prepared.

Do not keep shadow/reference implementations forever without a purpose. Once a decision is stable, retain only the comparison fixtures or adapters that still provide useful regression evidence.

## Landscape responsibilities

The existing coding-agent landscape should be extended rather than duplicated:

- `coding-agent-conventions` owns the policy that replacement must remain evidence-driven.
- `coding-agent-skills` owns the reusable capability-internalization procedure.
- `coding-tooling` discovers and invokes deterministic repository capabilities and may expose mechanical dependency/boundary inspection without deciding whether replacement is desirable.
- `runtime-profiler` captures reproducible runtime evidence.
- Moonlight evaluates compatible baseline/candidate behavior and evidence.
- `agent-contracts` owns neutral cross-component evidence/evaluation envelopes.
- `agent-loop-orchestrator` may coordinate long-running or multi-step replacement experiments but is not required for direct agent work.
- domain-neutral Rust foundations such as `moenarch-foundation` are appropriate homes for reusable native kernels that survive a successful experiment.

## Consequences

Docker Compose services and external libraries remain first-class bootstrap choices. Their presence does not imply a future rewrite commitment.

Applications gain an explicit path for reducing boundaries and general-purpose dependency cost where measurements justify it. Coding agents can explore specialized alternatives cheaply while correctness and performance gates prevent generated implementation volume from becoming unreviewed architectural complexity.

Over time, successful experiments can accumulate into a reusable native foundation without requiring the landscape to predict every future capability up front.
