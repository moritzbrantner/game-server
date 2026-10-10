# Agent guidance

## Ownership
- Preserve the server-authoritative runtime boundaries in `README.md` and the existing architecture decisions under `docs/adr/`.
- This repository owns session/match lifecycle, tick scheduling, WebTransport adapters, snapshots, reconnect/recovery, and multi-match hosting. Games own gameplay rules; `physics-engine` owns reusable physics.
- Do not add a second gameplay, physics, or transport authority.

## Conventions and context
- Read this file and applicable nested agent instructions before editing. Apply installed `conventions.json`, `conventions.lock.json`, and `.conventions/` when present; use the current shared `coding-agent-conventions` source if not yet installed.
- Prefer `coding-tooling inspect --target <existing-path> --json` to locate task-specific context; missing or partial inspection does not waive repository rules.

## Validation
- `.coding-tooling.json` declares the canonical Rust formatting, lint, test, and build capabilities. Use `coding-tooling plan --tier fast --json` to inspect the actual commands and `coding-tooling run --tier fast --strict --json` to run them.
- During development, run the narrowest relevant Cargo tests before the full tier.
- The hosted `.github/workflows/validate.yml` includes additional benchmark comparison, explicit binaries, and release builds; green local tooling does not substitute for those checks.
- Never change snapshot compatibility, replay semantics, or performance expectations merely to silence validation.
