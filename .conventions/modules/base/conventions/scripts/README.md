# Script conventions

## SCRIPTS-001 — Scripts are idempotent and self-verifying by default

- Canonical setup, bootstrap, generation, cache-population, installation, and maintenance scripts are safe to run repeatedly and converge on the same valid state.
- If a script has multiple steps, make each step idempotent where practical.
- Detect already-valid output/state and become a no-op rather than repeating expensive work merely because the command was run again.
- When safe convergence is impossible, fail before destructive or partial mutation rather than relying on a one-time-run assumption.

## SCRIPTS-002 — Use language-appropriate shell strictness

- Bash scripts use `set -euo pipefail` and pass ShellCheck by default.
- Portable `sh` scripts use only strict options supported by the declared shell and still pass ShellCheck.
- Handle commands that are expected to fail explicitly; do not weaken an entire script with broad `|| true` or equivalent patterns.

## SCRIPTS-003 — Guard destructive filesystem operations

- Resolve and validate deletion/overwrite targets before destructive mutation.
- Refuse filesystem roots, home/workspace parents, and paths outside the command's declared boundary.
- Prefer deleting known disposable directories over accepting arbitrary recursive-deletion paths; broad destructive behavior requires explicit intent.
