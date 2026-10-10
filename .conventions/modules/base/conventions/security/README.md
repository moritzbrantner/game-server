# Security conventions

## SEC-001 — Scan secrets in risk-appropriate layers

- Pre-commit or equivalent fast checks scan staged/changed content for high-confidence secrets.
- Broader security/full tiers may scan the repository and history where that provides useful additional coverage.
- High-confidence secret findings fail immediately; known false positives use narrow documented allowlisting rather than disabling the scanner globally.

## SEC-002 — Audit dependency vulnerabilities and licenses by risk

- Security/full tiers use ecosystem-native vulnerability and license audits where practical.
- Newly introduced high/critical vulnerabilities and prohibited licenses fail deterministic verification.
- Existing unavoidable findings may use an explicit baseline that can only shrink; unresolved findings remain visible rather than triggering arbitrary dependency upgrades.

## SEC-003 — Validate data when it crosses a trust boundary

- Validate HTTP/API input, CLI input, files, messages, external-service responses, deserialized records, and other untrusted data when it enters the trusted domain.
- Convert validated input into typed/domain structures so internal code does not repeatedly re-check the same invariants.
- Validation at a boundary does not remove the need for authorization or domain-level invariant enforcement.

## SEC-004 — Verify downloaded executable/build inputs

- External tools, binaries, archives, and artifacts used in deterministic setup, build, or release paths are version-pinned and verified with a cryptographic checksum, signature, immutable release identity, or equivalent ecosystem-native integrity mechanism.
- Cached copies are acceptable only when they satisfy the same declared identity check.

## SEC-005 — Treat reusable browser state and captured sessions as sensitive

- Cookies, storage state, session tokens, authenticated profiles, HARs, traces, screenshots, and videos may contain credentials, personal data, or other sensitive information.
- Keep reusable authentication state out of version control and place it under an ignored local path by default.
- Sanitize captured artifacts before committing or broadly sharing them.
