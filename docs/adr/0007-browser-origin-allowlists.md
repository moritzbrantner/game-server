# Hosted browser origin allowlists

Status: accepted

## Decision

`MatchHostWebTransportConfig::allowed_origins` optionally owns a validated `BrowserOriginAllowlist`. It contains 1–16 canonical serialized HTTP(S) origins, each bounded to 2,048 bytes. Origins have no path, query, fragment, credentials, or explicit default port. Matching is exact; there is no wildcard, subdomain expansion, or normalization of incoming headers.

With an allowlist, opaque and nonmatching Origin headers receive HTTP 403 before route lookup, player admission, reconnect epoch fencing, or snapshot publication. Sessions without an Origin header are admitted: browsers always send one, so the allowlist still keeps other websites out, while native clients (which send none) can join the same matches as browser players. The same policy covers static, live, and prepared hosted runtimes through their existing serving path. The policy is immutable for the serving instance and is not canonical gameplay or replay data. `None` admits every origin; browser deployments should explicitly configure their site's origin.

Amended 2026-11-12: missing Origin was previously rejected under an allowlist, which locked native clients out of browser-hosted deployments (shooter's mixed native/browser deathmatch).

Origin is a browser boundary, not client authentication. Native clients can supply a header themselves. Scoped sessions, reconnect capabilities, command sequencing, and private projections remain the authority boundaries. TLS and the consumer's HTTP creation surface must be configured independently.

Wire protocol 3 and all gameplay/recovery encodings remain unchanged. Consumers rebuilding against the new configuration field must choose their origin policy explicitly. The existing URL dependency is now declared directly for canonical origin validation.

## Evidence

Constructor tests cover missing/opaque origins, exact host/scheme matching, canonical serialization, credentials, paths, default ports, and count/byte bounds. A real loopback QUIC test rejects new and reconnect requests with wrong origins, proves rejected requests allocate no seat and cannot fence the original connection, resumes the same player with an allowed origin, and admits an origin-less (native) session. Existing unrestricted native/local hosting paths remain covered by the full suite.

Shared convention source revision: `46d8793bb3034326561f876dcc67dbaa5aa1e432`.
