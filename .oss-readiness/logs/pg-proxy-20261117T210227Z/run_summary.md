# oss-readiness Run Summary

**Repository**: /home/bits/go/src/github.com/DataDog/pg-proxy
**Started**: 2026-11-17T21:02:27.274504
**Finished**: 2026-11-17T21:02:29.341846

## Results

| Status | Count |
|--------|-------|
| ✅ Pass | 19 |
| ❌ Fail | 1 |
| ⚠️ Warn | 4 |
| ⏭ Skip | 0 |
| 💥 Error | 0 |
| 🔄 Deferred | 0 |
| 👁 Acknowledged | 0 |

**Total checks**: 24

## Findings

### ⚠️ [SHOULD] Missing CONTRIBUTING file

**Evidence**: No CONTRIBUTING.md found in root or .github/.

**Suggested fix**: Add a CONTRIBUTING.md. Template available at https://github.com/DataDog/ospo-templates

### ⚠️ [SHOULD] Missing GitHub PR template

**Evidence**: No .github/PULL_REQUEST_TEMPLATE.md found.

**Suggested fix**: Add .github/PULL_REQUEST_TEMPLATE.md to guide contributors.

### ⚠️ [SHOULD] Missing GitHub Issue template

**Evidence**: No .github/ISSUE_TEMPLATE/ directory or ISSUE_TEMPLATE.md found.

**Suggested fix**: Add at least one issue template under .github/ISSUE_TEMPLATE/.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: cmd/pg-proxy/config_test.go:23 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: cmd/pg-proxy/auth_config_test.go:49 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/listener/startup_error_test.go:64 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/listener/startup_error_test.go:172 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/scram/scram_test.go:157 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/scram/scram_test.go:179 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/pool/config_test.go:20 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/config/credentials_test.go:68 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/config/credentials_test.go:69 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ❌ [MUST] Potential secret: Generic token

**Evidence**: pkg/link/session_link_test.go:150 — matched pattern 'Generic token'

**Suggested fix**: Remove the secret, rotate it immediately, and use environment variables or a secrets manager instead. If this is a known test/mock value, use the acknowledgement system: run 'oss-readiness acknowledge' with the finding's fingerprint to record it as a known false positive.

### ⚠️ [SHOULD] License header missing in 1 sampled file(s)

**Evidence**: Files without header (sample): docs/site/src/content.config.ts

**Suggested fix**: Add header: "Unless explicitly stated otherwise all files in this repository are licensed under the $license License. This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright $year Datadog, Inc."


## ❌ Not ready — blocking issues found

Log directory: `/home/bits/go/src/github.com/DataDog/pg-proxy/.oss-readiness/logs/pg-proxy-20261117T210227Z`