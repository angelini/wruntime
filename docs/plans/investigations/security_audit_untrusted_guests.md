# Security Audit: Untrusted Guest Code Threat Model

## Context

This audit assumes guest WASM modules are untrusted and evaluates what damage a malicious guest can do within the wruntime sandbox. Wasmtime itself is assumed secure — the focus is on host bindings, routing, secrets, and configuration that the host exposes to guests.

### Trust Model

- **Namespace = trust boundary.** Modules within the same namespace may share data (DB, blobstore, inter-module calls) and are considered part of the same trust domain.
- **Cross-namespace access must be prevented.** A module in namespace `A` must not be able to read, write, or interfere with namespace `B`.
- **Module-to-host escalation must be prevented.** Guests must not access host-level resources (TLS keys, admin DB role, S3 credentials for other namespaces, host filesystem).
- Per-module DB schema isolation (`search_path`) exists to encourage good module design, not as a security boundary.

---

## CRITICAL Findings

### 1. Cross-Namespace Database Access via Fully-Qualified Table Names

**Files:** `wr-engine/src/engine.rs:198-207`, `wr-engine/src/db.rs:60-61`

A single `guest_role` (e.g. `wr_guest`) is `GRANT ALL` on **every** module's schema across all namespaces. Schema names are predictable (`wr__{namespace}__{name}` — see `wr-common/src/naming.rs:10-11`). A malicious guest can query any namespace's data:

```sql
SELECT * FROM wr__other_namespace__their_module.users;
DELETE FROM wr__secret_namespace__payments.transactions;
```

**Impact:** Full read/write/delete access to every namespace's database tables.

**Fix:** Use per-namespace Postgres roles (`wr_ns_{namespace}`). Each role is only granted access to schemas belonging to its own namespace. Roles are auto-created during engine startup with deterministic HMAC-derived passwords. See the implementation plan for per-namespace role isolation.

### 2. Data Exfiltration via LLM Binding

**Files:** `wr-engine/src/llm.rs`, `wit/llm.wit`

Guests with LLM access can embed arbitrary data in prompts sent to the Anthropic API:

```
complete(messages: [{ role: "user", content: "<all DB data here>" }])
```

No content filtering, rate limiting, or DLP. The guest controls `messages`, `system`, `model`, and `tools` parameters.

**Impact:** Unrestricted data exfiltration to a third-party API. Combined with finding #1, a guest can read cross-namespace DB data and exfiltrate it.

**Fix:** Add per-module rate limiting and audit logging of all LLM calls. Consider restricting model selection to an allowlist. Content filtering is hard to do well but logging provides accountability.

### 3. `job_type` Used Unsafely in URI Construction

**File:** `wr-engine/src/worker.rs:377-379`

```rust
.uri(format!("http://localhost{}", job.job_type))
```

The `job_type` comes from the `wr__jobs` database table, populated via `SubmitJob` gRPC (guest-reachable). No validation on format. A malicious value like `//evil.com/path` or containing `\r\n` could cause URI parsing issues or SSRF.

**Fix:** Validate `job_type` matches `^/[a-zA-Z0-9/_-]+$` before insertion and before use.

---

## HIGH Findings

### 4. Cross-Namespace Module Calls Not Restricted

**Files:** `wr-engine/src/state.rs:47-61`, `wr-proxy/src/layers/routing.rs:86-106`

Any module can call any other module across any namespace — the proxy routes purely on the routing table with no namespace-level authorization. A malicious guest in namespace `A` can call modules in namespace `B` by targeting `http://B.service/...`.

Within a namespace this is expected. Cross-namespace is the problem.

**Fix:** Add a namespace authorization check in the proxy routing layer. Compare `x-wr-source-ns` against the destination namespace. By default, deny cross-namespace calls unless explicitly allowed by configuration (e.g. an `allowed_callers` list per namespace or module).

### 5. Bucket Name Not Validated in Blobstore

**File:** `wr-engine/src/blobstore.rs`

The guest specifies the `bucket` parameter in blobstore operations. No validation that it matches the configured bucket. If the S3 credentials have access to multiple buckets, a guest can target any of them — potentially accessing other namespaces' data or host-level storage.

**Fix:** Restrict bucket to the configured bucket name; reject requests targeting other buckets.

### 6. Guest Can Change DB Session Variables

**File:** `wr-engine/src/db.rs:373-411`

`tokio_postgres::Client::query()` uses the extended protocol (single statement only), so `;`-based injection to change `search_path` is blocked. But a guest can submit session-control commands as standalone queries:

```sql
SET statement_timeout = '0'
SET work_mem = '10GB'
```

This is a module-to-host escalation — the guest can override resource limits set by the host, potentially causing resource exhaustion on the shared database.

**Fix:** Reject SQL statements starting with `SET`, `RESET`, `DISCARD`, or other session-control commands. A simple prefix blocklist is sufficient.

---

## MEDIUM Findings

### 7. Secrets Exposed as WASI Environment Variables

**Files:** `wr-engine/src/state.rs:221-222`, `wr-engine/src/main.rs:176-209`

Secrets are decrypted by the manager, sent as plaintext over gRPC to the engine, and injected as WASI environment variables via `builder.env(key, value)`. A guest can read all secrets assigned to its module via standard WASI `environ_get()`.

A module accessing its own namespace's secrets is by design. The risks are:
- Combined with exfiltration channels (LLM, egress HTTP), a compromised module can leak its namespace's secrets externally
- Secrets are not zeroized from host memory after module init
- No audit trail for secret access

**Fix:** Consider a host-side secrets API with audit logging instead of env vars. At minimum, document that secrets are fully accessible to the module and can be exfiltrated if the module has LLM or egress access.

### 8. Proxy/Engine Can Bind to Non-Loopback (Warning Only)

**Files:** `wr-proxy/src/main.rs:114-124`, `wr-engine/src/main.rs:40-48`

Both proxy and engine internal listeners only **warn** when binding to `0.0.0.0`. If misconfigured, external attackers can send crafted requests directly to the proxy or engine, bypassing mTLS and injecting `x-wr-*` headers.

**Fix:** Make non-loopback binding a hard error, or require an explicit `allow_non_loopback = true` flag.

### 9. Destination Parsing Dot-Split Allows Extra Labels

**File:** `wr-proxy/src/layers/routing.rs:99`

`split_once('.')` on the destination host means `namespace.service.extra` parses as `ns="namespace"`, `svc="service.extra"`. This won't match routing table entries (fails safely), but the preserved `dest_uri` is passed to egress checking, creating a potential mismatch between routing and egress decisions.

**Fix:** Validate exactly two labels in destination host (reject if more than one dot).

### 10. Tracing Attributes Can Leak Data

**File:** `wr-engine/src/tracing.rs`

Guests can set arbitrary key/value attributes on OpenTelemetry spans. If traces are exported to a shared observability platform, this is a cross-namespace data exfiltration channel — a module could read data from its namespace and embed it in trace attributes visible to operators of other namespaces.

**Fix:** Prefix guest-set attributes with the namespace/module name. If trace data is namespace-isolated in the observability backend, this is less of a concern.

---

## LOW Findings

### 11. `inherit_stdio()` — Guests Write to Engine Logs

**File:** `wr-engine/src/state.rs:220`

Guests can write arbitrary content to stdout/stderr, which appears in engine logs. Log injection / log flooding across namespace boundaries if logs are shared.

### 12. Unsafe CWASM Deserialization

**File:** `wr-engine/src/engine.rs:324-343`

`unsafe { Component::deserialize_file(...) }` trusts the CWASM file on disk. If an attacker can write to that path, they can load arbitrary native code. Config-controlled and host-side only, but worth noting as a module-to-host escalation if file paths are ever guest-influenced.

### 13. Schema Name SQL Interpolation

**File:** `wr-engine/src/db.rs:61`

`SET search_path = "{schema}"` uses string interpolation. Schema names come from config (not guest-controlled), and `sanitize()` replaces non-alphanumeric chars with `_`. Low risk since config is host-controlled.

---

## What's Done Well

- **Ingress header stripping** (`wr-proxy/src/layers/ingress.rs:83-95`) — all `x-wr-*` headers stripped from external requests. Well tested.
- **Forward header stripping** (`wr-proxy/src/layers/forward.rs:66-72`) — internal headers removed before reaching engines.
- **HTTP request interception** — all guest HTTP calls are rewritten to go through the proxy. Guests cannot directly address engines.
- **Blobstore namespace scoping** (`wr-common/src/naming.rs:14-19`) — blob prefix is `wr/{namespace}/`, correctly matching the namespace trust boundary. Path traversal protection via `normalize_key` prevents escaping.
- **Parameterized SQL queries** — prevents SQL injection in query parameters.
- **Per-request WASM store isolation** — no shared mutable state between requests.
- **Ephemeral filesystem** — opt-in tempdir per request, cleaned up on drop.
- **Secrets encrypted at rest** in manager (AES-256-GCM), scoped per namespace.
- **mTLS** on all inter-service network traffic.
- **Epoch interruption + pooling allocator** — CPU and memory limits on guest execution.

---

## Priority Remediation Order

| Priority | Finding | Category | Effort |
|----------|---------|----------|--------|
| P0 | #1 Cross-namespace DB access | Cross-namespace | Medium — per-namespace Postgres roles |
| P0 | #3 job_type URI injection | Module-to-host | Low — input validation |
| P1 | #4 Cross-namespace module calls | Cross-namespace | Medium — namespace authz in proxy |
| P1 | #5 Bucket name validation | Module-to-host | Low — reject non-configured buckets |
| P1 | #6 Guest can SET session vars | Module-to-host | Low — SQL command blocklist |
| P1 | #2 LLM data exfiltration | Cross-namespace | Medium — rate limiting + audit logging |
| P2 | #8 Non-loopback binding | Module-to-host | Low — hard error |
| P2 | #9 Destination parsing | Cross-namespace | Low — validate label count |
| P2 | #7 Secrets in env vars | Cross-namespace | High — new secrets API |
| P3 | #10-13 Low findings | Various | Low each |
