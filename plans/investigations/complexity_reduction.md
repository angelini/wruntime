# Investigation: Complexity Reduction in wruntime

## Context

The wruntime workspace has grown across 6 crates (wr-common, wr-engine, wr-proxy, wr-manager, wr-cli, wr-tests). As functionality expanded, several patterns were copy-pasted between services rather than shared. This investigation catalogs duplication, library-replacement candidates, and complex code that could be extracted into shared, tested modules.

---

## Category 1: Duplicated Implementations Across Crates

### 1.1 Config Loading (3 identical copies)

| File | Lines |
|------|-------|
| `wr-engine/src/config.rs` | 251-259 |
| `wr-proxy/src/config.rs` | 105-113 |
| `wr-manager/src/config.rs` | 53-61 |

All three implement the exact same pattern: `read_to_string` -> `toml::from_str` -> `validate()`. Could be a generic function in `wr-common` with a `Validatable` trait.

### 1.2 Database Pool Builder (2 identical copies)

| File | Lines |
|------|-------|
| `wr-engine/src/pool.rs` | 27-36 |
| `wr-manager/src/pool.rs` | 1-12 |

Byte-for-byte identical `build_pool()` function. Should live in `wr-common`.

### 1.3 Signal Handling (3 identical copies)

| File | Lines |
|------|-------|
| `wr-engine/src/main.rs` | 220-229 |
| `wr-proxy/src/main.rs` | 149-157 |
| `wr-manager/src/main.rs` | 108-116 |

Identical SIGINT/SIGTERM `tokio::select!` block. Extract to `wr-common::shutdown_signal()`.

### 1.4 Header Extraction Helpers (2 near-identical copies)

| File | Function | Returns |
|------|----------|---------|
| `wr-engine/src/server.rs:184` | `header_owned()` | `String` |
| `wr-proxy/src/layers/tracing.rs:87` | `header_str()` | `&str` |

Same logic, one owned, one borrowed. Consolidate in `wr-common` with both variants.

### ~~1.5 Error Response Builders~~ — SKIPPED (body types differ by design)

---

## Category 2: Code Replaceable by Public Libraries

### 2.1 Custom Path Normalization -> `std::path`

**File:** `wr-engine/src/blobstore.rs:57-88`

Hand-rolled `normalize_key()` with manual segment iteration, `.`/`..` handling. Replace with `std::path::Path::components()` — no new dependency needed.

### 2.2 Custom Polling/Wait Loops -> `tokio-retry`

**File:** `wr-cli/src/cmd/helpers.rs:74-108`

Two `wait_for_*` functions with manual deadline loops. `tokio-retry` or `backon` provides this with cleaner APIs and configurable backoff.

### 2.3 Custom HTTP Path Matching -> `matchit`

**File:** `wr-proxy/src/layers/ingress.rs:108-125`

Hand-written `path_matches()` with segment splitting and `{param}` detection. Replace with `matchit` crate — production-grade radix-tree router.

### 2.4 Address Normalization -> `url` crate

**File:** `wr-cli/src/cmd/helpers.rs:11-25`

Manual `trim_start_matches("http://")` and port extraction via `rsplit(':')`. The `url` crate handles this properly. Low priority — current code is short and works.

---

## Category 3: Complex Methods to Extract

### 3.1 DB Host Trait — Repetitive Connection Boilerplate

**File:** `wr-engine/src/db.rs:321-452`

Four methods (`query`, `execute`, `query_stream`, `begin_transaction`) each repeat:
1. Check pool exists (or return `Either::Left` error)
2. Clone pool/schema/timeouts
3. Get connection -> `prepare_connection()` -> build params -> execute -> map errors

A `get_prepared_connection()` helper would eliminate ~80 lines of repeated setup.

### 3.2 Postgres Type Conversion — 120-line Match -> macro

**File:** `wr-engine/src/db.rs:618-744`

`pg_col_to_wit()` is a 120+ line match with repetitive `opt(row.get(...), PgValue::Type)` patterns. Replace with a declarative macro that generates each arm.

### 3.3 LLM SSE Stream Parser — 4 Levels of Nesting

**File:** `wr-engine/src/llm.rs:105-165`

Nested `while chunk` -> `while buf.find()` -> `for line` -> `match event_type`. Extract an `SseParser` struct that takes chunks and yields events, reducing nesting and improving testability.

### 3.4 Routing Resolution — 160-line Service::call()

**File:** `wr-proxy/src/layers/routing.rs:125-283`

Mixes URI parsing, version resolution, semver matching, egress fallback, round-robin selection, and header injection in one method. Could split into `resolve_destination()` and `select_candidate()` helpers.

### 3.5 Engine Startup Sequence — 140-line main()

**File:** `wr-engine/src/main.rs:28-166`

Sequential operations (schema provisioning, descriptor building, secret resolution, module loading) in one long block. Could be split into named phases but this is a matter of style — the code is sequential by nature.

---

## Recommended Actions (Prioritized)

### High Impact / Low Risk — Deduplicate to `wr-common`

1. **Move `build_pool()` to `wr-common`** — identical code, zero behavior change
2. **Extract `shutdown_signal()` to `wr-common`** — identical code, zero behavior change
3. **Extract generic `load_config()` to `wr-common`** — add a `Validatable` trait, keep per-service `validate()` methods
4. **Extract `header_str()` / `header_owned()` to `wr-common`** — near-identical helpers

### Medium Impact / Low Risk — Internal Extractions

5. **Extract `get_prepared_connection()` in `wr-engine/src/db.rs`** — reduces 4 methods from ~30 lines each to ~10
6. **Extract `SseParser` from `wr-engine/src/llm.rs`** — improves testability, reduces nesting
7. **PG type conversion macro in `wr-engine/src/db.rs`** — replace 120-line match with declarative macro

### Medium Impact / Medium Risk — Library Replacements

8. **Replace `normalize_key()` with `std::path`** in `wr-engine/src/blobstore.rs` — no new dep
9. **Replace `path_matches()` with `matchit`** in `wr-proxy/src/layers/ingress.rs` — new dep
10. **Replace CLI wait loops with `tokio-retry`** in `wr-cli/src/cmd/helpers.rs` — new dep

### Medium Impact / Medium Risk — Refactors

11. **Split `RoutingService::call()` into sub-functions** — large method but well-tested; refactor carefully

### Dropped

- ~~1.5 Error response builders~~ — body types differ by design
- ~~2.4 Address normalization~~ — too small to matter

---

## Verification

After each refactoring step:
```bash
just tidy                # formatting + clippy
just test                # all unit tests
just test-wasm           # host binding tests (if db.rs/blobstore.rs touched)
just ecommerce-inline    # end-to-end (zero WARN lines)
```
