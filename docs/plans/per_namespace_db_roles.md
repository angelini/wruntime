# Plan: Per-Namespace Postgres Role Isolation

**Status:** Implemented (revised approach — manager-managed passwords instead of HMAC derivation).

## Context

A single shared `guest_url` role gets `GRANT ALL` on every module's schema across all namespaces. Any guest can use fully-qualified table names (`SELECT * FROM wr__other_ns__mod.table`) to read/write other namespaces' data.

## Implemented Approach

Instead of the original HMAC-based `WRT_DB_SECRET_KEY` design below, the implementation uses the manager's existing secret infrastructure to generate and store random per-namespace DB passwords. This eliminates the need for a separate secret key on engines:

1. Engines send `db_namespaces` in their registration request
2. The manager generates a random password per namespace (stored encrypted in `wr_secrets` under the reserved key `__db_password`), or retrieves the existing one
3. The manager returns `NamespaceDbCredential` (namespace, role, password) in the registration response
4. The engine creates the Postgres role, grants schema access, and builds namespace connection pools — the password never enters the WASM env var path
5. Secret keys prefixed with `__` are blocked in the public `SetSecret`/`ListSecrets` RPCs, preventing guests from accessing DB passwords

## Original Design (superseded)

## Design Decisions

- **Namespace = trust boundary** — modules in the same namespace share a DB role and can access each other's tables
- **Role creation**: engine-side, idempotent (`CREATE ROLE IF NOT EXISTS` + `ALTER ROLE PASSWORD`)
- **Passwords**: `HMAC-SHA256(WRT_DB_SECRET_KEY, namespace)`, hex-encoded
- **Role naming**: `wr_ns_{sanitize(namespace)}`
- **Config**: remove `guest_url` entirely; admin `url` is the only DB config
- **Worker jobs**: continue using admin pool for `wr__jobs` schema
- **Per-module `search_path`**: kept as a design convention (not a security boundary)

---

## Implementation Steps

### Step 1: Add `namespace_role()` and `derive_role_password()` to `wr-common`

**`wr-common/Cargo.toml`** — add `hmac = "0.12"` and `sha2 = "0.10"` as optional deps, gate behind `pool` feature.

**`wr-common/src/naming.rs`** — add:
```rust
pub fn namespace_role(namespace: &str) -> String {
    format!("wr_ns_{}", sanitize(namespace))
}

#[cfg(feature = "pool")]
pub fn derive_role_password(secret_key: &[u8], namespace: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).expect("HMAC accepts any key size");
    mac.update(namespace.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
```

Need `hex` dep too (or use manual hex encoding to avoid another dep — a simple `format!("{:02x}")` loop works).

Add tests for determinism, different-namespaces-differ, and special char handling.

### Step 2: Add `guest_pool_url()` to `wr-common/src/pool.rs`

```rust
pub fn guest_pool_url(admin_url: &str, role: &str, password: &str) -> String
```

Parse admin URL with simple string splitting (same pattern already used in `engine.rs:152-166`):
- Find `://`, then find `@` — replace `user:pass` between them with `role:password`
- Preserve host, port, dbname, query params

Add `build_guest_pool(admin_url, role, password, max_size)` convenience wrapper.

### Step 3: Remove `guest_url` from `DatabaseConfig`

**`wr-engine/src/config.rs:63`** — remove `pub guest_url: Option<String>`.

### Step 4: Rewrite pool construction in `EngineRunner::new()`

**`wr-engine/src/engine.rs:77-92`**

- Read `WRT_DB_SECRET_KEY` env var (error if missing and any module has `database = true`). Store as `Option<String>` field on `EngineRunner`.
- Change `db_pools: HashMap<(String, String), Arc<Pool>>` → `db_pools: HashMap<String, Arc<Pool>>` (keyed by namespace).
- Collect unique namespaces from DB-enabled modules.
- For each namespace: derive role + password, build pool via `build_guest_pool()`.
- For `max_connections`: sum per-module values within namespace (preserves total connection budget).

### Step 5: Rewrite `provision_schemas()` for per-namespace roles

**`wr-engine/src/engine.rs:142-219`**

- Collect unique namespaces from DB-enabled modules.
- For each namespace, using admin pool:
  ```sql
  DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN
      CREATE ROLE "{role}" LOGIN PASSWORD '{password}';
    END IF;
  END $$;
  ALTER ROLE "{role}" PASSWORD '{password}';
  ```
- For each module schema: same `CREATE SCHEMA IF NOT EXISTS` + `GRANT ALL ... TO "{namespace_role}"` as today, but using the namespace-specific role.
- No explicit REVOKE needed — new roles have no default access.

### Step 6: Update `resolve_module_services()`

**`wr-engine/src/engine.rs:290-296`**

Change pool lookup from `self.db_pools.get(&(namespace, name))` to `self.db_pools.get(&module_config.namespace)`.

### Step 7: Update CLI — remove `guest_db_url` from deploy tooling

**`wr-cli/src/cmd/config.rs:48`** — remove `guest_url` from CLI's DatabaseConfig.
**`wr-cli/src/cmd/config.rs:137-138`** — remove `guest_url` template logic in `to_bundle_config()`.
**`wr-cli/src/cmd/deploy_config.rs:25`** — remove `guest_db_url` field.
**`wr-cli/src/cmd/node.rs`**:
  - Remove `guest_db_url` CLI arg (line 83)
  - Remove template var logic (lines 587-589, 657-683, 994)
  - Add `db_secret_key` as new deploy parameter → injected into engine env as `WRT_DB_SECRET_KEY`

### Step 8: Update example configs

Remove `guest_url` lines from:
- `examples/config/engine.toml:16`
- `examples/ecommerce/engine-inventory-{1,2}.toml:15`
- `examples/codegen/engine.toml:15`
- `examples/stockmarket/engine-{exchange,ledger}.toml:15`

### Step 9: Update docs

**`docs/configuration.md`** — remove `guest_url` docs (lines 172, 194), document `WRT_DB_SECRET_KEY` and `wr_ns_{namespace}` convention.
**`docs/deployment.md`** — replace `guest_db_url` references (lines 53, 90, 95, 120) with `db_secret_key`.
**`CLAUDE.md`** — mention `WRT_DB_SECRET_KEY` in prerequisites if appropriate.
**`docs/plans/investigations/security_audit_untrusted_guests.md`** — update finding #1 to reflect the fix.

### Step 10: Update tests

- `wr-tests/` helpers don't use `guest_url` — no changes needed to existing tests.
- Add unit tests in `wr-common/src/naming.rs` for `namespace_role()` and `derive_role_password()`.
- Add unit test in `wr-common/src/pool.rs` for `guest_pool_url()` with various URL formats.

---

## Verification

1. `just tidy` — formatting and lints pass
2. `just test` — all existing tests pass
3. `just ecommerce-inline` — end-to-end with `WRT_DB_SECRET_KEY` set, zero warnings
4. Manual verification: connect as `wr_ns_ecommerce` role, confirm cannot `SELECT` from a schema in another namespace

## Files Modified

| File | Change |
|------|--------|
| `wr-common/Cargo.toml` | Add `hmac`, `sha2` deps |
| `wr-common/src/naming.rs` | Add `namespace_role()`, `derive_role_password()` |
| `wr-common/src/pool.rs` | Add `guest_pool_url()`, `build_guest_pool()` |
| `wr-engine/src/config.rs` | Remove `guest_url` |
| `wr-engine/src/engine.rs` | Rewrite pool creation, `provision_schemas()`, `resolve_module_services()` |
| `wr-cli/src/cmd/config.rs` | Remove `guest_url` |
| `wr-cli/src/cmd/deploy_config.rs` | Remove `guest_db_url` |
| `wr-cli/src/cmd/node.rs` | Remove `guest_db_url`, add `db_secret_key` |
| `examples/*/engine*.toml` | Remove `guest_url` lines |
| `docs/configuration.md` | Update DB config docs |
| `docs/deployment.md` | Replace `guest_db_url` with `db_secret_key` |
