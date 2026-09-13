#!/usr/bin/env bash
# Canonical cross-worktree development state, fixture compatibility, and lock helpers.
# This file is sourced; callers must not provide state/project overrides.

wrt_shared_state_init() {
  local repo common
  repo="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "cannot locate wruntime Git worktree" >&2
    return 1
  }
  repo="$(realpath -e "$repo")"
  common="$(git -C "$repo" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" || {
    echo "cannot locate wruntime common Git directory" >&2
    return 1
  }
  common="$(realpath -e "$common")" || {
    echo "wruntime common Git directory is unavailable" >&2
    return 1
  }
  WRT_REPO_ROOT="$repo"
  WRT_GIT_COMMON_DIR="$common"
  WRT_SHARED_DEV_STATE_ROOT="$common/wruntime-dev-state"
  WRT_POSTGRES_FIXTURE_DIR="$WRT_SHARED_DEV_STATE_ROOT/fixture"
  WRT_POSTGRES_PKI_DIR="$WRT_SHARED_DEV_STATE_ROOT/pki"
  WRT_POSTGRES_OWNER_FILE="$WRT_SHARED_DEV_STATE_ROOT/owner.json"
  WRT_POSTGRES_READY_FILE="$WRT_POSTGRES_FIXTURE_DIR/ready.json"
  WRT_POSTGRES_LOCK_FILE="$WRT_SHARED_DEV_STATE_ROOT/locks/postgres-fixture.lock"
  WRT_POSTGRES_COMPOSE_OVERRIDE="$WRT_SHARED_DEV_STATE_ROOT/compose-provisioner.generated.yml"
  WRT_COMPOSE_PROJECT_NAME="wruntime-dev"
}

wrt_hash_fixture_inputs() {
  local root="$1" manifest="$2"
  python3 - "$root" "$manifest" <<'PY'
import hashlib, pathlib, sys
root, manifest = map(pathlib.Path, sys.argv[1:])
entries = []
for raw in manifest.read_text().splitlines():
    name = raw.strip()
    if not name or name.startswith('#'):
        continue
    path = root / name
    if not path.exists():
        raise SystemExit(f"fixture input is missing: {name}")
    if path.is_dir():
        entries.extend(p for p in path.rglob('*') if p.is_file())
    else:
        entries.append(path)
h = hashlib.sha256()
for path in sorted(set(entries), key=lambda p: p.relative_to(root).as_posix()):
    name = path.relative_to(root).as_posix().encode()
    data = path.read_bytes()
    h.update(len(name).to_bytes(8, 'big')); h.update(name)
    h.update(len(data).to_bytes(8, 'big')); h.update(data)
print('sha256:' + h.hexdigest())
PY
}

wrt_fixture_source_digest() {
  wrt_hash_fixture_inputs "$WRT_REPO_ROOT" "$WRT_REPO_ROOT/dev/postgres-fixture-inputs.txt"
}

wrt_fixture_artifact_digest() {
  local fixture="${1:-$WRT_POSTGRES_FIXTURE_DIR}"
  python3 - "$fixture" <<'PY'
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
paths = sorted((p for p in root.rglob('*') if p.is_file() and p.name != 'ready.json'), key=lambda p: p.relative_to(root).as_posix())
h = hashlib.sha256()
for path in paths:
    name = path.relative_to(root).as_posix().encode(); data = path.read_bytes()
    h.update(len(name).to_bytes(8, 'big')); h.update(name)
    h.update(len(data).to_bytes(8, 'big')); h.update(data)
print('sha256:' + h.hexdigest())
PY
}

wrt_acquire_postgres_fixture_lock() {
  local context="${1:-database-sensitive command}" timeout_seconds="${2:-300}" canonical_inode inherited_inode holder
  command -v flock >/dev/null 2>&1 || { echo "missing required command: flock" >&2; return 127; }
  mkdir -p "$(dirname "$WRT_POSTGRES_LOCK_FILE")" || {
    echo "shared development state is not writable: $WRT_SHARED_DEV_STATE_ROOT" >&2
    return 1
  }
  : >>"$WRT_POSTGRES_LOCK_FILE" || {
    echo "shared PostgreSQL lock is unavailable: $WRT_POSTGRES_LOCK_FILE" >&2
    return 1
  }
  canonical_inode="$(stat -Lc '%d:%i' "$WRT_POSTGRES_LOCK_FILE")"
  if [ -n "${WRT_POSTGRES_FIXTURE_LOCK_FD:-}" ]; then
    case "$WRT_POSTGRES_FIXTURE_LOCK_FD" in *[!0-9]*|'') echo "invalid inherited PostgreSQL lock descriptor" >&2; return 1;; esac
    [ -e "/proc/self/fd/$WRT_POSTGRES_FIXTURE_LOCK_FD" ] || { echo "inherited PostgreSQL lock descriptor is closed" >&2; return 1; }
    inherited_inode="$(stat -Lc '%d:%i' "/proc/self/fd/$WRT_POSTGRES_FIXTURE_LOCK_FD")"
    [ "$inherited_inode" = "$canonical_inode" ] || {
      echo "inherited PostgreSQL lock descriptor does not name the canonical shared lock inode" >&2
      return 1
    }
    flock -n "$WRT_POSTGRES_FIXTURE_LOCK_FD" || { echo "inherited PostgreSQL lock is not held" >&2; return 1; }
    return 0
  fi
  exec {WRT_POSTGRES_FIXTURE_LOCK_FD}>>"$WRT_POSTGRES_LOCK_FILE" || return 1
  if ! flock -w "$timeout_seconds" "$WRT_POSTGRES_FIXTURE_LOCK_FD"; then
    holder="$(cat "$WRT_POSTGRES_LOCK_FILE" 2>/dev/null || true)"
    echo "timed out waiting for shared PostgreSQL fixture lock: $WRT_POSTGRES_LOCK_FILE" >&2
    [ -z "$holder" ] || printf 'lock owner:\n%s\n' "$holder" >&2
    return 1
  fi
  printf 'pid=%s\nworktree=%s\ncontext=%s\nstarted_at=%s\n' "$$" "$WRT_REPO_ROOT" "$context" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$WRT_POSTGRES_LOCK_FILE"
  export WRT_POSTGRES_FIXTURE_LOCK_FD
  printf 'shared PostgreSQL lock: %s\n' "$WRT_POSTGRES_LOCK_FILE"
}

wrt_owner_worktree() {
  python3 - "$WRT_POSTGRES_OWNER_FILE" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['owner_worktree'])
PY
}

wrt_validate_fixture_records() {
  local owner_path="$1" ready_path="$2" common="$3" source="$4" artifact="$5" ready_digest="$6" override_path="${7:-$WRT_POSTGRES_COMPOSE_OVERRIDE}"
  python3 - "$owner_path" "$ready_path" "$common" "$source" "$artifact" "$ready_digest" "$override_path" <<'PY'
import json, pathlib, sys
owner_path, ready_path = map(pathlib.Path, sys.argv[1:3])
common, source, artifact, ready_digest, override_path = sys.argv[3:]
o, r = json.loads(owner_path.read_text()), json.loads(ready_path.read_text())
def fail(message): raise SystemExit(message)
if o.get('schema_version') != 1 or r.get('schema_version') != 2: fail('unsupported shared PostgreSQL coordination schema')
if o.get('git_common_dir') != common: fail('shared PostgreSQL owner common-directory identity mismatch')
if o.get('compose_project') != 'wruntime-dev' or r.get('compose_project') != 'wruntime-dev': fail('shared PostgreSQL Compose identity mismatch')
if o.get('source_digest') != source or r.get('source_digest') != source: fail('shared PostgreSQL source binding mismatch')
if o.get('fixture_artifact_digest') != artifact or r.get('fixture_artifact_digest') != artifact: fail('shared PostgreSQL artifact digest mismatch; run host `just dev-reprepare`')
if o.get('ready_artifact_digest') != ready_digest: fail('shared PostgreSQL ready digest mismatch; run host `just dev-reprepare`')
if r.get('owner_worktree') != o.get('owner_worktree'): fail('shared PostgreSQL owner binding mismatch')
if r.get('provisioning_manifest_digest') != o.get('provisioning_manifest_digest'): fail('shared PostgreSQL provisioning digest mismatch')
if r.get('migration_bundle_digest') != o.get('migration_bundle_digest'): fail('shared PostgreSQL migration digest mismatch')
provenance = ('daemon_architecture', 'daemon_platform', 'rust_target', 'cargo_version', 'rustc_version',
 'cargo_zigbuild_version', 'zig_version', 'wr_cli_binary_sha256', 'minimal_context_sha256',
 'provisioner_image_tag', 'provisioner_image_id', 'base_postgres_image_id', 'image_smoke_passed')
for key in provenance:
    if not o.get(key) or o.get(key) != r.get(key): fail(f'shared PostgreSQL provisioner {key} binding mismatch')
if (r['daemon_architecture'], r['daemon_platform'], r['rust_target']) not in (
 ('amd64', 'linux/amd64', 'x86_64-unknown-linux-musl'), ('arm64', 'linux/arm64', 'aarch64-unknown-linux-musl')):
    fail('shared PostgreSQL daemon/target mapping is invalid')
for key in ('wr_cli_binary_sha256', 'minimal_context_sha256', 'provisioner_image_id', 'base_postgres_image_id'):
    if not r[key].startswith('sha256:'): fail(f'shared PostgreSQL {key} is invalid')
if r['image_smoke_passed'] is not True: fail('shared PostgreSQL provisioner smoke evidence is absent')
context_hex = r['minimal_context_sha256'].removeprefix('sha256:')
if r['provisioner_image_tag'] != f'wruntime-dev-postgres-provisioner:{context_hex}': fail('shared PostgreSQL provisioner tag/context mismatch')
override_path = pathlib.Path(override_path); context = override_path.parent/'build/provisioner'/context_hex
entries = list(context.iterdir()) if context.is_dir() else []
if sorted(p.name for p in entries) != ['Dockerfile', 'wr-cli'] or any(p.is_symlink() or not p.is_file() for p in entries):
    fail('shared PostgreSQL provisioner context is incomplete')
import hashlib, stat
if stat.S_IMODE((context/'wr-cli').stat().st_mode) != 0o755: fail('shared PostgreSQL provisioner binary mode mismatch')
h=hashlib.sha256()
for name in ('Dockerfile','wr-cli'):
    data=(context/name).read_bytes(); encoded=name.encode(); h.update(len(encoded).to_bytes(8,'big')); h.update(encoded); h.update(len(data).to_bytes(8,'big')); h.update(data)
if h.hexdigest() != context_hex: fail('shared PostgreSQL provisioner context digest mismatch')
if 'sha256:'+hashlib.sha256((context/'wr-cli').read_bytes()).hexdigest() != r['wr_cli_binary_sha256']:
    fail('shared PostgreSQL provisioner binary digest mismatch')
override = override_path.read_text()
if f"image: {r['provisioner_image_tag']}\n" not in override or 'pull_policy: never\n' not in override:
    fail('shared PostgreSQL provisioner Compose override binding mismatch')
PY
}

wrt_require_compatible_fixture() {
  local required active owner owner_current artifact ready_digest compatibility generation
  required="$(wrt_fixture_source_digest)" || return
  if [ ! -f "$WRT_POSTGRES_OWNER_FILE" ] || [ ! -f "$WRT_POSTGRES_READY_FILE" ]; then
    echo "shared PostgreSQL fixture is not ready under $WRT_SHARED_DEV_STATE_ROOT" >&2
    echo "run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  active="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_digest"])' "$WRT_POSTGRES_OWNER_FILE")" || return
  owner="$(wrt_owner_worktree)" || return
  if [ ! -d "$owner/.git" ] && [ ! -f "$owner/.git" ]; then
    printf 'shared PostgreSQL fixture owner worktree is unavailable\nowner: %s\nactive: %s\nrequired: %s\n' "$owner" "$active" "$required" >&2
    echo "run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  if [ ! -f "$owner/dev/postgres-fixture-inputs.txt" ]; then
    echo "shared PostgreSQL fixture owner input manifest is unavailable: $owner" >&2
    echo "run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  owner_current="$(wrt_hash_fixture_inputs "$owner" "$owner/dev/postgres-fixture-inputs.txt")" || return
  if [ "$owner_current" != "$active" ]; then
    printf 'shared PostgreSQL fixture owner source changed after preparation\nowner: %s\nactive: %s\nowner-current: %s\nrequired: %s\n' "$owner" "$active" "$owner_current" "$required" >&2
    echo "coordinate users, then run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  if [ "$active" != "$required" ]; then
    printf 'shared PostgreSQL fixture source mismatch\nowner: %s\nactive: %s\nrequired: %s\n' "$owner" "$active" "$required" >&2
    echo "coordinate users, then run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  artifact="$(wrt_fixture_artifact_digest)" || return
  ready_digest="sha256:$(sha256sum "$WRT_POSTGRES_READY_FILE" | cut -d' ' -f1)"
  wrt_validate_fixture_records "$WRT_POSTGRES_OWNER_FILE" "$WRT_POSTGRES_READY_FILE" \
    "$WRT_GIT_COMMON_DIR" "$active" "$artifact" "$ready_digest" || {
    printf 'shared PostgreSQL fixture coordination verification failed\nowner: %s\nactive: %s\nrequired: %s\n' "$owner" "$active" "$required" >&2
    echo "coordinate users, then run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  }
  cargo run --quiet --manifest-path "$WRT_REPO_ROOT/Cargo.toml" -p wr-tests \
    --example postgres_tenant_consume -- --verify-only --fixture "$WRT_POSTGRES_FIXTURE_DIR" || {
    printf 'shared PostgreSQL fixture manifest/PKI verification failed\nowner: %s\nactive: %s\nrequired: %s\n' "$owner" "$active" "$required" >&2
    echo "coordinate users, then run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  }
  compatibility="$(mktemp -d "$WRT_SHARED_DEV_STATE_ROOT/.compatibility.XXXXXX")" || return
  mkdir -p "$compatibility/config"
  cp "$WRT_REPO_ROOT/examples/ecommerce/engine-inventory-1.toml" "$compatibility/config/ecommerce.toml"
  cp "$WRT_REPO_ROOT/examples/stockmarket/engine-exchange.toml" "$compatibility/config/stock-exchange.toml"
  cp "$WRT_REPO_ROOT/examples/stockmarket/engine-ledger.toml" "$compatibility/config/stock-ledger.toml"
  cp "$WRT_REPO_ROOT/examples/codegen/engine.toml" "$compatibility/config/codegen.toml"
  generation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["provision_generation"])' "$WRT_POSTGRES_READY_FILE")"
  if ! cargo run --quiet --manifest-path "$WRT_REPO_ROOT/Cargo.toml" -p wr-tests \
      --example postgres_tenant_native -- \
      --output "$compatibility/data" --contract-out "$compatibility/expected.json" --generation "$generation" \
      --server-ca "$WRT_POSTGRES_PKI_DIR/root/ca.crt" --client-cert "$WRT_POSTGRES_PKI_DIR/node-a/leaf.pem" \
      --client-key "$WRT_POSTGRES_PKI_DIR/node-a/key.pem" \
      --config "$compatibility/config/ecommerce.toml" --config "$compatibility/config/stock-exchange.toml" \
      --config "$compatibility/config/stock-ledger.toml" --config "$compatibility/config/codegen.toml"; then
    rm -rf "$compatibility"
    echo "failed to compute desired shared PostgreSQL manifests; run host 'just dev-reprepare'" >&2
    return 1
  fi
  if ! python3 - "$compatibility/expected.json" "$WRT_POSTGRES_READY_FILE" <<'PY'
import json, sys
expected, ready = (json.load(open(path)) for path in sys.argv[1:])
for key in ('provision_generation', 'provisioning_manifest_digest', 'migration_bundle_digest'):
    if expected[key] != ready[key]:
        raise SystemExit(f"shared PostgreSQL desired {key} mismatch: active={ready[key]} required={expected[key]}")
PY
  then
    rm -rf "$compatibility"
    printf 'shared PostgreSQL desired manifest mismatch\nowner: %s\nactive: %s\nrequired: %s\n' "$owner" "$active" "$required" >&2
    echo "coordinate users, then run 'just dev-reprepare' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  rm -rf "$compatibility"
}

wrt_compose() {
  local owner
  owner="$(wrt_owner_worktree)" || {
    echo "shared fixture owner is unavailable; run 'just dev-reprepare'" >&2
    return 1
  }
  [ -f "$owner/docker-compose.yml" ] || { echo "owner Compose source is unavailable: $owner" >&2; return 1; }
  [ -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" ] || { echo "shared fixture provisioner Compose override is unavailable: $WRT_POSTGRES_COMPOSE_OVERRIDE" >&2; return 1; }
  WRT_SHARED_DEV_STATE_ROOT="$WRT_SHARED_DEV_STATE_ROOT" docker compose --project-name "$WRT_COMPOSE_PROJECT_NAME" --project-directory "$owner" \
    -f "$owner/docker-compose.yml" -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" "$@"
}

wrt_shared_state_init
