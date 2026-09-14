#!/usr/bin/env bash
# Canonical per-worktree development state, fixture compatibility, endpoint, and lock helpers.
# This file is sourced; callers must not provide state, project, slot, or endpoint overrides.

wrt_claim_worktree_slot() {
  local registry="$WRT_GIT_COMMON_DIR/wruntime-dev-slots" digest start index slot claim target
  mkdir -p "$registry" || {
    echo "cannot create worktree port-slot registry: $registry" >&2
    return 1
  }
  digest="$(printf '%s' "$WRT_GIT_DIR" | sha256sum | cut -d' ' -f1)"

  for claim in "$registry"/slot-*; do
    [ -L "$claim" ] || continue
    if [ "$(readlink "$claim")" = "$WRT_GIT_DIR" ]; then
      WRT_WORKTREE_SLOT="${claim##*-}"
      return 0
    fi
  done

  start=$((16#${digest:0:8} % 128))
  for ((index = 0; index < 128; index++)); do
    slot=$(((start + index) % 128))
    claim="$registry/slot-$slot"
    if [ -L "$claim" ]; then
      target="$(readlink "$claim")"
      if [ ! -e "$target" ]; then
        rm -f "$claim"
      else
        continue
      fi
    elif [ -e "$claim" ]; then
      continue
    fi
    if ln -s "$WRT_GIT_DIR" "$claim" 2>/dev/null \
        || { [ -L "$claim" ] && [ "$(readlink "$claim")" = "$WRT_GIT_DIR" ]; }; then
      WRT_WORKTREE_SLOT="$slot"
      return 0
    fi
  done
  echo "no free wruntime development port slots remain under $registry" >&2
  return 1
}

wrt_shared_state_init() {
  local repo common git_dir identity port_base
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
  git_dir="$(git -C "$repo" rev-parse --path-format=absolute --absolute-git-dir 2>/dev/null)" || {
    echo "cannot locate wruntime worktree Git directory" >&2
    return 1
  }
  git_dir="$(realpath -e "$git_dir")" || {
    echo "wruntime worktree Git directory is unavailable" >&2
    return 1
  }
  identity="$(printf '%s' "$git_dir" | sha256sum | cut -d' ' -f1)"

  WRT_REPO_ROOT="$repo"
  WRT_GIT_COMMON_DIR="$common"
  WRT_GIT_DIR="$git_dir"
  WRT_WORKTREE_ID="${identity:0:12}"
  WRT_WORKTREE_DEV_STATE_ROOT="$git_dir/wruntime-dev-state"
  WRT_POSTGRES_FIXTURE_DIR="$WRT_WORKTREE_DEV_STATE_ROOT/fixture"
  WRT_POSTGRES_PKI_DIR="$WRT_WORKTREE_DEV_STATE_ROOT/pki"
  WRT_POSTGRES_OWNER_FILE="$WRT_WORKTREE_DEV_STATE_ROOT/owner.json"
  WRT_POSTGRES_READY_FILE="$WRT_POSTGRES_FIXTURE_DIR/ready.json"
  WRT_POSTGRES_LOCK_FILE="$WRT_WORKTREE_DEV_STATE_ROOT/locks/postgres-fixture.lock"
  WRT_POSTGRES_COMPOSE_OVERRIDE="$WRT_WORKTREE_DEV_STATE_ROOT/compose-provisioner.generated.yml"
  WRT_COMPOSE_PROJECT_NAME="wruntime-dev-$WRT_WORKTREE_ID"

  wrt_claim_worktree_slot || return
  port_base=$((12000 + WRT_WORKTREE_SLOT * 256))
  WRT_POSTGRES_PORT=$port_base
  WRT_S3_PORT=$((port_base + 1))
  WRT_S3_CONSOLE_PORT=$((port_base + 2))
  WRT_GRAFANA_PORT=$((port_base + 3))
  WRT_OTLP_GRPC_PORT=$((port_base + 4))
  WRT_OTLP_HTTP_PORT=$((port_base + 5))
  WRT_MANAGER_PORT=$((port_base + 16))
  WRT_PROXY_PORT=$((port_base + 17))
  WRT_PROXY_CONTROL_PORT=$((port_base + 18))
  WRT_PROXY_PEER_PORT=$((port_base + 19))
  WRT_SECOND_PROXY_PORT=$((port_base + 20))
  WRT_SECOND_PROXY_CONTROL_PORT=$((port_base + 21))
  WRT_SECOND_PROXY_PEER_PORT=$((port_base + 22))
  WRT_ENGINE_BASE_PORT=$((port_base + 32))
  WRT_SIMULATOR_PORT=$((port_base + 96))
  WRT_JOB_ADMIN_BASE_PORT=$((port_base + 100))
  WRT_EXTERNAL_PORT=$((port_base + 110))

  export WRT_REPO_ROOT WRT_GIT_COMMON_DIR WRT_GIT_DIR WRT_WORKTREE_ID WRT_WORKTREE_SLOT
  export WRT_WORKTREE_DEV_STATE_ROOT WRT_COMPOSE_PROJECT_NAME
  export WRT_POSTGRES_PORT WRT_S3_PORT WRT_S3_CONSOLE_PORT WRT_GRAFANA_PORT
  export WRT_OTLP_GRPC_PORT WRT_OTLP_HTTP_PORT WRT_MANAGER_PORT WRT_PROXY_PORT
  export WRT_PROXY_CONTROL_PORT WRT_PROXY_PEER_PORT WRT_SECOND_PROXY_PORT
  export WRT_SECOND_PROXY_CONTROL_PORT WRT_SECOND_PROXY_PEER_PORT WRT_ENGINE_BASE_PORT
  export WRT_SIMULATOR_PORT WRT_JOB_ADMIN_BASE_PORT WRT_EXTERNAL_PORT
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
  local context="${1:-worktree database lifecycle command}" timeout_seconds="${2:-300}" canonical_inode inherited_inode holder
  command -v flock >/dev/null 2>&1 || { echo "missing required command: flock" >&2; return 127; }
  mkdir -p "$(dirname "$WRT_POSTGRES_LOCK_FILE")" || {
    echo "worktree development state is not writable: $WRT_WORKTREE_DEV_STATE_ROOT" >&2
    return 1
  }
  : >>"$WRT_POSTGRES_LOCK_FILE" || {
    echo "worktree PostgreSQL lock is unavailable: $WRT_POSTGRES_LOCK_FILE" >&2
    return 1
  }
  canonical_inode="$(stat -Lc '%d:%i' "$WRT_POSTGRES_LOCK_FILE")"
  if [ -n "${WRT_POSTGRES_FIXTURE_LOCK_FD:-}" ]; then
    case "$WRT_POSTGRES_FIXTURE_LOCK_FD" in *[!0-9]*|'') echo "invalid inherited PostgreSQL lock descriptor" >&2; return 1;; esac
    [ -e "/proc/self/fd/$WRT_POSTGRES_FIXTURE_LOCK_FD" ] || { echo "inherited PostgreSQL lock descriptor is closed" >&2; return 1; }
    inherited_inode="$(stat -Lc '%d:%i' "/proc/self/fd/$WRT_POSTGRES_FIXTURE_LOCK_FD")"
    [ "$inherited_inode" = "$canonical_inode" ] || {
      echo "inherited PostgreSQL lock descriptor does not name this worktree's lock inode" >&2
      return 1
    }
    flock -n "$WRT_POSTGRES_FIXTURE_LOCK_FD" || { echo "inherited PostgreSQL lock is not held" >&2; return 1; }
    return 0
  fi
  exec {WRT_POSTGRES_FIXTURE_LOCK_FD}>>"$WRT_POSTGRES_LOCK_FILE" || return 1
  if ! flock -w "$timeout_seconds" "$WRT_POSTGRES_FIXTURE_LOCK_FD"; then
    holder="$(cat "$WRT_POSTGRES_LOCK_FILE" 2>/dev/null || true)"
    echo "timed out waiting for worktree PostgreSQL lifecycle lock: $WRT_POSTGRES_LOCK_FILE" >&2
    [ -z "$holder" ] || printf 'lock owner:\n%s\n' "$holder" >&2
    return 1
  fi
  printf 'pid=%s\nworktree=%s\ncontext=%s\nstarted_at=%s\n' "$$" "$WRT_REPO_ROOT" "$context" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$WRT_POSTGRES_LOCK_FILE"
  export WRT_POSTGRES_FIXTURE_LOCK_FD
  printf 'worktree PostgreSQL lifecycle lock: %s\n' "$WRT_POSTGRES_LOCK_FILE"
}

wrt_validate_fixture_records() {
  local owner_path="$1" ready_path="$2" common="$3" git_dir="$4" owner="$5" source="$6" artifact="$7" ready_digest="$8" override_path="${9:-$WRT_POSTGRES_COMPOSE_OVERRIDE}"
  python3 - "$owner_path" "$ready_path" "$common" "$git_dir" "$owner" "$WRT_COMPOSE_PROJECT_NAME" "$WRT_WORKTREE_SLOT" "$WRT_POSTGRES_PORT" "$WRT_S3_PORT" "$source" "$artifact" "$ready_digest" "$override_path" <<'PY'
import json, pathlib, sys
owner_path, ready_path = map(pathlib.Path, sys.argv[1:3])
common, git_dir, expected_owner, project, slot, postgres_port, s3_port, source, artifact, ready_digest, override_path = sys.argv[3:]
o, r = json.loads(owner_path.read_text()), json.loads(ready_path.read_text())
def fail(message): raise SystemExit(message)
if o.get('schema_version') != 2 or r.get('schema_version') != 3: fail('unsupported worktree PostgreSQL coordination schema; run host `just dev-up`')
if o.get('git_common_dir') != common or r.get('git_common_dir') != common: fail('worktree PostgreSQL common-directory identity mismatch')
if o.get('git_dir') != git_dir or r.get('git_dir') != git_dir: fail('worktree PostgreSQL Git-directory identity mismatch')
if o.get('owner_worktree') != expected_owner or r.get('owner_worktree') != expected_owner: fail('worktree PostgreSQL owner identity mismatch')
if o.get('compose_project') != project or r.get('compose_project') != project: fail('worktree PostgreSQL Compose identity mismatch')
if o.get('worktree_slot') != int(slot) or r.get('worktree_slot') != int(slot): fail('worktree PostgreSQL port-slot identity mismatch')
if r.get('host_addr') != '127.0.0.1' or r.get('port') != int(postgres_port) or r.get('s3_port') != int(s3_port): fail('worktree PostgreSQL endpoint binding mismatch')
if o.get('source_digest') != source or r.get('source_digest') != source: fail('worktree PostgreSQL source binding mismatch')
if o.get('fixture_artifact_digest') != artifact or r.get('fixture_artifact_digest') != artifact: fail('worktree PostgreSQL artifact digest mismatch; run host `just dev-up`')
if o.get('ready_artifact_digest') != ready_digest: fail('worktree PostgreSQL ready digest mismatch; run host `just dev-up`')
if r.get('provisioning_manifest_digest') != o.get('provisioning_manifest_digest'): fail('worktree PostgreSQL provisioning digest mismatch')
if r.get('migration_bundle_digest') != o.get('migration_bundle_digest'): fail('worktree PostgreSQL migration digest mismatch')
provenance = ('daemon_architecture', 'daemon_platform', 'rust_target', 'cargo_version', 'rustc_version',
 'cargo_zigbuild_version', 'zig_version', 'wr_cli_binary_sha256', 'minimal_context_sha256',
 'provisioner_image_tag', 'provisioner_image_id', 'base_postgres_image_id', 'image_smoke_passed')
for key in provenance:
    if not o.get(key) or o.get(key) != r.get(key): fail(f'worktree PostgreSQL provisioner {key} binding mismatch')
if (r['daemon_architecture'], r['daemon_platform'], r['rust_target']) not in (
 ('amd64', 'linux/amd64', 'x86_64-unknown-linux-musl'), ('arm64', 'linux/arm64', 'aarch64-unknown-linux-musl')):
    fail('worktree PostgreSQL daemon/target mapping is invalid')
for key in ('wr_cli_binary_sha256', 'minimal_context_sha256', 'provisioner_image_id', 'base_postgres_image_id'):
    if not r[key].startswith('sha256:'): fail(f'worktree PostgreSQL {key} is invalid')
if r['image_smoke_passed'] is not True: fail('worktree PostgreSQL provisioner smoke evidence is absent')
context_hex = r['minimal_context_sha256'].removeprefix('sha256:')
if r['provisioner_image_tag'] != f'wruntime-dev-postgres-provisioner:{context_hex}': fail('worktree PostgreSQL provisioner tag/context mismatch')
override_path = pathlib.Path(override_path); context = override_path.parent/'build/provisioner'/context_hex
entries = list(context.iterdir()) if context.is_dir() else []
if sorted(p.name for p in entries) != ['Dockerfile', 'wr-cli'] or any(p.is_symlink() or not p.is_file() for p in entries):
    fail('worktree PostgreSQL provisioner context is incomplete')
import hashlib, stat
if stat.S_IMODE((context/'wr-cli').stat().st_mode) != 0o755: fail('worktree PostgreSQL provisioner binary mode mismatch')
h=hashlib.sha256()
for name in ('Dockerfile','wr-cli'):
    data=(context/name).read_bytes(); encoded=name.encode(); h.update(len(encoded).to_bytes(8,'big')); h.update(encoded); h.update(len(data).to_bytes(8,'big')); h.update(data)
if h.hexdigest() != context_hex: fail('worktree PostgreSQL provisioner context digest mismatch')
if 'sha256:'+hashlib.sha256((context/'wr-cli').read_bytes()).hexdigest() != r['wr_cli_binary_sha256']:
    fail('worktree PostgreSQL provisioner binary digest mismatch')
override = override_path.read_text()
if f"image: {r['provisioner_image_tag']}\n" not in override or 'pull_policy: never\n' not in override:
    fail('worktree PostgreSQL Compose override binding mismatch')
PY
}

wrt_require_compatible_fixture() {
  local required active artifact ready_digest compatibility generation
  required="$(wrt_fixture_source_digest)" || return
  if [ ! -f "$WRT_POSTGRES_OWNER_FILE" ] || [ ! -f "$WRT_POSTGRES_READY_FILE" ]; then
    echo "worktree PostgreSQL fixture is not ready under $WRT_WORKTREE_DEV_STATE_ROOT" >&2
    echo "run 'just dev-up' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  active="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_digest"])' "$WRT_POSTGRES_OWNER_FILE")" || return
  if [ "$active" != "$required" ]; then
    printf 'worktree PostgreSQL fixture source mismatch\nworktree: %s\nactive: %s\nrequired: %s\n' "$WRT_REPO_ROOT" "$active" "$required" >&2
    echo "run 'just dev-up' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  fi
  artifact="$(wrt_fixture_artifact_digest)" || return
  ready_digest="sha256:$(sha256sum "$WRT_POSTGRES_READY_FILE" | cut -d' ' -f1)"
  wrt_validate_fixture_records "$WRT_POSTGRES_OWNER_FILE" "$WRT_POSTGRES_READY_FILE" \
    "$WRT_GIT_COMMON_DIR" "$WRT_GIT_DIR" "$WRT_REPO_ROOT" "$active" "$artifact" "$ready_digest" || {
    printf 'worktree PostgreSQL fixture coordination verification failed\nworktree: %s\nactive: %s\nrequired: %s\n' "$WRT_REPO_ROOT" "$active" "$required" >&2
    echo "run 'just dev-up' on the Docker-capable host from $WRT_REPO_ROOT" >&2
    return 1
  }
  cargo run --quiet --manifest-path "$WRT_REPO_ROOT/Cargo.toml" -p wr-tests \
    --example postgres_tenant_consume -- --verify-only --fixture "$WRT_POSTGRES_FIXTURE_DIR" || {
    echo "worktree PostgreSQL fixture manifest/PKI verification failed; run host 'just dev-up'" >&2
    return 1
  }
  compatibility="$(mktemp -d "$WRT_WORKTREE_DEV_STATE_ROOT/.compatibility.XXXXXX")" || return
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
    echo "failed to compute desired worktree PostgreSQL manifests; run host 'just dev-up'" >&2
    return 1
  fi
  if ! python3 - "$compatibility/expected.json" "$WRT_POSTGRES_READY_FILE" <<'PY'
import json, sys
expected, ready = (json.load(open(path)) for path in sys.argv[1:])
for key in ('provision_generation', 'provisioning_manifest_digest', 'migration_bundle_digest'):
    if expected[key] != ready[key]:
        raise SystemExit(f"worktree PostgreSQL desired {key} mismatch: active={ready[key]} required={expected[key]}")
PY
  then
    rm -rf "$compatibility"
    echo "worktree PostgreSQL desired manifest mismatch; run host 'just dev-up'" >&2
    return 1
  fi
  rm -rf "$compatibility"
}

wrt_export_dev_endpoints() {
  export WRT_TEST_DB_URL="postgres://postgres:wruntime-dev-admin@127.0.0.1:${WRT_POSTGRES_PORT}/wruntime_test"
  export WRT_TEST_S3_ENDPOINT="http://127.0.0.1:${WRT_S3_PORT}"
  export WRT_TEST_S3_ACCESS_KEY="rustfsadmin"
  export WRT_TEST_S3_SECRET_KEY="rustfsadmin"
  export WRT_EXAMPLE_DB_URL="postgres://wr_manager_platform:wruntime-dev-manager@127.0.0.1:${WRT_POSTGRES_PORT}/wruntime_manager"
  export WRT_JOBS_DB_URL="postgres://wr_jobs_platform:wruntime-dev-jobs@127.0.0.1:${WRT_POSTGRES_PORT}/wruntime_jobs"
  export WRT_S3_ENDPOINT="$WRT_TEST_S3_ENDPOINT"
}

wrt_compose() {
  [ -f "$WRT_REPO_ROOT/docker-compose.yml" ] || { echo "worktree Compose source is unavailable: $WRT_REPO_ROOT" >&2; return 1; }
  [ -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" ] || { echo "worktree provisioner Compose override is unavailable: $WRT_POSTGRES_COMPOSE_OVERRIDE" >&2; return 1; }
  WRT_WORKTREE_DEV_STATE_ROOT="$WRT_WORKTREE_DEV_STATE_ROOT" docker compose \
    --project-name "$WRT_COMPOSE_PROJECT_NAME" --project-directory "$WRT_REPO_ROOT" \
    -f "$WRT_REPO_ROOT/docker-compose.yml" -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" "$@"
}

wrt_shared_state_init
