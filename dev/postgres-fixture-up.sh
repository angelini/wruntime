#!/usr/bin/env bash
set -Eeuo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo"
# shellcheck source=shared-dev-state.sh
source "$repo/dev/shared-dev-state.sh"
# shellcheck source=postgres-provisioner-image.sh
source "$repo/dev/postgres-provisioner-image.sh"
if [ "${DOTGEN_PI_SANDBOX:-0}" = 1 ]; then
  echo "PostgreSQL fixture preparation is host-only; run 'just dev-up' on the Docker-capable host" >&2
  exit 2
fi
[ "$#" -eq 0 ] || { echo "usage: dev/postgres-fixture-up.sh" >&2; exit 2; }
# Read-only host preflight precedes the lock-file write, destructive Compose,
# PKI/publication state, and compilation. It never changes the Rust toolchain.
wrt_detect_daemon_target
wrt_require_installed_musl_target
wrt_acquire_postgres_fixture_lock "host PostgreSQL fixture preparation"
required_source="$(wrt_fixture_source_digest)"
preparing=false
on_error() {
  local status=$?
  trap - ERR
  if [ "$preparing" = true ]; then
    rm -f "$WRT_POSTGRES_READY_FILE" "$WRT_POSTGRES_OWNER_FILE"
    echo "worktree PostgreSQL preparation failed; no generation is ready" >&2
    echo "rerun 'just dev-up' from $repo" >&2
  fi
  exit "$status"
}
trap on_error ERR

compose_current() {
  local files=(-f "$repo/docker-compose.yml")
  [ ! -f "$WRT_POSTGRES_COMPOSE_OVERRIDE" ] || files+=(-f "$WRT_POSTGRES_COMPOSE_OVERRIDE")
  WRT_WORKTREE_DEV_STATE_ROOT="$WRT_WORKTREE_DEV_STATE_ROOT" docker compose \
    --project-name "$WRT_COMPOSE_PROJECT_NAME" --project-directory "$repo" \
    "${files[@]}" "$@"
}
verify_active_local_images() {
  local record="$1" tag expected_image expected_base
  readarray -t values < <(python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(r["provisioner_image_tag"]); print(r["provisioner_image_id"]); print(r["base_postgres_image_id"])' "$record")
  tag="${values[0]:-}"; expected_image="${values[1]:-}"; expected_base="${values[2]:-}"
  [ "$(docker image inspect --format '{{.Id}}' "$tag")" = "$expected_image" ] || { echo "active provisioner image identity mismatch: $tag" >&2; return 1; }
  [ "$(docker image inspect --format '{{.Id}}' postgres:18-alpine)" = "$expected_base" ] || { echo 'active PostgreSQL base image identity mismatch' >&2; return 1; }
}
provenance_matches_record() {
  python3 - "$1" "$2" <<'PY'
import json, sys
actual, record = (json.load(open(path)) for path in sys.argv[1:])
for key, value in actual.items():
    if record.get(key) != value:
        raise SystemExit(f'provisioner provenance mismatch for {key}: active={record.get(key)!r} required={value!r}')
PY
}
prepare_buckets() {
  local attempt bucket endpoint="http://127.0.0.1:${WRT_S3_PORT}"
  for attempt in $(seq 1 30); do
    if AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin aws --endpoint-url "$endpoint" s3api list-buckets >/dev/null 2>&1; then break; fi
    [ "$attempt" -lt 30 ] || { echo "RustFS did not become ready" >&2; return 1; }; sleep 1
  done
  for bucket in test-bucket stockmarket codegen; do
    AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin aws --endpoint-url "$endpoint" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1 || \
      AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin aws --endpoint-url "$endpoint" s3 mb "s3://$bucket" >/dev/null
  done
}

reuse_active_generation() {
  local reuse_provenance reuse_override
  [ -f "$WRT_POSTGRES_OWNER_FILE" ] && [ -f "$WRT_POSTGRES_READY_FILE" ] || return 1
  wrt_require_compatible_fixture || return 1
  verify_active_local_images "$WRT_POSTGRES_OWNER_FILE" || return 1
  reuse_provenance="$(mktemp "$WRT_WORKTREE_DEV_STATE_ROOT/.provisioner-reuse.XXXXXX.json")" || return 1
  reuse_override="$reuse_provenance.compose.yml"
  if ! wrt_prepare_postgres_provisioner_image "$repo" "$WRT_WORKTREE_DEV_STATE_ROOT" "$reuse_provenance" "$reuse_override" \
      || ! provenance_matches_record "$reuse_provenance" "$WRT_POSTGRES_OWNER_FILE"; then
    rm -f "$reuse_provenance" "$reuse_override"
    return 1
  fi
  mv "$reuse_override" "$WRT_POSTGRES_COMPOSE_OVERRIDE" || return 1
  rm -f "$reuse_provenance" || return 1
  wrt_compose up -d --wait || return 1
  prepare_buckets || return 1
}

if reuse_active_generation; then
  echo "worktree PostgreSQL fixture reused: $WRT_POSTGRES_READY_FILE"
  exit 0
fi

preparing=true
echo "CONVERGING worktree project $WRT_COMPOSE_PROJECT_NAME: incompatible or partial local state will be replaced" >&2
# A removed/recreated Git worktree can reuse its deterministic project identity.
# Clear only this worktree's project, volumes, PKI, and fixture before publication.
compose_current down -v --remove-orphans
rm -rf "$WRT_POSTGRES_PKI_DIR" "$WRT_POSTGRES_FIXTURE_DIR"
rm -f "$WRT_POSTGRES_OWNER_FILE" "$WRT_POSTGRES_COMPOSE_OVERRIDE"

preparing=true
mkdir -p "$WRT_WORKTREE_DEV_STATE_ROOT" "$repo/dev/observability/data"
stage="$(mktemp -d "$WRT_WORKTREE_DEV_STATE_ROOT/.prepare.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
trap 'rm -f "$WRT_POSTGRES_READY_FILE" "$WRT_POSTGRES_OWNER_FILE"; exit 130' INT
trap 'rm -f "$WRT_POSTGRES_READY_FILE" "$WRT_POSTGRES_OWNER_FILE"; exit 143' TERM

provenance="$stage/provisioner-provenance.json"
wrt_prepare_postgres_provisioner_image "$repo" "$WRT_WORKTREE_DEV_STATE_ROOT" "$provenance"
bash dev/postgres-native-certs.sh
compose_current up -d --wait
expected_base_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base_postgres_image_id"])' "$provenance")"
postgres_container="$(compose_current ps -q postgres)"
if [ -z "$postgres_container" ] || [ "$(docker inspect --format '{{.Image}}' "$postgres_container")" != "$expected_base_id" ]; then
  echo 'running PostgreSQL container does not match recorded base image' >&2
  exit 1
fi
mkdir -p "$stage/config"
cp examples/ecommerce/engine-inventory-1.toml "$stage/config/ecommerce.toml"
cp examples/stockmarket/engine-exchange.toml "$stage/config/stock-exchange.toml"
cp examples/stockmarket/engine-ledger.toml "$stage/config/stock-ledger.toml"
cp examples/codegen/engine.toml "$stage/config/codegen.toml"

cargo run --quiet -p wr-tests --example postgres_tenant_native -- \
  --output "$stage/data" \
  --contract-out "$stage/expected-contract.json" \
  --server-ca "$WRT_POSTGRES_PKI_DIR/root/ca.crt" \
  --client-cert "$WRT_POSTGRES_PKI_DIR/node-a/leaf.pem" \
  --client-key "$WRT_POSTGRES_PKI_DIR/node-a/key.pem" \
  --config "$stage/config/ecommerce.toml" \
  --config "$stage/config/stock-exchange.toml" \
  --config "$stage/config/stock-ledger.toml" \
  --config "$stage/config/codegen.toml"

bash dev/postgres-provision-native.sh "$stage/data" "$repo" >"$stage/provision-receipt.json"
python3 - "$stage/expected-contract.json" "$stage/provision-receipt.json" <<'PY'
import json, sys
expected, receipt = (json.load(open(path)) for path in sys.argv[1:])
if expected['provisioning_manifest_digest'] != receipt['manifest_digest']:
    raise SystemExit('provisioning receipt does not match computed desired manifest digest')
PY
compose_current --profile postgres-tools run --rm \
  -v "$stage/data:/work:ro" postgres-provisioner migrate \
  --manifest /work/provisioning.toml \
  --bundle-manifest /work/migration-bundle.toml \
  --bundle-root /work/migrations \
  --admin-url-file /var/lib/postgresql/wruntime-config/admin-url

admin_ssl="postgres://postgres:wruntime-dev-admin@127.0.0.1:${WRT_POSTGRES_PORT}/wruntime_test?sslmode=verify-full&sslrootcert=$WRT_POSTGRES_PKI_DIR/root/ca.crt"
psql "$admin_ssl" -XAtqc "SELECT current_setting('server_version_num')::int / 10000 = 18 AND current_setting('ssl') = 'on' AND (SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid())" | grep -Fx t >/dev/null
psql "$admin_ssl" -XAtqc "SELECT min(rule_number) < (SELECT min(rule_number) FROM pg_hba_file_rules WHERE type IN ('host','hostssl') AND auth_method='scram-sha-256') FROM pg_hba_file_rules WHERE type='hostssl' AND user_name::text LIKE '%+wr__tenant_client_auth%' AND auth_method='cert' AND options::text LIKE '%map=wruntime_nodes%'" | grep -Fx t >/dev/null
psql "$admin_ssl" -XAtqc "SELECT count(*) = 0 FROM pg_hba_file_rules WHERE user_name::text LIKE '%+wr__tenant_client_auth%' AND auth_method IN ('trust','password','md5','scram-sha-256')" | grep -Fx t >/dev/null
psql "$admin_ssl" -XAtqc "SELECT count(*) = 6 AND bool_and(error IS NULL) FROM pg_ident_file_mappings WHERE map_name='wruntime_nodes'" | grep -Fx t >/dev/null
psql "$admin_ssl" -XAtqc "SELECT (SELECT count(*) = 2 FROM pg_database WHERE datname IN ('wruntime_manager','wruntime_jobs')) AND (SELECT count(*) = 2 FROM pg_roles WHERE rolname IN ('wr_manager_platform','wr_jobs_platform'))" | grep -Fx t >/dev/null
ident="$(psql "$admin_ssl" -XAtqc 'SHOW ident_file')"
[ "$ident" = /var/lib/postgresql/wruntime-config/pg_ident.conf ]
compose_current exec -T postgres sh -ec 'set -- $(stat -c "%u %a" /var/lib/postgresql/wruntime-config/pg_ident.conf); [ "$1" = 70 ] && [ "$2" = 600 ]; head -n 1 /var/lib/postgresql/wruntime-config/pg_ident.conf | grep -Fx "# wruntime postgres tenant mappings v1" >/dev/null'
openssl x509 -in "$WRT_POSTGRES_PKI_DIR/server/leaf.pem" -noout -ext subjectAltName | grep -F 'DNS:postgres.internal' >/dev/null
runtime_role="$(python3 - <<'PY'
import hashlib, struct
parts = ['node-a', 'stockmarket']; h = hashlib.sha256(); domain = 'namespace-runtime-login'
h.update(struct.pack('>Q', len(domain))); h.update(domain.encode())
for part in parts: h.update(struct.pack('>Q', len(part))); h.update(part.encode())
print('wr_runtime_node_a__stockmarket_' + h.hexdigest()[:12])
PY
)"
psql "host=postgres.internal hostaddr=127.0.0.1 port=$WRT_POSTGRES_PORT user=$runtime_role dbname=wr_db_stockmarket sslmode=verify-full sslrootcert=$WRT_POSTGRES_PKI_DIR/root/ca.crt sslcert=$WRT_POSTGRES_PKI_DIR/node-a/leaf.pem sslkey=$WRT_POSTGRES_PKI_DIR/node-a/key.pem" -XAtqc 'SELECT current_user' | grep -Fx "$runtime_role" >/dev/null
while read -r namespace expected; do
  database="wr_db_${namespace//-/_}"
  actual="$(PGPASSWORD=wruntime-dev-admin psql "host=127.0.0.1 port=$WRT_POSTGRES_PORT user=postgres dbname=$database sslmode=verify-full sslrootcert=$WRT_POSTGRES_PKI_DIR/root/ca.crt" -XAtqc "SELECT count(*) FROM (SELECT DISTINCT ON(namespace,module,migration_version) state FROM wr__platform.migration_attempts ORDER BY namespace,module,migration_version,attempt DESC) latest WHERE state='succeeded'")"
  [ "$actual" = "$expected" ] || { echo "migration ledger incomplete for $namespace: expected $expected, got $actual" >&2; exit 1; }
done < <(python3 - "$stage/data/migration-bundle.toml" <<'PY'
import collections, sys, tomllib
counts = collections.Counter(row['namespace'] for row in tomllib.load(open(sys.argv[1], 'rb')).get('files', []))
for namespace in sorted(counts): print(namespace, counts[namespace])
PY
)

artifact_digest="$(wrt_fixture_artifact_digest "$stage/data")"
python3 - "$stage/data" "$stage/provision-receipt.json" "$provenance" "$stage/ready.json" \
  "$repo" "$WRT_GIT_COMMON_DIR" "$WRT_GIT_DIR" "$WRT_COMPOSE_PROJECT_NAME" "$WRT_WORKTREE_SLOT" \
  "$WRT_POSTGRES_PORT" "$WRT_S3_PORT" "$required_source" "$artifact_digest" <<'PY'
import json, pathlib, sys, tomllib
source, provision_path, provenance_path, output = map(pathlib.Path, sys.argv[1:5])
owner, common, git_dir, project, slot, postgres_port, s3_port, source_digest, artifact_digest = sys.argv[5:]
provision = json.loads(provision_path.read_text()); provenance = json.loads(provenance_path.read_text()); manifest = tomllib.loads((source/'provisioning.toml').read_text()); migrations = tomllib.loads((source/'migration-bundle.toml').read_text())
ready = {'schema_version': 3, 'owner_worktree': owner, 'git_common_dir': common, 'git_dir': git_dir,
 'worktree_slot': int(slot), 'source_digest': source_digest, 'fixture_artifact_digest': artifact_digest,
 'compose_project': project, 'postgres_image': 'postgres:18-alpine', 'postgres_image_id': provenance['base_postgres_image_id'],
 'provision_generation': manifest['generation'], 'provisioning_manifest_digest': provision['manifest_digest'],
 'migration_bundle_digest': migrations['bundle_digest'], 'successful_migrations': migrations.get('files', []),
 'postgres_major': manifest['postgres_major'], 'postgres_ca_sha256': manifest['postgres_ca_sha256'],
 'postgres_client_leaf_fingerprint': manifest['nodes'][0]['certificate_sha256'], 'server_name': 'postgres.internal',
 'host_addr': '127.0.0.1', 'port': int(postgres_port), 's3_port': int(s3_port), 'connect_timeout_secs': 10}
ready.update(provenance)
output.write_text(json.dumps(ready, sort_keys=True, indent=2)+'\n')
PY
ready_digest="sha256:$(sha256sum "$stage/ready.json" | cut -d' ' -f1)"
python3 - "$stage/data/provisioning.toml" "$stage/data/migration-bundle.toml" "$provenance" "$stage/owner.json" \
  "$repo" "$WRT_GIT_COMMON_DIR" "$WRT_GIT_DIR" "$WRT_COMPOSE_PROJECT_NAME" "$WRT_WORKTREE_SLOT" \
  "$required_source" "$artifact_digest" "$ready_digest" <<'PY'
import datetime, json, pathlib, sys, tomllib
provision, migration, provenance_path, output = map(pathlib.Path, sys.argv[1:5]); owner, common, git_dir, project, slot, source, artifact, ready = sys.argv[5:]
p = tomllib.loads(provision.read_text()); m = tomllib.loads(migration.read_text()); provenance = json.loads(provenance_path.read_text())
value = {'schema_version': 2, 'owner_worktree': owner, 'git_common_dir': common, 'git_dir': git_dir,
 'worktree_slot': int(slot), 'compose_project': project, 'source_digest': source,
 'preparation_generation': p['generation'], 'prepared_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'provisioning_manifest_digest': p.get('manifest_digest', ''), 'migration_bundle_digest': m['bundle_digest'],
 'fixture_artifact_digest': artifact, 'ready_artifact_digest': ready}
value.update(provenance)
# normalized digest is supplied by the provision receipt through ready; owner is patched below before publication.
output.write_text(json.dumps(value, sort_keys=True, indent=2)+'\n')
PY
python3 - "$stage/owner.json" "$stage/ready.json" <<'PY'
import json, pathlib, sys
o, r = map(pathlib.Path, sys.argv[1:]); owner=json.loads(o.read_text()); ready=json.loads(r.read_text())
owner['provisioning_manifest_digest']=ready['provisioning_manifest_digest']; o.write_text(json.dumps(owner,sort_keys=True,indent=2)+'\n')
PY

rm -rf "$WRT_POSTGRES_FIXTURE_DIR"
mkdir -p "$WRT_POSTGRES_FIXTURE_DIR"
cp -a "$stage/data/." "$WRT_POSTGRES_FIXTURE_DIR/"
install -m 0644 "$stage/ready.json" "$WRT_POSTGRES_FIXTURE_DIR/.ready.json.tmp"
mv "$WRT_POSTGRES_FIXTURE_DIR/.ready.json.tmp" "$WRT_POSTGRES_READY_FILE"
install -m 0644 "$stage/owner.json" "$WRT_WORKTREE_DEV_STATE_ROOT/.owner.json.tmp"
mv "$WRT_WORKTREE_DEV_STATE_ROOT/.owner.json.tmp" "$WRT_POSTGRES_OWNER_FILE"

prepare_buckets
wrt_require_compatible_fixture
echo "worktree PostgreSQL fixture ready: $WRT_POSTGRES_READY_FILE"
