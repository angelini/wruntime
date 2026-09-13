#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
# shellcheck source=shared-dev-state.sh
source "$root/dev/shared-dev-state.sh"
expected="$(realpath -e "$(git rev-parse --path-format=absolute --git-common-dir)")/wruntime-dev-state"
[ "$WRT_SHARED_DEV_STATE_ROOT" = "$expected" ] || { echo "shared-state locator mismatch" >&2; exit 1; }
from_subdir="$(cd wr-tests && bash -c 'source ../dev/shared-dev-state.sh; printf %s "$WRT_SHARED_DEV_STATE_ROOT"')"
[ "$from_subdir" = "$expected" ] || { echo "subdirectory resolved split shared state" >&2; exit 1; }
from_spoof="$(WRT_SHARED_DEV_STATE_ROOT=/tmp/split WRT_COMPOSE_PROJECT_NAME=split bash -c 'source "$1"; printf "%s|%s" "$WRT_SHARED_DEV_STATE_ROOT" "$WRT_COMPOSE_PROJECT_NAME"' _ "$root/dev/shared-dev-state.sh")"
[ "$from_spoof" = "$expected|wruntime-dev" ] || { echo "environment override split shared state/project" >&2; exit 1; }
other="$(git worktree list --porcelain | awk -v root="$root" '$1=="worktree" && $2!=root && system("test -e \"" $2 "/.git\"")==0 {print $2; exit}')"
if [ -n "$other" ]; then
  from_linked="$(cd "$other" && bash -c 'source "$1"; printf %s "$WRT_SHARED_DEV_STATE_ROOT"' _ "$root/dev/shared-dev-state.sh")"
  [ "$from_linked" = "$expected" ] || { echo "linked worktree resolved split shared state" >&2; exit 1; }
fi

wrt_acquire_postgres_fixture_lock "fixture contract parent" 2
canonical_inode="$(stat -Lc '%d:%i' "$WRT_POSTGRES_LOCK_FILE")"
held_inode="$(stat -Lc '%d:%i' "/proc/self/fd/$WRT_POSTGRES_FIXTURE_LOCK_FD")"
[ "$canonical_inode" = "$held_inode" ] || { echo "held lock inode mismatch" >&2; exit 1; }
bash -c 'source dev/shared-dev-state.sh; wrt_acquire_postgres_fixture_lock nested 1' >/dev/null
if bash -c 'exec {WRT_POSTGRES_FIXTURE_LOCK_FD}>&- 2>/dev/null || true; unset WRT_POSTGRES_FIXTURE_LOCK_FD; source dev/shared-dev-state.sh; wrt_acquire_postgres_fixture_lock competitor 1' >/dev/null 2>&1; then
  echo "competing process bypassed shared PostgreSQL lock" >&2
  exit 1
fi
if WRT_POSTGRES_FIXTURE_LOCK_FD=0 bash -c 'source dev/shared-dev-state.sh; wrt_acquire_postgres_fixture_lock spoofed 1' >/dev/null 2>&1; then
  echo "noncanonical inherited descriptor bypassed shared PostgreSQL lock" >&2
  exit 1
fi

producer="$root/dev/postgres-fixture-up.sh"
consumer="$root/dev/test-tenant-isolation-e2e.sh"
examples="$root/examples/helpers.sh"
for required in \
  'source "$repo/dev/shared-dev-state.sh"' \
  'source "$repo/dev/postgres-provisioner-image.sh"' \
  'wrt_acquire_postgres_fixture_lock' \
  'required_source="$(wrt_fixture_source_digest)"' \
  '--project-name "$WRT_COMPOSE_PROJECT_NAME"' \
  'wrt_detect_daemon_target' \
  'wrt_require_installed_musl_target' \
  'wrt_prepare_postgres_provisioner_image' \
  'postgres-provision-native.sh' \
  'postgres-provisioner migrate' \
  'pg_ident_file_mappings' \
  'mv "$WRT_POSTGRES_FIXTURE_DIR/.ready.json.tmp" "$WRT_POSTGRES_READY_FILE"' \
  'mv "$WRT_SHARED_DEV_STATE_ROOT/.owner.json.tmp" "$WRT_POSTGRES_OWNER_FILE"'; do
  grep -Fq -- "$required" "$producer" || { echo "shared dev producer omitted: $required" >&2; exit 1; }
done
python3 - "$producer" <<'PY'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
architecture = 'wrt_detect_daemon_target'
target = 'wrt_require_installed_musl_target'
lock = 'wrt_acquire_postgres_fixture_lock'
build = 'wrt_prepare_postgres_provisioner_image'
destructive = 'compose_current down -v --remove-orphans'
pki = 'bash dev/postgres-native-certs.sh'
provision = 'bash dev/postgres-provision-native.sh'
migrate = 'postgres-provisioner migrate'
if not text.index(architecture) < text.index(target) < text.index(lock) < text.index(build) < text.index(pki) < text.index(provision) < text.index(migrate):
    raise SystemExit('daemon/target preflight and local image preparation ordering changed')
if not text.index(target) < text.index(destructive):
    raise SystemExit('mapped Rust target preflight must precede destructive Compose')
helper=pathlib.Path('dev/postgres-provisioner-image.sh').read_text()
if helper.count('wrt_require_installed_musl_target || return') != 1:
    raise SystemExit('defensive build-time Rust target check is absent')
if 'rustup target add "$WRT_RUST_MUSL_TARGET"' in helper:
    raise SystemExit('host helper must not auto-mutate the Rust toolchain')
PY
for path in "$consumer" "$examples"; do
  if grep -Eq 'docker compose|postgres (provision|migrate)|postgres-provision-native|postgres-native-certs|WRT_POSTGRES_NATIVE|WRT_POSTGRES_ADMIN|WRT_POSTGRES_IDENT|dev/postgres-(fixture|pki)' "$path"; then
    echo "consume-only path contains setup or per-worktree state: $path" >&2
    exit 1
  fi
  grep -Fq 'wrt_require_compatible_fixture' "$path" || { echo "consumer omitted compatibility check: $path" >&2; exit 1; }
done
[ "$(sed -n '1p' docker-compose.yml)" = 'name: wruntime-dev' ] || { echo "Compose project is not fixed" >&2; exit 1; }
grep -Fq 'WRT_SHARED_DEV_STATE_ROOT' docker-compose.yml || { echo "Compose does not mount canonical PKI" >&2; exit 1; }
# docker_process_sql clears PGHOST/PGHOSTADDR. The server must therefore expose
# libpq's image-default bootstrap socket and the separate shared tool socket.
grep -Fq 'unix_socket_directories=/var/run/postgresql,/var/run/wruntime-postgresql' docker-compose.yml || { echo "postgres does not expose both official-init and shared-tool sockets" >&2; exit 1; }
if grep -Eq '^[[:space:]]+PGHOST:' docker-compose.yml || grep -Eq '(^|[[:space:]])(export[[:space:]]+)?PGHOST=' dev/postgres-entrypoint.sh; then
  echo "invalid PGHOST bootstrap redirection assumption returned" >&2
  exit 1
fi
[ "$(grep -Fc 'postgres-socket:/var/run/wruntime-postgresql' docker-compose.yml)" -eq 2 ] || { echo "postgres/provisioner do not share one tool socket volume" >&2; exit 1; }
grep -Fq 'docker-library/postgres intentionally clears PGHOST and PGHOSTADDR' dev/postgres-entrypoint.sh || { echo "entrypoint does not encode upstream bootstrap behavior" >&2; exit 1; }
grep -Fq "printf 'host=%s user=postgres dbname=postgres" dev/postgres-entrypoint.sh || { echo "provisioner admin URL does not derive the shared tool socket" >&2; exit 1; }
grep -Eq '^local[[:space:]]+all[[:space:]]+\+wr__migration_executor_auth[[:space:]]+scram-sha-256([[:space:]]|$)' dev/postgres-config/pg_hba.conf || { echo "shared tool socket does not authenticate disposable migration executors" >&2; exit 1; }

# Exercise the real PKI permission helper: UID 70 can traverse/read only the
# top-level combined public bundle, while private directories and keys stay sealed.
pki_case="$(mktemp -d "${TMPDIR:-/tmp}/wr-postgres-pki.XXXXXX")/pki"
mkdir -p "$pki_case/root" "$pki_case/server" "$pki_case/node-a"
: >"$pki_case/root/ca.key"; : >"$pki_case/server/key.pem"; : >"$pki_case/node-a/key.pem"
printf 'leaf\nissuer\n' >"$pki_case/node-a-public.pem"
# shellcheck source=postgres-pki-permissions.sh
source dev/postgres-pki-permissions.sh
wrt_set_postgres_pki_permissions "$pki_case"
python3 - "$pki_case" <<'PY'
import pathlib, stat, sys
root = pathlib.Path(sys.argv[1])
def mode(path): return stat.S_IMODE(path.stat().st_mode)
if mode(root) != 0o755 or mode(root/'node-a-public.pem') != 0o644:
    raise SystemExit('public provisioner certificate boundary is not traversable/readable')
for directory in ('root', 'server', 'node-a'):
    if mode(root/directory) != 0o700:
        raise SystemExit(f'private PKI directory mode changed: {directory}')
for key in ('root/ca.key', 'server/key.pem', 'node-a/key.pem'):
    if mode(root/key) != 0o600:
        raise SystemExit(f'private PKI key mode changed: {key}')
PY
rm -rf "${pki_case%/pki}"
grep -Fq 'cat "$root/node-a/leaf.pem" "$root/node-a/chain.pem" >"$root/node-a-public.pem"' dev/postgres-native-certs.sh || { echo "public node bundle is not leaf+issuer chain" >&2; exit 1; }

# Docker receives only the locally built binary and this runtime-only template.
[ ! -e .dockerignore ] || { echo "obsolete repository-context .dockerignore remains" >&2; exit 1; }
[ "$(grep -Ec '^(FROM|COPY|USER|ENTRYPOINT)' dev/postgres-provisioner.Dockerfile)" -eq 4 ] || { echo "runtime Dockerfile shape changed" >&2; exit 1; }
grep -Fxq 'FROM postgres:18-alpine' dev/postgres-provisioner.Dockerfile || { echo "provisioner base changed" >&2; exit 1; }
grep -Fxq 'COPY --chmod=0755 wr-cli /usr/local/bin/wr-cli' dev/postgres-provisioner.Dockerfile || { echo "provisioner does not copy only staged wr-cli" >&2; exit 1; }
if grep -Eq 'cargo (build|zigbuild)|COPY \.|FROM rust' dev/postgres-provisioner.Dockerfile || grep -Eq '^[[:space:]]+build:' docker-compose.yml; then
  echo "repository-context or Docker-side Rust provisioner build remains" >&2
  exit 1
fi
grep -Fq 'pull_policy: never' docker-compose.yml || { echo "provisioner image can be pulled" >&2; exit 1; }
for required_input in Cargo.lock Cargo.toml proto/wruntime.proto dev/postgres-provisioner-image.sh dev/postgres-provisioner.Dockerfile wr-cli/src wr-common/src wr-common/build.rs; do
  grep -Fxq "$required_input" dev/postgres-fixture-inputs.txt || { echo "fixture source digest omits provisioner binary input: $required_input" >&2; exit 1; }
done
if grep -Eq '^\.dockerignore$|^wr-engine|^target|^\.git' dev/postgres-fixture-inputs.txt; then
  echo "fixture source digest includes obsolete context or unrelated runtime/build state" >&2
  exit 1
fi

# Execute the rendered CMD-SHELL value against a fake psql and inspect its argv.
# This catches YAML/shell quote folding that source-text greps cannot detect.
health_case="$(mktemp -d "${TMPDIR:-/tmp}/wr-postgres-health.XXXXXX")"
cat >"$health_case/provisioner.yml" <<'YAML'
services:
  postgres-provisioner:
    image: wruntime-dev-postgres-provisioner:contract-test
    pull_policy: never
YAML
WRT_SHARED_DEV_STATE_ROOT="$WRT_SHARED_DEV_STATE_ROOT" docker compose -f docker-compose.yml -f "$health_case/provisioner.yml" --profile postgres-tools config --format json >"$health_case/compose.json"
health_script="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["services"]["postgres"]["healthcheck"]["test"][1])' "$health_case/compose.json")"
python3 - "$health_case/compose.json" <<'PY'
import json, sys
service=json.load(open(sys.argv[1]))['services']['postgres-provisioner']
if service.get('image') != 'wruntime-dev-postgres-provisioner:contract-test' or service.get('pull_policy') != 'never' or 'build' in service:
    raise SystemExit(f'generated override does not select one exact no-build/no-pull image: {service!r}')
volumes=service.get('volumes', [])
by_target={v.get('target'):v for v in volumes}
if set(by_target) != {'/var/lib/postgresql/wruntime-config','/var/run/wruntime-postgresql','/wr-input'}:
    raise SystemExit(f'provisioner volume authority changed: {volumes!r}')
if by_target['/var/lib/postgresql/wruntime-config'].get('read_only') or by_target['/var/run/wruntime-postgresql'].get('read_only') or not by_target['/wr-input'].get('read_only'):
    raise SystemExit(f'provisioner socket/config/public-PKI modes changed: {volumes!r}')
PY
cat >"$health_case/psql" <<'SH'
#!/bin/sh
printf '%s\0' "$@" >"$HEALTH_ARGS"
printf 't\n'
SH
chmod +x "$health_case/psql"
POSTGRES_PASSWORD=rendered-secret HEALTH_ARGS="$health_case/args" PATH="$health_case:$PATH" /bin/sh -c "$health_script"
python3 - "$health_case/args" <<'PY'
import pathlib, sys
args = pathlib.Path(sys.argv[1]).read_bytes().split(b'\0')
if args and not args[-1]: args.pop()
args = [value.decode() for value in args]
expected = "select current_setting('server_version_num')::int / 10000 = 18 and current_setting('ssl') = 'on'"
if '-Atqc' not in args or args[args.index('-Atqc') + 1] != expected:
    raise SystemExit(f'rendered healthcheck changed SQL argv: {args!r}')
PY
rm -rf "$health_case"
if rg -n 'dev/postgres-(fixture|pki)/' Justfile dev examples wr-tests --glob '!postgres-fixture-contract-test.sh'; then
  echo "per-worktree PostgreSQL fixture path remains" >&2
  exit 1
fi
source_digest="$(wrt_fixture_source_digest)"
case "$source_digest" in sha256:[0-9a-f][0-9a-f]*) ;; *) echo "invalid fixture source digest" >&2; exit 1;; esac
digest_case="$(mktemp -d "${TMPDIR:-/tmp}/wr-fixture-digest.XXXXXX")"
trap 'rm -rf "$digest_case"' EXIT
mkdir -p "$digest_case/tree"; printf 'tree/a\n' >"$digest_case/inputs"; printf one >"$digest_case/tree/a"; printf ignored >"$digest_case/unrelated"
digest_a="$(wrt_hash_fixture_inputs "$digest_case" "$digest_case/inputs")"
printf changed >"$digest_case/unrelated"
[ "$(wrt_hash_fixture_inputs "$digest_case" "$digest_case/inputs")" = "$digest_a" ] || { echo "unrelated bytes changed fixture digest" >&2; exit 1; }
printf two >"$digest_case/tree/a"
[ "$(wrt_hash_fixture_inputs "$digest_case" "$digest_case/inputs")" != "$digest_a" ] || { echo "named uncommitted bytes did not change fixture digest" >&2; exit 1; }
printf 'tree\n' >"$digest_case/inputs"; printf untracked >"$digest_case/tree/new"
[ "$(wrt_hash_fixture_inputs "$digest_case" "$digest_case/inputs")" != "$digest_a" ] || { echo "named untracked bytes did not change fixture digest" >&2; exit 1; }
python3 - "$digest_case" <<'PY'
import json, pathlib, sys
root=pathlib.Path(sys.argv[1]); import hashlib
files={'Dockerfile':b'FROM postgres:18-alpine\n','wr-cli':b'fixture binary\n'}
h=hashlib.sha256()
for name,data in files.items():
    encoded=name.encode(); h.update(len(encoded).to_bytes(8,'big')); h.update(encoded); h.update(len(data).to_bytes(8,'big')); h.update(data)
context=h.hexdigest(); directory=root/'build/provisioner'/context; directory.mkdir(parents=True)
for name,data in files.items(): (directory/name).write_bytes(data)
(directory/'wr-cli').chmod(0o755)
provenance={'daemon_architecture':'amd64','daemon_platform':'linux/amd64','rust_target':'x86_64-unknown-linux-musl',
 'cargo_version':'cargo test','rustc_version':'rustc test','cargo_zigbuild_version':'cargo-zigbuild test','zig_version':'zig test',
 'wr_cli_binary_sha256':'sha256:'+hashlib.sha256(files['wr-cli']).hexdigest(),'minimal_context_sha256':'sha256:'+context,
 'provisioner_image_tag':'wruntime-dev-postgres-provisioner:'+context,'provisioner_image_id':'sha256:image',
 'base_postgres_image_id':'sha256:base','image_smoke_passed':True}
common={'owner_worktree':'/owner','git_common_dir':'/common','compose_project':'wruntime-dev','source_digest':'sha256:source',
 'fixture_artifact_digest':'sha256:artifact','provisioning_manifest_digest':'sha256:provision','migration_bundle_digest':'sha256:migration',**provenance}
(root/'owner.json').write_text(json.dumps({'schema_version':1,'ready_artifact_digest':'sha256:ready',**common}))
(root/'ready.json').write_text(json.dumps({'schema_version':2,**common}))
(root/'override.yml').write_text(f'services:\n  postgres-provisioner:\n    image: wruntime-dev-postgres-provisioner:{context}\n    pull_policy: never\n')
PY
wrt_validate_fixture_records "$digest_case/owner.json" "$digest_case/ready.json" /common sha256:source sha256:artifact sha256:ready "$digest_case/override.yml"
if wrt_validate_fixture_records "$digest_case/owner.json" "$digest_case/ready.json" /common sha256:different sha256:artifact sha256:ready "$digest_case/override.yml" >/dev/null 2>&1; then
  echo "source mismatch was accepted" >&2; exit 1
fi
if wrt_validate_fixture_records "$digest_case/owner.json" "$digest_case/ready.json" /common sha256:source sha256:different sha256:ready "$digest_case/override.yml" >/dev/null 2>&1; then
  echo "artifact mismatch was accepted" >&2; exit 1
fi
context_path="$(find "$digest_case/build/provisioner" -mindepth 1 -maxdepth 1 -type d -print -quit)"
printf tampered >>"$context_path/wr-cli"
if wrt_validate_fixture_records "$digest_case/owner.json" "$digest_case/ready.json" /common sha256:source sha256:artifact sha256:ready "$digest_case/override.yml" >/dev/null 2>&1; then
  echo "provisioner context/binary mismatch was accepted" >&2; exit 1
fi
printf 'shared state locator, inherited-inode lock, fixed project, digest compatibility, and consume-only contracts hold\n'
