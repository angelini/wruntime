#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=postgres-provisioner-image.sh
source "$root/dev/postgres-provisioner-image.sh"

for value in amd64 x86_64; do
  wrt_map_daemon_architecture "$value"
  [ "$WRT_DOCKER_PLATFORM|$WRT_RUST_MUSL_TARGET" = 'linux/amd64|x86_64-unknown-linux-musl' ]
done
for value in arm64 aarch64; do
  wrt_map_daemon_architecture "$value"
  [ "$WRT_DOCKER_PLATFORM|$WRT_RUST_MUSL_TARGET" = 'linux/arm64|aarch64-unknown-linux-musl' ]
done
for value in '' i386 armv7 linux/amd64 'amd64 arm64'; do
  if wrt_map_daemon_architecture "$value" >/dev/null 2>&1; then
    echo "unsupported daemon architecture accepted: $value" >&2; exit 1
  fi
done

case_root="$(mktemp -d "${TMPDIR:-/tmp}/wr-provisioner-image.XXXXXX")"
trap 'rm -rf "$case_root"' EXIT
mkdir -p "$case_root/bin" "$case_root/owner/dev"
cp "$root/dev/postgres-provisioner.Dockerfile" "$case_root/owner/dev/"
log="$case_root/commands.log"
cat >"$case_root/bin/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
{ printf 'docker'; printf ' %q' "$@"; printf '\n'; } >>"$FAKE_LOG"
if [ "$1 $2" = 'info --format' ]; then printf '%s\n' "${FAKE_DAEMON_ARCH:-amd64}"; exit; fi
if [ "$1 $2" = 'image inspect' ]; then
  if [ "${3:-}" = '--format' ]; then printf 'sha256:base-image\n'; exit; fi
  printf '[{"Id":"sha256:provisioner-image","Architecture":"%s","Config":{"User":"70:70","Entrypoint":["/usr/local/bin/wr-cli","postgres"]}}]\n' "${FAKE_IMAGE_ARCH:-amd64}"
  exit
fi
if [ "$1" = run ]; then
  if printf '%s\n' "$*" | grep -q sha256sum; then sha256sum "$FAKE_BINARY"; exit; fi
  [ "${FAKE_SMOKE_FAIL:-0}" != 1 ]
  exit
fi
[ "$1" = build ] && exit
exit 1
SH
cat >"$case_root/bin/cargo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
{ printf 'cargo CARGO_TARGET_DIR=%q' "${CARGO_TARGET_DIR:-}"; printf ' %q' "$@"; printf '\n'; } >>"$FAKE_LOG"
if [ "$1" = zigbuild ]; then
  while [ "$#" -gt 0 ]; do [ "$1" != --target ] || { target="$2"; break; }; shift; done
  mkdir -p "$CARGO_TARGET_DIR/$target/release"
  printf 'fake-musl-elf-binary\n' >"$CARGO_TARGET_DIR/$target/release/wr-cli"
  chmod 0755 "$CARGO_TARGET_DIR/$target/release/wr-cli"
else
  echo 'cargo 1.98.1'
fi
SH
cat >"$case_root/bin/cargo-zigbuild" <<'SH'
#!/bin/sh
echo 'cargo-zigbuild 0.test'
SH
cat >"$case_root/bin/zig" <<'SH'
#!/bin/sh
echo '0.test'
SH
cat >"$case_root/bin/rustc" <<'SH'
#!/bin/sh
echo 'rustc 1.test (test)'; echo 'host: x86_64-unknown-linux-gnu'
SH
cat >"$case_root/bin/rustup" <<'SH'
#!/usr/bin/env bash
{ printf 'rustup'; printf ' %q' "$@"; printf '\n'; } >>"$FAKE_LOG"
[ "${FAKE_TARGET_MISSING:-0}" = 1 ] || printf '%s\n' "${FAKE_INSTALLED_TARGET:-x86_64-unknown-linux-musl}"
SH
cat >"$case_root/bin/file" <<'SH'
#!/bin/sh
echo "${FAKE_FILE_OUTPUT:-ELF 64-bit LSB executable, x86-64, statically linked}"
SH
cat >"$case_root/bin/readelf" <<'SH'
#!/bin/sh
if [ "$1" = -hW ]; then
cat <<EOF
  Class:                             ELF64
  Data:                              2's complement, little endian
  Machine:                           ${FAKE_MACHINE:-Advanced Micro Devices X86-64}
EOF
else
  [ -z "${FAKE_INTERPRETER:-}" ] || echo "Requesting program interpreter: $FAKE_INTERPRETER"
fi
SH
chmod +x "$case_root/bin/"*
export FAKE_LOG="$log" PATH="$case_root/bin:$PATH"

: >"$log"
wrt_map_daemon_architecture arm64
FAKE_INSTALLED_TARGET=aarch64-unknown-linux-musl wrt_require_installed_musl_target
if FAKE_INSTALLED_TARGET=x86_64-unknown-linux-musl wrt_require_installed_musl_target >"$case_root/missing-arm-target.out" 2>&1; then
  echo 'wrong installed target satisfied arm64 mapping' >&2; exit 1
fi
grep -Fxq 'owner action: rustup target add aarch64-unknown-linux-musl' "$case_root/missing-arm-target.out" || {
  echo 'arm64 missing-target action was not exact' >&2; exit 1
}
if FAKE_DAEMON_ARCH=i386 wrt_detect_daemon_target >/dev/null 2>&1; then echo 'unsupported daemon architecture was detected as valid' >&2; exit 1; fi
if grep -Eq 'cargo|docker build|docker compose' "$log"; then echo 'unsupported architecture reached Cargo/Compose mutation' >&2; exit 1; fi
: >"$log"
wrt_detect_daemon_target
wrt_require_installed_musl_target
if FAKE_TARGET_MISSING=1 wrt_require_installed_musl_target >"$case_root/missing-target.out" 2>&1; then
  echo 'missing mapped Rust target was accepted' >&2; exit 1
fi
grep -Fxq "owner action: rustup target add $WRT_RUST_MUSL_TARGET" "$case_root/missing-target.out" || {
  echo 'missing target diagnostic omitted the exact owner action' >&2; exit 1
}
if grep -Eq 'cargo|docker build|docker compose' "$log"; then echo 'missing target reached Cargo/Compose mutation' >&2; exit 1; fi
: >"$log"
export FAKE_BINARY="$case_root/owner/target/$WRT_RUST_MUSL_TARGET/release/wr-cli"
wrt_prepare_postgres_provisioner_image "$case_root/owner" "$case_root/state" "$case_root/provenance.json"
python3 - "$case_root" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1]); p=json.loads((root/'provenance.json').read_text())
if p['daemon_platform'] != 'linux/amd64' or p['rust_target'] != 'x86_64-unknown-linux-musl': raise SystemExit('mapping provenance mismatch')
if not p['image_smoke_passed'] or p['provisioner_image_id'] != 'sha256:provisioner-image': raise SystemExit('image provenance mismatch')
context = root/'state/build/provisioner'/p['minimal_context_sha256'].removeprefix('sha256:')
if sorted(x.name for x in context.iterdir()) != ['Dockerfile','wr-cli']: raise SystemExit('context is not exactly two files')
override=(root/'state/compose-provisioner.generated.yml').read_text()
if p['provisioner_image_tag'] not in override or 'pull_policy: never' not in override: raise SystemExit('override does not bind exact image')
log=(root/'commands.log').read_text()
expected=f"cargo CARGO_TARGET_DIR={root/'owner/target'} zigbuild --release -p wr-cli --bin wr-cli --target x86_64-unknown-linux-musl"
if expected not in log: raise SystemExit(f'owner target was not reused: {log}')
build=next(line for line in log.splitlines() if line.startswith('docker build '))
if str(root/'owner') in build or '--no-cache' in build or '--pull' in build or 'prune' in log or 'cargo clean' in log: raise SystemExit('forbidden build/cache behavior occurred')
if str(context) not in build: raise SystemExit('Docker did not receive minimal content-addressed context')
PY

if FAKE_SMOKE_FAIL=1 wrt_prepare_postgres_provisioner_image "$case_root/owner" "$case_root/failed-state" "$case_root/failed-provenance.json" >/dev/null 2>&1; then
  echo 'failed daemon smoke was accepted' >&2; exit 1
fi
[ ! -e "$case_root/failed-provenance.json" ] && [ ! -e "$case_root/failed-state/compose-provisioner.generated.yml" ] || {
  echo 'failed image smoke published provenance/Compose state' >&2; exit 1
}

# Artifact validation fails closed for path, mode, format, machine, and interpreter.
if wrt_verify_musl_binary "$case_root/missing" >/dev/null 2>&1; then echo 'missing artifact accepted' >&2; exit 1; fi
chmod 0644 "$FAKE_BINARY"
if wrt_verify_musl_binary "$FAKE_BINARY" >/dev/null 2>&1; then echo 'non-executable artifact accepted' >&2; exit 1; fi
chmod 0755 "$FAKE_BINARY"
for invalid_format in 'PE32 executable' 'Mach-O 64-bit executable' 'ELF 32-bit LSB executable' 'ELF 64-bit MSB executable'; do
  if FAKE_FILE_OUTPUT="$invalid_format" wrt_verify_musl_binary "$FAKE_BINARY" >/dev/null 2>&1; then echo "invalid artifact accepted: $invalid_format" >&2; exit 1; fi
done
if FAKE_MACHINE=AArch64 wrt_verify_musl_binary "$FAKE_BINARY" >/dev/null 2>&1; then echo 'wrong-machine artifact accepted' >&2; exit 1; fi
if FAKE_INTERPRETER=/lib64/ld-linux-x86-64.so.2 wrt_verify_musl_binary "$FAKE_BINARY" >/dev/null 2>&1; then echo 'non-musl interpreter accepted' >&2; exit 1; fi

printf 'daemon mapping, owner-cache musl build, binary validation, two-file image, and provenance contracts hold\n'
