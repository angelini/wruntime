#!/usr/bin/env bash
# Host-only, daemon-native wr-cli build and minimal provisioner image assembly.
# Source this file and call wrt_detect_daemon_target followed by
# wrt_prepare_postgres_provisioner_image <owner> <state-root> <provenance-json>.

wrt_map_daemon_architecture() {
  case "$1" in
    amd64|x86_64)
      WRT_DOCKER_DAEMON_ARCHITECTURE=amd64
      WRT_DOCKER_PLATFORM=linux/amd64
      WRT_RUST_MUSL_TARGET=x86_64-unknown-linux-musl
      WRT_EXPECTED_ELF_MACHINE='Advanced Micro Devices X86-64'
      WRT_EXPECTED_MUSL_INTERPRETER='ld-musl-x86_64.so.1'
      ;;
    arm64|aarch64)
      WRT_DOCKER_DAEMON_ARCHITECTURE=arm64
      WRT_DOCKER_PLATFORM=linux/arm64
      WRT_RUST_MUSL_TARGET=aarch64-unknown-linux-musl
      WRT_EXPECTED_ELF_MACHINE='AArch64'
      WRT_EXPECTED_MUSL_INTERPRETER='ld-musl-aarch64.so.1'
      ;;
    *)
      printf 'unsupported Docker daemon architecture %q; supported mappings: amd64/x86_64, arm64/aarch64\n' "$1" >&2
      return 1
      ;;
  esac
  export WRT_DOCKER_DAEMON_ARCHITECTURE WRT_DOCKER_PLATFORM WRT_RUST_MUSL_TARGET
  export WRT_EXPECTED_ELF_MACHINE WRT_EXPECTED_MUSL_INTERPRETER
}

wrt_detect_daemon_target() {
  local observed
  observed="$(docker info --format '{{.Architecture}}')" || return
  [ -n "$observed" ] || { echo 'Docker daemon architecture is empty' >&2; return 1; }
  case "$observed" in *$'\n'*|*' '*|*'/'*) printf 'malformed Docker daemon architecture %q\n' "$observed" >&2; return 1;; esac
  wrt_map_daemon_architecture "$observed"
}

wrt_require_installed_musl_target() {
  if ! command -v rustup >/dev/null 2>&1; then
    echo 'missing required host provisioner tool: rustup' >&2
    echo "owner action: rustup target add $WRT_RUST_MUSL_TARGET" >&2
    return 127
  fi
  if ! rustup target list --installed | grep -Fxq "$WRT_RUST_MUSL_TARGET"; then
    echo "required Rust target is not installed: $WRT_RUST_MUSL_TARGET" >&2
    echo "owner action: rustup target add $WRT_RUST_MUSL_TARGET" >&2
    return 1
  fi
}

wrt_verify_musl_binary() {
  local binary="$1" file_output header program
  if [ ! -f "$binary" ] || [ -L "$binary" ] || [ ! -x "$binary" ]; then
    echo "wr-cli musl artifact must be a regular executable: $binary" >&2
    return 1
  fi
  file_output="$(file -b "$binary")" || return
  case "$file_output" in 'ELF 64-bit LSB'*) ;; *) echo "wr-cli artifact is not ELF64 little-endian: $file_output" >&2; return 1;; esac
  header="$(readelf -hW "$binary")" || return
  grep -Eq 'Class:[[:space:]]+ELF64$' <<<"$header" || { echo 'wr-cli artifact is not ELF64' >&2; return 1; }
  grep -Eq 'Data:[[:space:]]+2.s complement, little endian$' <<<"$header" || { echo 'wr-cli artifact is not little-endian' >&2; return 1; }
  local machine
  machine="$(awk -F: '$1 ~ /Machine/ {sub(/^[[:space:]]+/, "", $2); print $2}' <<<"$header")"
  [ "$machine" = "$WRT_EXPECTED_ELF_MACHINE" ] || {
    echo "wr-cli artifact machine does not match $WRT_DOCKER_PLATFORM: $machine" >&2
    return 1
  }
  program="$(readelf -lW "$binary")" || return
  if grep -q 'Requesting program interpreter' <<<"$program" && ! grep -Fq "$WRT_EXPECTED_MUSL_INTERPRETER" <<<"$program"; then
    echo 'wr-cli artifact has a non-musl dynamic interpreter' >&2
    return 1
  fi
}

wrt_context_digest() {
  python3 - "$1" "$2" <<'PY'
import hashlib, pathlib, sys
h = hashlib.sha256()
for name, raw in [('Dockerfile', sys.argv[1]), ('wr-cli', sys.argv[2])]:
    data = pathlib.Path(raw).read_bytes(); encoded = name.encode()
    h.update(len(encoded).to_bytes(8, 'big')); h.update(encoded)
    h.update(len(data).to_bytes(8, 'big')); h.update(data)
print(h.hexdigest())
PY
}

wrt_verify_two_file_context() {
  python3 - "$1" <<'PY'
import pathlib, stat, sys
root = pathlib.Path(sys.argv[1])
entries = list(root.iterdir())
if sorted(p.name for p in entries) != ['Dockerfile', 'wr-cli']:
    raise SystemExit('provisioner context must contain exactly Dockerfile and wr-cli')
if any(p.is_symlink() or not p.is_file() for p in entries):
    raise SystemExit('provisioner context entries must be regular files, not symlinks')
if stat.S_IMODE((root/'wr-cli').stat().st_mode) != 0o755:
    raise SystemExit('staged wr-cli mode must be 0755')
PY
}

wrt_prepare_postgres_provisioner_image() {
  local owner="$1" state_root="$2" provenance="$3" override_path="${4:-$2/compose-provisioner.generated.yml}"
  local binary binary_sha context_hex context_root context_tmp image_tag image_json image_id image_arch image_user image_entrypoint embedded_sha base_image_id
  local cargo_version rustc_version zigbuild_version zig_version
  if [ -z "${WRT_DOCKER_PLATFORM:-}" ] || [ -z "${WRT_RUST_MUSL_TARGET:-}" ]; then
    echo 'Docker daemon architecture must be mapped before building wr-cli' >&2
    return 1
  fi
  for tool in cargo cargo-zigbuild zig rustc rustup file readelf sha256sum docker python3; do
    command -v "$tool" >/dev/null 2>&1 || { echo "missing required host provisioner tool: $tool" >&2; return 127; }
  done
  # Defensive build-time check: callers must also preflight this before taking
  # destructive or publication-capable preparation steps.
  wrt_require_installed_musl_target || return
  owner="$(realpath -e "$owner")" || return
  CARGO_TARGET_DIR="$owner/target" cargo zigbuild --release -p wr-cli --bin wr-cli --target "$WRT_RUST_MUSL_TARGET" || return
  binary="$owner/target/$WRT_RUST_MUSL_TARGET/release/wr-cli"
  wrt_verify_musl_binary "$binary"
  binary_sha="sha256:$(sha256sum "$binary" | cut -d' ' -f1)"
  cargo_version="$(cargo --version)"
  rustc_version="$(rustc -Vv | tr '\n' ';')"
  zigbuild_version="$(cargo-zigbuild --version)"
  zig_version="$(zig version)"

  context_hex="$(wrt_context_digest "$owner/dev/postgres-provisioner.Dockerfile" "$binary")"
  context_root="$state_root/build/provisioner/$context_hex"
  context_tmp="$state_root/build/provisioner/$context_hex.tmp"
  mkdir -p "$state_root/build/provisioner"
  if [ ! -d "$context_root" ]; then
    rm -rf "$context_tmp"
    mkdir "$context_tmp"
    install -m 0644 "$owner/dev/postgres-provisioner.Dockerfile" "$context_tmp/Dockerfile"
    install -m 0755 "$binary" "$context_tmp/wr-cli"
    wrt_verify_two_file_context "$context_tmp"
    mv "$context_tmp" "$context_root"
  fi
  wrt_verify_two_file_context "$context_root"
  [ "sha256:$(sha256sum "$context_root/wr-cli" | cut -d' ' -f1)" = "$binary_sha" ] || {
    echo 'content-addressed provisioner context binary mismatch' >&2
    return 1
  }
  [ "$(wrt_context_digest "$context_root/Dockerfile" "$context_root/wr-cli")" = "$context_hex" ] || {
    echo 'content-addressed provisioner context digest mismatch' >&2
    return 1
  }

  image_tag="wruntime-dev-postgres-provisioner:$context_hex"
  docker build --platform "$WRT_DOCKER_PLATFORM" --file "$context_root/Dockerfile" --tag "$image_tag" "$context_root" || return
  image_json="$(docker image inspect "$image_tag")" || return
  readarray -t inspected < <(python3 -c 'import json,sys; i=json.load(sys.stdin)[0]; print(i["Id"]); print(i["Architecture"]); print(i["Config"].get("User", "")); print(__import__("json").dumps(i["Config"].get("Entrypoint")))' <<<"$image_json")
  image_id="${inspected[0]:-}"; image_arch="${inspected[1]:-}"; image_user="${inspected[2]:-}"; image_entrypoint="${inspected[3]:-}"
  case "$image_id" in sha256:*) ;; *) echo 'provisioner image identity is invalid' >&2; return 1;; esac
  [ "$image_arch" = "$WRT_DOCKER_DAEMON_ARCHITECTURE" ] || { echo 'provisioner image architecture mismatch' >&2; return 1; }
  [ "$image_user" = '70:70' ] || { echo "provisioner image user is not 70:70: $image_user" >&2; return 1; }
  [ "$image_entrypoint" = '["/usr/local/bin/wr-cli", "postgres"]' ] || { echo "provisioner image entrypoint mismatch: $image_entrypoint" >&2; return 1; }
  local embedded_hex
  embedded_hex="$(docker run --rm --platform "$WRT_DOCKER_PLATFORM" --entrypoint sha256sum "$image_tag" /usr/local/bin/wr-cli | awk '{print $1}')" || return
  embedded_sha="sha256:$embedded_hex"
  [ "$embedded_sha" = "$binary_sha" ] || { echo 'provisioner image embeds the wrong wr-cli binary' >&2; return 1; }
  docker run --rm --platform "$WRT_DOCKER_PLATFORM" --entrypoint /usr/local/bin/wr-cli "$image_tag" postgres --help >/dev/null || {
    echo 'provisioner image daemon-side wr-cli smoke failed' >&2
    return 1
  }
  base_image_id="$(docker image inspect --format '{{.Id}}' postgres:18-alpine)" || return
  case "$base_image_id" in sha256:*) ;; *) echo 'PostgreSQL base image identity is invalid' >&2; return 1;; esac

  mkdir -p "$state_root" "$(dirname "$override_path")"
  cat >"$override_path.tmp" <<YAML
services:
  postgres-provisioner:
    image: $image_tag
    pull_policy: never
YAML
  mv "$override_path.tmp" "$override_path"
  python3 - "$provenance" "$WRT_DOCKER_DAEMON_ARCHITECTURE" "$WRT_DOCKER_PLATFORM" "$WRT_RUST_MUSL_TARGET" "$cargo_version" "$rustc_version" "$zigbuild_version" "$zig_version" "$binary_sha" "sha256:$context_hex" "$image_tag" "$image_id" "$base_image_id" <<'PY'
import json, pathlib, sys
(path, arch, platform, target, cargo, rustc, zigbuild, zig, binary, context, tag, image, base) = sys.argv[1:]
value = {'daemon_architecture': arch, 'daemon_platform': platform, 'rust_target': target,
 'cargo_version': cargo, 'rustc_version': rustc, 'cargo_zigbuild_version': zigbuild, 'zig_version': zig,
 'wr_cli_binary_sha256': binary, 'minimal_context_sha256': context,
 'provisioner_image_tag': tag, 'provisioner_image_id': image, 'base_postgres_image_id': base,
 'image_smoke_passed': True}
pathlib.Path(path).write_text(json.dumps(value, sort_keys=True, indent=2)+'\n')
PY
}
