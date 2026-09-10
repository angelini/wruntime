#!/usr/bin/env python3
"""Render immutable live manager A→B→A protected-rollout artifacts."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shutil
import struct
import tomllib
from urllib.parse import urlparse

ZERO_DIGEST = "sha256:" + "0" * 64


def digest_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def digest_file(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def tree_digest(root: Path) -> str:
    value = hashlib.sha256()
    for path in sorted((p for p in root.rglob("*") if p.is_file()), key=lambda p: p.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode()
        data = path.read_bytes()
        value.update(struct.pack(">Q", len(relative)))
        value.update(relative)
        value.update(struct.pack(">Q", len(data)))
        value.update(data)
    return "sha256:" + value.hexdigest()


def require_remote_database(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise ValueError("WRT_DEPLOY_E2E_DB_URL must be an absolute PostgreSQL URL")
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = parsed.hostname.lower() == "localhost"
    if loopback:
        raise ValueError("Manager B database URL must not use a loopback host")


def render_policy(source: str, generation: int, manager_id: str, endpoint: str) -> str:
    parsed = tomllib.loads(source)
    manager_principals = [item for item in parsed["principals"] if item["kind"] == "manager"]
    manager_enrollments = parsed["manager_enrollments"]
    if len(manager_principals) != 1 or len(manager_enrollments) != 1:
        raise ValueError("source policy must declare and enroll exactly one manager")
    source_principal = manager_principals[0]["uri"]
    if manager_enrollments[0]["principal"] != source_principal:
        raise ValueError("source policy manager declaration and enrollment must match")
    target_principal = f'urn:wruntime:{parsed["cluster_id"]}:manager:{manager_id}'
    encoded_source_principal = quote(source_principal)
    if source.count(encoded_source_principal) != 2:
        raise ValueError("source manager principal must occur exactly twice")
    value = source.replace(encoded_source_principal, quote(target_principal))
    value, count = re.subn(r"(?m)^generation\s*=\s*\d+\s*$", f"generation = {generation}", value)
    if count != 1:
        raise ValueError("policy must contain exactly one generation")
    block = (
        "[[manager_enrollments]]\n"
        f"principal  = {quote(target_principal)}\n"
        f'manager_id = "{manager_id}"\n'
        f'endpoint   = "{endpoint}"\n'
    )
    value, count = re.subn(r"(?ms)\[\[manager_enrollments\]\].*?(?=\n\[\[|\Z)", block.rstrip(), value)
    if count != 1:
        raise ValueError("policy must contain exactly one manager enrollment")
    return value.rstrip() + "\n"


def render_config(template: str, manager_id: str, endpoint: str, db_url: str, set_name: str) -> str:
    require_remote_database(db_url)
    value = template.replace("{db_url}", db_url).replace("{advertise_address}", endpoint)
    root = f"/etc/wruntime/pki/manager-{manager_id}/sets/{set_name}"
    replacements = {
        "/etc/wruntime/pki/manager-endpoint/sets/v1/leaf.pem": f"{root}/endpoint/leaf.pem",
        "/etc/wruntime/pki/manager-endpoint/sets/v1/key.pem": f"{root}/endpoint/key.pem",
        "/etc/wruntime/pki/manager-client/sets/v1/leaf.pem": f"{root}/client/leaf.pem",
        "/etc/wruntime/pki/manager-client/sets/v1/key.pem": f"{root}/client/key.pem",
        "/etc/wruntime/pki/roots/client/ca.crt": f"{root}/roots/client-ca.crt",
        "/etc/wruntime/pki/roots/server/ca.crt": f"{root}/roots/server-ca.crt",
        f"/var/lib/wruntime/manager-config/{manager_id}/authorization.toml": f"{root}/authorization.toml",
    }
    for old, new in replacements.items():
        if old not in value:
            raise ValueError(f"manager config omitted expected path {old}")
        value = value.replace(old, new)
    if "{" in value or "}" in value or db_url not in value:
        raise ValueError("manager config contains unresolved variables")
    return value


def descriptor(manager_id: str, binary_digest: str, unit_digest: str, config_digest: str, credential_path: str, credential_digest: str, *, initial: bool = False) -> bytes:
    if initial:
        executable = "/opt/wruntime/wr-manager/bin/wr-manager"
        spec = "/opt/wruntime/wr-manager/systemd/wr-manager.service"
    else:
        executable = f"/opt/wruntime/manager-artifacts/binaries/{binary_digest[7:]}/wr-manager"
        spec = f"/opt/wruntime/manager-artifacts/backend-specs/{unit_digest[7:]}.json"
    value = {
        "schema_version": 1,
        "manager_id": manager_id,
        "backend": "systemd",
        "executable": executable,
        "executable_digest": binary_digest,
        "backend_spec_path": spec,
        "backend_spec_digest": unit_digest,
        "config_path": (f"/opt/wruntime/wr-manager/config/manager.toml" if initial else f"/var/lib/wruntime/manager-config/{manager_id}/current.toml"),
        "config_digest": config_digest,
        "credential_set_path": credential_path,
        "credential_digest": credential_digest,
    }
    return json.dumps(value, indent=2, separators=(",", ": ")).encode()


def host_digest(target: dict[str, str]) -> str:
    fields = ("manager_id", "endpoint", "remote", "backend", "executable_digest", "backend_spec_digest", "config_digest", "credential_digest", "old_selector_digest", "new_selector_digest")
    return digest_bytes(json.dumps({key: target[key] for key in fields}, separators=(",", ":")).encode())


def quote(value: str) -> str:
    return json.dumps(value)


def write_manifest(path: Path, operation_id: str, executor_id: str, manager_endpoint: str, policy: Path, deployment_certificate: str, source: dict[str, str], target: dict[str, str], ssh_key: str) -> None:
    lines = [
        "schema_version = 1",
        f"client_operation_id = {quote(operation_id)}",
        f"executor_id = {quote(executor_id)}",
        'cluster_id = "deployment"',
        f"manager_endpoint = {quote(manager_endpoint)}",
        f"target_policy = {quote(str(policy.resolve()))}",
        f"deployment_certificate = {quote(deployment_certificate)}",
        "max_parallel = 1",
        f"ssh_key = {quote(str(Path(ssh_key).resolve()))}",
        "",
        "[[sources]]",
    ]
    lines.extend(f"{key} = {quote(source[key])}" for key in ("manager_id", "endpoint", "remote", "host_digest", "selector_digest"))
    lines.extend(["", "[[targets]]"])
    for key in ("manager_id", "endpoint", "remote"):
        lines.append(f"{key} = {quote(target[key])}")
    lines.append('backend = "systemd"')
    for key in ("executable", "executable_digest", "backend_spec", "backend_spec_digest", "config", "config_digest", "credential_set", "credential_digest", "old_selector_digest", "new_selector_digest", "host_digest"):
        lines.append(f"{key} = {quote(target[key])}")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("base-policy", "manager-a-template", "manager-b-template", "initial-a-config", "binary", "unit", "a-initial-credential", "a-set", "b-set", "output-dir", "a-endpoint", "b-endpoint", "a-remote", "b-remote", "db-url", "ssh-key"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    binary, unit = Path(args.binary), Path(args.unit)
    binary_digest, unit_digest = digest_file(binary), digest_file(unit)
    base_policy = Path(args.base_policy).read_text()
    policies = {}
    for generation, manager_id, endpoint in ((2, "manager-b", args.b_endpoint), (3, "manager-a", args.a_endpoint)):
        path = output / f"policy-generation-{generation}.toml"
        path.write_text(render_policy(base_policy, generation, manager_id, endpoint))
        policies[generation] = path
    sets = {2: Path(args.b_set), 3: Path(args.a_set)}
    for generation in (2, 3):
        shutil.copyfile(policies[generation], sets[generation] / "authorization.toml")
    configs = {}
    for generation, manager_id, endpoint, template_path in (
        (2, "manager-b", args.b_endpoint, args.manager_b_template),
        (3, "manager-a", args.a_endpoint, args.manager_a_template),
    ):
        set_name = sets[generation].name
        config = output / f"manager-{manager_id[-1]}-generation-{generation}.toml"
        config.write_text(render_config(Path(template_path).read_text(), manager_id, endpoint, args.db_url, set_name))
        configs[generation] = config
    initial_config = Path(args.initial_a_config)
    initial_descriptor = descriptor("manager-a", binary_digest, unit_digest, digest_file(initial_config), "/etc/wruntime/pki/manager-endpoint/sets/v1", tree_digest(Path(args.a_initial_credential)), initial=True)
    initial_selector = digest_bytes(initial_descriptor)
    (output / "manager-a-initial-activation.json").write_bytes(initial_descriptor)
    targets = {}
    for generation, manager_id, endpoint, remote, old_selector in (
        (2, "manager-b", args.b_endpoint, args.b_remote, ZERO_DIGEST),
        (3, "manager-a", args.a_endpoint, args.a_remote, initial_selector),
    ):
        credential_digest = tree_digest(sets[generation])
        new_descriptor = descriptor(manager_id, binary_digest, unit_digest, digest_file(configs[generation]), f"/etc/wruntime/pki/manager-{manager_id}/sets/{sets[generation].name}", credential_digest)
        new_selector = digest_bytes(new_descriptor)
        target = {
            "manager_id": manager_id, "endpoint": endpoint, "remote": remote, "backend": "systemd",
            "executable": str(binary.resolve()), "executable_digest": binary_digest,
            "backend_spec": str(unit.resolve()), "backend_spec_digest": unit_digest,
            "config": str(configs[generation].resolve()), "config_digest": digest_file(configs[generation]),
            "credential_set": str(sets[generation].resolve()), "credential_digest": credential_digest,
            "old_selector_digest": old_selector, "new_selector_digest": new_selector,
        }
        target["host_digest"] = host_digest(target)
        targets[generation] = target
    a_to_b = output / "manager-a-to-b-systemd.toml"
    b_to_a = output / "manager-b-to-a-systemd.toml"
    write_manifest(a_to_b, "11111111-1111-4111-8111-111111111111", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", args.a_endpoint, policies[2], "deployment-manager-b-gen2", {"manager_id": "manager-a", "endpoint": args.a_endpoint, "remote": args.a_remote, "host_digest": initial_selector, "selector_digest": initial_selector}, targets[2], args.ssh_key)
    write_manifest(b_to_a, "33333333-3333-4333-8333-333333333333", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", args.b_endpoint, policies[3], "deployment-manager-a-gen3", {"manager_id": "manager-b", "endpoint": args.b_endpoint, "remote": args.b_remote, "host_digest": targets[2]["host_digest"], "selector_digest": targets[2]["new_selector_digest"]}, targets[3], args.ssh_key)
    summary = {
        "initial_a_selector_digest": initial_selector,
        "policies": {str(key): str(value.resolve()) for key, value in policies.items()},
        "manifests": {"a-to-b": str(a_to_b.resolve()), "b-to-a": str(b_to_a.resolve())},
        "targets": {str(key): value for key, value in targets.items()},
        "manager_b_db_url_rendered": True,
    }
    (output / "manager-rollout-artifacts.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
