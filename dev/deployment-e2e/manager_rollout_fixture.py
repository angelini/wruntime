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
    return json.dumps(value, indent=2, separators=(",", ": "), sort_keys=initial).encode()


def host_digest(target: dict[str, str]) -> str:
    fields = ("manager_id", "endpoint", "remote", "backend", "executable_digest", "backend_spec_digest", "config_digest", "credential_digest", "old_selector_digest", "new_selector_digest")
    return digest_bytes(json.dumps({key: target[key] for key in fields}, separators=(",", ":")).encode())


def quote(value: str) -> str:
    return json.dumps(value)


def write_manifest(
    path: Path,
    operation_id: str,
    manager_endpoint: str,
    policy: Path,
    deployment_certificate: str,
    sources: list[dict[str, str]],
    targets: list[dict[str, str]],
    ssh_key: str,
) -> None:
    lines = [
        "schema_version = 1",
        f"client_operation_id = {quote(operation_id)}",
        'cluster_id = "deployment"',
        f"manager_endpoint = {quote(manager_endpoint)}",
        f"target_policy = {quote(str(policy.resolve()))}",
        f"deployment_certificate = {quote(deployment_certificate)}",
        "max_parallel = 1",
        f"ssh_key = {quote(str(Path(ssh_key).resolve()))}",
    ]
    for source in sources:
        lines.extend(["", "[[sources]]"])
        lines.extend(f"{key} = {quote(source[key])}" for key in ("manager_id", "endpoint", "remote", "host_digest", "selector_digest"))
    for target in targets:
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
    scenarios = {
        "a-to-b": (2, "manager-b", args.b_endpoint, args.b_remote, Path(args.b_set), args.manager_b_template),
        "b-to-a": (3, "manager-a", args.a_endpoint, args.a_remote, Path(args.a_set), args.manager_a_template),
        "failed-closed": (4, "manager-b", args.b_endpoint, args.b_remote, output / "credential-sets" / "failed-gen4-manager-b", args.manager_b_template),
        "fresh": (4, "manager-a", args.a_endpoint, args.a_remote, output / "credential-sets" / "fresh-gen4-manager-a", args.manager_a_template),
    }
    shutil.copytree(args.b_set, scenarios["failed-closed"][4])
    shutil.copytree(args.a_set, scenarios["fresh"][4])
    policies: dict[str, Path] = {}
    configs: dict[str, Path] = {}
    for name, (generation, manager_id, endpoint, _remote, credential_set, template_path) in scenarios.items():
        policy = output / f"policy-{name}-generation-{generation}.toml"
        policy.write_text(render_policy(base_policy, generation, manager_id, endpoint))
        policies[name] = policy
        shutil.copyfile(policy, credential_set / "authorization.toml")
        config = output / f"manager-{manager_id[-1]}-{name}-generation-{generation}.toml"
        config.write_text(render_config(Path(template_path).read_text(), manager_id, endpoint, args.db_url, credential_set.name))
        configs[name] = config

    initial_config = Path(args.initial_a_config)
    initial_descriptor = descriptor("manager-a", binary_digest, unit_digest, digest_file(initial_config), "/etc/wruntime/pki/manager-endpoint/sets/v1", tree_digest(Path(args.a_initial_credential)), initial=True)
    initial_selector = digest_bytes(initial_descriptor)
    (output / "manager-a-initial-activation.json").write_bytes(initial_descriptor)

    targets: dict[str, dict[str, str]] = {}
    old_selectors = {"a-to-b": ZERO_DIGEST}
    for name in ("a-to-b", "b-to-a", "failed-closed", "fresh"):
        generation, manager_id, endpoint, remote, credential_set, _template = scenarios[name]
        if name == "b-to-a":
            old_selectors[name] = initial_selector
        elif name == "failed-closed":
            old_selectors[name] = targets["a-to-b"]["new_selector_digest"]
        elif name == "fresh":
            old_selectors[name] = targets["b-to-a"]["new_selector_digest"]
        credential_digest = tree_digest(credential_set)
        new_descriptor = descriptor(manager_id, binary_digest, unit_digest, digest_file(configs[name]), f"/etc/wruntime/pki/manager-{manager_id}/sets/{credential_set.name}", credential_digest)
        target = {
            "manager_id": manager_id, "endpoint": endpoint, "remote": remote, "backend": "systemd",
            "executable": str(binary.resolve()), "executable_digest": binary_digest,
            "backend_spec": str(unit.resolve()), "backend_spec_digest": unit_digest,
            "config": str(configs[name].resolve()), "config_digest": digest_file(configs[name]),
            "credential_set": str(credential_set.resolve()), "credential_digest": credential_digest,
            "old_selector_digest": old_selectors[name], "new_selector_digest": digest_bytes(new_descriptor),
        }
        target["host_digest"] = host_digest(target)
        targets[name] = target

    source_a_initial = {"manager_id": "manager-a", "endpoint": args.a_endpoint, "remote": args.a_remote, "host_digest": initial_selector, "selector_digest": initial_selector}
    source_b_gen2 = {"manager_id": "manager-b", "endpoint": args.b_endpoint, "remote": args.b_remote, "host_digest": targets["a-to-b"]["host_digest"], "selector_digest": targets["a-to-b"]["new_selector_digest"]}
    source_a_gen3 = {"manager_id": "manager-a", "endpoint": args.a_endpoint, "remote": args.a_remote, "host_digest": targets["b-to-a"]["host_digest"], "selector_digest": targets["b-to-a"]["new_selector_digest"]}
    manifests = {
        "a-to-b": output / "manager-a-to-b-systemd.toml",
        "b-to-a": output / "manager-b-to-a-systemd.toml",
        "failed-closed": output / "manager-failed-closed-systemd.toml",
        "fresh": output / "manager-fresh-systemd.toml",
    }
    write_manifest(manifests["a-to-b"], "11111111-1111-4111-8111-111111111111", args.a_endpoint, policies["a-to-b"], "deployment-manager-b-gen2", [source_a_initial], [targets["a-to-b"]], args.ssh_key)
    write_manifest(manifests["b-to-a"], "33333333-3333-4333-8333-333333333333", args.b_endpoint, policies["b-to-a"], "deployment-manager-a-gen3", [source_b_gen2], [targets["b-to-a"]], args.ssh_key)
    write_manifest(manifests["failed-closed"], "44444444-4444-4444-8444-444444444444", args.a_endpoint, policies["failed-closed"], "deployment-manager-b-failed-gen4", [source_a_gen3], [targets["failed-closed"]], args.ssh_key)
    write_manifest(manifests["fresh"], "55555555-5555-4555-8555-555555555555", args.a_endpoint, policies["fresh"], "deployment-manager-a-fresh-gen4", [], [targets["fresh"]], args.ssh_key)
    summary = {
        "initial_a_selector_digest": initial_selector,
        "policies": {key: str(value.resolve()) for key, value in policies.items()},
        "manifests": {key: str(value.resolve()) for key, value in manifests.items()},
        "targets": targets,
        "operation_ids": {
            "a-to-b": "11111111-1111-4111-8111-111111111111",
            "b-to-a": "33333333-3333-4333-8333-333333333333",
            "failed-closed": "44444444-4444-4444-8444-444444444444",
            "fresh": "55555555-5555-4555-8555-555555555555",
        },
        "manager_b_db_url_rendered": True,
    }
    (output / "manager-rollout-artifacts.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
