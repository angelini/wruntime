#!/usr/bin/env python3
"""Assert stable fields in `wr-cli cluster status --output json`."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


class AssertionFailure(RuntimeError):
    pass


def load_status(path: str) -> dict[str, Any]:
    try:
        value = json.load(sys.stdin) if path == "-" else json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise AssertionFailure(f"cannot read cluster status JSON: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise AssertionFailure("cluster status schema_version must be 1")
    return value


def one(items: list[dict[str, Any]], description: str, predicate) -> dict[str, Any]:
    matches = [item for item in items if isinstance(item, dict) and predicate(item)]
    if len(matches) != 1:
        raise AssertionFailure(f"expected exactly one {description}, found {len(matches)}")
    return matches[0]


def node(status: dict[str, Any], node_id: str) -> dict[str, Any]:
    return one(status.get("nodes", []), f"node {node_id!r}", lambda item: item.get("node_id") == node_id)


def expected_module(deployment: dict[str, Any], slot: str, version: str) -> dict[str, Any]:
    engines = deployment.get("expected_engines", [])
    engine = one(engines, f"desired engine slot {slot!r}", lambda item: item.get("engine_slot") == slot)
    if len(engines) != 1:
        raise AssertionFailure("desired deployment engine inventory is not exact")
    modules = engine.get("modules", [])
    module = one(
        modules,
        f"deployment.echo@{version}",
        lambda item: item.get("namespace") == "deployment" and item.get("name") == "echo" and item.get("version") == version,
    )
    if len(modules) != 1:
        raise AssertionFailure("desired deployment module inventory is not exact")
    return module


def assert_routes(status: dict[str, Any], version: str) -> None:
    service = one(
        status.get("services", []),
        f"deployment.echo@{version} service",
        lambda item: isinstance(item.get("service"), dict)
        and item["service"].get("namespace") == "deployment"
        and item["service"].get("name") == "echo"
        and item["service"].get("version") == version,
    )
    if service.get("desired_routes") != 1 or service.get("healthy_routes") != 1 or service.get("unhealthy_routes") != 0:
        raise AssertionFailure("desired echo route counts are not exactly 1 healthy and 0 unhealthy")
    desired = [route for route in service.get("routes", []) if route.get("desired")]
    if len(desired) != 1 or not desired[0].get("healthy"):
        raise AssertionFailure("authoritative desired echo route is not healthy")


def assert_manager(status: dict[str, Any], address: str) -> dict[str, Any]:
    manager = one(status.get("managers", []), f"manager at {address!r}", lambda item: item.get("grpc_address") == address)
    if not manager.get("manager_id"):
        raise AssertionFailure("manager ID is empty")
    return {"manager_id": manager["manager_id"], "address": address}


def assert_desired(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    desired = selected.get("desired_deployment")
    if not isinstance(desired, dict):
        raise AssertionFailure("node has no desired deployment")
    if desired.get("state") != "succeeded" or desired.get("bundle_digest") != args.digest:
        raise AssertionFailure("desired deployment state or digest mismatch")
    expected_module(desired, args.engine_slot, args.version)
    revision = desired.get("revision")
    engines = [
        engine for engine in selected.get("engines", [])
        if isinstance(engine.get("deployment"), dict)
        and engine["deployment"].get("engine_slot") == args.engine_slot
        and engine["deployment"].get("revision") == revision
        and engine["deployment"].get("bundle_digest") == args.digest
        and engine.get("authoritative_for_desired_revision")
    ]
    engine = one(engines, "authoritative desired engine", lambda _: True)
    if engine.get("severity") != "healthy" or not engine.get("last_heartbeat"):
        raise AssertionFailure("authoritative engine is not freshly healthy")
    modules = engine.get("modules", [])
    module = one(
        modules, "authoritative echo module",
        lambda item: isinstance(item.get("module"), dict)
        and item["module"].get("namespace") == "deployment"
        and item["module"].get("name") == "echo"
        and item["module"].get("version") == args.version,
    )
    if len(modules) != 1:
        raise AssertionFailure("authoritative engine module inventory is not exact")
    if module.get("severity") != "healthy" or not module.get("last_healthy"):
        raise AssertionFailure("authoritative echo module is not freshly healthy")
    assert_routes(status, args.version)
    return {"revision": revision, "digest": args.digest, "version": args.version}


def assert_failed(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    desired = selected.get("desired_deployment") or {}
    if desired.get("state") != "succeeded" or desired.get("bundle_digest") != args.serving_digest:
        raise AssertionFailure("failed attempt changed the desired serving deployment")
    failed = [item for item in selected.get("deployment_history", []) if item.get("state") == "failed"]
    if args.after_revision is not None:
        failed = [item for item in failed if isinstance(item.get("revision"), int) and item["revision"] > args.after_revision]
    record = max(failed, key=lambda item: item.get("revision", -1), default=None)
    if not record or record.get("bundle_digest") != args.failed_digest:
        raise AssertionFailure("the expected failed deployment attempt was not recorded")
    if not isinstance(record.get("failure_detail"), str) or not (0 < len(record["failure_detail"]) <= 4096):
        raise AssertionFailure("no bounded failed deployment attempt was recorded")
    return {"failed_revision": record["revision"], "serving_revision": desired.get("revision")}


def codes(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "conditions" and isinstance(child, list):
                for item in child:
                    if isinstance(item, dict):
                        code = item.get("code")
                        if isinstance(code, str):
                            found.add(code)
            found.update(codes(child))
    elif isinstance(value, list):
        for child in value:
            found.update(codes(child))
    return found


def assert_unhealthy(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    if selected.get("severity") != "unhealthy":
        raise AssertionFailure("node did not converge to unhealthy")
    observed = codes(selected)
    expected = set(args.condition)
    if not observed.intersection(expected):
        raise AssertionFailure(f"none of the expected condition codes were present: {sorted(expected)}")
    return {"severity": "unhealthy", "condition_codes": sorted(observed.intersection(expected))}


def assert_rollback(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    desired = selected.get("desired_deployment") or {}
    revision = desired.get("revision")
    if not isinstance(revision, int) or revision <= args.after_revision:
        raise AssertionFailure("rollback did not create a monotonic new revision")
    if desired.get("source_revision") != args.source_revision:
        raise AssertionFailure("rollback source_revision mismatch")
    if desired.get("bundle_digest") != args.digest or desired.get("state") != "succeeded":
        raise AssertionFailure("rollback did not restore the successful source digest")
    desired_args = argparse.Namespace(node_id=args.node_id, digest=args.digest, version=args.version, engine_slot=args.engine_slot)
    assert_desired(status, desired_args)
    return {"revision": revision, "source_revision": args.source_revision, "digest": args.digest}


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--input", default="-", help="status JSON file, or - for stdin")
    commands = root.add_subparsers(dest="command", required=True)
    manager = commands.add_parser("manager")
    manager.add_argument("--address", required=True)
    for name in ("desired", "failed", "unhealthy", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--node-id", required=True)
        if name in {"desired", "rollback"}:
            command.add_argument("--digest", required=True)
            command.add_argument("--version", required=True)
            command.add_argument("--engine-slot", default="engine")
    failed = commands.choices["failed"]
    failed.add_argument("--serving-digest", required=True)
    failed.add_argument("--failed-digest", required=True)
    failed.add_argument("--after-revision", type=int)
    unhealthy = commands.choices["unhealthy"]
    unhealthy.add_argument(
        "--condition",
        action="append",
        default=["MISSING_ENGINE", "REVISION_MISMATCH", "STALE_ENGINE_HEARTBEAT", "STALE_MODULE_HEARTBEAT"],
    )
    rollback = commands.choices["rollback"]
    rollback.add_argument("--source-revision", required=True, type=int)
    rollback.add_argument("--after-revision", required=True, type=int)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        status = load_status(args.input)
        if args.command == "manager": result = assert_manager(status, args.address)
        elif args.command == "desired": result = assert_desired(status, args)
        elif args.command == "failed": result = assert_failed(status, args)
        elif args.command == "unhealthy": result = assert_unhealthy(status, args)
        else: result = assert_rollback(status, args)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except AssertionFailure as exc:
        print(f"cluster assertion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
