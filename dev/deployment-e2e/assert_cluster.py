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


def expected_slots(args) -> list[str]:
    value = args.engine_slot
    slots = [value] if isinstance(value, str) else list(value or ["engine"])
    if len(slots) != len(set(slots)):
        raise AssertionFailure("duplicate expected engine slot")
    return slots


def expected_modules(deployment: dict[str, Any], slots: list[str], version: str) -> None:
    engines = deployment.get("expected_engines", [])
    if len(engines) != len(slots) or {item.get("engine_slot") for item in engines} != set(slots):
        raise AssertionFailure("desired deployment engine inventory is not exact")
    for slot in slots:
        engine = one(engines, f"desired engine slot {slot!r}", lambda item: item.get("engine_slot") == slot)
        modules = engine.get("modules", [])
        one(
            modules,
            f"deployment.probe@{version}",
            lambda item: item.get("namespace") == "deployment" and item.get("name") == "probe" and item.get("version") == version,
        )
        if len(modules) != 1:
            raise AssertionFailure("desired deployment module inventory is not exact")


def assert_routes(
    status: dict[str, Any],
    version: str,
    desired_routes: int,
    healthy_routes: int | None = None,
    unhealthy_routes: int = 0,
) -> None:
    probe_services = [
        item for item in status.get("services", [])
        if isinstance(item.get("service"), dict)
        and item["service"].get("namespace") == "deployment"
        and item["service"].get("name") == "probe"
    ]
    service = one(
        probe_services,
        f"deployment.probe@{version} service",
        lambda item: item["service"].get("version") == version,
    )
    if len(probe_services) != 1:
        raise AssertionFailure("probe service version inventory is not exact")
    healthy_routes = desired_routes if healthy_routes is None else healthy_routes
    if (
        service.get("desired_routes") != desired_routes
        or service.get("healthy_routes") != healthy_routes
        or service.get("unhealthy_routes") != unhealthy_routes
    ):
        raise AssertionFailure("desired probe route counts do not match the exact inventory")
    routes = service.get("routes", [])
    desired = [route for route in routes if route.get("desired")]
    if len(routes) != desired_routes or len(desired) != desired_routes:
        raise AssertionFailure("authoritative desired probe route inventory is not exact")
    if sum(route.get("healthy") is True for route in desired) != healthy_routes:
        raise AssertionFailure("authoritative desired probe route health is not exact")


def assert_manager(status: dict[str, Any], address: str) -> dict[str, Any]:
    manager = one(status.get("managers", []), f"manager at {address!r}", lambda item: item.get("grpc_address") == address)
    if not manager.get("manager_id"):
        raise AssertionFailure("manager ID is empty")
    return {"manager_id": manager["manager_id"], "address": address}


def assert_proxy_health(
    status: dict[str, Any], selected_node: dict[str, Any], desired: dict[str, Any]
) -> dict[str, Any]:
    proxies = status.get("proxies", [])
    proxy = one(
        proxies,
        f"selected expected proxy for node {selected_node.get('node_id')!r}",
        lambda item: item.get("node_id") == selected_node.get("node_id")
        and item.get("expected") is True
        and item.get("selected") is True,
    )
    embedded = selected_node.get("proxies", [])
    embedded_proxy = one(
        embedded,
        "node-embedded selected expected proxy",
        lambda item: item.get("proxy_id") == proxy.get("proxy_id")
        and item.get("process_instance_id") == proxy.get("process_instance_id"),
    )
    if embedded_proxy != proxy:
        raise AssertionFailure("top-level and node-embedded proxy inventory differ")
    identity = proxy.get("deployment")
    if not isinstance(identity, dict):
        raise AssertionFailure("selected proxy has no managed deployment identity")
    exact = ("node_id", "revision", "bundle_digest", "operation_id", "revision_digest")
    if any(not identity.get(field) or identity.get(field) != desired.get(field) for field in exact):
        raise AssertionFailure("selected proxy deployment identity is not exact and non-circular")
    if (
        proxy.get("severity") != "healthy"
        or proxy.get("lifecycle") != "ready"
        or proxy.get("admission_open") is not True
        or not proxy.get("report_received_at")
        or not isinstance(proxy.get("report_age_seconds"), int)
    ):
        raise AssertionFailure("selected proxy is not fresh, READY, healthy, and admitting")
    listeners = proxy.get("listeners")
    if not isinstance(listeners, list):
        raise AssertionFailure("selected proxy listener evidence is missing")
    data_plane = one(listeners, "data-plane listener", lambda item: item.get("kind") == "data-plane")
    if data_plane.get("configured") is not True or data_plane.get("accepting") is not True:
        raise AssertionFailure("selected proxy data-plane listener is not accepting")
    if any(item.get("configured") is True and item.get("accepting") is not True for item in listeners):
        raise AssertionFailure("a required configured proxy listener is not accepting")
    routing = proxy.get("routing")
    known_managers = {item.get("manager_id") for item in status.get("managers", [])}
    if (
        not isinstance(routing, dict)
        or routing.get("severity") != "healthy"
        or routing.get("synchronized") is not True
        or routing.get("installed_table_version", -1) < status.get("routing_table_version", 0)
        or routing.get("source_manager_id") not in known_managers
        or not isinstance(routing.get("synchronization_age_seconds"), int)
    ):
        raise AssertionFailure("selected proxy routing is not current, fresh, and manager-backed")
    breakers = proxy.get("breakers")
    if not isinstance(breakers, list) or not breakers:
        raise AssertionFailure("selected proxy breaker summary is missing")
    for breaker in breakers:
        counts = [breaker.get(name) for name in ("total", "closed", "open", "half_open")]
        if not all(isinstance(value, int) and value >= 0 for value in counts):
            raise AssertionFailure("selected proxy breaker summary is malformed")
        if counts[0] != sum(counts[1:]) or breaker.get("severity") != "healthy":
            raise AssertionFailure("selected proxy breaker summary is not healthy and complete")
    return {"proxy_id": proxy.get("proxy_id"), "process_instance_id": proxy.get("process_instance_id")}


def assert_desired(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    desired = selected.get("desired_deployment")
    if not isinstance(desired, dict):
        raise AssertionFailure("node has no desired deployment")
    if desired.get("state") != "succeeded" or desired.get("bundle_digest") != args.digest:
        raise AssertionFailure("desired deployment state or digest mismatch")
    slots = expected_slots(args)
    expected_modules(desired, slots, args.version)
    revision = desired.get("revision")
    engines = [
        engine for engine in selected.get("engines", [])
        if isinstance(engine.get("deployment"), dict)
        and engine["deployment"].get("revision") == revision
        and engine["deployment"].get("bundle_digest") == args.digest
        and engine.get("authoritative_for_desired_revision")
    ]
    observed_slots = [
        engine.get("deployment", {}).get("engine_slot")
        for engine in selected.get("engines", [])
        if isinstance(engine.get("deployment"), dict)
    ]
    if len(observed_slots) != len(set(observed_slots)) or set(observed_slots) != set(slots):
        raise AssertionFailure("observed engine inventory is not exact (duplicate, extra, or stale slots)")
    if len(engines) != len(slots) or {engine["deployment"]["engine_slot"] for engine in engines} != set(slots):
        raise AssertionFailure("authoritative desired engine inventory is not exact")
    for engine in engines:
        if engine.get("severity") != "healthy" or not engine.get("last_heartbeat"):
            raise AssertionFailure("authoritative engine is not freshly healthy")
        modules = engine.get("modules", [])
        module = one(
            modules, "authoritative probe module",
            lambda item: isinstance(item.get("module"), dict)
            and item["module"].get("namespace") == "deployment"
            and item["module"].get("name") == "probe"
            and item["module"].get("version") == args.version,
        )
        if len(modules) != 1:
            raise AssertionFailure("authoritative engine module inventory is not exact")
        if module.get("severity") != "healthy" or not module.get("last_healthy"):
            raise AssertionFailure("authoritative probe module is not freshly healthy")
    assert_routes(status, args.version, len(slots))
    assert_proxy_health(status, selected, desired)
    return {"revision": revision, "digest": args.digest, "version": args.version, "engine_slots": slots}


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


def assert_abandoned(status: dict[str, Any], args) -> dict[str, Any]:
    selected = node(status, args.node_id)
    desired = selected.get("desired_deployment") or {}
    if desired.get("state") != "succeeded" or desired.get("bundle_digest") != args.serving_digest:
        raise AssertionFailure("abandoned attempt changed the desired serving deployment")
    abandoned = [
        item
        for item in selected.get("deployment_history", [])
        if item.get("state") == "abandoned"
        and item.get("bundle_digest") == args.abandoned_digest
        and isinstance(item.get("revision"), int)
        and item["revision"] > args.after_revision
    ]
    record = max(abandoned, key=lambda item: item["revision"], default=None)
    if record is None:
        raise AssertionFailure("the expected abandoned deployment attempt was not recorded")
    return {"abandoned_revision": record["revision"], "serving_revision": desired.get("revision")}


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
    if getattr(args, "version", None) is not None:
        assert_routes(status, args.version, args.desired_routes, args.healthy_routes, args.unhealthy_routes)
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
    for name in ("desired", "failed", "abandoned", "unhealthy", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--node-id", required=True)
        if name in {"desired", "rollback"}:
            command.add_argument("--digest", required=True)
            command.add_argument("--version", required=True)
            command.add_argument("--engine-slot", action="append")
    failed = commands.choices["failed"]
    failed.add_argument("--serving-digest", required=True)
    failed.add_argument("--failed-digest", required=True)
    failed.add_argument("--after-revision", type=int)
    abandoned = commands.choices["abandoned"]
    abandoned.add_argument("--serving-digest", required=True)
    abandoned.add_argument("--abandoned-digest", required=True)
    abandoned.add_argument("--after-revision", required=True, type=int)
    unhealthy = commands.choices["unhealthy"]
    unhealthy.add_argument(
        "--condition",
        action="append",
        default=["MISSING_ENGINE", "REVISION_MISMATCH", "STALE_ENGINE_HEARTBEAT", "STALE_MODULE_HEARTBEAT"],
    )
    unhealthy.add_argument("--version")
    unhealthy.add_argument("--desired-routes", type=int, default=1)
    unhealthy.add_argument("--healthy-routes", type=int, default=0)
    unhealthy.add_argument("--unhealthy-routes", type=int, default=1)
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
        elif args.command == "abandoned": result = assert_abandoned(status, args)
        elif args.command == "unhealthy": result = assert_unhealthy(status, args)
        else: result = assert_rollback(status, args)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except AssertionFailure as exc:
        print(f"cluster assertion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
