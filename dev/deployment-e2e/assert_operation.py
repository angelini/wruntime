#!/usr/bin/env python3
"""Assert graceful stop evidence from `wr-cli operations get --json`."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


class AssertionFailure(RuntimeError):
    pass


def load_operation(path: str) -> dict[str, Any]:
    try:
        value = json.load(sys.stdin) if path == "-" else json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise AssertionFailure(f"cannot read operation detail JSON: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise AssertionFailure("operation detail schema_version must be 1")
    return value


def assert_evidence(evidence: Any, label: str) -> None:
    if not isinstance(evidence, dict):
        raise AssertionFailure(f"{label} termination evidence is absent")
    if evidence.get("disposition") != "graceful":
        raise AssertionFailure(f"{label} termination disposition is not graceful")
    if evidence.get("graceful_termination_requested") is not True:
        raise AssertionFailure(f"{label} did not request graceful termination")
    if evidence.get("kill_escalated") is not False:
        raise AssertionFailure(f"{label} used kill escalation")
    if evidence.get("backend") not in {"systemd", "docker"}:
        raise AssertionFailure(f"{label} backend kind is invalid")
    backend = evidence.get("backend_instance_id")
    process = evidence.get("process_instance_id")
    if not isinstance(backend, str) or not backend:
        raise AssertionFailure(f"{label} backend identity is absent")
    if not isinstance(process, str) or not process:
        raise AssertionFailure(f"{label} process identity is absent")


def assert_operation(operation: dict[str, Any], args) -> dict[str, Any]:
    expected = {
        "node_id": args.node_id,
        "request_token": args.request_token,
        "action": args.action,
        "state": "succeeded",
    }
    for field, value in expected.items():
        if operation.get(field) != value:
            raise AssertionFailure(f"operation {field} mismatch")
    if args.target_revision is not None and operation.get("target_revision") != args.target_revision:
        raise AssertionFailure("operation target_revision mismatch")
    if args.target_digest is not None and operation.get("bundle_digest") != args.target_digest:
        raise AssertionFailure("operation target digest mismatch")

    expected_slots = list(args.stopped_engine_slot or [])
    if len(expected_slots) != len(set(expected_slots)):
        raise AssertionFailure("duplicate expected stopped engine slot")
    slots = operation.get("slots")
    if not isinstance(slots, list) or any(not isinstance(slot, dict) for slot in slots):
        raise AssertionFailure("operation slots are malformed")
    names = [slot.get("engine_slot") for slot in slots]
    if len(names) != len(set(names)):
        raise AssertionFailure("operation contains duplicate engine slots")
    slot_order = getattr(args, "slot_order", None)
    if slot_order and names != slot_order:
        raise AssertionFailure(f"operation slot order mismatch: expected {slot_order!r}, found {names!r}")
    stopped = [slot for slot in slots if slot.get("termination_evidence") is not None]
    stopped_names = [slot.get("engine_slot") for slot in stopped]
    if set(stopped_names) != set(expected_slots) or len(stopped_names) != len(expected_slots):
        raise AssertionFailure("stopped engine slot inventory is not exact")
    for slot in stopped:
        if slot.get("changed") is not True:
            raise AssertionFailure(f"engine slot {slot.get('engine_slot')!r} was not changed")
        assert_evidence(
            slot.get("termination_evidence"),
            f"engine slot {slot.get('engine_slot')!r}",
        )

    proxy = operation.get("proxy")
    if not isinstance(proxy, dict):
        raise AssertionFailure("operation proxy detail is malformed")
    proxy_evidence = proxy.get("termination_evidence")
    if args.expect_proxy_stop:
        if proxy.get("changed") is not True:
            raise AssertionFailure("proxy was not changed")
        assert_evidence(proxy_evidence, "proxy")
    elif proxy_evidence is not None:
        raise AssertionFailure("operation contains an unexpected proxy stop")
    return {
        "operation_id": operation.get("operation_id"),
        "stopped_engine_slots": sorted(expected_slots),
        "proxy_stopped": args.expect_proxy_stop,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--input", default="-", help="operation detail JSON file, or - for stdin")
    value.add_argument("--node-id", required=True)
    value.add_argument("--request-token", required=True)
    value.add_argument("--action", required=True, choices=["deployment", "restart", "rollback"])
    value.add_argument("--target-revision", type=int)
    value.add_argument("--target-digest")
    value.add_argument("--stopped-engine-slot", action="append")
    value.add_argument("--slot-order", action="append", help="expected rollout order")
    value.add_argument("--expect-proxy-stop", action="store_true")
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = assert_operation(load_operation(args.input), args)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except AssertionFailure as exc:
        print(f"operation assertion failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
