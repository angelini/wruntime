#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import copy
import importlib.util
import io
from pathlib import Path
import sys
import unittest

SPEC = importlib.util.spec_from_file_location("assert_operation", Path(__file__).with_name("assert_operation.py"))
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load assert_operation test target")
assert_operation = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = assert_operation
SPEC.loader.exec_module(assert_operation)


def evidence(backend="backend-a", process="process-a"):
    return {
        "backend": "systemd",
        "backend_instance_id": backend,
        "process_instance_id": process,
        "graceful_termination_requested": True,
        "kill_escalated": False,
        "disposition": "graceful",
        "terminal_result": "success",
        "exit_code": 0,
        "signal": None,
    }


def detail():
    return {
        "schema_version": 1,
        "operation_id": "operation-a",
        "node_id": "node-a",
        "request_token": "token-a",
        "actor": "operator-a",
        "action": "deployment",
        "state": "succeeded",
        "committed": True,
        "source_revision": 1,
        "target_revision": 2,
        "bundle_digest": "sha256:target",
        "resolved_release_digest": "sha256:resolved",
        "phase": "complete",
        "lease_epoch": 3,
        "affected_slots": ["blue"],
        "conditions": [],
        "slots": [{
            "engine_slot": "blue",
            "changed": True,
            "effect_reported": False,
            "pinned_backend_instance_id": "backend-new",
            "pinned_process_instance_id": "process-new",
            "effect_backend_instance_id": "",
            "effect_process_instance_id": "",
            "termination_evidence": evidence(),
        }],
        "proxy": {
            "changed": True,
            "effect_reported": False,
            "pinned_backend_instance_id": "proxy-backend-new",
            "pinned_process_instance_id": "proxy-process-new",
            "effect_backend_instance_id": "",
            "effect_process_instance_id": "",
            "termination_evidence": evidence("proxy-backend", "proxy-process"),
        },
    }


def args():
    return argparse.Namespace(
        node_id="node-a",
        request_token="token-a",
        action="deployment",
        target_revision=2,
        target_digest="sha256:target",
        stopped_engine_slot=["blue"],
        expect_proxy_stop=True,
    )


class OperationAssertionTests(unittest.TestCase):
    def test_accepts_exact_graceful_engine_and_proxy_stops(self):
        result = assert_operation.assert_operation(detail(), args())
        self.assertEqual(result["stopped_engine_slots"], ["blue"])
        self.assertTrue(result["proxy_stopped"])

    def test_missing_unknown_forced_and_escalated_evidence_fail_closed(self):
        mutations = (
            ("missing", lambda value: value["slots"][0].update(termination_evidence=None)),
            ("unknown", lambda value: value["slots"][0]["termination_evidence"].update(disposition="unknown")),
            ("forced", lambda value: value["slots"][0]["termination_evidence"].update(disposition="forced")),
            ("escalated", lambda value: value["slots"][0]["termination_evidence"].update(kill_escalated=True)),
            ("invalid-backend", lambda value: value["slots"][0]["termination_evidence"].update(backend="unknown")),
        )
        for name, mutate in mutations:
            value = detail()
            mutate(value)
            with self.subTest(name=name), self.assertRaises(assert_operation.AssertionFailure):
                assert_operation.assert_operation(value, args())

    def test_missing_evidence_identity_fails(self):
        for field in ("backend_instance_id", "process_instance_id"):
            value = detail()
            value["slots"][0]["termination_evidence"][field] = ""
            with self.subTest(field=field), self.assertRaisesRegex(
                assert_operation.AssertionFailure, "identity is absent"
            ):
                assert_operation.assert_operation(value, args())

    def test_duplicate_missing_and_extra_stopped_slots_fail(self):
        duplicate = detail()
        duplicate["slots"].append(copy.deepcopy(duplicate["slots"][0]))
        with self.assertRaisesRegex(assert_operation.AssertionFailure, "duplicate engine"):
            assert_operation.assert_operation(duplicate, args())
        missing = detail()
        missing["slots"] = []
        with self.assertRaisesRegex(assert_operation.AssertionFailure, "inventory is not exact"):
            assert_operation.assert_operation(missing, args())
        extra = detail()
        extra_slot = copy.deepcopy(extra["slots"][0])
        extra_slot["engine_slot"] = "green"
        extra["slots"].append(extra_slot)
        with self.assertRaisesRegex(assert_operation.AssertionFailure, "inventory is not exact"):
            assert_operation.assert_operation(extra, args())

    def test_wrong_operation_and_nonterminal_state_fail(self):
        for field, value in (("request_token", "wrong"), ("action", "drain"), ("node_id", "node-b"), ("state", "running")):
            operation = detail()
            operation[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(assert_operation.AssertionFailure, field):
                assert_operation.assert_operation(operation, args())
        operation = detail()
        operation["target_revision"] = 9
        with self.assertRaisesRegex(assert_operation.AssertionFailure, "target_revision"):
            assert_operation.assert_operation(operation, args())

    def test_unexpected_proxy_stop_and_expected_absence(self):
        operation = detail()
        expected = args()
        expected.expect_proxy_stop = False
        with self.assertRaisesRegex(assert_operation.AssertionFailure, "unexpected proxy"):
            assert_operation.assert_operation(operation, expected)
        operation["proxy"]["termination_evidence"] = None
        operation["proxy"]["changed"] = False
        self.assertFalse(assert_operation.assert_operation(operation, expected)["proxy_stopped"])

    def test_parser_accepts_only_current_action_vocabulary(self):
        base = ["--node-id", "node-a", "--request-token", "token-a"]
        for action in ("deployment", "restart", "rollback"):
            with self.subTest(action=action):
                parsed = assert_operation.parser().parse_args([*base, "--action", action])
                self.assertEqual(parsed.action, action)
        for action in ("initial-apply", "drain", "rolling-upgrade", "scale"):
            with self.subTest(action=action), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                assert_operation.parser().parse_args([*base, "--action", action])


if __name__ == "__main__":
    unittest.main()
