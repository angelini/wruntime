#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

SPEC = importlib.util.spec_from_file_location("assert_cluster", Path(__file__).with_name("assert_cluster.py"))
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load assert_cluster test target")
assert_cluster = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = assert_cluster
SPEC.loader.exec_module(assert_cluster)


def deployment(revision=3, digest="sha256:a", version="1.0.0", state="succeeded", source=0):
    return {
        "node_id": "wr-e2e-node", "revision": revision, "attempt_token": "attempt",
        "bundle_digest": digest, "operation_id": f"operation-{revision}",
        "revision_digest": f"revision-{revision}", "state": state, "source_revision": source,
        "expected_engines": [{"engine_slot": "engine", "modules": [
            {"namespace": "deployment", "name": "probe", "version": version}
        ]}],
        "failure_detail": "staging certificate directory missing" if state == "failed" else "",
    }


def healthy_status(revision=3, digest="sha256:a", version="1.0.0", source=0):
    desired = deployment(revision, digest, version, source=source)
    module = {"module": {"namespace": "deployment", "name": "probe", "version": version}, "severity": "healthy", "last_healthy": {"seconds": 1}, "conditions": []}
    engine = {
        "engine_id": "engine-id", "severity": "healthy", "authoritative_for_desired_revision": True,
        "last_heartbeat": {"seconds": 1}, "conditions": [], "modules": [module],
        "deployment": {"node_id": "wr-e2e-node", "revision": revision, "bundle_digest": digest, "engine_slot": "engine"},
    }
    proxy = {
        "proxy_id": "urn:wruntime:test:proxy", "node_id": "wr-e2e-node",
        "process_instance_id": "proxy-process", "severity": "healthy",
        "expected": True, "selected": True, "superseded": False,
        "deployment": {
            "node_id": "wr-e2e-node", "revision": revision, "bundle_digest": digest,
            "operation_id": f"operation-{revision}", "revision_digest": f"revision-{revision}",
        },
        "lifecycle": "ready", "admission_open": True,
        "report_received_at": {"seconds": 1}, "report_age_seconds": 0,
        "receiving_manager_id": "manager-id", "conditions": [],
        "listeners": [{"kind": "data-plane", "configured": True, "accepting": True,
                       "severity": "healthy", "conditions": []}],
        "routing": {"installed_table_version": 7, "synchronization_age_seconds": 0,
                    "source_manager_id": "manager-id", "synchronized": True,
                    "severity": "healthy", "conditions": []},
        "breakers": [{"destination_kind": "local-engine", "total": 0, "closed": 0,
                      "open": 0, "half_open": 0, "severity": "healthy", "conditions": []}],
    }
    return {
        "schema_version": 2, "severity": "healthy", "routing_table_version": 7,
        "managers": [{"manager_id": "manager-id", "grpc_address": "https://192.0.2.10:9000"}],
        "nodes": [{"node_id": "wr-e2e-node", "severity": "healthy", "desired_deployment": desired, "deployment_history": [desired], "engines": [engine], "proxies": [proxy], "conditions": []}],
        "engines": [engine], "proxies": [proxy],
        "services": [{
            "service": {"namespace": "deployment", "name": "probe", "version": version},
            "severity": "healthy", "desired_routes": 1, "healthy_routes": 1, "unhealthy_routes": 0,
            "routes": [{"desired": True, "healthy": True, "conditions": []}], "conditions": [],
        }],
    }


class AssertionTests(unittest.TestCase):
    def test_manager_and_desired_success(self):
        status = healthy_status()
        self.assertEqual(assert_cluster.assert_manager(status, "https://192.0.2.10:9000")["manager_id"], "manager-id")
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot="engine")
        self.assertEqual(assert_cluster.assert_desired(status, args)["revision"], 3)

    def test_proxy_assertions_reject_duplicate_stale_mismatch_and_malformed_evidence(self):
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot="engine")
        duplicate = healthy_status()
        duplicate["proxies"].append(copy.deepcopy(duplicate["proxies"][0]))
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "exactly one"):
            assert_cluster.assert_desired(duplicate, args)

        stale = healthy_status()
        stale["proxies"][0]["severity"] = "degraded"
        stale["nodes"][0]["proxies"][0]["severity"] = "degraded"
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "not fresh"):
            assert_cluster.assert_desired(stale, args)

        mismatch = healthy_status()
        mismatch["proxies"][0]["deployment"]["revision_digest"] = "revision-other"
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "not exact"):
            assert_cluster.assert_desired(mismatch, args)

        malformed = healthy_status()
        malformed["proxies"][0]["breakers"][0]["total"] = 1
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "not healthy and complete"):
            assert_cluster.assert_desired(malformed, args)

    def test_failed_attempt_preserves_serving_revision(self):
        status = healthy_status()
        status["nodes"][0]["deployment_history"].append(deployment(4, "sha256:b", "2.0.0", "failed"))
        args = argparse.Namespace(
            node_id="wr-e2e-node", serving_digest="sha256:a",
            failed_digest="sha256:b", after_revision=3,
        )
        self.assertEqual(assert_cluster.assert_failed(status, args)["failed_revision"], 4)

    def test_failed_attempt_requires_expected_bundle(self):
        status = healthy_status()
        status["nodes"][0]["deployment_history"].append(deployment(4, "sha256:other", "2.0.0", "failed"))
        args = argparse.Namespace(
            node_id="wr-e2e-node", serving_digest="sha256:a",
            failed_digest="sha256:b", after_revision=3,
        )
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "expected failed"):
            assert_cluster.assert_failed(status, args)

    def test_unhealthy_uses_condition_codes(self):
        status = healthy_status()
        status["nodes"][0]["severity"] = "unhealthy"
        status["nodes"][0]["engines"][0]["conditions"] = [{"code": "STALE_ENGINE_HEARTBEAT", "detail": "ignored"}]
        args = argparse.Namespace(node_id="wr-e2e-node", condition=["STALE_ENGINE_HEARTBEAT"])
        self.assertEqual(assert_cluster.assert_unhealthy(status, args)["severity"], "unhealthy")

    def test_unhealthy_defaults_accept_clean_engine_deregistration(self):
        status = healthy_status()
        status["nodes"][0]["severity"] = "unhealthy"
        status["nodes"][0]["engines"] = []
        status["nodes"][0]["conditions"] = [{"code": "MISSING_ENGINE", "detail": "ignored"}]
        args = assert_cluster.parser().parse_args(["unhealthy", "--node-id", "wr-e2e-node"])
        result = assert_cluster.assert_unhealthy(status, args)
        self.assertEqual(result["condition_codes"], ["MISSING_ENGINE"])

    def test_unhealthy_requires_explicit_drained_route_counts(self):
        status = healthy_status()
        status["nodes"][0]["severity"] = "unhealthy"
        status["nodes"][0]["conditions"] = [{"code": "MISSING_ENGINE", "detail": "ignored"}]
        status["services"][0]["healthy_routes"] = 0
        status["services"][0]["unhealthy_routes"] = 1
        status["services"][0]["routes"][0]["healthy"] = False
        args = assert_cluster.parser().parse_args([
            "unhealthy", "--node-id", "wr-e2e-node", "--version", "1.0.0",
            "--desired-routes", "1", "--healthy-routes", "0", "--unhealthy-routes", "1",
        ])
        self.assertEqual(assert_cluster.assert_unhealthy(status, args)["severity"], "unhealthy")
        status["services"][0]["unhealthy_routes"] = 0
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "route counts"):
            assert_cluster.assert_unhealthy(status, args)

    def test_unhealthy_defaults_accept_deregistration_with_an_old_revision(self):
        status = healthy_status()
        status["nodes"][0]["severity"] = "unhealthy"
        old_engine = status["nodes"][0]["engines"][0]
        old_engine["deployment"]["revision"] = 1
        old_engine["authoritative_for_desired_revision"] = False
        old_engine["severity"] = "degraded"
        status["nodes"][0]["conditions"] = [{"code": "REVISION_MISMATCH", "detail": "ignored"}]
        args = assert_cluster.parser().parse_args(["unhealthy", "--node-id", "wr-e2e-node"])
        result = assert_cluster.assert_unhealthy(status, args)
        self.assertEqual(result["condition_codes"], ["REVISION_MISMATCH"])

    def test_rollback_is_monotonic_and_restores_source(self):
        status = healthy_status(7, source=3)
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot="engine", source_revision=3, after_revision=6)
        self.assertEqual(assert_cluster.assert_rollback(status, args)["revision"], 7)

    def test_mismatch_fails_without_using_detail_strings(self):
        status = healthy_status()
        status["services"][0]["healthy_routes"] = 0
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot="engine")
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "route counts"):
            assert_cluster.assert_desired(status, args)

    def test_repeated_slots_require_exact_multi_engine_inventory(self):
        status = healthy_status()
        second = copy.deepcopy(status["nodes"][0]["engines"][0])
        second["engine_id"] = "engine-id-b"
        second["deployment"]["engine_slot"] = "engine-b"
        status["nodes"][0]["engines"].append(second)
        status["engines"].append(second)
        status["nodes"][0]["desired_deployment"]["expected_engines"].append({
            "engine_slot": "engine-b",
            "modules": [{"namespace": "deployment", "name": "probe", "version": "1.0.0"}],
        })
        status["services"][0]["desired_routes"] = 2
        status["services"][0]["healthy_routes"] = 2
        status["services"][0]["routes"].append({"desired": True, "healthy": True, "conditions": []})
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot=["engine", "engine-b"])
        self.assertEqual(assert_cluster.assert_desired(status, args)["engine_slots"], ["engine", "engine-b"])
        omitted = copy.deepcopy(status)
        omitted["nodes"][0]["engines"].pop()
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "inventory is not exact"):
            assert_cluster.assert_desired(omitted, args)
        extra_args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot=["engine"])
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "inventory is not exact"):
            assert_cluster.assert_desired(status, extra_args)
        duplicate_args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot=["engine", "engine"])
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "duplicate expected"):
            assert_cluster.assert_desired(status, duplicate_args)

    def test_extra_authoritative_module_fails_exact_inventory(self):
        status = healthy_status()
        status["nodes"][0]["engines"][0]["modules"].append({
            "module": {"namespace": "deployment", "name": "unexpected", "version": "1.0.0"},
            "severity": "healthy", "last_healthy": {"seconds": 1}, "conditions": [],
        })
        args = argparse.Namespace(node_id="wr-e2e-node", digest="sha256:a", version="1.0.0", engine_slot="engine")
        with self.assertRaisesRegex(assert_cluster.AssertionFailure, "inventory is not exact"):
            assert_cluster.assert_desired(status, args)


if __name__ == "__main__":
    unittest.main()
