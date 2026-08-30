#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name("proxmox.py")
CONFIG_PATH = Path(__file__).parents[1] / "deployment-e2e.toml"
SPEC = importlib.util.spec_from_file_location("deployment_proxmox", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load Proxmox provider test target")
pve = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = pve
SPEC.loader.exec_module(pve)


def load_config_text(text):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".toml") as config_file:
        config_file.write(text)
        config_file.flush()
        return pve.load_config(Path(config_file.name))


def config():
    return pve.Config(
        "proxmox", "server", "wr-e2e-v1", "wr-e2e-v1",
        "/var/lock/wruntime-deployment-e2e.lock", "/opt/wruntime", "wr-e2e-node",
        (
            pve.Target("manager", 120, "wr-e2e-manager", "192.0.2.10", "deploy", "manager"),
            pve.Target("node", 121, "wr-e2e-node", "192.0.2.11", "deploy", "node"),
        ),
    )


class FakeClient:
    def __init__(self, states=None):
        self.states = states or {120: "running", 121: "running"}
        self.names = {120: "wr-e2e-manager", 121: "wr-e2e-node"}
        self.snapshots = {120: ["wr-e2e-v1"], 121: ["wr-e2e-v1"]}
        self.operations = []
        self.tasks = {}
        self.fail_operation: tuple[str, int] | None = None

    def version(self): return {"version": "8.4.1"}
    def vm_config(self, node, vmid): return {"name": self.names[vmid]}
    def vm_snapshots(self, node, vmid): return [{"name": x} for x in self.snapshots[vmid]]
    def vm_status(self, node, vmid): return {"status": self.states[vmid]}

    def _task(self, operation, vmid, result):
        self.operations.append((operation, vmid))
        upid = f"UPID:server:{operation}:{vmid}"
        exitstatus = "ERROR" if self.fail_operation == (operation, vmid) else "OK"
        self.tasks[upid] = {"status": "stopped", "exitstatus": exitstatus}
        if exitstatus == "OK":
            result()
        return upid

    def stop(self, node, vmid): return self._task("stop", vmid, lambda: self.states.__setitem__(vmid, "stopped"))
    def rollback(self, node, vmid, snapshot): return self._task("rollback", vmid, lambda: self.states.__setitem__(vmid, "stopped"))
    def start(self, node, vmid): return self._task("start", vmid, lambda: self.states.__setitem__(vmid, "running"))
    def task_status(self, node, upid): return self.tasks[upid]


def marker(target):
    return {"image_version": "wr-e2e-v1", "role": target.role, "hostname": target.name}


class ProviderTests(unittest.TestCase):
    def provider(self, fake, marker_reader=marker, **kwargs):
        return pve.LifecycleProvider(config(), fake, marker_reader, sleep=lambda _: None, **kwargs)

    def test_ca_bundle_defaults_to_system_path_and_accepts_override(self):
        with tempfile.NamedTemporaryFile() as default_bundle:
            with (
                mock.patch.object(pve, "DEFAULT_CA_BUNDLE", Path(default_bundle.name)),
                mock.patch.dict(os.environ, {"PVE_CA_BUNDLE": ""}),
            ):
                self.assertEqual(pve.ca_bundle_path(), Path(default_bundle.name))

        with tempfile.NamedTemporaryFile() as override_bundle:
            with mock.patch.dict(os.environ, {"PVE_CA_BUNDLE": override_bundle.name}):
                self.assertEqual(pve.ca_bundle_path(), Path(override_bundle.name))

    def test_ca_bundle_must_exist(self):
        with mock.patch.dict(os.environ, {"PVE_CA_BUNDLE": "/missing/proxmox-ca-bundle.pem"}):
            with self.assertRaisesRegex(pve.ProviderError, "PVE_CA_BUNDLE"):
                pve.ca_bundle_path()

    def test_successful_stop_rollback_start_task_order(self):
        fake = FakeClient()
        result = self.provider(fake).reset(start=True)
        self.assertEqual(fake.operations, [
            ("stop", 120), ("stop", 121),
            ("rollback", 120), ("rollback", 121),
            ("start", 120), ("start", 121),
        ])
        self.assertEqual([target["status"] for target in result["targets"]], ["running", "running"])

    def test_already_stopped_vms_do_not_issue_stop_tasks(self):
        fake = FakeClient({120: "stopped", 121: "stopped"})
        self.provider(fake).reset(start=False)
        self.assertEqual(fake.operations, [("rollback", 120), ("rollback", 121)])

    def test_missing_snapshot_is_fatal_before_mutation(self):
        fake = FakeClient()
        fake.snapshots[121] = []
        with self.assertRaisesRegex(pve.ProviderError, "missing snapshot"):
            self.provider(fake).reset(start=True)
        self.assertEqual(fake.operations, [])

    def test_wrong_vm_name_is_fatal_before_mutation(self):
        fake = FakeClient()
        fake.names[120] = "unrelated-vm"
        with self.assertRaisesRegex(pve.ProviderError, "name mismatch"):
            self.provider(fake).reset(start=True)
        self.assertEqual(fake.operations, [])

    def test_failed_upid_exit_status_is_fatal(self):
        fake = FakeClient()
        fake.fail_operation = ("stop", 120)
        with self.assertRaisesRegex(pve.ProviderError, "exitstatus"):
            self.provider(fake).reset(start=True)
        self.assertIn(("stop", 121), fake.operations)

    def test_task_timeout_is_fatal(self):
        fake = FakeClient()
        fake.tasks["UPID:server:stuck:120"] = {"status": "running"}
        ticks = iter((0.0, 2.0, 2.0))
        provider = self.provider(fake, monotonic=lambda: next(ticks), task_timeout=1)
        with self.assertRaisesRegex(pve.ProviderError, "timed out"):
            provider._wait_task(config().targets[0], "UPID:server:stuck:120", "stop")

    def test_marker_mismatch_is_fatal(self):
        fake = FakeClient()
        def wrong_marker(target):
            value = marker(target)
            if target.vmid == 121:
                value["role"] = "manager"
            return value
        with self.assertRaisesRegex(pve.ProviderError, "marker verification failed"):
            self.provider(fake, wrong_marker).reset(start=True)

    def test_partial_two_vm_failure_attempts_both_targets(self):
        fake = FakeClient()
        fake.fail_operation = ("rollback", 120)
        with self.assertRaisesRegex(pve.ProviderError, "rollback failed"):
            self.provider(fake).reset(start=False)
        self.assertIn(("rollback", 121), fake.operations)

    def test_secret_redaction(self):
        token = f"redaction-fixture-{self.id()}"
        message = pve.redact(
            f"Authorization: PVEAPIToken={token} token_value={token} "
            "postgres://user:password@example.invalid/db",
            (token,),
        )
        self.assertNotIn(token, message)
        self.assertNotIn("password", message)
        self.assertIn("<redacted>", message)

    def test_checked_in_configuration_is_valid_and_rejects_duplicate_ids(self):
        checked_in = pve.load_config(CONFIG_PATH)
        self.assertEqual([target.vmid for target in checked_in.targets], [120, 121])
        text = CONFIG_PATH.read_text()
        with self.assertRaisesRegex(pve.ProviderError, "VM IDs must differ"):
            load_config_text(re.sub(r"vmid\s*=\s*121", "vmid = 120", text))

    def test_configuration_rejects_duplicate_hosts(self):
        text = CONFIG_PATH.read_text().replace('host     = "192.168.178.78"', 'host     = "192.168.178.77"')
        with self.assertRaisesRegex(pve.ProviderError, "hosts must differ"):
            load_config_text(text)

    def test_configuration_rejects_unsafe_workdir(self):
        text = CONFIG_PATH.read_text().replace('workdir                = "/opt/wruntime"', 'workdir                = "/"')
        with self.assertRaisesRegex(pve.ProviderError, "safe absolute path"):
            load_config_text(text)

    def test_configuration_rejects_placeholder_and_empty_versions(self):
        text = CONFIG_PATH.read_text().replace('snapshot               = "wr-e2e-v1"', 'snapshot               = "<snapshot>"')
        with self.assertRaisesRegex(pve.ProviderError, "placeholder"):
            load_config_text(text)
        text = CONFIG_PATH.read_text().replace('expected_image_version = "wr-e2e-v1"', 'expected_image_version = ""')
        with self.assertRaisesRegex(pve.ProviderError, "must be non-empty"):
            load_config_text(text)

    def test_configuration_rejects_unexpected_vm_names(self):
        text = CONFIG_PATH.read_text().replace('name     = "wr-e2e-node"', 'name     = "unrelated-node"')
        with self.assertRaisesRegex(pve.ProviderError, "node.name must be"):
            load_config_text(text)


if __name__ == "__main__":
    unittest.main()
