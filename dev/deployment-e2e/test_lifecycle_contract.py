#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "dev" / "validate-deployment-lifecycle.sh"
HELPER = ROOT / "dev" / "deployment-e2e" / "lifecycle_contract.sh"


class LifecycleContractTests(unittest.TestCase):
    def executable(self, path: Path, body: str) -> None:
        path.write_text("#!/usr/bin/env bash\nset -Eeuo pipefail\n" + body)
        path.chmod(0o700)

    def test_harness_builds_deployment_probe_from_owning_config(self):
        harness = HARNESS.read_text()
        build_command = "dev build --config wr-tests/deployment/scenarios/baseline/engine-1.toml"
        self.assertEqual(harness.count(build_command), 1)
        self.assertNotIn("examples/multi-node/echo", harness)

    def test_node_bundles_use_fresh_release_host_binaries(self):
        harness = HARNESS.read_text()
        workspace_build = harness.index("run_logged build-workspace cargo build")
        host_build = harness.index(
            "run_logged build-node-host-binaries cargo zigbuild --release "
            "--target x86_64-unknown-linux-gnu"
        )
        first_bundle = harness.index("run_logged baseline-one-bundle ")
        self.assertLess(workspace_build, host_build)
        self.assertLess(host_build, first_bundle)
        host_build_command = harness[
            host_build:harness.index('mkdir -p "$CERT_DIR"', host_build)
        ]
        self.assertIn("-p wr-proxy -p wr-engine -p wr-cli", host_build_command)
        bundle_lines = [
            line for line in harness.splitlines()
            if line.startswith("run_logged ") and "-bundle target/debug/wr-cli node bundle" in line
        ]
        self.assertEqual(len(bundle_lines), 4)
        self.assertTrue(all("--skip-build" in line for line in bundle_lines))

    def test_manager_b_database_preflight_follows_systemd_vm_readiness(self):
        harness = HARNESS.read_text()
        lifecycle = harness[harness.index("lifecycle() {"):]
        provider_reset = lifecycle.index(
            'run_to_log "$backend provider reset" "$pass/provider-reset.json" provider reset'
        )
        preflight_command = (
            'run_to_log "manager-b-db-preflight" '
            '"$pass/manager-b-db-preflight.log" assert_manager_b_db_reachable'
        )
        database_preflight = lifecycle.index(preflight_command)
        first_manager_rollout = lifecycle.index(
            'run_to_log "manager A to B deploy-set"'
        )
        self.assertEqual(lifecycle.count(preflight_command), 1)
        self.assertLess(provider_reset, database_preflight)
        self.assertLess(database_preflight, first_manager_rollout)
        self.assertIn(
            'if [ "$backend" = systemd ]; then\n'
            '\t\trun_to_log "manager-b-db-preflight"',
            lifecycle,
        )

    def test_real_harness_entry_executes_ordered_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace = root / "trace.jsonl"
            calls = root / "calls"
            clock = root / "clock"
            cli = root / "cli"
            provider = root / "provider"
            ssh = root / "ssh"
            ssh_key = root / "id_ed25519"
            ssh_key.touch()
            self.executable(clock, "printf '1000\\n'\n")
            self.executable(ssh, """forward=''
while [ $# -gt 0 ]; do
    if [ "$1" = -L ]; then forward="$2"; shift 2; else shift; fi
done
port="${forward#127.0.0.1:}"; port="${port%%:*}"
exec python3 - "$port" <<'PY'
import socket, sys
with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", int(sys.argv[1])))
    listener.listen()
    while True:
        connection, _ = listener.accept()
        connection.close()
PY
""")
            self.executable(cli, f"""printf 'cli %s\\n' "$*" >> {calls!s}
if [[ " $* " == *" invoke "* ]]; then
    printf '{{"nonce":"probe"}}\\n'
elif [[ "$*" == *"operations list"* ]]; then
    printf '[{{"operation_id":"operation-a","node_id":"node-a","request_token":"upgrade-token","state":"succeeded"}}]\\n'
elif [[ "$*" == *"operations get operation-a"* ]]; then
    printf '{{"schema_version":1,"operation_id":"operation-a"}}\\n'
elif [[ "$*" == *"--exit-after-finalization"* ]]; then
    printf 'deterministic exit after inactive release finalization\\n' >&2
    exit 1
elif [[ " $* " == *" upgrade "* ]]; then
    sleep 0.4
fi
""")
            self.executable(provider, f"printf 'provider %s\\n' \"$*\" >> {calls!s}\n")
            env = os.environ | {
                "WRT_DEPLOY_CONTRACT_MODE": "1",
                "WRT_CONTRACT_CLOCK": str(clock),
                "WRT_CONTRACT_CLI": str(cli),
                "WRT_CONTRACT_PROVIDER": str(provider),
                "WRT_LIFECYCLE_TRACE": str(trace),
                "WRT_CONTRACT_ARTIFACT": str(root / "operation.json"),
                "WRT_CONTRACT_TRAFFIC_DIR": str(root),
                "WRT_CONTRACT_SSH_KEY": str(ssh_key),
                "PATH": f"{root}:{os.environ['PATH']}",
            }
            result = subprocess.run(["bash", str(HARNESS)], env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            events = [json.loads(line) for line in trace.read_text().splitlines()]
            operations = [event for event in events if event["event"] == "operation"]
            self.assertEqual([event["name"] for event in operations], [
                "systemd-scale-out", "systemd-upgrade-finalize", "systemd-upgrade-retry", "systemd-scale-in", "systemd-drain", "systemd-rollback",
                "docker-scale-out", "docker-upgrade-finalize", "docker-upgrade-retry", "docker-scale-in", "docker-drain", "docker-rollback",
            ])
            for event in operations:
                self.assertIn("submitted=1000,durable=2800,cli_wait=2860,watchdog=2920", event["detail"])
            artifacts = [event for event in events if event["event"] == "artifact"]
            self.assertEqual([event["name"] for event in artifacts], ["prepare", "systemd", "docker"])
            self.assertEqual({event["detail"].split("digest=")[1] for event in artifacts}, {"sha256:fixture"})
            self.assertEqual([event["name"] for event in events if event["event"] == "reset"], ["systemd-entry", "docker-entry"])
            manager_rollouts = [event for event in events if event["event"] == "manager-rollout"]
            self.assertEqual([event["name"] for event in manager_rollouts], ["a-to-b", "b-to-a"])
            self.assertTrue(all("barrier=120,lease=30,renew=10,watchdog=180" in event["detail"] for event in manager_rollouts))
            self.assertEqual(len([event for event in events if event["event"] == "cleanup"]), 1)
            captures = [event for event in events if event["event"] == "operation-detail"]
            self.assertEqual([event["name"] for event in captures], ["upgrade-token", "upgrade-token"])
            self.assertEqual(json.loads((root / "operation.json").read_text())["operation_id"], "operation-a")
            invoked = calls.read_text().splitlines()
            self.assertTrue(any("operations list" in line for line in invoked))
            self.assertTrue(any("operations get operation-a" in line for line in invoked))
            probe_calls = [line for line in invoked if " invoke " in line]
            self.assertTrue(probe_calls)
            self.assertTrue(all(
                "--destination http://deployment.probe/deployment.ProbeService/Check" in line
                and '--body {"nonce":"probe"}' in line
                for line in probe_calls
            ))
            self.assertEqual([line for line in invoked if line.startswith("provider ")], ["provider reset", "provider reset", "provider stop-reset"])
            upgrades = [line for line in invoked if line.startswith("cli upgrade ")]
            self.assertEqual(len(upgrades), 4)
            self.assertEqual(sum("--exit-after-finalization" in line for line in upgrades), 2)
            self.assertTrue(all("--request-token upgrade-token" in line for line in upgrades))
            self.assertEqual(sum("--bundle upgrade-two.tar.gz" in line for line in upgrades), 2)
            deploy_sets = [line for line in invoked if "managers deploy-set" in line]
            self.assertEqual(len(deploy_sets), 2)
            self.assertIn("manager-a-to-b-systemd.toml --generation 2 --manager-endpoint manager-a", deploy_sets[0])
            self.assertIn("manager-b-to-a-systemd.toml --generation 3 --manager-endpoint manager-b --old-selector-digest initial-a-selector", deploy_sets[1])
            self.assertTrue(all((root / f"{backend}-summary.json").exists() for backend in ("systemd", "docker")))
            traffic_events = [event for event in events if event["event"] in {"tunnel", "probe"}]
            self.assertEqual([(event["event"], event["name"]) for event in traffic_events], [
                ("tunnel", "start"), ("probe", "start"), ("probe", "stop"), ("tunnel", "stop"),
                ("tunnel", "start"), ("probe", "start"), ("probe", "stop"), ("tunnel", "stop"),
            ])
            upgrade = upgrades[0]
            drain = next(line for line in invoked if line.startswith("cli drain "))
            rollback = next(line for line in invoked if line.startswith("cli rollback "))
            self.assertNotIn("--allow-downtime", upgrade)
            self.assertIn("--allow-downtime", drain)
            self.assertIn("--allow-downtime", rollback)
            self.assertIn("--to revision_one_a", rollback)
            for line in (item for item in invoked if any(item.startswith(f"cli {command} ") for command in ("scale-out", "upgrade", "scale-in", "drain", "rollback"))):
                self.assertIn("--deadline 1800", line)
                self.assertIn("--wait-timeout 1860", line)
                self.assertNotIn("600", line)

    def test_manager_fixture_requires_reachable_database_and_monotonic_policies(self):
        import importlib.util
        path = ROOT / "dev" / "deployment-e2e" / "manager_rollout_fixture.py"
        spec = importlib.util.spec_from_file_location("manager_rollout_fixture", path)
        self.assertIsNotNone(spec)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with self.assertRaisesRegex(ValueError, "loopback"):
            module.require_remote_database("postgres://postgres@127.0.0.1:5432/db")
        module.require_remote_database("postgres://postgres@192.0.2.50:5432/db")
        base = (ROOT / "wr-tests" / "deployment" / "policy" / "authorization.toml").read_text()
        generation_two = module.render_policy(base, 2, "manager-b", "https://192.0.2.12:9000")
        generation_three = module.render_policy(base, 3, "manager-a", "https://192.0.2.10:9000")
        policies = (
            (base, 1, "manager-a", "https://127.0.0.1:9000"),
            (generation_two, 2, "manager-b", "https://192.0.2.12:9000"),
            (generation_three, 3, "manager-a", "https://192.0.2.10:9000"),
        )
        for policy, generation, manager_id, endpoint in policies:
            with self.subTest(generation=generation):
                value = tomllib.loads(policy)
                principal = f"urn:wruntime:deployment:manager:{manager_id}"
                manager_principals = [
                    item["uri"]
                    for item in value["principals"]
                    if item["kind"] == "manager"
                ]
                self.assertEqual(value["generation"], generation)
                self.assertEqual(manager_principals, [principal])
                self.assertEqual(
                    value["manager_enrollments"],
                    [{
                        "principal": principal,
                        "manager_id": manager_id,
                        "endpoint": endpoint,
                    }],
                )

    def test_stable_slot_fixtures_have_unique_pinned_endpoints(self):
        scenarios = ROOT / "wr-tests" / "deployment" / "scenarios"
        observed = {}
        for version, module_version in (("baseline", "1.0.0"), ("upgrade", "2.0.0")):
            for slot in ("engine-1", "engine-2"):
                value = tomllib.loads((scenarios / version / f"{slot}.toml").read_text())
                observed[(version, slot)] = (value["listen_address"], value["job_admin"]["listen_address"])
                module = value["module"][0]
                self.assertEqual(module["name"], "probe")
                self.assertEqual(module["namespace"], "deployment")
                self.assertEqual(module["version"], module_version)
                self.assertEqual(module["wasm_path"], "wr-tests/deployment/probe/target/wasm32-wasip2/debug/deployment_probe.wasm")
                self.assertEqual(module["schema_path"], "wr-tests/deployment/probe/schemas/probe.binpb")
        self.assertEqual(observed[("baseline", "engine-1")], observed[("upgrade", "engine-1")])
        self.assertEqual(observed[("baseline", "engine-2")], observed[("upgrade", "engine-2")])
        self.assertEqual(len(set(observed.values())), 2)
        self.assertEqual(len({endpoint for pair in observed.values() for endpoint in pair}), 4)

    def test_manifest_verifier_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "bundle.tar.gz"
            artifact.write_bytes(b"immutable")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"schema_version": 1, "artifacts": [{
                "role": "baseline-one", "path": str(artifact.resolve()),
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            }]}))
            command = ["bash", "-c", f"source {HELPER}; lifecycle_verify_manifest {manifest}"]
            self.assertEqual(subprocess.run(command).returncode, 0)
            artifact.write_bytes(b"mutated")
            result = subprocess.run(command, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("prepared artifact mutated", result.stderr)

    def test_background_probe_overlaps_foreground_window_and_fails_on_bad_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            responder = root / "responder.py"
            responder.write_text("import json; print(json.dumps({'nonce': 'probe'}))\n")
            log, stop = root / "probe.jsonl", root / "probe.stop"
            command = [sys.executable, str(ROOT / "dev/deployment-e2e/traffic_probe.py"), "run",
                       "--log", str(log), "--stop-file", str(stop), "--expected", "probe", "--",
                       sys.executable, str(responder)]
            process = subprocess.Popen(command)
            submitted = time.time()
            time.sleep(0.4)
            completed = time.time()
            stop.touch()
            self.assertEqual(process.wait(timeout=5), 0)
            result = subprocess.run([sys.executable, str(ROOT / "dev/deployment-e2e/traffic_probe.py"), "evaluate",
                                     "--log", str(log), "--submitted-at", str(submitted),
                                     "--completed-at", str(completed)], text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            samples = [json.loads(line) for line in log.read_text().splitlines()]
            samples.append({"launched_at": submitted, "completed_at": completed, "valid": False})
            log.write_text("".join(json.dumps(item) + "\n" for item in samples))
            result = subprocess.run([sys.executable, str(ROOT / "dev/deployment-e2e/traffic_probe.py"), "evaluate",
                                     "--log", str(log), "--submitted-at", str(submitted),
                                     "--completed-at", str(completed)], text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("failed/invalid", result.stderr)

    def test_expired_budget_prevents_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clock = root / "clock"
            marker = root / "mutated"
            self.executable(clock, "printf '101\\n'\n")
            script = textwrap.dedent(f"""
                set -Eeuo pipefail
                source {HELPER}
                export WRT_CONTRACT_CLOCK={clock}
                LIFECYCLE_WATCHDOG_CUTOFF=100
                lifecycle_remaining "$LIFECYCLE_WATCHDOG_CUTOFF"
                touch {marker}
            """)
            result = subprocess.run(["bash", "-c", script], text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(marker.exists())
            self.assertIn("expired before mutation", result.stderr)


if __name__ == "__main__":
    unittest.main()
