#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

HELPERS = Path(__file__).with_name("lifecycle_logging.sh")
REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_HELPERS = REPO_ROOT / "examples" / "helpers.sh"
DEPLOYMENT_LIFECYCLE = REPO_ROOT / "dev" / "validate-deployment-lifecycle.sh"
LIFECYCLE_CONTRACT = Path(__file__).with_name("lifecycle_contract.sh")


class LifecycleLoggingTests(unittest.TestCase):
    def run_bash(self, script: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "-c", textwrap.dedent(script)],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_failure_preserves_status_and_redacts_log_before_excerpt(self):
        protected_value = "deployment-logging-fixture"
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "failure.log"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                PYTHON=(python3)
                source {HELPERS}
                PROTECTED={protected_value}
                LOG={log}
                redact_logs() {{
                    python3 - "$ACTIVE_LOG" "$PROTECTED" <<'PY_REDACT'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
path.write_text(path.read_text().replace(sys.argv[2], "<redacted>"))
PY_REDACT
                }}
                on_error() {{
                    status=$?
                    trap - ERR
                    report_active_failure "$status"
                    exit "$status"
                }}
                trap on_error ERR
                run_to_log "fixture failure" "$LOG" bash -c 'printf "protected=%s\\n" "$1"; exit 23' _ "$PROTECTED"
                """
            )

            self.assertEqual(result.returncode, 23)
            self.assertIn("fixture failure (exit 23)", result.stderr)
            self.assertIn(str(log), result.stderr)
            self.assertIn("protected=<redacted>", result.stderr)
            self.assertNotIn(protected_value, result.stderr)
            self.assertEqual(log.read_text(), "protected=<redacted>\n")

    def test_success_clears_active_step_and_log(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "success.log"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                PYTHON=(python3)
                source {HELPERS}
                LOG={log}
                run_to_log "fixture success" "$LOG" printf 'ok\\n'
                test -z "$ACTIVE_STEP"
                test -z "$ACTIVE_LOG"
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(log.read_text(), "ok\n")

    def test_diagnostic_failure_is_recorded_without_masking_primary_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "diagnostic.log"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                collect_diagnostic "remote status" {log} bash -c 'echo unavailable; exit 42'
                test "${{#DIAGNOSTIC_FAILURES[@]}}" -eq 1
                report_diagnostic_failures
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"remote status=42:{log}", result.stderr)
            self.assertEqual(log.read_text(), "unavailable\n")

    def test_remaining_deadline_budget_is_capped_and_expires(self):
        result = self.run_bash(
            f"""
            set -Eeuo pipefail
            source {HELPERS}
            deadline=$((SECONDS + 10))
            test "$(remaining_deadline_seconds "$deadline" 5)" -eq 5
            deadline=$SECONDS
            if remaining_deadline_seconds "$deadline" 5; then exit 1; fi
            """
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_absolute_deadline_bounds_a_hung_probe(self):
        result = self.run_bash(
            f"""
            set -Eeuo pipefail
            source {HELPERS}
            started=$SECONDS
            deadline=$((SECONDS + 3))
            budget="$(remaining_deadline_seconds "$deadline" 5 2)"
            if timeout -k 1 "$budget" sh -c 'trap "" TERM; exec sleep 30'; then exit 1; fi
            elapsed=$((SECONDS - started))
            test "$elapsed" -le 3
            """
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_cleanup_failure_is_aggregated_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "cleanup.log"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                PRIMARY_STATUS=0
                record_promoted_cleanup_failure "SSH tunnel" 17 {log}
                test "$PRIMARY_STATUS" -eq 17
                PRIMARY_STATUS=23
                record_promoted_cleanup_failure "provider reset" 19 {log}
                test "$PRIMARY_STATUS" -eq 23
                test "${{#CLEANUP_FAILURES[@]}}" -eq 2
                report_cleanup_failures
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"cleanup failure: SSH tunnel=17:{log}", result.stderr)
            self.assertIn(f"cleanup failure: provider reset=19:{log}", result.stderr)

    def test_run_directory_removal_uses_real_cleanup_promotion(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                rm() {{ return 17; }}
                PRIMARY_STATUS=0
                if remove_directory_with_cleanup_accounting "run directory" {directory}; then exit 1; else status=$?; fi
                test "$status" -eq 17
                test "$PRIMARY_STATUS" -eq 17
                test "${{#CLEANUP_FAILURES[@]}}" -eq 1
                PRIMARY_STATUS=23
                if remove_directory_with_cleanup_accounting "run directory again" {directory}; then exit 1; else status=$?; fi
                test "$status" -eq 17
                test "$PRIMARY_STATUS" -eq 23
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)

    def test_example_cleanup_removal_failure_is_promoted_and_preserves_primary(self):
        for primary, expected in [(0, 17), (23, 23)]:
            with self.subTest(primary=primary), tempfile.TemporaryDirectory() as directory:
                result = self.run_bash(
                    f"""
                    WR_EXAMPLE_RUN_DIR={directory}
                    source {EXAMPLE_HELPERS}
                    remove_example_run_directory() {{ return 17; }}
                    exit {primary}
                    """
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertIn("run-directory removal=17", result.stderr)
                if primary:
                    self.assertIn("Primary run failed with 23", result.stderr)

    def test_deployment_uses_only_durable_node_lifecycle_commands(self):
        script = DEPLOYMENT_LIFECYCLE.read_text()
        for command in ("node deploy", "engines restart", "node rollback"):
            self.assertIn(command, script)
        self.assertIn("--exit-after-finalization", script)
        self.assertEqual(script.count("--allow-downtime"), 2)
        self.assertNotIn("--wait-timeout", script)
        self.assertIn("node inventory contraction", script)
        restart_invocation = script.split(
            'run_to_log "$backend durable engine restart"', 1
        )[1].split("lifecycle_capture_operation_detail", 1)[0]
        self.assertNotIn("--json", restart_invocation)
        contract = LIFECYCLE_CONTRACT.read_text()
        self.assertIn('operations list --node-id "$node_id" --include-terminal --json', contract)
        self.assertIn('len(matches) != 1', contract)
        for obsolete in (
            "node stop",
            "node upgrade",
            "node scale",
            "engines drain",
            "assert_node_stop_record",
            "wr-node/current",
            "lifecycle stop",
            "engine-lifecycle-tunnel",
            "engine-exit-probe",
            "systemctl show wr-engine",
            "status exited --services engine-engine",
        ):
            self.assertNotIn(obsolete, script)

    def test_deployment_materializes_complete_policy_before_manager_bundle(self):
        script = DEPLOYMENT_LIFECYCLE.read_text()
        self.assertIn("policy = policy_source.read_text()", script)
        self.assertIn('proxy_principal = f"urn:wruntime:{cluster_id}:proxy:{node_id}"', script)
        self.assertIn('agent_principal = f"urn:wruntime:{cluster_id}:node-agent:{node_id}"', script)
        self.assertIn('quote(manager_enrollment["endpoint"]), quote(manager_endpoint), 1', script)
        self.assertLess(
            script.index('run_logged cert-manager-client'),
            script.index('run_logged manager-bundle'),
        )
        self.assertIn('--manager-config "$MANAGER_CONFIG"', script)
        self.assertLess(
            script.index('run_to_log "$backend node agent fixture provisioning and initial activation"'),
            script.index('run_to_log "$backend node agent binary update restart and fresh activation"'),
        )
        self.assertLess(
            script.index('run_to_log "$backend node agent binary update restart and fresh activation"'),
            script.index('run_to_log "$backend node A deploy"'),
        )
        invocation = script.split('run_to_log "$backend node agent binary update restart and fresh activation"', 1)[1].split("job_admin queues", 1)[0]
        for forbidden in ("--agent-cert", "--agent-key", "--agent-ca-cert", "--retention-count", "--systemctl-path", "--docker-path"):
            self.assertNotIn(forbidden, invocation)

    def test_failure_excerpt_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "long.log"
            with log.open("w", encoding="utf-8") as stream:
                stream.writelines(f"line-{line}\n" for line in range(40))
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                PYTHON=(python3)
                source {HELPERS}
                print_failure_excerpt {log}
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("line-29", result.stderr)
            self.assertNotIn("line-30", result.stderr)
            self.assertIn("10 more lines", result.stderr)


if __name__ == "__main__":
    unittest.main()
