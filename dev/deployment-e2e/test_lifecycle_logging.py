#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

HELPERS = Path(__file__).with_name("lifecycle_logging.sh")


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
                    set +e
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

    def test_retry_replaces_failed_attempt_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output.log"
            error = root / "error.log"
            counter = root / "counter"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                attempt() {{
                    count=0
                    [ ! -f {counter} ] || count=$(cat {counter})
                    count=$((count + 1))
                    printf '%s' "$count" >{counter}
                    if [ "$count" -lt 3 ]; then
                        printf 'partial-%s\\n' "$count"
                        printf 'error-%s\\n' "$count" >&2
                        return 75
                    fi
                    printf 'ready\\n'
                }}
                run_with_retry {output} {error} 3 0 '^error-[12]$' attempt
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(counter.read_text(), "3")
            self.assertEqual(output.read_text(), "ready\n")
            self.assertIn("attempt 1 failed (exit 75)", error.read_text())
            self.assertIn("error-2", error.read_text())

    def test_retry_returns_final_failure_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output.log"
            error = root / "error.log"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                fail() {{
                    printf 'retryable error\\n' >&2
                    return 42
                }}
                if run_with_retry {output} {error} 2 0 '^retryable error$' fail; then
                    exit 99
                else
                    test "$?" -eq 42
                fi
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(error.read_text().count("retryable error"), 2)

    def test_retry_stops_after_non_matching_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output.log"
            error = root / "error.log"
            counter = root / "counter"
            result = self.run_bash(
                f"""
                set -Eeuo pipefail
                source {HELPERS}
                fail() {{
                    count=0
                    [ ! -f {counter} ] || count=$(cat {counter})
                    printf '%s' "$((count + 1))" >{counter}
                    printf 'schema mismatch\\n' >&2
                    return 19
                }}
                if run_with_retry {output} {error} 5 0 '^no route' fail; then
                    exit 99
                else
                    test "$?" -eq 19
                fi
                """
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(counter.read_text(), "1")
            self.assertIn("schema mismatch", error.read_text())


if __name__ == "__main__":
    unittest.main()
