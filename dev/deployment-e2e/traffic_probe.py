#!/usr/bin/env python3
"""Continuously invoke typed echo traffic and evaluate an upgrade window."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import time


def run(args: argparse.Namespace) -> int:
    log = Path(args.log)
    stop = Path(args.stop_file)
    while not stop.exists():
        launched = time.time()
        valid = False
        error = ""
        returncode = -1
        try:
            result = subprocess.run(args.command, capture_output=True, text=True, timeout=5)
            returncode = result.returncode
            if returncode == 0:
                try:
                    value = json.loads(result.stdout)
                    valid = value.get("message") == args.expected
                    if not valid:
                        error = f"unexpected response: {value!r}"
                except (json.JSONDecodeError, AttributeError) as exc:
                    error = f"invalid JSON response: {exc}"
            else:
                error = result.stderr[-1000:]
        except subprocess.TimeoutExpired:
            error = "request exceeded 5-second limit"
        completed = time.time()
        with log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({
                "launched_at": launched,
                "completed_at": completed,
                "returncode": returncode,
                "valid": valid,
                "error": error,
            }, sort_keys=True) + "\n")
        remaining = 0.25 - (time.time() - completed)
        if remaining > 0:
            time.sleep(remaining)
    return 0


def evaluate(args: argparse.Namespace) -> int:
    samples = [json.loads(line) for line in Path(args.log).read_text().splitlines()]
    failures = [sample for sample in samples if not sample.get("valid")]
    overlap = [sample for sample in samples
               if sample.get("launched_at", 0) >= args.submitted_at
               and sample.get("completed_at", float("inf")) <= args.completed_at]
    if failures:
        raise SystemExit(f"traffic probe recorded {len(failures)} failed/invalid calls")
    if not overlap:
        raise SystemExit("traffic probe did not complete a request inside the upgrade window")
    print(json.dumps({"samples": len(samples), "upgrade_window_samples": len(overlap), "failures": 0}, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="action", required=True)
    start = commands.add_parser("run")
    start.add_argument("--log", required=True)
    start.add_argument("--stop-file", required=True)
    start.add_argument("--expected", required=True)
    start.add_argument("command", nargs=argparse.REMAINDER)
    check = commands.add_parser("evaluate")
    check.add_argument("--log", required=True)
    check.add_argument("--submitted-at", required=True, type=float)
    check.add_argument("--completed-at", required=True, type=float)
    return root


def main() -> int:
    args = parser().parse_args()
    if args.action == "run":
        if args.command[:1] == ["--"]:
            args.command = args.command[1:]
        if not args.command:
            raise SystemExit("probe command is required")
        return run(args)
    return evaluate(args)


if __name__ == "__main__":
    raise SystemExit(main())
