#!/usr/bin/env python3
"""Reset the three dedicated deployment-E2E VMs through the Proxmox HTTPS API."""

from __future__ import annotations

from abc import abstractmethod
import argparse
import dataclasses
import importlib
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
from threading import Event
from typing import Any, Callable, Protocol

ENV_KEYS = ("PVE_HOST", "PVE_USER", "PVE_TOKEN_NAME", "PVE_TOKEN_VALUE")
DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "deployment-e2e.toml"
DEFAULT_KNOWN_HOSTS = Path("~/.ssh/wruntime-e2e-known_hosts").expanduser()
DEFAULT_CA_BUNDLE = Path("/etc/ssl/certs/ca-certificates.crt")
API_TIMEOUT = 15
TASK_TIMEOUT = 180
STATE_TIMEOUT = 120
SSH_TIMEOUT = 90
POLL_INTERVAL = 1.0
POLL_EVENT = Event()


class ProviderError(RuntimeError):
    """A bounded, operator-actionable provider failure."""


@dataclasses.dataclass(frozen=True)
class Target:
    key: str
    vmid: int
    name: str
    host: str
    ssh_user: str
    role: str
    snapshot: str


@dataclasses.dataclass(frozen=True)
class Config:
    provider: str
    proxmox_node: str
    snapshot: str
    expected_image_version: str
    lock_file: str
    workdir: str
    node_id: str
    targets: tuple[Target, Target, Target]


class Client(Protocol):
    @abstractmethod
    def version(self) -> dict[str, Any]:
        """Return Proxmox API version metadata."""

    @abstractmethod
    def vm_config(self, node: str, vmid: int) -> dict[str, Any]:
        """Return one VM's configuration."""

    @abstractmethod
    def vm_snapshots(self, node: str, vmid: int) -> list[dict[str, Any]]:
        """Return one VM's snapshots."""

    @abstractmethod
    def vm_status(self, node: str, vmid: int) -> dict[str, Any]:
        """Return one VM's current status."""

    @abstractmethod
    def stop(self, node: str, vmid: int) -> str:
        """Hard-stop one VM and return its task ID."""

    @abstractmethod
    def rollback(self, node: str, vmid: int, snapshot: str) -> str:
        """Roll back one VM and return its task ID."""

    @abstractmethod
    def start(self, node: str, vmid: int) -> str:
        """Start one VM and return its task ID."""

    @abstractmethod
    def task_status(self, node: str, upid: str) -> dict[str, Any]:
        """Return asynchronous task status."""


def _nonempty(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ProviderError(f"configuration field {key!r} must be non-empty")
    return value


def load_config(path: Path) -> Config:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ProviderError(f"cannot read provider configuration {path}: {exc}") from exc
    provider = _nonempty(data, "provider")
    if provider != "proxmox":
        raise ProviderError(f"unsupported provider {provider!r}")
    proxmox_node = _nonempty(data, "proxmox_node")
    snapshot = _nonempty(data, "snapshot")
    image = _nonempty(data, "expected_image_version")
    workdir = _nonempty(data, "workdir")
    lock_file = _nonempty(data, "lock_file")
    node_id = _nonempty(data, "node_id")
    if any("<" in value or "placeholder" in value.lower() for value in (proxmox_node, snapshot, image, workdir, lock_file, node_id)):
        raise ProviderError("configuration contains placeholder values")
    if not re.fullmatch(r"/[A-Za-z0-9._/-]+", workdir) or workdir == "/" or ".." in Path(workdir).parts:
        raise ProviderError("workdir must be a safe absolute path")

    targets: list[Target] = []
    expected = {
        "manager": ("wr-e2e-manager", "manager"),
        "manager_b": ("wr-e2e-manager-b", "manager-b"),
        "node": ("wr-e2e-node", "node"),
    }
    for key in ("manager", "manager_b", "node"):
        raw = data.get(key)
        if not isinstance(raw, dict):
            raise ProviderError(f"missing [{key}] target")
        if key == "manager_b":
            raw = dict(raw)
            inputs = {
                "vmid": os.environ.get("WRT_DEPLOY_E2E_MANAGER_B_VMID"),
                "host": os.environ.get("WRT_DEPLOY_E2E_MANAGER_B_IP"),
                "snapshot": os.environ.get("WRT_DEPLOY_E2E_MANAGER_B_SNAPSHOT"),
            }
            missing = [name for name, value in inputs.items() if not value]
            if missing:
                raise ProviderError(
                    "manager B requires protected inputs: "
                    "WRT_DEPLOY_E2E_MANAGER_B_VMID, WRT_DEPLOY_E2E_MANAGER_B_IP, "
                    "WRT_DEPLOY_E2E_MANAGER_B_SNAPSHOT"
                )
            raw.update(inputs)
        try:
            vmid = int(raw["vmid"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ProviderError(f"{key}.vmid must be an integer") from exc
        name = _nonempty(raw, "name")
        expected_name, expected_role = expected[key]
        if name != expected_name:
            raise ProviderError(f"{key}.name must be {expected_name!r}, got {name!r}")
        host = _nonempty(raw, "host")
        try:
            ipaddress.ip_address(host)
        except ValueError as exc:
            raise ProviderError(f"{key}.host must be a literal IP address") from exc
        role = _nonempty(raw, "role")
        if role != expected_role:
            raise ProviderError(f"{key}.role must be {expected_role!r}")
        target_snapshot = _nonempty(raw, "snapshot") if key == "manager_b" else snapshot
        targets.append(Target(key, vmid, name, host, _nonempty(raw, "ssh_user"), role, target_snapshot))
    if len({target.vmid for target in targets}) != len(targets):
        raise ProviderError("deployment target VM IDs must be unique")
    if len({target.host for target in targets}) != len(targets):
        raise ProviderError("deployment target hosts must be unique")
    return Config(provider, proxmox_node, snapshot, image, lock_file, workdir, node_id, tuple(targets))


def ca_bundle_path() -> Path:
    path = Path(os.environ.get("PVE_CA_BUNDLE") or DEFAULT_CA_BUNDLE).expanduser()
    if not path.is_file():
        raise ProviderError(
            f"Proxmox CA bundle is unavailable at {path}; install the CA into the OS trust store "
            "or set PVE_CA_BUNDLE"
        )
    return path


class ProxmoxerClient:
    def __init__(self, host: str, user: str, token_name: str, token_value: str, ca_bundle: Path):
        try:
            proxmoxer = importlib.import_module("proxmoxer")
        except ImportError as exc:
            raise ProviderError("proxmoxer is unavailable; run this provider through the locked dev/deployment-e2e uv project") from exc
        self.api = proxmoxer.ProxmoxAPI(
            host,
            user=user,
            token_name=token_name,
            token_value=token_value,
            backend="https",
            verify_ssl=str(ca_bundle),
            timeout=API_TIMEOUT,
        )

    def version(self) -> dict[str, Any]:
        return self.api.version.get()

    def _vm(self, node: str, vmid: int):
        return self.api.nodes(node).qemu(vmid)

    def vm_config(self, node: str, vmid: int) -> dict[str, Any]:
        return self._vm(node, vmid).config.get()

    def vm_snapshots(self, node: str, vmid: int) -> list[dict[str, Any]]:
        return self._vm(node, vmid).snapshot.get()

    def vm_status(self, node: str, vmid: int) -> dict[str, Any]:
        return self._vm(node, vmid).status.current.get()

    def stop(self, node: str, vmid: int) -> str:
        return self._vm(node, vmid).status.stop.post()

    def rollback(self, node: str, vmid: int, snapshot: str) -> str:
        return self._vm(node, vmid).snapshot(snapshot).rollback.post()

    def start(self, node: str, vmid: int) -> str:
        return self._vm(node, vmid).status.start.post()

    def task_status(self, node: str, upid: str) -> dict[str, Any]:
        return self.api.nodes(node).tasks(upid).status.get()


def redact(message: object, secrets: tuple[str, ...] = ()) -> str:
    text = str(message)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    text = re.sub(r"(?i)(Authorization\s*:\s*)\S+", r"\1<redacted>", text)
    text = re.sub(r"(?i)(token(?:_value)?[=:]\s*)\S+", r"\1<redacted>", text)
    text = re.sub(r"(?i)(postgres(?:ql)?://[^:/\s]+:)[^@\s]+@", r"\1<redacted>@", text)
    return text[:2048]


class LifecycleProvider:
    def __init__(
        self,
        config: Config,
        client: Client,
        marker_reader: Callable[[Target], dict[str, Any]],
        *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        task_timeout: float = TASK_TIMEOUT,
        state_timeout: float = STATE_TIMEOUT,
    ):
        self.config = config
        self.client = client
        self.marker_reader = marker_reader
        self.sleep = sleep
        self.monotonic = monotonic
        self.task_timeout = task_timeout
        self.state_timeout = state_timeout

    def _identity(self, target: Target) -> dict[str, Any]:
        cfg = self.client.vm_config(self.config.proxmox_node, target.vmid)
        actual_name = cfg.get("name")
        if actual_name != target.name:
            raise ProviderError(f"VM {target.vmid} name mismatch: expected {target.name!r}, got {actual_name!r}")
        snapshots = {item.get("name") for item in self.client.vm_snapshots(self.config.proxmox_node, target.vmid)}
        if target.snapshot not in snapshots:
            raise ProviderError(f"VM {target.vmid} is missing snapshot {target.snapshot!r}")
        status = self.client.vm_status(self.config.proxmox_node, target.vmid).get("status")
        if status not in {"running", "stopped"}:
            raise ProviderError(f"VM {target.vmid} returned unknown state {status!r}")
        return {"key": target.key, "vmid": target.vmid, "name": target.name, "status": status, "snapshot": target.snapshot}

    def preflight(self) -> dict[str, Any]:
        version = self.client.version()
        api_version = str(version.get("version", ""))
        if not re.match(r"^[0-9]+(?:\.[0-9]+)+", api_version):
            raise ProviderError("Proxmox API did not return a parseable version")
        return {"provider": "proxmox", "api_version": api_version, "node": self.config.proxmox_node, "targets": [self._identity(t) for t in self.config.targets]}

    def _wait_task(self, target: Target, upid: str, operation: str) -> None:
        if not isinstance(upid, str) or not upid.startswith("UPID:"):
            raise ProviderError(f"VM {target.vmid} {operation} returned an invalid task ID")
        deadline = self.monotonic() + self.task_timeout
        while True:
            status = self.client.task_status(self.config.proxmox_node, upid)
            if status.get("status") == "stopped":
                if status.get("exitstatus") != "OK":
                    raise ProviderError(f"VM {target.vmid} {operation} task failed with exitstatus {status.get('exitstatus')!r}")
                return
            if self.monotonic() >= deadline:
                raise ProviderError(f"VM {target.vmid} {operation} task timed out")
            self.sleep(POLL_INTERVAL)

    def _wait_state(self, target: Target, expected: str) -> None:
        deadline = self.monotonic() + self.state_timeout
        while True:
            actual = self.client.vm_status(self.config.proxmox_node, target.vmid).get("status")
            if actual == expected:
                return
            if self.monotonic() >= deadline:
                raise ProviderError(f"VM {target.vmid} did not reach state {expected!r}; last state {actual!r}")
            self.sleep(POLL_INTERVAL)

    def _run_all(self, operation: str, action: Callable[[Target], None]) -> None:
        failures: list[str] = []
        for target in self.config.targets:
            try:
                action(target)
            except Exception as exc:  # each dedicated target must still be attempted
                failures.append(f"{target.key}: {exc}")
        if failures:
            raise ProviderError(f"{operation} failed ({'; '.join(failures)})")

    def _stop(self, target: Target) -> None:
        if self.client.vm_status(self.config.proxmox_node, target.vmid).get("status") != "stopped":
            self._wait_task(target, self.client.stop(self.config.proxmox_node, target.vmid), "stop")
        self._wait_state(target, "stopped")

    def _rollback(self, target: Target) -> None:
        self._wait_task(target, self.client.rollback(self.config.proxmox_node, target.vmid, target.snapshot), "rollback")
        self._wait_state(target, "stopped")

    def _start(self, target: Target) -> None:
        self._wait_task(target, self.client.start(self.config.proxmox_node, target.vmid), "start")
        self._wait_state(target, "running")

    def _marker(self, target: Target) -> None:
        marker = self.marker_reader(target)
        expected = {"image_version": self.config.expected_image_version, "role": target.role, "hostname": target.name}
        actual = {key: marker.get(key) for key in expected}
        if actual != expected:
            raise ProviderError(f"VM {target.vmid} marker mismatch: expected {expected!r}, got {actual!r}")

    def reset(self, *, start: bool) -> dict[str, Any]:
        # Revalidate exact immutable targets immediately before every mutation.
        self.preflight()
        self._run_all("stop", self._stop)
        self._run_all("rollback", self._rollback)
        if start:
            self._run_all("start", self._start)
            self._run_all("marker verification", self._marker)
        return self.status()

    def status(self) -> dict[str, Any]:
        targets = []
        for target in self.config.targets:
            try:
                state = self.client.vm_status(self.config.proxmox_node, target.vmid).get("status")
                targets.append({"key": target.key, "vmid": target.vmid, "name": target.name, "status": state})
            except Exception as exc:
                targets.append({"key": target.key, "vmid": target.vmid, "name": target.name, "error": redact(exc)})
        return {"provider": "proxmox", "node": self.config.proxmox_node, "snapshot": self.config.snapshot, "targets": targets}


def ssh_marker_reader(target: Target) -> dict[str, Any]:
    key = os.environ.get("WRT_DEPLOY_E2E_SSH_KEY", "")
    known_hosts = Path(os.environ.get("WRT_DEPLOY_E2E_KNOWN_HOSTS", str(DEFAULT_KNOWN_HOSTS))).expanduser()
    if not key:
        raise ProviderError("WRT_DEPLOY_E2E_SSH_KEY is required for reset")
    if not Path(key).expanduser().is_file() or not known_hosts.is_file():
        raise ProviderError("deployment SSH key or dedicated known_hosts file is unavailable")
    cmd = [
        "ssh", "-i", str(Path(key).expanduser()), "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
        "-o", "ConnectTimeout=5", f"{target.ssh_user}@{target.host}",
        "cat /etc/wruntime-e2e-image.json",
    ]
    deadline = time.monotonic() + SSH_TIMEOUT
    last_error = "SSH not attempted"
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                value = json.loads(result.stdout)
                if not isinstance(value, dict):
                    raise ProviderError("guest marker is not a JSON object")
                return value
            last_error = result.stderr.strip() or f"ssh exit {result.returncode}"
        except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            last_error = str(exc)
        POLL_EVENT.wait(POLL_INTERVAL)
    raise ProviderError(f"VM {target.vmid} SSH/marker timeout: {redact(last_error)}")


def build_live_client() -> tuple[ProxmoxerClient, tuple[str, ...]]:
    missing = [name for name in ENV_KEYS if not os.environ.get(name)]
    if missing:
        raise ProviderError(f"missing required environment variables: {', '.join(missing)}")
    values = tuple(os.environ[name] for name in ENV_KEYS)
    return ProxmoxerClient(values[0], values[1], values[2], values[3], ca_bundle_path()), (values[3],)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("command", choices=("preflight", "reset", "stop-reset", "status"))
    args = parser.parse_args(argv)
    secrets = (os.environ.get("PVE_TOKEN_VALUE", ""),)
    try:
        config = load_config(args.config)
        client, secrets = build_live_client()
        provider = LifecycleProvider(config, client, ssh_marker_reader)
        if args.command == "preflight":
            result = provider.preflight()
        elif args.command == "reset":
            result = provider.reset(start=True)
        elif args.command == "stop-reset":
            result = provider.reset(start=False)
        else:
            result = provider.status()
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(json.dumps({"error": redact(exc, secrets), "command": args.command}, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
