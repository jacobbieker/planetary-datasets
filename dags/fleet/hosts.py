"""The machines Dagster runs are spread over, and how to reach and measure them.

The inventory lives in ``dags/fleet.yaml`` (override with ``PLANETARY_FLEET_CONFIG``).
Every host — including the one running the webserver and daemon — is described the same
way; the only difference is that the local one has ``ssh: null`` and its commands run in
a local shell instead of over SSH.

Nothing here imports Dagster, so it can be used from the CLI and tested on its own.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "dags" / "fleet.yaml"

#: Holds the Postgres password and anything else host-specific that must not be in git.
#: Sourced by every run before Dagster starts; written by ``python -m dags.fleet setup``.
HOST_ENV_FILE = "~/.config/planetary-fleet/env"

#: Multiplexed so the probe/launch/health-check round trips reuse one connection.
SSH_OPTIONS = (
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=8",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    "-o", "ControlMaster=auto",
    "-o", "ControlPath=~/.ssh/planetary-fleet-%r@%h:%p",
    "-o", "ControlPersist=10m",
)  # fmt: skip

#: How long a probe result is trusted. Placement also accounts for runs it has launched
#: since (see :mod:`dags.fleet.placement`), so this only bounds how stale the view of
#: *other* load on the machine can be.
PROBE_TTL_SECONDS = 15.0

#: Run on each host by the env's own interpreter. Doubles as a readiness check: a host
#: whose pixi environment is not installed yet cannot answer it.
_PROBE_SCRIPT = """
import json, os, platform, shutil, psutil
d = os.path.expanduser({work_dir!r})
os.makedirs(d, exist_ok=True)
vm = psutil.virtual_memory()
print(json.dumps({{
    "total_gb": vm.total / 1e9,
    "available_gb": vm.available / 1e9,
    "disk_free_gb": shutil.disk_usage(d).free / 1e9,
    "cpus": os.cpu_count(),
    "load1": os.getloadavg()[0],
    "platform": platform.system().lower() + "-" + platform.machine(),
    "home": os.path.expanduser("~"),
}}))
"""


class FleetConfigError(ValueError):
    """``dags/fleet.yaml`` is missing or malformed."""


def shell_path(path: str) -> str:
    """Quote a path for a POSIX shell, keeping a leading ``~/`` expandable."""
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        return '"$HOME"/' + shlex.quote(path[2:])
    return shlex.quote(path)


@dataclass(frozen=True)
class Host:
    """One machine runs can be placed on.

    Attributes:
        name: Short name, used in run tags and log lines.
        ssh: SSH destination, or None for the machine the daemon runs on.
        repo_dir: Where the code lives on the host. Remote hosts get a copy synced from
            the daemon's checkout before every launch, so this should be a directory used
            only by the fleet, never someone's working copy.
        work_dir: Root for scratch, downloads, Dagster storage and run logs. Put it on a
            large, real disk: on Linux ``/tmp`` is often RAM-backed, and scratch written
            there competes with the runs for memory.
        pixi: Path to the pixi executable on the host. Non-interactive SSH shells rarely
            have ``~/.pixi/bin`` on PATH.
        memory_gb: Memory the fleet may commit to runs on this host. None uses
            ``memory_fraction`` of physical memory.
        memory_fraction: Used when ``memory_gb`` is None.
        reserve_gb: Live free memory a launch must leave behind, whatever the ledger
            says. This guards against load from outside Dagster.
        min_free_disk_gb: Free space a launch must leave on the ``work_dir`` disk.
        max_runs: Concurrent runs on this host.
        capabilities: What the host offers that some assets require, e.g. ``docker``.
        data_dir: ``PLANETARY_DATASETS_DATA_DIR`` for runs here. Defaults to
            ``<work_dir>/data``.
        env: Extra environment variables for every run on this host.
        enabled: False keeps the host in the inventory without placing runs on it.
    """

    name: str
    ssh: str | None
    repo_dir: str
    work_dir: str
    pixi: str = "~/.pixi/bin/pixi"
    memory_gb: float | None = None
    memory_fraction: float = 0.6
    reserve_gb: float = 4.0
    min_free_disk_gb: float = 50.0
    max_runs: int = 4
    capabilities: frozenset[str] = frozenset()
    data_dir: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    enabled: bool = True

    @property
    def is_local(self) -> bool:
        """True for the machine the daemon runs on."""
        return self.ssh is None

    @property
    def scratch_dir(self) -> str:
        """``PLANETARY_DATASETS_SCRATCH_DIR`` for runs here."""
        return f"{self.work_dir}/scratch"

    @property
    def log_dir(self) -> str:
        """Where each run worker's stdout and stderr go."""
        return f"{self.work_dir}/logs"

    @property
    def python(self) -> str:
        """The pixi environment's interpreter on the host."""
        return f"{self.repo_dir}/.pixi/envs/default/bin/python"

    def expand(self, path: str, home: str) -> str:
        """``path`` with a leading ``~`` replaced by the host's home directory."""
        if path == "~" or path.startswith("~/"):
            return home + path[1:]
        return path

    def capacity_gb(self, total_gb: float) -> float:
        """Memory the fleet may commit here, given the host's physical memory."""
        if self.memory_gb is not None:
            return min(self.memory_gb, total_gb)
        return total_gb * self.memory_fraction

    def run_env(self) -> dict[str, str]:
        """Environment every run on this host gets, on top of :data:`HOST_ENV_FILE`."""
        env = {
            "DAGSTER_HOME": f"{self.repo_dir}/dags",
            # dagster.yaml names dags.fleet.launcher, which the worker imports while it
            # builds its instance, before it has loaded any code location.
            "PYTHONPATH": self.repo_dir,
            "DAGSTER_STORAGE_DIR": f"{self.work_dir}/storage",
            "DAGSTER_COMPUTE_LOG_DIR": f"{self.work_dir}/compute_logs",
            "PLANETARY_DATASETS_SCRATCH_DIR": self.scratch_dir,
            "PLANETARY_DATASETS_DATA_DIR": self.data_dir or f"{self.work_dir}/data",
            "PLANETARY_FLEET_HOST": self.name,
        }
        env.update(self.env)
        return env


@dataclass(frozen=True)
class Postgres:
    """Where the shared Dagster storage lives. The password comes from each host's env."""

    host: str
    port: int = 5432
    db: str = "dagster"
    user: str = "dagster"

    def env(self) -> dict[str, str]:
        """The ``DAGSTER_PG_*`` variables, without the password."""
        return {
            "DAGSTER_PG_HOST": self.host,
            "DAGSTER_PG_PORT": str(self.port),
            "DAGSTER_PG_DB": self.db,
            "DAGSTER_PG_USER": self.user,
        }


#: Where the code location is loaded from, on every host. See :attr:`Fleet.code_dir`.
DEFAULT_CODE_DIR = "~/planetary-fleet/planetary-datasets"


@dataclass(frozen=True)
class Fleet:
    """The parsed ``dags/fleet.yaml``.

    Attributes:
        postgres: The shared instance storage.
        hosts: Every machine, local one included.
        code_dir: The directory the daemon loads the code location from, which Dagster
            stores with every run as its working directory. A run worker loads its code
            from that stored path, not from anything the launcher passes it, so the path
            must hold the fleet's copy of the code on every host where it exists: it is
            each remote host's ``repo_dir``, and a symlink to the checkout on the local
            host (``pixi run dev`` creates it). Where it does not exist at all — a
            macOS home is ``/Users``, not ``/home`` — ``PYTHONPATH`` supplies the code.
    """

    postgres: Postgres
    hosts: tuple[Host, ...]
    code_dir: str = DEFAULT_CODE_DIR

    def host(self, name: str) -> Host:
        """The host called ``name``."""
        for host in self.hosts:
            if host.name == name:
                return host
        raise KeyError(f"no host named {name!r} in the fleet; have {self.names()}")

    def names(self) -> list[str]:
        """Every host's name, in inventory order."""
        return [host.name for host in self.hosts]

    @property
    def local(self) -> Host | None:
        """The host with ``ssh: null``, if any."""
        return next((host for host in self.hosts if host.is_local), None)


def _host_from_mapping(name: str, raw: Mapping[str, Any]) -> Host:
    unknown = set(raw) - set(Host.__dataclass_fields__)
    if unknown:
        raise FleetConfigError(f"host {name!r}: unknown key(s) {sorted(unknown)}")
    if "ssh" not in raw:
        raise FleetConfigError(f"host {name!r}: ssh is required (null for this machine)")
    if "repo_dir" not in raw or "work_dir" not in raw:
        raise FleetConfigError(f"host {name!r}: repo_dir and work_dir are required")
    kwargs = dict(raw)
    kwargs["capabilities"] = frozenset(raw.get("capabilities") or ())
    kwargs["env"] = {str(k): str(v) for k, v in (raw.get("env") or {}).items()}
    for key in ("repo_dir", "work_dir", "data_dir"):
        if kwargs.get(key):
            kwargs[key] = str(kwargs[key]).rstrip("/")
    return Host(name=name, **kwargs)


def load_fleet(path: str | os.PathLike | None = None) -> Fleet:
    """Read the inventory from ``path``, ``PLANETARY_FLEET_CONFIG`` or ``dags/fleet.yaml``."""
    path = Path(path or os.environ.get("PLANETARY_FLEET_CONFIG") or DEFAULT_CONFIG_PATH)
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except FileNotFoundError as exc:
        raise FleetConfigError(f"fleet inventory not found at {path}") from exc

    hosts = tuple(_host_from_mapping(n, h or {}) for n, h in (raw.get("hosts") or {}).items())
    if not hosts:
        raise FleetConfigError(f"{path}: no hosts defined")
    if sum(host.is_local for host in hosts) > 1:
        raise FleetConfigError(f"{path}: more than one host has ssh: null")
    pg = raw.get("postgres") or {}
    if "host" not in pg:
        raise FleetConfigError(f"{path}: postgres.host is required")
    code_dir = str(raw.get("code_dir") or DEFAULT_CODE_DIR).rstrip("/")
    return Fleet(postgres=Postgres(**pg), hosts=hosts, code_dir=code_dir)


def run_on(
    host: Host,
    command: str,
    timeout: float = 30.0,
    check: bool = False,
    input: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a POSIX shell command on ``host`` and capture its output.

    ``input`` is fed to the command's stdin, which is how secrets are passed: never on a
    command line, where ``ps`` on the host would show them.
    """
    if host.is_local:
        argv = ["/bin/sh", "-c", command]
    else:
        # The remote login shell may be zsh (macOS); hand the command to sh explicitly so
        # the syntax is the same everywhere.
        argv = ["ssh", *SSH_OPTIONS, host.ssh, f"/bin/sh -c {shlex.quote(command)}"]
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
        input=input,
        stdin=subprocess.DEVNULL if input is None else None,
    )


@dataclass(frozen=True)
class Probe:
    """What a host looked like when last asked."""

    reachable: bool
    total_gb: float = 0.0
    available_gb: float = 0.0
    disk_free_gb: float = 0.0
    cpus: int = 0
    load1: float = 0.0
    platform: str = ""
    home: str = ""
    error: str = ""
    at: float = 0.0


def probe(host: Host, timeout: float = 20.0) -> Probe:
    """Measure memory and disk on ``host``. Never raises; failures are unreachable."""
    script = _PROBE_SCRIPT.format(work_dir=host.work_dir)
    command = f"{shell_path(host.python)} -I -c {shlex.quote(script)}"
    now = time.time()
    try:
        result = run_on(host, command, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError) as exc:
        return Probe(reachable=False, error=f"{type(exc).__name__}: {exc}", at=now)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        return Probe(reachable=False, error=detail[-1] if detail else "probe failed", at=now)
    try:
        data = json.loads(result.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError) as exc:
        return Probe(reachable=False, error=f"unreadable probe output: {exc}", at=now)
    return Probe(reachable=True, at=now, **data)


class ProbeCache:
    """Probes each host at most once per :data:`PROBE_TTL_SECONDS`."""

    def __init__(self, ttl: float = PROBE_TTL_SECONDS):
        """Cache probes for ``ttl`` seconds."""
        self.ttl = ttl
        self._cache: dict[str, Probe] = {}

    def get(self, host: Host) -> Probe:
        """The host's probe, refreshed if older than the TTL."""
        cached = self._cache.get(host.name)
        if cached is not None and time.time() - cached.at < self.ttl:
            return cached
        fresh = probe(host)
        self._cache[host.name] = fresh
        return fresh

    def mark_down(self, name: str, error: str, seconds: float = 120.0) -> None:
        """Treat a host as unreachable for ``seconds``, e.g. after a launch on it failed."""
        self._cache[name] = Probe(reachable=False, error=error, at=time.time() - self.ttl + seconds)


__all__ = [
    "DEFAULT_CODE_DIR",
    "DEFAULT_CONFIG_PATH",
    "HOST_ENV_FILE",
    "Fleet",
    "FleetConfigError",
    "Host",
    "Postgres",
    "Probe",
    "ProbeCache",
    "load_fleet",
    "probe",
    "run_on",
    "shell_path",
]
