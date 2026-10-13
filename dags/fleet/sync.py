"""Keep each remote host's copy of the code identical to the daemon's checkout.

A run executes whatever code is in ``Host.repo_dir`` on the host it lands on, so that
copy has to match what the daemon loaded — including uncommitted and untracked asset
modules, and ``pixi.lock``, which is not in git. Syncing the working tree with rsync
rather than pulling a commit is what guarantees that.

``--delete`` is scoped to the synced paths, and ``repo_dir`` on a remote host is meant
to be a directory only the fleet uses, so nothing else is ever removed.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import threading

from loguru import logger

from dags.fleet.hosts import REPO_ROOT, SSH_OPTIONS, Host, run_on, shell_path

#: Everything a run needs to load the code location and resolve its environment.
SYNC_PATHS = ("dags", "planetary_datasets", "pixi.toml", "pixi.lock", "setup.py")

SYNC_EXCLUDES = (
    "__pycache__/",
    "*.pyc",
    ".pytest_cache/",
    # Standalone PEP-723 scripts and their virtualenvs; never imported by a run.
    "dags/assets/virt/",
    # Instance storage, should DAGSTER_HOME ever have been used without the env vars.
    "dags/storage/",
    "dags/history/",
    "dags/schedules/",
    "dags/logs/",
)


class SyncError(RuntimeError):
    """The code could not be copied to a host."""


def tree_fingerprint(root=REPO_ROOT) -> tuple[int, float]:
    """(file count, newest mtime) over the synced paths; changes whenever a file does."""
    count, newest = 0, 0.0
    for rel in SYNC_PATHS:
        path = root / rel
        if path.is_file():
            count, newest = count + 1, max(newest, path.stat().st_mtime)
            continue
        for dirpath, dirnames, filenames in os.walk(path):
            dirnames[:] = [d for d in dirnames if d not in ("__pycache__", "virt", ".pytest_cache")]
            for name in filenames:
                if name.endswith(".pyc"):
                    continue
                count += 1
                newest = max(newest, os.stat(os.path.join(dirpath, name)).st_mtime)
    return count, newest


def _remote_path(path: str) -> str:
    # rsync resolves a relative remote path against the login directory, which is the
    # one form of "~/..." that every rsync version (and macOS's openrsync) agrees on.
    return path[2:] if path.startswith("~/") else path


def sync_host(host: Host, timeout: float = 300.0) -> None:
    """Copy the synced paths to ``host``. A no-op for the local host."""
    if host.is_local:
        return
    mkdir = run_on(host, f"mkdir -p {shell_path(host.repo_dir)}", timeout=30)
    if mkdir.returncode != 0:
        raise SyncError(f"{host.name}: cannot create {host.repo_dir}: {mkdir.stderr.strip()}")

    argv = [
        "rsync",
        "-az",
        "--delete",
        *(f"--exclude={pattern}" for pattern in SYNC_EXCLUDES),
        "-e",
        shlex.join(["ssh", *SSH_OPTIONS]),
        *[rel for rel in SYNC_PATHS if (REPO_ROOT / rel).exists()],
        f"{host.ssh}:{_remote_path(host.repo_dir)}/",
    ]
    result = subprocess.run(
        argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout,
        stdin=subprocess.DEVNULL,
    )  # fmt: skip
    if result.returncode != 0:
        raise SyncError(f"{host.name}: rsync failed: {result.stderr.strip()[-500:]}")
    logger.info(f"fleet: synced code to {host.name}:{host.repo_dir}")


class CodeSync:
    """Syncs a host only when the local tree has changed since its last sync."""

    def __init__(self) -> None:
        """Start with no host synced."""
        self._synced: dict[str, tuple[int, float]] = {}
        self._lock = threading.Lock()

    def ensure(self, host: Host) -> None:
        """Sync ``host`` unless it already has the current tree."""
        if host.is_local:
            return
        with self._lock:
            fingerprint = tree_fingerprint()
            if self._synced.get(host.name) == fingerprint:
                return
            sync_host(host)
            self._synced[host.name] = fingerprint


__all__ = ["SYNC_EXCLUDES", "SYNC_PATHS", "CodeSync", "SyncError", "sync_host", "tree_fingerprint"]
