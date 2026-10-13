"""A run launcher that spreads Dagster runs over the machines in ``dags/fleet.yaml``.

Configured in ``dags/dagster.yaml``. For every run the queue releases it:

1. works out the run's memory, disk and capability needs from the tags of the steps it
   will execute (see :func:`dags.fleet.placement.needs_for_run`);
2. probes the hosts and reads the reservations of runs already in flight;
3. picks the host with the most headroom that fits, and records the reservation on the
   run's tags so the next launch sees it;
4. rsyncs the code to that host if the checkout changed, then starts
   ``dagster api execute_run`` there under ``nohup``, inside the host's pixi env.

The run process then talks to the shared Postgres instance like any other run worker, so
its events, asset materialisations and concurrency-pool slots are visible fleet-wide.
That shared storage is what keeps "one writer per icechunk store" true across machines.

**When no host has room**, the launcher raises :class:`FleetFull`. That is a subclass of
``DagsterUserCodeUnreachableError``, which the queue daemon answers by putting the run
back in the queue and pausing dequeues for ``user_code_failure_retry_delay`` seconds.
On its own that would block the whole queue behind the run — the pause applies to the
code location, and the run would be first in line again after it — leaving hosts idle
that could take the runs behind it. So before requeueing, the launcher lowers the run's
``dagster/priority`` by one: the next pass dequeues everything that does fit first, and
the run is retried behind them. A run that keeps not fitting sinks further, by at most
:data:`MAX_DEMOTION`. When a run could *never* fit, it fails instead.
"""

from __future__ import annotations

import shlex
import threading
from typing import Any, Mapping

import dagster._check as check
from dagster import Field, IntSource, StringSource
from dagster._core.errors import DagsterUserCodeUnreachableError
from dagster._core.launcher import (
    CheckRunHealthResult,
    LaunchRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.origin import JobPythonOrigin
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunsFilter
from dagster._core.storage.tags import PRIORITY_TAG
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from loguru import logger

from dags.fleet.hosts import HOST_ENV_FILE, Fleet, Host, ProbeCache, load_fleet, run_on, shell_path
from dags.fleet.placement import (
    FLEET_DISK_TAG,
    FLEET_HOST_TAG,
    FLEET_MEMORY_TAG,
    FLEET_PID_TAG,
    STEPS_PER_RUN,
    NoFit,
    RunNeeds,
    choose_host,
    ledger,
    needs_for_run,
)
from dags.fleet.sync import CodeSync

#: Statuses that hold a reservation. CANCELING runs are still using their memory.
IN_FLIGHT = [DagsterRunStatus.STARTING, DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]

PARENT_RUN_TAG = "dagster/parent_run_id"

#: How far below its original priority a run that keeps not fitting may sink. Bounded so
#: that a run waiting for a big host does not end up behind every run submitted later.
MAX_DEMOTION = 10
#: Where the run's priority before any demotion is kept.
BASE_PRIORITY_TAG = "planetary/fleet_base_priority"


class FleetFull(DagsterUserCodeUnreachableError):
    """No host can take the run right now; the queue daemon will requeue it."""


class FleetPlacementError(Exception):
    """No host could ever take the run; the run is failed."""


#: Exit status of the launch script when the run's code would not be the fleet's copy.
WRONG_CODE_EXIT = 98


def _remote_origin(origin: JobPythonOrigin, host: Host, home: str) -> JobPythonOrigin:
    """The run's origin with the interpreter and entry point swapped for the host's.

    Only what starts the process changes. The worker loads its *code* from the origin
    stored with the run, whatever is passed here, so the working directory is left alone;
    keeping that path valid on every host is :attr:`dags.fleet.hosts.Fleet.code_dir`'s job.
    """
    if host.is_local:
        return origin
    repo = origin.repository_origin
    return origin._replace(
        repository_origin=repo._replace(
            executable_path=host.expand(host.python, home),
            # Resolved on PATH inside the pixi env the launch command activates.
            entry_point=["dagster"],
        )
    )


def working_directory(origin: JobPythonOrigin) -> str | None:
    """The directory the worker will put first on ``sys.path`` to import the code."""
    return getattr(origin.repository_origin.code_pointer, "working_directory", None)


def code_guard(host: Host, origin_dir: str | None) -> str:
    """Shell that refuses to start a worker that would import someone else's code.

    The worker inserts the run's stored working directory ahead of ``PYTHONPATH``. If that
    path exists on the host but is not the fleet's copy — a personal checkout at the same
    path as the daemon's, say — the run would silently execute that code instead.
    """
    if not origin_dir:
        return ":"
    wd, repo = shlex.quote(origin_dir), shell_path(host.repo_dir)
    return (
        f'if [ -e {wd} ] && [ "$(cd {wd} && pwd -P)" != "$(cd {repo} && pwd -P)" ]; then '
        f'echo "{origin_dir} on {host.name} is not the fleet copy at {host.repo_dir}; '
        f"start Dagster with 'pixi run dev' so it loads code from the fleet code_dir\" >&2; "
        f"exit {WRONG_CODE_EXIT}; fi"
    )


def launch_command(
    host: Host,
    run_id: str,
    args: list[str],
    env: Mapping[str, str],
    origin_dir: str | None = None,
) -> str:
    """The shell command that starts a run worker on ``host`` and prints its pid.

    The worker is started under ``nohup`` with stdin closed so it outlives the SSH
    session, and ``exec``-ed so the printed pid is the Dagster process itself — which is
    what the health check and ``terminate`` signal.
    """
    work_dirs = [host.log_dir, host.scratch_dir, env["DAGSTER_STORAGE_DIR"],
                 env["DAGSTER_COMPUTE_LOG_DIR"], env["PLANETARY_DATASETS_DATA_DIR"]]  # fmt: skip
    exports = " ".join(f"{key}={shell_path(value)}" for key, value in env.items())
    log = f"{host.log_dir}/{run_id}.log"
    env_file = shell_path(HOST_ENV_FILE)
    # setsid makes the worker a session and process-group leader, so every process of the
    # run shares its pid as their group id and can be found again by it — see
    # reap_orphans_command. Done in Python because macOS has no setsid(1).
    activate_and_exec = (
        'p="$1"; m="$2"; shift 2; '
        'eval "$("$p" shell-hook --frozen --manifest-path "$m")" && '
        "exec python -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "
        '"$@"'
    )
    # Newlines, not "; ": a backgrounded command cannot be followed by ";".
    return "\n".join(
        [
            "mkdir -p " + " ".join(shell_path(d) for d in work_dirs),
            f"cd {shell_path(host.repo_dir)} || exit 97",
            code_guard(host, origin_dir),
            f"if [ -f {env_file} ]; then set -a; . {env_file}; set +a; fi",
            f"export {exports}",
            f"nohup /bin/sh -c {shlex.quote(activate_and_exec)} fleet "
            f"{shell_path(host.pixi)} {shell_path(host.repo_dir + '/pixi.toml')} "
            f"{shlex.join(args)} > {shell_path(log)} 2>&1 < /dev/null &",
            "echo $!",
        ]
    )  # fmt: skip


def reap_orphans_command(host: Host, pid: str) -> str:
    """Shell that kills what is left of a dead worker's process group, printing a count.

    A worker killed outright (``kill -9``, or an OOM kill that picked it rather than its
    step) leaves its step processes running, reparented to init. One of those may still be
    writing to a store when the retry starts writing to it too, so they must go — along
    with anything they started (``wgrib2``, ``cdo``, a download), which shares the group.

    The group is only taken to be the run's if one of its processes is running the fleet's
    copy of the code, so a group id recycled by some unrelated session is left alone.
    """
    find_group = (
        f"awk -v g={pid} -v d={shell_path(host.repo_dir)} "
        '\'$2 == g { pids = pids " " $1; if (index($0, d)) ours = 1 } '
        "END { if (ours) print pids }'"
    )
    return (
        f"n=0; for p in $(ps -ww -eo pid=,pgid=,command= | {find_group}); "
        'do kill -KILL "$p" 2>/dev/null && n=$((n+1)); done; echo "reaped $n"'
    )


class FleetRunLauncher(RunLauncher, ConfigurableClass):
    """Launches each run on whichever fleet host has room for it."""

    def __init__(
        self,
        inst_data: ConfigurableClassData | None = None,
        fleet_config: str | None = None,
        steps_per_run: int = STEPS_PER_RUN,
    ):
        """Create the launcher; see ``config_type`` for the options."""
        self._inst_data = inst_data
        self._fleet_config = fleet_config
        self._steps_per_run = steps_per_run
        self._probes = ProbeCache()
        self._sync = CodeSync()
        self._lock = threading.Lock()
        super().__init__()

    # -- ConfigurableClass ------------------------------------------------------------

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        """The config this launcher was built from."""
        return self._inst_data

    @classmethod
    def config_type(cls) -> dict[str, Field]:
        """Config accepted under ``run_launcher.config`` in ``dagster.yaml``."""
        return {
            "fleet_config": Field(StringSource, is_required=False),
            "steps_per_run": Field(IntSource, is_required=False, default_value=STEPS_PER_RUN),
        }

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> "FleetRunLauncher":
        """Build the launcher from its ``dagster.yaml`` config."""
        return cls(inst_data=inst_data, **config_value)

    # -- placement --------------------------------------------------------------------

    def _fleet(self) -> Fleet:
        # Re-read on every use: editing dags/fleet.yaml takes effect without a restart.
        return load_fleet(self._fleet_config)

    def _step_tags(self, run: DagsterRun) -> list[dict[str, str]]:
        snapshot_id = run.execution_plan_snapshot_id
        snapshot = self._instance.get_execution_plan_snapshot(snapshot_id) if snapshot_id else None
        if snapshot is None:
            return []
        selected = set(snapshot.step_keys_to_execute or ())
        return [
            dict(step.tags or {}) for step in snapshot.steps if not selected or step.key in selected
        ]

    def _preferred_host(self, run: DagsterRun) -> str | None:
        parent_id = run.tags.get(PARENT_RUN_TAG)
        parent = self._instance.get_run_by_id(parent_id) if parent_id else None
        return parent.tags.get(FLEET_HOST_TAG) if parent else None

    def needs(self, run: DagsterRun) -> RunNeeds:
        """What ``run`` needs from a host."""
        return needs_for_run(
            self._step_tags(run),
            run.tags,
            preferred_host=self._preferred_host(run),
            steps_per_run=self._steps_per_run,
        )

    def _in_flight_tags(self, exclude: str) -> list[Mapping[str, str]]:
        runs = self._instance.get_runs(filters=RunsFilter(statuses=IN_FLIGHT))
        return [run.tags for run in runs if run.run_id != exclude]

    # -- RunLauncher --------------------------------------------------------------------

    def launch_run(self, context: LaunchRunContext) -> None:
        """Place the run on a host and start its worker there."""
        run = context.dagster_run
        origin = check.not_none(context.job_code_origin)
        fleet = self._fleet()
        needs = self.needs(run)

        with self._lock:
            probes = {host.name: self._probes.get(host) for host in fleet.hosts if host.enabled}
            choice = choose_host(
                needs, fleet.hosts, probes, ledger(self._in_flight_tags(run.run_id))
            )
            if isinstance(choice, NoFit):
                message = (
                    f"no fleet host for {needs.memory_gb:g} GB / {needs.disk_gb:g} GB disk"
                    f"{' requiring ' + ','.join(sorted(needs.requires)) if needs.requires else ''}"
                    f": {choice.reason}"
                )
                if choice.permanent:
                    raise FleetPlacementError(message)
                self._demote(run)
                raise FleetFull(message)
            host = choice.host
            # Reserve before letting go of the lock, so the next launch sees it.
            self._instance.add_run_tags(
                run.run_id,
                {
                    FLEET_HOST_TAG: host.name,
                    FLEET_MEMORY_TAG: f"{needs.memory_gb:g}",
                    FLEET_DISK_TAG: f"{needs.disk_gb:g}",
                },
            )

        home = probes[host.name].home
        try:
            self._sync.ensure(host)
            pid = self._start(host, run, origin, needs, home)
        except FleetPlacementError:
            self._instance.add_run_tags(run.run_id, {FLEET_HOST_TAG: ""})
            raise
        except Exception as exc:  # noqa: BLE001 - any failure here is the host's, not the run's
            self._instance.add_run_tags(run.run_id, {FLEET_HOST_TAG: ""})
            self._probes.mark_down(host.name, f"launch failed: {exc}")
            self._demote(run)
            logger.warning(f"fleet: launch of {run.run_id} on {host.name} failed: {exc}")
            raise FleetFull(f"launch on {host.name} failed, will retry elsewhere: {exc}") from exc

        self._instance.add_run_tags(run.run_id, {FLEET_PID_TAG: str(pid)})
        self._instance.report_engine_event(
            f"Launched on {host.name} (pid {pid}), reserving {needs.memory_gb:g} GB memory "
            f"and {needs.disk_gb:g} GB disk. Worker log: "
            f"{host.name}:{host.log_dir}/{run.run_id}.log",
            run,
            cls=self.__class__,
        )

    def _demote(self, run: DagsterRun) -> None:
        """Move a run that cannot be placed behind the queued runs that can."""

        def as_int(value: str | None) -> int:
            try:
                return int(value or 0)
            except ValueError:
                return 0

        base = as_int(run.tags.get(BASE_PRIORITY_TAG, run.tags.get(PRIORITY_TAG)))
        current = as_int(run.tags.get(PRIORITY_TAG))
        self._instance.add_run_tags(
            run.run_id,
            {
                BASE_PRIORITY_TAG: str(base),
                PRIORITY_TAG: str(max(current - 1, base - MAX_DEMOTION)),
            },
        )

    def _start(
        self, host: Host, run: DagsterRun, origin: JobPythonOrigin, needs: RunNeeds, home: str
    ) -> int:
        args = ExecuteRunArgs(
            job_origin=_remote_origin(origin, host, home),
            run_id=run.run_id,
            # None: the worker builds its instance from DAGSTER_HOME on its own host,
            # whose dagster.yaml points at the same Postgres.
            instance_ref=None,
            set_exit_code_on_failure=True,
        ).get_command_args()
        env = host.run_env()
        # Holds the run's executor and require_memory() to what was reserved for it.
        env["MEMORY_CEILING_GB"] = f"{needs.memory_gb:g}"
        command = launch_command(host, run.run_id, list(args), env, working_directory(origin))
        result = run_on(host, command, timeout=60)
        if result.returncode == WRONG_CODE_EXIT:
            raise FleetPlacementError(result.stderr.strip())
        lines = result.stdout.strip().splitlines()
        if result.returncode != 0 or not lines or not lines[-1].isdigit():
            raise RuntimeError(
                f"exit {result.returncode}: {(result.stderr or result.stdout).strip()[-500:]}"
            )
        return int(lines[-1])

    def _worker(self, run: DagsterRun) -> tuple[Host, str] | None:
        name, pid = run.tags.get(FLEET_HOST_TAG), run.tags.get(FLEET_PID_TAG)
        if not name or not pid or not pid.isdigit():
            return None
        try:
            return self._fleet().host(name), pid
        except KeyError:
            return None

    def terminate(self, run_id: str) -> bool:
        """Send SIGTERM to the run's worker."""
        run = self._instance.get_run_by_id(run_id)
        if not run or run.is_finished:
            return False
        self._instance.report_run_canceling(run)
        worker = self._worker(run)
        if worker is None:
            self._instance.report_engine_event(
                "No fleet host/pid recorded for this run; cannot signal its worker.",
                run,
                cls=self.__class__,
            )
            return False
        host, pid = worker
        # SIGTERM, not SIGKILL: the run worker turns it into an interrupt, cleans up its
        # step processes and records the cancellation.
        result = run_on(host, f"kill -TERM {pid}", timeout=30)
        return result.returncode == 0

    @property
    def supports_check_run_worker_health(self) -> bool:
        """Workers are polled over SSH by run monitoring."""
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        """Whether the run's worker process is still alive on its host."""
        worker = self._worker(run)
        if worker is None:
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, msg="no fleet host/pid tags")
        host, pid = worker
        # Matching the run id on the command line guards against pid reuse.
        log = f"{shell_path(host.log_dir)}/{run.run_id}.log"
        command = (
            f"if ps -ww -p {pid} -o command= | grep -q -- {shlex.quote(run.run_id)}; "
            f"then echo alive; else echo gone; {reap_orphans_command(host, pid)}; "
            f"tail -n 20 {log} 2>/dev/null; fi; exit 0"
        )
        try:
            result = run_on(host, command, timeout=30)
        except Exception as exc:  # noqa: BLE001 - an unreachable host is unknown, not dead
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, msg=f"{host.name}: {exc}")
        lines = result.stdout.strip().splitlines()
        if result.returncode != 0 or not lines:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN, msg=f"{host.name}: {result.stderr.strip()[-300:]}"
            )
        if lines[0] == "alive":
            return CheckRunHealthResult(WorkerStatus.RUNNING)
        reaped = lines[1] if len(lines) > 1 and lines[1].startswith("reaped ") else ""
        tail = lines[2:] if reaped else lines[1:]
        orphans = f" ({reaped} orphaned process(es))" if reaped and reaped != "reaped 0" else ""
        return CheckRunHealthResult(
            WorkerStatus.FAILED,
            msg=f"worker pid {pid} on {host.name} has exited{orphans}. Log tail:\n"
            + "\n".join(tail),
        )

    def get_run_worker_debug_info(
        self, run: DagsterRun, include_container_logs: bool | None = True
    ) -> str | None:
        """Where the run's worker is and where its log is."""
        worker = self._worker(run)
        if worker is None:
            return None
        host, pid = worker
        return f"host={host.name} pid={pid} log={host.log_dir}/{run.run_id}.log"


__all__ = [
    "FleetFull",
    "FleetPlacementError",
    "FleetRunLauncher",
    "code_guard",
    "launch_command",
    "reap_orphans_command",
]
