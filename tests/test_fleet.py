"""Tests for spreading runs over machines: placement, the inventory, and the launch plumbing."""

from __future__ import annotations

import os
import pathlib
import stat
import subprocess
import time

import pytest
from dagster._core.launcher import WorkerStatus
from dagster._core.storage.dagster_run import DagsterRun

from dags.fleet import hosts as hosts_module
from dags.fleet.hosts import Fleet, FleetConfigError, Host, Postgres, Probe, load_fleet
from dags.fleet.launcher import FleetRunLauncher, _remote_origin, launch_command
from dags.fleet.placement import (
    FLEET_DISK_TAG,
    FLEET_HOST_TAG,
    FLEET_MEMORY_TAG,
    FLEET_PID_TAG,
    HOST_PIN_TAG,
    REQUIRES_TAG,
    RUN_OVERHEAD_GB,
    Load,
    NoFit,
    Placement,
    RunNeeds,
    choose_host,
    ledger,
    needs_for_run,
)

GB_TAG = "planetary/memory_gb"
CLASS_TAG = "planetary/memory_class"


def host(name: str, **overrides) -> Host:
    defaults = dict(
        ssh=name,
        repo_dir=f"~/fleet/{name}",
        work_dir=f"~/work/{name}",
        memory_gb=48.0,
        reserve_gb=4.0,
        min_free_disk_gb=50.0,
        max_runs=4,
    )
    defaults.update(overrides)
    return Host(name=name, **defaults)


def probe(total=64.0, available=60.0, disk=1000.0) -> Probe:
    return Probe(reachable=True, total_gb=total, available_gb=available, disk_free_gb=disk)


def needs(memory=8.0, disk=10.0, **kwargs) -> RunNeeds:
    return RunNeeds(memory_gb=memory, disk_gb=disk, **kwargs)


# --- what a run needs -------------------------------------------------------------------


def test_a_single_step_run_reserves_its_declared_memory_plus_overhead():
    result = needs_for_run([{GB_TAG: "12"}], {})
    assert result.memory_gb == 12 + RUN_OVERHEAD_GB


def test_a_multi_step_run_reserves_its_heaviest_concurrent_steps():
    steps = [{GB_TAG: str(gb)} for gb in (2, 30, 4, 16, 8)]
    result = needs_for_run(steps, {}, steps_per_run=2)
    assert result.memory_gb == 30 + 16 + RUN_OVERHEAD_GB


def test_an_undeclared_step_falls_back_to_its_class_then_the_default():
    assert needs_for_run([{CLASS_TAG: "medium"}], {}).memory_gb == 16 + RUN_OVERHEAD_GB
    assert needs_for_run([{}], {}).memory_gb == 8 + RUN_OVERHEAD_GB
    # Garbage is not a declaration.
    assert needs_for_run([{GB_TAG: "lots"}], {}).memory_gb == 8 + RUN_OVERHEAD_GB


def test_requirements_are_the_union_over_steps_and_the_run():
    result = needs_for_run(
        [{REQUIRES_TAG: "docker"}, {REQUIRES_TAG: "gpu, docker"}],
        {REQUIRES_TAG: "big-disk", HOST_PIN_TAG: "pb"},
    )
    assert result.requires == {"docker", "gpu", "big-disk"}
    assert result.pinned_host == "pb"


def test_the_ledger_sums_reservations_per_host_and_ignores_unplaced_runs():
    loads = ledger(
        [
            {FLEET_HOST_TAG: "a", FLEET_MEMORY_TAG: "8", FLEET_DISK_TAG: "10"},
            {FLEET_HOST_TAG: "a", FLEET_MEMORY_TAG: "4.5", FLEET_DISK_TAG: "5"},
            {FLEET_HOST_TAG: "b", FLEET_MEMORY_TAG: "16"},
            # A launch that failed blanks its host; it holds nothing.
            {FLEET_HOST_TAG: "", FLEET_MEMORY_TAG: "64"},
            {},
        ]
    )
    assert loads == {"a": Load(2, 12.5, 15.0), "b": Load(1, 16.0, 0.0)}


# --- choosing a host --------------------------------------------------------------------


def test_the_host_with_the_most_headroom_wins():
    a, b = host("a", memory_gb=32), host("b", memory_gb=48)
    choice = choose_host(needs(), [a, b], {"a": probe(), "b": probe()}, {})
    assert isinstance(choice, Placement) and choice.host.name == "b"


def test_reservations_already_on_a_host_count_against_it():
    a, b = host("a"), host("b")
    loads = {"b": Load(runs=1, memory_gb=40)}
    choice = choose_host(needs(memory=16), [a, b], {"a": probe(), "b": probe()}, loads)
    assert choice.host.name == "a"


def test_live_memory_is_checked_independently_of_the_ledger():
    # The ledger says a is empty, but something outside Dagster is using its memory.
    a, b = host("a"), host("b", memory_gb=16)
    probes = {"a": probe(available=10), "b": probe()}
    choice = choose_host(needs(memory=8), [a, b], probes, {})
    assert choice.host.name == "b"


def test_disk_and_run_count_limits_are_respected():
    a, b, c = host("a"), host("b", max_runs=1), host("c")
    probes = {"a": probe(disk=55), "b": probe(), "c": probe()}
    loads = {"b": Load(runs=1, memory_gb=1)}
    choice = choose_host(needs(disk=10), [a, b, c], probes, loads)
    assert choice.host.name == "c"


def test_a_full_fleet_is_temporary_and_says_why():
    a = host("a")
    choice = choose_host(needs(memory=30), [a], {"a": probe()}, {"a": Load(1, 40, 0)})
    assert isinstance(choice, NoFit) and not choice.permanent
    assert "a:" in choice.reason and "capacity" in choice.reason


def test_a_run_bigger_than_any_host_fails_rather_than_waiting_forever():
    a, b = host("a", memory_gb=32), host("b", memory_gb=48)
    choice = choose_host(needs(memory=100), [a, b], {"a": probe(), "b": probe()}, {})
    assert isinstance(choice, NoFit) and choice.permanent


def test_an_unreachable_host_might_be_big_enough_so_too_big_is_not_yet_permanent():
    a, b = host("a", memory_gb=32), host("b")
    probes = {"a": probe(), "b": Probe(reachable=False, error="down")}
    choice = choose_host(needs(memory=100), [a, b], probes, {})
    assert isinstance(choice, NoFit) and not choice.permanent


def test_capabilities_restrict_placement_and_a_missing_one_is_permanent():
    a, pb = host("a"), host("pb", ssh=None, capabilities=frozenset({"docker"}))
    probes = {"a": probe(), "pb": probe(available=20)}
    choice = choose_host(needs(requires=frozenset({"docker"})), [a, pb], probes, {})
    assert choice.host.name == "pb"
    missing = choose_host(needs(requires=frozenset({"gpu"})), [a, pb], probes, {})
    assert isinstance(missing, NoFit) and missing.permanent


def test_disabled_hosts_and_pins():
    a, b = host("a", enabled=False), host("b")
    probes = {"a": probe(), "b": probe()}
    assert choose_host(needs(), [a, b], probes, {}).host.name == "b"
    pinned = choose_host(needs(pinned_host="a"), [a, b], probes, {})
    assert isinstance(pinned, NoFit) and pinned.permanent
    assert choose_host(needs(pinned_host="b"), [a, b], probes, {}).host.name == "b"


def test_a_retry_goes_back_to_its_previous_host_when_it_fits():
    a, b = host("a", memory_gb=16), host("b", memory_gb=48)
    probes = {"a": probe(), "b": probe()}
    assert choose_host(needs(preferred_host="a"), [a, b], probes, {}).host.name == "a"
    # ...but not when it no longer fits there.
    loads = {"a": Load(1, 12, 0)}
    assert choose_host(needs(preferred_host="a"), [a, b], probes, loads).host.name == "b"


def test_capacity_is_the_declared_figure_or_a_fraction_of_physical_memory():
    assert host("a", memory_gb=48).capacity_gb(64) == 48
    assert host("a", memory_gb=128).capacity_gb(64) == 64
    assert host("a", memory_gb=None, memory_fraction=0.5).capacity_gb(64) == 32


# --- the inventory ----------------------------------------------------------------------


def test_the_committed_inventory_loads():
    fleet = load_fleet()
    assert fleet.local is not None
    assert len(fleet.names()) == len(set(fleet.names()))
    for entry in fleet.hosts:
        # A remote repo_dir is rsynced with --delete; it must never be a working copy.
        if not entry.is_local:
            assert "planetary-fleet" in entry.repo_dir, entry.name


def test_inventory_errors_are_reported(tmp_path):
    path = tmp_path / "fleet.yaml"
    path.write_text("postgres: {host: x}\nhosts:\n  a: {ssh: a, repo_dir: /r}\n")
    with pytest.raises(FleetConfigError, match="work_dir"):
        load_fleet(path)
    path.write_text("postgres: {host: x}\nhosts:\n  a: {repo_dir: /r, work_dir: /w}\n")
    with pytest.raises(FleetConfigError, match="ssh"):
        load_fleet(path)
    path.write_text(
        "postgres: {host: x}\nhosts:\n  a: {ssh: a, repo_dir: /r, work_dir: /w, ram: 4}\n"
    )
    with pytest.raises(FleetConfigError, match="ram"):
        load_fleet(path)
    path.write_text("hosts:\n  a: {ssh: null, repo_dir: /r, work_dir: /w}\n")
    with pytest.raises(FleetConfigError, match="postgres"):
        load_fleet(path)


def test_paths_under_home_stay_expandable_in_the_shell():
    assert hosts_module.shell_path("~/a b") == "\"$HOME\"/'a b'"
    assert hosts_module.shell_path("/abs/path") == "/abs/path"
    out = subprocess.run(
        ["/bin/sh", "-c", f"echo {hosts_module.shell_path('~/x')}"], capture_output=True, text=True
    ).stdout.strip()
    assert out == os.path.expanduser("~/x")


# --- launching and watching a worker ----------------------------------------------------


@pytest.fixture
def local_host(tmp_path) -> Host:
    """A local host whose "pixi" activates an env that sets one marker variable."""
    pixi = tmp_path / "pixi"
    pixi.write_text("#!/bin/sh\n[ \"$1\" = shell-hook ] && echo 'export ACTIVATED=yes'\n")
    pixi.chmod(pixi.stat().st_mode | stat.S_IEXEC)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pixi.toml").write_text("")
    return Host(
        name="here",
        ssh=None,
        repo_dir=str(repo),
        work_dir=str(tmp_path / "work"),
        pixi=str(pixi),
    )


def _wait_for(predicate, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_launch_command_starts_a_detached_worker_in_the_activated_env(local_host, monkeypatch):
    from dags.fleet import launcher as launcher_module

    monkeypatch.setattr(launcher_module, "HOST_ENV_FILE", "/nonexistent/env")
    run_id = "run-0123"
    env = {**local_host.run_env(), "MEMORY_CEILING_GB": "9"}
    # "; true" keeps sh from exec-ing sleep, which would drop the run id ($0) from ps.
    script = 'echo "$ACTIVATED $MEMORY_CEILING_GB $PLANETARY_FLEET_HOST $PWD"; sleep 30; true'
    command = launch_command(local_host, run_id, ["sh", "-c", script, run_id], env)

    result = hosts_module.run_on(local_host, command)
    pid = int(result.stdout.strip().splitlines()[-1])
    try:
        log = pathlib.Path(local_host.log_dir) / f"{run_id}.log"
        assert _wait_for(lambda: log.exists() and log.read_text().strip())
        assert log.read_text().split() == ["yes", "9", "here", local_host.repo_dir]
        # The printed pid is the worker itself (exec'd), with the run id on its command
        # line — which is what the health check and terminate rely on.
        cmdline = pathlib.Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        assert run_id.encode() in cmdline
        for sub in ("scratch", "storage", "compute_logs", "data", "logs"):
            assert (pathlib.Path(local_host.work_dir) / sub).is_dir()
    finally:
        os.kill(pid, 15)


def _launcher_for(fleet_host: Host) -> FleetRunLauncher:
    launcher = FleetRunLauncher()
    fleet = Fleet(postgres=Postgres(host="127.0.0.1"), hosts=(fleet_host,))
    launcher._fleet = lambda: fleet  # type: ignore[method-assign]
    return launcher


def test_worker_health_follows_the_process_and_reports_its_log(local_host):
    run_id = "6f1c1a52-0000-4000-8000-000000000001"
    # The run id rides along as $0, as it does inside the real worker's JSON argument.
    proc = subprocess.Popen(["sh", "-c", "sleep 30; true", run_id])
    launcher = _launcher_for(local_host)
    run = DagsterRun(
        job_name="job",
        run_id=run_id,
        tags={FLEET_HOST_TAG: "here", FLEET_PID_TAG: str(proc.pid)},
    )
    try:
        assert launcher.check_run_worker_health(run).status == WorkerStatus.RUNNING
    finally:
        proc.terminate()
        proc.wait()

    pathlib.Path(local_host.log_dir).mkdir(parents=True)
    (pathlib.Path(local_host.log_dir) / f"{run_id}.log").write_text("Killed\n")
    result = launcher.check_run_worker_health(run)
    assert result.status == WorkerStatus.FAILED
    assert "Killed" in result.msg


def test_a_reused_pid_is_not_mistaken_for_the_worker(local_host):
    # Some other process now holds the pid: its command line lacks the run id.
    proc = subprocess.Popen(["sleep", "30"])
    try:
        run = DagsterRun(
            job_name="job",
            run_id="6f1c1a52-0000-4000-8000-000000000002",
            tags={FLEET_HOST_TAG: "here", FLEET_PID_TAG: str(proc.pid)},
        )
        result = _launcher_for(local_host).check_run_worker_health(run)
        assert result.status == WorkerStatus.FAILED
    finally:
        proc.terminate()
        proc.wait()


def test_an_untagged_run_has_no_worker_to_find(local_host):
    run = DagsterRun(job_name="job", run_id="6f1c1a52-0000-4000-8000-000000000003")
    result = _launcher_for(local_host).check_run_worker_health(run)
    assert result.status == WorkerStatus.NOT_FOUND


def test_a_remote_origin_swaps_only_how_the_worker_is_started():
    from dagster._core.code_pointer import ModuleCodePointer
    from dagster._core.origin import JobPythonOrigin, RepositoryPythonOrigin

    origin = JobPythonOrigin(
        "job",
        RepositoryPythonOrigin(
            executable_path="/home/me/repo/.pixi/envs/default/bin/python",
            code_pointer=ModuleCodePointer("dags.definitions", "defs", "/home/me/repo"),
        ),
    )
    remote = host("r", repo_dir="~/planetary-fleet/repo")
    rewritten = _remote_origin(origin, remote, home="/Users/me")
    repo = rewritten.repository_origin
    # The worker loads code from the origin stored with the run, never this one, so the
    # working directory is deliberately left as stored; code_guard checks it instead.
    assert repo.code_pointer == origin.repository_origin.code_pointer
    assert repo.executable_path == "/Users/me/planetary-fleet/repo/.pixi/envs/default/bin/python"
    assert repo.entry_point == ["dagster"]
    local = host("l", ssh=None)
    assert _remote_origin(origin, local, home="/x") is origin


@pytest.mark.parametrize("layout", ["missing", "symlink", "foreign"])
def test_the_code_guard_refuses_a_working_directory_that_is_not_the_fleet_copy(
    tmp_path, local_host, layout
):
    """Regression: a worker on aero imported a personal checkout at the daemon's path.

    The run's stored working directory goes first on the worker's sys.path, so where that
    path exists it must be the fleet's copy of the code.
    """
    from dags.fleet.launcher import WRONG_CODE_EXIT, code_guard

    stored = tmp_path / "stored"
    if layout == "symlink":
        stored.symlink_to(local_host.repo_dir, target_is_directory=True)
    elif layout == "foreign":
        stored.mkdir()
    result = hosts_module.run_on(local_host, code_guard(local_host, str(stored)))
    if layout == "foreign":
        assert result.returncode == WRONG_CODE_EXIT
        assert "not the fleet copy" in result.stderr
    else:
        assert result.returncode == 0, result.stderr


def test_a_killed_worker_leaves_no_orphans_behind(local_host, monkeypatch):
    """Regression: kill -9 on a worker left its step process running, reparented to init.

    An orphaned step can go on writing to a store while the run's retry writes to it too.
    The worker is started as its own process group, and the health check that finds it
    dead kills whatever is left of that group: the step, and anything the step started.
    """
    from dags.fleet import launcher as launcher_module

    monkeypatch.setattr(launcher_module, "HOST_ENV_FILE", "/nonexistent/env")
    run_id = "6f1c1a52-0000-4000-8000-000000000004"
    # A "step" whose command line runs the fleet's code, with a child of its own that
    # does not — as a step running wgrib2 would have.
    step = f'sh -c "sleep 300; true" {local_host.repo_dir}/step.py'
    script = f"{step} & sleep 300; true"
    command = launch_command(local_host, run_id, ["sh", "-c", script, run_id], local_host.run_env())
    pid = int(hosts_module.run_on(local_host, command).stdout.strip().splitlines()[-1])

    def group() -> list[str]:
        out = subprocess.run(["ps", "-eo", "pid=,pgid="], capture_output=True, text=True).stdout
        return [line.split()[0] for line in out.splitlines() if line.split()[1] == str(pid)]

    assert _wait_for(lambda: len(group()) >= 4), group()  # worker, step, two sleeps
    os.kill(pid, 9)
    assert _wait_for(lambda: str(pid) not in group())

    run = DagsterRun(
        job_name="job",
        run_id=run_id,
        tags={FLEET_HOST_TAG: "here", FLEET_PID_TAG: str(pid)},
    )
    result = _launcher_for(local_host).check_run_worker_health(run)
    assert result.status == WorkerStatus.FAILED
    assert "orphaned" in result.msg
    assert _wait_for(lambda: not group()), group()


def test_reaping_leaves_a_group_with_none_of_the_fleets_code_alone(local_host):
    from dags.fleet.launcher import reap_orphans_command

    proc = subprocess.Popen(["sleep", "300"], start_new_session=True)
    try:
        out = hosts_module.run_on(local_host, reap_orphans_command(local_host, str(proc.pid)))
        assert out.stdout.strip() == "reaped 0"
        assert proc.poll() is None
    finally:
        proc.kill()
        proc.wait()
