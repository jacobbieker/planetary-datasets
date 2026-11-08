"""``_backfill_box.sh stop`` with and without lane names."""

import os
import pathlib
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
#: Two lanes the box script defines.
LANES = ("kenda", "uk_radar")


def _start_ticks(pid: int) -> str:
    stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    return stat.rsplit(") ", 1)[1].split()[19]


@pytest.fixture
def lanes(tmp_path):
    """A stand-in process, in its own process group, recorded as each lane."""
    logs = tmp_path / "backfill-logs"
    logs.mkdir()
    procs = {}
    for name in LANES:
        proc = subprocess.Popen(["sleep", "300"], start_new_session=True)
        (logs / f"{name}.start").write_text(_start_ticks(proc.pid) + "\n")
        (logs / f"{name}.pid").write_text(f"{proc.pid}\n")
        procs[name] = proc
    yield tmp_path, procs
    for proc in procs.values():
        proc.kill()
        proc.wait()


def stop(home: pathlib.Path, *names: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(ROOT / "_backfill_box.sh"), "stop", *names],
        env={**os.environ, "HOME": str(home)},
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_stop_with_a_name_stops_only_that_lane(lanes):
    home, procs = lanes
    done = stop(home, "kenda")
    assert done.returncode == 0, done.stderr
    assert procs["kenda"].wait(timeout=10) is not None
    assert procs["uk_radar"].poll() is None
    assert (home / "backfill-logs" / "uk_radar.pid").exists()


def test_an_unknown_name_stops_nothing(lanes):
    home, procs = lanes
    done = stop(home, "kenda", "no_such_lane")
    assert done.returncode == 2
    assert "no_such_lane" in done.stderr
    assert all(proc.poll() is None for proc in procs.values())


def test_stop_with_no_names_stops_every_lane(lanes):
    home, procs = lanes
    assert stop(home).returncode == 0
    for proc in procs.values():
        assert proc.wait(timeout=10) is not None
