"""A caught-up lane sleeps as a small process and then starts again."""

from __future__ import annotations

import importlib.util
import pathlib
import subprocess
import sys

spec = importlib.util.spec_from_file_location(
    "_backfill_lanes", pathlib.Path(__file__).resolve().parents[1] / "_backfill_lanes.py"
)


def test_the_sleeper_waits_then_runs_the_lane_again(tmp_path):
    lanes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lanes)
    marker = tmp_path / "ran"
    lane = [sys.executable, "-c", f"open({str(marker)!r}, 'w').write('again')"]
    done = subprocess.run(
        [sys.executable, "-c", lanes._SLEEPER, "0.2", *lane], capture_output=True, timeout=30
    )
    assert done.returncode == 0
    assert marker.read_text() == "again"


def test_pause_lightly_execs_the_sleeper_with_the_lane_command(monkeypatch):
    lanes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lanes)
    calls = []
    monkeypatch.setattr(lanes.os, "execv", lambda path, args: calls.append((path, args)))
    lanes.pause_lightly(7200, ["_backfill_lanes.py", "hafs_a_parent", "--pause", "7200"])
    path, args = calls[0]
    assert args[1] == "-c" and args[3] == "7200"
    assert args[4:] == [sys.executable, "_backfill_lanes.py", "hafs_a_parent", "--pause", "7200"]


def test_lanes_with_a_recent_window_replan_long_passes():
    lanes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lanes)
    recent = lanes.LANES["jpss_atms"]
    plain = next(lane for lane in lanes.LANES.values() if not lane.recent_days)
    began = 1_000_000.0
    assert not lanes.should_replan(recent, began, now=began + 60)
    assert lanes.should_replan(recent, began, now=began + lanes.REPLAN_SECONDS + 1)
    assert not lanes.should_replan(plain, began, now=began + 10 * lanes.REPLAN_SECONDS)
