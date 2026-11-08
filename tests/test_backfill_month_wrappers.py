"""The month loops in the box's satellite-consumer wrappers.

``_backfill_satcons.sh`` and ``_backfill_himawari.sh`` walk months newest to oldest, run
sat-consumer for each, log its exit code and run a month killed by a signal again.
"""

import os
import pathlib
import shutil
import stat
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

#: Each wrapper, its arguments for a walk over 2026-08..2026-07, and its log's month label.
WRAPPERS = {
    "satcons": (
        "_backfill_satcons.sh",
        ["sat", "1000", "store", "202607", "202608"],
        "store",
    ),
    "himawari": ("_backfill_himawari.sh", ["2000", "202608", "202607"], "himawari 2000m"),
}


def _executable(path: pathlib.Path, body: str) -> None:
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@pytest.fixture(params=sorted(WRAPPERS))
def run_lane(request, tmp_path):
    """Run one walk of a wrapper, sat-consumer exiting with the given codes in turn.

    Returns the month each sat-consumer call was for, the lane's log, and the label its
    log lines give a month.
    """
    script, args, label = WRAPPERS[request.param]

    def run(exit_codes: list[int]) -> tuple[list[str], str, str]:
        lane = tmp_path / "repo"
        (lane / "scripts").mkdir(parents=True)
        shutil.copy(ROOT / script, lane)
        (lane / ".env").write_text("")
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (tmp_path / "codes").write_text("\n".join(map(str, exit_codes)) + "\n")
        calls = tmp_path / "calls"
        # sat-consumer runs pop the next exit code; the store-names helper always succeeds.
        _executable(
            bin_dir / "pixi",
            f"""[[ $* == *sat-consumer* ]] || exit 0
echo "$SATCONS_START_TIMESTAMP" >> {calls}
code=$(head -1 {tmp_path}/codes); sed -i 1d {tmp_path}/codes
exit "${{code:-0}}"
""",
        )
        # The end-of-walk hour-long sleep ends the test; the retry pause returns at once.
        _executable(bin_dir / "sleep", '[[ $1 == 3600 ]] && kill -TERM "$PPID"\nexit 0\n')
        env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "PIXI": str(bin_dir / "pixi"),
            "SATCONS_KILLED_RETRIES": "2",
            "SATCONS_KILLED_PAUSE": "1",
        }
        log = subprocess.run(
            ["bash", str(lane / script), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        ).stdout
        months = [line[:7] for line in calls.read_text().split()]
        return months, log, label

    return run


def test_the_exit_code_logged_is_sat_consumers(run_lane):
    """Each month is logged with sat-consumer's own exit code."""
    months, log, label = run_lane([1, 0])
    assert months == ["2026-08", "2026-07"]
    assert f"{label} 202608 exited 1" in log
    assert f"{label} 202607 exited 0" in log


def test_a_killed_month_is_run_again_before_moving_on(run_lane):
    """A month killed by a signal runs again before the walk moves on."""
    months, log, label = run_lane([137, 0, 0])
    assert months == ["2026-08", "2026-08", "2026-07"]
    assert f"{label} 202608 exited 137" in log
    assert f"{label} 202608 killed; retry 1/2" in log


def test_a_month_that_keeps_dying_moves_on(run_lane):
    """After its retries, a month that keeps dying is left for the next walk."""
    months, log, _ = run_lane([137, 137, 137, 0])
    assert months == ["2026-08"] * 3 + ["2026-07"]
    assert "retry 3/2" not in log
