"""The month loop in the Himawari lane, ``_backfill_himawari.sh``."""

import os
import pathlib
import shutil
import stat
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _executable(path: pathlib.Path, body: str) -> None:
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def run_lane(tmp_path: pathlib.Path, exit_codes: list[int]) -> tuple[list[str], str]:
    """Run one walk over 2026-08..2026-07 with sat-consumer exiting with ``exit_codes`` in turn.

    Returns the month each sat-consumer call was for, and the lane's log.
    """
    lane = tmp_path / "repo"
    (lane / "scripts").mkdir(parents=True)
    shutil.copy(ROOT / "_backfill_himawari.sh", lane)
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
        "SATCONS_KILLED_PAUSE": "1",
    }
    log = subprocess.run(
        ["bash", str(lane / "_backfill_himawari.sh"), "2000", "202608", "202607"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout
    months = [line[:7] for line in calls.read_text().split()]
    return months, log


def test_the_exit_code_logged_is_sat_consumers(tmp_path):
    """A failing month is logged with its own exit code, not that of a later command."""
    months, log = run_lane(tmp_path, [1, 0])
    assert months == ["2026-08", "2026-07"]
    assert "himawari 2000m 202608 exited 1" in log
    assert "himawari 2000m 202607 exited 0" in log


def test_a_killed_month_is_run_again_before_moving_on(tmp_path):
    """A month killed by a signal runs again before the walk moves on."""
    months, log = run_lane(tmp_path, [137, 0, 0])
    assert months == ["2026-08", "2026-08", "2026-07"]
    assert "202608 exited 137" in log
    assert "202608 killed; retry 1/2" in log


def test_a_month_that_keeps_dying_moves_on(tmp_path):
    """After its retries, a month that keeps dying is left for the next walk."""
    months, log = run_lane(tmp_path, [137, 137, 137, 0])
    assert months == ["2026-08"] * 3 + ["2026-07"]
    assert "retry 3/2" not in log
