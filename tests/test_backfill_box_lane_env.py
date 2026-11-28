"""``_backfill_box.sh`` LANE_ENV: environment set for one lane's python only."""

import os
import pathlib
import subprocess
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
#: A lane with an entry in LANE_ENV, and one without.
WITH_ENV, WITHOUT_ENV = "ncep_upper_air", "uk_radar"


def run(home: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    """Run ``_backfill_box.sh`` with ``HOME`` at ``home``."""
    base = {k: v for k, v in os.environ.items() if k not in ("BOX_ROLE", "MALLOC_ARENA_MAX")}
    return subprocess.run(
        ["bash", str(ROOT / "_backfill_box.sh"), *args],
        env={**base, "HOME": str(home)},
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture
def box(tmp_path):
    """Two lanes running a stand-in pixi that records the lane's MALLOC_ARENA_MAX."""
    seen = tmp_path / "seen"
    seen.mkdir()
    pixi = tmp_path / "pixi"
    # Called as: pixi run python _backfill_lanes.py <lane> ...
    pixi.write_text(
        f'#!/bin/bash\necho "${{MALLOC_ARENA_MAX-unset}}" > {seen}/"$4"\nexec sleep 300\n'
    )
    pixi.chmod(0o755)
    config = tmp_path / ".config/planetary-box"
    config.mkdir(parents=True)
    (config / "host.env").write_text(f"PIXI={pixi}\n")
    logs = tmp_path / "backfill-logs"
    logs.mkdir()
    for name in run(tmp_path, "names").stdout.split():
        if name not in (WITH_ENV, WITHOUT_ENV) and not name.startswith("noon_"):
            (logs / f"{name}.paused").write_text("manual: test\n")
    yield tmp_path
    run(tmp_path, "stop")


def seen_env(home: pathlib.Path, lane: str) -> str:
    """MALLOC_ARENA_MAX as the stand-in pixi of ``lane`` saw it."""
    path = home / "seen" / lane
    deadline = time.monotonic() + 10
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    return path.read_text().strip()


def test_lane_env_reaches_only_its_own_lane(box):
    """ncep_upper_air's python gets MALLOC_ARENA_MAX=2; another lane's does not."""
    proc = run(box, "start")
    assert proc.returncode == 0, proc.stderr
    assert seen_env(box, WITH_ENV) == "2"
    assert seen_env(box, WITHOUT_ENV) == "unset"


def test_lane_env_stays_out_of_the_lane_loop():
    """The variables are given to ``env`` for the child, not exported by the loop."""
    script = (ROOT / "_backfill_box.sh").read_text()
    assert "env $lane_env $PIXI run python $cmd" in script
    assert "export MALLOC_ARENA_MAX" not in script


def test_script_parses():
    """``bash -n`` is clean."""
    assert subprocess.run(["bash", "-n", str(ROOT / "_backfill_box.sh")]).returncode == 0
