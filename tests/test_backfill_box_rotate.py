"""``_backfill_box.sh rotate``: the big backfills take turns on a small box."""

import os
import pathlib
import subprocess
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
#: Three real lane names, rotated through one slot.
ROTATE = ("metop_gome", "metop_avhrr", "geos_cf_v1")


@pytest.fixture
def box(tmp_path):
    """A home whose lanes run a stand-in pixi that only sleeps; stopped afterwards."""
    pixi = tmp_path / "pixi"
    pixi.write_text("#!/bin/bash\nexec sleep 300\n")
    pixi.chmod(0o755)
    config = tmp_path / ".config/planetary-box"
    config.mkdir(parents=True)
    (config / "host.env").write_text(
        f"PIXI={pixi}\nROTATE_SLOTS=1\nROTATE_HOURS=1\nROTATE_MIN_AVAILABLE_GB=0\n"
        f'ROTATE_LANES="{" ".join(ROTATE)}"\n'
    )
    logs = tmp_path / "backfill-logs"
    logs.mkdir()
    # Only the rotated lanes: every other lane is paused by hand.
    names = run(tmp_path, "names").stdout.split()
    for name in names:
        if name not in ROTATE and not name.startswith("noon_"):
            (logs / f"{name}.paused").write_text("manual: test\n")
    yield tmp_path
    run(tmp_path, "stop")


def run(home: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    """Run ``_backfill_box.sh`` with ``HOME`` at ``home``."""
    base = {k: v for k, v in os.environ.items() if k != "BOX_ROLE"}
    return subprocess.run(
        ["bash", str(ROOT / "_backfill_box.sh"), *args],
        env={**base, "HOME": str(home)},
        capture_output=True,
        text=True,
        timeout=60,
    )


def waiting(home: pathlib.Path) -> set[str]:
    """The rotated lanes waiting for their turn."""
    logs = home / "backfill-logs"
    return {
        name
        for name in ROTATE
        if (logs / f"{name}.paused").exists()
        and (logs / f"{name}.paused").read_text().startswith("rotate")
    }


def test_rotation_keeps_one_turn_and_hands_it_on(box):
    """Beyond the slots lanes wait; an expired turn passes to the next in line."""
    run(box, "start")
    proc = run(box, "rotate")
    assert proc.returncode == 0, proc.stderr
    assert len(waiting(box)) == 2
    (current,) = set(ROTATE) - waiting(box)
    # Its turn is over: back-date it, then rotate twice (end the turn, start the next).
    since = box / "backfill-logs/.rotate" / f"{current}.since"
    since.write_text(f"{int(time.time()) - 7200}\n")
    run(box, "rotate")
    run(box, "rotate")
    assert current in waiting(box)
    assert len(waiting(box)) == 2
    (following,) = set(ROTATE) - waiting(box)
    assert following == ROTATE[(ROTATE.index(current) + 1) % len(ROTATE)]


def test_guard_never_resumes_a_lane_waiting_its_turn():
    """Guard skips .paused files starting rotate, as it does manual ones."""
    script = (ROOT / "_backfill_box.sh").read_text()
    assert "grep -qE '^(manual|rotate)' \"$LOGS/$name.paused\" && continue" in script


def test_rotation_is_off_without_slots(tmp_path):
    """On a box with no ROTATE_SLOTS, rotate does nothing."""
    proc = run(tmp_path, "rotate")
    assert proc.returncode == 0
    assert not (tmp_path / "backfill-logs/.rotate").exists()
