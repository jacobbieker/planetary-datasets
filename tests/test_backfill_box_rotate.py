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


def test_a_lane_started_by_rotate_does_not_hold_its_lock(box):
    """After rotate starts a lane, the next rotate can still take the lock."""
    run(box, "start")
    run(box, "rotate")
    (current,) = set(ROTATE) - waiting(box)
    since = box / "backfill-logs/.rotate" / f"{current}.since"
    since.write_text(f"{int(time.time()) - 7200}\n")
    run(box, "rotate")
    run(box, "rotate")
    lock = box / "backfill-logs/.rotate/.lock"
    assert subprocess.run(["flock", "-n", str(lock), "true"]).returncode == 0


def allow_extra_turns(home: pathlib.Path, max_slots: int) -> None:
    """Let rotate add turns up to ``max_slots`` whatever memory is available."""
    env = home / ".config/planetary-box/host.env"
    env.write_text(env.read_text() + f"ROTATE_MAX_SLOTS={max_slots}\nROTATE_EXTRA_AVAILABLE_GB=0\n")


def test_extra_turns_start_one_at_a_time_while_memory_is_spare(box):
    """With memory to spare, each rotate adds one turn, up to ROTATE_MAX_SLOTS."""
    run(box, "start")
    run(box, "rotate")
    allow_extra_turns(box, 2)
    proc = run(box, "rotate")
    assert proc.returncode == 0, proc.stderr
    assert "2 turns" in proc.stdout
    assert len(waiting(box)) == 1
    run(box, "rotate")
    assert len(waiting(box)) == 1


def test_no_extra_turn_while_guard_holds_a_lane_back(box):
    """A lane paused by guard means memory is short: no extra turn starts."""
    run(box, "start")
    run(box, "rotate")
    allow_extra_turns(box, 2)
    (box / "backfill-logs/uk_radar.paused").write_text("2026-11-24 only 38 GB available\n")
    run(box, "rotate")
    assert len(waiting(box)) == 2


def test_no_extra_turn_soon_after_memory_pressure(box):
    """Within GUARD_QUIET_MINUTES of guard's last memory pressure, no extra turn starts."""
    run(box, "start")
    run(box, "rotate")
    allow_extra_turns(box, 2)
    guard = box / "backfill-logs/.guard"
    guard.mkdir(parents=True, exist_ok=True)
    (guard / "last_pressure").write_text(f"{int(time.time()) - 60}\n")
    run(box, "rotate")
    assert len(waiting(box)) == 2


def test_guard_never_resumes_a_lane_waiting_its_turn():
    """Guard skips .paused files starting rotate, as it does manual ones."""
    script = (ROOT / "_backfill_box.sh").read_text()
    assert "grep -qE '^(manual|rotate)' \"$LOGS/$name.paused\" && continue" in script


def test_rotation_is_off_without_slots(tmp_path):
    """On a box with no ROTATE_SLOTS, rotate does nothing."""
    proc = run(tmp_path, "rotate")
    assert proc.returncode == 0
    assert not (tmp_path / "backfill-logs/.rotate").exists()
