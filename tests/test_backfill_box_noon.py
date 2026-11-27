"""The lanes ``_backfill_box.sh noon`` pauses are ones the guard resumes."""

import os
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _box_list(name: str) -> list[str]:
    """The lane names in a ``NAME="..."`` or ``NAME=${NAME-"..."}`` assignment of the script."""
    script = (ROOT / "_backfill_box.sh").read_text()
    match = re.search(rf'^{name}=(?:\$\{{{name}-)?"([^"]*)"', script, re.M)
    assert match, name
    return match.group(1).split()


def test_every_lane_paused_for_noon_is_resumed_by_the_guard():
    """A lane outside GUARD_PAUSE_ORDER would stay paused for good after its first noon."""
    make_room = _box_list("NOON_MAKE_ROOM")
    assert "mtg_1000m" in make_room
    assert "himawari_500m_3" in make_room
    assert set(make_room) <= set(_box_list("GUARD_PAUSE_ORDER"))


def test_the_guard_resumes_noon_lanes_even_with_an_overridden_pause_order():
    """GUARD_PAUSE_ORDER can be overridden from the environment; noon's lanes still resume."""
    script = (ROOT / "_backfill_box.sh").read_text()
    resume = re.search(
        r"^  for name in (.*); do\n    \[\[ -f \$LOGS/\$name\.paused \]\] \|\| continue",
        script,
        re.M,
    )
    assert resume, "the guard's resume loop"
    assert "$GUARD_PAUSE_ORDER" in resume.group(1)
    assert "$NOON_MAKE_ROOM" in resume.group(1)


def test_a_relaunched_catch_up_waits_for_the_running_one(tmp_path):
    """Two noon loops at once would run twice NOON_PARALLEL stores, as on 2026-11-26."""
    running = tmp_path / "running"
    running.mkdir()
    peak = tmp_path / "peak"
    pixi = tmp_path / "pixi"
    pixi.write_text(
        "#!/bin/bash\n"
        f'touch "{running}/$$"\n'
        f'n=$(ls "{running}" | wc -l); echo "$n" >> "{peak}"\n'
        "sleep 0.3\n"
        f'rm "{running}/$$"\n'
    )
    pixi.chmod(0o755)
    config = tmp_path / ".config/planetary-box"
    config.mkdir(parents=True)
    (config / "role").write_text("live\n")
    (config / "host.env").write_text(f"PIXI={pixi}\nNOON_PARALLEL=2\n")
    env = {k: v for k, v in os.environ.items() if k != "BOX_ROLE"}
    loops = [
        subprocess.Popen(
            ["bash", str(ROOT / "_backfill_box.sh"), "noon"],
            env={**env, "HOME": str(tmp_path)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(2)
    ]
    for loop in loops:
        assert loop.wait(timeout=60) == 0
    counts = [int(n) for n in peak.read_text().split()]
    # Both loops ran every store, one loop after the other.
    assert len(counts) == 2 * len(_box_list("NOON_STORES"))
    assert max(counts) <= 2
