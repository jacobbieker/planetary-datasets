"""The lanes ``_backfill_box.sh noon`` pauses are ones the guard resumes."""

import pathlib
import re

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
