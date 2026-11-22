"""``_backfill_box.sh`` runs a host's share of the lanes by its role."""

import os
import pathlib
import re
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "_backfill_box.sh").read_text()


def _names(array: str) -> set[str]:
    """The keys of one of the script's ``declare -A`` arrays."""
    body = re.search(rf"^declare -A {array}=\((.*?)^\)", SCRIPT, re.M | re.S)
    assert body, array
    return set(re.findall(r"^\s*\[([a-z0-9_]+)\]=", body.group(1), re.M))


def _live() -> set[str]:
    match = re.search(r'^LIVE_LANES="([^"]*)"', SCRIPT, re.M)
    assert match
    return set(match.group(1).split())


def status(tmp_path: pathlib.Path, role: str | None) -> subprocess.CompletedProcess:
    """Run ``status`` with ``HOME`` at ``tmp_path`` and the given role file."""
    if role is not None:
        config = tmp_path / ".config" / "planetary-box"
        config.mkdir(parents=True, exist_ok=True)
        (config / "role").write_text(role + "\n")
    env = {k: v for k, v in os.environ.items() if k != "BOX_ROLE"}
    return subprocess.run(
        ["bash", str(ROOT / "_backfill_box.sh"), "status"],
        env={**env, "HOME": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=60,
    )


def _listed(proc: subprocess.CompletedProcess) -> set[str]:
    return {line.split()[0] for line in proc.stdout.splitlines() if line.strip()}


def test_every_live_lane_is_a_lane():
    """A typo in LIVE_LANES would leave that store on neither host."""
    assert _live() <= _names("LANES") | _names("SCRIPTS")
    assert {"wis2_archive", "kenda", "icon_v1_global", "icon_d2_ruc_mirror"} <= _live()
    # MARS requests queue at ECMWF for hours; a spot interruption would abandon them.
    assert "mars_pipeline" in _live()


@pytest.mark.parametrize("role", [None, "all"])
def test_without_a_role_every_lane_runs(tmp_path, role):
    """With no role file (or "all") the one box runs every lane, as before."""
    proc = status(tmp_path, role)
    assert proc.returncode == 0, proc.stderr
    assert _listed(proc) == _names("LANES") | _names("SCRIPTS")


def test_live_and_bulk_split_the_lanes_between_them(tmp_path):
    """Each lane runs on exactly one of the two hosts."""
    live = _listed(status(tmp_path / "live", "live"))
    bulk = _listed(status(tmp_path / "bulk", "bulk"))
    assert live == _live()
    assert not live & bulk
    assert live | bulk == _names("LANES") | _names("SCRIPTS")


def test_an_unknown_role_runs_nothing(tmp_path):
    """A mistyped role must not fall back to running every lane."""
    proc = status(tmp_path, "both")
    assert proc.returncode == 2
    assert "unknown box role" in proc.stderr
    assert not proc.stdout


def test_the_bulk_box_does_not_run_the_noon_catch_up(tmp_path):
    """The noon stores have one writer, the live box."""
    config = tmp_path / ".config" / "planetary-box"
    config.mkdir(parents=True)
    (config / "role").write_text("bulk\n")
    proc = subprocess.run(
        ["bash", str(ROOT / "_backfill_box.sh"), "noon"],
        env={**os.environ, "HOME": str(tmp_path), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0
    assert "live box" in proc.stderr
    assert not (tmp_path / "backfill-logs" / "noon_meps_det.log").exists()
