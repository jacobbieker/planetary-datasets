"""``_backfill_backup.sh``: switching lanes and noon stores on and off on a backup host."""

import os
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def run(home: pathlib.Path, script: str, *args: str, **env: str) -> subprocess.CompletedProcess:
    """Run one of the box scripts with ``HOME`` at ``home``."""
    base = {k: v for k, v in os.environ.items() if k != "BOX_ROLE"}
    return subprocess.run(
        ["bash", str(ROOT / script), *args],
        env={**base, "HOME": str(home), **env},
        capture_output=True,
        text=True,
        timeout=60,
    )


def backup(home: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    """Run ``_backfill_backup.sh``."""
    return run(home, "_backfill_backup.sh", *args)


def switched_on(proc: subprocess.CompletedProcess) -> set[str]:
    """The names ``list`` marks as switched on."""
    return {line.split("] ")[1] for line in proc.stdout.splitlines() if line.startswith("  [x]")}


def test_list_shows_every_lane_and_noon_store_off(tmp_path):
    """A fresh host lists everything, all off."""
    proc = backup(tmp_path, "list")
    assert proc.returncode == 0, proc.stderr
    assert "backup is off" in proc.stdout
    assert "  [ ] kenda" in proc.stdout
    assert "  [ ] noon_meps_det" in proc.stdout
    assert not switched_on(proc)


def test_enable_and_disable_switch_names(tmp_path):
    """Names switch on and off, and the host takes the backup role."""
    assert backup(tmp_path, "enable", "kenda", "noon_meps_det", "uk_radar").returncode == 0
    assert backup(tmp_path, "disable", "uk_radar").returncode == 0
    assert switched_on(backup(tmp_path, "list")) == {"kenda", "noon_meps_det"}
    assert (tmp_path / ".config/planetary-box/role").read_text().strip() == "backup"


def test_an_unknown_name_switches_nothing(tmp_path):
    """A typo switches nothing, not even the valid names beside it."""
    proc = backup(tmp_path, "enable", "kenda", "kendaa")
    assert proc.returncode == 2
    assert "kendaa" in proc.stderr
    assert not switched_on(backup(tmp_path, "list"))


def test_defaults_switch_on_the_hosts_set(tmp_path):
    """Each host has a suggested set; an unknown host is refused."""
    proc = backup(tmp_path, "defaults", "aero")
    assert proc.returncode == 0, proc.stderr
    assert switched_on(proc) == {"wis2_archive", "uk_marine", "uk_radar", "nordic_radar"}
    assert backup(tmp_path, "defaults", "nowhere").returncode == 2


def test_the_box_script_runs_only_the_switched_on_lanes(tmp_path):
    """The box script, with role backup, knows only the switched-on lanes."""
    backup(tmp_path, "enable", "kenda", "uk_radar", "noon_meps_det")
    proc = run(tmp_path, "_backfill_box.sh", "status")
    assert proc.returncode == 0, proc.stderr
    assert {line.split()[0] for line in proc.stdout.splitlines()} == {"kenda", "uk_radar"}


def test_on_needs_the_boxes_down(tmp_path):
    """Turning the backup on needs the boxes confirmed down."""
    proc = backup(tmp_path, "on")
    assert proc.returncode == 2
    assert "--boxes-down" in proc.stderr
    assert not (tmp_path / ".config/planetary-box/backup-on").exists()


def test_nothing_starts_while_the_backup_is_off(tmp_path):
    """Switched-on lanes stay stopped while the backup is off."""
    backup(tmp_path, "enable", "kenda")
    proc = run(tmp_path, "_backfill_box.sh", "start")
    assert "backup is off" in proc.stderr
    assert not (tmp_path / "backfill-logs/kenda.pid").exists()


def test_the_backup_will_not_take_over_a_box(tmp_path):
    """A box (role live/bulk/all) never becomes a backup host."""
    config = tmp_path / ".config/planetary-box"
    config.mkdir(parents=True)
    (config / "role").write_text("live\n")
    proc = backup(tmp_path, "enable", "kenda")
    assert proc.returncode == 2
    assert "role 'live'" in proc.stderr


def test_noon_catches_up_only_the_switched_on_stores_one_at_a_time(tmp_path):
    """A stand-in pixi records each store's catch-up instead of running it."""
    calls = tmp_path / "calls"
    pixi = tmp_path / "pixi"
    pixi.write_text(f'#!/bin/bash\necho "${{@: -1}}" >> {calls}\nsleep 0.2\n')
    pixi.chmod(0o755)
    backup(tmp_path, "enable", "noon_meps_det", "noon_silam_dust")
    config = tmp_path / ".config/planetary-box"
    (config / "backup-on").touch()
    (config / "host.env").write_text(f"PIXI={pixi}\nNOON_PARALLEL=1\n")
    proc = run(tmp_path, "_backfill_box.sh", "noon")
    assert proc.returncode == 0, proc.stderr
    assert sorted(calls.read_text().split()) == ["=meps_det", "=silam_dust"]
