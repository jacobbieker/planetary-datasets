"""``OperaCatchUp.stage``: a hung staging container is abandoned and removed."""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "one_offs"))

import silam_harmonie_catchup as catchup  # noqa: E402


class Provider:
    product = "rainfall"
    name = "opera_rainfall"
    allow_partial = True
    archive_root = pathlib.Path("/tmp")

    def has_staged(self, it):
        return False


def test_a_hung_container_is_removed_and_the_hour_left_for_next_pass(monkeypatch, tmp_path):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[:2] == ["docker", "run"]:
            raise subprocess.TimeoutExpired(command, kwargs.get("timeout"))
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(catchup.subprocess, "run", run)
    stager = catchup.OperaCatchUp(Provider(), days=0, freq="1h")
    assert stager.stage(pd.Timestamp("2024-07-01T11:00"), tmp_path) is False
    name = calls[0][calls[0].index("--name") + 1]
    assert calls[1] == ["docker", "rm", "-f", name]
