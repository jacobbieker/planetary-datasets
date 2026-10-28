"""The earth2studio staging in the box's lane runner, ``_backfill_lanes.py``."""

import importlib.util
import pathlib
import subprocess
from types import SimpleNamespace

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("_backfill_lanes", ROOT / "_backfill_lanes.py")
lanes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lanes)


class FakeProvider:
    """Hourly partitions of five-minute frames, with a fixed set of stored frames."""

    name = "goes_glm_grid_east"
    append_dim = "time"
    dataset = SimpleNamespace(memory_gb=8.0)

    def __init__(self, stored, staged=()):
        self.stored = pd.DatetimeIndex(stored)
        self.staged = set(staged)

    def get_icechunk_repo(self):
        return None

    def window(self, it):
        return it, it + pd.Timedelta(1, "h")

    def has_staged(self, it):
        return it in self.staged


def test_an_hour_with_any_stored_frame_is_not_staged_again(monkeypatch):
    hours = list(pd.date_range("2026-10-02T05:00", periods=4, freq="1h"))
    # 05:00 complete, 06:xx stored without its first frame, 07:00 and 08:00 missing.
    stored = [*pd.date_range("2026-10-02T05:00", periods=12, freq="5min"), "2026-10-02T06:05"]
    monkeypatch.setattr(
        "planetary_datasets.common.store.existing_times", lambda repo, dim: stored[::-1]
    )
    assert lanes.unstored_partitions(FakeProvider(stored), hours) == hours[2:]


def test_the_stager_runs_the_image_for_the_dataset(monkeypatch, tmp_path):
    it = pd.Timestamp("2026-10-02T07:00")
    provider = FakeProvider([])
    seen = {}

    def run(command, **kwargs):
        seen["command"] = command
        provider.staged.add(it)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    assert lanes.Earth2StudioStager(provider).stage(it, tmp_path)
    command = seen["command"]
    assert command[command.index("obs") :] == [
        "obs",
        "--dataset",
        "goes_glm_grid_east",
        "--time",
        "2026-10-02T07:00",
        "--target",
        "/data/earth2studio",
    ]
    assert f"{tmp_path}:/data/earth2studio" in command
    assert command[command.index("--memory") + 1] == "12g"


def test_a_failed_stage_is_reported_unstaged(monkeypatch, tmp_path):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(command, 1, "", "no files"),
    )
    assert not lanes.Earth2StudioStager(FakeProvider([])).stage(
        pd.Timestamp("2026-10-02T07:00"), tmp_path
    )


def test_an_already_staged_partition_skips_the_image(monkeypatch, tmp_path):
    it = pd.Timestamp("2026-10-02T07:00")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError))
    assert lanes.Earth2StudioStager(FakeProvider([], staged=[it])).stage(it, tmp_path)


def test_the_new_lanes_are_registered():
    assert lanes.LANES["goes_glm_grid_east"].earth2studio
    assert lanes.LANES["goes_glm_grid_west"].earth2studio
    assert not lanes.LANES["jpss_atms"].earth2studio
