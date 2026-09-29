"""Tests for the shared staged-pipeline pieces: the store helpers and ``dags.staged``."""

from __future__ import annotations

import pathlib
import sys

import dagster as dg
import icechunk
import numpy as np
import pandas as pd
import pytest
import xarray as xr

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dags import staged  # noqa: E402
from planetary_datasets.base import BaseProvider  # noqa: E402
from planetary_datasets.common import store  # noqa: E402


def frame(when: str, static: float) -> xr.Dataset:
    return xr.Dataset(
        {"v": (("time", "y"), np.ones((1, 3), dtype="float32"))},
        coords={
            "time": [pd.Timestamp(when)],
            "y": np.arange(3),
            "footprint": (("y",), np.full(3, static, dtype="float32")),
        },
    )


@pytest.fixture
def repo(tmp_path):
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path / "s")))


def test_an_append_leaves_static_variables_as_first_written(repo):
    assert store.write_to_icechunk(repo, frame("2020-01-01", 1.0))
    assert store.write_to_icechunk(repo, frame("2020-01-02", 99.0))

    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes["time"] == 2
    assert ds["footprint"].values.tolist() == [1.0, 1.0, 1.0]


def test_latest_time_and_committed_data(repo):
    assert store.latest_time(repo) is None
    assert not store.has_committed_data(repo)
    for day in ("2020-01-01", "2020-01-03", "2020-01-02"):
        store.write_to_icechunk(repo, frame(day, 1.0))
    assert store.has_committed_data(repo)
    # The out-of-order day is dropped, so the store still ends on the 3rd.
    assert store.latest_time(repo) == pd.Timestamp("2020-01-03")


class Tiny(BaseProvider):
    name = "tiny"
    store_prefix = "test/tiny.icechunk"

    def fetch(self, it, temp_dir=None, **kwargs):
        return ["x"]

    def process(self, input_files, it, temp_dir=None, **kwargs):
        return frame(str(it), 1.0)


def test_a_provider_is_appendable_only_after_its_last_stored_step(local_config):
    provider = Tiny(config=local_config)
    first = pd.Timestamp("2020-01-02")
    assert provider.appendable(first), "an empty store accepts anything"
    assert provider.run_partition(first)
    assert not provider.appendable(first)
    assert not provider.appendable(pd.Timestamp("2020-01-01"))
    assert provider.appendable(pd.Timestamp("2020-01-03", tz="UTC")), "tz-aware is normalised"


class NothingStaged:
    store_path = "memory://"
    archive_root = pathlib.Path("/nonexistent")

    def appendable(self, start):
        return True

    def partition_stored(self, start):
        return False

    def has_staged(self, start):
        return False

    def run_partition(self, start, check_present=True):
        raise AssertionError("must not publish when nothing is staged")

    def discard_staged(self, start, settled=False):
        return []


def test_publishing_fails_when_the_store_wants_the_partition_but_nothing_is_staged():
    partitions = dg.DailyPartitionsDefinition(start_date="2020-01-01")
    download = dg.AssetSpec("thing_download", partitions_def=partitions)
    publish = staged.make_staged_publish_asset(
        name="thing",
        description="test",
        partitions_def=partitions,
        download=download,
        publisher=NothingStaged,
        memory_gb=1,
    )
    result = dg.materialize([publish], partition_key="2020-01-02", raise_on_error=False)
    assert not result.success


def test_memory_tags_match_the_provider_factory():
    from dags.factory import MEMORY_CLASS_TAG, MEMORY_GB_TAG

    assert staged.memory_tags(6) == {MEMORY_CLASS_TAG: "medium", MEMORY_GB_TAG: "6"}
