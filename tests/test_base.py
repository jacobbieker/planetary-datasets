"""The BaseProvider lifecycle, exercised with a fake provider against a local store."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.base import BaseProvider


class FakeProvider(BaseProvider):
    """A provider that invents its data instead of downloading it."""

    name = "fake"
    append_dim = "time"
    store_prefix = "test/fake.icechunk"

    def __init__(self, config=None, inputs=("one.grib",)):
        super().__init__(config=config)
        self.inputs = list(inputs)
        self.fetch_calls: list[pd.Timestamp] = []
        self.process_calls: list[pd.Timestamp] = []

    def fetch(self, it, temp_dir=None, **kwargs):
        self.fetch_calls.append(it)
        return list(self.inputs)

    def process(self, input_files, it, temp_dir=None, **kwargs):
        self.process_calls.append(it)
        return xr.Dataset(
            {"temperature": (("time", "x"), np.zeros((1, 3), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([it]), "x": np.arange(3)},
        )


@pytest.fixture
def provider(local_config):
    return FakeProvider(config=local_config)


def test_store_path_resolves_through_config(provider, tmp_path):
    assert provider.store_path.startswith(str(tmp_path))
    assert "s3://" not in provider.store_path


def test_run_partition_writes_data(provider):
    assert provider.run_partition(pd.Timestamp("2026-01-01T00:00")) is True
    assert provider.process_calls == [pd.Timestamp("2026-01-01T00:00")]


def test_rerunning_the_same_partition_skips_before_fetching(provider):
    stamp = pd.Timestamp("2026-01-01T00:00")
    provider.run_partition(stamp)
    assert provider.run_partition(stamp) is False
    # The second call must not re-download.
    assert provider.fetch_calls == [stamp]


def test_no_inputs_is_not_a_failure(local_config):
    empty = FakeProvider(config=local_config, inputs=())
    assert empty.run_partition(pd.Timestamp("2026-01-01T00:00")) is False
    assert empty.process_calls == []


def test_missing_timesteps_reflects_the_store(provider):
    wanted = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00"])
    assert len(provider.missing_timesteps(wanted)) == 2
    provider.run_partition(wanted[0])
    assert provider.missing_timesteps(wanted) == [pd.Timestamp("2026-01-01T01:00")]


def test_run_range_writes_every_missing_partition(provider):
    stamps = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"])
    assert provider.run_range(stamps) == 3
    assert provider.missing_timesteps(stamps) == []


def test_run_range_continues_past_a_failing_partition(local_config):
    class Flaky(FakeProvider):
        def process(self, input_files, it, temp_dir=None, **kwargs):
            if it == pd.Timestamp("2026-01-01T01:00"):
                raise RuntimeError("bad partition")
            return super().process(input_files, it, temp_dir=temp_dir, **kwargs)

    flaky = Flaky(config=local_config)
    flaky.store_prefix = "test/flaky.icechunk"
    stamps = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"])
    assert flaky.run_range(stamps) == 2


def test_local_tempdir_exists_inside_the_block_and_is_cleaned_up():
    with BaseProvider.local_tempdir() as td:
        assert isinstance(td, pathlib.Path)
        # Regression: a bare TemporaryDirectory() whose handle goes out of scope is
        # finalised immediately, deleting the directory before the caller can use it.
        assert td.is_dir()
        (td / "scratch.txt").write_text("still here")
        assert (td / "scratch.txt").read_text() == "still here"
        saved = td
    assert not saved.exists()


def test_fetch_receives_a_usable_temp_dir(local_config):
    seen = {}

    class Checking(FakeProvider):
        def fetch(self, it, temp_dir=None, **kwargs):
            seen["dir"] = temp_dir
            seen["exists"] = temp_dir is not None and temp_dir.is_dir()
            return ["one.grib"]

    provider = Checking(config=local_config)
    provider.store_prefix = "test/tempdir.icechunk"
    provider.run_partition(pd.Timestamp("2026-01-01T00:00"))
    assert seen["exists"] is True


def test_memory_guard_can_be_disabled(local_config, monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "0.000001")
    from planetary_datasets import config as config_module

    config_module.reset_config_cache()

    unguarded = FakeProvider(config=local_config)
    unguarded.store_prefix = "test/unguarded.icechunk"
    unguarded.guard_memory = False
    assert unguarded.run_partition(pd.Timestamp("2026-01-01T00:00")) is True


def test_repo_handle_is_reused_across_partitions(provider, monkeypatch):
    """Regression: run_range reopened the store for every partition."""
    from planetary_datasets.config import Config

    opens = []
    original = Config.icechunk_repo

    def counting(self, prefix):
        opens.append(prefix)
        return original(self, prefix)

    monkeypatch.setattr(Config, "icechunk_repo", counting)
    provider.run_range(pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00", "2026-01-01T02:00"]))
    assert len(opens) == 1
    assert provider.get_icechunk_repo() is provider.get_icechunk_repo()


def test_run_range_does_not_recheck_each_partition(provider):
    """run_range filters once; run_partition should not re-query per timestep."""
    calls = []
    original = provider.missing_timesteps

    def counting(desired):
        calls.append(list(desired))
        return original(desired)

    provider.missing_timesteps = counting
    provider.run_range(pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00"]))
    assert len(calls) == 1


def test_abstract_methods_must_be_implemented():
    with pytest.raises(TypeError):
        BaseProvider()
