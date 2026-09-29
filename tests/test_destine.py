"""Offline tests for the Destination Earth accessor and ERA5-Land mirror."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets import config as config_module
from planetary_datasets.config import MissingCredential
from planetary_datasets.providers import destine

TOKEN = "edh_pat_test_value_not_a_real_token"


@pytest.fixture
def config_with_pat(local_config, monkeypatch):
    monkeypatch.setenv("DESTINE_PAT", TOKEN)
    config_module.reset_config_cache()
    return config_module.get_config()


def era5_land_like(hours: int = 48, start: str = "2024-01-01") -> xr.Dataset:
    """A miniature of the Destination Earth ERA5-Land store, with its `valid_time` dim."""
    times = pd.date_range(start, periods=hours, freq="h")
    latitude = np.array([10.0, 0.0, -10.0])
    longitude = np.array([0.0, 90.0, 200.0, 350.0])
    shape = (hours, len(latitude), len(longitude))
    return xr.Dataset(
        {
            "t2m": (("valid_time", "latitude", "longitude"), np.full(shape, 280.0, dtype="float64")),
            "tp": (("valid_time", "latitude", "longitude"), np.full(shape, 1e-4, dtype="float64")),
        },
        coords={"valid_time": times, "latitude": latitude, "longitude": longitude},
    )


@pytest.fixture
def make_provider(config_with_pat, monkeypatch):
    """Build a provider whose remote zarr is replaced by ``source``: nothing touches the network."""

    def make(source: xr.Dataset | None = None, **kwargs):
        p = destine.DestinEERA5LandProvider(config=config_with_pat, **kwargs)
        ds = era5_land_like() if source is None else source
        monkeypatch.setattr(p, "source", lambda: ds)
        return p

    return make


@pytest.fixture
def provider(make_provider):
    return make_provider(variables=("t2m", "tp"))


def test_destine_url_requires_the_token(local_config):
    with pytest.raises(MissingCredential, match="DESTINE_PAT"):
        destine.destine_url(config=local_config)


def test_destine_url_embeds_the_token(config_with_pat):
    url = destine.destine_url(config=config_with_pat)
    assert url == f"https://edh:{TOKEN}@{destine.DESTINE_HOST}/{destine.ERA5_LAND_PATH}"


def test_no_token_is_hardcoded_in_the_module():
    source = pathlib.Path(destine.__file__.replace(".pyc", ".py"))
    assert "edh_pat_" not in source.read_text(encoding="utf-8")


@pytest.mark.parametrize("name", ["valid_time", "time"])
def test_time_name_detection(name):
    ds = era5_land_like().rename({"valid_time": name})
    assert destine.DestinEERA5LandProvider.time_name(ds) == name


def test_time_name_raises_when_there_is_none():
    ds = era5_land_like().rename({"valid_time": "step"})
    with pytest.raises(ValueError, match="no time dimension"):
        destine.DestinEERA5LandProvider.time_name(ds)


def test_fetch_returns_the_unauthenticated_path(provider):
    assert provider.fetch(pd.Timestamp("2024-01-01")) == [destine.ERA5_LAND_PATH]


def test_fetch_returns_empty_before_the_source_starts(provider):
    """The reanalysis will never reach back this far, so the partition is genuinely done."""
    assert provider.fetch(pd.Timestamp("1900-01-01")) == []


def test_fetch_raises_for_a_day_not_published_yet(provider):
    """Returning [] here would retire a partition that will exist tomorrow."""
    with pytest.raises(RuntimeError, match="not fully published"):
        provider.fetch(pd.Timestamp("2099-01-01"))


def test_fetch_raises_for_a_half_published_final_day(make_provider):
    """A short day would be committed, read as present, and never completed."""
    p = make_provider(era5_land_like(hours=30))
    assert p.fetch(pd.Timestamp("2024-01-01")) == [destine.ERA5_LAND_PATH]
    with pytest.raises(RuntimeError, match="not fully published"):
        p.fetch(pd.Timestamp("2024-01-02"))


def test_process_refuses_a_day_with_missing_hours(make_provider):
    gappy = era5_land_like(hours=48).isel(valid_time=list(range(0, 20)) + list(range(24, 48)))
    p = make_provider(gappy, variables=("t2m",))
    with pytest.raises(ValueError, match="20 of 24 hours"):
        p.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))


def test_unsorted_source_time_axis_is_rejected(make_provider):
    p = make_provider(era5_land_like().isel(valid_time=[3, 1, 2, 0]))
    with pytest.raises(ValueError, match="not sorted ascending"):
        p.source_times()


def test_process_returns_one_day_renamed_to_time(provider):
    day = provider.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-02"))
    assert "time" in day.dims and "valid_time" not in day.dims
    assert day.sizes["time"] == 24
    assert pd.Timestamp(day.time.values[0]) == pd.Timestamp("2024-01-02T00:00")
    assert pd.Timestamp(day.time.values[-1]) == pd.Timestamp("2024-01-02T23:00")


def test_process_casts_to_float32_and_normalises_longitude(provider):
    day = provider.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))
    assert day.t2m.dtype == np.float32
    longitudes = day.longitude.values
    assert longitudes.min() >= -180 and longitudes.max() <= 180
    assert np.all(np.diff(longitudes) > 0)
    # 1e-4 m of precipitation must survive; float16 would not hold the tail of this.
    assert np.all(day.tp.values > 0)


def test_unknown_variable_is_named_in_the_error(make_provider):
    p = make_provider(variables=("t2m", "nope"))
    with pytest.raises(KeyError, match="nope"):
        p.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))


def test_run_partition_round_trip(provider):
    assert provider.run_partition(pd.Timestamp("2024-01-01")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-02")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-02")) is False

    stored = read_store(provider)
    assert stored.sizes["time"] == 48
    assert stored.t2m.dtype == np.float32


def test_timezone_aware_partition_is_normalised(provider):
    assert provider.run_partition(pd.Timestamp("2024-01-01", tz="UTC")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-01")) is False
