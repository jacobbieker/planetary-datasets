"""Offline tests for the Destination Earth accessor and ERA5-Land mirror."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.config import MissingCredential, load_config
from planetary_datasets.providers import destine

TOKEN = "edh_pat_test_value_not_a_real_token"


@pytest.fixture
def config_with_pat(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("DESTINE_PAT", TOKEN)
    return load_config(env_file=tmp_path / "nonexistent.env")


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


def test_destine_url_requires_the_token(local_config):
    with pytest.raises(MissingCredential, match="DESTINE_PAT"):
        destine.destine_url(config=local_config)


def test_destine_url_embeds_the_token(config_with_pat):
    url = destine.destine_url(config=config_with_pat)
    assert url == f"https://edh:{TOKEN}@{destine.DESTINE_HOST}/{destine.ERA5_LAND_PATH}"


def test_no_token_is_hardcoded_in_the_module():
    source = (destine.__file__).replace(".pyc", ".py")
    with open(source, encoding="utf-8") as fh:
        text = fh.read()
    assert "edh_pat_" not in text


def test_time_name_detection():
    assert destine.DestinEERA5LandProvider.time_name(era5_land_like()) == "valid_time"
    renamed = era5_land_like().rename({"valid_time": "time"})
    assert destine.DestinEERA5LandProvider.time_name(renamed) == "time"


def test_time_name_raises_when_there_is_none():
    ds = era5_land_like().rename({"valid_time": "step"})
    with pytest.raises(ValueError, match="no time dimension"):
        destine.DestinEERA5LandProvider.time_name(ds)


@pytest.fixture
def provider(config_with_pat, monkeypatch):
    p = destine.DestinEERA5LandProvider(variables=("t2m", "tp"), config=config_with_pat)
    # Stand in for the remote zarr: nothing here may touch the network.
    monkeypatch.setattr(p, "source", lambda: era5_land_like())
    return p


def test_fetch_returns_the_unauthenticated_path(provider):
    assert provider.fetch(pd.Timestamp("2024-01-01")) == [destine.ERA5_LAND_PATH]
    assert TOKEN not in provider.fetch(pd.Timestamp("2024-01-01"))[0]


def test_fetch_returns_empty_before_the_source_starts(provider):
    """The reanalysis will never reach back this far, so the partition is genuinely done."""
    assert provider.fetch(pd.Timestamp("1900-01-01")) == []


def test_fetch_raises_for_a_day_not_published_yet(provider):
    """Returning [] here would retire a partition that will exist tomorrow."""
    with pytest.raises(RuntimeError, match="not fully published"):
        provider.fetch(pd.Timestamp("2099-01-01"))


def test_fetch_raises_for_a_half_published_final_day(config_with_pat, monkeypatch):
    """A short day would be committed, read as present, and never completed."""
    p = destine.DestinEERA5LandProvider(config=config_with_pat)
    monkeypatch.setattr(p, "source", lambda: era5_land_like(hours=30))
    assert p.fetch(pd.Timestamp("2024-01-01")) == [destine.ERA5_LAND_PATH]
    with pytest.raises(RuntimeError, match="not fully published"):
        p.fetch(pd.Timestamp("2024-01-02"))


def test_process_refuses_a_day_with_missing_hours(config_with_pat, monkeypatch):
    p = destine.DestinEERA5LandProvider(variables=("t2m",), config=config_with_pat)
    gappy = era5_land_like(hours=48).isel(valid_time=list(range(0, 20)) + list(range(24, 48)))
    monkeypatch.setattr(p, "source", lambda: gappy)
    with pytest.raises(ValueError, match="20 of 24 hours"):
        p.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))


def test_unsorted_source_time_axis_is_rejected(config_with_pat, monkeypatch):
    p = destine.DestinEERA5LandProvider(config=config_with_pat)
    shuffled = era5_land_like().isel(valid_time=[3, 1, 2, 0])
    monkeypatch.setattr(p, "source", lambda: shuffled)
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


def test_process_keeps_small_precipitation_values(provider):
    """1e-4 m of precipitation must survive; float16 would not hold the tail of this."""
    day = provider.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))
    assert np.all(day.tp.values > 0)


def test_unknown_variable_is_named_in_the_error(config_with_pat, monkeypatch):
    p = destine.DestinEERA5LandProvider(variables=("t2m", "nope"), config=config_with_pat)
    monkeypatch.setattr(p, "source", lambda: era5_land_like())
    with pytest.raises(KeyError, match="nope"):
        p.process([destine.ERA5_LAND_PATH], pd.Timestamp("2024-01-01"))


def test_run_partition_round_trip(provider):
    assert provider.run_partition(pd.Timestamp("2024-01-01")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-02")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-02")) is False

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert stored.sizes["time"] == 48
    assert stored.t2m.dtype == np.float32


def test_timezone_aware_partition_is_normalised(provider):
    assert provider.run_partition(pd.Timestamp("2024-01-01", tz="UTC")) is True
    assert provider.run_partition(pd.Timestamp("2024-01-01")) is False
