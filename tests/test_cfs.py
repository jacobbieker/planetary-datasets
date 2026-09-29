"""Offline tests for the NOAA CFS local-NetCDF ingest."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import cfs


def cfs_like_dataset(init: str, steps: int) -> xr.Dataset:
    """A miniature CFS forecast: one initialisation, a handful of six-hourly steps."""
    dims = ("time", "step", "latitude", "longitude")
    return xr.Dataset(
        {
            "tmp2m": (dims, np.arange(steps * 6, dtype="float32").reshape(1, steps, 2, 3)),
            # An integer field, because the longest run creates the store and must not
            # leave it with integer arrays that cannot hold the NaN padding.
            "cover": (dims, np.ones((1, steps, 2, 3), dtype="int16")),
        },
        coords={
            "time": pd.DatetimeIndex([init]),
            "step": pd.to_timedelta(np.arange(steps) * 6, unit="h"),
            "latitude": [-1.0, 1.0],
            "longitude": [0.0, 1.0, 2.0],
        },
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("flxf2011010100.01.2011010100.nc", "2011-01-01T00:00"),
        ("cfs_20240315.nc", "2024-03-15T00:00"),
        ("cfs_2024031518.nc", "2024-03-15T18:00"),
        ("no-timestamp-here.nc", None),
        ("cfs_20241332.nc", None),
    ],
)
def test_timestamp_from_name(name, expected):
    got = cfs.timestamp_from_name(name)
    assert got is None if expected is None else got == pd.Timestamp(expected)


def test_pad_to_steps_extends_with_nan():
    ds = cfs_like_dataset("2011-01-01", steps=3)
    padded = cfs.pad_to_steps(ds, 5)
    assert padded.sizes["step"] == 5
    assert np.isnan(padded.tmp2m.values[0, 3:]).all()
    # The new step coordinate continues the existing spacing.
    assert padded.step.values[-1] == np.timedelta64(24, "h")
    # The real data is untouched.
    assert np.array_equal(padded.tmp2m.values[0, :3], ds.tmp2m.values[0])


def test_pad_to_steps_leaves_the_data_alone_at_the_target_length():
    ds = cfs_like_dataset("2011-01-01", steps=4)
    same = cfs.pad_to_steps(ds, 4)
    assert same.sizes["step"] == 4
    assert np.array_equal(same.tmp2m.values, ds.tmp2m.values)


def test_integer_variables_are_promoted_whether_or_not_padding_happens():
    """The longest run creates the store, so it must not be left integer."""
    ds = cfs_like_dataset("2011-01-01", steps=3)
    assert ds["cover"].dtype == np.int16
    assert cfs.pad_to_steps(ds, 3)["cover"].dtype == np.float32
    padded = cfs.pad_to_steps(ds, 5)
    assert padded["cover"].dtype == np.float32
    assert np.isnan(padded["cover"].values[0, 3:]).all()


@pytest.mark.parametrize(
    ("ds", "match"),
    [
        (cfs_like_dataset("2011-01-01", steps=6), "more than the 4"),
        (cfs_like_dataset("2011-01-01", steps=3).isel(step=slice(0, 0)), "empty 'step' dimension"),
        (cfs_like_dataset("2011-01-01", steps=2).isel(step=0, drop=True), "no 'step' dimension"),
    ],
    ids=["would-truncate", "empty-step", "no-step"],
)
def test_pad_to_steps_rejects_what_it_cannot_pad(ds, match):
    with pytest.raises(ValueError, match=match):
        cfs.pad_to_steps(ds, 4)


@pytest.fixture
def source_dir(tmp_path):
    """Two initialisations on disk, a short run of 2 steps and a long one of 5."""
    directory = tmp_path / "cfs"
    directory.mkdir()
    for init, steps in (("2011-01-01", 2), ("2011-01-02", 5)):
        path = directory / f"cfs_{pd.Timestamp(init):%Y%m%d}00.nc"
        cfs_like_dataset(init, steps).to_netcdf(path)
    return directory


@pytest.fixture
def provider(source_dir, local_config):
    return cfs.CFSSeasonalProvider(source_dir=source_dir, config=local_config)


def test_max_step_count(source_dir):
    assert cfs.max_step_count(sorted(source_dir.glob("*.nc"))) == 5


def test_discover_indexes_by_initialisation_and_ignores_unstamped_files(source_dir, provider):
    (source_dir / "readme.nc").write_bytes(b"")
    assert sorted(provider.discover()) == [pd.Timestamp("2011-01-01"), pd.Timestamp("2011-01-02")]


def test_missing_source_dir_is_an_error_not_an_empty_archive(tmp_path, local_config):
    provider = cfs.CFSSeasonalProvider(source_dir=tmp_path / "absent", config=local_config)
    with pytest.raises(FileNotFoundError, match="does not exist"):
        provider.discover()


def test_fetch_returns_empty_for_an_unknown_initialisation(provider):
    assert provider.fetch(pd.Timestamp("1999-01-01")) == []


def test_max_steps_is_measured_when_not_given(provider):
    assert provider.max_steps == 5


def test_run_all_pads_and_appends_every_initialisation(provider):
    assert provider.run_all() == 2
    # A second pass finds nothing left to do.
    assert provider.run_all() == 0

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store,
        consolidated=False,
        decode_timedelta=True,
    )
    assert list(pd.DatetimeIndex(stored.time.values)) == [
        pd.Timestamp("2011-01-01"),
        pd.Timestamp("2011-01-02"),
    ]
    assert stored.sizes["step"] == 5
    # The short run was padded, the long one was not.
    assert np.isnan(stored.tmp2m.isel(time=0).values[2:]).all()
    assert not np.isnan(stored.tmp2m.isel(time=1).values).any()
    # The long run created the store; had it stayed int16 the padded NaNs appended after
    # it would have been written into an integer array as garbage.
    assert stored.cover.dtype == np.float32
    assert np.isnan(stored.cover.isel(time=0).values[2:]).all()


def test_process_rejects_a_file_whose_time_disagrees_with_its_name(tmp_path, local_config):
    directory = tmp_path / "cfs"
    directory.mkdir()
    path = directory / "cfs_2011010100.nc"
    cfs_like_dataset("2011-06-01", steps=2).to_netcdf(path)
    provider = cfs.CFSSeasonalProvider(source_dir=directory, max_steps=2, config=local_config)
    with pytest.raises(ValueError, match="filename and the file disagree"):
        provider.process([str(path)], pd.Timestamp("2011-01-01"))


def test_timezone_aware_partition_is_normalised(provider):
    assert provider.run_partition(pd.Timestamp("2011-01-01", tz="UTC")) is True
    assert provider.run_partition(pd.Timestamp("2011-01-01")) is False
