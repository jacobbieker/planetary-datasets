"""Datetime and timedelta columns survive repeated appends exactly."""

from __future__ import annotations

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr

from planetary_datasets.common import multidim


def _repo(tmp_path):
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))


def _times(*text):
    return pd.to_datetime(list(text), format="ISO8601").values


def _read(repo):
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def test_later_appends_keep_sub_day_times_exactly(tmp_path):
    repo = _repo(tmp_path)
    first = _times("2026-10-01", "2026-10-02")
    later = _times("2026-10-03T12:34:56.5", "2026-10-04T00:00:01.000000001")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", first)}), "a")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", later)}), "b")
    assert np.array_equal(_read(repo)["when"].values, np.concatenate([first, later]))
    attrs = zarr.open_group(repo.readonly_session("main").store, mode="r")["when"].attrs
    assert attrs["units"] == multidim.TIME_UNITS


def test_missing_times_are_stored_as_the_declared_fill(tmp_path):
    """xarray 2025.12 cannot decode an integer time column holding NaT at all (any
    units, any fill), so this checks what is stored: exact nanoseconds and the fill."""
    repo = _repo(tmp_path)
    first = _times("2026-10-01T00:00:00.5")
    later = np.array(["2026-10-02T03:04:05.000000006", "NaT"], dtype="datetime64[ns]")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", first)}), "a")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", later)}), "b")
    array = zarr.open_group(repo.readonly_session("main").store, mode="r")["when"]
    expected = np.concatenate([first, later]).view("int64")
    assert array[:].tolist() == expected.tolist()
    assert array.attrs["_FillValue"] == multidim.NAT == expected[-1]


def test_a_store_in_whole_day_units_is_rescaled_before_finer_times_append(tmp_path):
    repo = _repo(tmp_path)
    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="a")
    old = group.create_array(
        "when",
        shape=(2,),
        dtype="int64",
        dimension_names=("obs",),
        attributes={"units": "days since 2026-10-01 00:00:00", "calendar": "proleptic_gregorian"},
    )
    old[:] = [0, 1]
    session.commit("an older store")
    later = _times("2026-10-03T06:00:00")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", later)}), "b")
    got = _read(repo)["when"].values
    assert np.array_equal(got, np.concatenate([_times("2026-10-01", "2026-10-02"), later]))


def test_whole_day_appends_to_a_day_unit_store_stay_in_its_units(tmp_path):
    repo = _repo(tmp_path)
    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="a")
    old = group.create_array(
        "when",
        shape=(1,),
        dtype="int64",
        dimension_names=("obs",),
        attributes={"units": "days since 2026-10-01", "calendar": "proleptic_gregorian"},
    )
    old[:] = [0]
    session.commit("an older store")
    multidim.append_partition(repo, xr.Dataset({"when": ("obs", _times("2026-10-05"))}), "b")
    array = zarr.open_group(repo.readonly_session("main").store, mode="r")["when"]
    assert array.attrs["units"].startswith("days since") and array[:].tolist() == [0, 4]


def test_timedeltas_append_exactly(tmp_path):
    repo = _repo(tmp_path)
    one = np.array([1, 2], dtype="timedelta64[s]").astype("timedelta64[ns]")
    two = np.array([1500], dtype="timedelta64[ms]").astype("timedelta64[ns]")
    multidim.append_partition(repo, xr.Dataset({"lag": ("obs", one)}), "a")
    multidim.append_partition(repo, xr.Dataset({"lag": ("obs", two)}), "b")
    got = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
    )["lag"].values
    assert np.array_equal(got, np.concatenate([one, two]))


def test_second_resolution_datetimes_are_not_lost_on_creation_or_append(tmp_path):
    """xarray writes datetime64[s] under nanosecond units as NaT unless widened first."""
    repo = _repo(tmp_path)
    first = np.array(["2026-10-28T07:30:00"], dtype="datetime64[s]")
    later = np.array(["2026-10-28T19:30:00"], dtype="datetime64[s]")
    multidim.append_partition(repo, xr.Dataset({"arrived": ("obs", first)}), "a")
    multidim.append_partition(repo, xr.Dataset({"arrived": ("obs", later)}), "b")
    got = _read(repo)["arrived"].values
    assert got.tolist() == np.concatenate([first, later]).astype("datetime64[ns]").tolist()
