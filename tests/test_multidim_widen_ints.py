"""Integer columns narrowed by an early partition widen when a later one needs it."""

from __future__ import annotations

import icechunk
import numpy as np
import xarray as xr

from planetary_datasets.common import multidim


def test_an_int8_column_widens_to_hold_later_int16_values(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    # The WIS2 satellite workers narrow integer columns before writing.
    multidim.append_partition(repo, xr.Dataset({"flags": ("obs", np.array([1, 2], "int8"))}), "a")
    store = lambda: xr.open_zarr(repo.readonly_session("main").store, consolidated=False)  # noqa: E731
    later = np.array([300, -5, 32000], "int16")
    multidim.append_partition(repo, xr.Dataset({"flags": ("obs", later)}), "b")
    got = store()["flags"]
    assert got.dtype == np.int16
    assert got.values.tolist() == [1, 2, 300, -5, 32000]


def test_an_unsigned_column_widens_for_negative_values(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    multidim.append_partition(repo, xr.Dataset({"n": ("obs", np.array([200], "uint8"))}), "a")
    multidim.append_partition(repo, xr.Dataset({"n": ("obs", np.array([-1], "int8"))}), "b")
    got = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)["n"]
    assert got.values.tolist() == [200, -1]


def test_integer_casts_that_wrap_are_not_exact():
    from planetary_datasets.common.lossless import fits

    assert not fits(np.array([-1], "int8"), np.dtype("uint8"))
    assert not fits(np.array([300], "int16"), np.dtype("int8"))
    assert fits(np.array([0, 255], "int16"), np.dtype("uint8"))


def test_an_integer_column_widens_to_float_when_fractions_arrive(tmp_path):
    """Aeolus L2C: a field first seen with whole values, later with fractions."""
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    multidim.append_partition(repo, xr.Dataset({"arg": ("obs", np.array([1, 2], "int32"))}), "a")
    multidim.append_partition(repo, xr.Dataset({"arg": ("obs", np.array([2.5, -7.25]))}), "b")
    got = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)["arg"]
    assert got.dtype == np.float64
    assert got.values.tolist() == [1.0, 2.0, 2.5, -7.25]


def test_an_integer_column_with_a_fill_value_widens_to_float(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    first = xr.Dataset({"arg": ("obs", np.array([1, -999], "int32"))})
    first["arg"].attrs["_FillValue"] = np.int32(-999)
    multidim.append_partition(repo, first, "a")
    multidim.append_partition(repo, xr.Dataset({"arg": ("obs", np.array([0.5]))}), "b")
    got = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)["arg"].values
    assert got[0] == 1.0 and np.isnan(got[1]) and got[2] == 0.5
