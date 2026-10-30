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
