"""Fixed-width strings append to a store that holds variable-length strings."""

from __future__ import annotations

import icechunk
import numpy as np
import xarray as xr

from planetary_datasets.common.store import match_string_dtypes, write_to_icechunk


def step(t: int, platform: np.ndarray) -> xr.Dataset:
    return xr.Dataset(
        {"platform_name": ("time", platform), "v": ("time", np.array([float(t)]))},
        coords={"time": np.array([np.datetime64("2026-10-30T00:00") + np.timedelta64(t, "h")])},
    )


def test_a_fixed_width_platform_name_appends_to_a_vlen_string_store(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    assert write_to_icechunk(repo, step(0, np.array(["Metop-B"], dtype=object)))
    # xarray 2026.9 rejects <U7 against the stored StringDType unless it is converted.
    assert write_to_icechunk(repo, step(1, np.array(["Metop-C"])))
    got = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert got["platform_name"].values.tolist() == ["Metop-B", "Metop-C"]


def test_only_string_variables_against_string_arrays_are_converted():
    existing = xr.Dataset(
        {"s": ("time", np.array(["a"], dtype=object)), "n": ("time", np.array([1.0]))}
    )
    incoming = xr.Dataset({"s": ("time", np.array(["bb"])), "n": ("time", np.array([2.0]))})
    out = match_string_dtypes(incoming, existing)
    assert out["s"].dtype == object and out["n"].dtype == np.float64
    assert match_string_dtypes(incoming, xr.Dataset())["s"].dtype.kind == "U"
