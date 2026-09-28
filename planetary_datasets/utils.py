"""Small helpers for reading icechunk stores.

For writing datasets use :mod:`planetary_datasets.common.store`; these are read-side
conveniences used by analysis scripts and Dagster sensors.
"""

from __future__ import annotations

import icechunk
import xarray as xr


def open_icechunk_store(storage_config: dict) -> icechunk.Repository:
    """Open an existing S3-backed icechunk repository from a storage config dict."""
    return icechunk.Repository.open(icechunk.s3_storage(**storage_config))


def open_xarray_from_icechunk(repo: icechunk.Repository, branch: str = "main") -> xr.Dataset:
    """Open a repository's contents as an xarray Dataset."""
    return xr.open_zarr(repo.readonly_session(branch).store, consolidated=False)


def get_time_dim(ds: xr.Dataset) -> str:
    """Return the name of the dataset's time dimension, preferring ``init_time``."""
    return "init_time" if "init_time" in ds.dims else "time"


def get_latest_time_and_timestamps(ds: xr.Dataset) -> tuple:
    """Return ``(latest_time, all_timestamps)`` along the dataset's time dimension."""
    timestamps = ds[get_time_dim(ds)].values
    if len(timestamps) == 0:
        return None, timestamps
    return timestamps[-1], timestamps
