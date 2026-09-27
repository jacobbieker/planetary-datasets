"""Destination Earth Data Lake (earthdatahub.destine.eu) analysis-ready zarr.

Destination Earth publishes a set of reanalysis and climate-projection datasets as cloud
zarr behind a personal access token, the most useful of which here is hourly ERA5-Land.
Opening one is a single :func:`xarray.open_dataset` call, so most uses want
:func:`open_destine_zarr` and no store at all; :class:`DestinEERA5LandProvider` exists for
the case where a subset needs to be pinned into an icechunk store next to the rest of the
archive, so downstream jobs are not reading through someone else's availability.

Credentials
-----------
Set ``DESTINE_PAT`` in the environment or ``.env``. It is a personal access token from
https://auth.destine.eu - obtain one from your own account rather than sharing. The token
is sent as the password half of HTTP basic auth against the ``edh`` user, which means it
lands inside the dataset URL; nothing in this module logs that URL, and neither should
anything else.

Mirroring
---------
The full ERA5-Land grid is 0.1 degrees, so one hourly field is about 26 MB and a day of
one variable about 620 MB. Mirroring therefore takes an explicit variable list;
:data:`DEFAULT_VARIABLES` is a small, generally useful subset. Passing ``variables=None``
mirrors everything the source holds, which for a day is tens of gigabytes and will be
stopped by the memory guard on anything but a large machine.
"""

from __future__ import annotations

import pathlib
from typing import List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import make_lat_lon_coords_consistent
from planetary_datasets.providers._timestamps import NaiveUTCPartitions, to_naive_utc

#: Host serving the Destination Earth Data Lake zarr stores.
DESTINE_HOST = "data.earthdatahub.destine.eu"

#: Path of the hourly ERA5-Land reanalysis, everything but Antarctica.
ERA5_LAND_PATH = "era5/reanalysis-era5-land-no-antartica-v0.zarr"

#: A small, generally useful ERA5-Land subset: 2 m temperature and dewpoint, 10 m wind,
#: total precipitation and downward shortwave radiation.
DEFAULT_VARIABLES: tuple[str, ...] = ("t2m", "d2m", "u10", "v10", "tp", "ssrd")

#: Names the Destination Earth stores use for their time dimension.
_TIME_NAMES = ("valid_time", "time", "forecast_reference_time")

#: Hours expected in one partition. ERA5-Land is hourly and has no gaps, so anything else
#: means the day is not fully published.
HOURS_PER_DAY = 24


def destine_url(path: str = ERA5_LAND_PATH, config=None) -> str:
    """Build the authenticated URL for a Destination Earth dataset.

    The returned string embeds the personal access token. Do not log it.

    Raises:
        MissingCredential: If ``DESTINE_PAT`` is not configured.
    """
    from planetary_datasets.config import get_config

    (pat,) = (config or get_config()).credentials.require("destine_pat")
    return f"https://edh:{pat}@{DESTINE_HOST}/{path.lstrip('/')}"


def open_destine_zarr(
    path: str = ERA5_LAND_PATH,
    config=None,
    chunks: dict | str | None = None,
    dtype: str | None = "float32",
) -> xr.Dataset:
    """Open a Destination Earth zarr store read-only.

    This is the read-only accessor the original ``destinE.py`` script was: nothing is
    downloaded and nothing is written. Use :class:`DestinEERA5LandProvider` to mirror a
    subset into a store.

    Args:
        path: Dataset path under the Data Lake host.
        config: Configuration to take the token from. Defaults to the process config.
        chunks: Passed to :func:`xarray.open_dataset`. The default, None, becomes ``{}``,
            which keeps the store's own chunking and makes the result lazy and
            dask-backed.
        dtype: Cast the fields to this dtype on open, or None to leave them alone. The
            source is float32 or float64 depending on the variable.
    """
    ds = xr.open_dataset(
        destine_url(path, config=config),
        chunks={} if chunks is None else chunks,
        engine="zarr",
    )
    if dtype is not None:
        # Cast the fields only; casting the dataset would turn the time coordinate into
        # a float.
        for var in ds.data_vars:
            if np.issubdtype(ds[var].dtype, np.floating):
                ds[var] = ds[var].astype(dtype)
    return ds


class DestinEERA5LandProvider(NaiveUTCPartitions, BaseProvider):
    """Mirror one day of Destination Earth hourly ERA5-Land into an icechunk store.

    Args:
        variables: Fields to mirror. None mirrors every field in the source; see the
            module docstring for why that is rarely what you want.
        path: Dataset path under the Data Lake host.
        store_prefix: Override the store location.
    """

    name = "destine_era5_land"
    append_dim = "time"

    def __init__(
        self,
        variables: Sequence[str] | None = DEFAULT_VARIABLES,
        path: str = ERA5_LAND_PATH,
        store_prefix: str | None = None,
        config=None,
    ):
        super().__init__(config=config)
        self.variables = tuple(variables) if variables is not None else None
        self.path = path
        self.store_prefix = store_prefix or "bkr/destine/era5_land.icechunk"
        self._source: xr.Dataset | None = None
        self._times: pd.DatetimeIndex | None = None

    def source(self) -> xr.Dataset:
        """The remote dataset, opened once per provider instance."""
        if self._source is None:
            self._source = open_destine_zarr(self.path, config=self.config, dtype=None)
        return self._source

    def source_times(self) -> pd.DatetimeIndex:
        """The source's time axis, read once per provider instance.

        Checked for monotonicity here so the positional day slice in :meth:`process` can
        rely on it.
        """
        if self._times is None:
            ds = self.source()
            times = pd.DatetimeIndex(ds[self.time_name(ds)].values)
            if not times.is_monotonic_increasing:
                raise ValueError(f"{self.name}: the source time axis is not sorted ascending")
            self._times = times
        return self._times

    @staticmethod
    def time_name(ds: xr.Dataset) -> str:
        """Name of the time dimension in a Destination Earth dataset."""
        for candidate in _TIME_NAMES:
            if candidate in ds.dims:
                return candidate
        raise ValueError(f"no time dimension found in the source; looked for {_TIME_NAMES}")

    def fetch(self, it, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Check the day is inside the source's time range.

        Nothing is downloaded here: the source is a remote zarr and the slice is read in
        :meth:`process`. The returned marker is the unauthenticated dataset path, so the
        token never reaches a log line.
        """
        it = to_naive_utc(it).normalize()
        times = self.source_times()
        if not len(times):
            raise RuntimeError(f"{self.name}: the source has an empty time axis")
        if it < times[0]:
            # Genuinely absent: the reanalysis does not go back this far and never will.
            logger.info(f"{self.name}: {it:%Y-%m-%d} predates the source, skipping")
            return []
        # A day whose last hour is not published yet must not be written: the store would
        # hold a short day that reads as present and is never revisited. That covers both
        # "the day has not started" and "the day is half here".
        if times[-1] < it + np.timedelta64(23, "h"):
            raise RuntimeError(
                f"{self.name}: {it:%Y-%m-%d} is not fully published; the source ends at "
                f"{times[-1]:%Y-%m-%d %H:%M}"
            )
        return [self.path]

    def process(
        self,
        input_files: List[str],
        it,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        it = to_naive_utc(it).normalize()
        ds = self.source()
        time_name = self.time_name(ds)

        if self.variables is not None:
            missing = [v for v in self.variables if v not in ds.data_vars]
            if missing:
                raise KeyError(f"{self.name}: source has no variable(s) {missing}")
            ds = ds[list(self.variables)]

        # Positional rather than label slicing: the source time axis is hourly and
        # monotonic, so searchsorted picks out one contiguous day without xarray having
        # to coerce a label slice against a lazily-backed index.
        bounds = pd.DatetimeIndex([it, it + np.timedelta64(24, "h")])
        start, stop = self.source_times().searchsorted(bounds)
        day = ds.isel({time_name: slice(int(start), int(stop))})
        found = day.sizes.get(time_name, 0)
        if found != HOURS_PER_DAY:
            # A short day would be committed and then read as present forever, so the
            # missing hours would never be filled. Fail instead and let the partition be
            # retried once the source is complete.
            raise ValueError(
                f"{self.name}: {it:%Y-%m-%d} has {found} of {HOURS_PER_DAY} hours in the source"
            )

        if time_name != self.append_dim:
            day = day.rename({time_name: self.append_dim})
        day = make_lat_lon_coords_consistent(day)

        for var in day.data_vars:
            if np.issubdtype(day[var].dtype, np.floating):
                day[var] = day[var].astype("float32")

        day = day.load()
        day.attrs.update(
            {
                "title": "ERA5-Land hourly, mirrored from the Destination Earth Data Lake",
                "source": f"https://{DESTINE_HOST}/{self.path}",
                "references": "https://earthdatahub.destine.eu",
            }
        )
        return day
