"""MetOp AVHRR level 1B imagery from the EUMETSAT Data Store.

AVHRR EPS products are read with satpy's ``avhrr_l1b_eps`` reader, one orbit per granule.
Orbits vary in length, so each is padded to a fixed swath size before being concatenated.

Replaces ``dags/assets/icechunky/metop.py``, which held source.coop credentials inline and
could only ever append (its initial-write branch was ``if False``).
"""

from __future__ import annotations

import pathlib
from typing import List

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar._granule import (
    mid_time,
    serialize_dataset_attrs,
)

#: Channels and geometry loaded from each orbit.
AVHRR_VARIABLES = [
    "1",
    "2",
    "3a",
    "3b",
    "4",
    "5",
    "cloud_flags",
    "latitude",
    "longitude",
    "satellite_azimuth_angle",
    "satellite_zenith_angle",
    "solar_azimuth_angle",
    "solar_zenith_angle",
]

#: Swath size every orbit is padded up to. Orbits larger than this are dropped rather than
#: silently truncated.
SWATH_SHAPE = {"y": 37800, "x": 2048}

#: Variables that keep full precision; everything else is stored as float16.
FULL_PRECISION = ("latitude", "longitude", "start_time", "end_time", "platform_name")


class MetopAvhrrProvider(EumdacProvider):
    """AVHRR level 1B imagery for the MetOp series."""

    name = "metop_avhrr"
    append_dim = "time"
    store_prefix = "bkr/polar/metop_avhrr.icechunk"
    collection_id = "EO:EUM:DAT:METOP:AVHRRL1"
    window = pd.Timedelta(4, "h")
    swath_shape = SWATH_SHAPE

    def process_granule(self, filename: str) -> xr.Dataset:
        """Read one AVHRR orbit into a single-timestep dataset."""
        from satpy import Scene

        scene = Scene([filename], reader="avhrr_l1b_eps")
        scene.load(AVHRR_VARIABLES)
        ds = scene.to_xarray_dataset()

        longitude, latitude = scene[AVHRR_VARIABLES[0]].attrs["area"].get_lonlats()
        ds["latitude"] = (("y", "x"), latitude)
        ds["longitude"] = (("y", "x"), longitude)

        start = pd.Timestamp(ds.attrs["start_time"])
        end = pd.Timestamp(ds.attrs["end_time"])
        ds = ds.expand_dims("time").assign_coords(time=[mid_time(start, end)])
        ds["start_time"] = xr.DataArray([start], dims=["time"]).astype("datetime64[ns]")
        ds["end_time"] = xr.DataArray([end], dims=["time"]).astype("datetime64[ns]")
        ds["platform_name"] = xr.DataArray([str(ds.attrs["platform_name"])], dims=["time"])

        ds = ds.load()
        for var in ds.data_vars:
            if var in ("latitude", "longitude"):
                ds[var] = ds[var].astype(np.float32)
            elif var not in FULL_PRECISION:
                ds[var] = ds[var].astype(np.float16)

        ds = serialize_dataset_attrs(ds, drop=("start_time", "end_time", "platform_name"))
        return ds.drop_vars("crs", errors="ignore")

    def align(self, ds: xr.Dataset) -> xr.Dataset | None:
        """Pad an orbit to :attr:`swath_shape`, or return None if it is larger than that."""
        return self.fit_to_shape(ds, self.swath_shape)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Unpack, read, pad and concatenate every AVHRR orbit in the partition."""
        datasets = []
        for native in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                aligned = self.align(self.process_granule(native))
            except Exception as exc:  # noqa: BLE001 - one bad orbit must not lose the window
                logger.warning(f"{self.name}: {native} failed: {exc}")
                continue
            if aligned is not None:
                datasets.append(aligned)

        ds = self.restrict_to_window(self.concat_granules(datasets), it)
        if ds.sizes.get(self.append_dim, 0) == 0:
            return ds
        return ds.chunk({"time": 1, "y": -1, "x": -1})
