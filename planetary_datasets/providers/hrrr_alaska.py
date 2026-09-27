"""NOAA HRRR Alaska nest.

The High-Resolution Rapid Refresh runs a 3 km Alaska nest every three hours. Each run
publishes three GRIB2 files per forecast hour — surface, native (hybrid) and pressure
levels — on the public ``noaa-hrrr-bdp-pds`` bucket. The first three forecast hours are
kept, stored as a ``step`` axis under each init time.

Source: ``s3://noaa-hrrr-bdp-pds/hrrr.<YYYYMMDD>/alaska/hrrr.t<HH>z.wrf<kind>f<FF>.ak.grib2``
"""

from __future__ import annotations

from typing import List

import pandas as pd
import xarray as xr

from planetary_datasets.common.dataset import reduce_precision, sort_vertical_coords
from planetary_datasets.providers.regional_lam_common import (
    GribMergeSpec,
    GribNestProvider,
    chunk_present,
    rename_present,
)

BUCKET = "noaa-hrrr-bdp-pds"

#: HRRR publishes the full 9-layer soil profile alongside thinner subsets of it. Only the
#: full profile lines up with the store, so anything shallower is dropped.
_MIN_SOIL_LEVELS = 9

ALASKA_MERGE_SPEC = GribMergeSpec(
    min_soil_levels=_MIN_SOIL_LEVELS,
    # Steps stay as their own axis here, so valid_time is redundant with time + step and
    # would block the concatenation.
    drop_coords_after_merge=("valid_time",),
)


class AlaskaHRRRProvider(GribNestProvider):
    """HRRR Alaska nest, one partition per 3-hourly init time with three forecast steps."""

    name = "alaska_hrrr"
    append_dim = "time"
    store_prefix = "bkr/dmi/alaska_hrrr.icechunk"

    forecast_steps = (0, 1, 2)
    products = ("wrfsfc", "wrfnat", "wrfprs")
    merge_spec = ALASKA_MERGE_SPEC

    chunks = {"time": 1, "y": -1, "x": -1, "step": 1}

    def file_url(self, it: pd.Timestamp, step: int, product: str) -> str:
        """Public HTTPS URL for one level type of one forecast hour."""
        return (
            f"https://{BUCKET}.s3.amazonaws.com/hrrr.{it.strftime('%Y%m%d')}/alaska/"
            f"hrrr.t{it.hour:02d}z.{product}f{step:02d}.ak.grib2"
        )

    def combine(self, per_step: List[xr.Dataset], it: pd.Timestamp) -> xr.Dataset:
        """Stack the forecast hours on ``step`` under a single init time."""
        ds = xr.concat(per_step, dim="step").sortby("step")
        # The merged sub-datasets each carry the init time as a scalar; restate it from the
        # partition key so a mislabelled GRIB header cannot shift a run in the store.
        ds = ds.assign_coords(time=it).expand_dims("time")
        ds = rename_present(ds, {"isobaricInhPa": "level"})
        ds = sort_vertical_coords(ds, level_name="level")
        ds = reduce_precision(ds)
        return chunk_present(ds, self.chunks).sortby("time")
