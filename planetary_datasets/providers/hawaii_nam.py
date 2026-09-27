"""NOAA NAM Hawaii nest.

The North American Mesoscale model runs a 3 km nest over Hawaii four times a day. Each
run publishes hourly GRIB2 files on the public ``noaa-nam-pds`` bucket; the first six
hours of each run are taken so that consecutive runs tile into a continuous hourly
analysis-like series.

Source: ``s3://noaa-nam-pds/nam.<YYYYMMDD>/nam.t<HH>z.hawaiinest.hiresf<FF>.tm00.grib2``
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

BUCKET = "noaa-nam-pds"

#: The NAM nest publishes both the full soil profile and a single-layer summary; the
#: summary conflicts with the profile on merge.
_MIN_SOIL_LEVELS = 1


def _soil_temperature_without_profile(ds: xr.Dataset) -> bool:
    """Soil temperature is only useful alongside the soil depth axis it belongs to."""
    return "soil_temperature" in ds.data_vars and "depthBelowLandLayer" not in ds.coords


def _wind_on_hybrid_levels(ds: xr.Dataset) -> bool:
    """Hybrid-level winds duplicate the pressure-level winds on a grid nothing else uses."""
    return "u_component_of_wind" in ds.data_vars and "hybrid" in ds.dims


def _duplicate_surface_flux_block(ds: xr.Dataset) -> bool:
    """A second copy of the surface flux fields, published with a different step type."""
    return {
        "precipitation_rate_at_surface",
        "surface_downward_short-wave_radiation_flux_at_surface",
        "ground_heat_flux_at_surface",
    } <= set(map(str, ds.data_vars))


def _pressure_only(ds: xr.Dataset) -> bool:
    """A bare pressure field with no level information to hang it on."""
    return {str(v) for v in ds.data_vars} == {"pressure"}


HAWAII_MERGE_SPEC = GribMergeSpec(
    min_soil_levels=_MIN_SOIL_LEVELS,
    extra_skips=(
        _soil_temperature_without_profile,
        _wind_on_hybrid_levels,
        _duplicate_surface_flux_block,
        _pressure_only,
    ),
    # Steps are folded into valid_time by combine(), so the init time and step markers
    # would only get in the way of the concatenation.
    drop_coords_after_merge=("time", "step"),
)


class HawaiiNAMProvider(GribNestProvider):
    """NAM Hawaii nest, written as an hourly series keyed on valid time.

    One partition is one 6-hourly init time and contributes six hourly steps, so the
    store's ``time`` axis is continuous rather than one point per run.
    """

    name = "hawaii_nam"
    append_dim = "time"
    store_prefix = "bkr/dmi/hawaii_nams.icechunk"

    forecast_steps = (0, 1, 2, 3, 4, 5)
    products = ("hawaiinest",)
    merge_spec = HAWAII_MERGE_SPEC

    chunks = {"time": 1, "values": -1, "level": -1, "depth": -1}

    def file_url(self, it: pd.Timestamp, step: int, product: str) -> str:
        """Public HTTPS URL for one forecast hour of one run."""
        return (
            f"https://{BUCKET}.s3.amazonaws.com/nam.{it.strftime('%Y%m%d')}/"
            f"nam.t{it.hour:02d}z.{product}.hiresf{step:02d}.tm00.grib2"
        )

    def combine(self, per_step: List[xr.Dataset], it: pd.Timestamp) -> xr.Dataset:
        """Stack the forecast hours on valid time and present them as the time axis."""
        ds = xr.concat(per_step, dim="valid_time").rename({"valid_time": "time"})
        ds = rename_present(ds, {"isobaricInhPa": "level", "depthBelowLandLayer": "depth"})
        # Pressure descending, soil depth ascending: the order the store was created with.
        ds = sort_vertical_coords(ds, level_name="level", height_name="depth")
        ds = reduce_precision(ds)
        return chunk_present(ds, self.chunks).sortby("time")
