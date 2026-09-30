"""MetOp-SG level 1b products from the EUMETSAT Data Store.

EPS-SG products are grouped netCDF4, so they are read directly rather than through the
Data Tailor: each granule's chosen variables are pulled from their groups, stacked on a
``time`` dimension at the middle of the sensing window, and padded to the store's shape.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.providers.polar._eumdac import EumdacProvider
from planetary_datasets.providers.polar._granule import mid_time

VII_CHANNELS = (
    443, 555, 668, 752, 763, 865, 914, 1240, 1375, 1630,
    2250, 3740, 3959, 4050, 6725, 7325, 8540, 10690, 12020, 13345,
)  # fmt: skip


class MetopSgProvider(EumdacProvider):
    """Base class for the EPS-SG level 1b netCDF products.

    Attributes:
        groups: netCDF group -> variables read from it.
        dim_renames: Product dimension names -> store dimension names.
        pad_dims: Dimensions padded to the store's size, or the partition's largest.
        chunks: Chunking applied before writing.
    """

    append_dim = "time"
    product_suffix = ".nc"
    obs_store = True
    decode_times = True
    groups: Dict[str, Sequence[str]] = {}
    dim_renames: Dict[str, str] = {}
    pad_dims: Sequence[str] = ()
    chunks: Dict[str, int] = {"time": 1}

    def read_granule(self, path: str) -> xr.Dataset:
        """Read one product into a single ``time`` step."""
        with xr.open_dataset(path, engine="h5netcdf") as root:
            attrs = dict(root.attrs)
        parts = []
        for group, names in self.groups.items():
            with xr.open_dataset(
                path, engine="h5netcdf", group=group, decode_times=self.decode_times
            ) as ds:
                parts.append(ds[list(names)].load())
        ds = xr.merge(parts, combine_attrs="drop", compat="no_conflicts").drop_encoding()
        ds = ds.rename({k: v for k, v in self.dim_renames.items() if k in ds.dims})
        start = pd.Timestamp(attrs["sensing_start_time_utc"])
        end = pd.Timestamp(attrs["sensing_end_time_utc"])
        ds = ds.expand_dims("time").assign_coords(time=[mid_time(start, end)])
        ds["platform_name"] = xr.DataArray([str(attrs.get("spacecraft", ""))], dims=["time"])
        return ds

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read, pad and concatenate every granule in the partition."""
        granules = []
        for path in self.iter_natives(input_files, temp_dir=temp_dir):
            try:
                granules.append(self.read_granule(path))
            except Exception as exc:  # noqa: BLE001 - one bad granule must not lose the window
                logger.warning(f"{self.name}: {path} failed: {exc}")
        if granules and self.pad_dims:
            stored = self.stored_sizes()
            shape = {
                d: stored.get(d) or max(g.sizes.get(d, 0) for g in granules) for d in self.pad_dims
            }
            granules = [g for g in (self.fit_to_shape(g, shape) for g in granules) if g is not None]
        ds = self.restrict_to_window(self.concat_granules(granules), it)
        if ds.sizes.get("time", 0) == 0:
            return ds
        return ds.chunk({d: n for d, n in self.chunks.items() if d in ds.dims})


class MetopSgMwsProvider(MetopSgProvider):
    """MWS microwave sounder brightness temperatures, 24 channels."""

    name = "metop_sg_mws"
    store_prefix = "bkr/polar/metop_sg_mws.icechunk"
    collection_id = "EO:EUM:DAT:0450"
    window = pd.Timedelta(1, "D")
    groups = {
        "data/navigation": (
            "mws_lat",
            "mws_lon",
            "mws_solar_zenith_angle",
            "mws_solar_azimuth_angle",
            "mws_satellite_zenith_angle",
            "mws_satellite_azimuth_angle",
            "mws_scantime_utc",
        ),
        "data/calibration": ("mws_toa_brightness_temperature",),
        "quality": ("L1B_quality_flag",),
    }
    dim_renames = {"n_scans": "y", "n_fovs": "x", "n_channels": "channel"}
    pad_dims = ("y",)
    chunks = {"time": 24}

    def read_granule(self, path: str) -> xr.Dataset:
        """Read one granule, storing brightness temperatures as float32."""
        ds = super().read_granule(path).rename({"mws_lat": "latitude", "mws_lon": "longitude"})
        bt = "mws_toa_brightness_temperature"
        ds[bt] = ds[bt].astype("float32")
        return ds.assign_coords(channel=range(1, ds.sizes["channel"] + 1))


class MetopSgMetimageProvider(MetopSgProvider):
    """METimage (VII) radiances, 20 channels, with geolocation on the product's tie points."""

    name = "metop_sg_metimage"
    store_prefix = "bkr/polar/metop_sg_metimage.icechunk"
    collection_id = "EO:EUM:DAT:0464"
    window = pd.Timedelta(10, "min")
    decode_times = False
    groups = {
        "data/measurement_data": (
            *(f"vii_{c}" for c in VII_CHANNELS),
            "latitude",
            "longitude",
            "solar_zenith",
            "solar_azimuth",
            "observation_zenith",
            "observation_azimuth",
        ),
    }
    dim_renames = {
        "num_lines": "y",
        "num_pixels": "x",
        "num_tie_points_alt": "y_tie",
        "num_tie_points_act": "x_tie",
    }
    pad_dims = ("y", "y_tie")


class MetopSgRoProvider(MetopSgProvider):
    """GRAS-2 radio occultation bending-angle profiles on the thinned impact levels."""

    name = "metop_sg_ro"
    store_prefix = "bkr/polar/metop_sg_ro.icechunk"
    collection_id = "EO:EUM:DAT:0452"
    window = pd.Timedelta(1, "h")
    decode_times = False
    groups = {
        "data/occultation": (
            "latitude",
            "longitude",
            "azimuth_north",
            "occultation_id",
            "undulation",
            "r_curve",
        ),
        "data/level_1b/thinned": (
            "impact",
            "impact_height",
            "bangle",
            "bangle_sdev",
            "bangle_l1",
            "bangle_l5",
            "lat_tp",
            "lon_tp",
            "azimuth_tp",
        ),
        "quality": ("overall_quality_ok",),
    }
    dim_renames = {"z": "level"}
    pad_dims = ("level",)
    chunks = {"time": 500}
