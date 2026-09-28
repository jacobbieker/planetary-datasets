"""MeteoSwiss KENDA-CH1.

KENDA-CH1 is the 1 km kilometre-scale ensemble data assimilation over Switzerland. It is
delivered as one GRIB2 file per variable per hour, named
``kenda-ch1-<YYYYMMDDHH00>-<step>-<variable>-ctrl.grib2``, alongside two files of
horizontal and vertical constants that every timestep needs merged in.

Step ``0`` is the analysis and step ``1`` the one-hour forecast; they go to separate
stores because the analysis carries the full-layer fields and the forecast does not.

Unlike the other regional models here, KENDA arrives on local disk from a MeteoSwiss
feed rather than from a bucket, so :attr:`KENDAProviderBase.archive_path` points at a
directory under the configured data directory.
"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import reduce_precision
from planetary_datasets.config import Config
from planetary_datasets.providers.regional_lam_common import (
    chunk_present,
    long_name_slug,
    resolve_renames,
)

#: Sub-directory of the configured data directory holding the MeteoSwiss feed.
ARCHIVE_SUBDIR = "meteoswiss"

HORIZONTAL_CONSTANTS = "horizontal_constants_kenda-ch1.grib2"
VERTICAL_CONSTANTS = "vertical_constants_kenda-ch1.grib2"

#: KENDA carries cloud and soil-depth fields that, like the wind and temperature fields,
#: do not justify float32 at this resolution.
PRECISION_HINTS = ("wind", "fraction", "temperature", "cloud", "depth")

#: Scalar level markers cfgrib attaches to the per-variable files; they differ between
#: variables and would block the merge.
_DROP_COORDS = ("valid_time", "level", "surface", "meanSea", "heightAboveGround")


def load_constants(constant_files: List[str]) -> xr.Dataset:
    """Load the horizontal and vertical constant fields and name them usefully.

    The horizontal file carries the real latitude/longitude of the rotated grid, which is
    the only place the geolocation is published, so a timestep without it cannot be
    written.
    """
    horizontal_path = next((f for f in constant_files if "horizontal" in f), None)
    vertical_path = next((f for f in constant_files if "vertical" in f), None)
    if horizontal_path is None or vertical_path is None:
        raise FileNotFoundError(
            "KENDA needs both horizontal and vertical constants; "
            f"got {[str(f) for f in constant_files]}"
        )

    horizontal = xr.open_dataset(horizontal_path, engine="cfgrib", decode_cf=True).drop_vars(
        "unknown", errors="ignore"
    )
    vertical = xr.open_dataset(vertical_path, engine="cfgrib", decode_cf=True)

    renames = {
        var: long_name_slug(str(horizontal[var].attrs["long_name"]))
        for var in horizontal.data_vars
        if "long_name" in horizontal[var].attrs
    }
    renames["h"] = "height"
    horizontal = horizontal.rename(resolve_renames(horizontal, renames))
    horizontal = horizontal.rename(
        resolve_renames(
            horizontal,
            {"longitude_on_t_grid": "longitude", "latitude_on_t_grid": "latitude"},
        )
    ).drop_vars(["valid_time", "level", "surface", "time", "step"], errors="ignore")

    vertical = vertical.rename(
        resolve_renames(vertical, {"h": "height_above_ground"})
    ).drop_vars(
        ["valid_time", "level", "surface", "time", "step"], errors="ignore"
    )

    return xr.merge([horizontal, vertical], compat="no_conflicts")


class KENDAProviderBase(BaseProvider):
    """Shared reading and merging for the KENDA analysis and forecast stores."""

    append_dim = "time"
    #: GRIB forecast step in the filename: ``0`` for analysis, ``1`` for the +1 h forecast.
    step: str = "0"
    #: Renames applied to the merged dataset's vertical dimensions.
    vertical_renames: dict[str, str] = {"generalVertical": "model_level_half"}
    #: Chunking applied to the assembled dataset.
    chunks: dict[str, int] = {"time": 1, "model_level_half": -1, "values": -1}
    #: Extra renames applied before the long-name renames, for variables cfgrib decodes
    #: with an unhelpful short name.
    short_name_renames: dict[str, str] = {}

    def __init__(
        self,
        config: Config | None = None,
        archive_path: str | pathlib.Path | None = None,
    ):
        """Create the provider.

        Args:
            config: Configuration override, defaulting to the process-wide config.
            archive_path: Directory holding the MeteoSwiss GRIB feed. Defaults to
                ``<data_dir>/meteoswiss``.
        """
        super().__init__(config)
        self._archive_path = pathlib.Path(archive_path) if archive_path is not None else None

    @property
    def archive_path(self) -> pathlib.Path:
        """Directory the MeteoSwiss feed is delivered into."""
        if self._archive_path is not None:
            return self._archive_path
        return self.config.data_dir / ARCHIVE_SUBDIR

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Locate the per-variable files and the constants for one init time.

        Nothing is downloaded: the feed writes straight into :attr:`archive_path`.
        """
        path = self.archive_path
        pattern = f"kenda-ch1-{it.strftime('%Y%m%d%H00')}-{self.step}-*-ctrl.grib2"
        data_files = sorted(path.glob(pattern))
        if not data_files:
            logger.warning(f"{self.name}: no files matching {pattern} under {path}")
            return []

        constants = [path / HORIZONTAL_CONSTANTS, path / VERTICAL_CONSTANTS]
        missing = [c for c in constants if not c.is_file()]
        if missing:
            names = [c.name for c in missing]
            logger.warning(f"{self.name}: missing constants {names} under {path}")
            return []

        return [str(f) for f in data_files] + [str(c) for c in constants]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the per-variable files with the constants into one timestep."""
        constant_files = [f for f in input_files if "constants" in pathlib.Path(f).name]
        data_files = [f for f in input_files if "constants" not in pathlib.Path(f).name]
        if not data_files:
            raise FileNotFoundError(f"{self.name}: no data files for {it}")

        constants = load_constants(constant_files)

        datasets = []
        for path in data_files:
            ds = xr.open_dataset(path, engine="cfgrib", decode_cf=True)
            if "unknown" in ds.data_vars:
                # cfgrib could not resolve the parameter; the filename carries its name.
                ds = ds.rename({"unknown": pathlib.Path(path).name.split("-")[-2]})
            datasets.append(ds)

        merged = (
            xr.merge(datasets, compat="no_conflicts")
            .drop_vars(list(_DROP_COORDS), errors="ignore")
            .expand_dims("time")
        )

        renames = dict(self.short_name_renames)
        for var in merged.data_vars:
            long_name = merged[var].attrs.get("long_name")
            if long_name and long_name != "unknown":
                renames[var] = long_name_slug(str(long_name))
        merged = merged.rename(resolve_renames(merged, renames))

        merged = xr.merge([merged, constants], compat="no_conflicts")
        merged = merged.rename(
            {old: new for old, new in self.vertical_renames.items() if old in merged.dims}
        )

        merged = reduce_precision(merged, hints=PRECISION_HINTS)
        if "model_level_half" in merged.dims:
            merged = merged.sortby("model_level_half", ascending=True)
        return chunk_present(merged.sortby("time"), self.chunks)


class KENDAAnalysisProvider(KENDAProviderBase):
    """KENDA-CH1 analysis (step 0), including the full-layer model levels."""

    name = "kenda_analysis"
    store_prefix = "bkr/dmi/kenda_switzerland.icechunk"
    step = "0"
    vertical_renames = {
        "generalVertical": "model_level_half",
        "generalVerticalLayer": "model_level",
    }
    chunks = {"time": 1, "model_level_half": -1, "model_level": -1, "values": -1}
    short_name_renames = {"twater": "total_water"}


class KENDAForecastProvider(KENDAProviderBase):
    """KENDA-CH1 one-hour forecast (step 1)."""

    name = "kenda_forecast"
    store_prefix = "bkr/dmi/kenda_forecast_switzerland.icechunk"
    step = "1"
    vertical_renames = {"generalVertical": "model_level_half"}
    chunks = {"time": 1, "model_level_half": -1, "values": -1}


#: Backwards-compatible alias: the analysis store is what ``KENDAProvider`` used to write
#: by default.
KENDAProvider = KENDAAnalysisProvider
