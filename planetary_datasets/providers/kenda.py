"""MeteoSwiss KENDA-CH1.

KENDA-CH1 is the 1 km kilometre-scale ensemble data assimilation over Switzerland. It is
delivered as one GRIB2 file per variable per hour, named
``kenda-ch1-<YYYYMMDDHH00>-<step>-<variable>-ctrl.grib2``, alongside two files of
horizontal and vertical constants that every timestep needs merged in.

Step ``0`` is the analysis and step ``1`` the one-hour forecast; they go to separate
stores because the analysis carries the full-layer fields and the forecast does not.

Each product is archived in *eras*, one store per era, listed in :data:`ANALYSIS_ERAS` and
:data:`FORECAST_ERAS`. MeteoSwiss widened the forecast from 10 to 20 fields at
2026-07-21 09:00, which cannot be appended to the store holding the narrower fields, so
the archive was restarted in ``..._20260721T1100.icechunk`` stores. The analysis did not
change shape, but it was restarted alongside, so its eras differ only in where they live.
An init time is routed to its era by time, and must carry exactly that era's fields: an
hour with fields missing is skipped (the feed has a handful of half-delivered hours, and
appending one of those once left a store with variables of different lengths), and one
with fields the era does not know raises :class:`KENDASchemaChanged`, because it means
MeteoSwiss has changed the product again and a new era needs adding.

Unlike the other regional models here, KENDA arrives on local disk from a MeteoSwiss
feed rather than from a bucket, so :attr:`KENDAProviderBase.archive_path` points at a
directory under the configured data directory.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import List

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import reduce_precision
from planetary_datasets.common.store import missing_timesteps as _missing_timesteps
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

_FILENAME = re.compile(r"^kenda-ch1-(?P<time>\d{12})-(?P<step>\d+)-(?P<variable>.+)-ctrl\.grib2$")


class KENDASchemaChanged(RuntimeError):
    """An init time carries fields its era does not know: MeteoSwiss changed the product."""


@dataclass(frozen=True)
class KENDAEra:
    """A stretch of the archive with one set of fields, written to one store.

    Attributes:
        start: First init time in the era.
        store_prefix: Store the era is written to.
        variables: GRIB short names, as they appear in the filenames, that every init time
            in the era carries.
    """

    start: pd.Timestamp
    store_prefix: str
    variables: frozenset[str]


#: The 18 analysis fields, unchanged since the feed began.
ANALYSIS_VARIABLES = frozenset(
    {
        "cape_ml", "cape_mu", "cin_ml", "cin_mu", "clc", "den", "h_snow", "p", "pmsl",
        "qc", "qv", "t", "td_2m", "tke", "twater", "u", "v", "w",
    }
)  # fmt: skip

#: The 10 forecast fields published until 2026-07-21 08:00.
FORECAST_VARIABLES_V1 = frozenset(
    {
        "asob_s", "aswdifd_s", "aswdifu_s", "aswdir_s", "athb_s", "grau_gsp", "rain_gsp",
        "snow_gsp", "tot_prec", "vmax_10m",
    }
)  # fmt: skip

#: From 2026-07-21 09:00 the forecast adds the outgoing-shortwave, longwave component,
#: momentum flux and column-integrated water fields.
FORECAST_VARIABLES_V2 = FORECAST_VARIABLES_V1 | frozenset(
    {
        "asob_s_os", "aswdifu_s_os", "aswdir_s_os", "athd_s", "athu_s", "aumfl_s",
        "avmfl_s", "tqc", "tqi", "tqv",
    }
)  # fmt: skip

#: The archive restarted on 2026-07-21; the new stores are named after their first hour.
RESTART_SUFFIX = "_20260721T1100"

ANALYSIS_ERAS = (
    KENDAEra(pd.Timestamp.min, "bkr/dmi/kenda_switzerland.icechunk", ANALYSIS_VARIABLES),
    KENDAEra(
        pd.Timestamp("2026-07-21T11:00"),
        f"bkr/dmi/kenda_switzerland{RESTART_SUFFIX}.icechunk",
        ANALYSIS_VARIABLES,
    ),
)

FORECAST_ERAS = (
    KENDAEra(
        pd.Timestamp.min, "bkr/dmi/kenda_forecast_switzerland.icechunk", FORECAST_VARIABLES_V1
    ),
    # Starts two hours before the store's name: 09:00 and 10:00 already carry the wider
    # set, so they can only be written here.
    KENDAEra(
        pd.Timestamp("2026-07-21T09:00"),
        f"bkr/dmi/kenda_forecast_switzerland{RESTART_SUFFIX}.icechunk",
        FORECAST_VARIABLES_V2,
    ),
)


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

    vertical = vertical.rename(resolve_renames(vertical, {"h": "height_above_ground"})).drop_vars(
        ["valid_time", "level", "surface", "time", "step"], errors="ignore"
    )

    return xr.merge([horizontal, vertical], compat="no_conflicts")


class KENDAProviderBase(BaseProvider):
    """Shared reading and merging for the KENDA analysis and forecast stores.

    Writes go to the store of the era an init time falls in (see :attr:`eras`), so
    :attr:`store_prefix` names only the current era's store.
    """

    append_dim = "time"
    #: The product's eras, oldest first. See the module docstring.
    eras: tuple[KENDAEra, ...] = ()
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
        self._repos: dict[str, icechunk.Repository] = {}

    def era_for(self, it: pd.Timestamp) -> KENDAEra:
        """The era an init time belongs to."""
        era = self.eras[0]
        for candidate in self.eras:
            if pd.Timestamp(it) >= candidate.start:
                era = candidate
        return era

    def repo_for(self, era: KENDAEra) -> icechunk.Repository:
        """The repository holding ``era``, opened once per provider."""
        if era.store_prefix not in self._repos:
            self._repos[era.store_prefix] = self.config.icechunk_repo(era.store_prefix)
        return self._repos[era.store_prefix]

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Timesteps in ``desired`` not yet in the store of the era each belongs to."""
        by_era: dict[KENDAEra, list[pd.Timestamp]] = {}
        for it in pd.DatetimeIndex(desired):
            by_era.setdefault(self.era_for(it), []).append(it)
        missing: list[pd.Timestamp] = []
        for era, times in by_era.items():
            missing.extend(
                _missing_timesteps(self.repo_for(era), times, append_dim=self.append_dim)
            )
        return sorted(missing)

    def store_for(self, processed: xr.Dataset, repo: icechunk.Repository) -> icechunk.Repository:
        """The repository of the era ``processed`` falls in."""
        times = pd.DatetimeIndex(np.atleast_1d(processed[self.append_dim].values))
        eras = {self.era_for(t) for t in times}
        if len(eras) != 1:
            raise ValueError(f"{self.name}: {len(times)} timesteps span {len(eras)} eras")
        return self.repo_for(eras.pop())

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write, refusing loudly rather than skipping when the store's fields differ.

        The shared writer logs a variable mismatch and returns False, which reads as
        "nothing to do" and would let an hour quietly go unarchived. Here the filenames
        have already been checked against the era, so a mismatch is a decoding change.
        """
        try:
            stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        except (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError):
            stored = None  # an empty store is created by the write
        if stored is not None and set(stored.data_vars) != set(processed.data_vars):
            raise KENDASchemaChanged(
                f"{self.name}: fields do not match the store. Only in the data: "
                f"{sorted(set(processed.data_vars) - set(stored.data_vars))}; only in the "
                f"store: {sorted(set(stored.data_vars) - set(processed.data_vars))}"
            )
        return super().write_to_icechunk(repo, processed)

    @property
    def archive_path(self) -> pathlib.Path:
        """Directory the MeteoSwiss feed is delivered into."""
        if self._archive_path is not None:
            return self._archive_path
        return self.config.data_dir / ARCHIVE_SUBDIR

    def is_complete(self, it: pd.Timestamp, found: set[str]) -> bool:
        """Whether the fields ``found`` for ``it`` are exactly its era's.

        Raises:
            KENDASchemaChanged: if ``found`` includes fields the era does not know.
        """
        era = self.era_for(it)
        unknown = found - era.variables
        if unknown:
            raise KENDASchemaChanged(
                f"{self.name}: {it} carries {sorted(unknown)}, which the era starting "
                f"{era.start} does not; add a new era to {type(self).__name__}.eras"
            )
        absent = era.variables - found
        if absent:
            logger.warning(f"{self.name}: {it} is incomplete, missing {sorted(absent)}; skipping")
            return False
        return True

    @staticmethod
    def parse_filename(name: str) -> tuple[pd.Timestamp, str, str] | None:
        """``(init time, step, variable)`` from a feed filename, or None for other files."""
        match = _FILENAME.match(name)
        if match is None:
            return None
        it = pd.to_datetime(match.group("time"), format="%Y%m%d%H%M")
        return it, match.group("step"), match.group("variable")

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

        if not self.is_complete(
            it, {_FILENAME.match(f.name).group("variable") for f in data_files}
        ):
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

        # Opened lazily (dask-backed) so nothing is decoded until the dataset is written,
        # and then one variable at a time rather than the whole hour at once.
        datasets = []
        for path in data_files:
            ds = xr.open_dataset(path, engine="cfgrib", decode_cf=True, chunks={})
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
    eras = ANALYSIS_ERAS
    store_prefix = ANALYSIS_ERAS[-1].store_prefix
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
    eras = FORECAST_ERAS
    store_prefix = FORECAST_ERAS[-1].store_prefix
    step = "1"
    vertical_renames = {"generalVertical": "model_level_half"}
    chunks = {"time": 1, "model_level_half": -1, "values": -1}


#: Backwards-compatible alias: the analysis store is what ``KENDAProvider`` used to write
#: by default.
KENDAProvider = KENDAAnalysisProvider
