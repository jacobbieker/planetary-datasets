"""DMI HARMONIE over Greenland and Iceland.

The Danish Meteorological Institute runs HARMONIE over the Greenland/Iceland domain every
three hours and publishes GRIB on the open ``dmi-opendata`` bucket, keeping roughly the
last three days. Each init time is stored with its first three hourly forecast steps.

Two stores are produced from the same archive:

* :class:`DMIHarmonieProvider` — pressure levels (``PL``) plus surface (``SF``).
* :class:`DMIHarmonieModelLevelProvider` — native model levels (``ML``).

Source: ``s3://dmi-opendata/forecastdata/HARMONIE_IG_<TYPE>/HARMONIE_IG_<TYPE>_<init>_<valid>.grib``
"""

from __future__ import annotations

import pathlib
from typing import List

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.grib import open_grib_datasets
from planetary_datasets.providers.regional_lam_common import (
    chunk_present,
    download_with_filesystem,
    init_time_download_dir,
    long_name_slug,
    rename_present,
    resolve_renames,
)

BUCKET = "dmi-opendata"
TIME_FORMAT = "%Y-%m-%dT%H%M%SZ"

#: Coordinates worth keeping on the merged surface dataset. Everything else cfgrib
#: attaches is a scalar level marker that would block the merge.
_KEEP_COORDS = ("latitude", "longitude", "time", "valid_time", "step")

#: ``h`` means a different height depending on which level type it came from, so it is
#: renamed after the level type rather than after its (identical) long name.
_HEIGHT_BY_LEVEL_TYPE = {
    "neutralBuoyancy": "height_of_neutral_buoyancy",
    "isothermal": "height_of_isothermal_layer",
    "isothermZero": "height_of_zero_isotherm",
    "freeConvection": "height_of_free_convection",
    "adiabaticCondensation": "height_of_adiabatic_condensation_level",
}

#: Short names that appear on several near-surface level stacks and so need a height
#: suffix to stay distinct.
_MULTI_LEVEL_SHORT_NAMES = ("t", "u", "v", "q", "gh", "r", "cc")


def _drop_scalar_level_coords(ds: xr.Dataset, keep: tuple[str, ...] = _KEEP_COORDS) -> xr.Dataset:
    """Drop scalar level-marker coordinates, keeping the real axes."""
    return ds.drop_vars(
        [coord for coord in ds.coords if coord not in keep and coord not in ds.dims]
    )


def _rename_by_long_name(ds: xr.Dataset, suffix: str = "") -> xr.Dataset:
    """Rename every variable after its long name, parentheses stripped."""
    renames = {
        var: long_name_slug(str(ds[var].attrs.get("long_name", var)), strip_parens=True) + suffix
        for var in ds.data_vars
    }
    return ds.rename(resolve_renames(ds, renames))


def _split_by_height(
    ds: xr.Dataset,
    height_coord: str,
    label: str,
    rename_scalar: bool = True,
) -> xr.Dataset:
    """Fold a near-surface height axis into the variable names.

    A stack such as temperature at 2 m and 10 m becomes two variables, because the store
    keeps one flat set of near-surface fields rather than a height dimension that only a
    handful of variables use.

    Args:
        ds: Sub-dataset carrying the height axis.
        height_coord: Name of the height coordinate to fold in.
        label: Infix used in the new names, e.g. ``height_above_ground``.
        rename_scalar: Whether a single height should also be folded into the name. The
            surface pressure fields in this archive were written without the suffix, so
            adding one now would not line up with the existing store.
    """
    heights = ds.coords[height_coord].values
    if heights.ndim == 0:
        if rename_scalar:
            ds = ds.rename({var: f"{var}_at_{label}_{heights}" for var in ds.data_vars})
        return ds.drop_vars(height_coord)

    for var in list(ds.data_vars):
        for height in heights:
            ds[f"{var}_at_{label}_{height}"] = ds[var].sel({height_coord: height})
        ds = ds.drop_vars(var)
    return ds.drop_vars(height_coord)


def process_surface_file(path: str) -> xr.Dataset:
    """Turn one HARMONIE ``SF`` file into a single merged surface dataset."""
    cleaned: list[xr.Dataset] = []
    for sub_ds in open_grib_datasets(str(path)):
        if "h" in sub_ds.data_vars:
            name = next(
                (new for coord, new in _HEIGHT_BY_LEVEL_TYPE.items() if coord in sub_ds.coords),
                "height",
            )
            cleaned.append(sub_ds.rename({"h": name}))
        elif "unknown" in sub_ds.data_vars:
            continue
        elif "pres" in sub_ds.data_vars:
            if "heightAboveSea" in sub_ds.coords:
                cleaned.append(sub_ds.rename({"pres": "pressure_at_sea_level"}))
            else:
                cleaned.append(
                    _split_by_height(
                        sub_ds, "heightAboveGround", "height_above_ground", rename_scalar=False
                    )
                )
        elif any(v in sub_ds.data_vars for v in _MULTI_LEVEL_SHORT_NAMES):
            if "heightAboveGround" in sub_ds.coords:
                cleaned.append(
                    _split_by_height(sub_ds, "heightAboveGround", "height_above_ground")
                )
            elif "heightAboveSea" in sub_ds.coords:
                cleaned.append(_split_by_height(sub_ds, "heightAboveSea", "height_above_sea"))
            elif "hybrid" in sub_ds.coords:
                # Model levels belong in the ML store, not here.
                continue
            else:
                sub_ds = sub_ds.drop_vars(
                    ["heightAboveSea", "heightAboveGround", "hybrid"], errors="ignore"
                )
                cleaned.append(_rename_by_long_name(sub_ds))
        else:
            cleaned.append(_drop_scalar_level_coords(_rename_by_long_name(sub_ds)))

    return xr.merge(cleaned, compat="no_conflicts")


def process_pressure_level_file(path: str) -> xr.Dataset:
    """Turn one HARMONIE ``PL`` file into a single merged dataset."""
    ds = xr.merge(open_grib_datasets(str(path)), compat="no_conflicts")
    # Near-surface fields already name their height, so only the rest take the suffix.
    renames = {
        var: long_name_slug(str(ds[var].attrs.get("long_name", var)), strip_parens=True)
        + "_at_surface"
        for var in ds.data_vars
        if "2m" not in str(var) and "10m" not in str(var)
    }
    return ds.rename(resolve_renames(ds, renames))


class _DMIHarmonieBase(BaseProvider):
    """Shared download and assembly for the HARMONIE stores.

    Subclasses pick the GRIB level types they need through :attr:`level_types`; the files
    are fetched step-major then level-type-minor and handed to :meth:`process_step` in
    that order.
    """

    append_dim = "time"
    #: GRIB level types making up one forecast step, e.g. ``("PL", "SF")``.
    level_types: tuple[str, ...] = ()
    #: Forecast hours retrieved for each init time.
    forecast_steps: tuple[int, ...] = (0, 1, 2)
    #: Chunking applied to the assembled dataset.
    chunks: dict[str, int] = {"time": 1, "step": 1, "y": -1, "x": -1}

    def _filesystem(self):
        import fsspec

        # The DMI bucket is world-readable; signing with whatever credentials happen to be
        # in the environment would fail rather than help.
        return fsspec.filesystem("s3", anon=True)

    def remote_key(self, it: pd.Timestamp, step: int, level_type: str) -> str:
        """Bucket key of one GRIB file, without a scheme."""
        init = it.strftime(TIME_FORMAT)
        valid = (it + pd.Timedelta(step, "h")).strftime(TIME_FORMAT)
        name = f"HARMONIE_IG_{level_type}_{init}_{valid}.grib"
        return f"{BUCKET}/forecastdata/HARMONIE_IG_{level_type}/{name}"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every file for ``it``, or nothing at all.

        DMI keeps only the last few days, so most misses are simply "not published yet" or
        "already expired"; both are reported as nothing to do rather than as failures.
        """
        target = init_time_download_dir(self.config.scratch_dir, self.name, it, temp_dir)
        fs = self._filesystem()

        paths: List[str] = []
        for step in self.forecast_steps:
            for level_type in self.level_types:
                key = self.remote_key(it, step, level_type)
                if not fs.exists(key):
                    logger.warning(f"{self.name}: {key} is not in the archive, skipping {it}")
                    return []
                local = download_with_filesystem(fs, key, target / key.split("/")[-1])
                if local is None:
                    return []
                paths.append(str(local))
        return paths

    def process_step(self, files: List[str]) -> xr.Dataset:
        """Build the dataset for a single forecast step from its level-type files."""
        raise NotImplementedError

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge each forecast step, then stack the steps under one init time."""
        per_type = len(self.level_types)
        expected = per_type * len(self.forecast_steps)
        if len(input_files) != expected:
            raise ValueError(
                f"{self.name}: expected {expected} files for {it}, got {len(input_files)}"
            )

        per_step = [
            self.process_step(input_files[i * per_type : (i + 1) * per_type])
            for i in range(len(self.forecast_steps))
        ]
        ds = xr.concat(per_step, dim="step").sortby("step")
        ds = ds.assign_coords(time=it).expand_dims("time")
        ds = rename_present(ds, {"isobaricInhPa": "level"})
        return chunk_present(ds, self.chunks)


class DMIHarmonieProvider(_DMIHarmonieBase):
    """HARMONIE pressure-level and surface fields."""

    name = "dmi_harmonie"
    store_prefix = "bkr/dmi/harmonie_greenland_iceland_3.icechunk"
    level_types = ("PL", "SF")
    chunks = {"time": 1, "step": 1, "y": -1, "x": -1, "level": -1}

    def process_step(self, files: List[str]) -> xr.Dataset:
        """Merge the pressure-level and surface files of one forecast step."""
        pressure_file, surface_file = files
        merged = xr.merge(
            [process_pressure_level_file(pressure_file), process_surface_file(surface_file)],
            compat="no_conflicts",
        )
        return _drop_scalar_level_coords(merged, keep=("latitude", "longitude", "time", "step"))


class DMIHarmonieModelLevelProvider(_DMIHarmonieBase):
    """HARMONIE native model levels."""

    name = "dmi_harmonie_model_level"
    store_prefix = "bkr/dmi/harmonie_greenland_iceland_model_level.icechunk"
    level_types = ("ML",)
    chunks = {"time": 1, "step": 1, "y": -1, "x": -1, "hybrid": -1}

    def process_step(self, files: List[str]) -> xr.Dataset:
        """Merge the model-level file of one forecast step."""
        # The local renamer rather than common.rename_vars_by_long_name: it goes through
        # resolve_renames, so a long_name shared by two model-level fields cannot take
        # the whole init time down with it.
        return _rename_by_long_name(xr.merge(
            open_grib_datasets(str(files[0])), compat="no_conflicts"
        ))
