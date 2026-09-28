"""ECMWF IFS HRES operational analysis from the NSF NCAR GDEX archive (ds113.1).

One partition is one day. For each day the provider downloads

* the daily surface analysis file for every surface parameter, and
* the 00/06/12/18 UTC pressure-level analysis files for every upper-air parameter,

merges them into a single dataset and appends it along ``time``.

Two flavours of the archive are supported and differ only in the file suffix requested:

``nc``
    NetCDF files (``engine="h5netcdf"``). This is what the public
    ``bkr/ifs/hres_analysis.icechunk`` store on source.coop was built from.
``grb``
    GRIB files (``engine="cfgrib"``), which can additionally be regridded to a regular
    lat/lon grid with Metview before being read. The regridded output normally goes to a
    different bucket, so :class:`IFSAnalysisProvider` accepts ``bucket``/``region``
    overrides that are layered on top of :func:`~planetary_datasets.config.get_config`.

This replaces the standalone ``ifs.py``, ``ifs_anay.py`` and ``pb/ifs_analysis.py``
scripts.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib
import re
from typing import Iterable, List

import numpy as np
import pandas as pd
import requests
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import (
    make_lat_lon_coords_consistent,
    rename_vars_by_long_name,
    sort_vertical_coords,
)
from planetary_datasets.common.download import cleanup_files, download_one
from planetary_datasets.config import Config

GDEX_BASE_URL = "https://data.gdex.ucar.edu/d113001"

#: Analysis hours available on the pressure-level archive.
ANALYSIS_HOURS = ("00", "06", "12", "18")

#: (GRIB parameter number, shortname) for the pressure-level analysis.
ATMOSPHERE_VAR_CODES: tuple[tuple[str, str], ...] = (
    ("129", "z"),
    ("130", "t"),
    ("131", "u"),
    ("132", "v"),
    ("133", "q"),
    ("135", "w"),
    ("157", "r"),
)

#: (GRIB parameter number, shortname) for the surface analysis.
SURFACE_VAR_CODES: tuple[tuple[str, str], ...] = (
    ("031", "ci"),
    ("032", "asn"),
    ("033", "rsn"),
    ("034", "sstk"),
    ("035", "istl1"),
    ("036", "istl2"),
    ("037", "istl3"),
    ("038", "istl4"),
    ("039", "swvl1"),
    ("040", "swvl2"),
    ("041", "swvl3"),
    ("042", "swvl4"),
    ("043", "slt"),
    ("066", "lailv"),
    ("067", "laihv"),
    ("074", "sdfor"),
    ("129", "z"),
    ("134", "sp"),
    ("136", "tcw"),
    ("137", "tcwv"),
    ("139", "stl1"),
    ("141", "sd"),
    ("148", "chnk"),
    ("151", "msl"),
    ("160", "sdor"),
    ("161", "isor"),
    ("162", "anor"),
    ("163", "slor"),
    ("164", "tcc"),
    ("165", "10u"),
    ("166", "10v"),
    ("167", "2t"),
    ("168", "2d"),
    ("170", "stl2"),
    ("173", "sr"),
    ("174", "al"),
    ("183", "stl3"),
    ("186", "lcc"),
    ("187", "mcc"),
    ("188", "hcc"),
    ("198", "src"),
    ("206", "tco3"),
    ("234", "lsrh"),
    ("235", "skt"),
    ("236", "stl4"),
    ("238", "tsn"),
    ("246", "100u"),
    ("247", "100v"),
)

#: Variables kept at full precision: float16 cannot represent their range or the small
#: relative differences that matter for them.
KEEP_FLOAT32_VARS = frozenset(
    {
        "specific_humidity",
        "geopotential",
        "geopotential_sfc",
        "surface_pressure",
        "surface_pressure_sfc",
        "mean_sea_level_pressure",
        "mean_sea_level_pressure_sfc",
        "ozone_mass_mixing_ratio",
    }
)

#: Time-invariant fields. They are dropped rather than stored once per timestep.
STATIC_VARS = frozenset(
    {
        "low_vegetation_cover",
        "high_vegetation_cover",
        "type_of_low_vegetation",
        "type_of_high_vegetation",
        "soil_type",
        "standard_deviation_of_filtered_subgrid_orography",
        "geopotential_at_the_surface",
        "standard_deviation_of_orography",
        "anisotropy_of_sub-gridscale_orography",
        "angle_of_sub-gridscale_orography",
        "slope_of_sub-gridscale_orography",
        "land-sea_mask",
        "surface_roughness",
        "logarithm_of_surface_roughness_length_for_heat",
    }
)

#: Fields that are not worth the storage for the downstream forecasting use case.
DROP_VARS = frozenset(
    {
        "ozone_mass_mixing_ratio",
        "divergence",
        "vorticity_relative",
        "potential_vorticity",
        "charnock",
        "uv_visible_albedo_for_direct_radiation",
        "uv_visible_albedo_for_diffuse_radiation",
        "near_ir_albedo_for_direct_radiation",
        "near_ir_albedo_for_diffuse_radiation",
        "leaf_area_index,_high_vegetation",
        "leaf_area_index,_low_vegetation",
    }
)

#: Bookkeeping variables the NetCDF files carry that have no place in the store. Dropped
#: before the long-name rename, so these are the raw names.
_METADATA_VARS = ("utc_date", "quantization_info")

_SURFACE_MARKER = "ec.oper.an.sfc"
_ATMOSPHERE_MARKER = "ec.oper.an.pl"
#: ``...regn1280sc.2026010100.grb`` -> ``2026010100``. Works for regridded names too,
#: which have a ``.regrid_<grid>`` segment inserted before the suffix.
_ANALYSIS_STAMP = re.compile(r"(?<!\d)(\d{10})(?!\d)")


def surface_url(time: pd.Timestamp, var_code: str, var: str, suffix: str = "nc") -> str:
    """URL of the daily surface analysis file for one parameter."""
    # 100m winds live in GRIB table 228, everything else in table 128.
    table = "228" if var in ("100u", "100v") else "128"
    return (
        f"{GDEX_BASE_URL}/ec.oper.an.sfc/{time:%Y%m}/"
        f"ec.oper.an.sfc.{table}_{var_code}_{var}.regn1280sc.{time:%Y%m%d}.{suffix}"
    )


def atmosphere_url(
    time: pd.Timestamp, hour: str, var_code: str, var: str, suffix: str = "nc"
) -> str:
    """URL of the pressure-level analysis file for one parameter and analysis hour."""
    # Wind components are archived on the vector ("uv") reduced Gaussian grid.
    grid_type = "uv" if var in ("u", "v") else "sc"
    return (
        f"{GDEX_BASE_URL}/ec.oper.an.pl/{time:%Y%m}/"
        f"ec.oper.an.pl.128_{var_code}_{var}.regn1280{grid_type}.{time:%Y%m%d}{hour}.{suffix}"
    )


def regrid_grib(input_path: str | os.PathLike, grid: Iterable[float]) -> pathlib.Path:
    """Interpolate a GRIB file onto a regular lat/lon grid with Metview.

    Metview is imported lazily: it is a heavyweight optional dependency and only the
    regridded flavour of this dataset needs it.
    """
    import metview as mv

    grid = list(grid)
    input_path = pathlib.Path(input_path)
    output_path = _regridded_path(input_path, grid)
    if output_path.is_file() and output_path.stat().st_size > 0:
        return output_path
    mv.write(str(output_path), mv.regrid(data=mv.read(str(input_path)), grid=grid))
    return output_path


def _regridded_path(path: pathlib.Path, grid: Iterable[float]) -> pathlib.Path:
    # Every component of the grid goes into the name: an anisotropic target such as
    # [1.0, 0.5] must not collide with [1.0, 1.0] in the skip-if-present check.
    tag = "_".join(str(g) for g in grid)
    return path.with_name(f"{path.stem}.regrid_{tag}{path.suffix}")


def url_is_absent(url: str, timeout: float = 60.0) -> bool:
    """True when the archive answers 404 for ``url``.

    Used to tell "this day was never archived", which is a skip, from "the download
    failed", which is worth retrying. A HEAD that itself fails is reported as present so
    the caller treats the situation as an error rather than a gap.
    """
    try:
        return requests.head(url, allow_redirects=True, timeout=timeout).status_code == 404
    except requests.RequestException as exc:
        logger.debug(f"could not probe {url}: {exc}")
        return False


def _analysis_stamp(path: str | os.PathLike) -> str:
    """Return the ``YYYYMMDDHH`` stamp embedded in a pressure-level filename."""
    match = _ANALYSIS_STAMP.search(pathlib.Path(path).name)
    if match is None:
        raise ValueError(f"no analysis timestamp found in {path}")
    return match.group(1)


def _with_suffix(names: Iterable[str], suffix: str) -> set[str]:
    """The given names, plus their ``suffix``-ed spellings when a suffix is in use."""
    names = set(names)
    if not suffix:
        return names
    return names | {f"{name}{suffix}" for name in names}


def to_float16_except(ds: xr.Dataset, keep: Iterable[str] = ()) -> xr.Dataset:
    """Downcast every data variable to float16 except the named ones.

    ``common.dataset.reduce_precision`` selects variables to downcast by substring hint;
    here the selection is an exclusion list, which that helper cannot express.
    """
    keep = set(keep)
    out = ds.copy()
    for var in out.data_vars:
        if str(var) in keep:
            continue
        out[var] = out[var].astype(np.float16)
    return out


def merge_surface_and_atmosphere(
    surface_ds: xr.Dataset,
    atmos_ds: xr.Dataset,
    surface_suffix: str = "",
    float16: bool = True,
) -> xr.Dataset:
    """Merge the surface and pressure-level analyses into one store-ready dataset.

    Variables are renamed from their GRIB shortname to a slugified ``long_name`` so the
    store is self-describing. Surface names get ``surface_suffix`` appended, which the
    GRIB flavour needs because cfgrib reports the same ``long_name`` for a field at the
    surface and on pressure levels.
    """
    surface_ds = surface_ds.drop_vars(_METADATA_VARS, errors="ignore")
    atmos_ds = atmos_ds.drop_vars(_METADATA_VARS, errors="ignore")

    surface_ds = rename_vars_by_long_name(surface_ds, suffix=surface_suffix)
    atmos_ds = rename_vars_by_long_name(atmos_ds)

    # The surface archive is hourly and the pressure-level archive 6-hourly, so the time
    # axes genuinely differ; an outer join is what keeps both.
    ds = xr.merge([surface_ds, atmos_ds], join="outer", compat="no_conflicts")

    # The drop and keep lists name variables as they appear without a surface suffix, so
    # the suffixed spellings have to be generated too or they slip through.
    ds = ds.drop_vars(_with_suffix(DROP_VARS | STATIC_VARS, surface_suffix), errors="ignore")

    if float16:
        ds = to_float16_except(ds, keep=_with_suffix(KEEP_FLOAT32_VARS, surface_suffix))

    ds = make_lat_lon_coords_consistent(ds)
    ds = sort_vertical_coords(ds)
    ds = sort_vertical_coords(ds, level_name="isobaricInhPa")

    # cfgrib presents a native reduced Gaussian field as a 1-D ``values`` dimension with
    # latitude and longitude as non-dimension coordinates, so chunk only what is a dim.
    chunks = {
        dim: (1 if dim == "time" else -1)
        for dim in ("time", "level", "isobaricInhPa", "latitude", "longitude", "values")
        if dim in ds.dims
    }
    return ds.chunk(chunks)


class IFSAnalysisProvider(BaseProvider):
    """ECMWF IFS HRES operational analysis, one day per partition."""

    name = "ifs_analysis"
    append_dim = "time"
    store_prefix = "bkr/ifs/hres_analysis.icechunk"

    #: A day of N1280 analysis is far larger than memory, but it is dask-backed and
    #: written chunk by chunk, so the whole-dataset size check does not apply.
    guard_memory = False

    def __init__(
        self,
        config: Config | None = None,
        *,
        store_prefix: str | None = None,
        suffix: str = "nc",
        grid: Iterable[float] | None = None,
        surface_suffix: str | None = None,
        float16: bool = True,
        bucket: str | None = None,
        region: str | None = None,
        hours: Iterable[str] = ANALYSIS_HOURS,
    ):
        """Configure which flavour of the archive this instance reads and where it writes.

        Args:
            config: Override configuration. Defaults to the process-wide config.
            store_prefix: Store location relative to the bucket. Defaults to the class
                attribute.
            suffix: ``nc`` or ``grb``; selects the archive flavour and the reader.
            grid: Target ``[dlat, dlon]`` for Metview regridding. GRIB only; ``None``
                keeps the native reduced Gaussian grid.
            surface_suffix: Appended to surface variable names. Defaults to ``_sfc`` for
                GRIB, where surface and pressure-level long names collide, and to ``""``
                for NetCDF, whose long names already distinguish them.
            float16: Downcast variables outside :data:`KEEP_FLOAT32_VARS`.
            bucket: Alternate S3 bucket for this store, layered over the configured one.
            region: Alternate S3 region, used with ``bucket``.
            hours: Analysis hours to fetch from the pressure-level archive.
        """
        super().__init__(config=config)
        if suffix not in ("nc", "grb"):
            raise ValueError(f"suffix must be 'nc' or 'grb', got {suffix!r}")
        if grid is not None and suffix != "grb":
            raise ValueError("regridding is only supported for the 'grb' flavour")
        if store_prefix is not None:
            self.store_prefix = store_prefix
        self.suffix = suffix
        self.grid = list(grid) if grid is not None else None
        self.surface_suffix = surface_suffix if surface_suffix is not None else (
            "_sfc" if suffix == "grb" else ""
        )
        self.float16 = float16
        self.hours = tuple(hours)
        self._bucket = bucket
        self._region = region

    @property
    def config(self) -> Config:
        """Configuration, with the per-provider bucket/region override applied."""
        cfg = super().config
        if self._bucket is None and self._region is None:
            return cfg
        return dataclasses.replace(
            cfg,
            bucket=self._bucket or cfg.bucket,
            region=self._region or cfg.region,
        )

    @property
    def engine(self) -> str:
        """The xarray backend that reads this flavour's files."""
        return "h5netcdf" if self.suffix == "nc" else "cfgrib"

    def urls(self, it: pd.Timestamp) -> list[str]:
        """Every source URL needed for one day, surface first then pressure levels."""
        urls = [
            surface_url(it, code, var, suffix=self.suffix) for code, var in SURFACE_VAR_CODES
        ]
        for hour in self.hours:
            urls.extend(
                atmosphere_url(it, hour, code, var, suffix=self.suffix)
                for code, var in ATMOSPHERE_VAR_CODES
            )
        return urls

    def fetch(
        self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs
    ) -> List[str]:
        """Download (and optionally regrid) a day of analysis files.

        Returns an empty list when the archive genuinely does not hold a file for this
        day: a partial day would be written as a timestep that can never be completed,
        since the store is append-only. A download that fails for any other reason raises
        instead of quietly yielding nothing, so the caller retries rather than recording
        the partition as done.
        """
        temp_dir = pathlib.Path(temp_dir or self.config.scratch_dir) / f"{it:%Y%m%d}_hres"
        temp_dir.mkdir(parents=True, exist_ok=True)

        paths: list[str] = []
        for url in self.urls(it):
            dest = temp_dir / url.rsplit("/", 1)[-1]
            if self.grid is not None:
                regridded = _regridded_path(dest, self.grid)
                if regridded.is_file() and regridded.stat().st_size > 0:
                    paths.append(str(regridded))
                    continue
            downloaded = download_one(url, dest)
            if downloaded is None:
                if url_is_absent(url):
                    logger.warning(
                        f"{self.name}: {url} is not in the archive, skipping {it:%Y-%m-%d}"
                    )
                    return []
                raise RuntimeError(f"{self.name}: failed to download {url} for {it:%Y-%m-%d}")
            if self.grid is not None:
                regridded = regrid_grib(downloaded, self.grid)
                # The native file can be several GB; a whole day of them plus their
                # regridded copies does not need to sit in scratch at once.
                if regridded != downloaded:
                    cleanup_files(downloaded)
                downloaded = regridded
            paths.append(str(downloaded))
        return paths

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge a day's downloaded files into one dataset, ready to append."""
        surface_files = [f for f in input_files if _SURFACE_MARKER in pathlib.Path(f).name]
        atmosphere_files = [f for f in input_files if _ATMOSPHERE_MARKER in pathlib.Path(f).name]
        if not surface_files or not atmosphere_files:
            raise ValueError(
                f"{self.name}: expected both surface and pressure-level files for {it}, "
                f"got {len(surface_files)} and {len(atmosphere_files)}"
            )

        surface_ds = xr.open_mfdataset(
            surface_files, engine=self.engine, compat="override", chunks={}
        )
        atmos_ds = self._open_atmosphere(atmosphere_files)
        return merge_surface_and_atmosphere(
            surface_ds,
            atmos_ds,
            surface_suffix=self.surface_suffix,
            float16=self.float16,
        )

    def _open_atmosphere(self, atmosphere_files: List[str]) -> xr.Dataset:
        """Open the pressure-level files, merging per analysis hour then concatenating.

        ``open_mfdataset(combine="by_coords")`` cannot be used: cfgrib does not always
        expose a dimension coordinate xarray can order the files by, so the files are
        grouped explicitly by the timestamp in their name.
        """
        by_hour: dict[str, list[str]] = {}
        for path in atmosphere_files:
            by_hour.setdefault(_analysis_stamp(path), []).append(path)

        hour_datasets = []
        for stamp in sorted(by_hour):
            dsets = [
                xr.open_dataset(path, engine=self.engine, chunks={}) for path in by_hour[stamp]
            ]
            hour_datasets.append(xr.merge(dsets, compat="override"))
        if len(hour_datasets) == 1:
            return hour_datasets[0]
        return xr.concat(hour_datasets, dim="time")


class IFSRegriddedAnalysisProvider(IFSAnalysisProvider):
    """IFS HRES analysis interpolated to a regular lat/lon grid.

    The regridded archive lives in its own bucket rather than on source.coop. That bucket
    is read from ``IFS_REGRID_BUCKET``/``IFS_REGRID_REGION`` and falls back to the
    configured default bucket when unset, so nothing about it is hardcoded here.
    """

    name = "ifs_analysis_regrid"
    store_prefix = "ifs_ana/hres_analysis.icechunk"

    def __init__(
        self,
        config: Config | None = None,
        *,
        grid: Iterable[float] = (1.0, 1.0),
        bucket: str | None = None,
        region: str | None = None,
        **kwargs,
    ):
        """Default to GRIB input and to the bucket named by the regrid environment."""
        super().__init__(
            config,
            suffix=kwargs.pop("suffix", "grb"),
            grid=grid,
            bucket=bucket if bucket is not None else os.environ.get("IFS_REGRID_BUCKET"),
            region=region if region is not None else os.environ.get("IFS_REGRID_REGION"),
            **kwargs,
        )
