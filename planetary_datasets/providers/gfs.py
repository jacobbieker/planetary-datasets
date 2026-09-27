"""NCEP GFS deterministic forecasts from the NCAR/NSF GDEX archive.

The National Centers for Environmental Prediction run the deterministic Global Forecast
System four times a day. NCAR's GDEX archive publishes the 0.25 degree GRIB2 output over
OSDF as two complementary datasets that together make one complete forecast:

``d084001`` (``gfs.0p25.*``)
    The main parameter set.
``d084003`` (``gfs.0p25b.*``)
    The "b" parameter set, mostly extra isobaric levels and surface fields.

Both are public, so no credentials are needed to read them. This provider downloads one
init time's worth of both sets, optionally regrids to a coarser lat/lon grid with Metview,
merges them per forecast step with cfgrib, and appends the result to an icechunk store
along ``time``.

This consolidates ``pb/gfs.py``, ``pb/gfs_t.py``, ``pb/gfs_write.py``,
``amdar_download.py`` and the old script-style ``planetary_datasets/providers/gfs.py``.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
from typing import Iterable, List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import (
    make_lat_lon_coords_consistent,
    rename_vars_by_long_name,
)
from planetary_datasets.common.download import download_one
from planetary_datasets.common.store import ALIGNMENT_COORDS, write_to_icechunk
from planetary_datasets.config import Config

#: OSDF director in front of the NCAR GDEX archive.
GDEX_BASE = "https://osdf-director.osg-htc.org/ncar/gdex"

#: GDEX dataset id and filename stem for each half of a GFS forecast.
GFS_MAIN = ("d084001", "gfs.0p25")
GFS_EXTRA = ("d084003", "gfs.0p25b")

#: Forecast steps, in hours, kept for each init time. The archive holds 3-hourly output;
#: the store is 6-hourly out to a day, which is what the original scripts wrote.
DEFAULT_FORECAST_STEPS: tuple[int, ...] = (0, 6, 12, 18, 24)

#: cfgrib splits a GFS GRIB2 file into one dataset per level type. These level types are
#: dropped: they are either diagnostics on exotic vertical coordinates or duplicates of
#: fields already present on a supported level type, and none of them merge cleanly.
EXCLUDED_LEVEL_COORDS = frozenset(
    {
        "boundaryLayerCloudBottom",
        "boundaryLayerCloudLayer",
        "boundaryLayerCloudTop",
        "convectiveCloudBottom",
        "convectiveCloudLayer",
        "convectiveCloudTop",
        "heightAboveGroundLayer",
        "heightAboveSea",
        "highCloudBottom",
        "highCloudLayer",
        "highCloudTop",
        "highestTroposphericFreezing",
        "isothermZero",
        "lowCloudBottom",
        "lowCloudLayer",
        "lowCloudTop",
        "maxWind",
        "middleCloudBottom",
        "middleCloudLayer",
        "middleCloudTop",
        "nominalTop",
        "planetaryBoundaryLayer",
        "potentialVorticity",
        "pressureFromGroundLayer",
        "sigma",
        "sigmaLayer",
        "tropopause",
    }
)

#: Scalar level coordinates left behind after merging; they are not data and break appends.
SCALAR_COORDS_TO_DROP = (
    "atmosphere",
    "atmosphereSingleLayer",
    "heightAboveGround",
    "meanSea",
    "surface",
    "valid_time",
)

#: Height prefixes that cfgrib's long names carry but that the ``_at_Nm`` suffix already
#: encodes, e.g. ``2m_temperature`` on the 2 m level becomes ``temperature_at_2m``.
HEIGHT_PREFIXES = ("2m_", "10m_", "80m_", "100m_")

#: Written chunking. One chunk per init time and forecast step, whole field otherwise.
DEFAULT_CHUNKS = {
    "time": 1,
    "step": 1,
    "level": -1,
    "depth": -1,
    "latitude": -1,
    "longitude": -1,
}


#: Matches the ``.f018.`` forecast-step field of a GDEX GFS filename.
STEP_PATTERN = re.compile(r"\.f(\d{3})\.")


def gfs_url(it: pd.Timestamp, step: int, dataset: tuple[str, str] = GFS_MAIN) -> str:
    """Build the GDEX URL for one init time and forecast step.

    Args:
        it: Forecast init time, one of 00/06/12/18Z.
        step: Forecast step in hours.
        dataset: ``(gdex_id, filename_stem)``, either :data:`GFS_MAIN` or :data:`GFS_EXTRA`.
    """
    gdex_id, stem = dataset
    stamp = it.strftime("%Y%m%d%H")
    return f"{GDEX_BASE}/{gdex_id}/{it.year}/{stamp[:8]}/{stem}.{stamp}.f{step:03d}.grib2"


def gfs_urls(
    it: pd.Timestamp,
    steps: Iterable[int] = DEFAULT_FORECAST_STEPS,
) -> list[tuple[str, str]]:
    """Return the ``(main_url, extra_url)`` pair for every forecast step of an init time."""
    return [(gfs_url(it, s, GFS_MAIN), gfs_url(it, s, GFS_EXTRA)) for s in steps]


def ncep_prepbufr_urls(
    dates: Sequence[pd.Timestamp] | pd.DatetimeIndex,
    cycles: Iterable[int] = (0, 6, 12, 18),
) -> list[str]:
    """Return GDAS PrepBUFR (``d337000``) URLs for the given days.

    The 48-hour-delayed GDAS PrepBUFR files are the source of the AMDAR aircraft
    observations that feed the GFS analysis. Kept here, alongside the rest of the NCEP
    downloads, because it is the same archive and the same URL scheme.
    """
    return [
        f"{GDEX_BASE}/d337000/prep48h/{date.year}/prepbufr.gdas.{date:%Y%m%d}.t{cycle:02d}z.nr.48h"
        for date in dates
        for cycle in cycles
    ]


def regrid_grib(input_path: str | pathlib.Path, degrees: float) -> pathlib.Path | None:
    """Regrid a GRIB file onto a regular ``degrees`` x ``degrees`` lat/lon grid.

    Uses Metview, which is an optional dependency: when it is not installed, or the regrid
    fails, None is returned and the caller should fall back to the native resolution file.
    """
    input_path = pathlib.Path(input_path)
    output_path = input_path.with_name(f"{input_path.stem}.regrid_{degrees}{input_path.suffix}")
    if output_path.is_file() and output_path.stat().st_size > 0:
        return output_path

    try:
        import metview as mv
    except ImportError:
        logger.debug("metview is not installed, keeping GRIB files at native resolution")
        return None

    # Write to a .part file and rename, so a run killed mid-write cannot leave a truncated
    # GRIB behind that the size check above would then hand back for ever.
    part = output_path.with_name(output_path.name + ".part")
    try:
        regridded = mv.regrid(data=mv.read(str(input_path)), grid=[degrees, degrees])
        mv.write(str(part), regridded)
        os.replace(part, output_path)
    except Exception as exc:  # noqa: BLE001 - Metview raises bare exceptions on bad GRIB
        logger.warning(f"could not regrid {input_path.name} to {degrees} deg: {exc}")
        part.unlink(missing_ok=True)
        return None
    return output_path


def _level_suffix(ds: xr.Dataset) -> str:
    """Suffix distinguishing variables that share a name across GRIB level types."""
    if "atmosphereSingleLayer" in ds.coords:
        return "_atmosphere_single_layer"
    if "surface" in ds.coords:
        return "_at_surface"
    if "heightAboveGround" in ds.coords:
        # Formatted rather than interpolated: cfgrib reports the level as float64 or int64
        # depending on the eccodes build, and ``_at_2.0m`` vs ``_at_2m`` would look like a
        # different variable to the store and block every later append.
        return f"_at_{float(ds.coords['heightAboveGround'].values):g}m"
    return ""


def _strip_height_prefixes(ds: xr.Dataset) -> xr.Dataset:
    """Drop leading ``2m_``/``10m_``/``80m_``/``100m_`` from variable names.

    The height is already carried by the ``_at_Nm`` suffix, so the prefix is redundant and
    differs between the two GDEX datasets for the same field. A rename that would collide
    with a variable already present is skipped: the two spellings are the same field, and
    renaming onto an existing name raises rather than merging them.
    """
    renames: dict[str, str] = {}
    for var in ds.data_vars:
        for prefix in HEIGHT_PREFIXES:
            if not str(var).startswith(prefix):
                continue
            stripped = str(var)[len(prefix) :]
            if stripped in ds.data_vars or stripped in renames.values():
                logger.debug(f"keeping {var} as-is, {stripped} already exists")
            else:
                renames[var] = stripped
            break
    return ds.rename(renames) if renames else ds


def filter_datasets(datasets: Iterable[xr.Dataset]) -> list[xr.Dataset]:
    """Keep the mergeable level types from ``cfgrib.open_datasets`` and name their variables.

    Datasets on an excluded vertical coordinate are dropped entirely. The rest have their
    variables renamed from GRIB shortnames to slugified long names plus a level suffix, so
    that e.g. surface and 2 m temperature do not collide when merged.
    """
    kept: list[xr.Dataset] = []
    for ds in datasets:
        if EXCLUDED_LEVEL_COORDS & set(ds.coords):
            continue
        # A heightAboveGround *dimension* means several heights in one dataset, which the
        # single scalar suffix cannot describe; those fields arrive again per-level anyway.
        if "heightAboveGround" in ds.dims:
            continue
        ds = rename_vars_by_long_name(ds, suffix=_level_suffix(ds))
        kept.append(ds.drop_vars("heightAboveGround", errors="ignore"))
    return kept


def _temperature_levels(datasets: Iterable[xr.Dataset]) -> list:
    """Isobaric levels that temperature is available on.

    Temperature is the most completely archived isobaric field, so its levels define the
    vertical grid; other variables are subset to it to keep the merged cube rectangular.
    """
    levels: set = set()
    for ds in datasets:
        if "isobaricInhPa" in ds.coords and any(
            str(v).startswith("temperature") for v in ds.data_vars
        ):
            levels.update(ds.coords["isobaricInhPa"].values.ravel().tolist())
    return sorted(levels)


def group_files_by_step(paths: Iterable[str | pathlib.Path]) -> dict[int, dict[str, str]]:
    """Group downloaded GRIB paths into ``{step: {"main": path, "extra": path}}``.

    The two GDEX datasets are told apart by their filename stem and the forecast step is
    read from the ``.fNNN.`` field, so the grouping survives the ``.regrid_N.N`` infix that
    :func:`regrid_grib` adds.
    """
    grouped: dict[int, dict[str, str]] = {}
    for path in paths:
        name = pathlib.Path(path).name
        match = STEP_PATTERN.search(name)
        if match is None:
            raise ValueError(f"cannot read a forecast step from {name!r}")
        key = "extra" if name.startswith(f"{GFS_EXTRA[1]}.") else "main"
        grouped.setdefault(int(match.group(1)), {})[key] = str(path)
    return grouped


def merge_step(main_path: str | pathlib.Path, extra_path: str | pathlib.Path) -> xr.Dataset:
    """Merge the two GDEX files for one forecast step into a single dataset."""
    import cfgrib

    datasets = filter_datasets(cfgrib.open_datasets(str(main_path)))
    datasets += filter_datasets(cfgrib.open_datasets(str(extra_path)))
    if not datasets:
        raise ValueError(f"no usable GRIB messages in {main_path} / {extra_path}")

    levels = _temperature_levels(datasets)
    ds = xr.merge(datasets, combine_attrs="drop_conflicts")

    renames = {
        old: new
        for old, new in (("depthBelowLandLayer", "depth"), ("isobaricInhPa", "level"))
        if old in ds.coords
    }
    ds = ds.rename(renames).drop_vars(SCALAR_COORDS_TO_DROP, errors="ignore")
    if levels and "level" in ds.coords:
        ds = ds.sel(level=levels)
    return _strip_height_prefixes(ds)


class GFSProvider(BaseProvider):
    """NCEP GFS deterministic forecasts, 6-hourly out to 24 hours, on a 1 degree grid."""

    name = "gfs"
    append_dim = "time"
    store_prefix = "bkr/gfs/gfs_forecast_6hr_1deg.icechunk"

    def __init__(
        self,
        config: Config | None = None,
        forecast_steps: Iterable[int] = DEFAULT_FORECAST_STEPS,
        regrid_degrees: float | None = 1.0,
        retries: int = 10,
        workers: int = 8,
    ):
        """Build a GFS provider.

        Args:
            config: Configuration override; defaults to the process-wide config.
            forecast_steps: Forecast steps in hours to include in each init time.
            regrid_degrees: Target grid spacing in degrees, or None to keep the native
                0.25 degree grid. Requires Metview; ignored if it is not installed.
            retries: Download attempts per file before giving up on the init time.
            workers: Concurrent downloads.
        """
        super().__init__(config=config)
        self.forecast_steps = tuple(forecast_steps)
        self.regrid_degrees = regrid_degrees
        self.retries = retries
        self.workers = workers

    def download_dir(self, temp_dir: pathlib.Path | None = None) -> pathlib.Path:
        """Where GRIB files are staged. Uses the partition temp dir when one is given."""
        return temp_dir if temp_dir is not None else self.config.data_dir / "gfs"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download both GDEX files for every forecast step of ``it``.

        Returns an empty list when any file of the init time is unavailable. A partial init
        time is never written: the store would then hold a forecast with holes in it that a
        later run has no way to fill, because the init time already looks done.
        """
        from concurrent.futures import ThreadPoolExecutor

        dest_dir = self.download_dir(temp_dir)
        urls = [url for pair in gfs_urls(it, self.forecast_steps) for url in pair]

        def grab(url: str) -> pathlib.Path | None:
            return download_one(url, dest_dir / url.rsplit("/", 1)[-1], retries=self.retries)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            downloaded = list(pool.map(grab, urls))

        missing = [url for url, path in zip(urls, downloaded) if path is None]
        if missing:
            logger.warning(
                f"{self.name}: {len(missing)}/{len(urls)} file(s) unavailable for {it}, "
                f"skipping this init time (first: {missing[0]})"
            )
            return []

        return self._regrid_all(downloaded)

    def _regrid_all(self, paths: Sequence[pathlib.Path]) -> List[str]:
        """Regrid every file of an init time, or none of them.

        Regridding has to be all-or-nothing: mixing a 1 degree file with a 0.25 degree one
        inside the same init time makes ``xr.merge`` produce a union grid that is almost
        entirely NaN, and nothing downstream would notice.
        """
        if self.regrid_degrees is None:
            return [str(p) for p in paths]

        regridded = [regrid_grib(path, self.regrid_degrees) for path in paths]
        if any(p is None for p in regridded):
            logger.warning(
                f"{self.name}: could not regrid every file to {self.regrid_degrees} deg, "
                "falling back to the native grid for this init time"
            )
            return [str(p) for p in paths]
        return [str(p) for p in regridded]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge every forecast step into one dataset with ``time`` and ``step`` dims."""
        grouped = group_files_by_step(input_files)
        incomplete = sorted(step for step, files in grouped.items() if len(files) != 2)
        if incomplete:
            raise ValueError(f"forecast step(s) {incomplete} for {it} are missing a GDEX file")
        if set(grouped) != set(self.forecast_steps):
            raise ValueError(
                f"expected forecast steps {sorted(self.forecast_steps)} for {it}, "
                f"got {sorted(grouped)}"
            )

        per_step = [
            merge_step(grouped[step]["main"], grouped[step]["extra"])
            for step in sorted(grouped)
        ]
        ds = xr.concat(per_step, dim="step").sortby("step")
        if "level" in ds.coords:
            ds = ds.sortby("level")
        ds = make_lat_lon_coords_consistent(ds)
        ds = ds.expand_dims(self.append_dim).assign_coords(
            {self.append_dim: pd.DatetimeIndex([it])}
        )
        return ds.chunk(self._chunks(ds))

    @staticmethod
    def _chunks(ds: xr.Dataset) -> dict:
        return {dim: size for dim, size in DEFAULT_CHUNKS.items() if dim in ds.dims}

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Line the dataset up with what is already stored, then append.

        GFS occasionally publishes an init time with extra isobaric levels or an extra
        soil layer. Subsetting to the store's existing coordinates keeps those init times
        usable instead of having the append rejected outright.

        ``step`` and ``depth`` join the default alignment coordinates so that a forecast
        that could not be lined up is refused before the append rather than part-way
        through it.
        """
        return write_to_icechunk(
            repo,
            self._align_with_store(repo, processed),
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=(*ALIGNMENT_COORDS, "step", "depth"),
        )

    def _align_with_store(self, repo, ds: xr.Dataset) -> xr.Dataset:
        """Subset ``ds`` to the coordinates and variables the store already holds.

        Returns the dataset unchanged when it cannot be lined up; the base class then
        refuses the write rather than corrupting the store.
        """
        import icechunk

        try:
            existing = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        except (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError):
            return ds
        if self.append_dim not in existing.coords:
            return ds

        absent = set(existing.data_vars) - set(ds.data_vars)
        if absent:
            logger.warning(f"{self.name}: incoming data is missing {sorted(absent)}")
            return ds
        ds = ds[list(existing.data_vars)]

        # ``step`` is included because forecast_steps is a constructor argument: running
        # with a shorter step list against an existing store would otherwise only fail
        # deep inside the append, with the store half-written.
        for coord in ("step", "level", "depth"):
            if coord not in existing.coords or coord not in ds.coords:
                continue
            wanted = existing[coord].values
            if set(wanted.ravel().tolist()) - set(ds[coord].values.ravel().tolist()):
                logger.warning(f"{self.name}: incoming data is missing {coord} values")
                return ds
            ds = ds.sel({coord: wanted})

        return ds.chunk(self._chunks(ds))


def main(argv: Sequence[str] | None = None) -> int:
    """Backfill a range of GFS init times into the configured store."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default="2016-01-01", help="First init time, inclusive.")
    parser.add_argument("--end", default="2026-01-01", help="Last init time, inclusive.")
    parser.add_argument("--freq", default="6h", help="Init time frequency, e.g. 6h.")
    parser.add_argument(
        "--regrid-degrees",
        type=float,
        default=1.0,
        help="Target grid spacing in degrees. Use 0 to keep the native 0.25 degree grid.",
    )
    args = parser.parse_args(argv)

    provider = GFSProvider(regrid_degrees=args.regrid_degrees or None)
    written = provider.run_range(pd.date_range(args.start, args.end, freq=args.freq))
    logger.info(f"wrote {written} init time(s) to {provider.store_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
