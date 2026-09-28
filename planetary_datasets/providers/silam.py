"""SILAM dust and surface aerosol forecasts from the Finnish Meteorological Institute.

Two products, both global forecasts on a regular lat/lon grid and both stored as
``(init_time, step, latitude, longitude)``:

``SILAMDustProvider``
    The 10 km global dust forecast, one NetCDF4 file per hourly forecast step, served from
    the FMI THREDDS server.
``SILAMAerosolProvider``
    The surface aerosol and trace gas forecast (PM2.5, PM10, O3, NO, NO2, SO2, CO,
    air density), published on the public ``fmi-opendata-silam-surface-netcdf`` S3 bucket
    as one file per species per forecast day.

Both replace a pile of one-off scripts that hardcoded credentials, absolute paths and a
locally rsynced mirror of the FMI bucket. Stores are resolved through
:class:`~planetary_datasets.config.Config`, so the same code writes to source.coop in
production and to a directory under ``ICECHUNK_LOCAL_PATH`` in tests.
"""

from __future__ import annotations

import contextlib
import pathlib
import tempfile
from typing import Iterator, List

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_many
from planetary_datasets.common.store import ALIGNMENT_COORDS, has_timestep
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.config import Config

DUST_THREDDS_BASE = "https://thredds.silam.fmi.fi/thredds/fileServer"
DUST_VERSION = "v5_7_2"
DUST_MAX_STEP = 120

AEROSOL_BUCKET = "fmi-opendata-silam-surface-netcdf"
AEROSOL_REGION = "eu-west-1"
AEROSOL_AREA = "global"
AEROSOL_FORECAST_DAYS = 5

#: ``step`` has to line up as well as the spatial coordinates, otherwise appending a
#: forecast with a different step count silently corrupts the store.
FORECAST_ALIGNMENT_COORDS = (*ALIGNMENT_COORDS, "step")

#: Units whose values are safely representable in float16. Concentrations in µg/m³ run
#: from ~1e-3 to ~1e5; the same field published in kg/m³ would be ~1e-8 and would round to
#: zero, so anything not listed here keeps its source precision.
FLOAT16_SAFE_UNITS = ("ug/m3", "µg/m3", "microgram")


class IncompleteForecast(RuntimeError):
    """Raised when only part of a forecast run is available.

    Writing a short forecast would put a truncated ``step`` axis in the store that can
    never be appended to again, so the partition is failed instead. Failing is also what
    makes Dagster retry it: returning "nothing to do" would mark the partition done.
    """


def _to_step_dim(ds: xr.Dataset, init_time: pd.Timestamp) -> xr.Dataset:
    """Turn a valid-time axis into ``(init_time, step)``.

    ``init_time`` is taken from the partition rather than from the data. The first valid
    time in a SILAM file is the run time plus one hour, so deriving it from the data — as
    the original scripts did — shifted every forecast an hour and broke the
    "is this partition already stored?" check.
    """
    init = np.datetime64(pd.Timestamp(init_time))
    ds = ds.assign_coords(step=("time", ds["time"].values - init))
    ds = ds.swap_dims({"time": "step"}).drop_vars("time")
    ds = ds.assign_coords(init_time=pd.Timestamp(init_time)).expand_dims("init_time")
    return ds


class _SILAMForecastProvider(BaseProvider):
    """Shared behaviour for the SILAM forecast products."""

    append_dim = "init_time"

    @contextlib.contextmanager
    def local_tempdir(self) -> Iterator[pathlib.Path]:
        """Stage downloads under the configured scratch directory.

        A dust run is ~20 GB of NetCDF; the default temporary directory is usually on the
        root volume, which is not where that belongs. ``PLANETARY_DATASETS_SCRATCH_DIR``
        points it at the big disk.
        """
        scratch = pathlib.Path(self.config.scratch_dir)
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=f"{self.name}-", dir=scratch) as td:
            yield pathlib.Path(td)

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        written = _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=FORECAST_ALIGNMENT_COORDS,
        )
        if not written and not has_timestep(
            repo, processed[self.append_dim].values[0], append_dim=self.append_dim
        ):
            # run_partition only gets here for a timestep the store does not have, so a
            # refused write means the forecast did not line up. Raise rather than report
            # success, which would mark the partition done and never retry it.
            raise IncompleteForecast(
                f"{self.name}: store refused {processed[self.append_dim].values[0]}; "
                "the variables or the step axis do not match what is already there"
            )
        return written


class SILAMDustProvider(_SILAMForecastProvider):
    """SILAM global dust forecast from the FMI THREDDS server.

    One NetCDF4 file per hourly forecast step is downloaded, then concatenated along
    ``step``. A full 120 step forecast is roughly 20 GB on disk and far larger in memory,
    so the concatenated dataset is kept lazy and written chunk by chunk.
    """

    name = "silam_dust"
    store_prefix = "bkr/silam-dust/silam_global_dust.icechunk"
    # ``require_dataset_fits`` sizes the dataset from nbytes, which for the lazy 120 step
    # concat is hundreds of GB and would reject a job that comfortably fits: dask streams
    # it a step at a time into icechunk, peaking at a few GB. Guarding the write instead
    # is not an option — ``memory_guard`` raises on block exit, so a breach would fail a
    # run whose data had already been committed.
    guard_memory = False

    def __init__(
        self,
        config: Config | None = None,
        version: str = DUST_VERSION,
        max_step: int = DUST_MAX_STEP,
        workers: int = 4,
    ):
        super().__init__(config=config)
        self.version = version
        self.max_step = max_step
        self.workers = workers

    def urls_for(self, it: pd.Timestamp) -> List[str]:
        """URLs of every forecast step for the run initialised on ``it``."""
        run = pd.Timestamp(it).strftime("%Y%m%d%H")
        return [
            f"{DUST_THREDDS_BASE}/dust_glob01_{self.version}/files/"
            f"SILAM-dust-glob01_{self.version}_{run}_{step:03d}.nc4"
            for step in range(1, self.max_step + 1)
        ]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        dest = pathlib.Path(temp_dir or self.config.scratch_dir) / f"silam_dust_{it:%Y%m%d%H}"
        urls = self.urls_for(it)
        paths = download_many(urls, dest, workers=self.workers)
        if not paths:
            logger.info(f"{self.name}: nothing published for {it:%Y-%m-%d}")
            return []
        if len(paths) < len(urls):
            raise IncompleteForecast(
                f"{self.name}: only {len(paths)}/{len(urls)} steps downloaded for {it}"
            )
        return [str(p) for p in paths]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        steps = [_open_dust_step(path, it) for path in sorted(input_files)]
        data = xr.concat(steps, dim="step", combine_attrs="override").sortby("step")
        return data.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1})


def _open_dust_step(path: str, init_time: pd.Timestamp) -> xr.Dataset:
    """Open one dust forecast step and reshape it to ``(init_time, step, lat, lon)``."""
    data = xr.open_dataset(path, chunks={})
    data = data.rename({"lat": "latitude", "lon": "longitude"})
    # The hybrid level machinery describes a single surface level; the coefficient
    # variables differ between SILAM versions, hence errors="ignore".
    data = data.drop_dims("hybrid_half", errors="ignore")
    data = data.drop_vars(["a", "b", "da", "db"], errors="ignore")
    if "hybrid" in data.dims:
        data = data.isel(hybrid=0, drop=True)
    data = data.drop_vars("hybrid", errors="ignore")
    return _to_step_dim(data, init_time)


class SILAMAerosolProvider(_SILAMForecastProvider):
    """SILAM global surface aerosol forecast from the FMI open data S3 bucket.

    The bucket holds one file per species per forecast day (``..._PM25_d0.nc`` through
    ``..._airdens_d4.nc``), which ``open_mfdataset`` merges across species and
    concatenates across days into a single 120 hour forecast. Concentration fields are
    downcast to float16 — halving the store size is what made the archive affordable —
    but only where the units say the values fit; see :data:`FLOAT16_SAFE_UNITS`.

    Only about a month of runs is retained upstream, so backfills beyond that window find
    nothing and the partition is skipped. A run that is published but incomplete is
    failed rather than skipped, so it is retried once the rest lands.
    """

    name = "silam_aerosol"
    store_prefix = "bkr/silam-aerosol/silam_global_surface_aerosol.icechunk"

    def __init__(
        self,
        config: Config | None = None,
        bucket: str = AEROSOL_BUCKET,
        region: str = AEROSOL_REGION,
        area: str = AEROSOL_AREA,
        forecast_days: int = AEROSOL_FORECAST_DAYS,
        workers: int = 8,
        dtype: str = "float16",
        species: tuple[str, ...] | None = None,
    ):
        super().__init__(config=config)
        self.bucket = bucket
        self.region = region
        self.area = area
        self.forecast_days = forecast_days
        self.workers = workers
        self.dtype = dtype
        #: Restrict to a subset of species. ``None`` means everything published.
        self.species = species

    @property
    def _endpoint(self) -> str:
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com"

    def list_keys(self, it: pd.Timestamp) -> List[str]:
        """Object keys for the run initialised on ``it``.

        An absent day returns an empty list. Anything else — a bad region, an S3 outage —
        propagates, because "the listing failed" and "there is no such day" must not lead
        to the same outcome.
        """
        import fsspec

        prefix = f"{self.bucket}/{self.area}/{pd.Timestamp(it):%Y%m%d}"
        fs = fsspec.filesystem("s3", anon=True)
        keys = [k.split(f"{self.bucket}/", 1)[-1] for k in fs.glob(f"{prefix}/silam_glob_*_d*.nc")]
        if self.species is not None:
            wanted = {s.lower() for s in self.species}
            keys = [k for k in keys if _species_of(k).lower() in wanted]
        return sorted(keys)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        keys = self.list_keys(it)
        if not keys:
            logger.info(f"{self.name}: nothing published for {it:%Y-%m-%d}")
            return []

        by_species: dict[str, list[str]] = {}
        for key in keys:
            by_species.setdefault(_species_of(key), []).append(key)
        short = {s: len(k) for s, k in by_species.items() if len(k) != self.forecast_days}
        if short:
            raise IncompleteForecast(
                f"{self.name}: {it:%Y-%m-%d} is still being published, "
                f"expected {self.forecast_days} days per species but found {short}"
            )

        dest = pathlib.Path(temp_dir or self.config.scratch_dir) / f"silam_aerosol_{it:%Y%m%d}"
        urls = [f"{self._endpoint}/{key}" for key in keys]
        paths = download_many(urls, dest, workers=self.workers)
        if len(paths) < len(urls):
            raise IncompleteForecast(
                f"{self.name}: only {len(paths)}/{len(urls)} files downloaded for {it}"
            )
        return [str(p) for p in paths]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        data = xr.open_mfdataset(sorted(input_files), combine="by_coords", chunks={})
        data = data.rename({"lat": "latitude", "lon": "longitude"}).sortby("time")
        data = _to_step_dim(data, it)
        data = _downcast_safe_vars(data, self.dtype)
        # One lead time per chunk, matching the dust store. Keeping the whole forecast in
        # a single chunk, as the original script did, made every read of one hour pull
        # 400 MB.
        return data.chunk({"init_time": 1, "step": 1, "latitude": -1, "longitude": -1})


def _species_of(key: str) -> str:
    """Species name embedded in an aerosol object key, e.g. ``PM25`` in ``..._PM25_d0.nc``."""
    return key.rsplit("/", 1)[-1].removesuffix(".nc").rsplit("_", 2)[-2]


def _downcast_safe_vars(ds: xr.Dataset, dtype: str) -> xr.Dataset:
    """Downcast the variables whose units say the values survive ``dtype``.

    ``common.reduce_precision`` matches variable names by substring, which here would
    make the hint ``NO`` also catch ``NO2``, so the decision is made per variable on the
    declared units instead. Coordinates are never touched: ``init_time`` and ``step``
    have to keep their datetime and timedelta dtypes.
    """
    out = ds.copy()
    skipped = []
    for name, var in ds.data_vars.items():
        units = str(var.attrs.get("units", "")).lower().replace(" ", "")
        if var.dtype.kind == "f" and any(u in units for u in FLOAT16_SAFE_UNITS):
            out[name] = var.astype(dtype)
        else:
            skipped.append(str(name))
    if skipped:
        logger.debug(f"keeping source precision for {sorted(skipped)}: units are not µg/m³")
    return out
