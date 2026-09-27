"""GloFAS river discharge from the Copernicus Emergency Management Service.

GloFAS (the Global Flood Awareness System) runs the LISFLOOD hydrological model on a global
0.05-degree river network and publishes three products through the Copernicus Climate Data
Store. They share a request shape and a download path but are genuinely different datasets,
so each gets its own provider and its own store:

``cems-glofas-forecast``
    The operational ensemble forecast, issued daily out to seven days.
    :class:`GloFASForecastProvider`.
``cems-glofas-reforecast``
    Retrospective forecasts from the frozen version 4.0 model, issued twice a week over the
    hindcast period. Used to calibrate and verify the operational system.
    :class:`GloFASReforecastProvider`.
``cems-glofas-historical``
    The consolidated reanalysis: a single simulated discharge history with no forecast
    dimension. :class:`GloFASHistoricalProvider`.

Everything common -- building the CDS client, retrieving atomically, unpacking the zip CDS
hands back and normalising the NetCDF inside it -- lives on :class:`GloFASProvider`.

Credentials come from ``CDSAPI_URL``/``CDSAPI_KEY``, falling back to ``~/.cdsapirc`` when
those are unset, which is how the CDS client is normally configured.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import zipfile
from typing import Any, Iterable, List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import make_lat_lon_coords_consistent
from planetary_datasets.common.store import missing_periods
from planetary_datasets.config import Config, MissingCredential, get_config

#: The two GloFAS surface variables the archive carries.
DEFAULT_VARIABLES: tuple[str, ...] = (
    "river_discharge_in_the_last_24_hours",
    "runoff_water_equivalent",
)

#: Forecast lead times in hours. GloFAS is a daily-accumulation product, so lead times are
#: whole days out to the seven-day horizon.
DEFAULT_LEADTIME_HOURS: tuple[int, ...] = (24, 48, 72, 96, 120, 144, 168)

ALL_MONTHS: tuple[str, ...] = tuple(f"{m:02d}" for m in range(1, 13))
ALL_DAYS: tuple[str, ...] = tuple(f"{d:02d}" for d in range(1, 32))

#: Chunking applied before writing. GloFAS grids are 3600x7200, so the spatial dims are cut
#: into tiles small enough that a single chunk is a few megabytes.
DEFAULT_CHUNKS: dict[str, int] = {"latitude": 600, "longitude": 600}


def cdsapirc_path() -> pathlib.Path:
    """Path the cdsapi client reads its configuration from."""
    return pathlib.Path(os.environ.get("CDSAPI_RC", "~/.cdsapirc")).expanduser()


def cds_client(config: Config | None = None):
    """Build a ``cdsapi.Client``.

    Prefers the configured ``CDSAPI_URL``/``CDSAPI_KEY`` and falls back to whatever the
    client picks up itself (``~/.cdsapirc`` or the ``CDSAPI_*`` environment variables).
    Raises :class:`~planetary_datasets.config.MissingCredential` when neither is available,
    rather than letting cdsapi fail later with a less obvious message.
    """
    import cdsapi

    cfg = config if config is not None else get_config()
    creds = cfg.credentials

    if creds.cdsapi_key:
        # The URL has a sensible default in cdsapi and is commonly left unset, so a key on
        # its own is enough.
        kwargs = {"key": creds.cdsapi_key}
        if creds.cdsapi_url:
            kwargs["url"] = creds.cdsapi_url
        return cdsapi.Client(**kwargs)

    if not cdsapirc_path().is_file():
        raise MissingCredential(
            "Missing required credentials: CDSAPI_KEY. Set it in .env or the environment "
            f"(with CDSAPI_URL if your endpoint is not the default), or write a "
            f"{cdsapirc_path()} file."
        )
    return cdsapi.Client()


def retrieve_to(
    client,
    dataset: str,
    request: dict[str, Any],
    target: str | os.PathLike,
    overwrite: bool = False,
) -> pathlib.Path:
    """Retrieve a CDS request to ``target``, atomically and skipping completed downloads.

    CDS requests take minutes to hours, so a resumed backfill must be able to trust that a
    file already on disk is complete. The download therefore lands on a ``.part`` file and
    is renamed into place only once cdsapi returns.
    """
    target = pathlib.Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)

    if not overwrite and target.is_file() and target.stat().st_size > 0:
        logger.debug(f"skipping {target.name}, already retrieved")
        return target

    part = target.with_name(target.name + ".part")
    part.unlink(missing_ok=True)
    logger.info(f"requesting {dataset} -> {target.name}")
    try:
        client.retrieve(dataset, request, str(part))
        os.replace(part, target)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    return target


def extract_netcdfs(
    archives: Iterable[str | os.PathLike],
    dest_dir: str | os.PathLike,
) -> List[str]:
    """Unpack the NetCDF members of the zips CDS returns, returning their paths.

    A plain ``.nc`` input is passed through untouched, so the same processing path works
    whether the request asked for ``download_format="zip"`` or not.
    """
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    extracted: List[str] = []
    for index, archive in enumerate(archives):
        archive = pathlib.Path(archive)
        if not zipfile.is_zipfile(archive):
            extracted.append(str(archive))
            continue
        # Each archive is unpacked into its own directory: CDS names every member inside a
        # zip "data.nc", so a shared directory would have them overwrite one another. The
        # index keeps that true even for two archives with the same name.
        out_dir = dest_dir / f"{index:04d}_{archive.stem}"
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive) as zf:
            for member in zf.namelist():
                if not member.lower().endswith((".nc", ".nc4", ".netcdf")):
                    continue
                zf.extract(member, out_dir)
                extracted.append(str(out_dir / member))
    return extracted


def normalise_glofas(ds: xr.Dataset) -> xr.Dataset:
    """Give a GloFAS NetCDF the coordinate names and ordering the stores expect."""
    renames = {
        old: new
        for old, new in (("lat", "latitude"), ("lon", "longitude"))
        if old in ds.dims or old in ds.coords
    }
    if renames:
        ds = ds.rename(renames)
    ds = make_lat_lon_coords_consistent(ds)
    # In a forecast file valid_time is time + step, so it is redundant once both are
    # coordinates, and keeping it would make an append fail because its values differ
    # between initialisation times. Newer CDS NetCDFs instead use valid_time *as* the time
    # dimension, and dropping that would throw the data away, so only the derived form is
    # removed.
    if "valid_time" in ds.coords and "valid_time" not in ds.dims:
        ds = ds.drop_vars("valid_time")
    return ds


def variables_token(variables: Sequence[str]) -> str:
    """A short stable token identifying a set of requested variables.

    Archive filenames that do not encode what was asked for are dangerous, because
    :func:`retrieve_to` skips any target that already exists: change ``variables`` and the
    stale archive would be reused and the new variable never downloaded.
    """
    digest = hashlib.sha256("\x00".join(sorted(variables)).encode()).hexdigest()
    return digest[:8]


#: Coordinates CDS may hand back as a scalar when a request pinned them to one value.
PROMOTABLE_COORDS: tuple[str, ...] = ("time", "valid_time", "step", "number")


def open_members(members: Sequence[str], chunks: dict[str, int] | None = None) -> xr.Dataset:
    """Open and combine the NetCDFs making up one partition.

    ``open_mfdataset(combine="by_coords")`` is not enough here. The reforecast asks for one
    lead time per request, and CDS then writes ``step`` as a *scalar* coordinate rather
    than a size-1 dimension; with nothing to order the files by, xarray raises "Could not
    find any dimension coordinates to use to order the Dataset objects". The same happens
    when a zip holds the control forecast and the perturbed members separately. So each
    file is opened on its own, the coordinates that distinguish it are promoted to size-1
    dimensions, and only then are the files combined.
    """
    datasets: list[xr.Dataset] = []
    for member in members:
        ds = normalise_glofas(xr.open_dataset(member, chunks=chunks or {}))
        for coord in PROMOTABLE_COORDS:
            if coord in ds.coords and ds[coord].ndim == 0:
                ds = ds.expand_dims(coord)
        datasets.append(ds)

    # ECMWF numbers the control forecast 0 and the perturbed members from 1, but the
    # control file carries no ensemble coordinate at all. Giving it one lets the two be
    # concatenated instead of silently overriding one another.
    if any("number" in ds.dims for ds in datasets):
        datasets = [
            ds if "number" in ds.dims else ds.assign_coords(number=0).expand_dims("number")
            for ds in datasets
        ]

    if len(datasets) == 1:
        return datasets[0]
    return xr.combine_by_coords(datasets, combine_attrs="override")


class GloFASProvider(BaseProvider):
    """Shared engine for the GloFAS products on the Climate Data Store.

    Subclasses set :attr:`dataset` and implement :meth:`requests_for`, which turns a
    partition timestamp into the ``(filename, request)`` pairs to retrieve.
    """

    #: CDS dataset name, e.g. ``cems-glofas-forecast``.
    dataset: str
    #: Frequency of the partitions this provider expects, for documentation and assets.
    partition_freq: str = "D"

    def __init__(
        self,
        config: Config | None = None,
        archive_dir: str | os.PathLike | None = None,
        variables: Sequence[str] = DEFAULT_VARIABLES,
        leadtime_hours: Sequence[int] = DEFAULT_LEADTIME_HOURS,
        chunks: dict[str, int] | None = None,
    ):
        super().__init__(config=config)
        self._archive_dir = pathlib.Path(archive_dir) if archive_dir is not None else None
        self.variables = list(variables)
        self.leadtime_hours = list(leadtime_hours)
        self.chunks = dict(DEFAULT_CHUNKS if chunks is None else chunks)

    @property
    def archive_dir(self) -> pathlib.Path:
        """Where retrieved archives are kept, under the configured data directory."""
        if self._archive_dir is not None:
            return self._archive_dir
        return self.config.data_dir / "glofas" / self.name

    def requests_for(self, it: pd.Timestamp) -> List[tuple[str, dict[str, Any]]]:
        """Return the ``(filename, request)`` pairs making up one partition."""
        raise NotImplementedError

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        client = cds_client(self.config)
        paths: List[str] = []
        for filename, request in self.requests_for(it):
            target = self.archive_dir / filename
            paths.append(str(retrieve_to(client, self.dataset, request, target)))
        return paths

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        scratch = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        members = extract_netcdfs(input_files, scratch / "glofas-extract")
        if not members:
            raise ValueError(
                f"{self.name}: no NetCDF members found in {len(input_files)} archive(s)"
            )

        logger.info(f"{self.name}: opening {len(members)} NetCDF file(s) for {it}")
        ds = open_members(sorted(members))
        ds = self.finalise(ds, it)
        return ds.chunk({dim: size for dim, size in self.chunks.items() if dim in ds.dims})

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Hook for a subclass to shape the merged dataset before it is chunked."""
        return ds


def _as_init_time(ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
    """Present a forecast dataset with ``init_time`` as its leading dimension.

    GloFAS names the forecast reference time ``time`` and the lead time ``step``. Renaming
    it makes the store self-describing and keeps ``time`` free for the valid time.
    """
    if "init_time" not in ds.dims and "init_time" not in ds.coords:
        if "time" in ds.coords:
            ds = ds.rename({"time": "init_time"})
        else:
            ds = ds.assign_coords(init_time=pd.Timestamp(it))
    if "init_time" not in ds.dims:
        ds = ds.expand_dims("init_time")
    return ds.transpose("init_time", ...)


class GloFASForecastProvider(GloFASProvider):
    """Operational GloFAS ensemble forecast, one partition per initialisation day.

    Each day is retrieved as one archive per variable, covering the control forecast, the
    perturbed ensemble members and every lead time out to seven days.
    """

    name = "glofas-forecast"
    dataset = "cems-glofas-forecast"
    append_dim = "init_time"
    store_prefix = "bkr/glofas/forecast.icechunk"
    partition_freq = "D"

    #: First initialisation day available from the operational system.
    start_date = pd.Timestamp("2020-01-01")

    def requests_for(self, it: pd.Timestamp) -> List[tuple[str, dict[str, Any]]]:
        it = pd.Timestamp(it)
        requests: List[tuple[str, dict[str, Any]]] = []
        for var in self.variables:
            request = {
                "system_version": ["operational"],
                "hydrological_model": ["lisflood"],
                "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
                "variable": [var],
                "year": [f"{it.year:04d}"],
                "month": [f"{it.month:02d}"],
                "day": [f"{it.day:02d}"],
                "leadtime_hour": [str(h) for h in self.leadtime_hours],
                "data_format": "netcdf",
                "download_format": "zip",
            }
            requests.append((f"glofas_forecast_{var}_{it:%Y%m%d}.zip", request))
        return requests

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        return _as_init_time(ds, it)


class GloFASReforecastProvider(GloFASProvider):
    """GloFAS version 4.0 reforecast, one partition per hindcast month.

    The reforecast is initialised twice a week, so a month is a manageable request. Lead
    times come back in separate archives because the CDS splits them server-side.
    """

    name = "glofas-reforecast"
    dataset = "cems-glofas-reforecast"
    append_dim = "init_time"
    store_prefix = "bkr/glofas/reforecast.icechunk"
    partition_freq = "MS"

    #: The version 4.0 hindcast period.
    start_date = pd.Timestamp("2003-01-01")
    end_date = pd.Timestamp("2023-12-01")

    #: The reforecast only carries discharge; asking for runoff returns an empty request.
    default_variables: tuple[str, ...] = ("river_discharge_in_the_last_24_hours",)

    def __init__(
        self,
        config: Config | None = None,
        archive_dir: str | os.PathLike | None = None,
        variables: Sequence[str] | None = None,
        leadtime_hours: Sequence[int] = DEFAULT_LEADTIME_HOURS,
        chunks: dict[str, int] | None = None,
    ):
        super().__init__(
            config=config,
            archive_dir=archive_dir,
            variables=self.default_variables if variables is None else variables,
            leadtime_hours=leadtime_hours,
            chunks=chunks,
        )

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Return the hindcast months not yet in the store.

        The reforecast is initialised on Mondays and Thursdays, so the first instant of a
        month is almost never one of its initialisation times. Asking for the exact
        partition timestamp would report every month as missing forever, and each rerun
        would re-extract and re-merge a month that is already stored.
        """
        missing = missing_periods(
            self.get_icechunk_repo(),
            list(desired),
            unit="M",
            append_dim=self.append_dim,
        )
        return [pd.Timestamp(t) for t in missing]

    def requests_for(self, it: pd.Timestamp) -> List[tuple[str, dict[str, Any]]]:
        it = pd.Timestamp(it)
        requests: List[tuple[str, dict[str, Any]]] = []
        for lead_time in self.leadtime_hours:
            request = {
                "system_version": ["version_4_0"],
                "hydrological_model": ["lisflood"],
                "product_type": ["control_reforecast", "ensemble_perturbed_reforecast"],
                "variable": list(self.variables),
                "hyear": [f"{it.year:04d}"],
                "hmonth": [f"{it.month:02d}"],
                "hday": list(ALL_DAYS),
                "leadtime_hour": [str(lead_time)],
                "data_format": "netcdf",
                "download_format": "zip",
            }
            token = variables_token(self.variables)
            name = f"glofas_reforecast_{it:%Y%m}_leadtime_{lead_time:03d}_{token}.zip"
            requests.append((name, request))
        return requests

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        return _as_init_time(ds, it)


class GloFASHistoricalProvider(GloFASProvider):
    """Consolidated GloFAS reanalysis, one partition per month.

    This is a simulated history rather than a forecast: there is no lead time and no
    ensemble, so the partition appends straight along ``time``.
    """

    name = "glofas-historical"
    dataset = "cems-glofas-historical"
    append_dim = "time"
    store_prefix = "bkr/glofas/historical.icechunk"
    partition_freq = "MS"

    start_date = pd.Timestamp("1979-01-01")

    def requests_for(self, it: pd.Timestamp) -> List[tuple[str, dict[str, Any]]]:
        it = pd.Timestamp(it)
        request = {
            "system_version": ["version_4_0"],
            "hydrological_model": ["lisflood"],
            "product_type": ["consolidated"],
            "variable": list(self.variables),
            "hyear": [f"{it.year:04d}"],
            "hmonth": [f"{it.month:02d}"],
            "hday": list(ALL_DAYS),
            "data_format": "netcdf",
            "download_format": "zip",
        }
        token = variables_token(self.variables)
        return [(f"glofas_historical_{it:%Y%m}_{token}.zip", request)]

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        # Newer CDS NetCDFs call the time axis valid_time. The store appends along time.
        if "time" not in ds.dims and "valid_time" in ds.dims:
            ds = ds.rename({"valid_time": "time"})
        return ds.sortby("time") if "time" in ds.dims else ds


#: Every GloFAS provider, keyed by name, for the Dagster assets and the CLI.
PROVIDERS: dict[str, type[GloFASProvider]] = {
    cls.name: cls
    for cls in (GloFASForecastProvider, GloFASReforecastProvider, GloFASHistoricalProvider)
}


__all__ = [
    "ALL_DAYS",
    "ALL_MONTHS",
    "DEFAULT_LEADTIME_HOURS",
    "DEFAULT_VARIABLES",
    "GloFASForecastProvider",
    "GloFASHistoricalProvider",
    "GloFASProvider",
    "GloFASReforecastProvider",
    "PROVIDERS",
    "cds_client",
    "extract_netcdfs",
    "normalise_glofas",
    "retrieve_to",
]
