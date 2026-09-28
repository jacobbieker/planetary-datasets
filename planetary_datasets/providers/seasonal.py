"""C3S seasonal forecasts (NCEP, ECMWF, UKMO, DWD) from the Climate Data Store.

The ``seasonal-original-single-levels`` collection on the Copernicus Climate Data Store
holds the raw single-level fields of the seasonal forecasting systems contributed to C3S.
Each originating centre initialises once per month (NCEP more often) and runs out to
between six and thirteen months, so one partition here is one initialisation *month*.

https://cds.climate.copernicus.eu/datasets/seasonal-original-single-levels

Credentials
-----------
Set ``CDSAPI_URL`` and ``CDSAPI_KEY`` in the environment or ``.env``. If they are absent a
``~/.cdsapirc`` file is used instead, and if there is no such file a
:class:`~planetary_datasets.config.MissingCredential` is raised naming what is missing.

Systems
-------
Each centre revises its forecasting system every year or two and the CDS exposes the
revisions as separate ``system`` numbers, so the right number depends on the
initialisation date. :data:`CENTRE_SYSTEMS` records the transitions that are known; dates
before a centre's first entry raise, because silently picking the wrong system returns a
different model's forecast rather than an error. Pass ``system=`` to override.

Layout of the store
-------------------
``init_time`` is the append dimension. ``step`` is the lead time in whole hours as an
``int32`` coordinate rather than a ``timedelta64``, and ``valid_time`` is dropped, because
xarray gives every datetime-like variable its own reference date on the first write and
that reference then disagrees with the next partition's and breaks the append.
``valid_time`` is recoverable as ``init_time + step``.

Precision is left at float32. Total precipitation is an accumulation in metres whose
values sit around 1e-3 and below, which float16 cannot represent without losing the small
end of the distribution entirely.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import zipfile
from typing import List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import make_lat_lon_coords_consistent
from planetary_datasets.common.store import ALIGNMENT_COORDS
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.providers._timestamps import NaiveUTCPartitions, to_naive_utc

CDS_DATASET = "seasonal-original-single-levels"

#: Fallback credentials file the ``cdsapi`` package reads when none are configured here.
CDSAPIRC = pathlib.Path("~/.cdsapirc")

#: ``centre -> ((in effect from, system number), ...)``, oldest first.
#:
#: * NCEP has only ever contributed CFSv2 as system 2.
#: * ECMWF moved from SEAS5 (5) to SEAS5.1 (51) with the November 2022 initialisation.
#: * UKMO's GloSea6 revisions run 600-605 and are hard to date precisely, so only the
#:   transitions that are certain are recorded and earlier dates require an explicit
#:   ``system=``.
#: * DWD moved from GCFS2.1 (21) to GCFS2.2 (22) in April 2025.
CENTRE_SYSTEMS: dict[str, tuple[tuple[str, str], ...]] = {
    "ncep": (("1982-01-01", "2"),),
    "ecmwf": (("1981-01-01", "5"), ("2022-11-01", "51")),
    "ukmo": (("2023-01-01", "603"), ("2026-04-01", "610")),
    "dwd": (("2020-11-01", "21"), ("2025-04-01", "22")),
}

#: Default lead times: daily out to 215 days, which every contributing system covers.
DEFAULT_LEADTIME_HOURS: tuple[int, ...] = tuple(range(24, 5161, 24))

DEFAULT_VARIABLES: tuple[str, ...] = ("total_precipitation",)

#: Attributes of the ``step`` coordinate. There is deliberately no CF ``units``: xarray
#: decodes any variable carrying a plural time unit straight back into ``timedelta64``, so
#: the int32 coordinate written here would come back as a timedelta, stop matching the
#: coordinate held in memory, and every append after the first would be rejected as a
#: coordinate mismatch.
STEP_ATTRS: dict[str, str] = {
    "standard_name": "forecast_period",
    "long_name": "forecast lead time",
    "comment": "whole hours after init_time",
}

#: Renames applied to whatever the CDS hands back, so every centre lands in the same shape.
_DIM_RENAMES = {
    "forecast_reference_time": "init_time",
    "time": "init_time",
    "forecast_period": "step",
    "number": "ensemble_member",
    "lat": "latitude",
    "lon": "longitude",
}


def system_for(centre: str, it: pd.Timestamp) -> str:
    """Return the CDS ``system`` number for a centre at an initialisation date.

    Raises:
        ValueError: If the centre is unknown, or the date is earlier than the first system
            recorded for it. Guessing would quietly return another model's forecast.
    """
    try:
        transitions = CENTRE_SYSTEMS[centre]
    except KeyError:
        raise ValueError(
            f"unknown originating centre {centre!r}, expected one of {sorted(CENTRE_SYSTEMS)}"
        ) from None

    it = to_naive_utc(it)
    chosen: str | None = None
    for start, system in transitions:
        if it >= pd.Timestamp(start):
            chosen = system
    if chosen is None:
        raise ValueError(
            f"no {centre} system is recorded for {it:%Y-%m}; the earliest known is "
            f"{transitions[0][1]} from {transitions[0][0]}. Pass system= explicitly."
        )
    return chosen


def cds_client(config=None):
    """Build a ``cdsapi.Client`` from the configured credentials.

    Falls back to ``~/.cdsapirc`` when ``CDSAPI_URL``/``CDSAPI_KEY`` are unset, and raises
    :class:`~planetary_datasets.config.MissingCredential` when neither is available.
    """
    import cdsapi

    from planetary_datasets.config import get_config

    creds = (config or get_config()).credentials
    if not (creds.cdsapi_url and creds.cdsapi_key):
        if CDSAPIRC.expanduser().is_file():
            return cdsapi.Client()
        # Raises MissingCredential naming exactly what is absent.
        creds.require("cdsapi_url", "cdsapi_key")
    return cdsapi.Client(url=creds.cdsapi_url, key=creds.cdsapi_key)


def _extract_if_zip(path: pathlib.Path, temp_dir: pathlib.Path) -> List[pathlib.Path]:
    """Return the NetCDF files in ``path``, unzipping it first if the CDS zipped them."""
    if not zipfile.is_zipfile(path):
        return [path]
    target = temp_dir / (path.stem + "_unzipped")
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path) as archive:
        archive.extractall(target)
    members = sorted(p for p in target.rglob("*") if p.suffix in (".nc", ".nc4", ".grib"))
    if not members:
        raise RuntimeError(f"{path} is a zip archive with no NetCDF members")
    return members


class SeasonalForecastProvider(NaiveUTCPartitions, BaseProvider):
    """One initialisation month of a C3S seasonal forecast per partition.

    Args:
        centre: Originating centre, one of :data:`CENTRE_SYSTEMS`.
        variables: CDS variable names to retrieve.
        system: Override the system number that :func:`system_for` would pick.
        leadtime_hours: Lead times to retrieve, in hours. Must be the same for every
            partition written to a store; the append is rejected otherwise.
        days: Initialisation days within the month. Only the 1st for the monthly
            systems; NCEP also initialises through the month.
        download_dir: Keep the retrieved file here instead of in the partition's
            temporary directory, so a re-run does not re-queue a CDS request.
        store_prefix: Override the store location.
    """

    append_dim = "init_time"

    def __init__(
        self,
        centre: str = "ecmwf",
        variables: Sequence[str] = DEFAULT_VARIABLES,
        system: str | None = None,
        leadtime_hours: Sequence[int] = DEFAULT_LEADTIME_HOURS,
        days: Sequence[str] = ("01",),
        download_dir: str | pathlib.Path | None = None,
        store_prefix: str | None = None,
        config=None,
    ):
        super().__init__(config=config)
        if centre not in CENTRE_SYSTEMS:
            raise ValueError(
                f"unknown originating centre {centre!r}, expected one of {sorted(CENTRE_SYSTEMS)}"
            )
        self.centre = centre
        self.name = f"seasonal_{centre}"
        self.variables = tuple(variables)
        self.system = system
        self.leadtime_hours = tuple(int(h) for h in leadtime_hours)
        self.days = tuple(days)
        self.download_dir = pathlib.Path(download_dir) if download_dir is not None else None
        self.store_prefix = store_prefix or f"bkr/seasonal/{centre}_single_levels.icechunk"

    def system_number(self, it: pd.Timestamp) -> str:
        """System number used for an initialisation month."""
        return self.system if self.system is not None else system_for(self.centre, it)

    def build_request(self, it: pd.Timestamp) -> dict:
        """The CDS request body for one initialisation month."""
        it = to_naive_utc(it)
        return {
            "originating_centre": self.centre,
            "system": self.system_number(it),
            "variable": list(self.variables),
            "year": [f"{it.year:04d}"],
            "month": [f"{it.month:02d}"],
            "day": list(self.days),
            "leadtime_hour": [str(h) for h in self.leadtime_hours],
            "data_format": "netcdf",
        }

    def fetch(self, it, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        it = to_naive_utc(it)
        request = self.build_request(it)
        directory = self.download_dir or temp_dir or self.config.scratch_dir
        directory = pathlib.Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        # The digest covers the whole request, so changing the variables or lead times
        # does not silently reuse a file downloaded for the previous ones.
        digest = hashlib.sha256(repr(sorted(request.items())).encode()).hexdigest()[:8]
        target = directory / f"{self.centre}_{request['system']}_{it:%Y%m}_{digest}.nc"

        if target.is_file() and target.stat().st_size > 0:
            logger.debug(f"{self.name}: reusing {target}")
            return [str(target)]

        # Retrieve into a .part file and rename once complete, so an interrupted run does
        # not leave a truncated file that the check above would accept as finished.
        part = target.with_name(target.name + ".part")
        part.unlink(missing_ok=True)
        logger.info(f"{self.name}: requesting {CDS_DATASET} {it:%Y-%m} system {request['system']}")
        cds_client(self.config).retrieve(CDS_DATASET, request, target=str(part))
        if not part.is_file() or part.stat().st_size == 0:
            # Transient, not "nothing to do": returning [] would retire the partition.
            raise RuntimeError(f"{self.name}: CDS returned no data for {it:%Y-%m}")
        os.replace(part, target)
        return [str(target)]

    def process(
        self,
        input_files: List[str],
        it,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        it = to_naive_utc(it)
        scratch = pathlib.Path(temp_dir or self.config.scratch_dir)
        members: List[pathlib.Path] = []
        for path in input_files:
            members.extend(_extract_if_zip(pathlib.Path(path), scratch))

        datasets = [xr.open_dataset(p) for p in members]
        ds = datasets[0] if len(datasets) == 1 else xr.merge(datasets)
        return self.normalise(ds, it)

    def normalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Put a CDS seasonal dataset into the store's layout."""
        renames: dict[str, str] = {}
        present = set(ds.variables) | set(ds.dims)
        taken = set(present)
        for source, target in _DIM_RENAMES.items():
            # Dimensions without a coordinate variable count on both sides: a bare
            # `number` dim still has to become `ensemble_member`, or the ensemble-size
            # guard in write_to_icechunk silently checks nothing. Skip a rename whose
            # target already exists, including one an earlier rename in this loop has
            # just claimed; xarray raises rather than merging them.
            if source in present and target not in taken:
                renames[source] = target
                taken.add(target)
        ds = ds.rename(renames)

        # valid_time is init_time + step and carries its own datetime encoding, which is
        # what makes the second append fail. Drop it and recompute on read.
        ds = ds.drop_vars([v for v in ("valid_time", "forecast_period_bnds") if v in ds.variables])

        if "step" in ds.coords and np.issubdtype(ds["step"].dtype, np.timedelta64):
            hours = (ds["step"].values / np.timedelta64(1, "h")).round().astype("int32")
            ds = ds.assign_coords(step=("step", hours))
            ds["step"].attrs = dict(STEP_ATTRS)

        for dim in ("step", "ensemble_member"):
            # A dimension with no coordinate variable cannot be indexed by name, and the
            # alignment check in write_to_icechunk does exactly that. Give it a positional
            # index rather than letting the check raise.
            if dim in ds.dims and dim not in ds.coords:
                ds = ds.assign_coords({dim: np.arange(ds.sizes[dim], dtype="int32")})
                ds[dim].attrs = {"long_name": f"{dim} index (no coordinate in the source)"}

        if "init_time" not in ds.coords:
            ds = ds.expand_dims(init_time=pd.DatetimeIndex([it]))
        elif "init_time" not in ds.dims:
            ds = ds.expand_dims("init_time")

        ds = make_lat_lon_coords_consistent(ds)
        # Cast the fields only. Dataset.astype would also hit init_time and turn the
        # datetime coordinate into a float.
        for var in ds.data_vars:
            if np.issubdtype(ds[var].dtype, np.floating) and ds[var].dtype != np.float32:
                ds[var] = ds[var].astype("float32", keep_attrs=True)
        ds.attrs.update(
            {
                "title": f"C3S seasonal forecast, {self.centre.upper()}",
                "source": f"{CDS_DATASET} (Copernicus Climate Data Store)",
                "originating_centre": self.centre,
                "system": self.system_number(it),
            }
        )
        return ds

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Append, refusing to write when the lead times or ensemble size have changed.

        An ensemble that grew from 25 hindcast members to 51 real-time members, or a
        shortened lead time list, would otherwise produce a store whose later timesteps
        silently mean something different from its earlier ones.
        """
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {pd.Timestamp(processed[self.append_dim].values[0]):%Y-%m}",
            alignment_coords=(*ALIGNMENT_COORDS, "step", "ensemble_member"),
        )
