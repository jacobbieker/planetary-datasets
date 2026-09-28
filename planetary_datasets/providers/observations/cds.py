"""Shared plumbing for the CDS in-situ observation datasets.

The Copernicus Climate Data Store publishes several station networks under its
``insitu-observations-*`` collections. They all come back as a single NetCDF per request
in *long* form: one row per observation, with the station, the time, the observed
quantity and its value in separate variables. :func:`long_table_to_cube` pivots that into
the same ``(time, station)`` (optionally ``(time, station, level)``) layout the rest of
this subpackage writes.

Credentials come from ``CDSAPI_URL`` and ``CDSAPI_KEY`` via
:func:`~planetary_datasets.config.get_config`, falling back to ``~/.cdsapirc``. Neither
present raises :class:`~planetary_datasets.config.MissingCredential` rather than queueing
an anonymous request that will be rejected hours later.
"""

from __future__ import annotations

import pathlib
from abc import abstractmethod

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import write_to_icechunk as _write_to_icechunk
from planetary_datasets.config import MissingCredential
from planetary_datasets.providers.observations.base import (
    OBSERVATION_ALIGNMENT_COORDS,
    NoStationDataError,
    existing_station_axis,
    partial_path,
    partition_time_index,
)

#: Candidate names for each column of the long table, in preference order. CADS has
#: renamed some of these between dataset versions, so each is resolved by trying the
#: alternatives rather than assuming one spelling.
TIME_COLUMNS = ("report_timestamp", "date_time", "time")
STATION_COLUMNS = ("primary_station_id", "station_name", "station_id", "platform_id")
VARIABLE_COLUMNS = ("observed_variable", "variable")
VALUE_COLUMNS = ("observation_value", "value")
LATITUDE_COLUMNS = ("latitude", "lat", "latitude|station_configuration")
LONGITUDE_COLUMNS = ("longitude", "lon", "longitude|station_configuration")
LEVEL_COLUMNS = ("z_coordinate", "air_pressure", "pressure")

ALL_DAYS = tuple(f"{day:02d}" for day in range(1, 32))


def _pick(df: pd.DataFrame, candidates) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _decode(series: pd.Series) -> pd.Series:
    """Turn CADS byte-string columns into plain strings."""
    if series.dtype == object and len(series) and isinstance(series.iloc[0], bytes):
        return series.str.decode("utf-8", errors="replace")
    return series


def long_table_to_cube(
    ds: xr.Dataset,
    times: pd.DatetimeIndex,
    stations: list[str] | None = None,
    variables: tuple[str, ...] | None = None,
    levels: tuple[float, ...] | None = None,
    how: str = "mean",
) -> xr.Dataset:
    """Pivot a CADS long-format observation table into a gridded dataset.

    Every axis of the result is fixed by the caller rather than by the response, because
    a data variable or a pressure level that happens to be absent from one month would
    otherwise make that month unappendable to the store.

    Args:
        ds: The dataset as returned by the CDS, with an observation dimension.
        times: Target time axis. Observations are binned onto it.
        stations: Canonical station axis. Defaults to the sorted stations present, which
            is only safe for a one-off extraction — a store that is appended to needs a
            fixed list.
        variables: Data variables to emit, in order. Ones the response does not carry are
            written as all-NaN. Defaults to whatever the response contained.
        levels: When given, a vertical axis is added and observations are assigned to the
            nearest level. Used by the radiosonde datasets.
        how: Aggregation applied when several observations fall in the same bin.

    Raises:
        NoStationDataError: when the response does not look like a long table, naming the
            columns that are missing. The CADS schema is not contractual, so failing with
            the actual column list beats writing a silently empty store.
    """
    df = ds.to_dataframe().reset_index()
    resolved = {
        "time": _pick(df, TIME_COLUMNS),
        "station": _pick(df, STATION_COLUMNS),
        "variable": _pick(df, VARIABLE_COLUMNS),
        "value": _pick(df, VALUE_COLUMNS),
    }
    missing = [key for key, column in resolved.items() if column is None]
    if missing:
        raise NoStationDataError(
            f"CDS response is not a recognised long table: no column for {missing}. "
            f"Columns present: {sorted(df.columns)[:30]}"
        )

    time_col, station_col, variable_col, value_col = (
        resolved["time"],
        resolved["station"],
        resolved["variable"],
        resolved["value"],
    )
    df[station_col] = _decode(df[station_col]).astype(str)
    df[variable_col] = _decode(df[variable_col]).astype(str)

    stamps = pd.to_datetime(df[time_col], errors="coerce", utc=True).dt.tz_localize(None)
    step = times[1] - times[0] if len(times) > 1 else pd.Timedelta("1h")
    # Bin to the grid rather than reindexing: soundings and GNSS solutions do not land on
    # round hours.
    df["_time"] = stamps.dt.floor(step)
    df = df[df["_time"].between(times[0], times[-1])]
    if df.empty:
        raise NoStationDataError("CDS response holds no observations inside the partition")

    station_axis = stations if stations is not None else sorted(df[station_col].unique())
    df = df[df[station_col].isin(station_axis)]
    if df.empty:
        raise NoStationDataError("no observations from any station on the canonical axis")

    level_col = _pick(df, LEVEL_COLUMNS) if levels is not None else None
    group_keys = ["_time", station_col, variable_col]
    if level_col is not None:
        level_values = pd.to_numeric(df[level_col], errors="coerce").to_numpy(dtype="float64")
        # A row whose level does not parse gives an all-NaN distance row, and `argmin` of
        # that is 0 — it would be filed at `levels[0]`, the surface, rather than discarded.
        # Soundings do carry rows with a blank pressure, so this is not hypothetical.
        usable = np.isfinite(level_values)
        if not usable.all():
            logger.warning(
                f"dropping {int((~usable).sum())} row(s) with no usable {level_col!r} rather "
                f"than snapping them to {np.asarray(levels, dtype='float64')[0]:g}"
            )
            df = df[usable]
            level_values = level_values[usable]
            if df.empty:
                raise NoStationDataError(
                    f"every observation in the CDS response has an unusable {level_col!r}"
                )
        nearest = np.asarray(levels, dtype="float64")[
            np.abs(np.asarray(levels, dtype="float64")[None, :] - level_values[:, None]).argmin(
                axis=1
            )
        ]
        df["_level"] = nearest
        group_keys.append("_level")

    values = pd.to_numeric(df[value_col], errors="coerce")
    grouped = values.groupby([df[key] for key in group_keys]).agg(how)
    cube = grouped.to_xarray()

    renames = {"_time": "time", station_col: "station", variable_col: "variable"}
    if level_col is not None:
        renames["_level"] = "level"
    cube = cube.rename(renames)
    reindex_to: dict[str, object] = {"time": times, "station": station_axis}
    if levels is not None and level_col is not None:
        reindex_to["level"] = list(levels)
    cube = cube.reindex(**reindex_to)

    if variables is not None:
        absent = sorted(set(variables) - set(cube.coords["variable"].values.tolist()))
        if absent:
            logger.warning(f"CDS response has no rows for {absent}; writing them as NaN")
        cube = cube.reindex(variable=list(variables))
    ds_out = cube.to_dataset(dim="variable")

    coordinate_columns = (
        (_pick(df, LATITUDE_COLUMNS), "latitude"),
        (_pick(df, LONGITUDE_COLUMNS), "longitude"),
    )
    for column, axis_name in coordinate_columns:
        if column is None:
            continue
        per_station = pd.to_numeric(df[column], errors="coerce").groupby(df[station_col]).first()
        ds_out = ds_out.assign_coords(
            {axis_name: ("station", per_station.reindex(station_axis).to_numpy(dtype="float64"))}
        )

    for name in ds_out.data_vars:
        ds_out[name] = ds_out[name].astype("float32")
    return ds_out


class CDSInsituProvider(BaseProvider):
    """Base for the monthly CDS in-situ retrievals.

    Subclasses set :attr:`dataset`, :attr:`request_template` and :attr:`sample_freq`, and
    the retrieval, credential handling and pivot are handled here.
    """

    append_dim = "time"
    partition_freq = "MS"
    sample_freq = "1h"
    #: Vertical axis, when the dataset has one.
    levels: tuple[float, ...] | None = None
    #: Data variables written, in order. Fixed so a month missing one still appends.
    variables: tuple[str, ...] = ()
    #: CDS collection name.
    dataset: str = ""

    def __init__(self, config=None, stations: list[str] | None = None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            stations: Canonical station axis. The CDS publishes no roster endpoint, so
                when this is omitted the axis is taken from the store if one exists and
                otherwise from the first month written. See :meth:`station_axis`.
        """
        super().__init__(config=config)
        self.stations = list(stations) if stations is not None else None

    def station_axis(self) -> list[str] | None:
        """The station axis this partition must produce.

        An explicit list wins. Otherwise the axis already in the store is reused, so the
        second and later partitions line up with the first instead of being silently
        refused for a one-station difference in who reported. None means "this is the
        first write, take the axis from the response".
        """
        if self.stations is not None:
            return self.stations
        axis = existing_station_axis(self.get_icechunk_repo())
        if axis:
            logger.debug(f"{self.name}: pinning to the store's {len(axis)} station axis")
        return axis

    @abstractmethod
    def build_request(self, it: pd.Timestamp) -> dict:
        """The CDS request body for one partition."""

    def client(self):
        """A ``cdsapi`` client configured from the environment or ``~/.cdsapirc``."""
        import cdsapi

        creds = self.config.credentials
        if creds.cdsapi_url and creds.cdsapi_key:
            return cdsapi.Client(url=creds.cdsapi_url, key=creds.cdsapi_key)
        if (pathlib.Path.home() / ".cdsapirc").is_file():
            return cdsapi.Client()
        raise MissingCredential(
            "Missing required credentials: CDSAPI_URL, CDSAPI_KEY. Set them in .env or the "
            "environment, or write a ~/.cdsapirc."
        )

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        return partition_time_index(it, self.partition_freq, self.sample_freq)

    def target_name(self, it: pd.Timestamp) -> str:
        return f"{self.name}_{pd.Timestamp(it):%Y%m}.nc"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / self.target_name(it)

        logger.info(f"{self.name}: requesting {self.dataset} for {pd.Timestamp(it):%Y-%m}")
        # Written to a .part file first: a retrieval interrupted halfway would otherwise
        # leave a truncated NetCDF that looks like a finished download.
        part = partial_path(target)
        self.client().retrieve(self.dataset, self.build_request(it), target=str(part))
        part.replace(target)
        return [str(target)]

    def process(
        self,
        input_files: list[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        with xr.open_dataset(input_files[0]) as raw:
            return long_table_to_cube(
                raw,
                self.partition_times(it),
                stations=self.station_axis(),
                variables=self.variables or None,
                levels=self.levels,
            )

    def write_to_icechunk(self, repo, processed):
        return _write_to_icechunk(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {processed[self.append_dim].values[0]}",
            alignment_coords=OBSERVATION_ALIGNMENT_COORDS,
        )
