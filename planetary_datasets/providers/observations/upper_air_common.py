"""Shared plumbing for the upper-air and aircraft observation providers.

AMDAR aircraft reports, IGRA radiosonde soundings and Sondehub amateur radiosonde
telemetry are all *point* observations rather than fields: a partition is a window of
wall-clock time holding an arbitrary number of reports, not a grid of the same shape
every step. They are therefore stored as a flat table whose single dimension is the
observation time, and appended to along that dimension.

Two consequences of that shape are handled here rather than repeated in each provider:

* The partition timestamp is not itself a value in the store, so the base class's "is
  this timestep already written?" check cannot be used as written. :class:`
  PointObservationProvider` instead asks whether *any* stored observation falls inside
  the partition's window.
* An append is refused when the variable set differs from the store's, and which fields
  a given day happens to report varies. :func:`table_to_dataset` therefore fills a fixed
  schema, inserting an all-missing column for anything absent.

Providers must trim their observations to the partition window before writing. Windows
are half-open and non-overlapping, so trimmed partitions cannot produce two observations
with the same timestamp from different runs — which matters because
:func:`~planetary_datasets.common.store.write_to_icechunk` drops incoming steps whose
value is already stored.
"""

from __future__ import annotations

from typing import Iterable, List, Mapping, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import existing_times

#: Value used for a string field that an observation does not carry. Empty rather than
#: "nan", so a consumer filtering on truthiness gets the right answer.
MISSING_STRING = ""

#: dtypes :func:`table_to_dataset` knows how to build a column for.
SUPPORTED_DTYPES = ("float32", "float64", "str")

#: Resolution observation times are floored to before they are stored.
#:
#: This is not cosmetic. :func:`~planetary_datasets.common.store.build_encoding` fixes the
#: store's time units at "seconds since 1970-01-01" on the *first* write only. If that
#: partition happens to hold whole-second times, a later partition with sub-second times
#: is encoded at a finer resolution by xarray and written under the store's second-based
#: units, so it reads back a thousand times too large. Sondehub frames carry microsecond
#: upload timestamps, so this is a live hazard rather than a theoretical one. Flooring
#: here makes every partition agree with the encoding.
TIME_RESOLUTION = "s"


def _float_column(values, dtype: str, length: int) -> np.ndarray:
    """Coerce a column to floats, turning anything unparseable into NaN."""
    if values is None:
        return np.full(length, np.nan, dtype=dtype)
    numeric = pd.to_numeric(pd.Series(list(values)), errors="coerce")
    return numeric.to_numpy(dtype=dtype, na_value=np.nan)


def _string_column(values, length: int) -> np.ndarray:
    """Coerce a column to Python strings, with :data:`MISSING_STRING` for gaps.

    Object dtype rather than a fixed-width numpy string: zarr stores these as
    variable-length UTF-8, and a fixed width would silently truncate the long station
    identifiers IGRA uses.
    """
    if values is None:
        return np.full(length, MISSING_STRING, dtype=object)
    series = pd.Series(list(values))
    return series.where(series.notna(), MISSING_STRING).astype(str).to_numpy(dtype=object)


def table_to_dataset(
    table: pd.DataFrame,
    schema: Mapping[str, str],
    time_column: str = "time",
    attrs: Mapping[str, str] | None = None,
    resolution: str = TIME_RESOLUTION,
) -> xr.Dataset:
    """Turn a table of point observations into a dataset indexed by observation time.

    Args:
        table: One row per observation. Must have ``time_column``; any column not named
            in ``schema`` is dropped, and any schema entry not in the table becomes an
            all-missing variable so every partition writes the same variables.
        schema: Variable name to dtype, one of :data:`SUPPORTED_DTYPES`.
        time_column: Column holding the observation time. Parsed as UTC and stored naive,
            because mixing tz-aware and naive coordinates across partitions breaks appends.
        attrs: Dataset attributes.
        resolution: Times are floored to this pandas frequency. See
            :data:`TIME_RESOLUTION` for why this is not optional in practice.

    Returns:
        A dataset with a single ``time`` dimension, sorted by time.
    """
    if time_column not in table.columns:
        raise ValueError(f"observation table has no {time_column!r} column")

    bad = {name: dtype for name, dtype in schema.items() if dtype not in SUPPORTED_DTYPES}
    if bad:
        raise ValueError(f"unsupported dtypes in schema: {bad}. Supported: {SUPPORTED_DTYPES}")

    times = pd.to_datetime(table[time_column], utc=True, errors="coerce")
    table = table.loc[times.notna()].copy()
    times = times[times.notna()]
    # tz_localize(None) after utc=True gives naive UTC; xarray cannot store tz-aware times.
    times = pd.DatetimeIndex(times).tz_localize(None).floor(resolution)

    order = np.argsort(times.values, kind="stable")
    times = times[order]
    table = table.iloc[order]

    length = len(table)
    data_vars: dict[str, tuple] = {}
    for name, dtype in schema.items():
        column = table[name] if name in table.columns else None
        if dtype == "str":
            values = _string_column(column, length)
        else:
            values = _float_column(column, dtype, length)
        data_vars[name] = ("time", values)

    return xr.Dataset(data_vars, coords={"time": times}, attrs=dict(attrs or {}))


class PointObservationProvider(BaseProvider):
    """A provider whose partitions are time windows of individually-timed observations.

    Subclasses set :attr:`partition_freq` to the width of one partition and are
    responsible for returning only observations inside
    :meth:`partition_window` from :meth:`~planetary_datasets.base.BaseProvider.process`.
    """

    #: Width of one partition, as a pandas offset alias: ``1D``, ``MS``, ``6h``.
    partition_freq: str = "1D"

    def partition_window(self, it: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Half-open ``[start, end)`` window of wall-clock time covered by a partition."""
        start = pd.Timestamp(it)
        return start, start + pd.tseries.frequencies.to_offset(self.partition_freq)

    def trim_to_window(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Drop observations outside the partition window.

        Sources routinely hand back more than was asked for — a MET ``obs_window`` of
        +/-6 hours, a sonde flight that crosses midnight. Keeping the overspill would
        write the same observation under two partitions.
        """
        if self.append_dim not in ds.coords or ds.sizes.get(self.append_dim, 0) == 0:
            return ds
        start, end = self.partition_window(it)
        times = pd.DatetimeIndex(np.atleast_1d(ds[self.append_dim].values))
        inside = (times >= start) & (times < end)
        return ds.isel({self.append_dim: np.flatnonzero(inside)})

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write, unless the partition turned out to hold no observations.

        A quiet day is normal for these sources and is not a failure. The base
        implementation would index the first element of an empty time coordinate.
        """
        if processed.sizes.get(self.append_dim, 0) == 0:
            logger.info(f"{self.name}: no observations in this partition, nothing to write")
            return False
        return super().write_to_icechunk(repo, processed)

    def missing_timesteps(self, desired: Iterable[pd.Timestamp]) -> List[pd.Timestamp]:
        """Return the partitions with no stored observation inside their window."""
        desired = list(desired)
        stored = existing_times(self.get_icechunk_repo(), append_dim=self.append_dim)
        if stored.size == 0:
            return desired
        stored_index = pd.DatetimeIndex(np.atleast_1d(stored))
        missing = []
        for it in desired:
            start, end = self.partition_window(it)
            if not ((stored_index >= start) & (stored_index < end)).any():
                missing.append(it)
        return missing


def concat_tables(tables: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate observation tables, tolerating empties and differing columns."""
    tables = [t for t in tables if t is not None and not t.empty]
    if not tables:
        return pd.DataFrame()
    return pd.concat(tables, ignore_index=True, sort=False)
