"""Shared machinery for point-observation stores.

Flight state vectors and marine float reports are *points*, not grids. Every row is one
observation carrying its own timestamp, and many rows legitimately share a timestamp:
two drifters report in the same minute, hundreds of aircraft are sampled in the same
second. Such a store uses ``time`` as a plain 1-D dimension coordinate with repeated
values, which breaks two assumptions the gridded write path makes:

* :func:`planetary_datasets.common.store.write_to_icechunk` drops incoming steps whose
  timestamp is already present. For point data that would silently discard a report
  merely because a *different* platform reported at the same instant.
* :meth:`~planetary_datasets.base.BaseProvider.missing_timesteps` asks whether one exact
  timestamp is stored. A point partition covers a *window* (an hour, a month) whose
  start need not appear verbatim in the data.

So :class:`PointObservationProvider` derives "already ingested?" from the partition
window, and :func:`append_point_observations` appends whole windows without
per-timestamp deduplication.
"""

from __future__ import annotations

import pathlib
from typing import List, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from loguru import logger
from xarray.coding.times import decode_cf_datetime

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import (
    STORE_READ_ERRORS,
    StoreReadError,
    build_encoding,
    has_committed_data,
)

#: Fixed width used for string variables such as ``icao24`` or ``platform_type``.
#: Appending a ``<U32`` array onto a store created from ``<U16`` data truncates silently,
#: so every write declares the same width up front.
STRING_WIDTH = 64


def naive_utc(value) -> pd.Timestamp:
    """Coerce a timestamp to timezone-naive UTC.

    Dagster hands partition windows over as tz-aware UTC timestamps while the stored
    coordinate is naive. ``np.datetime64`` of a tz-aware Timestamp drops the zone with a
    UserWarning, so the conversion is done explicitly here instead.
    """
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return stamp


def partition_bounds(it, freq: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return the half-open ``[start, end)`` window a partition timestamp covers."""
    start = naive_utc(it)
    return start, start + pd.tseries.frequencies.to_offset(freq)


def window_has_data(times: np.ndarray, start, end) -> bool:
    """True when any value in ``times`` falls in the half-open window ``[start, end)``."""
    if times is None or np.size(times) == 0:
        return False
    stamps = np.asarray(times, dtype="datetime64[ns]")
    lo = np.datetime64(naive_utc(start), "ns")
    hi = np.datetime64(naive_utc(end), "ns")
    return bool(((stamps >= lo) & (stamps < hi)).any())


def widen_string_vars(ds: xr.Dataset, width: int = STRING_WIDTH) -> xr.Dataset:
    """Cast object/str variables and coordinates to a fixed-width unicode dtype.

    ERDDAP and the OpenSky CSVs hand back ``object`` arrays of Python strings whose
    inferred width varies from one month to the next. Pinning the width keeps appends
    compatible with the store that the first write created.
    """
    out = ds.copy()
    for name, var in list(out.variables.items()):
        if var.dtype.kind in ("O", "U", "S"):
            out[name] = var.astype(f"<U{width}")
    return out


def clip_to_window(ds: xr.Dataset, start, end, dim: str = "time") -> xr.Dataset:
    """Drop observations outside ``[start, end)`` so partitions stay disjoint.

    Without this a re-run of a partition whose source file spills past its nominal hour
    would append those stray rows a second time, because the window check below would
    not see them as belonging to the partition.
    """
    stamps = np.asarray(ds[dim].values, dtype="datetime64[ns]")
    lo = np.datetime64(naive_utc(start), "ns")
    hi = np.datetime64(naive_utc(end), "ns")
    keep = np.flatnonzero((stamps >= lo) & (stamps < hi))
    if keep.size != stamps.size:
        logger.warning(
            f"dropping {stamps.size - keep.size} of {stamps.size} observations outside "
            f"{naive_utc(start)} .. {naive_utc(end)}"
        )
    return ds.isel({dim: keep})


def append_point_observations(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    append_dim: str = "time",
    message: str | None = None,
) -> bool:
    """Create or append to a point store. Returns True when something was committed.

    Unlike the gridded writer this does *not* drop incoming rows whose timestamp is
    already stored; duplicate timestamps are expected and meaningful here. Callers are
    responsible for not re-running a window that is already in the store, which
    :class:`PointObservationProvider` does via the window check.
    """
    if append_dim not in ds.coords:
        raise ValueError(f"dataset has no {append_dim!r} coordinate to append along")
    if ds.sizes.get(append_dim, 0) == 0:
        logger.warning("no observations to write, skipping")
        return False

    session = repo.writable_session("main")
    try:
        existing = xr.open_zarr(session.store, consolidated=False)
        first_write = append_dim not in existing.coords
    except STORE_READ_ERRORS as exc:
        # A read failure against a store that already holds data must not fall through
        # to the create-fresh path: that would replace the whole archive with one window.
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but could not be read ({type(exc).__name__}: {exc}). "
                "Refusing to overwrite it with a fresh write."
            ) from exc
        logger.debug(f"creating new point store ({type(exc).__name__})")
        existing = None
        first_write = True

    first = np.atleast_1d(ds[append_dim].values)[0]
    count = ds.sizes[append_dim]

    if first_write:
        to_icechunk(ds, session, encoding=build_encoding(ds, append_dim=append_dim))
        session.commit(message or f"Initial write of {count} observations from {first}")
        logger.info(f"created point store with {count} observation(s)")
        return True

    if set(ds.data_vars) != set(existing.data_vars):
        only_new = set(ds.data_vars) - set(existing.data_vars)
        only_old = set(existing.data_vars) - set(ds.data_vars)
        logger.error(
            f"variable mismatch, skipping write. only in new: {sorted(only_new)}; "
            f"only in store: {sorted(only_old)}"
        )
        return False

    to_icechunk(ds, session, append_dim=append_dim)
    session.commit(
        message or f"Append {count} observations from {first}",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"appended {count} observation(s) starting {first}")
    return True


#: Values read per block when scanning a stored coordinate. 4M int64s is 32 MB.
COORD_SCAN_BLOCK = 4_000_000


def _decode_block(block: np.ndarray, attrs: dict) -> np.ndarray:
    """Decode a raw block of a stored time coordinate to datetime64."""
    if block.dtype.kind == "M":
        return block.astype("datetime64[ns]")
    units = attrs.get("units")
    if not units:
        raise ValueError("stored time coordinate is numeric but carries no CF units")
    return decode_cf_datetime(block, units, attrs.get("calendar", "standard")).astype(
        "datetime64[ns]"
    )


def stored_windows(
    repo: icechunk.Repository,
    windows: Sequence[tuple[pd.Timestamp, pd.Timestamp]],
    append_dim: str = "time",
    block: int = COORD_SCAN_BLOCK,
) -> List[bool]:
    """For each ``[start, end)`` window, whether the store already holds observations.

    The coordinate is read block by block straight from zarr rather than through
    :func:`~planetary_datasets.common.store.existing_times`. That helper hands back the
    whole coordinate as one numpy array, and for a mature point archive — hundreds of
    thousands of observations an hour, for years — that is tens of GB pulled into memory
    just to answer "has this hour been ingested?". Here memory is bounded by ``block``
    regardless of how large the store grows.

    Every window not yet satisfied is carried into the next block, so one pass answers
    the whole list, and the scan stops early once all of them are.
    """
    answers = [False] * len(windows)
    if not windows:
        return answers

    # STORE_READ_ERRORS is a tuple, so it is unpacked rather than nested: a nested tuple
    # makes `except` raise TypeError instead of catching anything.
    try:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
        array = group[append_dim]
    except (*STORE_READ_ERRORS, KeyError, zarr.errors.GroupNotFoundError) as exc:
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but its {append_dim!r} coordinate could not "
                f"be read ({type(exc).__name__}: {exc}). Refusing to report it as empty."
            ) from exc
        logger.debug(f"point store is empty ({type(exc).__name__})")
        return answers

    total = int(array.shape[0])
    if total == 0:
        return answers

    attrs = dict(array.attrs)
    bounds = [
        (np.datetime64(naive_utc(s), "ns"), np.datetime64(naive_utc(e), "ns")) for s, e in windows
    ]
    for offset in range(0, total, block):
        stamps = _decode_block(np.asarray(array[offset : offset + block]), attrs)
        for index, (lo, hi) in enumerate(bounds):
            if not answers[index] and ((stamps >= lo) & (stamps < hi)).any():
                answers[index] = True
        if all(answers):
            break
    return answers


class PointObservationProvider(BaseProvider):
    """A :class:`~planetary_datasets.base.BaseProvider` for ragged point archives.

    Subclasses set :attr:`partition_freq` to the pandas offset alias describing how much
    time one partition covers (``"h"`` for the hourly OpenSky archives, ``"MS"`` for the
    monthly OSMC pulls) alongside the usual :attr:`name` and :attr:`store_prefix`.
    """

    #: Pandas offset alias for the span of a single partition.
    partition_freq: str = "h"

    def partition_bounds(self, it) -> tuple[pd.Timestamp, pd.Timestamp]:
        """The half-open ``[start, end)`` window covered by partition ``it``."""
        return partition_bounds(it, self.partition_freq)

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Return the partitions whose window holds no observations yet.

        One bounded-memory pass over the stored coordinate answers the whole list, so
        this costs the same whether Dagster asks about one partition or a year of them.
        """
        desired = list(desired)
        windows = [self.partition_bounds(it) for it in desired]
        covered = stored_windows(
            self.get_icechunk_repo(), windows, append_dim=self.append_dim
        )
        return [it for it, done in zip(desired, covered) if not done]

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Append the window without per-timestamp deduplication."""
        count = processed.sizes.get(self.append_dim, 0)
        first = np.atleast_1d(processed[self.append_dim].values)[0] if count else "nothing"
        return append_point_observations(
            repo,
            processed,
            append_dim=self.append_dim,
            message=f"{self.name}: {count} observations from {first}",
        )

    def local_dir(self, *parts: str) -> pathlib.Path:
        """A directory under the configured data directory, created on demand."""
        path = self.config.data_dir.joinpath(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path
