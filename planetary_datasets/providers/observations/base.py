"""Shared machinery for surface observation networks.

Every network in this subpackage has the same shape. For one partition — a day, a month,
a year — you hit an endpoint once per station (or once for the whole network), parse each
response into a dataframe indexed by UTC time, and stack the per-station frames into a
dense ``(time, station)`` cube that can be appended to an Icechunk store.

Two pieces are provided:

* :func:`frames_to_dataset` — the reshape. Use it directly when a network is fetched in
  one bulk request rather than station by station.
* :class:`StationObservationProvider` — a :class:`~planetary_datasets.base.BaseProvider`
  for the per-station case. Subclasses implement :meth:`~StationObservationProvider.stations`,
  :meth:`~StationObservationProvider.fetch_station` and
  :meth:`~StationObservationProvider.read_station` and get fetching, parsing, gridding,
  and the Icechunk write for free.

The station axis
----------------
Appending along ``time`` requires the station axis to be byte-identical between the
incoming partition and the store. Every provider therefore reindexes onto a *canonical*
station list — the full network roster, not "whichever stations reported today" — and
leaves absent stations as NaN. A station list that later gains or loses entries will be
refused by :func:`~planetary_datasets.common.store.write_to_icechunk` with a message
naming the ``station`` coordinate rather than silently corrupting the store.
"""

from __future__ import annotations

import os
import pathlib
import re
import time
import uuid
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import ALIGNMENT_COORDS

STATION_DIM = "station"
TIME_DIM = "time"

#: Coordinates that must line up before an append. The base tuple covers gridded data;
#: ``station`` is what matters for point observations.
OBSERVATION_ALIGNMENT_COORDS = (*ALIGNMENT_COORDS, STATION_DIM)

_UNSAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]")


class NoStationDataError(RuntimeError):
    """Files were fetched for a partition but none of them yielded usable rows.

    Distinct from "the archive has no data here", which is signalled by an empty
    :meth:`~planetary_datasets.base.BaseProvider.fetch`. Raised rather than returning an
    empty dataset so the partition is retried instead of being recorded as complete.
    """


@dataclass(frozen=True)
class Station:
    """One observing site.

    Attributes:
        id: Network identifier. Used as the ``station`` coordinate value and, after
            sanitising, as the download filename, so it must be unique within a network.
        latitude: Degrees north.
        longitude: Degrees east, on -180 to 180.
        elevation: Metres above sea level.
        name: Human readable site name, kept only for metadata.
    """

    id: str
    latitude: float | None = None
    longitude: float | None = None
    elevation: float | None = None
    name: str | None = None

    @property
    def safe_id(self) -> str:
        """The identifier with anything awkward in a filename replaced."""
        return _UNSAFE_FILENAME.sub("_", self.id)


def safe_filename(station_id: str) -> str:
    """Filename-safe form of a station id, matching :attr:`Station.safe_id`."""
    return _UNSAFE_FILENAME.sub("_", station_id)


def partial_path(dest: pathlib.Path) -> pathlib.Path:
    """A private scratch name to download ``dest`` through before renaming it into place.

    The name carries the process id and a random suffix. Several providers cache a
    station-year file that all twelve of that year's monthly partitions reuse, and a
    Dagster backfill happily runs those partitions at once; a fixed ``.part`` name would
    let two of them interleave writes and then promote the result as if it were sound.
    """
    return dest.with_name(f"{dest.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.part")


def existing_station_axis(repo, station_dim: str = STATION_DIM) -> list[str] | None:
    """Return the station axis a store already uses, or None when it has none yet.

    Providers whose upstream gives no station roster use this to pin later partitions to
    the axis the first write established, instead of letting the append be refused.
    """
    try:
        ds = xr.open_zarr(
            repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
        )
    except Exception as exc:  # noqa: BLE001 - an unreadable store is handled by the writer
        logger.debug(f"could not read the station axis ({type(exc).__name__}: {exc})")
        return None
    if station_dim not in ds.coords:
        return None
    return [str(value) for value in ds.coords[station_dim].values]


def partition_time_index(
    it: pd.Timestamp,
    partition_freq: str,
    sample_freq: str,
) -> pd.DatetimeIndex:
    """Regular time grid covering the partition starting at ``it``.

    The grid is half open — it starts at ``it`` and stops before the next partition — so
    consecutive partitions tile the archive without repeating a timestep.
    """
    start = pd.Timestamp(it)
    end = start + pd.tseries.frequencies.to_offset(partition_freq)
    return pd.date_range(start, end, freq=sample_freq, inclusive="left")


def align_to_grid(
    df: pd.DataFrame,
    times: pd.DatetimeIndex,
    sample_freq: str,
    how: str = "exact",
    tolerance: str | None = None,
) -> pd.DataFrame:
    """Put an irregularly sampled station frame onto the regular grid ``times``.

    Args:
        df: Observations indexed by UTC time. A tz-aware index is converted to UTC and
            made naive, because the store's time axis is naive UTC.
        times: The target grid.
        sample_freq: Spacing of ``times``, needed by the resampling modes.
        how: ``exact`` reindexes and keeps only timestamps that land on the grid;
            ``nearest`` snaps within ``tolerance``; ``first``/``last``/``mean``/``max``/
            ``min`` resample each grid interval with that aggregation.
        tolerance: Maximum distance for ``nearest``, e.g. ``"30min"``.
    """
    if df.empty:
        return pd.DataFrame(index=times, columns=df.columns, dtype="float64")

    index = pd.DatetimeIndex(df.index)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    df = df.set_axis(index).sort_index()
    # Two reports for the same instant happen constantly in METAR-derived archives
    # (a routine plus a special). Keep the later one rather than letting reindex raise.
    df = df[~df.index.duplicated(keep="last")]

    if how == "exact":
        return df.reindex(times)
    if how == "nearest":
        return df.reindex(times, method="nearest", tolerance=tolerance)
    if how in ("first", "last", "mean", "max", "min"):
        resampler = df.resample(sample_freq, origin=times[0])
        return getattr(resampler, how)().reindex(times)
    raise ValueError(f"unknown alignment mode {how!r}")


def download_or_none(
    url: str,
    dest: pathlib.Path,
    timeout: float = 120.0,
    retries: int = 3,
    backoff: float = 2.0,
) -> pathlib.Path | None:
    """Download ``url`` to ``dest``, returning None only for a genuine 404.

    Distinguishing "the archive has no file here" from "the server is having a bad day"
    matters: an empty fetch is recorded as a completed partition and never retried, so
    anything that might succeed later has to raise instead.
    """
    import requests

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = partial_path(dest)
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            part.write_bytes(response.content)
            part.replace(dest)
            return dest
        except requests.RequestException as exc:
            last = exc
            part.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"{retries} attempts failed for {url}: {last}")


def save_frame(df: pd.DataFrame, path: pathlib.Path) -> pathlib.Path:
    """Write a station frame to CSV so ``fetch`` and ``process`` stay separable.

    Several networks are read through a library that does its own HTTP and hands back a
    dataframe. Rather than short-circuit the provider lifecycle, the frame is parked on
    disk in ``fetch`` and read back in ``process``, which keeps the fetched inputs
    inspectable and the memory profile the same as the download-based providers.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index_label="time")
    return path


def load_frame(path: pathlib.Path) -> pd.DataFrame:
    """Read back a frame written by :func:`save_frame`."""
    df = pd.read_csv(path, index_col="time")
    return df.set_axis(pd.DatetimeIndex(pd.to_datetime(df.index, errors="coerce", utc=True)))


def frames_to_dataset(
    frames: Mapping[str, pd.DataFrame],
    stations: Sequence[Station],
    times: pd.DatetimeIndex,
    variables: Sequence[str] | None = None,
    sample_freq: str = "1h",
    how: str = "exact",
    tolerance: str | None = None,
    attrs: Mapping[str, str] | None = None,
    dtype: str = "float32",
) -> xr.Dataset:
    """Stack per-station frames into a dense ``(time, station)`` dataset.

    Args:
        frames: Station id to observations indexed by time. Stations absent from this
            mapping still appear in the output, filled with NaN.
        stations: The canonical station roster. Defines the ``station`` axis and its
            order; see the module docstring for why this must not vary per partition.
        times: The time axis, usually from :func:`partition_time_index`.
        variables: Data variables to write, in order. When omitted the sorted union of
            the frames' columns is used, which is convenient but makes the variable set
            depend on what happened to arrive — declare it explicitly for anything that
            appends to a long-lived store.
        sample_freq: Spacing of ``times``, passed to :func:`align_to_grid`.
        how: Alignment mode, passed to :func:`align_to_grid`.
        tolerance: Alignment tolerance, passed to :func:`align_to_grid`.
        attrs: Dataset attributes, typically naming the network and its source URL.
        dtype: Storage dtype for the data variables.
    """
    station_ids = [s.id for s in stations]
    if len(set(station_ids)) != len(station_ids):
        raise ValueError("station ids must be unique; the station axis is keyed on them")
    position = {sid: i for i, sid in enumerate(station_ids)}

    unknown = set(frames) - set(position)
    if unknown:
        raise ValueError(
            f"frames reference {len(unknown)} station(s) missing from the canonical list, "
            f"e.g. {sorted(unknown)[:5]}"
        )

    if variables is None:
        variables = sorted({str(c) for df in frames.values() for c in df.columns})
        logger.debug(f"inferred {len(variables)} variable(s) from the fetched frames")
    variables = list(variables)

    arrays = {
        var: np.full((len(times), len(station_ids)), np.nan, dtype=dtype) for var in variables
    }
    for station_id, df in frames.items():
        aligned = align_to_grid(df, times, sample_freq, how=how, tolerance=tolerance)
        column = position[station_id]
        for var in variables:
            if var not in aligned.columns:
                continue
            values = pd.to_numeric(aligned[var], errors="coerce").to_numpy(dtype="float64")
            arrays[var][:, column] = values

    coords: dict[str, object] = {
        TIME_DIM: pd.DatetimeIndex(times),
        # Object dtype keeps zarr from padding every id to the longest one, and lets a
        # later station list with longer names still compare equal element-wise.
        STATION_DIM: np.array(station_ids, dtype=object),
    }
    for field in ("latitude", "longitude", "elevation"):
        values = [getattr(s, field) for s in stations]
        if any(v is not None for v in values):
            coords[field] = (
                STATION_DIM,
                np.array([np.nan if v is None else v for v in values], dtype="float64"),
            )
    names = [s.name for s in stations]
    if any(n is not None for n in names):
        coords["station_name"] = (
            STATION_DIM,
            np.array(["" if n is None else n for n in names], dtype=object),
        )

    ds = xr.Dataset(
        {var: ((TIME_DIM, STATION_DIM), arrays[var]) for var in variables},
        coords=coords,
        attrs=dict(attrs or {}),
    )
    return ds


class StationObservationProvider(BaseProvider):
    """Base for networks fetched one station at a time.

    Subclasses implement three methods and set a handful of class attributes; fetching in
    parallel, gridding, reindexing onto the canonical station axis and the Icechunk write
    are handled here.
    """

    append_dim = TIME_DIM
    station_dim = STATION_DIM

    #: pandas offset spanning one partition, e.g. ``"D"``, ``"MS"``, ``"YS"``.
    partition_freq: str = "D"
    #: Spacing of the regular time grid inside a partition.
    sample_freq: str = "1h"
    #: How raw observations are put on that grid. See :func:`align_to_grid`.
    align_how: str = "exact"
    align_tolerance: str | None = None
    #: Data variables written, in order. Declared explicitly so every partition writes the
    #: same set; an empty tuple falls back to the union of what was fetched.
    variables: tuple[str, ...] = ()
    #: Parallel station downloads. The public endpoints here rate limit, so keep it modest.
    max_workers: int = 8
    #: Free-form provenance recorded on the dataset.
    source_url: str = ""
    #: Station tables align on the station axis as well as the grid coordinates.
    alignment_coords = OBSERVATION_ALIGNMENT_COORDS

    def __init__(self, config=None, stations: Sequence[Station] | Iterable[str] | None = None):
        """Build the provider.

        Args:
            config: Optional configuration override.
            stations: Restrict the network to these stations. Accepts
                :class:`Station` objects or bare ids, which are looked up in the full
                roster. Mostly useful for tests and targeted backfills; note that a store
                written with a subset can only ever be appended to with that same subset.
        """
        super().__init__(config=config)
        self._station_override = stations
        self._stations: list[Station] | None = None

    # -- to implement -------------------------------------------------------------

    @abstractmethod
    def all_stations(self) -> list[Station]:
        """Return the network's full station roster."""

    @abstractmethod
    def fetch_station(
        self,
        station: Station,
        it: pd.Timestamp,
        temp_dir: pathlib.Path,
    ) -> pathlib.Path | None:
        """Download one station's observations for the partition.

        Return the local path, or None when the archive genuinely has nothing for this
        station and partition. Raise for transient failures so the partition is retried
        rather than recorded as empty.
        """

    @abstractmethod
    def read_station(
        self,
        path: pathlib.Path,
        station: Station,
        it: pd.Timestamp,
    ) -> pd.DataFrame | None:
        """Parse a downloaded file into a dataframe indexed by UTC time."""

    # -- provided -----------------------------------------------------------------

    def station_list(self) -> list[Station]:
        """The canonical station axis for this provider, resolved once and cached."""
        if self._stations is not None:
            return self._stations

        override = self._station_override
        if override is None:
            stations = list(self.all_stations())
        else:
            override = list(override)
            if override and isinstance(override[0], Station):
                stations = list(override)
            else:
                wanted = {str(s) for s in override}
                roster = {s.id: s for s in self.all_stations()}
                missing = sorted(wanted - set(roster))
                if missing:
                    raise ValueError(
                        f"{self.name}: unknown station id(s) {missing[:5]}"
                        + (f" and {len(missing) - 5} more" if len(missing) > 5 else "")
                    )
                # Keep the caller's order so the store's station axis is predictable.
                stations = [roster[str(s)] for s in override]

        if not stations:
            raise ValueError(
                f"{self.name}: station roster is empty, refusing to build an empty axis"
            )
        self._stations = stations
        return stations

    def partition_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The time grid this partition writes."""
        return partition_time_index(it, self.partition_freq, self.sample_freq)

    def station_filename(self, station: Station, it: pd.Timestamp) -> str:
        """Name of the local file for one station and partition."""
        return f"{station.safe_id}.dat"

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> list[str]:
        """Download every station for the partition, in parallel.

        Returns the paths that produced data. A station the archive does not cover is
        skipped quietly. If nothing at all came back *and* at least one station raised,
        the partition is failed rather than reported empty: an empty fetch is recorded as
        "done" and never retried, so a transient outage must not be able to produce one.
        """
        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory.mkdir(parents=True, exist_ok=True)
        stations = self.station_list()

        failures: list[str] = []

        def _one(station: Station) -> pathlib.Path | None:
            try:
                return self.fetch_station(station, it, directory)
            except Exception as exc:  # noqa: BLE001 - one bad station must not sink the day
                failures.append(f"{station.id}: {exc}")
                return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(_one, stations))

        paths = [str(p) for p in results if p is not None]
        if failures and not paths:
            # Not "every station failed": most of a roster is usually absent from any
            # given partition, so a handful of errors can still be the whole of what was
            # available. Nothing fetched plus any error means the partition is unproven.
            raise RuntimeError(
                f"{self.name}: nothing fetched for {it} and {len(failures)} of "
                f"{len(stations)} station request(s) failed; first error was {failures[0]}"
            )
        if failures:
            logger.warning(
                f"{self.name}: {len(failures)}/{len(stations)} station(s) failed for {it}"
            )
        logger.info(f"{self.name}: {len(paths)}/{len(stations)} station(s) returned data for {it}")
        return paths

    def process(
        self,
        input_files: list[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Parse the fetched files and stack them into a ``(time, station)`` cube."""
        stations = self.station_list()
        by_filename = {self.station_filename(s, it): s for s in stations}

        frames: dict[str, pd.DataFrame] = {}
        for path in input_files:
            path = pathlib.Path(path)
            station = by_filename.get(path.name)
            if station is None:
                logger.warning(f"{self.name}: {path.name} does not match any station, ignoring")
                continue
            frame = self.read_station(path, station, it)
            if frame is None or frame.empty:
                continue
            frames[station.id] = frame

        if not frames:
            raise NoStationDataError(
                f"{self.name}: fetched {len(input_files)} file(s) for {it} "
                "but none parsed into rows"
            )

        ds = frames_to_dataset(
            frames,
            stations,
            self.partition_times(it),
            variables=self.variables or None,
            sample_freq=self.sample_freq,
            how=self.align_how,
            tolerance=self.align_tolerance,
            attrs=self.dataset_attrs(),
        )
        return self.finalise(ds, it)

    def dataset_attrs(self) -> dict[str, str]:
        """Provenance attributes written onto the dataset."""
        attrs = {"network": self.name}
        if self.source_url:
            attrs["source"] = self.source_url
        return attrs

    def finalise(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Hook for last-minute adjustments. The default returns ``ds`` unchanged."""
        return ds

