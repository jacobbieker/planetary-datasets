"""Sondehub amateur radiosonde telemetry, read from the public S3 history archive.

Sondehub publishes every tracked radiosonde flight in ``s3://sondehub-history``, laid out
two ways: ``date/YYYY/MM/DD/<serial>.json`` holds a subsampled day of every flight that
reported that day, and ``serial/<serial>.json.gz`` holds one flight at full rate. The
day layout is what a time-partitioned store wants; the serial layout is what you want for
a single flight.

:class:`SondehubProvider` covers both, because they answer different questions and neither
substitutes for the other:

* ``fetch``/``process`` build the **day-partitioned cube** from the subsampled ``date/``
  layout — the ordinary provider contract, one partition per UTC day.
* :meth:`SondehubProvider.write_flights` and :meth:`SondehubProvider.select_serial` build
  and read a **per-flight store** from the full-rate ``serial/`` layout. One NetCDF per
  flight is fine for archival but hopeless to query: 1.5M files of a few hundred KB each,
  with no way to ask "every flight from this launch site last March".

The per-flight store uses the CF *contiguous ragged array* layout — every flight's samples
concatenated end to end along one ``obs`` dimension, with ``obs_start``/``obs_count`` on a
``flight`` dimension saying where each one lives. Flights run from ~1k to ~450k samples,
which is exactly what regular chunking handles badly: size the chunk for the big flights
and small ones share a chunk with dozens of others; size it for the small ones and a big
flight scatters over hundreds. So the ``obs`` arrays use Zarr v3 **rectilinear
(variable-width) chunks**, with every chunk boundary landing on a flight boundary. Chunks
come out near-uniform in bytes *and* no flight is ever split, so reading one serial touches
exactly one chunk — never two, never a partial. Selection by serial is a binary search over
a sorted index array: two small reads plus one chunk.

The archive is read with :mod:`fsspec` directly rather than through the ``sondehub``
package. The package's ``download`` helper starts fifty unbounded threads per call and
pulls a whole prefix into memory, which is what made the original script unusable on a
busy day; going through fsspec also drops a dependency.
"""

from __future__ import annotations

import gzip
import json
import pathlib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Sequence

import fsspec
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger
from zarr.codecs import BloscCodec

from planetary_datasets.config import Config
from planetary_datasets.providers.observations.upper_air_common import (
    PointObservationProvider,
    concat_tables,
    table_to_dataset,
)

#: Public, anonymously readable bucket. Not a credential.
SONDEHUB_BUCKET = "sondehub-history"

#: Frame field to store variable. Sondehub frames are JSON with every value as a string.
SONDEHUB_FIELDS = {
    "lat": "latitude",
    "lon": "longitude",
    "alt": "altitude",
    "vel_h": "horizontal_velocity",
    "vel_v": "vertical_velocity",
    "pressure": "pressure",
    "temp": "temperature",
    "humidity": "humidity",
    "heading": "heading",
    "sats": "satellites",
    "serial": "serial",
    "type": "sonde_type",
    "launch_site": "launch_site",
}

SONDEHUB_SCHEMA = {
    "latitude": "float32",
    "longitude": "float32",
    "altitude": "float32",
    "horizontal_velocity": "float32",
    "vertical_velocity": "float32",
    "pressure": "float32",
    "temperature": "float32",
    "humidity": "float32",
    "heading": "float32",
    "satellites": "float32",
    "serial": "str",
    "sonde_type": "str",
    "launch_site": "str",
}


def _filesystem(storage_options: dict | None = None):
    """An anonymous S3 filesystem. The archive is public; signing it would fail."""
    return fsspec.filesystem("s3", anon=True, **(storage_options or {}))


def list_serials(
    day: pd.Timestamp | str,
    bucket: str = SONDEHUB_BUCKET,
    storage_options: dict | None = None,
) -> List[str]:
    """The serials that flew on ``day``, taken from the listable ``date/`` index.

    Not a listing of ``serial/``: that prefix no longer grants ``ListBucket`` and returns
    Access Denied, which is what broke ``sondehub.download(serial=...)`` too. The objects
    themselves are still public, so a direct read of ``serial/<serial>.json.gz`` works —
    only the enumeration had to move. ``date/`` *is* listable and is the archive's own
    index of which serials flew when, so it is used purely as that; the day objects hold a
    three-frame daily summary and are never the data source for a flight.
    """
    return [
        key.rsplit("/", 1)[-1].removesuffix(".json")
        for key in list_day_keys(day, bucket=bucket, storage_options=storage_options)
    ]


def list_day_keys(
    day: pd.Timestamp | str,
    bucket: str = SONDEHUB_BUCKET,
    storage_options: dict | None = None,
) -> List[str]:
    """List the per-sonde JSON objects the archive holds for one UTC day.

    Returns an empty list for a day the archive does not cover, which is a genuine
    absence rather than a failure.
    """
    day = pd.Timestamp(day)
    fs = _filesystem(storage_options)
    prefix = f"{bucket}/date/{day:%Y/%m/%d}/"
    try:
        keys = fs.ls(prefix, detail=False)
    except FileNotFoundError:
        logger.info(f"sondehub archive has no data for {day:%Y-%m-%d}")
        return []
    return sorted(key for key in keys if key.endswith(".json"))


def read_frames(key: str, storage_options: dict | None = None) -> List[dict]:
    """Read the frames from one archive object, local path or ``s3://`` key.

    Both ``.json`` and gzipped ``.json.gz`` objects are handled; a file holding a single
    frame object rather than a list is normalised to a one-element list.
    """
    path = str(key)
    if path.startswith("s3://") or not pathlib.Path(path).exists():
        fs = _filesystem(storage_options)
        opener = fs.open(path.removeprefix("s3://"), "rb")
    else:
        opener = open(path, "rb")
    with opener as handle:
        raw = handle.read()
    if path.endswith(".gz"):
        raw = gzip.decompress(raw)
    frames = json.loads(raw)
    if isinstance(frames, dict):
        return [frames]
    return list(frames)


def download_serial(serial: str, storage_options: dict | None = None) -> List[dict]:
    """Read every frame of one flight from the per-serial layout."""
    return read_frames(f"{SONDEHUB_BUCKET}/serial/{serial}.json.gz", storage_options)


#: Fields that describe the *receiver*, not the sonde, so they must not survive a merge.
_RECEIVER_FIELDS = frozenset(
    {"uploader_callsign", "uploader_position", "uploader_antenna", "snr", "rssi", "time_received"}
)


def merge_frames(frames: Iterable[dict]) -> tuple[List[dict], "Counter"]:
    """Collapse multi-receiver uploads into one record per timestamp.

    Every receiver that hears a sonde uploads its own copy of each frame, so raw frame
    counts run 2-20x the number of distinct observations. Duplicates are *merged* rather
    than dropped: receivers disagree about which optional fields they decode, so a
    first-wins dedupe loses pressure and humidity readings that a later upload of the same
    timestamp carries. Fields absent from the first upload are filled from subsequent ones.

    Returns the merged records in time order, and a count of raw uploads per timestamp.
    """
    merged: dict[pd.Timestamp, dict] = {}
    uploads: Counter = Counter()
    for frame in frames:
        if not isinstance(frame, dict) or "datetime" not in frame:
            continue
        stamp = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
        if pd.isna(stamp):
            continue
        uploads[stamp] += 1
        record = merged.get(stamp)
        if record is None:
            merged[stamp] = {
                k: v for k, v in frame.items() if k not in _RECEIVER_FIELDS and v is not None
            }
        else:
            for key, value in frame.items():
                if value is not None and key not in _RECEIVER_FIELDS and record.get(key) is None:
                    record[key] = value
    return [merged[stamp] for stamp in sorted(merged)], uploads


def frames_to_table(frames: Iterable[dict], merge: bool = True) -> pd.DataFrame:
    """Turn raw Sondehub frames into a table, one row per distinct observation time.

    Frames carry every value as a string and omit fields the receiver did not decode, so
    absent fields become missing rather than an error.

    Args:
        frames: Raw frames as published.
        merge: Collapse multi-receiver uploads of the same timestamp with
            :func:`merge_frames`. Pass False to keep one row per raw upload, which is what
            you want when counting receiver coverage rather than reading telemetry.
    """
    records = merge_frames(frames)[0] if merge else [f for f in frames if "datetime" in f]
    rows = []
    for frame in records:
        row = {"time": frame["datetime"]}
        for source, target in SONDEHUB_FIELDS.items():
            if source in frame:
                row[target] = frame[source]
        rows.append(row)
    return pd.DataFrame(rows)


def flight_summary(serial: str, frames: Sequence[dict]) -> dict:
    """Summarise one flight: frame count, start and end time, maximum altitude.

    Replaces the running numpy pickle the original script accumulated. Returns a record
    the caller can collect however it likes.
    """
    table = frames_to_table(frames)
    if table.empty:
        return {"serial": serial, "frames": 0, "start": None, "end": None, "max_altitude": None}
    times = pd.to_datetime(table["time"], utc=True, errors="coerce").dropna()
    # frames_to_table only creates a column for a field some frame actually carried, so a
    # flight whose receiver never decoded altitude has no altitude column at all.
    if "altitude" in table.columns:
        altitudes = pd.to_numeric(table["altitude"], errors="coerce")
    else:
        altitudes = pd.Series(dtype="float64")
    return {
        "serial": serial,
        "frames": int(len(table)),
        "start": times.min() if not times.empty else None,
        "end": times.max() if not times.empty else None,
        "max_altitude": float(altitudes.max()) if altitudes.notna().any() else None,
    }


# ---------------------------------------------------------------------------------------
# Per-flight ragged store
# ---------------------------------------------------------------------------------------

#: Samples per chunk, before flight-boundary rounding. 131072 float32 is a 512 KB chunk
#: holding ~15 median flights: small enough that pulling one serial is a single modest
#: read, large enough to keep the chunk count — and so the manifest — manageable across the
#: whole archive.
TARGET_CHUNK_POINTS = 131_072

#: Chunk length for the per-flight metadata arrays, which are one scalar per flight.
FLIGHT_CHUNK = 65_536

#: Per-sample variables and their stored dtype. ``latitude``/``longitude`` keep float64
#: because five-decimal degrees needs more mantissa than float32 has, and
#: ``frame_counter`` because counters run past 1e9. The rest are instrument readings where
#: float32 is already well past the sensor's precision.
OBS_VARS: dict[str, str] = {
    "latitude": "f8",
    "longitude": "f8",
    "altitude": "f4",
    "vertical_velocity": "f4",
    "horizontal_velocity": "f4",
    "heading": "f4",
    "pressure": "f4",
    "temperature": "f4",
    "humidity": "f4",
    "satellites": "f4",
    "frame_counter": "f8",
    "receiver_count": "i2",
}

#: Per-flight string metadata, lifted from the flight's global attributes.
FLIGHT_STRS: tuple[str, ...] = (
    "serial",
    "sonde_type",
    "launch_site",
    "launch_site_name",
)

#: Per-flight numeric metadata.
FLIGHT_NUMS: dict[str, str] = {
    "launch_site_latitude": "f8",
    "launch_site_longitude": "f8",
    "launch_site_altitude": "f4",
    "max_altitude": "f4",
    "duration_seconds": "f4",
    "n_frames": "i4",
    "n_raw_uploads": "i4",
}

#: Per-flight timestamps, stored as int64 nanoseconds.
FLIGHT_TIMES: tuple[str, ...] = ("start_time", "end_time")

#: Sentinel for an absent timestamp. NaT does not survive the int64 round trip.
_NO_TIME = np.iinfo(np.int64).min

FLIGHT_VAR_ATTRS: dict[str, dict] = {
    "latitude": {"units": "degrees_north", "standard_name": "latitude"},
    "longitude": {"units": "degrees_east", "standard_name": "longitude"},
    "altitude": {"units": "m", "standard_name": "altitude", "positive": "up"},
    "vertical_velocity": {"units": "m s-1", "long_name": "ascent rate"},
    "horizontal_velocity": {"units": "m s-1", "long_name": "ground speed"},
    "heading": {"units": "degrees", "long_name": "direction of travel"},
    "pressure": {"units": "hPa", "standard_name": "air_pressure"},
    "temperature": {"units": "degC", "standard_name": "air_temperature"},
    "humidity": {"units": "%", "standard_name": "relative_humidity"},
    "satellites": {"long_name": "GNSS satellites used"},
    "frame_counter": {"long_name": "sonde frame counter"},
    "receiver_count": {"long_name": "distinct receivers that uploaded this sample"},
    "time": {"units": "nanoseconds since 1970-01-01", "calendar": "proleptic_gregorian"},
    "obs_start": {"long_name": "index of this flight's first sample in obs"},
    "obs_count": {"long_name": "number of samples", "sample_dimension": "obs"},
    "serial": {"cf_role": "trajectory_id", "long_name": "radiosonde serial number"},
}

#: Blosc/zstd with byte shuffle: the float columns are smooth and the int64 timestamps
#: share their high bytes within a chunk, so shuffling before zstd wins on both.
_FLIGHT_CODEC = [BloscCodec(cname="zstd", clevel=5, shuffle="shuffle")]


def _enable_rectilinear_chunks() -> None:
    """Rectilinear chunk grids are still behind a flag in zarr 3.4."""
    zarr.config.set({"array.rectilinear_chunks": True})


def _as_int64_time(value) -> int:
    """A timestamp as int64 nanoseconds, or :data:`_NO_TIME`."""
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return _NO_TIME
    if pd.isna(stamp):
        return _NO_TIME
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert(None)
    return int(stamp.value)


def flight_columns(ds: xr.Dataset) -> tuple[dict, np.ndarray, dict[str, np.ndarray]] | None:
    """Flatten one flight Dataset into the ``(attrs, times, columns)`` the writer takes.

    A variable the sonde never reported is *filled* rather than omitted, so every flight
    contributes the same columns and the ``obs`` arrays stay aligned. Returns None for a
    flight with no samples.
    """
    n = ds.sizes.get("time", 0)
    if not n:
        return None
    times = ds["time"].values.astype("datetime64[ns]").astype("i8")
    columns: dict[str, np.ndarray] = {}
    for name, dtype in OBS_VARS.items():
        if name in ds:
            columns[name] = np.asarray(ds[name].values).astype(dtype)
        elif dtype.startswith("i"):
            columns[name] = np.zeros(n, dtype=dtype)
        else:
            columns[name] = np.full(n, np.nan, dtype=dtype)
    return dict(ds.attrs), times, columns


def _append_array(group, name: str, values, dtype, varying: bool, attrs: dict) -> None:
    """Append ``values`` to ``group[name]``, creating the array on first use.

    On a rectilinear array ``resize`` turns the added extent into exactly one new chunk,
    which is what keeps one batch equal to one chunk equal to a whole number of flights.
    """
    if name in group:
        array = group[name]
        start = array.shape[0]
        array.resize((start + len(values),))
        array[start:] = values
        return
    chunks = ((len(values),),) if varying else (FLIGHT_CHUNK,)
    array = group.create_array(
        name, shape=(len(values),), dtype=dtype, chunks=chunks, compressors=_FLIGHT_CODEC
    )
    array[:] = values
    array.attrs.update(attrs)


def write_flight_batch(group, batch: Sequence[tuple], starts: Sequence[int], counts: Sequence[int]):
    """Append one batch of flights: their samples as a single ``obs`` chunk, then metadata."""
    times = np.concatenate([flight[1] for flight in batch])
    _append_array(group, "time", times, "i8", True, FLIGHT_VAR_ATTRS["time"])
    for name, dtype in OBS_VARS.items():
        values = np.concatenate([flight[2][name] for flight in batch])
        _append_array(group, name, values, dtype, True, FLIGHT_VAR_ATTRS.get(name, {}))

    attrs_list = [flight[0] for flight in batch]
    for name in FLIGHT_STRS:
        values = np.array([str(a.get(name, "")) for a in attrs_list], dtype=object)
        _append_array(group, name, values, str, False, FLIGHT_VAR_ATTRS.get(name, {}))
    for name, dtype in FLIGHT_NUMS.items():
        raw = np.array([_as_float(a.get(name)) for a in attrs_list], dtype="f8")
        if dtype.startswith("i"):
            raw = np.nan_to_num(raw, nan=0)
        _append_array(group, name, raw.astype(dtype), dtype, False, FLIGHT_VAR_ATTRS.get(name, {}))
    for name in FLIGHT_TIMES:
        values = np.array([_as_int64_time(a.get(name)) for a in attrs_list], dtype="i8")
        _append_array(group, name, values, "i8", False, FLIGHT_VAR_ATTRS["time"])
    _append_array(
        group, "obs_start", np.asarray(starts, "i8"), "i8", False, FLIGHT_VAR_ATTRS["obs_start"]
    )
    _append_array(
        group, "obs_count", np.asarray(counts, "i8"), "i8", False, FLIGHT_VAR_ATTRS["obs_count"]
    )


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def rebuild_serial_index(group) -> None:
    """Rewrite the sorted serial index, so selection is a binary search, not a full scan."""
    serials = np.asarray(group["serial"][:])
    order = np.argsort(serials, kind="stable")
    for name, values, dtype in (
        ("_serial_sorted", serials[order], str),
        ("_serial_flight", order.astype("i8"), "i8"),
    ):
        if name in group:
            del group[name]
        array = group.create_array(
            name,
            shape=(len(values),),
            dtype=dtype,
            chunks=(FLIGHT_CHUNK,),
            compressors=_FLIGHT_CODEC,
        )
        array[:] = values
    group.attrs["n_flights"] = int(len(serials))


class FlightWriter:
    """Append flights to the ragged store as they are produced.

    Holds a batch in memory until it reaches :data:`TARGET_CHUNK_POINTS`, then writes it as
    one rectilinear chunk. :meth:`commit` closes the current Icechunk session and opens a
    fresh one, so commit on a natural boundary — a finished day — rather than per flight.

    Only one writer may hold the store at a time.
    """

    def __init__(self, repo, target_chunk_points: int = TARGET_CHUNK_POINTS, branch: str = "main"):
        _enable_rectilinear_chunks()
        self.repo = repo
        self.branch = branch
        self.target = target_chunk_points
        self._open_session()
        self.seen = set(np.asarray(self.group["serial"][:])) if "serial" in self.group else set()
        self.obs_total = int(self.group["time"].shape[0]) if "time" in self.group else 0
        self._reset()

    def _open_session(self) -> None:
        self.session = self.repo.writable_session(self.branch)
        self.group = zarr.open_group(store=self.session.store, mode="a")

    def _reset(self) -> None:
        self.batch: list[tuple] = []
        self.starts: list[int] = []
        self.counts: list[int] = []
        self.batch_points = 0

    def __contains__(self, serial: str) -> bool:
        return serial in self.seen

    def add(self, ds: xr.Dataset) -> bool:
        """Queue one flight. False if it is empty or the store already holds its serial."""
        serial = str(ds.attrs.get("serial", ""))
        if not serial or serial in self.seen:
            return False
        flight = flight_columns(ds)
        if flight is None:
            return False
        attrs, times, _ = flight
        attrs["serial"] = serial
        self.seen.add(serial)
        self.batch.append(flight)
        self.starts.append(self.obs_total)
        self.counts.append(len(times))
        self.obs_total += len(times)
        self.batch_points += len(times)
        if self.batch_points >= self.target:
            self.flush()
        return True

    def flush(self) -> None:
        """Write the queued flights as one chunk. Does not commit."""
        if not self.batch:
            return
        write_flight_batch(self.group, self.batch, self.starts, self.counts)
        self._reset()

    def commit(self, message: str) -> str | None:
        """Flush, refresh the serial index and commit. Returns the snapshot id, or None."""
        pending = bool(self.batch)
        self.flush()
        if not pending and "serial" not in self.group:
            return None
        rebuild_serial_index(self.group)
        snapshot = self.session.commit(message)
        self._open_session()
        return snapshot


class SondeFlights:
    """Read side of the ragged store: pull flights out by serial, or filter the metadata."""

    def __init__(self, repo, branch: str = "main"):
        # Reading a rectilinear array needs the flag as much as writing one does: without
        # it, parsing the array metadata raises rather than returning a chunk grid. A
        # reader in a fresh process would otherwise fail on a store this module wrote.
        _enable_rectilinear_chunks()
        self.repo = repo
        self.session = repo.readonly_session(branch)
        self.group = zarr.open_group(store=self.session.store, mode="r")
        self._sorted: np.ndarray | None = None
        self._index: np.ndarray | None = None
        self._meta: dict[str, np.ndarray] = {}

    def __len__(self) -> int:
        return int(self.group["serial"].shape[0]) if "serial" in self.group else 0

    def _lookup(self, serial: str) -> int:
        if self._sorted is None:
            self._sorted = np.asarray(self.group["_serial_sorted"][:])
            self._index = np.asarray(self.group["_serial_flight"][:])
        pos = int(np.searchsorted(self._sorted, serial))
        if pos >= len(self._sorted) or self._sorted[pos] != serial:
            raise KeyError(f"serial {serial!r} is not in the store")
        return int(self._index[pos])

    def _column(self, name: str) -> np.ndarray:
        """A whole flight-dimension column, cached.

        Reading the metadata scalars one at a time costs ~2.5 ms each in per-read overhead
        regardless of chunk size, which dwarfs the flight read itself. Pulling each column
        once and keeping it makes every subsequent flight's metadata free.
        """
        if name not in self._meta:
            self._meta[name] = np.asarray(self.group[name][:])
        return self._meta[name]

    def sel(self, serial: str) -> xr.Dataset:
        """One flight as a Dataset, read from a single chunk per variable."""
        flight = self._lookup(serial)
        start = int(self._column("obs_start")[flight])
        stop = start + int(self._column("obs_count")[flight])
        data = {
            name: ("time", np.asarray(self.group[name][start:stop]), FLIGHT_VAR_ATTRS.get(name, {}))
            for name in OBS_VARS
            if name in self.group
        }
        times = np.asarray(self.group["time"][start:stop]).astype("datetime64[ns]")
        ds = xr.Dataset(data, coords={"time": times})
        ds.attrs = self.flight_attrs(flight)
        return ds

    def flight_attrs(self, flight: int) -> dict:
        """The per-flight metadata for one flight index, as plain attributes."""
        attrs: dict = {}
        for name in FLIGHT_STRS:
            if name in self.group:
                value = str(self._column(name)[flight])
                if value:
                    attrs[name] = value
        for name in FLIGHT_NUMS:
            if name in self.group:
                value = float(self._column(name)[flight])
                if not np.isnan(value):
                    attrs[name] = value
        for name in FLIGHT_TIMES:
            if name in self.group:
                raw = int(self._column(name)[flight])
                if raw != _NO_TIME:
                    attrs[name] = str(np.datetime64(raw, "ns"))
        return attrs

    def flights(self) -> xr.Dataset:
        """Every flight's metadata as one Dataset — the thing to filter on."""
        data: dict = {}
        for name in FLIGHT_STRS:
            if name in self.group:
                data[name] = ("flight", self._column(name))
        for name in FLIGHT_NUMS:
            if name in self.group:
                data[name] = ("flight", self._column(name))
        for name in FLIGHT_TIMES:
            if name in self.group:
                raw = self._column(name).astype("i8")
                stamps = np.where(raw == _NO_TIME, np.datetime64("NaT", "ns"), raw.astype("M8[ns]"))
                data[name] = ("flight", stamps)
        for name in ("obs_start", "obs_count"):
            if name in self.group:
                data[name] = ("flight", self._column(name))
        return xr.Dataset(data)


class SondehubProvider(PointObservationProvider):
    """Sondehub radiosonde telemetry, in both the layouts the archive publishes.

    ``fetch``/``process`` build the day-partitioned cube from the subsampled ``date/``
    layout, one partition per UTC day — the ordinary provider contract.

    :meth:`write_flights` and :meth:`select_serial` build and read the per-flight ragged
    store from the full-rate ``serial/`` layout. The two stores are separate and answer
    different questions; see the module docstring.

    Args:
        config: Configuration override.
        bucket: Archive bucket. Public, and only a parameter so a mirror can be pointed at.
        workers: Concurrent object reads. A day is thousands of small objects, so this is
            latency-bound and threads pay off.
        max_sondes: Read at most this many sondes from the day. For trying the provider
            out against a real day without pulling all of it.
    """

    name = "sondehub"
    append_dim = "time"
    store_prefix = "bkr/observation/sondehub.icechunk"
    partition_freq = "1D"

    #: The per-flight ragged store, separate from the day-partitioned cube above.
    flight_store_prefix = "bkr/obs/sondehub_flights.icechunk"

    def __init__(
        self,
        config: Config | None = None,
        bucket: str = SONDEHUB_BUCKET,
        workers: int = 16,
        max_sondes: int | None = None,
        storage_options: dict | None = None,
    ):
        super().__init__(config=config)
        self.bucket = bucket
        self.workers = workers
        self.max_sondes = max_sondes
        self.storage_options = storage_options

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """List the day's per-sonde objects in the archive."""
        keys = list_day_keys(it, bucket=self.bucket, storage_options=self.storage_options)
        if self.max_sondes is not None:
            keys = keys[: self.max_sondes]
        return keys

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read every sonde in the day and flatten them into one table of frames."""

        def _one(key: str) -> pd.DataFrame:
            try:
                return frames_to_table(read_frames(key, self.storage_options))
            except Exception as exc:  # noqa: BLE001 - one unreadable sonde is not a failed day
                logger.warning(f"could not read {key}: {exc}")
                return pd.DataFrame()

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            tables = list(pool.map(_one, input_files))

        table = concat_tables(tables)
        if table.empty:
            table = pd.DataFrame({"time": []})
        dataset = table_to_dataset(
            table,
            SONDEHUB_SCHEMA,
            attrs={
                "source": f"s3://{self.bucket}/date",
                "partition": f"{pd.Timestamp(it):%Y-%m-%d}",
            },
        )
        return self.trim_to_window(dataset, it)

    # -- per-flight ragged store ---------------------------------------------------

    def flight_repo(self):
        """Open or create the per-flight store, resolved through the configuration."""
        return self.config.icechunk_repo(self.flight_store_prefix)

    @property
    def flight_store_path(self) -> str:
        """Fully resolved location of the per-flight store, for logging."""
        return self.config.store_path(self.flight_store_prefix)

    def flight_dataset(self, serial: str) -> xr.Dataset | None:
        """One full-rate flight from the archive, ready for the writer.

        Returns None when the serial has no usable frames, which happens for a sonde that
        was indexed under a day but never uploaded readable telemetry.
        """
        frames = download_serial(serial, self.storage_options)
        table = frames_to_table(frames)
        if table.empty:
            return None
        _, uploads = merge_frames(frames)
        summary = flight_summary(serial, frames)
        attrs = {
            "serial": serial,
            "n_frames": summary["frames"],
            "n_raw_uploads": int(sum(uploads.values())),
        }
        if summary["max_altitude"] is not None:
            attrs["max_altitude"] = summary["max_altitude"]
        if summary["start"] is not None and summary["end"] is not None:
            attrs["start_time"] = str(summary["start"])
            attrs["end_time"] = str(summary["end"])
            attrs["duration_seconds"] = float(
                (summary["end"] - summary["start"]).total_seconds()
            )
        for field in ("sonde_type", "launch_site"):
            if field in table.columns and table[field].notna().any():
                attrs[field] = str(table[field].dropna().iloc[0])
        return table_to_dataset(table, SONDEHUB_SCHEMA, attrs=attrs)

    def write_flights(
        self,
        day: pd.Timestamp | str,
        repo=None,
        target_chunk_points: int = TARGET_CHUNK_POINTS,
    ) -> int:
        """Append every flight indexed under ``day`` to the per-flight store.

        Incremental in two senses. Serials the store already holds are skipped, so this can
        be re-run over a day that was interrupted; and ``resize`` on a rectilinear array
        adds the growth as a single new chunk, so each batch becomes one chunk rather than
        rewriting what is there. The commit happens once, at the end of the day, so a day
        lands atomically.

        Returns the number of flights written.
        """
        serials = list_serials(day, bucket=self.bucket, storage_options=self.storage_options)
        if not serials:
            logger.info(f"{self.name}: no serials indexed for {pd.Timestamp(day):%Y-%m-%d}")
            return 0
        if self.max_sondes is not None:
            serials = serials[: self.max_sondes]

        writer = FlightWriter(repo or self.flight_repo(), target_chunk_points)
        pending = [s for s in serials if s not in writer]
        logger.info(
            f"{self.name}: {len(pending)} of {len(serials)} serial(s) for "
            f"{pd.Timestamp(day):%Y-%m-%d} are not yet stored"
        )

        def _one(serial: str) -> xr.Dataset | None:
            try:
                return self.flight_dataset(serial)
            except Exception as exc:  # noqa: BLE001 - one bad sonde is not a failed day
                logger.warning(f"{self.name}: could not read {serial}: {exc}")
                return None

        written = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for ds in pool.map(_one, pending):
                if ds is not None and writer.add(ds):
                    written += 1

        if written:
            writer.commit(f"{self.name}: {written} flight(s) for {pd.Timestamp(day):%Y-%m-%d}")
        return written

    def select_serial(self, serial: str, repo=None) -> xr.Dataset:
        """Read one flight back out of the per-flight store by serial.

        A binary search over the sorted index plus one chunk per variable, because no
        flight is ever split across a chunk boundary.
        """
        return SondeFlights(repo or self.flight_repo()).sel(serial)

    def flight_index(self, repo=None) -> xr.Dataset:
        """Every stored flight's metadata as one Dataset, for filtering."""
        return SondeFlights(repo or self.flight_repo()).flights()


def flight_dataset(serial: str, storage_options: dict | None = None) -> xr.Dataset:
    """Read one full-rate flight into a dataset, for looking at a single sonde."""
    frames = download_serial(serial, storage_options)
    table = frames_to_table(frames)
    if table.empty:
        raise ValueError(f"sonde {serial} has no usable frames")
    return table_to_dataset(table, SONDEHUB_SCHEMA, attrs={"serial": serial})


__all__ = [
    "FLIGHT_NUMS",
    "FLIGHT_STRS",
    "FLIGHT_TIMES",
    "OBS_VARS",
    "SONDEHUB_BUCKET",
    "SONDEHUB_SCHEMA",
    "TARGET_CHUNK_POINTS",
    "FlightWriter",
    "SondeFlights",
    "SondehubProvider",
    "download_serial",
    "flight_columns",
    "flight_dataset",
    "flight_summary",
    "frames_to_table",
    "list_day_keys",
    "list_serials",
    "merge_frames",
    "read_frames",
    "rebuild_serial_index",
    "write_flight_batch",
]
