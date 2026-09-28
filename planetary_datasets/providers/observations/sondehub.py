"""Sondehub amateur radiosonde telemetry, read from the public S3 history archive.

Sondehub publishes every tracked radiosonde flight in ``s3://sondehub-history``, laid out
two ways: ``date/YYYY/MM/DD/<serial>.json`` holds a subsampled day of every flight that
reported that day, and ``serial/<serial>.json.gz`` holds one flight at full rate. The
day layout is what a time-partitioned store wants; the serial layout is what you want for
a single flight.

Consolidates ``sondehub_t.py``. That script listed serials, accumulated flight statistics
in a numpy pickle and plotted a flight path; the listing and frame parsing are kept, the
plotting is dropped, and the statistics become :func:`flight_summary` returning a record
rather than appending to a file on disk.

The archive is read with :mod:`fsspec` directly rather than through the ``sondehub``
package. The package's ``download`` helper starts fifty unbounded threads per call and
pulls a whole prefix into memory, which is what made the original script unusable on a
busy day; going through fsspec also drops a dependency.
"""

from __future__ import annotations

import gzip
import json
import pathlib
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Sequence

import fsspec
import pandas as pd
import xarray as xr
from loguru import logger

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


def list_serials(bucket: str = SONDEHUB_BUCKET, storage_options: dict | None = None) -> List[str]:
    """List every sonde serial in the archive's per-serial layout.

    This is a listing of the whole bucket prefix and takes a while; it is useful for
    surveying the archive, not for a partitioned ingest.
    """
    fs = _filesystem(storage_options)
    keys = fs.ls(f"{bucket}/serial/", detail=False)
    return sorted(
        key.split("/")[-1].removesuffix(".json.gz") for key in keys if key.endswith(".json.gz")
    )


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


def frames_to_table(frames: Iterable[dict]) -> pd.DataFrame:
    """Turn raw Sondehub frames into a table with one row per frame.

    Frames carry every value as a string and omit fields the receiver did not decode, so
    absent fields become missing rather than an error.
    """
    rows = []
    for frame in frames:
        if "datetime" not in frame:
            continue
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


class SondehubProvider(PointObservationProvider):
    """One UTC day of Sondehub radiosonde telemetry.

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


def flight_dataset(serial: str, storage_options: dict | None = None) -> xr.Dataset:
    """Read one full-rate flight into a dataset, for looking at a single sonde."""
    frames = download_serial(serial, storage_options)
    table = frames_to_table(frames)
    if table.empty:
        raise ValueError(f"sonde {serial} has no usable frames")
    return table_to_dataset(table, SONDEHUB_SCHEMA, attrs={"serial": serial})


__all__ = [
    "SONDEHUB_BUCKET",
    "SONDEHUB_SCHEMA",
    "SondehubProvider",
    "download_serial",
    "flight_dataset",
    "flight_summary",
    "frames_to_table",
    "list_day_keys",
    "list_serials",
    "read_frames",
]
