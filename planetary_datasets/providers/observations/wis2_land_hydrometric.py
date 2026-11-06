"""Real-time hydrometric data (river level and discharge) for the WIS2 land store.

Part of the land target (:mod:`~planetary_datasets.providers.observations.wis2_land`):
the objects under any centre's ``data/core/hydrology/`` topic are spooled to disk as the
window streams past and appended, after the tar is complete, to the ``hydrometric`` group
of ``bkr/obs/wis2_land.icechunk``, one row per station and time along ``observation``.

What is read
    Environment and Climate Change Canada's real-time hydrometric CSV (the Water Survey of
    Canada network, ~2,200 stations), published per station and province as "hourly"
    files (the last day or two) and "daily" files (the last 30 days), every value every
    5 minutes. Each file is republished whenever it changes, so a window holds the same
    values many times over (22,000 objects, 2.4 GB, for some 4 million distinct rows in
    the first window and a few hundred thousand new ones in each later one). Objects that
    are not this CSV are counted and skipped.

Values
    As the CSV gives them: ``water_level`` in m, ``discharge`` in m3 s-1, each with its
    ``*_grade``, ``*_symbol`` (text, usually empty) and ``*_qa_qc`` code as given (1, 2,
    5 and 500 occur; -1 when absent). Times are converted from the station's local
    standard time (the CSV gives the offset) to UTC; nothing else is changed. The CSV
    carries no positions: ``station_id`` is the Water Survey of Canada station number,
    which the station list at https://dd.weather.gc.ca/hydrometric/doc/ locates.

Duplicates
    A row's identity is its CSV line: station, time and every value and flag
    (``observation_identity``, a 64-bit BLAKE2b hash of the line). Copies of a line are
    stored once, with the arrival of the earliest copy; a line whose values were later
    revised (a discharge filled in, a QA/QC code raised) is stored again as it was
    revised, so readers wanting the latest values take the last ``arrival_time`` per
    ``station_id`` and ``time``. Identities in the group's last
    :data:`RECENT_IDENTITIES` rows (more than the 30 days the daily files repeat), and
    in older appends whose times overlap (an archived window backfilled long after later
    windows), are not written again.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import pathlib
import shutil
import tempfile
from collections import Counter
from typing import Any, Iterator

import numpy as np
import xarray as xr
from loguru import logger

GROUP = "hydrometric"
#: Rows at the end of the group whose identities a window is checked against: 30 days of
#: 5-minute values from ~2,200 stations is ~19 million.
RECENT_IDENTITIES = 32_000_000
#: Rows per append, bounding memory while writing.
BATCH_ROWS = 250_000
#: The header of the Water Survey of Canada real-time CSV (after a byte-order mark).
HEADER = (
    "ID,Date,Water Level / Niveau d'eau (m),Grade,Symbol / Symbole,QA/QC,"
    "Discharge / Débit (cms),Grade,Symbol / Symbole,QA/QC"
)

ARRIVAL_ATTRS = {
    "arrival_time": {
        "long_name": "time the earliest copy of this line arrived in the WIS2 Global Cache",
    },
}

VARIABLES: dict[str, tuple[str, dict]] = {
    "station_id": ("str", {"long_name": "Water Survey of Canada station number"}),
    "time": ("datetime64[s]", {"long_name": "observation time (UTC)"}),
    "water_level": ("float64", {"units": "m", "long_name": "water level (stage)"}),
    "water_level_grade": ("str", {"long_name": "grade of water_level"}),
    "water_level_symbol": ("str", {"long_name": "symbol of water_level (e.g. B ice)"}),
    "water_level_qa_qc": (
        "int16",
        {"long_name": "QA/QC code of water_level, as given", "_FillValue": -1},
    ),
    "discharge": ("float64", {"units": "m3 s-1", "long_name": "discharge"}),
    "discharge_grade": ("str", {"long_name": "grade of discharge"}),
    "discharge_symbol": ("str", {"long_name": "symbol of discharge"}),
    "discharge_qa_qc": (
        "int16",
        {"long_name": "QA/QC code of discharge, as given", "_FillValue": -1},
    ),
    "arrival_time": ("datetime64[s]", ARRIVAL_ATTRS["arrival_time"]),
    "observation_identity": (
        "int64",
        {"long_name": "64-bit BLAKE2b hash of the CSV line (station, time, values, flags)"},
    ),
}


def wants(key: str, size: int = 0) -> bool:
    """Whether an object is on a hydrology topic (cheap; by key alone)."""
    return "/data/core/hydrology/" in key


def identity(line: str) -> int:
    """A line's identity: its 64-bit BLAKE2b hash, as a signed integer."""
    digest = hashlib.blake2b(line.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "little", signed=True)


def utc(stamp: str) -> np.datetime64:
    """An ISO 8601 local time with offset (``2026-10-06T00:00:00-07:00``) in UTC.

    Raises:
        ValueError: The time cannot be read.
    """
    stamp = stamp.strip()
    if stamp.endswith("Z"):
        return np.datetime64(stamp[:-1], "s")
    if len(stamp) > 19 and stamp[-6] in "+-" and stamp[-3] == ":":
        sign = -1 if stamp[-6] == "-" else 1
        minutes = int(stamp[-5:-3]) * 60 + int(stamp[-2:])
        return np.datetime64(stamp[:-6], "s") - np.timedelta64(sign * minutes, "m")
    return np.datetime64(stamp, "s")


def _float(text: str) -> float:
    return float(text) if text.strip() else np.nan


def _code(text: str) -> int:
    return int(text) if text.strip() else -1


def parse_line(line: str) -> dict | None:
    """One CSV line as a row (without arrival or identity); None if it is not one."""
    fields = line.split(",")
    if len(fields) != 10:
        return None
    try:
        return {
            "station_id": fields[0].strip(),
            "time": utc(fields[1]),
            "water_level": _float(fields[2]),
            "water_level_grade": fields[3].strip(),
            "water_level_symbol": fields[4].strip(),
            "water_level_qa_qc": _code(fields[5]),
            "discharge": _float(fields[6]),
            "discharge_grade": fields[7].strip(),
            "discharge_symbol": fields[8].strip(),
            "discharge_qa_qc": _code(fields[9]),
        }
    except ValueError:
        return None


def lines(body: bytes) -> Iterator[str] | None:
    """The data lines of a hydrometric CSV, or None if ``body`` is not one."""
    text = body.decode("utf-8-sig", errors="replace")
    first, _, rest = text.partition("\n")
    if first.strip() != HEADER:
        return None
    return (line.rstrip("\r") for line in rest.split("\n") if line.strip())


def to_dataset(rows: list[dict]) -> xr.Dataset:
    """Rows as the group's variables along ``observation``."""
    data = {}
    for name, (dtype, attrs) in VARIABLES.items():
        values = [r[name] for r in rows]
        if dtype == "str":
            array = np.array(values, dtype=object)
        else:
            array = np.array(values, dtype=dtype)
        data[name] = xr.Variable(("observation",), array, dict(attrs))
    ds = xr.Dataset(data)
    ds.attrs = {
        "title": "WIS2 Global Cache real-time hydrometric data",
        "source": "WMO WIS2 Global Cache (s3://wis2globalcache), ca-eccc-msc hydrology topic",
        "deduplication": (
            "one row per distinct CSV line (station, time, values and flags); a revised "
            "line is a further row with a later arrival_time"
        ),
    }
    return ds


class Spool:
    """Objects written to a scratch directory as they stream past, kept in arrival order."""

    def __init__(self, root: pathlib.Path, name: str):
        self.root = pathlib.Path(root) / name
        self.directory: pathlib.Path | None = None
        self.items: list[tuple[str, str, dt.datetime | None]] = []  # (path, key, arrived)
        self.failed = 0

    def add(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Write one object to disk; never raises."""
        try:
            if self.directory is None:
                self.root.mkdir(parents=True, exist_ok=True)
                self._remove_orphans()
                self.directory = pathlib.Path(
                    tempfile.mkdtemp(prefix=f"{os.getpid()}-", dir=self.root)
                )
            path = self.directory / f"{len(self.items) + 1:08d}"
            path.write_bytes(body)
            self.items.append((str(path), key, arrived))
        except Exception as exc:  # noqa: BLE001 - a target must never break the archive
            self.failed += 1
            logger.warning(f"wis2 land: could not spool {key}: {exc}")

    def _remove_orphans(self) -> None:
        """Remove spools left by processes that died before finishing their window."""
        for directory in self.root.iterdir():
            pid = directory.name.split("-", 1)[0]
            if not pid.isdigit() or int(pid) == os.getpid():
                continue
            try:
                os.kill(int(pid), 0)
            except ProcessLookupError:
                shutil.rmtree(directory, ignore_errors=True)
            except PermissionError:
                continue

    def in_arrival_order(self) -> list[tuple[str, str, dt.datetime | None]]:
        """The spooled objects, earliest arrival first (unknown arrivals last)."""
        far = dt.datetime.max.replace(tzinfo=dt.timezone.utc)
        return sorted(self.items, key=lambda item: item[2] or far)

    def clear(self) -> None:
        """Remove the spooled files."""
        if self.directory is not None:
            shutil.rmtree(self.directory, ignore_errors=True)
        self.directory, self.items = None, []


#: Group attribute listing each append's rows and the span of times they hold:
#: ``[[start_row, stop_row, earliest, latest], ...]`` (times in seconds since 1970).
RANGES_ATTR = "row_time_ranges"


def _identities(repo):
    """The group's ``observation_identity`` array and attributes (None before it exists)."""
    import zarr

    try:
        node = zarr.open_group(repo.readonly_session("main").store, path=GROUP, mode="r")
        return node["observation_identity"], dict(node.attrs)
    except (KeyError, zarr.errors.GroupNotFoundError):  # not written yet; others propagate
        return None, {}


def stored_identities(repo) -> tuple[np.ndarray, int]:
    """Sorted identities of the group's last :data:`RECENT_IDENTITIES` rows.

    Returns:
        The identities and the first row they cover.
    """
    array, _ = _identities(repo)
    if array is None:
        return np.empty(0, dtype="int64"), 0
    start = max(0, array.shape[0] - RECENT_IDENTITIES)
    return np.unique(array[start:]), start


def older_identities(repo, earliest: int, latest: int, before: int, loaded: set) -> np.ndarray:
    """Identities of rows before row ``before`` whose append spans ``earliest..latest``.

    An archived window backfilled long after later ones repeats lines that have left the
    recent rows; the appends' time spans (:data:`RANGES_ATTR`) find where they are. Ranges
    in ``loaded`` are skipped and the ones read are added to it.
    """
    array, attrs = _identities(repo)
    if array is None:
        return np.empty(0, dtype="int64")
    parts = []
    for start, stop, first, last in attrs.get(RANGES_ATTR, []):
        if start >= before or last < earliest or first > latest or start in loaded:
            continue
        loaded.add(start)
        parts.append(array[start : min(stop, before)])
    return np.unique(np.concatenate(parts)) if parts else np.empty(0, dtype="int64")


def _record_range(repo, start: int, stop: int, earliest: int, latest: int) -> None:
    """Add one append's rows and time span to :data:`RANGES_ATTR`."""
    import icechunk
    import zarr

    from planetary_datasets.common.multidim import COMMIT_ATTEMPTS

    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        session = repo.writable_session("main")
        node = zarr.open_group(session.store, path=GROUP, mode="a")
        ranges = list(node.attrs.get(RANGES_ATTR, []))
        ranges.append([int(start), int(stop), int(earliest), int(latest)])
        node.attrs[RANGES_ATTR] = ranges
        try:
            session.commit(
                f"wis2_land {GROUP}: rows {start}..{stop}", rebase_with=icechunk.ConflictDetector()
            )
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == COMMIT_ATTEMPTS:
                raise


def contains(sorted_values: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Which of ``values`` are in ``sorted_values`` (ascending), by binary search."""
    if not sorted_values.size:
        return np.zeros(values.shape, dtype=bool)
    at = np.minimum(np.searchsorted(sorted_values, values), sorted_values.size - 1)
    return sorted_values[at] == values


def write(repo, spool: Spool, partition: str | None) -> dict[str, Any]:
    """Deduplicate the spooled objects and append their new rows to the group.

    Rows go in batches of :data:`BATCH_ROWS`, each committed without the ledger; the
    window is added to the group's ledger once all are written, so a run cut short is
    finished by the next one without writing a row twice (the identities stop it).

    Returns:
        Counts: objects, lines read, rows written, and objects or lines skipped.
    """
    from planetary_datasets.common.multidim import (
        append_partition,
        describe_variables,
        ledger,
        mark_written,
    )

    counts: Counter = Counter(objects=len(spool.items))
    if partition is not None and partition in ledger(repo, group=GROUP):
        counts["already_written"] = 1
        return dict(counts)
    seen, recent_from = stored_identities(repo)
    loaded: set[int] = set()  # older appends whose identities are in ``seen``
    # identity -> (line, arrival): lines are parsed only when their batch is written.
    batch: dict[int, tuple[str, np.datetime64]] = {}
    written = 0

    def flush() -> None:
        nonlocal seen, batch, written
        if not batch:
            return
        rows = []
        for ident, (line, stamp) in batch.items():
            row = parse_line(line)
            if row is None:
                counts["unreadable_lines"] += 1
                continue
            row["arrival_time"] = stamp
            row["observation_identity"] = ident
            rows.append(row)
        if rows:
            times = np.array([r["time"] for r in rows], dtype="datetime64[s]").astype("int64")
            older = older_identities(repo, int(times.min()), int(times.max()), recent_from, loaded)
            if older.size:
                seen = np.union1d(seen, older)
                held = contains(seen, np.array([r["observation_identity"] for r in rows]))
                counts["lines_in_older_rows"] += int(held.sum())
                rows = [r for r, h in zip(rows, held, strict=True) if not h]
        rows.sort(key=lambda r: (r["time"], r["station_id"]))
        if rows:
            array, _ = _identities(repo)
            start = 0 if array is None else array.shape[0]
            append_partition(repo, to_dataset(rows), None, label=f"wis2_land {GROUP}", group=GROUP)
            times = np.array([r["time"] for r in rows], dtype="datetime64[s]").astype("int64")
            _record_range(repo, start, start + len(rows), int(times.min()), int(times.max()))
        written += len(rows)
        seen = np.union1d(seen, np.fromiter(batch, dtype="int64", count=len(batch)))
        batch = {}

    for path, key, arrived in spool.in_arrival_order():
        body = pathlib.Path(path).read_bytes()
        found = lines(body)
        if found is None:
            counts["unrecognised_objects"] += 1
            continue
        found = list(found)
        ids = np.fromiter((identity(line) for line in found), dtype="int64", count=len(found))
        counts["lines"] += len(found)
        new = np.flatnonzero(~contains(seen, ids))
        stamp = (
            np.datetime64(arrived.astimezone(dt.timezone.utc).replace(tzinfo=None), "s")
            if arrived
            else np.datetime64("NaT", "s")
        )
        for index, ident in zip(new.tolist(), ids[new].tolist(), strict=True):
            # The earliest copy is kept: objects come in arrival order.
            batch.setdefault(ident, (found[index], stamp))
        if len(batch) >= BATCH_ROWS:
            flush()
    flush()
    counts["rows_written"] = written
    if partition is not None and (written or GROUP in _groups(repo)):
        mark_written(repo, partition, label=f"wis2_land {GROUP}", group=GROUP)
    if written:
        describe_variables(repo, ARRIVAL_ATTRS, label=f"wis2_land {GROUP}", group=GROUP)
    return dict(counts)


def _groups(repo) -> set[str]:
    """Names of the store's top-level groups."""
    import zarr

    try:
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except Exception:  # noqa: BLE001 - no store yet
        return set()
    return set(root.group_keys())
