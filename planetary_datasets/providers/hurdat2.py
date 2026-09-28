"""NOAA HURDAT2 best-track hurricane archive.

HURDAT2 is the National Hurricane Center's re-analysed best track database: for every
tropical or subtropical cyclone it gives the position, intensity and wind radii at
(mostly) six-hourly synoptic times. It is published as two plain fixed-field text files,
one for the North Atlantic and one for the Northeast/Central Pacific, each a few megabytes
covering the whole archive.

https://www.nhc.noaa.gov/data/#hurdat

Layout of the store
-------------------
One partition is one hurricane *season*, so the append dimension ``time`` is midnight on
1 January of that year. Within a season the tracks are ragged - a season has between zero
and thirty-one storms and a storm has between one and about 130 six-hourly records - so
they are padded onto fixed ``storm`` and ``record`` dimensions
(:data:`MAX_STORMS` x :data:`MAX_RECORDS`). Padding is NaN for the numeric fields, ``NaT``
for ``record_time`` and an empty string for the text fields; ``record_count`` gives the
number of real records in each slot. The padding compresses away to almost nothing.

Times of the individual track points are stored in ``record_time`` as float64 seconds
since the epoch with CF ``units``/``calendar`` attributes, rather than as ``datetime64``.
A second datetime variable would be given its own reference-date encoding by xarray on the
first write, which then disagrees with the next season's and breaks the append.

Caveats
-------
A season is only final after the NHC's post-season reanalysis, which lands in the spring
following the season and can add, remove or rename storms. A season that is already in the
store is not rewritten, so recent seasons should be materialised after the reanalysis. The
NHC also republishes the whole archive under a new filename on every release;
:func:`latest_release_url` resolves the newest one from the directory listing rather than
pinning a filename that goes stale.

No credentials are needed - the files are public and unauthenticated.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_one
from planetary_datasets.providers._timestamps import NaiveUTCPartitions, to_naive_utc

#: Directory listing the NHC publishes every HURDAT2 release into.
INDEX_URL = "https://www.nhc.noaa.gov/data/hurdat/"

#: Fixed size of the padded ``storm`` dimension. The busiest season on record holds 31
#: storms (Atlantic 2005, Northeast Pacific 2015), so this leaves a healthy margin. It
#: cannot be changed without rewriting an existing store, which is why it is generous.
MAX_STORMS = 48

#: Fixed size of the padded ``record`` dimension. The longest track in the archive has 133
#: six-hourly entries.
MAX_RECORDS = 256

#: Value HURDAT2 uses for "not reported". A few 1970s records use -99 instead, which is
#: why :func:`_parse_number` treats any negative value as missing rather than matching
#: this exact number.
MISSING = -999

BASINS: dict[str, dict] = {
    "atlantic": {
        # hurdat2-1851-2025-091226.txt
        "pattern": re.compile(r"^hurdat2-(\d{4})-(\d{4})-(\d{6,8})([a-z]?)\.txt$"),
        "first_year": 1851,
        "basin_codes": ("AL",),
    },
    "pacific": {
        # hurdat2-nepac-1949-2025-091426.txt
        "pattern": re.compile(r"^hurdat2-nepac-(\d{4})-(\d{4})-(\d{6,8})([a-z]?)\.txt$"),
        "first_year": 1949,
        "basin_codes": ("EP", "CP"),
    },
}

# Field order of a HURDAT2 track line, after the date and time.
_RADII_NAMES = tuple(
    f"wind_radii_{kt}kt_{quadrant}"
    for kt in (34, 50, 64)
    for quadrant in ("ne", "se", "sw", "nw")
)

#: Numeric per-record variables, in the order they appear on a track line.
RECORD_FLOAT_VARS: tuple[str, ...] = (
    "latitude",
    "longitude",
    "max_sustained_wind_knots",
    "min_pressure_mb",
    *_RADII_NAMES,
    "radius_max_wind_nmi",
)

VAR_ATTRS: dict[str, dict] = {
    "latitude": {"units": "degrees_north", "long_name": "storm centre latitude"},
    "longitude": {"units": "degrees_east", "long_name": "storm centre longitude"},
    "max_sustained_wind_knots": {
        "units": "knots",
        "long_name": "maximum sustained surface wind, 1-minute average",
    },
    "min_pressure_mb": {"units": "hPa", "long_name": "minimum central pressure"},
    "radius_max_wind_nmi": {"units": "nautical_mile", "long_name": "radius of maximum wind"},
    **{
        name: {
            "units": "nautical_mile",
            "long_name": (
                f"maximum extent of {name.split('_')[2]} wind in the "
                f"{name.rsplit('_', 1)[1].upper()} quadrant"
            ),
        }
        for name in _RADII_NAMES
    },
}

#: Meaning of the two-letter ``status`` codes, attached to the store as an attribute.
STATUS_CODES = {
    "TD": "tropical cyclone of tropical depression intensity (< 34 kt)",
    "TS": "tropical cyclone of tropical storm intensity (34-63 kt)",
    "HU": "tropical cyclone of hurricane intensity (>= 64 kt)",
    "EX": "extratropical cyclone",
    "SD": "subtropical cyclone of subtropical depression intensity (< 34 kt)",
    "SS": "subtropical cyclone of subtropical storm intensity (>= 34 kt)",
    "LO": "low, not a tropical, subtropical or extratropical cyclone",
    "WV": "tropical wave",
    "DB": "disturbance",
    "ET": "extratropical (legacy code)",
    "ST": "subtropical (legacy code)",
    "TY": "typhoon (legacy code)",
    "PT": "post-tropical (legacy code)",
}

#: Meaning of the optional record identifier column.
RECORD_IDENTIFIERS = {
    "C": "closest approach to a coast without a landfall",
    "G": "genesis",
    "I": "intensity peak in both pressure and wind",
    "L": "landfall (centre of system crossing a coastline)",
    "P": "minimum in central pressure",
    "R": "provides additional detail on the intensity when rapidly changing",
    "S": "change of status of the system",
    "T": "provides additional detail on the track (position)",
    "W": "maximum sustained wind speed",
}

_HEADER_ID = re.compile(r"^([A-Z]{2})(\d{2})(\d{4})$")


class HURDAT2ParseError(ValueError):
    """The HURDAT2 file did not have the structure this parser expects."""


@dataclass
class Storm:
    """One cyclone: the header line plus its track."""

    storm_id: str
    name: str
    basin_code: str
    number: int
    year: int
    records: List[dict] = field(default_factory=list)

    @property
    def genesis_time(self) -> pd.Timestamp | None:
        return self.records[0]["time"] if self.records else None


def latest_release_url(basin: str, index_url: str = INDEX_URL, timeout: float = 60.0) -> str:
    """Return the URL of the newest HURDAT2 release for ``basin``.

    The NHC keeps every past release in the same directory and only the filename says
    which is current, so the listing is parsed and the file with the latest
    (last season, release date, revision letter) wins. Pinning a filename instead is how
    the previous version of this module ended up three years out of date.

    The revision letter matters: a same-day re-release is published as ``...043021a.txt``
    alongside ``...043021.txt``, and ranking on the date alone would keep the superseded
    one.
    """
    import fsspec

    spec = _basin_spec(basin)
    if not index_url.endswith("/"):
        index_url += "/"

    with fsspec.open(index_url, "rt", timeout=timeout, encoding="utf-8", errors="replace") as fh:
        listing = fh.read()

    best: tuple[tuple[int, pd.Timestamp, str], str] | None = None
    for name in sorted(set(re.findall(r"hurdat2[A-Za-z0-9\-]*\.txt", listing))):
        match = spec["pattern"].match(name)
        if match is None:
            continue
        released = _parse_release_date(match.group(3))
        if released is None:
            continue
        key = (int(match.group(2)), released, match.group(4))
        if best is None or key > best[0]:
            best = (key, name)

    if best is None:
        raise HURDAT2ParseError(
            f"no HURDAT2 {basin} release found in {index_url}; the naming scheme may have changed"
        )
    logger.debug(f"resolved latest HURDAT2 {basin} release to {best[1]}")
    return index_url + best[1]


def _parse_release_date(token: str) -> pd.Timestamp | None:
    """Parse the ``MMDDYY`` or ``MMDDYYYY`` release stamp in a HURDAT2 filename."""
    fmt = "%m%d%y" if len(token) == 6 else "%m%d%Y"
    try:
        return pd.Timestamp(pd.to_datetime(token, format=fmt))
    except ValueError:
        return None


def _basin_spec(basin: str) -> dict:
    try:
        return BASINS[basin]
    except KeyError:
        raise ValueError(f"unknown basin {basin!r}, expected one of {sorted(BASINS)}") from None


def _parse_coordinate(token: str) -> float:
    """Turn ``28.0N`` / ``94.8W`` into a signed degree value."""
    token = token.strip()
    if not token:
        return float("nan")
    hemisphere = token[-1].upper()
    if hemisphere in ("N", "S", "E", "W"):
        body = token[:-1]
        sign = -1.0 if hemisphere in ("S", "W") else 1.0
    else:
        # A handful of 1970s Atlantic records omit the hemisphere letter entirely
        # ("38.83"). Every position in the archive is in the northern and western
        # hemispheres apart from the odd extratropical remnant, so take the sign as given.
        body, sign = token, 1.0
    try:
        return sign * float(body)
    except ValueError:
        return float("nan")


def _parse_number(token: str) -> float:
    """Parse one of the non-negative numeric fields, mapping the sentinels to NaN.

    HURDAT2 uses -999 for "not reported" and, in a few dozen 1970s records, -99. Every
    field parsed here - wind speed, pressure, wind radii, radius of maximum wind - is
    non-negative by definition, so any negative value means missing.
    """
    token = token.strip()
    if not token:
        return float("nan")
    try:
        value = float(token)
    except ValueError:
        return float("nan")
    return float("nan") if value < 0 else value


def _track_fields(line: str) -> List[str] | None:
    """Split a track line into its 21 fields, repairing the known defects.

    Returns None when the line cannot be made sense of.
    """
    fields = [f.strip() for f in line.split(",")]
    while fields and fields[-1] == "":
        fields.pop()

    if len(fields) == 20:
        # At least one line in the Atlantic archive (1969-09-29 06Z) is missing the comma
        # between latitude and longitude: "63.3N    7.5E". Split it back apart rather than
        # dropping a real observation.
        for i, value in enumerate(fields):
            parts = value.split()
            if len(parts) == 2 and parts[0][-1:].upper() in "NS" and parts[1][-1:].upper() in "EW":
                fields = [*fields[:i], *parts, *fields[i + 1 :]]
                break

    if len(fields) == 20:
        # Releases before the 2021 format have no radius-of-maximum-wind column.
        fields.append(str(MISSING))
    if len(fields) < 21:
        return None
    return fields[:21]


def parse_hurdat2(lines: Iterable[str]) -> List[Storm]:
    """Parse HURDAT2 text into a list of :class:`Storm`.

    Args:
        lines: The file's lines, in order.

    Raises:
        HURDAT2ParseError: If a track line appears before any header line, or a header
            declares a record count that does not match the track lines that follow it.
    """
    storms: List[Storm] = []
    current: Storm | None = None
    declared: int = 0

    for lineno, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue

        head = [f.strip() for f in line.split(",")]
        header_match = _HEADER_ID.match(head[0]) if head else None
        if header_match is not None and len(head) <= 4:
            if current is not None:
                _check_count(current, declared)
                storms.append(current)
            try:
                declared = int(head[2])
            except (IndexError, ValueError) as exc:
                raise HURDAT2ParseError(f"line {lineno}: bad header {line!r}") from exc
            current = Storm(
                storm_id=head[0],
                name=head[1],
                basin_code=header_match.group(1),
                number=int(header_match.group(2)),
                year=int(header_match.group(3)),
            )
            continue

        if current is None:
            raise HURDAT2ParseError(f"line {lineno}: track line before any storm header")

        fields = _track_fields(line)
        if fields is None:
            logger.warning(f"line {lineno}: skipping unparseable HURDAT2 track line {line!r}")
            continue

        try:
            timestamp = pd.Timestamp(f"{fields[0]}T{fields[1][:2]}:{fields[1][2:4]}")
        except ValueError:
            logger.warning(f"line {lineno}: skipping track line with bad time {line!r}")
            continue

        record = {
            "time": timestamp,
            "record_identifier": fields[2],
            "status": fields[3],
            "latitude": _parse_coordinate(fields[4]),
            "longitude": _parse_coordinate(fields[5]),
        }
        for name, token in zip(RECORD_FLOAT_VARS[2:], fields[6:21]):
            record[name] = _parse_number(token)
        current.records.append(record)

    if current is not None:
        _check_count(current, declared)
        storms.append(current)
    return storms


def _check_count(storm: Storm, declared: int) -> None:
    if declared != len(storm.records):
        raise HURDAT2ParseError(
            f"{storm.storm_id} declares {declared} track entries but {len(storm.records)} "
            "were parsed"
        )


def storms_to_dataset(
    storms: Sequence[Storm],
    season: pd.Timestamp,
    basin: str,
    source: str | None = None,
    max_storms: int = MAX_STORMS,
    max_records: int = MAX_RECORDS,
) -> xr.Dataset:
    """Pad one season's storms onto fixed dimensions and return it as a dataset.

    Args:
        storms: The storms of a single season, in archive order.
        season: Midnight on 1 January of the season, used as the ``time`` coordinate.
        basin: ``atlantic`` or ``pacific``, recorded in the attributes.
        source: URL the data came from, recorded in the attributes.
        max_storms: Size of the padded ``storm`` dimension.
        max_records: Size of the padded ``record`` dimension.

    Raises:
        ValueError: If the season holds more storms, or a storm more records, than the
            fixed dimensions allow. Growing them would make the new data inconsistent with
            everything already in the store, so this fails loudly instead.
    """
    if len(storms) > max_storms:
        raise ValueError(
            f"season {season:%Y} has {len(storms)} storms but the store is laid out for "
            f"{max_storms}; MAX_STORMS must be raised and the store rebuilt"
        )
    longest = max((len(s.records) for s in storms), default=0)
    if longest > max_records:
        raise ValueError(
            f"season {season:%Y} has a storm with {longest} track entries but the store is "
            f"laid out for {max_records}; MAX_RECORDS must be raised and the store rebuilt"
        )

    shape = (max_storms, max_records)
    floats = {name: np.full(shape, np.nan, dtype="float32") for name in RECORD_FLOAT_VARS}
    record_seconds = np.full(shape, np.nan, dtype="float64")
    status = np.full(shape, "", dtype=object)
    identifier = np.full(shape, "", dtype=object)

    storm_id = np.full(max_storms, "", dtype=object)
    storm_name = np.full(max_storms, "", dtype=object)
    basin_code = np.full(max_storms, "", dtype=object)
    storm_number = np.zeros(max_storms, dtype="int16")
    record_count = np.zeros(max_storms, dtype="int16")
    genesis = np.full(max_storms, np.nan, dtype="float64")

    for s_idx, storm in enumerate(storms):
        storm_id[s_idx] = storm.storm_id
        storm_name[s_idx] = storm.name
        basin_code[s_idx] = storm.basin_code
        storm_number[s_idx] = storm.number
        record_count[s_idx] = len(storm.records)
        if storm.records:
            genesis[s_idx] = _epoch_seconds(storm.genesis_time)
        for r_idx, record in enumerate(storm.records):
            record_seconds[s_idx, r_idx] = _epoch_seconds(record["time"])
            status[s_idx, r_idx] = record["status"]
            identifier[s_idx, r_idx] = record["record_identifier"]
            for name in RECORD_FLOAT_VARS:
                floats[name][s_idx, r_idx] = record[name]

    time_attrs = {"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"}
    data_vars: dict = {
        "storm_id": (("time", "storm"), storm_id[None, :], {"long_name": "ATCF cyclone identifier"}),
        "storm_name": (("time", "storm"), storm_name[None, :], {"long_name": "storm name"}),
        "basin_code": (("time", "storm"), basin_code[None, :], {"long_name": "ATCF basin code"}),
        "storm_number": (
            ("time", "storm"),
            storm_number[None, :],
            {"long_name": "cyclone number within the season"},
        ),
        "record_count": (
            ("time", "storm"),
            record_count[None, :],
            {"long_name": "number of real track entries in this storm slot"},
        ),
        "genesis_time": (
            ("time", "storm"),
            genesis[None, :],
            {**time_attrs, "long_name": "time of the first track entry"},
        ),
        "record_time": (
            ("time", "storm", "record"),
            record_seconds[None, :, :],
            {**time_attrs, "long_name": "time of the track entry"},
        ),
        "status": (
            ("time", "storm", "record"),
            status[None, :, :],
            {"long_name": "system status", "codes": str(STATUS_CODES)},
        ),
        "record_identifier": (
            ("time", "storm", "record"),
            identifier[None, :, :],
            {"long_name": "record identifier", "codes": str(RECORD_IDENTIFIERS)},
        ),
    }
    for name, values in floats.items():
        data_vars[name] = (("time", "storm", "record"), values[None, :, :], VAR_ATTRS.get(name, {}))

    ds = xr.Dataset(
        data_vars,
        coords={
            "time": pd.DatetimeIndex([season]),
            "storm": np.arange(max_storms, dtype="int16"),
            "record": np.arange(max_records, dtype="int16"),
        },
        attrs={
            "title": "NOAA HURDAT2 best track archive",
            "basin": basin,
            "source": source or INDEX_URL,
            "institution": "NOAA National Hurricane Center",
            "references": "https://www.nhc.noaa.gov/data/#hurdat",
            "comment": (
                "Seasons are padded onto fixed storm and record dimensions; record_count "
                "gives the number of real entries per storm."
            ),
        },
    )
    ds["time"].attrs["long_name"] = "start of the hurricane season"
    ds["storm"].attrs["long_name"] = "storm slot within the season"
    ds["record"].attrs["long_name"] = "track entry index within the storm"
    return ds


def _epoch_seconds(timestamp: pd.Timestamp) -> float:
    """Seconds since 1970-01-01 for a timestamp, as a float so NaN can mean missing."""
    return float(pd.Timestamp(timestamp).value) / 1e9


class HURDAT2Provider(NaiveUTCPartitions, BaseProvider):
    """One hurricane season of HURDAT2 best tracks per partition.

    Args:
        basin: ``atlantic`` (North Atlantic, from 1851) or ``pacific``
            (Northeast and Central Pacific, from 1949).
        url: Pin a specific release instead of resolving the newest one. Useful for
            reproducing an earlier build and for tests.
        cache_dir: Where the downloaded text file is kept. Defaults to
            ``<data_dir>/hurdat2``; the file is shared by every season partition.
        max_storms, max_records: Fixed sizes of the padded dimensions. Changing these is
            incompatible with an existing store.
    """

    append_dim = "time"
    # A few megabytes of text: the memory guard has nothing useful to do here.
    guard_memory = False

    def __init__(
        self,
        basin: str = "atlantic",
        url: str | None = None,
        cache_dir: str | pathlib.Path | None = None,
        max_storms: int = MAX_STORMS,
        max_records: int = MAX_RECORDS,
        config=None,
    ):
        super().__init__(config=config)
        self.spec = _basin_spec(basin)
        self.basin = basin
        self.name = f"hurdat2_{basin}"
        self.store_prefix = f"bkr/noaa/hurdat2_{basin}.icechunk"
        self.max_storms = max_storms
        self.max_records = max_records
        self._url = url
        self._cache_dir = pathlib.Path(cache_dir) if cache_dir is not None else None
        self._parsed: dict[str, List[Storm]] = {}

    @property
    def first_year(self) -> int:
        """First season present in this basin's archive."""
        return int(self.spec["first_year"])

    @staticmethod
    def season_start(it) -> pd.Timestamp:
        """Midnight on 1 January of the season a timestamp falls in.

        The store is indexed by season, so the partition timestamp is snapped to the
        season before the base class compares it with what is already there.
        """
        return pd.Timestamp(year=to_naive_utc(it).year, month=1, day=1)

    def run_partition(self, it, check_present: bool = True) -> bool:
        return super().run_partition(self.season_start(it), check_present=check_present)

    def run_range(self, timestamps) -> int:
        """Run every season covered by ``timestamps``, each at most once.

        Snapping here as well as in :meth:`run_partition` matters: the base class filters
        against the store using the timestamps it is given, so an unsnapped mid-year date
        would never match the stored 1 January and the season would be re-downloaded,
        re-parsed and rebuilt on every run only to be skipped at the write.
        """
        seasons = sorted({self.season_start(t) for t in timestamps})
        return super().run_range(pd.DatetimeIndex(seasons))

    @property
    def cache_dir(self) -> pathlib.Path:
        return self._cache_dir or (self.config.data_dir / "hurdat2")

    def release_url(self) -> str:
        """URL of the release this provider reads, resolved once per instance."""
        if self._url is None:
            self._url = latest_release_url(self.basin)
        return self._url

    def storms(self, path: str | pathlib.Path) -> List[Storm]:
        """Parse a HURDAT2 file, caching the result for the life of this provider.

        Every season partition reads the same file, so parsing it once matters for a
        backfill of 175 partitions.
        """
        key = str(path)
        if key not in self._parsed:
            text = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
            self._parsed[key] = parse_hurdat2(text.splitlines())
            logger.debug(f"parsed {len(self._parsed[key])} storms from {key}")
        return self._parsed[key]

    def season_storms(self, path: str | pathlib.Path, year: int) -> List[Storm]:
        """The storms of one season, in archive order."""
        return [s for s in self.storms(path) if s.year == year]

    def fetch(self, it, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download the current release (once) and check the season has any storms."""
        it = to_naive_utc(it)
        url = self.release_url()
        destination = self.cache_dir / url.rsplit("/", 1)[-1]
        path = download_one(url, destination)
        if path is None:
            # Transient: returning [] here would mark the partition permanently done.
            raise RuntimeError(f"could not download HURDAT2 release {url}")

        if not self.season_storms(path, it.year):
            logger.info(f"{self.name}: no storms recorded for {it.year}")
            return []
        return [str(path)]

    def process(
        self,
        input_files: List[str],
        it,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        season = self.season_start(it)
        storms = self.season_storms(input_files[0], season.year)
        logger.info(f"{self.name}: {len(storms)} storms in {season.year}")
        return storms_to_dataset(
            storms,
            season=season,
            basin=self.basin,
            source=self.release_url(),
            max_storms=self.max_storms,
            max_records=self.max_records,
        )
