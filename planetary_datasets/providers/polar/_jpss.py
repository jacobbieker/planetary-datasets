"""Shared plumbing for granule products in the NOAA JPSS open data buckets.

Every JPSS product on AWS lives in one bucket per spacecraft, ``noaa-nesdis-<sat>-pds``
(us-east-1, anonymous), under ``<PRODUCT>/<YYYY>/<MM>/<DD>/``, filed by the day the granule
*started*. Two filename conventions are in use:

* IDPS SDRs and geolocation (ATMS, VIIRS):
  ``SVM05_j01_d20261015_t1200015_e1201260_b46154_c20261015122500448000_oebc_ops.h5`` —
  spacecraft, day, start and end time of day in tenths of a second, orbit, creation stamp.
* NDE level 2 products (ATMS snowfall rate, MiRS):
  ``SFR_v2r0_n20_s202610151159550_e202610151200266_c202610151221350.nc`` — spacecraft,
  then full start and end stamps in tenths of a second.

**Retention.** Since 2026-09-14 most products are kept for a rolling 90 days and then
deleted; only ATMS-SDR/TDR/SDR-GEO, CrIS-FS-SDR/SDR-GEO, VIIRS-DNB-SDR/GEO,
VIIRS-I1..I5-SDR, VIIRS-IMG-GEO-TC and VIIRS_EFIRE_VIIRSI_EDR are kept indefinitely
(https://registry.opendata.aws/noaa-jpss/). Objects expire by creation date, so the oldest
sensing day still listed is about ``today - 90 days``. Anything archived from a rolling
product has to be copied out before then; a virtual reference to it would dangle.

The separate ``noaa-jpss`` bucket on the same registry entry is NOAA's development bucket
(blended products, ocean colour, a 2014 DNB reprocessing); it carries no ATMS and no
operational VIIRS SDR stream.
"""

from __future__ import annotations

import pathlib
import re
from typing import Dict, List, Tuple

import pandas as pd
from loguru import logger

from planetary_datasets.providers.polar._granule import GranuleProvider, mid_time

#: Public NOAA open-data bucket per spacecraft.
BUCKETS: Dict[str, str] = {
    "n21": "noaa-nesdis-n21-pds",
    "n20": "noaa-nesdis-n20-pds",
    "snpp": "noaa-nesdis-snpp-pds",
}

#: Spacecraft name as satpy and the other polar stores spell it.
PLATFORM_NAMES: Dict[str, str] = {
    "n21": "NOAA-21",
    "n20": "NOAA-20",
    "snpp": "Suomi-NPP",
}

#: Days a rolling product stays in the buckets.
ROLLING_RETENTION = pd.Timedelta(90, "D")

#: Encoding for the per-granule ``start_time``/``end_time``. Left to xarray, the units are
#: chosen from the first values written (``hours since <first granule>`` when that happens
#: to fall on the hour), and every later append is truncated to them.
GRANULE_TIME_ENCODING = {"units": "microseconds since 1970-01-01", "dtype": "int64"}


class IncompleteDownload(RuntimeError):
    """Some files of a partition failed to download; the partition must be retried.

    Writing what did arrive would mark the partition done, and for a rolling product the
    rest would be lost for good once the archive drops it.
    """


def oldest_rolling_partition(now: pd.Timestamp | None = None) -> pd.Timestamp:
    """First whole day of a rolling product that is still safely in the bucket.

    Objects are deleted ~90 days after they were *created*, at midnight UTC; the day 90
    days back is already partly gone, so the safe start is one day later.
    """
    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now.tzinfo is not None:
        now = now.tz_convert("UTC").tz_localize(None)
    return (now - ROLLING_RETENTION).normalize() + pd.Timedelta(1, "D")


# ------------------------------------------------------------------ IDPS filenames


def granule_key(filename: str) -> Tuple[str, ...]:
    """Identity of an IDPS granule, shared by all its band and geolocation files.

    ``SATMS_j02_d20250601_t0000244_e0000560_b13247_c2025…_oeac_ops.h5`` and the matching
    ``GATMO_…`` differ only in the product prefix and creation stamp, so the spacecraft,
    day, start, end and orbit fields identify the granule. The spacecraft is part of the
    key because several are ingested into one store and are paired up together.
    """
    parts = pathlib.Path(filename).name.split("_")
    if len(parts) < 6:
        raise ValueError(f"not an IDPS granule filename: {filename}")
    return tuple(parts[1:6])


def _time_of_day(day: str, field: str) -> pd.Timestamp:
    """Decode a ``HHMMSSd`` filename field — hours, minutes, seconds and tenths."""
    tod = field[1:]
    stamp = pd.Timestamp(f"{day}T{tod[0:2]}:{tod[2:4]}:{tod[4:6]}")
    return stamp + pd.Timedelta(int(tod[6]) * 100, "ms")


def granule_start(filename: str) -> pd.Timestamp:
    """Sensing start time encoded in an IDPS granule filename."""
    parts = pathlib.Path(filename).name.split("_")
    return _time_of_day(parts[2][1:], parts[3])


def granule_end(filename: str) -> pd.Timestamp:
    """Sensing end time encoded in an IDPS granule filename.

    The day is only in the filename once, so a granule running over midnight has an end
    field earlier than its start field; that rolls over to the following day.
    """
    parts = pathlib.Path(filename).name.split("_")
    day = parts[2][1:]
    start = _time_of_day(day, parts[3])
    end = _time_of_day(day, parts[4])
    return end + pd.Timedelta(1, "D") if end < start else end


def granule_overlaps(filename: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    """True when a granule's sensing interval intersects ``[start, end)``.

    Selection has to be by overlap, not by start time: the timestamp a granule is stored
    under is the middle of its sensing interval, so a granule starting just before the
    hour is owned by the *next* partition. Selecting on start alone would have neither
    partition keep it — this one discards it as out of window, and the next one never
    looks at it.
    """
    return granule_start(filename) < end and granule_end(filename) > start


# ------------------------------------------------------------------ NDE filenames

_NDE_STAMPS = re.compile(r"_(?P<sat>[a-z0-9]+)_s(?P<start>\d{15})_e(?P<end>\d{15})_c")


def _nde_match(filename: str) -> re.Match:
    match = _NDE_STAMPS.search(pathlib.Path(filename).name)
    if match is None:
        raise ValueError(f"not an NDE granule filename: {filename}")
    return match


def _nde_stamp(stamp: str) -> pd.Timestamp:
    """Decode ``YYYYMMDDHHMMSSd`` — a full timestamp in tenths of a second."""
    return pd.Timestamp(
        f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}T{stamp[8:10]}:{stamp[10:12]}:{stamp[12:14]}"
    ) + pd.Timedelta(int(stamp[14]) * 100, "ms")


def nde_granule_key(filename: str) -> Tuple[str, ...]:
    """Identity of an NDE granule: spacecraft, start and end stamps."""
    match = _nde_match(filename)
    return (match["sat"], match["start"], match["end"])


def nde_granule_start(filename: str) -> pd.Timestamp:
    """Sensing start time encoded in an NDE granule filename."""
    return _nde_stamp(_nde_match(filename)["start"])


def nde_granule_end(filename: str) -> pd.Timestamp:
    """Sensing end time encoded in an NDE granule filename."""
    return _nde_stamp(_nde_match(filename)["end"])


# ------------------------------------------------------------------ provider


class JpssGranuleProvider(GranuleProvider):
    """A granule stream read from the ``noaa-nesdis-<sat>-pds`` buckets.

    Lists the day prefixes a partition needs, parses granule identities and times out of
    the object keys, and builds anonymous HTTPS URLs for :func:`download_many`. The
    filename convention is chosen by overriding :meth:`key_of`, :meth:`start_of` and
    :meth:`end_of`; the defaults parse IDPS names.
    """

    @staticmethod
    def key_of(filename: str) -> Tuple[str, ...]:
        """Granule identity of an object key."""
        return granule_key(filename)

    @staticmethod
    def start_of(filename: str) -> pd.Timestamp:
        """Sensing start of an object key."""
        return granule_start(filename)

    @staticmethod
    def end_of(filename: str) -> pd.Timestamp:
        """Sensing end of an object key."""
        return granule_end(filename)

    def overlaps(self, filename: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
        """True when the granule's sensing interval intersects ``[start, end)``."""
        return self.start_of(filename) < end and self.end_of(filename) > start

    def owned_by(self, filename: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
        """True when the granule's stored timestamp falls in ``[start, end)``.

        The stored timestamp is :func:`mid_time` of the sensing interval, so this is the
        exact test :meth:`restrict_to_window` applies after processing. Filtering on it at
        listing time avoids downloading the granules the neighbouring partition owns.
        """
        stamp = mid_time(self.start_of(filename), self.end_of(filename))
        return start <= stamp < end

    @staticmethod
    def _https_url(bucket: str, key: str) -> str:
        return f"https://{bucket}.s3.amazonaws.com/{key}"

    @staticmethod
    def _s3():
        import s3fs

        return s3fs.S3FileSystem(anon=True)

    def _list_day(self, fs, bucket: str, prefix: str, day: pd.Timestamp) -> List[str]:
        path = f"{bucket}/{prefix}/{day.year:04d}/{day.month:02d}/{day.day:02d}/"
        try:
            # refresh: s3fs shares one instance per process and caches listings, so a lane
            # that listed today's prefix once would never see the granules landing after.
            return [str(key) for key in fs.ls(path, detail=False, refresh=True)]
        except FileNotFoundError:
            logger.debug(f"{self.name}: no listing for {path}")
            return []

    def _index_keys(
        self,
        keys: List[str],
        window: Tuple[pd.Timestamp, pd.Timestamp] | None = None,
        owned: bool = False,
    ) -> Dict[Tuple[str, ...], str]:
        """Index listed object keys by granule, skipping anything unparseable.

        A day prefix occasionally holds an object that is not a granule (an
        ``index.html``, a ``.sha384`` checksum); one of them must not take the partition
        down with it.

        Args:
            keys: Listed object keys.
            window: Keep only granules overlapping this ``[start, end)``.
            owned: With ``window``, keep only granules whose stored timestamp is inside
                it rather than every granule that overlaps it.
        """
        indexed: Dict[Tuple[str, ...], str] = {}
        for key in keys:
            if key.endswith((".sha384", ".html", ".md5")):
                continue
            try:
                if window is not None:
                    test = self.owned_by if owned else self.overlaps
                    if not test(key, *window):
                        continue
                indexed[self.key_of(key)] = key
            except (ValueError, IndexError) as exc:
                logger.warning(f"{self.name}: ignoring unparseable key {key} ({exc})")
        return indexed

    def _days_to_list(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
        """Day prefixes that can hold a granule overlapping ``[start, end)``.

        A granule is filed under the day it *started*, so a window beginning exactly at
        midnight also has to look at the day before. Any later window cannot, which keeps
        the usual case to a single listing.
        """
        first = start.normalize()
        if start == first:
            first -= pd.Timedelta(1, "D")
        return pd.date_range(first, end.normalize(), freq="1D")
