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

**One store.** Every product is written to ``bkr/polar/jpss.icechunk``, one zarr group per
product (``atms``, ``atms_sfr``, ``mirs_img``, ``mirs_snd``, ``viirs_mband``), and every
group holds all three spacecraft. See :class:`JpssProductProvider` for the layout.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.common import naming
from planetary_datasets.common.store import STORE_READ_ERRORS, StoreReadError, holds_data
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

#: Every spacecraft, newest first.
SATELLITES: Tuple[str, ...] = ("n21", "n20", "snpp")

#: Spacecraft code in IDPS filenames (``SATMS_j01_…``).
IDPS_SPACECRAFT: Dict[str, str] = {"n20": "j01", "n21": "j02", "snpp": "npp"}

#: Spacecraft code in NDE filenames (``SFR_v2r0_n20_…``).
NDE_SPACECRAFT: Dict[str, str] = {"n20": "n20", "n21": "n21", "snpp": "npp"}

#: The one store every JPSS product is written to, a zarr group per product.
STORE_PREFIX = "bkr/polar/jpss.icechunk"

#: Days a rolling product stays in the buckets.
ROLLING_RETENTION = pd.Timedelta(90, "D")

#: Encoding for the per-granule ``start_time``/``end_time``. Left to xarray, the units are
#: chosen from the first values written (``hours since <first granule>`` when that happens
#: to fall on the hour), and every later append is truncated to them.
#: Dtype every granule time is built in before encoding.
NS_DATETIME = "datetime64[ns]"

GRANULE_TIME_ENCODING = {"units": "microseconds since 1970-01-01", "dtype": "int64"}

#: Chunk reads in flight while reading a group's granule axis: it is stored in chunks of
#: tens of granules, so the ATMS group's 7.6M are ~250k chunks, read ~4x faster at 256
#: than at zarr's default.
GRANULE_AXIS_CONCURRENCY = 256


class PackingChanged(RuntimeError):
    """A granule is packed differently from the store's fixed packing."""


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


def _satellite_from_code(code: str, table: Mapping[str, str], filename: str) -> str:
    for satellite, known in table.items():
        if known == code:
            return satellite
    raise ValueError(f"unknown JPSS spacecraft code {code!r} in {filename}")


def idps_satellite(filename: str) -> str:
    """Spacecraft (``n20``, ``n21``, ``snpp``) of an IDPS granule filename."""
    return _satellite_from_code(granule_key(filename)[0], IDPS_SPACECRAFT, filename)


def nde_satellite(filename: str) -> str:
    """Spacecraft (``n20``, ``n21``, ``snpp``) of an NDE granule filename."""
    return _satellite_from_code(_nde_match(filename)["sat"], NDE_SPACECRAFT, filename)


def granule_id(satellite: str, start: pd.Timestamp) -> str:
    """The stored identity of one granule: its sensing start and its spacecraft.

    ``2026-10-15T12:00:01.5_n20``. Both filename conventions carry the start to a tenth of
    a second, and one spacecraft never starts two granules in the same tenth, so this is
    unique within a product. The spacecraft is part of it because NOAA-20 and NOAA-21
    granules start within 0.1 s of each other. Fixed-width time first, so sorting the ids
    sorts the granules by time.
    """
    start = pd.Timestamp(start)
    return f"{start:%Y-%m-%dT%H:%M:%S}.{start.microsecond // 100000}_{satellite}"


# ------------------------------------------------------------------ stored granules


@dataclass(frozen=True)
class StoredGranules:
    """A group's granule axis, held compactly for membership tests.

    Strings are fixed-width ASCII bytes: ~25 bytes a granule with no Python object each,
    and sorted and searched natively (numpy's variable-width ``StringDType`` sorted the
    ATMS group's 7.6M ids in minutes, not seconds).

    Attributes:
        ids: Every stored :func:`granule_id`, sorted.
        times: Per ``platform_name``, the stored granule ``time`` values, sorted.
    """

    ids: np.ndarray
    times: Dict[str, np.ndarray]

    @staticmethod
    def _strings(values: Any) -> np.ndarray:
        """``values`` as fixed-width bytes, as wide as the longest."""
        strings = np.asarray(values)
        if strings.dtype.kind == "S":
            return strings
        if strings.size == 0:
            return np.zeros(0, dtype="S1")
        strings = strings.astype(np.dtypes.StringDType())
        width = max(int(np.strings.str_len(strings).max()), 1)
        return strings.astype(f"S{width}")

    @classmethod
    def build(cls, ids: Any, times: Any, platforms: Any) -> StoredGranules:
        """Index a granule axis as read from the store (or as about to be written)."""
        platforms = cls._strings(platforms)
        times = np.asarray(times, dtype=NS_DATETIME)
        return cls(
            ids=np.sort(cls._strings(ids)),
            times={
                platform.decode(): np.sort(times[platforms == platform])
                for platform in np.unique(platforms)
            },
        )

    @classmethod
    def extend(
        cls, stored: StoredGranules | None, ids: Any, times: Any, platforms: Any
    ) -> StoredGranules:
        """``stored`` with granules just written added."""
        added = cls.build(ids, times, platforms)
        if stored is None:
            return added
        merged = dict(stored.times)
        for platform, stamps in added.times.items():
            if platform in merged:
                held = merged[platform]
                merged[platform] = np.insert(held, np.searchsorted(held, stamps), stamps)
            else:
                merged[platform] = stamps
        held = stored.ids.astype(np.promote_types(stored.ids.dtype, added.ids.dtype))
        return cls(ids=np.insert(held, np.searchsorted(held, added.ids), added.ids), times=merged)

    def contains(self, ids: Sequence[str]) -> np.ndarray:
        """Whether each of ``ids`` is stored."""
        wanted = self._strings(list(ids))
        if self.ids.size == 0 or wanted.size == 0:
            return np.zeros(wanted.size, dtype=bool)
        # Longer than every stored id: not stored, and cast to the ids' width it would
        # be cut short, so it is masked out rather than looked up.
        fits = np.strings.str_len(wanted) <= self.ids.dtype.itemsize
        probe = wanted.astype(self.ids.dtype)
        at = np.minimum(np.searchsorted(self.ids, probe), self.ids.size - 1)
        return fits & (self.ids[at] == probe)

    def has_time_in(self, platform: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
        """Whether ``platform`` has a stored granule ``time`` in ``[start, end)``."""
        stamps = self.times.get(platform)
        if stamps is None:
            return False
        return bool(
            np.searchsorted(stamps, np.datetime64(end), side="left")
            > np.searchsorted(stamps, np.datetime64(start), side="left")
        )


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


class JpssProductProvider(JpssGranuleProvider):
    """One JPSS product, all three spacecraft, as a group of ``bkr/polar/jpss.icechunk``.

    **Layout.** Each product is a zarr group of the one repository. Its granules are
    appended along a ``granule`` dimension whose coordinate is :func:`granule_id`
    (``2026-10-15T12:00:01.5_n20``), and every granule carries

    * ``time(granule)``: middle of the sensing interval, to the microsecond;
    * ``start_time(granule)``, ``end_time(granule)``: the sensing interval;
    * ``platform_name(granule)``: ``NOAA-21``, ``NOAA-20`` or ``Suomi-NPP``.

    **Why not ``time``.** NOAA-20 and NOAA-21 granule boundaries coincide to within 0.1 s,
    so on a ``time`` axis rounded to the second two spacecraft claim one step and the
    writer, which drops incoming steps already stored, silently loses one of them. Keyed
    by spacecraft and start, every (spacecraft, granule) has its own row.

    **What is done.** A partition is done only when *every* spacecraft expected in it has
    a granule stored inside its window (:meth:`missing_timesteps`); one spacecraft's
    granules do not mark the hour done for the others. Within a partition, granules whose
    id is already stored are not downloaded again, and the writer drops any that slip
    through, so re-running an hour writes nothing.

    **Writes.** A partition's granules from all three spacecraft are written in one
    commit, sorted by id and so by time; the axis stays in time order as long as hours
    are written in order. Each group has one writer; different groups are written by
    different lanes concurrently, and since a commit touches only its own group they
    rebase past each other.

    Attributes:
        group: The product's group in the store.
        satellite_starts: First day each spacecraft has this product, for spacecraft that
            joined after the store's first partition. An hour before it does not wait for
            that spacecraft.
        granule_chunk: Granules per chunk along ``granule`` for every variable, or None to
            leave chunking to the subclass.
        source_product: Bucket prefix(es) named in the group's ``source`` attribute.
    """

    store_prefix = STORE_PREFIX
    append_dim = "granule"
    window = pd.Timedelta(1, "h")
    group: str
    satellite_starts: Mapping[str, str] = {}
    granule_chunk: int | None = None
    source_product: str = ""

    #: Variables written with :data:`GRANULE_TIME_ENCODING`.
    TIME_VARIABLES = ("time", "start_time", "end_time")

    def __init__(
        self,
        config=None,
        satellites: Sequence[str] = SATELLITES,
        granule_limit: int | None = None,
        download_workers: int = 16,
    ):
        """Build a provider for some or all of the constellation.

        Args:
            config: Override the process-wide configuration.
            satellites: Subset of :data:`SATELLITES` to ingest.
            granule_limit: Stop after this many granules per partition. For smoke tests
                against the live archive, not for production backfills.
            download_workers: Concurrent downloads.
        """
        super().__init__(config=config)
        unknown = set(satellites) - set(BUCKETS)
        if unknown:
            raise ValueError(f"unknown JPSS satellites: {sorted(unknown)}")
        self.satellites = tuple(satellites)
        self.granule_limit = granule_limit
        self.download_workers = download_workers
        self._stored_cache: StoredGranules | None = None
        self._stored_cache_valid = False

    # ------------------------------------------------------------------ identity

    @staticmethod
    def satellite_of(filename: str) -> str:
        """Spacecraft of an object key; IDPS names by default."""
        return idps_satellite(filename)

    def id_of(self, filename: str) -> str:
        """Stored :func:`granule_id` of an object key."""
        return granule_id(self.satellite_of(filename), self.start_of(filename))

    def expected_satellites(self, end: pd.Timestamp) -> List[str]:
        """Spacecraft that should have granules in a window ending at ``end``."""
        return [
            sat
            for sat in self.satellites
            if sat not in self.satellite_starts or pd.Timestamp(self.satellite_starts[sat]) < end
        ]

    # ------------------------------------------------------------------ what is stored

    def stored_granules(self) -> StoredGranules | None:
        """The group's granule axis, read from the store, or None when the group is empty.

        Read with zarr, not xarray: the ATMS group's granule axis is 7.6M entries in chunks
        of 64-224, and xarray's pandas index over it (one Python string per granule) held
        ~3 GB and took minutes on every partition. Zarr reads the many small chunks
        :data:`GRANULE_AXIS_CONCURRENCY` at a time, into numpy strings.

        Raises:
            StoreReadError: the group exists but could not be read. Reporting it as empty
                would send every partition round again.
        """
        repo = self.get_icechunk_repo()
        try:
            group = zarr.open_group(repo.readonly_session("main").store, mode="r", path=self.group)
        except STORE_READ_ERRORS as exc:
            if holds_data(repo, self.group):
                raise StoreReadError(
                    f"{self.name}: group {self.group!r} exists but could not be read "
                    f"({type(exc).__name__}: {exc})"
                ) from exc
            return None
        names = set(group.array_keys())
        if self.append_dim not in names or group[self.append_dim].shape[0] == 0:
            return None
        with zarr.config.set({"async.concurrency": GRANULE_AXIS_CONCURRENCY}):
            ids = group[self.append_dim][...]
            stamps = group["time"]
            times = xr.conventions.decode_cf_variable(
                "time", xr.Variable((self.append_dim,), stamps[...], dict(stamps.attrs))
            ).values
            platforms = group["platform_name"][...]
        return StoredGranules.build(ids, times, platforms)

    def stored_index(self, refresh: bool = False) -> StoredGranules | None:
        """:meth:`stored_granules`, read once and then kept current by this provider's writes.

        A group has one writer, so the index only changes through
        :meth:`write_to_icechunk`, which adds what it commits. ``refresh`` reads it again.
        """
        if refresh or not self._stored_cache_valid:
            self._stored_cache = self.stored_granules()
            self._stored_cache_valid = True
        return self._stored_cache

    def stored_ids(self) -> set[str]:
        """Every granule id already in the group."""
        stored = self.stored_index()
        return set() if stored is None else {value.decode() for value in stored.ids}

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Partitions in which some expected spacecraft has no stored granule.

        Checked per spacecraft: a partition written while one spacecraft's granules had
        not reached the bucket yet stays missing for that spacecraft, and is completed by
        the next pass rather than counted as done. Reads the store afresh: this is called
        once per pass, and the pass then works from the index.
        """
        stored = self.stored_index(refresh=True)
        if stored is None:
            return list(desired)

        missing = []
        for it in desired:
            start, end = self.partition_window(it)
            for satellite in self.expected_satellites(end):
                if not stored.has_time_in(PLATFORM_NAMES[satellite], start, end):
                    missing.append(it)
                    break
        return missing

    def drop_stored(self, keys: Sequence[str]) -> List[str]:
        """``keys`` without the granules already in the group, so they are not re-fetched."""
        stored = self.stored_index()
        if stored is None or not keys:
            return list(keys)
        held = stored.contains([self.id_of(key) for key in keys])
        kept = [key for key, present in zip(keys, held) if not present]
        if len(kept) < len(keys):
            logger.info(f"{self.name}: {len(keys) - len(kept)} granule(s) already stored")
        return kept

    # ------------------------------------------------------------------ shaping

    def on_granule_axis(
        self,
        ds: xr.Dataset,
        satellite: str,
        id_start: pd.Timestamp,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> xr.Dataset:
        """Put one granule on the ``granule`` axis with its time and spacecraft.

        Args:
            ds: The granule, without a granule or time dimension.
            satellite: Its spacecraft.
            id_start: Sensing start as the filename gives it, for :func:`granule_id`.
            start: Sensing start.
            end: Sensing end.
        """
        dim = self.append_dim
        # expand_dims only touches data variables, so static coordinates stay static.
        ds = ds.expand_dims(dim).assign_coords(
            {dim: np.array([granule_id(satellite, id_start)], dtype=object)}
        )
        ds["time"] = xr.DataArray(
            np.array([mid_time(start, end, round_to=None)], dtype=NS_DATETIME), dims=dim
        )
        ds["start_time"] = xr.DataArray(np.array([start], dtype=NS_DATETIME), dims=dim)
        ds["end_time"] = xr.DataArray(np.array([end], dtype=NS_DATETIME), dims=dim)
        # object, not fixed-width: a first write of only NOAA-20 would otherwise fix the
        # width at seven characters and refuse "Suomi-NPP" ever after.
        ds["platform_name"] = xr.DataArray(
            np.array([PLATFORM_NAMES[satellite]], dtype=object), dims=dim
        )
        return ds

    def group_attrs(self, instrument: str) -> Dict[str, str]:
        """Attributes of the group, the same whichever spacecraft a partition held."""
        buckets = ", ".join(f"s3://{BUCKETS[sat]}" for sat in SATELLITES)
        return {
            "instrument": instrument,
            "platforms": ", ".join(PLATFORM_NAMES[sat] for sat in SATELLITES),
            "source": f"{buckets} ({self.source_product})",
        }

    def restrict_to_window(self, ds: xr.Dataset, it: pd.Timestamp) -> xr.Dataset:
        """Drop granules whose ``time`` falls outside the partition's own window."""
        dim = self.append_dim
        if "time" not in ds or ds.sizes.get(dim, 0) == 0:
            return ds
        start, end = self.partition_window(it)
        times = ds["time"].values
        keep = (times >= np.datetime64(start)) & (times < np.datetime64(end))
        dropped = int((~keep).sum())
        if not dropped:
            return ds
        logger.info(
            f"{self.name}: dropping {dropped} granule(s) outside {start} - {end}; "
            "the neighbouring partition owns them"
        )
        return ds.isel({dim: keep})

    def owned_by(self, filename: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
        """True when the granule's stored ``time`` falls in ``[start, end)``.

        The same unrounded midpoint :meth:`on_granule_axis` stores, so it is the exact
        test :meth:`restrict_to_window` applies after processing.
        """
        stamp = mid_time(self.start_of(filename), self.end_of(filename), round_to=None)
        return start <= stamp < end

    # ------------------------------------------------------------------ writing

    def encoding_for(self, ds: xr.Dataset) -> Dict[str, Dict[str, Any]]:
        """Encoding applied when the group is created, by source variable name.

        :attr:`granule_chunk` granules per chunk for every variable on the granule axis,
        and microseconds since the epoch for the granule times. Subclasses add packing.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if self.granule_chunk is not None:
            for name, var in ds.data_vars.items():
                if self.append_dim in var.dims:
                    chunks = tuple(
                        self.granule_chunk if dim == self.append_dim else var.sizes[dim]
                        for dim in var.dims
                    )
                    out[str(name)] = {"chunks": chunks}
        for name in self.TIME_VARIABLES:
            out[name] = {**out.get(name, {}), **GRANULE_TIME_ENCODING}
        return out

    def _store_names(self) -> Dict[str, str]:
        return naming.store_names(self.store_prefix, self.group)

    def keepbits_for(self, variable: str) -> int | None:
        """:meth:`BaseProvider.keepbits_for`, looked up by the variable's source name.

        Rounding runs after the store's renames, while the per-variable table is keyed by
        source name; without the translation a packed variable would be rounded too.
        """
        source = {new: old for old, new in self._store_names().items()}
        return super().keepbits_for(source.get(variable, variable))

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Write into :attr:`group` with :meth:`encoding_for` when creating it.

        The encoding is keyed by source name and translated to the stored names; it only
        takes effect on the group's first write, appends reuse the stored encoding.
        """
        renames = self._store_names()
        overrides: Dict[str, Dict[str, Any]] = {}
        # Chunks come keyed by the stored name, packing by the source name: merge the two.
        for name, encoding in self.encoding_for(processed).items():
            stored_name = renames.get(name, name)
            overrides[stored_name] = {**overrides.get(stored_name, {}), **encoding}
        self.encoding_overrides = overrides
        try:
            wrote = super().write_to_icechunk(repo, processed)
        except BaseException:
            # Whatever reached the store is unknown; read it again next time.
            self._stored_cache_valid = False
            raise
        if wrote and self._stored_cache_valid:
            # The granule axis keeps its names in the store, so these are the stored ones.
            self._stored_cache = StoredGranules.extend(
                self._stored_cache,
                processed[self.append_dim].values,
                processed["time"].values,
                processed["platform_name"].values,
            )
        return wrote
