"""Virtual Icechunk stores over NREL's National Solar Radiation Database (NSRDB).

NREL publishes the NSRDB on the AWS Open Data registry as HDF5 files in
``s3://nrel-pds-nsrdb`` (us-west-2, anonymous). Every dataset splits each year
into seven *group* files — ``ancillary_a``, ``ancillary_b``, ``clearsky``,
``clouds``, ``csp``, ``irradiance`` and ``pv`` — of 0.2 to 5 TB each, far too
large to load and too slow to copy. This module therefore builds one *virtual*
store per dataset: every ``(time, gid)`` variable of every group and every year
sits on a single ``time`` axis as Icechunk virtual chunk references (byte
ranges) into NREL's files. Only the small arrays are materialised: ``time``, a
``valid`` mask and the per-site ``meta`` table.

Why the store looks the way it does:

* **The HDF5 chunks are raw bytes.** The 2D variables carry no filters at all,
  so a Zarr array with only a ``BytesCodec`` decodes them. Any compressor —
  including the zstd default xarray and Zarr add — breaks decoding, so every
  virtual array is created with ``compressors=None``.
* **Years are padded to whole chunks.** Zarr and VirtualiZarr only support a
  regular chunk grid, and a year rarely spans a whole number of time chunks.
  HDF5 stores the edge chunk at full size, so padding each year to
  ``ceil(n_time / chunk_t) * chunk_t`` rows makes it exactly whole chunks and
  years concatenate by reference. Pad rows have ``time = NaT`` and
  ``valid = False``; :func:`open_nsrdb` drops them. (numpy 2.5 with xarray
  2025.12 cannot decode a NaT time at all; open such a store there with
  ``decode_times=False``, or through :func:`open_nsrdb`, which decodes itself.)
* **A year with a different chunk shape goes into a layout group.** Himawari-7
  leap years are chunked ``(1348, 742)`` instead of ``(1344, 745)``, which
  cannot share an array with the other years. Such a year is written to a
  subgroup ``layout_<t>x<g>`` with its own ``time``, ``valid`` and variables;
  :func:`open_nsrdb` stitches the groups back together.
* **Variables are float32 arrays over integer chunks.** NREL stores scaled
  integers with ``physical = stored / scale_factor``, the inverse of CF. A CF
  ``scale_factor`` attribute cannot fix that well: Zarr attributes are JSON, so
  it reads back as a Python float and xarray decodes every variable to
  float64. Instead each array is declared float32 with a
  ``numcodecs.fixedscaleoffset`` codec (``scale = nsrdb_scale_factor``,
  ``astype`` the stored integer dtype), so any Zarr reader gets float32
  ``stored / scale_factor``, as NREL's ``rex`` does, and absent chunks read as
  NaN. The original factor and dtype are kept as ``nsrdb_scale_factor`` and
  ``nsrdb_stored_dtype``. :func:`migrate_float32` converts a store written with
  the earlier integer-plus-CF-attributes encoding, in place.
* **One commit per year.** A reader never sees half a year. When a year's
  references are too many to hold in one change set, the year is committed in
  pieces to a scratch branch ``ingest-<year>`` and ``main`` is then reset to
  its tip in one step.

Finding where each chunk lives is the slow part: about a hundred chunks a
second through h5py over S3. The indexer is pluggable; the default prefers
``planetary_datasets.common.hdf5_chunk_index.read_chunk_index`` when it is
installed and falls back to h5py's ``chunk_iter`` in a process pool.

Usage::

    python -m planetary_datasets.providers.virtualized.nsrdb list
    python -m planetary_datasets.providers.virtualized.nsrdb ingest --dataset himawari7
    python -m planetary_datasets.providers.virtualized.nsrdb ingest --dataset msg_v4 --years 2005
    python -m planetary_datasets.providers.virtualized.nsrdb check

    from planetary_datasets.providers.virtualized.nsrdb import open_nsrdb
    ds = open_nsrdb("himawari7")
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import math
import multiprocessing
import pathlib
import re
import sys
import time
import zipfile
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping, Sequence

import numpy as np
import xarray as xr
from loguru import logger

from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import log_memory, memory_guard
from planetary_datasets.providers.virtualized import virtual_repo
from planetary_datasets.providers.virtualized.virtual_repo import OutOfOrderPartition

if TYPE_CHECKING:
    import h5py
    import icechunk
    import zarr

__all__ = [
    "BUCKET",
    "DATASETS",
    "GROUPS",
    "IncompleteYear",
    "IngestReport",
    "LayoutConflict",
    "MetaMismatch",
    "NSRDBDataset",
    "NSRDBError",
    "OutOfOrderPartition",
    "Source",
    "UnsupportedLayout",
    "describe",
    "discover_files",
    "ingest",
    "open_nsrdb",
    "open_repo",
    "store_prefix_for",
]

# =============================================================================
# Constants
# =============================================================================
#: NREL's public bucket. This is the identity of the dataset rather than a
#: deployment choice, so it is a constant; the *output* store location is
#: resolved through the shared config.
BUCKET = "s3://nrel-pds-nsrdb"
SOURCE_REGION = "us-west-2"

#: Logical store location, one store per catalog key under it.
STORE_BASE = "bkr/nsrdb"

#: The seven files every NSRDB year is split into.
GROUPS: tuple[str, ...] = (
    "ancillary_a",
    "ancillary_b",
    "clearsky",
    "clouds",
    "csp",
    "irradiance",
    "pv",
)

#: Upper bound on references per manifest split. The time split is one padded
#: year; the gid split is sized so the two together stay under this.
MAX_REFS_PER_MANIFEST = 100_000

#: References held in one Icechunk change set before a year is committed in
#: pieces to a scratch branch. A reference costs on the order of a hundred
#: bytes in the change set, plus the Python list VirtualiZarr builds to hand it
#: over, so two million keeps a commit well under a gigabyte.
DEFAULT_MAX_REFS_PER_COMMIT = 2_000_000

DEFAULT_WORKERS = 4

#: Rows of ``meta`` read at a time, and the chunk length of every gid array.
#: ``meta`` is 0.3-1.4 GB, so it is never read whole.
META_SLICE_ROWS = 1 << 20

#: The fingerprint hashes latitude/longitude of this many rows at each end of
#: ``meta``, plus an evenly strided sample. Every strided row costs a ranged
#: read of one ``meta`` chunk, so the sample is kept small.
FINGERPRINT_EDGE_ROWS = 1000
FINGERPRINT_SAMPLES = 32

#: Meta fields normalised to float32, whatever their dtype in a given year.
FLOAT_META_FIELDS = ("latitude", "longitude", "elevation", "timezone")

#: Meta fields renamed because their name is taken in the store.
META_RENAME = {"gid": "source_gid"}

META_ATTRS: dict[str, dict[str, str]] = {
    "latitude": {"standard_name": "latitude", "units": "degrees_north"},
    "longitude": {"standard_name": "longitude", "units": "degrees_east"},
    "elevation": {"long_name": "site elevation", "units": "m"},
    "timezone": {"long_name": "offset of local standard time from UTC, in hours"},
}

#: Datasets in a group file that are not ``(time, gid)`` variables.
#: ``coordinates`` duplicates latitude/longitude and is dropped.
NON_VARIABLE_DATASETS = frozenset({"meta", "time_index", "coordinates"})

#: HDF5 variable attributes not carried over. ``chunks`` records NREL's nominal
#: chunking, which is not the chunking of the file and would only mislead.
DROPPED_VARIABLE_ATTRS = frozenset({"chunks"})

#: ``time`` is encoded as int64 seconds; pad rows are NaT.
TIME_UNITS = "seconds since 1970-01-01"
_NAT_INT64 = np.iinfo(np.int64).min

LAYOUT_PREFIX = "layout_"
SCRATCH_BRANCH = "ingest-{year}"

#: Root attributes recording provenance, keyed by year as a string.
ATTR_VERSIONS = "nsrdb_versions"
ATTR_FILES = "nsrdb_source_files"
ATTR_YEAR_GROUPS = "nsrdb_year_groups"
ATTR_YEAR_LAYOUT = "nsrdb_year_layout"
ATTR_FINGERPRINT = "nsrdb_meta_fingerprint"
ATTR_META_FIELDS = "nsrdb_meta_fields"


# =============================================================================
# Errors
# =============================================================================
class NSRDBError(RuntimeError):
    """Base class for NSRDB ingest errors."""


class IncompleteYear(NSRDBError):
    """A requested year lacks one or more of its group files in the bucket."""


class MetaMismatch(NSRDBError):
    """A year's site table does not match the one the store was built with."""


class UnsupportedLayout(NSRDBError):
    """A source variable cannot be referenced virtually as raw chunks."""


class LayoutConflict(NSRDBError):
    """A variable cannot be placed in the group its year already lives in."""


# =============================================================================
# Catalog
# =============================================================================
@dataclass(frozen=True)
class NSRDBDataset:
    """One NSRDB dataset in the bucket, ingested into one store.

    Attributes:
        key: Catalog key, also the store name.
        directory: Bucket prefix the year files sit directly under.
        stem: File name stem; files are ``<stem>_<group>_<YYYY>.h5``.
        step_minutes: Nominal time step.
        time_chunk: Nominal time chunk length of the 16-bit variables, used
            only to size the manifest splits.
        description: Human-readable summary.
    """

    key: str
    directory: str
    stem: str
    step_minutes: int
    time_chunk: int
    description: str

    @property
    def file_pattern(self) -> re.Pattern[str]:
        """Matches a year's group file name, and nothing else (no tmy/tdy/tgy)."""
        groups = "|".join(GROUPS)
        return re.compile(rf"^{re.escape(self.stem)}_(?P<group>{groups})_(?P<year>\d{{4}})\.h5$")

    def filename(self, group: str, year: int) -> str:
        """The bucket key of one group file."""
        return f"{self.directory}{self.stem}_{group}_{year}.h5"

    @property
    def time_chunks_per_year(self) -> int:
        """Time chunks in a padded non-leap year; the time split size."""
        steps = 365 * 24 * 60 // self.step_minutes
        return math.ceil(steps / self.time_chunk)


DATASETS: dict[str, NSRDBDataset] = {
    d.key: d
    for d in (
        NSRDBDataset(
            "goes_full_disc_v4",
            "GOES/full_disc/v4.0.0/",
            "nsrdb_full_disc",
            10,
            2000,
            "NSRDB v4 GOES full disc, 10-minute, 2 km",
        ),
        NSRDBDataset(
            "msg_v4",
            "msg/v1.0.0/",
            "msg",
            15,
            2000,
            "NSRDB Meteosat Second Generation (Europe/Africa), 15-minute",
        ),
        NSRDBDataset(
            "polar_v4",
            "polar/v4.0.0/",
            "nsrdb_polar",
            60,
            2000,
            "NSRDB v4 polar (Arctic), hourly",
        ),
        NSRDBDataset(
            "himawari8",
            # The bucket root of himawari/, not the incomplete himawari/himawari8/ copy.
            "himawari/",
            "himawari8",
            10,
            2016,
            "NSRDB Himawari-8 (Asia/Pacific), 10-minute",
        ),
        NSRDBDataset(
            "himawari7",
            # The subfolder; the himawari7_* files at the root are duplicates.
            "himawari/himawari7/",
            "himawari7",
            30,
            1344,
            "NSRDB Himawari-7 (Asia/Pacific), 30-minute",
        ),
        NSRDBDataset(
            "meteosat",
            "meteosat/",
            "meteosat",
            15,
            1344,
            "NSRDB Meteosat IODC (South Asia), 15-minute",
        ),
    )
}


def get_dataset(key: str) -> NSRDBDataset:
    """Look up a catalog entry.

    Raises:
        KeyError: for an unknown key, naming the known ones.
    """
    try:
        return DATASETS[key]
    except KeyError:
        raise KeyError(f"unknown NSRDB dataset {key!r}; known: {', '.join(DATASETS)}") from None


def store_prefix_for(key: str) -> str:
    """Store prefix of one dataset, e.g. ``bkr/nsrdb/himawari7.icechunk``."""
    return virtual_repo.store_prefix(f"{STORE_BASE}/{get_dataset(key).key}")


# =============================================================================
# Source bucket
# =============================================================================
@dataclass(frozen=True)
class Source:
    """Where the HDF5 files are read from, and how virtual chunks reach them.

    The default is NREL's bucket. Tests point ``url`` at a ``file://``
    directory laid out like the bucket, which swaps the obstore listing, the
    h5py opener and the Icechunk virtual chunk container for local ones.

    Attributes:
        url: ``s3://bucket`` or ``file:///absolute/dir``, without a trailing slash.
        region: Region of an S3 source.
    """

    url: str = BUCKET
    region: str = SOURCE_REGION

    @property
    def is_local(self) -> bool:
        """True for a ``file://`` source."""
        return self.url.startswith("file://")

    @property
    def url_prefix(self) -> str:
        """The URL prefix with one trailing slash, as Icechunk containers want."""
        return self.url.rstrip("/") + "/"

    def key_url(self, key: str) -> str:
        """Full URL of a bucket key."""
        return self.url_prefix + key.lstrip("/")

    def object_store(self) -> Any:
        """An anonymous obstore store rooted at the source."""
        from obstore.store import LocalStore, from_url

        if self.is_local:
            return LocalStore(prefix=self.url[len("file://") :])
        return from_url(self.url, region=self.region, skip_signature=True)

    def container(self) -> "icechunk.VirtualChunkContainer":
        """The Icechunk virtual chunk container serving this source."""
        import icechunk

        if self.is_local:
            store = icechunk.local_filesystem_store(self.url[len("file://") :])
        else:
            store = icechunk.s3_store(region=self.region, anonymous=True)
        return icechunk.VirtualChunkContainer(url_prefix=self.url_prefix, store=store)

    def credentials(self) -> Any:
        """Credentials authorising reads through :meth:`container`."""
        import icechunk

        if self.is_local:
            return icechunk.credentials.LocalFileSystemAccess
        return icechunk.s3_anonymous_credentials()


@dataclass(frozen=True)
class SourceFile:
    """One group file as listed in the bucket."""

    group: str
    year: int
    key: str
    url: str
    size: int
    e_tag: str | None
    last_modified: datetime.datetime | None


def discover_files(key: str, *, source: Source | None = None) -> dict[int, dict[str, SourceFile]]:
    """List a dataset's group files in the bucket, by year then group.

    Only files named ``<stem>_<group>_<YYYY>.h5`` directly under the dataset's
    directory count, which leaves out the tmy/tdy/tgy files and the duplicate
    copies some datasets keep in sibling folders.

    Args:
        key: Catalog key.
        source: Source bucket; defaults to NREL's.

    Returns:
        ``{year: {group: SourceFile}}``. A year may lack some groups.
    """
    import obstore as obs

    dataset = get_dataset(key)
    source = source or Source()
    listing = obs.list_with_delimiter(source.object_store(), prefix=dataset.directory)
    pattern = dataset.file_pattern
    found: dict[int, dict[str, SourceFile]] = {}
    for meta in listing["objects"]:
        name = meta["path"].rsplit("/", 1)[-1]
        match = pattern.match(name)
        if match is None:
            continue
        year, group = int(match["year"]), match["group"]
        found.setdefault(year, {})[group] = SourceFile(
            group=group,
            year=year,
            key=meta["path"],
            url=source.key_url(meta["path"]),
            size=int(meta["size"]),
            e_tag=meta.get("e_tag"),
            last_modified=meta.get("last_modified"),
        )
    return dict(sorted(found.items()))


@contextlib.contextmanager
def _open_h5(
    url: str,
    region: str = SOURCE_REGION,
    *,
    block_size: int = 1 << 20,
    cache_type: str = "blockcache",
) -> Iterator["h5py.File"]:
    """Open a local or anonymous-S3 HDF5 file read-only.

    Remote files are read through s3fs with ranged requests only; opening one
    takes a couple of seconds and never downloads it.
    """
    import h5py

    if url.startswith("file://"):
        with h5py.File(url[len("file://") :], "r") as f:
            yield f
        return
    import s3fs

    fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": region})
    with fs.open(url, "rb", block_size=block_size, cache_type=cache_type) as fh:
        with h5py.File(fh, "r") as f:
            yield f


# =============================================================================
# File metadata
# =============================================================================
@dataclass(frozen=True)
class VarInfo:
    """A ``(time, gid)`` variable of one group file, from h5py metadata alone."""

    name: str
    group: str
    url: str
    dtype: str
    shape: tuple[int, int]
    chunks: tuple[int, int]
    attrs: dict[str, Any]

    @property
    def scale_factor(self) -> float:
        """NSRDB scale factor: physical = stored / scale_factor."""
        return float(self.attrs.get("scale_factor", 1.0))

    def signature(self) -> tuple[Any, ...]:
        """What must agree for two years to share this variable's array."""
        return _signature(self.chunks[1], self.dtype, self.scale_factor)


@dataclass(frozen=True)
class FileInfo:
    """What one group file holds, read cheaply from its HDF5 metadata."""

    group: str
    url: str
    version: str | None
    times: np.ndarray
    n_gid: int
    meta_fields: tuple[tuple[str, str], ...]
    variables: tuple[VarInfo, ...]


def _signature(gid_chunk: int, stored_dtype: Any, scale_factor: float) -> tuple[Any, ...]:
    return (int(gid_chunk), np.dtype(stored_dtype).str, float(scale_factor))


def _scale_codec(stored_dtype: Any, scale_factor: float) -> dict[str, Any]:
    """The codec that turns NREL's stored integers into float32 physical values.

    ``numcodecs.fixedscaleoffset`` decodes ``stored / scale + offset`` and casts
    to ``dtype``, which is NREL's convention exactly. Absent chunks are not
    decoded at all and read as the array's NaN fill, so no sentinel is needed;
    the HDF5 fill of 0 is a legitimate reading (night-time GHI, clear-sky cloud
    type) and could not have served as one.
    """
    stored = np.dtype(stored_dtype)
    if stored.kind not in "iu" and stored != np.float32:
        raise UnsupportedLayout(f"unsupported variable dtype {stored}")
    return {
        "name": "numcodecs.fixedscaleoffset",
        "configuration": {
            "offset": 0,
            "scale": float(scale_factor),
            "dtype": "<f4",
            "astype": stored.newbyteorder("<").str,
        },
    }


def _sanitize_attr(value: Any) -> Any:
    """Turn an HDF5 attribute into something JSON can hold."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, np.ndarray):
        return [_sanitize_attr(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_sanitize_attr(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def _parse_time_index(raw: np.ndarray) -> np.ndarray:
    """Parse NSRDB ``time_index`` strings into naive-UTC datetime64[ns].

    Most years read ``2018-01-01 00:00:00+00:00``; some lack the offset.
    """
    import pandas as pd

    strings = [s.decode() if isinstance(s, bytes) else str(s) for s in raw]
    index = pd.to_datetime(strings, utc=True, format="ISO8601")
    return index.tz_convert(None).values.astype("datetime64[ns]")


def _read_file_info(url: str, region: str, group: str) -> FileInfo:
    """Read one group file's layout. Runs in a worker process.

    Raises:
        UnsupportedLayout: when a variable is contiguous or filtered, so its
            chunks cannot be referenced as raw bytes.
    """
    with _open_h5(url, region) as f:
        version = f.attrs.get("version")
        times = _parse_time_index(f["time_index"][:])
        meta = f["meta"]
        n_gid = int(meta.shape[0])
        meta_fields = tuple((name, meta.dtype[name].str) for name in meta.dtype.names)
        variables = []
        for name, obj in f.items():
            if name in NON_VARIABLE_DATASETS or not hasattr(obj, "shape"):
                continue
            if obj.ndim != 2 or obj.shape != (len(times), n_gid):
                logger.debug(f"{url}: skipping {name} {obj.shape}, not (time, gid)")
                continue
            if obj.chunks is None:
                raise UnsupportedLayout(f"{url}:{name} is contiguous, not chunked")
            if obj.id.get_create_plist().get_nfilters():
                raise UnsupportedLayout(f"{url}:{name} has HDF5 filters; only raw chunks work")
            if obj.dtype.byteorder == ">":
                raise UnsupportedLayout(
                    f"{url}:{name} is big-endian; chunks are read little-endian"
                )
            attrs = {
                k: _sanitize_attr(v)
                for k, v in obj.attrs.items()
                if k not in DROPPED_VARIABLE_ATTRS
            }
            variables.append(
                VarInfo(
                    name=name,
                    group=group,
                    url=url,
                    dtype=obj.dtype.str,
                    shape=(int(obj.shape[0]), int(obj.shape[1])),
                    chunks=(int(obj.chunks[0]), int(obj.chunks[1])),
                    attrs=attrs,
                )
            )
    return FileInfo(
        group=group,
        url=url,
        version=None if version is None else str(_sanitize_attr(version)),
        times=times,
        n_gid=n_gid,
        meta_fields=meta_fields,
        variables=tuple(variables),
    )


@dataclass
class YearPlan:
    """Everything needed to write one year (or some of its groups)."""

    year: int
    files: dict[str, SourceFile]
    infos: dict[str, FileInfo]
    times: np.ndarray
    n_gid: int
    t_chunk: int
    variables: dict[str, VarInfo]

    @property
    def n_time(self) -> int:
        """Real time steps in the year."""
        return int(self.times.size)

    @property
    def n_pad(self) -> int:
        """Rows the year occupies once padded to whole time chunks."""
        return math.ceil(self.n_time / self.t_chunk) * self.t_chunk

    @property
    def groups(self) -> list[str]:
        """Groups present in this plan, in canonical order."""
        return [g for g in GROUPS if g in self.infos]

    @property
    def meta_url(self) -> str:
        """The file the year's site table is read from."""
        return self.infos[self.groups[0]].url

    @property
    def meta_fields(self) -> tuple[tuple[str, str], ...]:
        """Fields of the year's site table."""
        return self.infos[self.groups[0]].meta_fields

    def padded_times(self) -> tuple[np.ndarray, np.ndarray]:
        """``time`` (NaT on pad rows) and ``valid`` for the padded year."""
        times = np.full(self.n_pad, np.datetime64("NaT", "ns"))
        times[: self.n_time] = self.times
        valid = np.zeros(self.n_pad, dtype=bool)
        valid[: self.n_time] = True
        return times, valid

    def versions(self, earlier: Any = None, earlier_groups: Sequence[str] = ()) -> Any:
        """The ``version`` attribute, or one per group when the groups disagree.

        Args:
            earlier: The year's recorded value before this plan's groups were
                added: a version, ``{group: version}``, or None.
            earlier_groups: Groups that recorded value covers.
        """
        if isinstance(earlier, dict):
            by_group = dict(earlier)
        else:
            by_group = {g: earlier for g in earlier_groups}
        by_group.update({g: self.infos[g].version for g in self.groups})
        distinct = set(by_group.values())
        return distinct.pop() if len(distinct) == 1 else by_group

    def last_modified(self) -> datetime.datetime | None:
        """Newest modification time of the year's files, for the reference checksum."""
        stamps = [f.last_modified for f in self.files.values() if f.last_modified is not None]
        return max(stamps) + datetime.timedelta(seconds=1) if stamps else None


def _build_plan(year: int, files: dict[str, SourceFile], infos: dict[str, FileInfo]) -> YearPlan:
    """Check one year's group files agree with each other and assemble a plan.

    Raises:
        UnsupportedLayout: when the files disagree on time, sites or time chunk.
    """
    groups = [g for g in GROUPS if g in infos]
    first = infos[groups[0]]
    for g in groups[1:]:
        info = infos[g]
        if info.n_gid != first.n_gid:
            raise UnsupportedLayout(
                f"{year}: {g} has {info.n_gid} sites, {groups[0]} has {first.n_gid}"
            )
        if not np.array_equal(info.times, first.times):
            raise UnsupportedLayout(f"{year}: {g} and {groups[0]} have different time_index")

    steps = np.unique(np.diff(first.times))
    if steps.size > 1:
        logger.warning(f"{year}: time_index is not regular ({steps.size} distinct steps)")
    if first.times.size and first.times[0].astype("datetime64[Y]").astype(int) + 1970 != year:
        logger.warning(f"{year}: time_index starts at {first.times[0]}, outside the file's year")

    variables: dict[str, VarInfo] = {}
    for g in groups:
        for var in infos[g].variables:
            if var.name in variables:
                logger.warning(
                    f"{year}: {var.name} is in both {variables[var.name].group} and {g}; "
                    f"keeping {variables[var.name].group}"
                )
                continue
            variables[var.name] = var
    if not variables:
        raise UnsupportedLayout(f"{year}: no (time, gid) variables in {groups}")

    t_chunks = {v.chunks[0] for v in variables.values()}
    if len(t_chunks) != 1:
        raise UnsupportedLayout(f"{year}: variables disagree on the time chunk: {sorted(t_chunks)}")
    return YearPlan(
        year=year,
        files={g: files[g] for g in groups},
        infos=infos,
        times=first.times,
        n_gid=first.n_gid,
        t_chunk=t_chunks.pop(),
        variables=variables,
    )


# =============================================================================
# Chunk indexing
# =============================================================================
@dataclass(frozen=True)
class _ChunkIndex:
    """Where each chunk of one HDF5 dataset lives. Same shape as the shared one."""

    shape: tuple[int, ...]
    chunk_shape: tuple[int, ...]
    dtype: np.dtype
    offsets: np.ndarray
    lengths: np.ndarray


#: ``(url, dataset) -> chunk index``. Anything with the attributes of
#: :class:`_ChunkIndex` will do.
ChunkIndexer = Callable[[str, str], Any]


def _h5py_chunk_index(dset: "h5py.Dataset") -> _ChunkIndex:
    """Index an open dataset with ``chunk_iter``; unallocated chunks get length 0."""
    shape = tuple(int(n) for n in dset.shape)
    chunks = tuple(int(n) for n in dset.chunks)
    grid = tuple(math.ceil(s / c) for s, c in zip(shape, chunks))
    offsets = np.zeros(grid, dtype=np.uint64)
    lengths = np.zeros(grid, dtype=np.uint64)

    def record(info: Any) -> None:
        if info.filter_mask:
            raise UnsupportedLayout(f"chunk {info.chunk_offset} has filters applied")
        idx = tuple(o // c for o, c in zip(info.chunk_offset, chunks))
        offsets[idx] = info.byte_offset
        lengths[idx] = info.size

    dset.id.chunk_iter(record)
    return _ChunkIndex(shape, chunks, dset.dtype, offsets, lengths)


def _h5py_index_task(url: str, region: str, dataset: str) -> _ChunkIndex:
    """Index one dataset of one file. Top-level so a process pool can run it."""
    with _open_h5(url, region, block_size=1 << 18) as f:
        return _h5py_chunk_index(f[dataset])


def _shared_indexer() -> Callable[..., Any] | None:
    """``read_chunk_index`` from the shared module, when it is installed."""
    try:
        from planetary_datasets.common.hdf5_chunk_index import read_chunk_index
    except ImportError:
        return None
    return read_chunk_index


def _make_pool(workers: int) -> Executor:
    """A process pool for h5py work. h5py serialises on a global lock, so threads don't help.

    Spawned rather than forked: the parent holds Icechunk's and s3fs's runtimes
    and threads, which a fork would copy in an arbitrary state.
    """
    return ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"))


def _cache_path(cache_dir: pathlib.Path, plan_file: SourceFile, var: VarInfo) -> pathlib.Path:
    ident = f"{plan_file.url}|{plan_file.e_tag}|{plan_file.size}|{var.name}"
    digest = hashlib.sha256(ident.encode()).hexdigest()[:20]
    return cache_dir / str(plan_file.year) / f"{var.group}_{var.name}_{digest}.npz"


def _load_cached(path: pathlib.Path) -> _ChunkIndex | None:
    try:
        with np.load(path) as z:
            return _ChunkIndex(
                tuple(int(n) for n in z["shape"]),
                tuple(int(n) for n in z["chunk_shape"]),
                np.dtype(str(z["dtype"])),
                z["offsets"],
                z["lengths"],
            )
    except (OSError, KeyError, ValueError, zipfile.BadZipFile) as exc:
        logger.warning(f"ignoring unreadable chunk index cache {path}: {exc}")
        return None


def _save_cached(path: pathlib.Path, index: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.npz")
    np.savez(
        tmp,
        shape=np.asarray(index.shape),
        chunk_shape=np.asarray(index.chunk_shape),
        dtype=np.asarray(np.dtype(index.dtype).str),
        offsets=np.asarray(index.offsets, dtype=np.uint64),
        lengths=np.asarray(index.lengths, dtype=np.uint64),
    )
    tmp.replace(path)


def _check_index(var: VarInfo, index: Any) -> None:
    grid = tuple(math.ceil(s / c) for s, c in zip(var.shape, var.chunks))
    if tuple(index.shape) != var.shape or tuple(index.chunk_shape) != var.chunks:
        raise UnsupportedLayout(
            f"{var.url}:{var.name}: index says {index.shape}/{index.chunk_shape}, "
            f"h5py says {var.shape}/{var.chunks}"
        )
    if tuple(np.shape(index.offsets)) != grid or tuple(np.shape(index.lengths)) != grid:
        raise UnsupportedLayout(f"{var.url}:{var.name}: index grid is not {grid}")


def index_variables(
    plan: YearPlan,
    names: Sequence[str],
    *,
    source: Source,
    indexer: ChunkIndexer | None = None,
    workers: int = DEFAULT_WORKERS,
    cache_dir: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Find every chunk of the named variables of one year.

    Args:
        plan: The year.
        names: Variables to index.
        source: Source bucket.
        indexer: ``(url, dataset) -> index``; replaces the default entirely.
        workers: Concurrent indexing tasks. ``0`` or ``1`` runs in-process.
        cache_dir: Directory of saved indexes, keyed by file ETag. Indexing a
            full-disc year takes hours, so a rerun after a failure reuses it.

    Returns:
        ``{name: index}`` with ``shape``, ``chunk_shape``, ``offsets`` and ``lengths``.

    Raises:
        UnsupportedLayout: when an index disagrees with the h5py metadata.
    """
    out: dict[str, Any] = {}
    todo: list[VarInfo] = []
    for name in names:
        var = plan.variables[name]
        cached = None
        if cache_dir is not None:
            path = _cache_path(cache_dir, plan.files[var.group], var)
            cached = _load_cached(path) if path.exists() else None
            if cached is not None:
                try:
                    _check_index(var, cached)
                except UnsupportedLayout as exc:
                    logger.warning(f"ignoring stale chunk index cache {path}: {exc}")
                    cached = None
        if cached is not None:
            out[name] = cached
        else:
            todo.append(var)
    if not todo:
        return out

    n_chunks = sum(math.prod(math.ceil(s / c) for s, c in zip(v.shape, v.chunks)) for v in todo)
    logger.info(
        f"{plan.year}: indexing {len(todo)} variable(s), {n_chunks:,} chunks, {len(out)} from cache"
    )
    started = time.monotonic()

    def done(var: VarInfo, index: Any) -> None:
        _check_index(var, index)
        out[var.name] = index
        if cache_dir is not None:
            _save_cached(_cache_path(cache_dir, plan.files[var.group], var), index)
        logger.info(
            f"{plan.year}: indexed {var.group}/{var.name} "
            f"({len(out)}/{len(names)}, {time.monotonic() - started:.0f}s)"
        )

    if indexer is not None:
        for var in todo:
            done(var, indexer(var.url, var.name))
        return out

    fallback: list[VarInfo] = []
    shared = _shared_indexer()
    if shared is None:
        fallback = todo
    else:
        store = source.object_store()
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(shared, store, plan.files[v.group].key, v.name): v for v in todo}
            for future in as_completed(futures):
                var = futures[future]
                try:
                    done(var, future.result())
                except NotImplementedError as exc:
                    logger.info(f"{var.name}: shared indexer declined ({exc}); using h5py")
                    fallback.append(var)

    if not fallback:
        return out
    if workers <= 1:
        for var in fallback:
            done(var, _h5py_index_task(var.url, source.region, var.name))
        return out
    with _make_pool(min(workers, len(fallback))) as pool:
        futures = {pool.submit(_h5py_index_task, v.url, source.region, v.name): v for v in fallback}
        for future in as_completed(futures):
            done(futures[future], future.result())
    return out


# =============================================================================
# Virtual arrays
# =============================================================================
def _array_attrs(var: VarInfo) -> dict[str, Any]:
    """Zarr attributes of a data variable: NREL's, minus what the codec now applies."""
    attrs = {k: v for k, v in var.attrs.items() if k not in ("scale_factor", "_FillValue")}
    attrs["nsrdb_scale_factor"] = var.scale_factor
    attrs["nsrdb_stored_dtype"] = np.dtype(var.dtype).str
    attrs["nsrdb_group"] = var.group
    return attrs


def _manifest_array(var: VarInfo, n_rows: int, index: Any | None) -> Any:
    """A ManifestArray of ``n_rows`` rows, referencing ``index`` (or nothing)."""
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from virtualizarr.manifests.utils import create_v3_array_metadata

    t_chunk, g_chunk = var.chunks
    if n_rows % t_chunk:
        raise ValueError(f"{var.name}: {n_rows} rows is not whole {t_chunk}-row chunks")
    grid = (n_rows // t_chunk, math.ceil(var.shape[1] / g_chunk))
    string_dtype = np.dtypes.StringDType()
    if index is None:
        paths = np.full(grid, "", dtype=string_dtype)
        offsets = np.zeros(grid, dtype=np.uint64)
        lengths = np.zeros(grid, dtype=np.uint64)
    else:
        lengths = np.asarray(index.lengths, dtype=np.uint64)
        offsets = np.asarray(index.offsets, dtype=np.uint64)
        if lengths.shape != grid:
            raise UnsupportedLayout(f"{var.name}: chunk grid {lengths.shape} is not {grid}")
        paths = np.full(grid, var.url, dtype=string_dtype)
        paths[lengths == 0] = ""
    manifest = ChunkManifest.from_arrays(
        paths=paths, offsets=offsets, lengths=lengths, validate_paths=False
    )
    metadata = create_v3_array_metadata(
        shape=(n_rows, var.shape[1]),
        data_type=np.dtype(np.float32),
        chunk_shape=var.chunks,
        fill_value=float("nan"),
        # The HDF5 chunks are unfiltered little-endian integers; the scale codec
        # turns them into float32 physical values on read.
        codecs=[
            _scale_codec(var.dtype, var.scale_factor),
            {"name": "bytes", "configuration": {"endian": "little"}},
        ],
        dimension_names=("time", "gid"),
    )
    return ManifestArray(metadata=metadata, chunkmanifest=manifest)


def _virtual_variable(var: VarInfo, n_rows: int, index: Any | None) -> xr.Variable:
    return xr.Variable(
        ("time", "gid"), _manifest_array(var, n_rows, index), attrs=_array_attrs(var)
    )


def _year_dataset(plan: YearPlan, names: Sequence[str], indexes: Mapping[str, Any]) -> xr.Dataset:
    """The virtual dataset for one batch of a year's variables."""
    return xr.Dataset(
        {name: _virtual_variable(plan.variables[name], plan.n_pad, indexes[name]) for name in names}
    )


def _write_time(store: Any, path: str, plan: YearPlan, old_len: int) -> None:
    """Append the year's ``time`` and ``valid`` rows, creating them on a group's first year.

    Written with Zarr rather than through VirtualiZarr, which hands non-virtual
    variables to xarray's ``to_zarr``. An append there decodes the stored
    ``time`` first, and some numpy/xarray combinations (numpy 2.5 with xarray
    2025.12) cannot decode the NaT pad rows at all, so every append after the
    first year would fail. The encoding is exactly what xarray writes: int64
    seconds since 1970 with NaT as the int64 minimum.
    """
    import zarr

    group = zarr.open_group(store, path=path, mode="a", zarr_format=3)
    times, valid = plan.padded_times()
    raw = times.astype("datetime64[s]").astype(np.int64)
    new_len = old_len + plan.n_pad
    if old_len == 0:
        time_arr = group.create_array(
            "time",
            shape=(new_len,),
            chunks=(plan.t_chunk,),
            dtype=np.int64,
            fill_value=_NAT_INT64,
            dimension_names=("time",),
            attributes={
                "standard_name": "time",
                "long_name": "time (UTC); NaT on pad rows",
                "units": TIME_UNITS,
                "calendar": "proleptic_gregorian",
            },
        )
        valid_arr = group.create_array(
            "valid",
            shape=(new_len,),
            chunks=(plan.t_chunk,),
            dtype=bool,
            fill_value=False,
            dimension_names=("time",),
            attributes={"long_name": "row holds a real time step; False on padding"},
        )
    else:
        time_arr, valid_arr = group["time"], group["valid"]
        time_arr.resize((new_len,))
        valid_arr.resize((new_len,))
    time_arr[old_len:new_len] = raw
    valid_arr[old_len:new_len] = valid


# =============================================================================
# Store state
# =============================================================================
@dataclass
class _GroupState:
    path: str
    length: int
    t_chunk: int
    signatures: dict[str, tuple[Any, ...]]
    year_rows: dict[int, tuple[int, int]]
    times: np.ndarray


@dataclass
class _StoreState:
    groups: dict[str, _GroupState] = field(default_factory=dict)
    attrs: dict[str, Any] = field(default_factory=dict)
    n_gid: int | None = None
    meta_vars: set[str] = field(default_factory=set)

    @property
    def year_group(self) -> dict[int, str]:
        return {y: g.path for g in self.groups.values() for y in g.year_rows}

    @property
    def newest_year(self) -> int | None:
        years = self.year_group
        return max(years) if years else None

    def year_groups(self, year: int) -> set[str]:
        recorded = self.attrs.get(ATTR_YEAR_GROUPS, {}).get(str(year))
        return set(GROUPS) if recorded is None else set(recorded)


def _decode_time(raw: np.ndarray) -> np.ndarray:
    times = raw.astype("datetime64[s]")
    times[raw == _NAT_INT64] = np.datetime64("NaT")
    return times


def _time_arrays(group: "zarr.Group") -> dict[str, "zarr.Array"]:
    """Every array in ``group`` whose first dimension is ``time``."""
    out = {}
    for name, arr in group.arrays():
        dims = arr.metadata.dimension_names or ()
        if dims and dims[0] == "time":
            out[name] = arr
    return out


def _group_state(group: "zarr.Group", path: str) -> _GroupState:
    time_arr = group["time"]
    if time_arr.attrs.get("units") != TIME_UNITS:
        raise NSRDBError(f"{path or '/'}: time units {time_arr.attrs.get('units')!r}")
    times = _decode_time(np.asarray(time_arr[:]))
    valid = np.asarray(group["valid"][:], dtype=bool)
    years = times.astype("datetime64[Y]").astype(np.int64) + 1970
    year_rows: dict[int, tuple[int, int]] = {}
    rows = np.flatnonzero(valid)
    for y in np.unique(years[rows]):
        in_year = rows[years[rows] == y]
        year_rows[int(y)] = (int(in_year[0]), int(in_year[-1]) + 1)
    signatures = {}
    for name, arr in _time_arrays(group).items():
        if arr.ndim != 2:
            continue
        signatures[name] = _signature(
            arr.chunks[1],
            arr.attrs.get("nsrdb_stored_dtype", arr.dtype),
            arr.attrs.get("nsrdb_scale_factor", 1.0),
        )
    return _GroupState(
        path, int(time_arr.shape[0]), int(time_arr.chunks[0]), signatures, year_rows, times
    )


def _read_state(repo: "icechunk.Repository", branch: str = "main") -> _StoreState:
    """What the store already holds. An empty or new store gives an empty state."""
    import zarr

    try:
        root = zarr.open_group(repo.readonly_session(branch).store, mode="r", zarr_format=3)
    except (FileNotFoundError, zarr.errors.GroupNotFoundError):
        return _StoreState()
    state = _StoreState(attrs=dict(root.attrs))
    names = set(root.array_keys())
    if "gid" in names:
        state.n_gid = int(root["gid"].shape[0])
        state.meta_vars = {
            n for n in names if (root[n].metadata.dimension_names or ()) == ("gid",)
        } - {"gid"}
    if "time" in names:
        state.groups[""] = _group_state(root, "")
    for name in sorted(root.group_keys()):
        if name.startswith(LAYOUT_PREFIX):
            state.groups[name] = _group_state(root[name], name)
    return state


def _compatible(group: _GroupState, plan: YearPlan, names: Sequence[str]) -> bool:
    if group.t_chunk != plan.t_chunk:
        return False
    return all(
        group.signatures.get(n, plan.variables[n].signature()) == plan.variables[n].signature()
        for n in names
    )


def _choose_group(state: _StoreState, plan: YearPlan, names: Sequence[str]) -> str:
    """The group a new year goes into: root if it fits, else a layout group."""
    if "" not in state.groups:
        return ""
    for path, group in state.groups.items():
        if _compatible(group, plan, names):
            return path
    g_chunk = min(plan.variables[n].chunks[1] for n in names)
    base = f"{LAYOUT_PREFIX}{plan.t_chunk}x{g_chunk}"
    name, n = base, 1
    while name in state.groups:
        n += 1
        name = f"{base}_{n}"
    logger.info(f"{plan.year}: chunk layout differs from the root's; writing to group {name}")
    return name


# =============================================================================
# Meta
# =============================================================================
def _meta_var(field_name: str) -> str:
    return META_RENAME.get(field_name, field_name)


def _meta_column(rows: np.ndarray, field_name: str) -> np.ndarray:
    col = rows[field_name]
    if field_name in FLOAT_META_FIELDS:
        return col.astype(np.float32)
    if col.dtype.kind == "S":
        return np.char.decode(col, "utf-8", errors="replace").astype(object)
    if col.dtype.kind == "O":
        return np.array(
            [v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v) for v in col],
            dtype=object,
        )
    return col


def meta_fingerprint(meta: Any) -> str:
    """Hash of latitude/longitude at both ends of ``meta`` and a strided sample.

    Cheap enough to take of every year over S3, and enough to catch a site
    table that was reordered or regridded.
    """
    n = int(meta.shape[0])
    digest = hashlib.sha256(str(n).encode())

    def add(rows: np.ndarray) -> None:
        for name in ("latitude", "longitude"):
            digest.update(np.ascontiguousarray(rows[name], dtype="<f4").tobytes())

    add(meta[: min(n, FINGERPRINT_EDGE_ROWS)])
    add(meta[max(0, n - FINGERPRINT_EDGE_ROWS) :])
    for i in np.unique(np.linspace(0, n - 1, FINGERPRINT_SAMPLES).astype(np.int64)):
        add(meta[int(i) : int(i) + 1])
    return digest.hexdigest()[:32]


def _write_meta(
    store: Any,
    url: str,
    region: str,
    n_gid: int,
    fields: Sequence[str],
    *,
    create_gid: bool,
) -> None:
    """Materialise ``meta`` fields as root ``(gid,)`` arrays, a slice at a time."""
    import zarr

    root = zarr.open_group(store, mode="a", zarr_format=3)
    chunk = min(META_SLICE_ROWS, n_gid)
    if create_gid:
        gid_dtype = np.int32 if n_gid < np.iinfo(np.int32).max else np.int64
        gid = root.create_array(
            "gid",
            shape=(n_gid,),
            chunks=(chunk,),
            dtype=gid_dtype,
            dimension_names=("gid",),
            attributes={"long_name": "row of the NSRDB site table"},
        )
        for start in range(0, n_gid, chunk):
            stop = min(n_gid, start + chunk)
            gid[start:stop] = np.arange(start, stop, dtype=gid_dtype)
    if not fields:
        return
    arrays: dict[str, Any] = {}
    with _open_h5(url, region, block_size=8 << 20, cache_type="readahead") as f:
        meta = f["meta"]
        for start in range(0, n_gid, chunk):
            rows = meta[start : min(n_gid, start + chunk)]
            for name in fields:
                col = _meta_column(rows, name)
                if name not in arrays:
                    is_str = col.dtype == object
                    arrays[name] = root.create_array(
                        _meta_var(name),
                        shape=(n_gid,),
                        chunks=(chunk,),
                        dtype=str if is_str else col.dtype,
                        fill_value="" if is_str else (np.nan if col.dtype.kind == "f" else 0),
                        dimension_names=("gid",),
                        attributes={"nsrdb_meta_field": name, **META_ATTRS.get(name, {})},
                    )
                arrays[name][start : start + len(col)] = col
            logger.debug(f"meta: wrote rows {start:,}-{start + len(rows):,} of {n_gid:,}")


def _check_meta(plan: YearPlan, state: _StoreState, source: Source) -> tuple[str, list[str]]:
    """Fingerprint the year's sites against the store; return it and any new fields.

    Raises:
        MetaMismatch: when the site count or fingerprint differs from the store's.
    """
    with _open_h5(plan.meta_url, source.region, block_size=1 << 18) as f:
        fingerprint = meta_fingerprint(f["meta"])
    if state.n_gid is not None and state.n_gid != plan.n_gid:
        raise MetaMismatch(f"{plan.year}: {plan.n_gid:,} sites, the store has {state.n_gid:,}")
    stored = state.attrs.get(ATTR_FINGERPRINT)
    if stored is not None and stored != fingerprint:
        raise MetaMismatch(
            f"{plan.year}: site table fingerprint {fingerprint} differs from the store's "
            f"{stored}; the gid order or coordinates changed"
        )
    known = set(state.attrs.get(ATTR_META_FIELDS, {}))
    new_fields = [name for name, _ in plan.meta_fields if name not in known]
    return fingerprint, new_fields


# =============================================================================
# Writing
# =============================================================================
def _batches(
    plan: YearPlan, names: Sequence[str], indexes: Mapping[str, Any], limit: int
) -> list[list[str]]:
    """Split variables into commit batches of at most ``limit`` refs, whole groups at a time."""
    batches: list[list[str]] = []
    current: list[str] = []
    refs = 0
    for group in GROUPS:
        members = [n for n in names if plan.variables[n].group == group]
        if not members:
            continue
        group_refs = sum(int(np.count_nonzero(indexes[n].lengths)) for n in members)
        if current and refs + group_refs > limit:
            batches.append(current)
            current, refs = [], 0
        current.extend(members)
        refs += group_refs
    if current:
        batches.append(current)
    return batches


def _commit(
    repo: "icechunk.Repository",
    steps: Sequence[Callable[[Any], None]],
    finish: Callable[[Any], None],
    message: str,
    *,
    scratch: str,
    branch: str = "main",
) -> str:
    """Run ``steps`` and ``finish`` and land them on ``branch`` as one change.

    One step is one commit straight to ``branch``. Several steps are committed
    one by one to a scratch branch and ``branch`` is then reset to its tip, so
    a reader of ``branch`` still sees the year appear all at once while no
    change set ever holds more than one step's references. The reset is
    conditional on ``branch`` not having moved meanwhile.
    """
    if len(steps) == 1:
        session = repo.writable_session(branch)
        steps[0](session.store)
        finish(session.store)
        return session.commit(message)

    base = repo.lookup_branch(branch)
    if scratch in repo.list_branches():
        logger.info(f"deleting stale scratch branch {scratch}")
        repo.delete_branch(scratch)
    repo.create_branch(scratch, base)
    snapshot = base
    for i, step in enumerate(steps, start=1):
        session = repo.writable_session(scratch)
        step(session.store)
        if i == len(steps):
            finish(session.store)
        snapshot = session.commit(f"{message} [{i}/{len(steps)}]")
        logger.info(f"{scratch}: committed part {i}/{len(steps)}")
    repo.reset_branch(branch, snapshot, from_snapshot_id=base)
    repo.delete_branch(scratch)
    return snapshot


def _assert_lengths(store: Any, path: str, expected: int) -> None:
    """Every time-dim array in the group has ``expected`` rows.

    Raises:
        NSRDBError: when one does not, before anything is committed.
    """
    import zarr

    group = zarr.open_group(store, path=path, mode="r", zarr_format=3)
    wrong = {n: a.shape[0] for n, a in _time_arrays(group).items() if a.shape[0] != expected}
    if wrong:
        raise NSRDBError(f"{path or '/'}: time-dim arrays not {expected} rows long: {wrong}")


def _resize_missing(store: Any, path: str, old: int, new: int) -> None:
    """Grow every time-dim array still at ``old`` rows to ``new``; they read as fill."""
    import zarr

    group = zarr.open_group(store, path=path, mode="a", zarr_format=3)
    for name, arr in _time_arrays(group).items():
        if arr.shape[0] == old and old != new:
            arr.resize((new, *arr.shape[1:]))
            logger.debug(f"{path or '/'}/{name}: absent this year, grown to {new} rows")


def _create_absent(
    store: Any, path: str, plan: YearPlan, names: Sequence[str], existing: set[str], rows: int
) -> None:
    """Create variables new to the group at its current length, with no references.

    Created through VirtualiZarr from an empty manifest, so the codecs and
    metadata match exactly what an append will later compare against.
    """
    new = [n for n in names if n not in existing]
    if not new or rows == 0:
        return
    ds = xr.Dataset({n: _virtual_variable(plan.variables[n], rows, None) for n in new})
    ds.vz.to_icechunk(store, group=path or None, mode="a", validate_containers=False)
    logger.info(f"{path or '/'}: new variable(s) {new}; earlier years read as fill")


def _merge_root_attrs(store: Any, updates: Mapping[str, Any]) -> None:
    """Update root attributes, merging dict values instead of replacing them."""
    import zarr

    root = zarr.open_group(store, mode="a", zarr_format=3)
    current = dict(root.attrs)
    merged: dict[str, Any] = {}
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(current.get(key), dict):
            merged[key] = {**current[key], **value}
        else:
            merged[key] = value
    root.update_attributes(merged)


def _check_containers(plan: YearPlan, source: Source) -> None:
    bad = [f.url for f in plan.files.values() if not f.url.startswith(source.url_prefix)]
    if bad:
        raise NSRDBError(f"files outside the virtual chunk container {source.url_prefix}: {bad}")


def _provenance(
    plan: YearPlan, dataset: NSRDBDataset, source: Source, state: _StoreState, path: str
) -> dict[str, Any]:
    year = str(plan.year)
    stored = plan.year in state.year_group
    earlier_groups = sorted(state.year_groups(plan.year)) if stored else []
    groups = set(earlier_groups) | set(plan.groups)
    earlier_version = state.attrs.get(ATTR_VERSIONS, {}).get(year) if stored else None
    return {
        "nsrdb_dataset": dataset.key,
        "nsrdb_source": source.url_prefix + dataset.directory,
        ATTR_VERSIONS: {year: plan.versions(earlier_version, earlier_groups)},
        ATTR_FILES: {
            year: {
                **state.attrs.get(ATTR_FILES, {}).get(year, {}),
                **{g: f.url for g, f in plan.files.items()},
            }
        },
        ATTR_YEAR_GROUPS: {year: [g for g in GROUPS if g in groups]},
        ATTR_YEAR_LAYOUT: {year: path},
    }


def _write_new_year(
    repo: "icechunk.Repository",
    plan: YearPlan,
    state: _StoreState,
    indexes: Mapping[str, Any],
    *,
    dataset: NSRDBDataset,
    source: Source,
    max_refs: int,
    branch: str,
) -> str:
    """Append one year to the store, creating it on the first year. Returns the group."""
    names = list(plan.variables)
    fingerprint, new_fields = _check_meta(plan, state, source)
    path = _choose_group(state, plan, names)
    group = state.groups.get(path)
    old_len = group.length if group else 0
    new_len = old_len + plan.n_pad
    existing = set(group.signatures) if group else set()
    first_in_store = state.n_gid is None
    batches = _batches(plan, names, indexes, max_refs)
    checksum = plan.last_modified()

    def step(batch: list[str], i: int) -> Callable[[Any], None]:
        def run(store: Any) -> None:
            if i == 0 and new_fields:
                _write_meta(
                    store,
                    plan.meta_url,
                    source.region,
                    plan.n_gid,
                    new_fields,
                    create_gid=first_in_store,
                )
            if group is not None:
                _create_absent(store, path, plan, batch, existing, old_len)
            if i == 0:
                _write_time(store, path, plan, old_len)
            ds = _year_dataset(plan, batch, indexes)
            ds.vz.to_icechunk(
                store,
                group=path or None,
                append_dim="time" if group is not None else None,
                mode=None if group is not None else "a",
                validate_containers=False,
                last_updated_at=checksum,
            )

        return run

    def finish(store: Any) -> None:
        _resize_missing(store, path, old_len, new_len)
        _assert_lengths(store, path, new_len)
        fields_seen = {name: plan.year for name in new_fields}
        attrs = _provenance(plan, dataset, source, state, path)
        attrs[ATTR_FINGERPRINT] = fingerprint
        attrs[ATTR_META_FIELDS] = fields_seen
        known = set(state.meta_vars) | {_meta_var(n) for n in new_fields}
        attrs["coordinates"] = " ".join(sorted(known))
        _merge_root_attrs(store, attrs)

    _check_containers(plan, source)
    steps = [step(batch, i) for i, batch in enumerate(batches)]
    _commit(
        repo,
        steps,
        finish,
        f"NSRDB {dataset.key} {plan.year}: {len(names)} variables from {', '.join(plan.groups)}"
        + (f" into {path}" if path else ""),
        scratch=SCRATCH_BRANCH.format(year=plan.year),
        branch=branch,
    )
    return path


def _backfill_year(
    repo: "icechunk.Repository",
    plan: YearPlan,
    state: _StoreState,
    indexes: Mapping[str, Any],
    *,
    dataset: NSRDBDataset,
    source: Source,
    max_refs: int,
    branch: str,
) -> str:
    """Add groups to a year that is already stored, in place.

    A store built from a subset of groups would otherwise never gain the rest
    for its earlier years. The year's rows are already fixed, so the new
    variables are written as a region of the existing arrays.

    Raises:
        LayoutConflict: when the new variables cannot share the year's group.
    """
    names = list(plan.variables)
    path = state.year_group[plan.year]
    group = state.groups[path]
    if not _compatible(group, plan, names):
        raise LayoutConflict(
            f"{plan.year}: {plan.groups} cannot join group {path or '/'}, whose chunking or "
            "scaling differs; ingest them into a fresh store"
        )
    _check_meta(plan, state, source)
    row0, row1 = group.year_rows[plan.year]
    if row0 % plan.t_chunk or row0 + plan.n_pad > group.length:
        raise LayoutConflict(f"{plan.year}: stored rows start at {row0}, not a chunk boundary")
    stored = group.times[row0:row1]
    if not np.array_equal(stored, plan.times.astype("datetime64[s]")):
        raise LayoutConflict(
            f"{plan.year}: the time_index of {plan.groups} ({plan.n_time} steps) differs from "
            f"the stored year's ({row1 - row0} steps); they cannot share rows"
        )
    region = {"time": slice(row0, row0 + plan.n_pad)}
    checksum = plan.last_modified()
    existing = set(group.signatures)

    def step(batch: list[str]) -> Callable[[Any], None]:
        def run(store: Any) -> None:
            _create_absent(store, path, plan, batch, existing, group.length)
            ds = _year_dataset(plan, batch, indexes)
            ds.vz.to_icechunk(
                store,
                group=path or None,
                region=region,
                validate_containers=False,
                last_updated_at=checksum,
            )

        return run

    def finish(store: Any) -> None:
        _assert_lengths(store, path, group.length)
        _merge_root_attrs(store, _provenance(plan, dataset, source, state, path))

    _check_containers(plan, source)
    steps = [step(batch) for batch in _batches(plan, names, indexes, max_refs)]
    _commit(
        repo,
        steps,
        finish,
        f"NSRDB {dataset.key} {plan.year}: backfill {', '.join(plan.groups)}",
        scratch=SCRATCH_BRANCH.format(year=plan.year),
        branch=branch,
    )
    return path


# =============================================================================
# Public API
# =============================================================================
def open_repo(
    key: str,
    *,
    source: Source | None = None,
    config: Config | None = None,
    create: bool = True,
) -> "icechunk.Repository":
    """Open (or create) the virtual store of one dataset.

    Manifests split once per padded year along ``time`` and along ``gid`` so
    that each holds at most :data:`MAX_REFS_PER_MANIFEST` references.

    Args:
        key: Catalog key.
        source: Source bucket. A non-default one (tests) gets its own virtual
            chunk container in place of NREL's.
        config: Override configuration; defaults to the process-wide one.
        create: Create the store if it does not exist.

    Returns:
        The repository, authorised for anonymous reads of the source.
    """
    import icechunk

    dataset = get_dataset(key)
    source = source or Source()
    per_year = dataset.time_chunks_per_year
    repo = virtual_repo.open_virtual_repo(
        store_prefix_for(key),
        virtual_buckets=BUCKET,
        split_dim="time",
        split_size=per_year,
        extra_splits={"gid": max(1, MAX_REFS_PER_MANIFEST // per_year)},
        source_region=SOURCE_REGION,
        config=config,
        create=create,
    )
    if source.url_prefix == Source().url_prefix:
        return repo
    repo_config = repo.config
    repo_config.set_virtual_chunk_container(source.container())
    repo = repo.reopen(
        config=repo_config,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {
                Source().url_prefix: icechunk.s3_anonymous_credentials(),
                source.url_prefix: source.credentials(),
            }
        ),
    )
    if create:
        repo.save_config()
    return repo


@dataclass
class IngestReport:
    """What one :func:`ingest` call did."""

    dataset: str
    store: str
    written: list[int] = field(default_factory=list)
    backfilled: list[int] = field(default_factory=list)
    already_present: list[int] = field(default_factory=list)
    incomplete: list[int] = field(default_factory=list)
    held_back: list[int] = field(default_factory=list)
    stranded: list[int] = field(default_factory=list)
    layouts: dict[int, str] = field(default_factory=dict)
    peak_memory_gb: float = 0.0
    seconds: float = 0.0


def _normalise_groups(groups: Sequence[str] | None) -> list[str]:
    if not groups:
        return list(GROUPS)
    unknown = sorted(set(groups) - set(GROUPS))
    if unknown:
        raise ValueError(f"unknown group(s) {unknown}; known: {', '.join(GROUPS)}")
    return [g for g in GROUPS if g in groups]


def _load_plan(
    year: int, files: Mapping[str, SourceFile], source: Source, workers: int
) -> YearPlan:
    """Read the layout of a year's group files, in parallel when ``workers`` allows."""
    started = time.monotonic()
    if workers <= 1:
        infos = {g: _read_file_info(f.url, source.region, g) for g, f in files.items()}
    else:
        with _make_pool(min(workers, len(files))) as pool:
            futures = {
                g: pool.submit(_read_file_info, f.url, source.region, g) for g, f in files.items()
            }
            infos = {g: fut.result() for g, fut in futures.items()}
    plan = _build_plan(year, dict(files), infos)
    logger.info(
        f"{year}: {len(plan.variables)} variables, {plan.n_time} steps padded to {plan.n_pad}, "
        f"{plan.n_gid:,} sites, time chunk {plan.t_chunk} ({time.monotonic() - started:.0f}s)"
    )
    return plan


def ingest(
    key: str,
    *,
    years: Sequence[int] | None = None,
    groups: Sequence[str] | None = None,
    workers: int = DEFAULT_WORKERS,
    source: Source | None = None,
    config: Config | None = None,
    indexer: ChunkIndexer | None = None,
    cache_dir: pathlib.Path | str | None = None,
    max_refs_per_commit: int = DEFAULT_MAX_REFS_PER_COMMIT,
    branch: str = "main",
) -> IngestReport:
    """Ingest a dataset's missing years into its virtual store, oldest first.

    A year already in the store is skipped, unless it lacks some of the
    requested groups, which are then added to it in place.

    Args:
        key: Catalog key.
        years: Years to ingest. None means every year found in the bucket.
        groups: Variable groups to ingest; default all seven. A store built
            from a subset takes the rest later as new variables.
        workers: Processes for reading file metadata and indexing chunks.
        source: Source bucket; defaults to NREL's.
        config: Override configuration; defaults to the process-wide one.
        indexer: ``(url, dataset) -> chunk index``, replacing the default.
        cache_dir: Where chunk indexes are saved for reuse. None disables it.
        max_refs_per_commit: Above this many references a year is committed
            in pieces through a scratch branch.
        branch: Branch to write.

    Returns:
        An :class:`IngestReport`.

    Raises:
        IncompleteYear: when an explicitly requested year lacks a group file, or,
            with ``years=None``, after ingesting up to the first incomplete year:
            later years are held back so that it can still be filled in.
        OutOfOrderPartition: when a missing year is older than the newest
            stored one. With ``years=None`` every other year is ingested first.
        MetaMismatch: when a year's site table differs from the store's.
    """
    dataset = get_dataset(key)
    source = source or Source()
    groups = _normalise_groups(groups)
    cfg = config if config is not None else get_config()
    cache = pathlib.Path(cache_dir) / dataset.key if cache_dir is not None else None
    started = time.monotonic()
    report = IngestReport(dataset=key, store=cfg.store_path(store_prefix_for(key)))

    listing = discover_files(key, source=source)
    explicit = years is not None
    wanted = sorted(set(years)) if years is not None else sorted(listing)
    logger.info(
        f"NSRDB {key}: {len(listing)} year(s) in the bucket, considering {wanted or 'none'}, "
        f"groups {groups}, store {report.store}"
    )
    repo = open_repo(key, source=source, config=cfg)

    with memory_guard(what=f"nsrdb {key}") as usage:
        state = _read_state(repo, branch)
        gap: int | None = None
        for year in wanted:
            files = listing.get(year, {})
            missing = [g for g in groups if g not in files]
            if missing:
                message = f"{key} {year}: no {', '.join(missing)} file(s) in the bucket"
                if explicit:
                    raise IncompleteYear(message)
                logger.warning(f"{message}; skipping the year")
                report.incomplete.append(year)
                if year not in state.year_group and gap is None:
                    gap = year
                continue
            if gap is not None and year not in state.year_group:
                # Appending past the gap would leave it older than the newest
                # stored year, so it could never be written once its files appear.
                logger.warning(f"{key} {year}: held back behind incomplete year {gap}")
                report.held_back.append(year)
                continue

            if year in state.year_group:
                todo = [g for g in groups if g not in state.year_groups(year)]
                if not todo:
                    logger.info(f"{key} {year}: already stored, skipping")
                    report.already_present.append(year)
                    continue
                plan = _load_plan(year, {g: files[g] for g in todo}, source, workers)
                indexes = index_variables(
                    plan,
                    list(plan.variables),
                    source=source,
                    indexer=indexer,
                    workers=workers,
                    cache_dir=cache,
                )
                report.layouts[year] = _backfill_year(
                    repo,
                    plan,
                    state,
                    indexes,
                    dataset=dataset,
                    source=source,
                    max_refs=max_refs_per_commit,
                    branch=branch,
                )
                report.backfilled.append(year)
            else:
                newest = state.newest_year
                if newest is not None and year < newest:
                    message = (
                        f"{key} {year}: older than the newest stored year {newest}. The store "
                        "only appends along time, so this year needs a fresh store"
                    )
                    if explicit:
                        raise OutOfOrderPartition(message)
                    logger.error(message)
                    report.stranded.append(year)
                    continue
                plan = _load_plan(year, {g: files[g] for g in groups}, source, workers)
                indexes = index_variables(
                    plan,
                    list(plan.variables),
                    source=source,
                    indexer=indexer,
                    workers=workers,
                    cache_dir=cache,
                )
                report.layouts[year] = _write_new_year(
                    repo,
                    plan,
                    state,
                    indexes,
                    dataset=dataset,
                    source=source,
                    max_refs=max_refs_per_commit,
                    branch=branch,
                )
                report.written.append(year)
            logger.info(f"{key} {year}: committed to {report.layouts[year] or '/'}")
            log_memory(f"nsrdb {key} after {year}")
            state = _read_state(repo, branch)

    report.peak_memory_gb = usage.peak_gb
    report.seconds = time.monotonic() - started
    logger.info(
        f"NSRDB {key}: wrote {report.written}, backfilled {report.backfilled}, "
        f"{len(report.already_present)} already present, peak {usage.peak_gb:.2f} GB, "
        f"{report.seconds:.0f}s"
    )
    if report.stranded:
        raise OutOfOrderPartition(
            f"{key}: year(s) {report.stranded} are older than the newest stored year and can "
            "never be appended; everything else was ingested"
        )
    if report.held_back:
        raise IncompleteYear(
            f"{key}: year(s) {report.held_back} held back behind incomplete year(s) "
            f"{report.incomplete}. Rerun once the missing files appear, or name the later "
            "years explicitly to ingest past the gap for good"
        )
    return report


def _layout_groups(store: Any) -> list[str]:
    import zarr

    root = zarr.open_group(store, mode="r", zarr_format=3)
    return sorted(n for n in root.group_keys() if n.startswith(LAYOUT_PREFIX))


def _decode_variable(da: xr.DataArray) -> xr.DataArray:
    """NSRDB decoding: stored / scale_factor as float32, fill as NaN.

    Arrays written with the scale codec are float32 already. Integer arrays come
    from a store still in the earlier encoding (see :func:`migrate_float32`).
    """
    attrs = {k: v for k, v in da.attrs.items() if k not in ("scale_factor", "_FillValue")}
    if da.dtype == np.float32:
        out = da.copy(deep=False)
        out.attrs = attrs
        return out
    fill = da.attrs.get("_FillValue")
    out = da.astype(np.float32)
    if fill is not None:
        out = out.where(da != fill)
    out = out / np.float32(da.attrs.get("nsrdb_scale_factor", 1.0))
    out.attrs = attrs
    return out


def _open_group(store: Any, path: str, chunks: Mapping[str, int] | None) -> xr.Dataset:
    import zarr

    group = zarr.open_group(store, path=path, mode="r", zarr_format=3)
    if "time" not in group:
        raise NSRDBError(f"{path or '/'}: no time axis; the store holds no years yet")
    if chunks is None:
        data = [a for a in _time_arrays(group).values() if a.ndim == 2]
        t_chunk = int(group["time"].chunks[0])
        g_chunk = max((int(a.chunks[1]) for a in data), default=META_SLICE_ROWS)
        chunks = {"time": t_chunk, "gid": g_chunk * 16}
    # Times are decoded here rather than by xarray, which in some numpy/xarray
    # combinations cannot decode the NaT pad rows; see _write_time.
    ds = xr.open_zarr(
        store,
        group=path or None,
        consolidated=False,
        zarr_format=3,
        chunks=dict(chunks),
        mask_and_scale=False,
        decode_times=False,
    )
    units = ds["time"].attrs.get("units")
    if units != TIME_UNITS:
        raise NSRDBError(f"{path or '/'}: time units {units!r}, expected {TIME_UNITS!r}")
    times = _decode_time(np.asarray(ds["time"].values)).astype("datetime64[ns]")
    return ds.assign_coords(time=("time", times, {"standard_name": "time"}))


def open_nsrdb(
    key: str,
    *,
    branch: str = "main",
    config: Config | None = None,
    source: Source | None = None,
    chunks: Mapping[str, int] | None = None,
) -> xr.Dataset:
    """Open one dataset's store as a single lazy dataset on a clean time axis.

    The root and any layout groups are stitched together, pad rows dropped and
    the result ordered by time. Variables are decoded the NSRDB way, ``stored
    / scale_factor`` as float32 with absent chunks as NaN, exactly as NREL's
    ``rex`` does; the site table is attached as ``gid`` coordinates.

    Args:
        key: Catalog key.
        branch: Branch to read.
        config: Override configuration; defaults to the process-wide one.
        source: Source bucket; defaults to NREL's.
        chunks: Dask chunks; default one time chunk by sixteen gid chunks.

    Returns:
        A dask-backed dataset with monotonic, unique ``time``.

    Raises:
        NSRDBError: when the store is empty or its times overlap.
    """
    import dask.array as dsa

    repo = open_repo(key, source=source, config=config, create=False)
    store = repo.readonly_session(branch).store
    root = _open_group(store, "", chunks)
    if "time" not in root.dims:
        raise NSRDBError(f"{key}: the store holds no years yet")
    meta = {n: root[n].variable for n in root.variables if root[n].dims == ("gid",)}
    root = root.drop_vars(list(meta))

    pieces: list[tuple[np.datetime64, xr.Dataset]] = []
    for ds in [root, *(_open_group(store, p, chunks) for p in _layout_groups(store))]:
        valid = ds["valid"].values.astype(bool)
        edges = np.flatnonzero(np.diff(np.concatenate([[0], valid.astype(np.int8), [0]])))
        decoded = ds.drop_vars("valid")
        for name in list(decoded.data_vars):
            if decoded[name].dims == ("time", "gid"):
                decoded[name] = _decode_variable(decoded[name])
        for start, stop in zip(edges[::2], edges[1::2]):
            piece = decoded.isel(time=slice(int(start), int(stop)))
            pieces.append((piece["time"].values[0], piece))
    pieces.sort(key=lambda p: p[0])

    names = sorted({n for _, p in pieces for n in p.data_vars})
    aligned = []
    for _, piece in pieces:
        for name in names:
            if name not in piece:
                template = next(p[name] for _, p in pieces if name in p)
                piece[name] = (
                    ("time", "gid"),
                    dsa.full(
                        (piece.sizes["time"], template.shape[1]),
                        np.nan,
                        dtype=np.float32,
                        chunks=(piece.sizes["time"], template.chunks[1]),
                    ),
                    template.attrs,
                )
        aligned.append(piece)
    ds = xr.concat(
        aligned, dim="time", data_vars="all", coords="minimal", compat="override", join="override"
    )
    index = ds.indexes["time"]
    if not (index.is_monotonic_increasing and index.is_unique):
        raise NSRDBError(f"{key}: stored years overlap in time")
    ds = ds.assign_coords(meta)
    ds.attrs = {k: v for k, v in root.attrs.items() if k != "coordinates"}
    return ds


def describe(
    key: str,
    *,
    branch: str = "main",
    config: Config | None = None,
    source: Source | None = None,
) -> dict[str, Any]:
    """Summarise a store: years, groups, variables and time span.

    Raises:
        FileNotFoundError: when the store does not exist locally.
        NSRDBError: when it exists but holds no years.
    """
    repo = open_repo(key, source=source, config=config, create=False)
    state = _read_state(repo, branch)
    if not state.groups:
        raise NSRDBError(f"{key}: the store holds no years yet")
    years = state.year_group
    variables = sorted({n for g in state.groups.values() for n in g.signatures})
    return {
        "years": sorted(years),
        "layouts": {y: p or "/" for y, p in sorted(years.items()) if p},
        "groups": {y: sorted(state.year_groups(y)) for y in sorted(years)},
        "variables": len(variables),
        "sites": state.n_gid,
        "valid_steps": sum(
            hi - lo for g in state.groups.values() for lo, hi in g.year_rows.values()
        ),
    }


def _float32_metadata(arr: "zarr.Array") -> dict[str, Any] | None:
    """``zarr.json`` for an integer-encoded data variable as a float32 scale-codec array.

    Returns None for an array already in the float32 encoding, or one that is
    not an NSRDB data variable.
    """
    if arr.ndim != 2 or arr.dtype.kind not in "iu" or "nsrdb_scale_factor" not in arr.attrs:
        return None
    meta = arr.metadata.to_dict()
    attrs = {k: v for k, v in arr.attrs.items() if k not in ("scale_factor", "_FillValue")}
    attrs["nsrdb_stored_dtype"] = arr.dtype.str
    meta.update(
        data_type="float32",
        fill_value="NaN",
        codecs=[
            _scale_codec(arr.dtype, float(arr.attrs["nsrdb_scale_factor"])),
            {"name": "bytes", "configuration": {"endian": "little"}},
        ],
        attributes=attrs,
    )
    return meta


def migrate_float32(
    key: str,
    *,
    branch: str = "main",
    config: Config | None = None,
    source: Source | None = None,
) -> int:
    """Rewrite a store's integer data variables as float32 scale-codec arrays.

    Stores written before the scale codec hold each variable as NREL's integer
    dtype with CF ``scale_factor``/``_FillValue`` attributes, which xarray
    decodes to float64. Only each array's ``zarr.json`` changes: the chunk
    references, shapes and chunking stay as they are, so this is one small
    commit, and the earlier encoding remains in the snapshot history.

    Args:
        key: Catalog key.
        branch: Branch to migrate.
        config: Override configuration; defaults to the process-wide one.
        source: Source bucket; defaults to NREL's.

    Returns:
        The number of arrays rewritten; 0 when the store is already migrated.
    """
    import json

    import zarr
    from zarr.core.buffer import default_buffer_prototype

    repo = open_repo(key, source=source, config=config, create=False)
    session = repo.writable_session(branch)
    store = session.store
    root = zarr.open_group(store, mode="r", zarr_format=3)
    rewritten = 0
    for path in ["", *_layout_groups(store)]:
        group = root if not path else root[path]
        for name, arr in _time_arrays(group).items():
            meta = _float32_metadata(arr)
            if meta is None:
                continue
            key_path = f"{path}/{name}/zarr.json" if path else f"{name}/zarr.json"
            payload = json.dumps(meta, allow_nan=False).encode()
            buf = default_buffer_prototype().buffer.from_bytes(payload)
            zarr.core.sync.sync(store.set(key_path, buf))
            rewritten += 1
    if rewritten:
        session.commit(f"{key}: data variables as float32 via the scale codec ({rewritten} arrays)")
    logger.info(f"{key}: migrated {rewritten} array(s) to float32")
    return rewritten


# =============================================================================
# CLI
# =============================================================================
def _cmd_list(args: argparse.Namespace) -> int:
    for key in args.dataset or list(DATASETS):
        listing = discover_files(key)
        complete = [y for y, files in listing.items() if len(files) == len(GROUPS)]
        partial = {
            y: sorted(set(GROUPS) - set(files))
            for y, files in listing.items()
            if len(files) != len(GROUPS)
        }
        logger.info(f"{key}: {DATASETS[key].description}")
        logger.info(f"  complete years: {complete or 'none'}")
        if partial:
            logger.info(f"  incomplete years (missing groups): {partial}")
    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    failed = 0
    for key in args.dataset or list(DATASETS):
        try:
            summary = describe(key)
            logger.info(f"{key}: OK — {summary}")
        except Exception as exc:  # noqa: BLE001 - report every store, not just the first bad one
            logger.error(f"{key}: FAILED — {type(exc).__name__}: {exc}")
            failed += 1
    return 1 if failed else 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    cache_dir = (
        None
        if args.no_cache
        else (args.cache_dir or get_config().scratch_dir / "nsrdb_chunk_index")
    )
    report = ingest(
        args.dataset,
        years=args.years,
        groups=args.groups,
        workers=args.workers,
        cache_dir=cache_dir,
        max_refs_per_commit=args.max_refs_per_commit,
    )
    logger.info(f"report: {report}")
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    for key in args.dataset or list(DATASETS):
        migrate_float32(key)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="python -m planetary_datasets.providers.virtualized.nsrdb"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="datasets and the years found in the bucket")
    p_list.add_argument("--dataset", nargs="*", choices=list(DATASETS))
    p_list.set_defaults(func=_cmd_list)

    p_check = sub.add_parser("check", help="describe each store")
    p_check.add_argument("--dataset", nargs="*", choices=list(DATASETS))
    p_check.set_defaults(func=_cmd_check)

    p_migrate = sub.add_parser(
        "migrate-float32", help="rewrite integer-encoded variables as float32 arrays"
    )
    p_migrate.add_argument("--dataset", nargs="*", choices=list(DATASETS))
    p_migrate.set_defaults(func=_cmd_migrate)

    p_ingest = sub.add_parser("ingest", help="ingest missing years into a dataset's store")
    p_ingest.add_argument("--dataset", required=True, choices=list(DATASETS))
    p_ingest.add_argument("--years", nargs="+", type=int)
    p_ingest.add_argument("--groups", nargs="+", choices=list(GROUPS))
    p_ingest.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p_ingest.add_argument("--cache-dir", type=pathlib.Path)
    p_ingest.add_argument("--no-cache", action="store_true")
    p_ingest.add_argument("--max-refs-per-commit", type=int, default=DEFAULT_MAX_REFS_PER_COMMIT)
    p_ingest.set_defaults(func=_cmd_ingest)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    # Run the imported module rather than ``__main__``, so the functions handed
    # to spawned workers pickle under their real module name.
    from planetary_datasets.providers.virtualized import nsrdb as _nsrdb

    sys.exit(_nsrdb.main())
