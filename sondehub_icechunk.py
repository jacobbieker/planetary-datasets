"""Pack the per-flight radiosonde NetCDFs into a single Icechunk store.

One NetCDF per flight is fine for archival but hopeless to query: 1.5M files, each a few
hundred KB, with no way to ask "every flight from this launch site last March". This packs
them into one store using the CF *contiguous ragged array* layout -- every flight's samples
concatenated end to end along a single `obs` dimension, with `obs_start`/`obs_count` on a
`flight` dimension saying where each one lives.

Flights vary from ~1k to ~450k samples, which is exactly the case regular chunking handles
badly: size the chunk for the big flights and small ones share a chunk with dozens of
others; size it for the small ones and a big flight is scattered over hundreds. So the obs
arrays use Zarr v3 **rectilinear (variable-width) chunks**, with every chunk boundary
landing on a flight boundary. Chunks come out near-uniform in bytes *and* no flight is ever
split, so reading one serial touches exactly one chunk -- never two, never a partial.

Selection by serial is a binary search over a sorted index array, so pulling one flight out
of millions costs two small reads plus one chunk.

Appends are incremental: `resize` on a rectilinear array adds the growth as a single new
chunk, so each batch of flights becomes one chunk and re-running only ingests serials the
store has not seen. That means this can run against `sondehub_pressure_flights/` while the
backfill is still filling it.

Examples
--------
    python sondehub_icechunk.py                          # build/update the store
    python sondehub_icechunk.py --sel W3411216           # read one flight back
    python sondehub_icechunk.py --info                   # store summary
"""

import argparse
import glob
import os
import sys
import time as _time

import boto3
import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from zarr.codecs import BloscCodec

# Rectilinear chunk grids are still behind a flag in zarr 3.4.
zarr.config.set({"array.rectilinear_chunks": True})

DEFAULT_STORE = "s3://us-west-2.opendata.source.coop/bkr/obs/sondehub_flights.icechunk"
LOCAL_STORE = "sondehub_flights.icechunk"
S3_REGION = "us-west-2"
DEFAULT_FLIGHTS = "sondehub_pressure_flights"

# Samples per chunk, before flight-boundary rounding. 131072 float32 is a 512 KB chunk
# holding ~15 median flights: small enough that pulling one serial is a single modest read,
# large enough to keep the chunk count (and so the manifest) manageable across the archive.
TARGET_CHUNK_POINTS = 131_072

# Flights per chunk on the metadata arrays. Deliberately small: appending rewrites
# whichever chunk the new flights land in, so a large value means every batch rewrites the
# entire column and leaves the previous copy as garbage. At 65536 that cost 11 GB of dead
# objects in the first 58k flights; at 2048 a rewrite touches a few hundred KB at most.
FLIGHT_CHUNK = 2_048

# Commits between full rebuilds of the sorted serial index; see FlightWriter.commit.
INDEX_EVERY = 7

# Per-sample variables. lat/lon keep float64 because 5-decimal degrees needs more mantissa
# than float32 has; frame_counter because counters run past 1e9. The rest are instrument
# readings where float32 is well past the sensor's precision.
OBS_VARS = {
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
    "battery": "f4",
    "snr": "f4",
    "rssi": "f4",
    "frame_counter": "f8",
    "receiver_count": "i2",
}

# Per-flight metadata, lifted from the NetCDF global attributes into `flight` arrays.
FLIGHT_STRS = (
    "serial",
    "manufacturer",
    "type",
    "subtype",
    "dfmcode",
    "software",
    "receivers",
    "launch_site",
    "launch_site_name",
    "ref_datetime",
    "ref_position",
    "indexed_day",
    "rs41_mainboard",
    "rs41_mainboard_fw",
)
FLIGHT_NUMS = {
    "frequency": "f4",
    "tx_frequency": "f4",
    "burst_timer": "f4",
    "launch_site_latitude": "f8",
    "launch_site_longitude": "f8",
    "launch_site_altitude": "f4",
    "launch_site_range_estimate": "f4",
    "max_altitude": "f4",
    "duration_seconds": "f4",
    "n_frames": "i4",
    "n_raw_uploads": "i4",
    "n_receivers": "i2",
}
FLIGHT_TIMES = ("start_time", "end_time", "burst_time")
# Offsets into the obs dimension; the CF ragged-array index.
FLIGHT_INDEX = {"obs_start": "i8", "obs_count": "i8"}

VAR_ATTRS = {
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
    "battery": {"units": "V", "long_name": "sonde battery voltage"},
    "snr": {"units": "dB", "long_name": "receiver signal-to-noise ratio"},
    "rssi": {"units": "dBm", "long_name": "receiver signal strength"},
    "frame_counter": {"long_name": "sonde frame counter"},
    "receiver_count": {"long_name": "distinct receivers that uploaded this sample"},
    "time": {"units": "nanoseconds since 1970-01-01", "calendar": "proleptic_gregorian"},
    "obs_start": {"long_name": "index of this flight's first sample in obs"},
    "obs_count": {"long_name": "number of samples", "sample_dimension": "obs"},
    "serial": {"cf_role": "trajectory_id", "long_name": "radiosonde serial number"},
}

# Blosc/zstd with byte shuffle: the float columns are smooth and the int64 timestamps share
# their high bytes within a chunk, so shuffling before zstd is a large win on both.
CODEC = [BloscCodec(cname="zstd", clevel=5, shuffle="shuffle")]


def make_storage(path, anonymous=False):
    """Icechunk storage for a local directory or an ``s3://bucket/prefix`` URI.

    Credentials come from the normal AWS chain (env, ~/.aws, instance role) via boto3 --
    several scripts in this repo paste the source.coop keys inline, which puts live
    credentials in git; set AWS_PROFILE to pick a different one.
    """
    if not path.startswith("s3://"):
        return icechunk.local_filesystem_storage(path)
    bucket, _, prefix = path[len("s3://") :].partition("/")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or S3_REGION
    if anonymous:  # the published store is world-readable; no credentials needed to read it
        return icechunk.s3_storage(
            bucket=bucket, prefix=prefix.rstrip("/"), region=region, anonymous=True
        )
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None)
    credentials = session.get_credentials()
    if credentials is None:
        raise SystemExit(f"no AWS credentials resolved for {path}")
    frozen = credentials.get_frozen_credentials()
    return icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix.rstrip("/"),
        region=session.region_name or region,
        access_key_id=frozen.access_key,
        secret_access_key=frozen.secret_key,
        session_token=frozen.token,
    )


def open_repo(path, create=True, anonymous=False):
    storage = make_storage(path, anonymous)
    if icechunk.Repository.exists(storage):
        return icechunk.Repository.open(storage)
    if not create:
        raise SystemExit(f"no Icechunk store at {path}")
    return icechunk.Repository.create(storage)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _to_time(value):
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError):
        return np.datetime64("NaT", "ns").astype("i8")
    if pd.isna(stamp):
        return np.iinfo(np.int64).min
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert(None)
    return stamp.value


def read_flight(path):
    """(attrs, time, {var: values}) for one flight NetCDF, or None if unreadable."""
    try:
        with xr.open_dataset(path) as ds:
            ds = ds.load()
    except Exception as err:
        print(f"  ! {path}: {type(err).__name__}: {err}", file=sys.stderr)
        return None
    return flight_from_dataset(ds)


def flight_from_dataset(ds):
    """Flatten an in-memory flight Dataset into the (attrs, time, columns) the writer takes.

    Variables a sonde never reported are filled rather than omitted, so every flight
    contributes the same columns and the obs arrays stay aligned.
    """
    n = ds.sizes.get("time", 0)
    if not n:
        return None
    times = ds.time.values.astype("datetime64[ns]").astype("i8")
    columns = {}
    for name, dtype in OBS_VARS.items():
        if name in ds:
            columns[name] = ds[name].values.astype(dtype)
        elif dtype.startswith("i"):
            columns[name] = np.zeros(n, dtype=dtype)
        else:
            columns[name] = np.full(n, np.nan, dtype=dtype)
    return dict(ds.attrs), times, columns


def _create(group, name, dtype, first, chunks, attrs):
    array = group.create_array(
        name, shape=(len(first),), dtype=dtype, chunks=chunks, compressors=CODEC
    )
    array[:] = first
    array.attrs.update(attrs)
    return array


def _append(group, name, values, dtype, varying, attrs):
    """Append `values`, creating the array on first use.

    On a rectilinear array `resize` turns the added extent into exactly one new chunk,
    which is what keeps one batch == one chunk == a whole number of flights.
    """
    if name in group:
        array = group[name]
        start = array.shape[0]
        array.resize((start + len(values),))
        array[start:] = values
        return
    chunks = ((len(values),),) if varying else (FLIGHT_CHUNK,)
    _create(group, name, dtype, values, chunks, attrs)


def write_batch(group, batch):
    """Append one batch of flights as a single chunk along obs."""
    times = np.concatenate([f[1] for f in batch])
    _append(group, "time", times, "i8", True, VAR_ATTRS["time"])
    for name, dtype in OBS_VARS.items():
        values = np.concatenate([f[2][name] for f in batch])
        _append(group, name, values, dtype, True, VAR_ATTRS.get(name, {}))


def write_flight_meta(group, attrs_list, starts, counts):
    for name in FLIGHT_STRS:
        values = np.array([str(a.get(name, "")) for a in attrs_list], dtype=object)
        _append(group, name, values, str, False, VAR_ATTRS.get(name, {}))
    for name, dtype in FLIGHT_NUMS.items():
        raw = np.array([_to_float(a.get(name)) for a in attrs_list])
        if dtype.startswith("i"):
            raw = np.nan_to_num(raw, nan=0)
        _append(group, name, raw.astype(dtype), dtype, False, VAR_ATTRS.get(name, {}))
    for name in FLIGHT_TIMES:
        values = np.array([_to_time(a.get(name)) for a in attrs_list], dtype="i8")
        _append(group, name, values, "i8", False, VAR_ATTRS["time"])
    _append(group, "obs_start", np.asarray(starts, "i8"), "i8", False, VAR_ATTRS["obs_start"])
    _append(group, "obs_count", np.asarray(counts, "i8"), "i8", False, VAR_ATTRS["obs_count"])


def rebuild_index(group):
    """Sorted serial index, so selection is a binary search rather than a full scan."""
    serials = np.asarray(group["serial"][:])
    order = np.argsort(serials, kind="stable")
    for name, values, dtype in (
        ("_serial_sorted", serials[order], str),
        ("_serial_flight", order.astype("i8"), "i8"),
    ):
        if name in group:
            del group[name]
        array = group.create_array(
            name, shape=(len(values),), dtype=dtype, chunks=(FLIGHT_CHUNK,), compressors=CODEC
        )
        array[:] = values
    group.attrs["n_flights"] = int(len(serials))


class WriterError(RuntimeError):
    """A store write failed and the session was dropped; the day must be retried."""


class FlightWriter:
    """Append flights to the store as they are produced.

    Holds a batch in memory until it reaches the chunk target, then writes it as one
    rectilinear chunk. `commit` closes the current Icechunk session and opens a fresh one,
    so callers should commit on a natural boundary (a finished day) rather than per flight.

    Only one writer may hold the store at a time.
    """

    def __init__(
        self, store, target_chunk_points=TARGET_CHUNK_POINTS, branch="main", index_every=INDEX_EVERY
    ):
        self.repo = open_repo(store)
        self.branch = branch
        self.target = target_chunk_points
        self.index_every = max(index_every, 1)
        self.commits_since_index = 0
        self._open_session()
        self.seen = set(np.asarray(self.group["serial"][:])) if "serial" in self.group else set()
        self.obs_total = int(self.group["time"].shape[0]) if "time" in self.group else 0
        self._reset()

    def _open_session(self):
        self.session = self.repo.writable_session(self.branch)
        self.group = zarr.open_group(store=self.session.store, mode="a")

    def _reset(self):
        self.batch, self.attrs_list, self.starts, self.counts = [], [], [], []
        self.batch_points = 0

    def reset_to_store(self):
        """Throw away uncommitted work and resync with what is actually committed.

        A write that fails partway leaves arrays resized inconsistently, and retrying the
        same batch would append it a second time. Starting from a fresh session is the only
        safe recovery -- the discarded flights are simply refetched.
        """
        self._open_session()
        self.seen = set(np.asarray(self.group["serial"][:])) if "serial" in self.group else set()
        self.obs_total = int(self.group["time"].shape[0]) if "time" in self.group else 0
        self.commits_since_index = 0
        self._reset()

    def __contains__(self, serial):
        return serial in self.seen

    def add(self, ds):
        """Queue one flight. False if it is empty or already in the store."""
        serial = str(ds.attrs.get("serial", ""))
        if not serial or serial in self.seen:
            return False
        flight = flight_from_dataset(ds)
        if flight is None:
            return False
        attrs, times, _ = flight
        attrs["serial"] = serial
        self.seen.add(serial)
        self.batch.append(flight)
        self.attrs_list.append(attrs)
        self.starts.append(self.obs_total)
        self.counts.append(len(times))
        self.obs_total += len(times)
        self.batch_points += len(times)
        if self.batch_points >= self.target:
            try:
                self.flush()
            except Exception as err:
                # A half-written batch leaves the obs arrays at different lengths. That must
                # never reach a commit, and it must not be swallowed as a per-flight error,
                # so the session is dropped and the failure raised as its own type.
                self.reset_to_store()
                raise WriterError(f"batch flush failed: {err}") from err
        return True

    def flush(self):
        if not self.batch:
            return
        write_batch(self.group, self.batch)
        write_flight_meta(self.group, self.attrs_list, self.starts, self.counts)
        self._reset()

    def commit(self, message, force_index=False):
        """Flush and commit. Returns the snapshot id, or None if there was nothing to do.

        The sorted serial index is a full rewrite of both index arrays, so refreshing it on
        every commit would re-upload the whole thing daily -- ~20 MB a day once the archive
        is complete, most of it immediately garbage. It is rebuilt every `index_every`
        commits instead; readers fall back to scanning the un-indexed tail, so a stale index
        costs a little speed but never correctness.
        """
        try:
            self.flush()
            self.commits_since_index += 1
            if force_index or self.commits_since_index >= self.index_every:
                rebuild_index(self.group)
                self.commits_since_index = 0
            # A day can legitimately add nothing -- every serial already pulled, or all of
            # them filtered out -- and Icechunk rejects an empty commit, so check first.
            if not self.session.has_uncommitted_changes:
                return None
            snapshot = self.session.commit(message)
        except Exception:
            # Writing to object storage can fail at any point (a dropped connection took
            # the whole run down once). Leave the writer on a clean session so the caller
            # can carry on; the caller decides whether to retry the day.
            self.reset_to_store()
            raise
        self._open_session()
        return snapshot


def build(args):
    repo = open_repo(args.store)
    session = repo.writable_session("main")
    group = zarr.open_group(store=session.store, mode="a")

    have = set(np.asarray(group["serial"][:])) if "serial" in group else set()
    obs_total = int(group["time"].shape[0]) if "time" in group else 0

    paths = sorted(glob.glob(os.path.join(args.flights_dir, "*.nc")))
    todo = [p for p in paths if _serial_of(p) not in have]
    print(
        f"{len(paths)} flight files, {len(have)} already in store, {len(todo)} to ingest; "
        f"obs so far {obs_total:,}",
        flush=True,
    )
    if not todo:
        if "serial" in group and not group.attrs.get("n_flights"):
            rebuild_index(group)
            session.commit("rebuild serial index")
        return

    batch, attrs_list, starts, counts = [], [], [], []
    batch_points = ingested = committed = 0
    started = _time.time()

    def flush():
        nonlocal batch, attrs_list, starts, counts, batch_points
        if not batch:
            return
        write_batch(group, batch)
        write_flight_meta(group, attrs_list, starts, counts)
        batch, attrs_list, starts, counts = [], [], [], []
        batch_points = 0

    try:
        for path in todo:
            flight = read_flight(path)
            if flight is None:
                continue
            attrs, times, _ = flight
            serial = str(attrs.get("serial") or _serial_of(path))
            if serial in have:
                continue
            have.add(serial)
            attrs["serial"] = serial
            batch.append(flight)
            attrs_list.append(attrs)
            starts.append(obs_total)
            counts.append(len(times))
            obs_total += len(times)
            batch_points += len(times)
            ingested += 1

            # Cut the chunk at a flight boundary once it is big enough; a flight longer
            # than the target simply becomes its own chunk.
            if batch_points >= args.target_chunk_points:
                flush()
            if ingested % args.commit_every == 0:
                flush()
                rebuild_index(group)
                snapshot = session.commit(f"ingest {ingested} flights")
                committed = ingested
                session = repo.writable_session("main")
                group = zarr.open_group(store=session.store, mode="a")
                rate = ingested / max(_time.time() - started, 1e-9)
                print(
                    f"  {ingested:,}/{len(todo):,} flights, {obs_total:,} samples, "
                    f"{rate:.0f} flights/s, snapshot {snapshot[:8]}",
                    flush=True,
                )
        flush()
        rebuild_index(group)
        session.commit(f"ingest {ingested} flights")
    except KeyboardInterrupt:
        print("\nInterrupted; committing what is ingested", file=sys.stderr)
        flush()
        rebuild_index(group)
        session.commit(f"ingest {ingested} flights (interrupted)")

    elapsed = _time.time() - started
    print(f"Done. {ingested:,} flights ingested ({obs_total:,} samples) in {elapsed / 60:.1f} min.")


def _serial_of(path):
    return os.path.basename(path)[len("flight_") : -len(".nc")]


class SondeFlights:
    """Read side: pull flights out by serial, or filter on the flight metadata."""

    def __init__(self, store=DEFAULT_STORE, branch="main", anonymous=False):
        self.repo = open_repo(store, create=False, anonymous=anonymous)
        self.session = self.repo.readonly_session(branch)
        self.group = zarr.open_group(store=self.session.store, mode="r")
        self._sorted = None
        self._index = None
        self._tail = None
        self._tail_offset = 0
        self._meta = {}

    def __len__(self):
        return int(self.group["serial"].shape[0])

    def _lookup(self, serial):
        if self._sorted is None:
            # A store whose index has not been built yet is served entirely by the tail
            # scan below, rather than failing on the missing array.
            if "_serial_sorted" in self.group:
                self._sorted = np.asarray(self.group["_serial_sorted"][:])
                self._index = np.asarray(self.group["_serial_flight"][:])
            else:
                self._sorted = np.asarray([], dtype=object)
                self._index = np.asarray([], dtype="i8")
        pos = np.searchsorted(self._sorted, serial)
        if pos < len(self._sorted) and self._sorted[pos] == serial:
            return int(self._index[pos])
        # The index only covers flights up to the last rebuild; anything committed since
        # sits in the tail of `serial`, which is short enough to scan directly.
        if self._tail is None:
            self._tail_offset = len(self._sorted)
            self._tail = np.asarray(self.group["serial"][self._tail_offset :])
        hits = np.flatnonzero(self._tail == serial)
        if hits.size:
            return int(self._tail_offset + int(hits[0]))
        raise KeyError(f"serial {serial!r} not in store")

    def sel(self, serial):
        """One flight as an xarray Dataset, read from a single chunk per variable."""
        flight = self._lookup(serial)
        start = int(self._meta_column("obs_start")[flight])
        count = int(self._meta_column("obs_count")[flight])
        stop = start + count
        data = {
            name: ("time", self.group[name][start:stop], VAR_ATTRS.get(name, {}))
            for name in OBS_VARS
        }
        times = np.asarray(self.group["time"][start:stop]).astype("datetime64[ns]")
        ds = xr.Dataset(data, coords={"time": times})
        ds.attrs = self.flight_attrs(flight)
        return ds

    def _meta_column(self, name):
        """Whole flight-dim column, cached.

        Reading 29 metadata scalars one at a time costs ~2.5 ms each in per-read overhead
        regardless of chunk size, which dwarfs the actual flight read. Pulling each column
        once and keeping it makes every subsequent flight's metadata free.
        """
        if name not in self._meta:
            self._meta[name] = np.asarray(self.group[name][:])
        return self._meta[name]

    def flight_attrs(self, flight):
        attrs = {}
        for name in FLIGHT_STRS:
            if name in self.group:
                value = str(self._meta_column(name)[flight])
                if value:
                    attrs[name] = value
        for name in FLIGHT_NUMS:
            if name in self.group:
                value = float(self._meta_column(name)[flight])
                if not np.isnan(value):
                    attrs[name] = value
        for name in FLIGHT_TIMES:
            if name in self.group:
                raw = int(self._meta_column(name)[flight])
                if raw != np.iinfo(np.int64).min:
                    attrs[name] = str(np.datetime64(raw, "ns"))
        return attrs

    def flights(self):
        """All per-flight metadata as one Dataset -- the thing to filter on."""
        data = {}
        for name in FLIGHT_STRS:
            if name in self.group:
                data[name] = ("flight", np.asarray(self.group[name][:]))
        for name in FLIGHT_NUMS:
            if name in self.group:
                data[name] = ("flight", self.group[name][:])
        for name in FLIGHT_TIMES:
            if name in self.group:
                data[name] = ("flight", np.asarray(self.group[name][:]).astype("datetime64[ns]"))
        for name in FLIGHT_INDEX:
            data[name] = ("flight", self.group[name][:])
        return xr.Dataset(data)


def store_bytes(path):
    """Total bytes held by the store, local or in S3."""
    if not path.startswith("s3://"):
        return sum(
            os.path.getsize(os.path.join(root, f))
            for root, _, files in os.walk(path)
            for f in files
        )
    bucket, _, prefix = path[len("s3://") :].partition("/")
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE") or None)
    client = session.client("s3", region_name=session.region_name or S3_REGION)
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/"
    )
    return sum(obj["Size"] for page in pages for obj in page.get("Contents", []))


def show_info(args):
    store = SondeFlights(args.store)
    group = store.group
    n_obs = int(group["time"].shape[0])
    print(f"store      : {args.store}")
    print(f"flights    : {len(store):,}")
    print(f"samples    : {n_obs:,}")
    grid = group["altitude"].metadata.chunk_grid
    sizes = np.asarray(grid.chunk_shapes[0]) if hasattr(grid, "chunk_shapes") else np.array([])
    if sizes.size:
        print(
            f"obs chunks : {sizes.size:,} rectilinear "
            f"(min {sizes.min():,} / median {int(np.median(sizes)):,} / max {sizes.max():,} samples)"
        )
    print(
        f"on disk    : {store_bytes(args.store) / 1e9:.2f} GB "
        f"({store_bytes(args.store) / max(n_obs, 1):.2f} bytes/sample)"
    )


def show_sel(args):
    store = SondeFlights(args.store)
    started = _time.time()
    ds = store.sel(args.sel)
    elapsed = _time.time() - started
    print(ds)
    print()
    for key, value in ds.attrs.items():
        print(f"  {key}: {value}")
    print(f"\nread in {elapsed * 1000:.0f} ms")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--store", default=DEFAULT_STORE)
    parser.add_argument("--flights-dir", default=DEFAULT_FLIGHTS)
    parser.add_argument(
        "--target-chunk-points",
        type=int,
        default=TARGET_CHUNK_POINTS,
        help="samples per obs chunk before rounding to a flight boundary",
    )
    parser.add_argument(
        "--commit-every", type=int, default=20000, help="flights between Icechunk commits"
    )
    parser.add_argument("--sel", metavar="SERIAL", help="read one flight back and print it")
    parser.add_argument("--info", action="store_true", help="summarise the store")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    if args.sel:
        show_sel(args)
    elif args.info:
        show_info(args)
    else:
        build(args)
