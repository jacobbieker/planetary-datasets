"""Virtual Icechunk stores of NOAA's Hurricane Analysis and Forecast System (HAFS).

No GRIB is copied. As in :mod:`~planetary_datasets.providers.virtualized.noaa_nests`, every
GRIB2 message on ``s3://noaa-nws-hafs-pds`` becomes one chunk, referenced by byte range and
decoded at read time by the ``gribberish`` zarr codec.

Source layout (``hfsa/`` is HAFS-A, ``hfsb/`` is HAFS-B)::

    hfsa/20261015/00/20e.2026101500.hfsa.parent.atm.f003.grb2   (+ .idx)
    hfsa/20261015/00/20e.2026101500.hfsa.storm.atm.f003.grb2    (+ .idx)

One run per storm per cycle, four cycles a day, steps 0-126 h every 3 h. Both domains are
regular latitude-longitude grids, and neither is fixed: the ``parent`` (1361 x 1681 at
0.06 deg) is centred on the storm when the run starts and holds still for the run, and the
``storm`` nest (801 x 1001 at 0.02 deg) follows the storm, moving at every step.

Stores, one per variant and domain, under ``bkr/dmi/virtualized``: ``hafs_a_storm``,
``hafs_a_parent``, ``hafs_b_storm`` and ``hafs_b_parent``. Only the ``.atm`` files are
referenced: the ``.sat`` files hold synthetic satellite channels gribberish cannot parse,
and the swath and wave files are one-off products without a step axis.

Layout. A run is a storm *and* a cycle, so the store's append dimension is ``run``, not
``init_time``: every array is ``(run, step, [level], y, x)``, with ``init_time``, ``storm``
(the ATCF id as in the file names, e.g. ``20e``) and ``storm_name`` as coordinates along
``run``. A dense ``(init_time, storm)`` grid would be mostly empty — storms live a week or
two — and could not grow its storm axis by appending. Runs are appended in the order they
are referenced, not sorted, so a backfill can run in any order; select runs by coordinate.

Moving grids. Because the grid moves, ``latitude`` and ``longitude`` are coordinates per
run and step, ``latitude(run, step, y)`` and ``longitude(run, step, x)``. They are virtual
too: each chunk references the smallest message of that step's file, and the gribberish
codec rebuilds the axis from the message's grid definition, so they cost no storage and
are exact. ``center_latitude(run, step)`` and ``center_longitude(run, step)``, the middle
of the grid, are stored as plain values, so finding runs near a place reads a few bytes.
Longitudes are in ``[0, 360)`` and wrap at the prime meridian for grids that cross it.

Groups, generations and the run ledger are those of ``noaa_nests``: gribberish's
content-independent groups (``/sfc/instant``, ``/isobar/instant``, ...), a dated
``<store>_<DDMMYYYY>`` generation when a run brings fields no store holds (HAFS added
10 m gust in October 2026), and the referenced runs listed in the root attributes as
``<init_time>/<storm>``.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.config import Config
from planetary_datasets.providers.virtualized.noaa_nests import (
    LEDGER_ATTR,
    NestVirtualProvider,
    _collect,
    _field_attrs,
    _manifest_array,
    _parse,
    _registry,
    _Schema,
    _store_schema,
    _Var,
)

BUCKET = "noaa-nws-hafs-pds"
STORE_BASE = "bkr/dmi/virtualized"
RUN_DIM = "run"
STEPS: Tuple[int, ...] = tuple(range(0, 127, 3))
CADENCE_HOURS = 6
#: The first cycle on the bucket, for both variants (Bret, 03L).
ARCHIVE_START = pd.Timestamp("2023-06-19T18:00")
#: A run published with fewer than every step is left for later while it is younger than
#: this: its files arrive over about two hours, from roughly 4.5 h after the cycle.
SETTLE = pd.Timedelta(hours=24)

GRID_COORDS = ("latitude", "longitude")
CENTRE_COORDS = ("center_latitude", "center_longitude")
RUN_COORDS = ("init_time", "storm", "storm_name")
#: Chunk length along ``run`` of the plain per-run coordinates, about four months of HAFS-A
#: runs, so reading them is a few requests rather than one per run.
RUN_CHUNK = 1024

_FILE = re.compile(
    r"^(?P<storm>[0-9]{2}[a-z])\.(?P<cycle>\d{10})\.(?P<model>hfs[ab])\."
    r"(?P<domain>parent|storm)\.atm\.f(?P<step>\d{3})\.grb2(?P<idx>\.idx)?$"
)


@dataclass(frozen=True)
class HafsProduct:
    """One HAFS variant and domain, and the store it is referenced into."""

    variant: str  # "a" or "b"
    domain: str  # "storm" or "parent"
    bucket: str = BUCKET
    steps: Tuple[int, ...] = STEPS
    use_index: bool | str = ".idx"
    parse_workers: int = 16
    #: Runs per manifest split along ``run``: about a day of storms.
    split_size: int = 8

    @property
    def model(self) -> str:
        """The bucket's name for the variant: ``hfsa`` or ``hfsb``."""
        return f"hfs{self.variant}"

    @property
    def name(self) -> str:
        """The store's name, e.g. ``hafs_a_storm``."""
        return f"hafs_{self.variant}_{self.domain}"

    @property
    def store_prefix(self) -> str:
        """The base generation's location, relative to the configured bucket."""
        return f"{STORE_BASE}/{self.name}.icechunk"

    def folder(self, it: pd.Timestamp) -> str:
        """The cycle's folder, as a bucket-relative key."""
        return f"{self.model}/{it:%Y%m%d}/{it:%H}"

    def key(self, it: pd.Timestamp, storm: str, step: int) -> str:
        """The bucket-relative key of one step's ``.atm`` GRIB2 file."""
        return (
            f"{self.folder(it)}/{storm}.{it:%Y%m%d%H}.{self.model}.{self.domain}.atm."
            f"f{step:03d}.grb2"
        )

    def url(self, it: pd.Timestamp, storm: str, step: int) -> str:
        """The ``s3://`` URL of one step's ``.atm`` GRIB2 file."""
        return f"s3://{self.bucket}/{self.key(it, storm, step)}"


PRODUCTS: Dict[str, HafsProduct] = {
    p.name: p for p in (HafsProduct(v, d) for v in ("a", "b") for d in ("storm", "parent"))
}


def cycles(start, end) -> pd.DatetimeIndex:
    """Every HAFS cycle from ``start`` to ``end``, clipped to the archive's start."""
    start = max(pd.Timestamp(start).floor(f"{CADENCE_HOURS}h"), ARCHIVE_START)
    return pd.date_range(start, pd.Timestamp(end), freq=f"{CADENCE_HOURS}h")


# ---------------------------------------------------------------------------- listing


@dataclass
class StormRun:
    """The files one storm has in one cycle, for one domain."""

    storm: str
    steps: List[int]
    indexed: List[int]

    @property
    def complete(self) -> bool:
        """Whether every step is published, each with its index."""
        return set(self.indexed) >= set(STEPS) and set(self.steps) >= set(STEPS)


def _list_folder(bucket: str, folder: str) -> List[str]:
    import fsspec

    fs = fsspec.filesystem("s3", anon=True)
    try:
        return [p.rsplit("/", 1)[-1] for p in fs.ls(f"{bucket}/{folder}", refresh=True)]
    except FileNotFoundError:
        return []


def _storm_runs(product: HafsProduct, it: pd.Timestamp, names: Iterable[str]) -> List[StormRun]:
    """The storms of the cycle at ``it`` with files of this domain, from a folder listing."""
    runs: Dict[str, StormRun] = {}
    for name in names:
        m = _FILE.match(name)
        if (
            m is None
            or m["model"] != product.model
            or m["domain"] != product.domain
            or m["cycle"] != f"{it:%Y%m%d%H}"
        ):
            continue
        run = runs.setdefault(m["storm"], StormRun(m["storm"], [], []))
        step = int(m["step"])
        if step not in product.steps:
            continue
        (run.indexed if m["idx"] else run.steps).append(step)
    for run in runs.values():
        run.steps.sort()
        run.indexed.sort()
    return [runs[s] for s in sorted(runs) if runs[s].steps]


def _storm_name(product: HafsProduct, it: pd.Timestamp, storm: str) -> str:
    """The storm's name (``simon``, ``invest``) from the run's ``storm_info`` file."""
    import fsspec

    fs = fsspec.filesystem("s3", anon=True)
    key = f"{product.bucket}/{product.folder(it)}/{storm}.{it:%Y%m%d%H}.{product.model}.storm_info"
    try:
        text = fs.cat_file(key).decode(errors="replace").strip().lower()
    except (FileNotFoundError, OSError):
        return ""
    return text.removesuffix(storm.lower())


# ---------------------------------------------------------------------------- assembly


def _step_grids(
    parsed: Dict[int, Dict[str, xr.Dataset]],
) -> Dict[int, Tuple[np.ndarray, np.ndarray]]:
    """Each step's 1-D latitude and longitude, as the parser computed them (north first)."""
    out = {}
    for step, groups in parsed.items():
        for ds in groups.values():
            if "latitude" in ds.coords and "longitude" in ds.coords:
                out[step] = (np.asarray(ds["latitude"].values), np.asarray(ds["longitude"].values))
                break
    return out


def _smallest_messages(variables: Dict[Tuple[str, str], _Var]) -> Dict[int, Tuple[str, int, int]]:
    """Per step, the smallest referenced message: the cheapest one to decode a grid from."""
    best: Dict[int, Tuple[str, int, int]] = {}
    for var in variables.values():
        for (step, _), ref in var.cells.items():
            if step not in best or ref[2] < best[step][2]:
                best[step] = ref
    return best


def _to_yx(variables: Dict[Tuple[str, str], _Var]) -> None:
    """Rename the parser's ``latitude``/``longitude`` dims to ``y``/``x``, in place.

    The axes move between runs, so they cannot be index coordinates named after them.
    """
    for var in variables.values():
        var.dims = (*var.dims[:-2], "y", "x")


def _grid_coord_array(
    name: str, size: int, steps: Tuple[int, ...], refs: Dict[int, Tuple[str, int, int]]
):
    """``latitude(run=1, step, y)`` or ``longitude(run=1, step, x)``, decoded from messages."""
    from gribberish.virtualizarr.parser import _gribberish_codecs
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from virtualizarr.manifests.utils import create_v3_array_metadata

    grid = (1, len(steps), 1)
    paths = np.full(grid, "", dtype=np.dtypes.StringDType())
    offsets = np.zeros(grid, dtype=np.uint64)
    lengths = np.zeros(grid, dtype=np.uint64)
    for i, step in enumerate(steps):
        if step in refs:
            paths[0, i, 0], offsets[0, i, 0], lengths[0, i, 0] = refs[step]
    is_lat = name == "latitude"
    attrs = {
        "standard_name": name,
        "long_name": name,
        "units": "degrees_north" if is_lat else "degrees_east",
    }
    metadata = create_v3_array_metadata(
        shape=(1, len(steps), size),
        data_type=np.dtype("float64"),
        chunk_shape=(1, 1, size),
        fill_value=float("nan"),
        codecs=_gribberish_codecs(name, north_up=True),
        attributes=attrs,
        dimension_names=(RUN_DIM, "step", "y" if is_lat else "x"),
    )
    manifest = ChunkManifest.from_arrays(paths=paths, offsets=offsets, lengths=lengths)
    return ManifestArray(metadata=metadata, chunkmanifest=manifest)


#: Attributes the parser copies from one message that would be wrong for every other run.
def _centres(
    grids: Dict[int, Tuple[np.ndarray, np.ndarray]], steps: Tuple[int, ...]
) -> Tuple[np.ndarray, np.ndarray]:
    """The middle grid point of each step, NaN where the step is missing."""
    lat = np.full(len(steps), np.nan)
    lon = np.full(len(steps), np.nan)
    for i, step in enumerate(steps):
        if step in grids:
            la, lo = grids[step]
            lat[i] = la[len(la) // 2]
            lon[i] = lo[len(lo) // 2]
    return lat, lon


# ---------------------------------------------------------------------------- provider


class HafsVirtualProvider(NestVirtualProvider):
    """Reference HAFS runs of one variant and domain into its generational store.

    A partition is a cycle: :meth:`run_partition` references every storm of that cycle
    the store does not hold yet, one commit per storm.
    """

    append_dim = RUN_DIM
    # The run-per-row writer below has no way to extend a store, so a run with a field the
    # store lacks starts a generation (older runs without it still fit the newer store).
    extend_new_variables = False

    def __init__(self, product: HafsProduct | str, config: Config | None = None):
        """Set up a provider for one store.

        Args:
            product: A :class:`HafsProduct`, or its name in :data:`PRODUCTS`.
            config: Override configuration; defaults to the process-wide one.
        """
        super().__init__(PRODUCTS[product] if isinstance(product, str) else product, config)

    @staticmethod
    def run_key(it: pd.Timestamp, storm: str) -> str:
        """The ledger entry of one run: ``2026-10-15T00:00:00/20e``."""
        return f"{pd.Timestamp(it).isoformat()}/{storm}"

    def _read_schema(self, repo) -> _Schema | None:
        return _store_schema(
            repo,
            lead=(RUN_DIM, "step"),
            skip=frozenset(GRID_COORDS + CENTRE_COORDS + RUN_COORDS),
        )

    def done(self) -> set[str]:
        """Every run key any generation lists."""
        out: set[str] = set()
        for prefix in self.generations():
            out |= self.ledger(prefix)
        return out

    def pending(
        self,
        it: pd.Timestamp,
        listing: List[str] | None = None,
        now: pd.Timestamp | None = None,
        done: set[str] | None = None,
    ) -> List[StormRun]:
        """Runs of the cycle at ``it`` that are published, settled and not yet stored.

        A run missing steps is pending only once it is older than :data:`SETTLE`, so a
        cycle still being published is not referenced half-written.
        """
        it = pd.Timestamp(it)
        now = pd.Timestamp.now(tz="UTC").tz_localize(None) if now is None else now
        names = (
            listing
            if listing is not None
            else _list_folder(self.product.bucket, self.product.folder(it))
        )
        done = self.done() if done is None else done
        out = []
        for run in _storm_runs(self.product, it, names):
            if self.run_key(it, run.storm) in done:
                continue
            if not run.complete and now - it < SETTLE:
                logger.info(
                    f"{self.name}: {it} {run.storm} has {len(run.steps)}/{len(STEPS)} steps; "
                    "waiting for the rest"
                )
                continue
            out.append(run)
        return out

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Cycles in ``desired`` with at least one published run not yet stored."""
        desired = [pd.Timestamp(t) for t in desired]
        with ThreadPoolExecutor(16) as pool:
            listings = list(
                pool.map(
                    lambda t: _list_folder(self.product.bucket, self.product.folder(t)), desired
                )
            )
        done = self.done()
        return [
            t for t, names in zip(desired, listings) if self.pending(t, listing=names, done=done)
        ]

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Reference every pending storm of the cycle at ``it``; False if there was none.

        ``check_present`` is accepted for the shared signature; the ledger is always
        consulted, because a cycle is written storm by storm.
        """
        it = pd.Timestamp(it)
        runs = self.pending(it)
        if not runs:
            logger.info(f"{self.name}: nothing to reference for {it}")
            return False
        wrote = False
        for run in runs:
            wrote |= self.run_storm(it, run.storm, run.steps)
        return wrote

    def run_storm(self, it: pd.Timestamp, storm: str, steps: List[int] | None = None) -> bool:
        """Reference one storm's run of the cycle at ``it``."""
        it = pd.Timestamp(it)
        steps = list(self.product.steps) if steps is None else steps
        registry = _registry(self.product.bucket)
        with ThreadPoolExecutor(self.product.parse_workers) as pool:
            results = list(
                pool.map(
                    lambda s: (s, _parse(self.product, self.product.url(it, storm, s), registry)),
                    steps,
                )
            )
        parsed = {s: groups for s, groups in results if groups}
        if not parsed:
            logger.warning(f"{self.name}: no readable files for {it} {storm}")
            return False
        grids = _step_grids(parsed)
        variables, coords = _collect(parsed)
        _to_yx(variables)
        prefix, schema = self.choose_generation(it, variables)
        return self._write_run(
            prefix,
            schema,
            it,
            storm,
            _storm_name(self.product, it, storm),
            variables,
            coords,
            grids,
        )

    def _write_run(
        self,
        prefix: str,
        schema: _Schema | None,
        it: pd.Timestamp,
        storm: str,
        storm_name: str,
        variables: Dict[Tuple[str, str], _Var],
        coords: Dict[str, xr.Dataset],
        grids: Dict[int, Tuple[np.ndarray, np.ndarray]],
    ) -> bool:
        steps = self.product.steps
        first = schema is None
        if first:
            arrays = {
                k: {"dims": v.dims, "attrs": v.attrs, "spatial": v.spatial}
                for k, v in variables.items()
            }
            levels: Dict[Tuple[str, str], np.ndarray] = {}
            for (group, _), var in variables.items():
                for dim, values in var.levels.items():
                    have = levels.get((group, dim))
                    levels[(group, dim)] = np.sort(
                        values if have is None else np.union1d(have, values)
                    )
            schema = _Schema(arrays=arrays, levels=levels)

        ny, nx = next(iter(schema.arrays.values()))["spatial"]
        refs = _smallest_messages(variables)
        centre_lat, centre_lon = _centres(grids, steps)
        run_coords: Dict[str, Any] = {
            "init_time": (RUN_DIM, np.array([np.datetime64(it, "ns")])),
            "storm": (RUN_DIM, np.array([storm], dtype=object)),
            "storm_name": (RUN_DIM, np.array([storm_name], dtype=object)),
            "center_latitude": ((RUN_DIM, "step"), centre_lat[None]),
            "center_longitude": ((RUN_DIM, "step"), centre_lon[None]),
            "latitude": ((RUN_DIM, "step", "y"), _grid_coord_array("latitude", ny, steps, refs)),
            "longitude": ((RUN_DIM, "step", "x"), _grid_coord_array("longitude", nx, steps, refs)),
        }

        by_group: Dict[str, Dict[str, Any]] = {}
        for (group, name), meta in schema.arrays.items():
            var = variables.get((group, name))
            axes = [schema.levels[(group, d)] for d in meta["dims"][:-2]]
            by_group.setdefault(group, {})[name] = (
                _manifest_array(
                    name,
                    meta["dims"],
                    meta["attrs"],
                    meta["spatial"],
                    steps,
                    axes,
                    var.cells if var is not None else {},
                    lead_dim=RUN_DIM,
                ),
                (RUN_DIM, "step", *meta["dims"]),
            )

        repo = self.repo(prefix)
        session = repo.writable_session("main")
        for group, data in sorted(by_group.items()):
            # The writer takes attributes from the variable, not the array's metadata.
            ds = xr.Dataset(
                {
                    name: xr.Variable(dims, ma, attrs=_field_attrs(ma.metadata.attributes))
                    for name, (ma, dims) in data.items()
                },
                coords={
                    k: xr.Variable(d, v, attrs=v.metadata.attributes if k in GRID_COORDS else {})
                    for k, (d, v) in run_coords.items()
                },
            )
            path = group.lstrip("/") or None
            if first:
                ds = ds.assign_coords(step=("step", pd.to_timedelta(steps, unit="h").values))
                for (g, dim), values in schema.levels.items():
                    if g == group:
                        ds = ds.assign_coords({dim: (dim, values)})
                if "spatial_ref" in coords.get(group, {}):
                    ds = ds.assign_coords(spatial_ref=coords[group]["spatial_ref"])
                ds["init_time"].encoding.update(
                    {"units": "seconds since 1970-01-01", "dtype": "int64"}
                )
                for name in RUN_COORDS:
                    ds[name].encoding["chunks"] = (RUN_CHUNK,)
                for name in CENTRE_COORDS:
                    ds[name].encoding["chunks"] = (RUN_CHUNK // 4, len(steps))
                ds.vz.to_icechunk(session.store, group=path)
            else:
                ds.vz.to_icechunk(session.store, group=path, append_dim=RUN_DIM)
            # An append rewrites the group's coordinate list from what it carried.
            node = zarr.open_group(session.store, path=path or "", mode="a")
            names = [*run_coords, *(["spatial_ref"] if "spatial_ref" in node else [])]
            node.attrs["coordinates"] = " ".join(names)
        root = zarr.open_group(session.store, mode="a")
        root.attrs[LEDGER_ATTR] = sorted(
            set(root.attrs.get(LEDGER_ATTR, [])) | {self.run_key(it, storm)}
        )
        session.commit(f"{self.name}: {storm} run {it:%Y-%m-%dT%H}")
        logger.info(f"{self.name}: referenced {storm} {it} into {prefix.rsplit('/', 1)[-1]}")
        return True


def open_store(product: HafsProduct | str, config: Config | None = None) -> xr.DataTree:
    """Open the newest generation of a HAFS store as a tree of lazily-read groups."""
    provider = HafsVirtualProvider(product, config)
    for prefix in reversed(provider.generations()):
        repo = provider._existing_repo(prefix)
        if repo is not None:
            return xr.open_datatree(
                repo.readonly_session("main").store,
                engine="zarr",
                consolidated=False,
                chunks={},
            )
    raise FileNotFoundError(f"no {provider.name} store yet")


__all__ = [
    "ARCHIVE_START",
    "PRODUCTS",
    "HafsProduct",
    "HafsVirtualProvider",
    "StormRun",
    "cycles",
    "open_store",
]
