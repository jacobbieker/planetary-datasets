"""Virtual Icechunk stores of the NOAA HRRR Alaska and NAM Hawaii nests.

No GRIB is copied. Each store holds *references* to the GRIB2 messages on NOAA's public
buckets, written by gribberish's VirtualiZarr parser: every message is one chunk, decoded at
read time by the ``gribberish`` zarr codec (``pip install gribberish``), so a run of the
whole model costs a few hundred kilobytes of manifest rather than gigabytes of data.

Stores, one per GRIB file kind, under ``bkr/dmi/virtualized``:

* ``hrrr_alaska_sfc``, ``hrrr_alaska_prs``, ``hrrr_alaska_nat`` — HRRR's ``wrfsfc``,
  ``wrfprs`` and ``wrfnat`` files, every three hours from 2018-07-11, steps 0-48 h;
* ``nam_hawaii`` — the NAM Hawaii nest, every six hours from 2021-09-16, steps 0-60 h.

Layout. Inside a store, variables sit in gribberish's content-independent groups, by level
type then product kind: ``/sfc/instant``, ``/isobar/instant``, ``/hag/max1h`` and so on.
Each array is ``(init_time, step, [level], y, x)``. ``step`` is fixed per model, so a run that
stops short (HRRR's off-synoptic runs end at 18 h) and a field absent at some steps
(accumulations have no 0 h) are *missing chunks*, which read back as NaN at no cost.
Latitude and longitude are 2-D coordinates referencing the first run written.

Generations. A model upgrade adds fields. A run whose variables are all in a store's schema
is appended there, with anything it lacks left missing; a run with variables no store holds
starts ``<store>_<DDMMYYYY>`` named for its date, so an upgrade never drops its new fields
and never forces old runs to be rewritten.

Bookkeeping. Each store lists the runs it holds in its root attributes (:data:`LEDGER_ATTR`),
updated in the same commit as the run, so "is this run stored?" is an attribute read.
"""

from __future__ import annotations

import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.common.generations import prefix_for_date
from planetary_datasets.config import Config, get_config
from planetary_datasets.providers.virtualized.virtual_repo import open_virtual_repo

LEDGER_ATTR = "ingested_runs"
STORE_BASE = "bkr/dmi/virtualized"
SPATIAL = ("y", "x")
#: How many generations a store may reach before a run is refused rather than starting
#: another: more than this means the schema is splitting on noise, not on upgrades.
MAX_GENERATIONS = 12


@dataclass(frozen=True)
class NestProduct:
    """One GRIB file kind of one nest, and the store it is referenced into."""

    name: str
    bucket: str
    #: ``{date}``, ``{hour}`` and ``{step}`` are filled per file.
    key_template: str
    steps: Tuple[int, ...]
    cadence_hours: int
    archive_start: pd.Timestamp
    #: ``".idx"`` to build manifests from NOAA's wgrib2 index (header bytes only), or False
    #: to read each file whole, for files whose index the parser cannot use.
    use_index: bool | str = ".idx"
    parse_workers: int = 8
    #: Runs per manifest split along ``init_time``; one day of runs.
    split_size: int = field(default=8)

    @property
    def store_prefix(self) -> str:
        return f"{STORE_BASE}/{self.name}.icechunk"

    def url(self, it: pd.Timestamp, step: int) -> str:
        key = self.key_template.format(date=it.strftime("%Y%m%d"), hour=it.hour, step=step)
        return f"s3://{self.bucket}/{key}"


def _hrrr(kind: str) -> NestProduct:
    return NestProduct(
        name=f"hrrr_alaska_{kind.removeprefix('wrf')}",
        bucket="noaa-hrrr-bdp-pds",
        key_template="hrrr.{date}/alaska/hrrr.t{hour:02d}z." + kind + "f{step:02d}.ak.grib2",
        steps=tuple(range(49)),
        cadence_hours=3,
        archive_start=pd.Timestamp("2018-07-11T00:00"),
    )


PRODUCTS: Dict[str, NestProduct] = {
    p.name: p
    for p in (
        _hrrr("wrfsfc"),
        _hrrr("wrfprs"),
        _hrrr("wrfnat"),
        NestProduct(
            name="nam_hawaii",
            bucket="noaa-nam-pds",
            key_template="nam.{date}/nam.t{hour:02d}z.hawaiinest.hiresf{step:02d}.tm00.grib2",
            steps=tuple(range(61)),
            cadence_hours=6,
            archive_start=pd.Timestamp("2021-09-16T00:00"),
            # The index is present, but the header windows it yields do not parse.
            use_index=False,
            split_size=4,
        ),
    )
}


# ---------------------------------------------------------------------------- parsing


def _registry(bucket: str):
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import S3Store

    return ObjectStoreRegistry(
        {f"s3://{bucket}": S3Store(bucket, region="us-east-1", skip_signature=True)}
    )


def _parse(product: NestProduct, url: str, registry) -> Dict[str, xr.Dataset] | None:
    """One file's groups, as virtual datasets; None if the file is not there."""
    import virtualizarr as vz
    from gribberish.virtualizarr import GribberishParser

    def open_with(use_index):
        parser = GribberishParser(use_index=use_index, accumulate_dims=True, north_up=True)
        return vz.open_virtual_datatree(url, registry=registry, parser=parser)

    try:
        tree = open_with(product.use_index)
    except FileNotFoundError:
        if not product.use_index:
            return None
        # No index for this file (older archive years), or none it can parse: read it whole.
        try:
            tree = open_with(False)
        except FileNotFoundError:
            return None
    except ValueError:
        if not product.use_index:
            raise
        tree = open_with(False)
    return {node.path: node.to_dataset(inherit=False) for node in tree.subtree if node.ds.data_vars}


def _listed_steps(product: NestProduct, it: pd.Timestamp) -> List[int]:
    """Steps of the run at ``it`` that are actually published."""
    import fsspec

    fs = fsspec.filesystem("s3", anon=True)
    wanted = {product.url(it, s).removeprefix("s3://"): s for s in product.steps}
    folder = next(iter(wanted)).rsplit("/", 1)[0]
    try:
        listed = set(fs.ls(folder, refresh=True))
    except FileNotFoundError:
        return []
    return sorted(s for key, s in wanted.items() if key in listed)


@dataclass
class _Var:
    """One variable assembled across the steps of a run."""

    dims: Tuple[str, ...]  # without init_time/step
    attrs: dict
    levels: Dict[str, np.ndarray]  # level dim -> values, per the run
    cells: Dict[Tuple[int, Tuple[Any, ...]], Tuple[str, int, int]] = field(default_factory=dict)
    spatial: Tuple[int, int] = (0, 0)


def _collect(parsed: Dict[int, Dict[str, xr.Dataset]]):
    """Gather every message reference of a run, keyed by step and level *values*.

    Returns ``(variables, coords)``: ``variables[(group, name)]`` is a :class:`_Var`, and
    ``coords[group]`` holds the group's non-step coordinates from the first step that has
    them (spatial references, level values, ``spatial_ref``).
    """
    variables: Dict[Tuple[str, str], _Var] = {}
    coords: Dict[str, xr.Dataset] = {}
    for step, groups in sorted(parsed.items()):
        for group, ds in groups.items():
            if group not in coords:
                keep = [c for c in ds.coords if "time" not in ds[c].dims and c != "time"]
                coords[group] = ds[keep].reset_coords()
            for name, da in ds.data_vars.items():
                ma = da.data
                own = tuple(d for d in da.dims if d != "time")
                key = (group, str(name))
                var = variables.setdefault(
                    key, _Var(dims=own, attrs=dict(da.attrs), levels={}, spatial=da.shape[-2:])
                )
                level_dims = own[:-2]
                level_values = [np.asarray(ds[d].values) for d in level_dims]
                for d, v in zip(level_dims, level_values):
                    seen = var.levels.get(d)
                    var.levels[d] = v if seen is None else np.union1d(seen, v)
                manifest = ma.manifest
                paths = np.asarray(manifest._paths)
                offsets = np.asarray(manifest._offsets)
                lengths = np.asarray(manifest._lengths)
                # Chunk grid is (time=1, levels..., 1, 1).
                grid = paths.reshape(paths.shape[1:-2]) if paths.ndim > 3 else paths.reshape(())
                for index in np.ndindex(grid.shape):
                    full = (0, *index, 0, 0)
                    path = str(paths[full])
                    if not path:
                        continue
                    level_key = tuple(level_values[i][index[i]].item() for i in range(len(index)))
                    var.cells[(step, level_key)] = (path, int(offsets[full]), int(lengths[full]))
    return variables, coords


# ---------------------------------------------------------------------------- schema


@dataclass
class _Schema:
    """What a store holds: per variable its dims, level axes and metadata."""

    arrays: Dict[Tuple[str, str], dict]
    levels: Dict[Tuple[str, str], np.ndarray]  # (group, level dim) -> values


def _store_schema(repo) -> _Schema | None:
    try:
        root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    except Exception:  # noqa: BLE001 - no group yet: an empty store
        return None
    arrays, levels = {}, {}

    def walk(group, path):
        for name, array in group.arrays():
            dims = tuple(array.metadata.dimension_names or ())
            if dims[:2] == ("init_time", "step"):
                arrays[(path, name)] = {
                    "dims": dims[2:],
                    "attrs": dict(array.attrs),
                    "spatial": tuple(array.shape[-2:]),
                }
            elif len(dims) == 1 and dims[0] == name and name not in ("init_time", "step"):
                levels[(path, name)] = np.asarray(array[:])
        for name, sub in group.groups():
            walk(sub, f"{path.rstrip('/')}/{name}")

    walk(root, "/")
    return _Schema(arrays=arrays, levels=levels) if arrays else None


def _misfits(variables: Dict[Tuple[str, str], _Var], schema: _Schema) -> List[str]:
    """What of the run ``schema`` cannot hold, which forces a new generation.

    A variable the store has never seen is *not* a misfit: it is added to the store, with
    every earlier run missing (see ``NestVirtualProvider._extend``). Only a change to
    something already stored is: a variable on other dimensions, a different grid, or
    levels outside an axis the store already has.
    """
    out = []
    spatial = {meta["spatial"] for meta in schema.arrays.values()}
    for key, var in variables.items():
        stored = schema.arrays.get(key)
        if stored is None:
            if spatial and var.spatial not in spatial:
                out.append(f"{key[0]}/{key[1]}: grid {var.spatial} vs {sorted(spatial)}")
            for dim, values in var.levels.items():
                have = schema.levels.get((key[0], dim))
                if have is not None and not np.isin(values, have).all():
                    out.append(f"{key[0]}/{key[1]}: {dim} values outside the store's axis")
            continue
        if stored["dims"] != var.dims or stored["spatial"] != var.spatial:
            out.append(f"{key[0]}/{key[1]}: dims {var.dims} vs {stored['dims']}")
            continue
        for dim, values in var.levels.items():
            have = schema.levels.get((key[0], dim))
            if have is None or not np.isin(values, have).all():
                out.append(f"{key[0]}/{key[1]}: {dim} values outside the store's axis")
    return out


# ---------------------------------------------------------------------------- writing


def _manifest_array(
    name: str,
    dims: Tuple[str, ...],
    attrs: dict,
    spatial: Tuple[int, int],
    steps: Tuple[int, ...],
    level_axes: List[np.ndarray],
    cells: Dict[Tuple[int, Tuple[Any, ...]], Tuple[str, int, int]],
    runs: int = 1,
):
    """``(init_time=runs, step, levels..., y, x)``: every message where it belongs, gaps
    empty. ``cells`` are placed in the first run; ``runs > 1`` with no cells gives the
    all-missing history a newly seen variable is given for the runs before it."""
    from gribberish.virtualizarr.parser import _gribberish_codecs
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from virtualizarr.manifests.utils import create_v3_array_metadata

    level_sizes = [len(a) for a in level_axes]
    grid = (runs, len(steps), *level_sizes, 1, 1)
    paths = np.full(grid, "", dtype=np.dtypes.StringDType())
    offsets = np.zeros(grid, dtype=np.uint64)
    lengths = np.zeros(grid, dtype=np.uint64)
    step_index = {s: i for i, s in enumerate(steps)}
    level_index = [
        {v.item() if hasattr(v, "item") else v: i for i, v in enumerate(a)} for a in level_axes
    ]
    for (step, level_key), (path, offset, length) in cells.items():
        if step not in step_index:
            continue
        try:
            idx = tuple(level_index[i][v] for i, v in enumerate(level_key))
        except KeyError:
            continue  # a level outside the store's axis; checked by _fits before writing
        cell = (0, step_index[step], *idx, 0, 0)
        paths[cell], offsets[cell], lengths[cell] = path, offset, length
    shape = (runs, len(steps), *level_sizes, *spatial)
    metadata = create_v3_array_metadata(
        shape=shape,
        data_type=np.dtype("float64"),
        chunk_shape=(1, 1, *([1] * len(level_sizes)), *spatial),
        fill_value=float("nan"),
        codecs=_gribberish_codecs(name, north_up=True),
        attributes=attrs,
        dimension_names=("init_time", "step", *dims),
    )
    manifest = ChunkManifest.from_arrays(paths=paths, offsets=offsets, lengths=lengths)
    return ManifestArray(metadata=metadata, chunkmanifest=manifest)


class NestVirtualProvider:
    """Reference runs of one :class:`NestProduct` into its generational virtual store."""

    append_dim = "init_time"

    def __init__(self, product: NestProduct | str, config: Config | None = None):
        self.product = PRODUCTS[product] if isinstance(product, str) else product
        self.name = self.product.name
        self._config = config
        self._repos: Dict[str, Any] = {}

    @property
    def config(self) -> Config:
        return self._config if self._config is not None else get_config()

    @property
    def store_path(self) -> str:
        return self.config.store_path(self.product.store_prefix)

    def repo(self, prefix: str):
        if prefix not in self._repos:
            self._repos[prefix] = open_virtual_repo(
                prefix,
                virtual_buckets=f"s3://{self.product.bucket}",
                split_dim="init_time",
                split_size=self.product.split_size,
                config=self.config,
            )
        return self._repos[prefix]

    def generations(self) -> List[str]:
        """The base store and every dated generation of it that exists, oldest first.

        Listed read-only: probing a name by opening it would create the store.
        """
        import re

        import fsspec

        base = self.product.store_prefix
        parent, _, name = base.rpartition("/")
        stem = name.removesuffix(".icechunk")
        pattern = re.compile(rf"^{re.escape(stem)}_(\d{{8}})(?:_\d+)?\.icechunk$")
        try:
            fs, path = fsspec.core.url_to_fs(
                self.config.store_path(parent), **self.config.fsspec_storage_options()
            )
            fs.invalidate_cache(path)
            entries = [e.rstrip("/").rpartition("/")[2] for e in fs.ls(path, detail=False)]
        except (FileNotFoundError, OSError):
            entries = []
        dated = sorted(
            (pd.Timestamp(dt.datetime.strptime(m.group(1), "%d%m%Y")), e)
            for e in entries
            if (m := pattern.match(e))
        )
        return [base, *(f"{parent}/{e}" for _, e in dated)]

    def _existing_repo(self, prefix: str):
        """The repository at ``prefix`` if it holds anything, else None; never creates."""
        if prefix in self._repos:
            return self._repos[prefix]
        try:
            repo = open_virtual_repo(
                prefix,
                virtual_buckets=f"s3://{self.product.bucket}",
                split_dim="init_time",
                split_size=self.product.split_size,
                config=self.config,
                create=False,
            )
        except Exception:  # noqa: BLE001 - not there
            return None
        self._repos[prefix] = repo
        return repo

    def ledger(self, prefix: str) -> set[str]:
        repo = self._existing_repo(prefix)
        if repo is None:
            return set()
        try:
            root = zarr.open_group(repo.readonly_session("main").store, mode="r")
        except Exception:  # noqa: BLE001 - created but nothing committed
            return set()
        return set(root.attrs.get(LEDGER_ATTR, []))

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Runs in ``desired`` that no generation lists."""
        done: set[str] = set()
        for prefix in self.generations():
            done |= self.ledger(prefix)
        return [pd.Timestamp(t) for t in desired if pd.Timestamp(t).isoformat() not in done]

    def choose_generation(self, it: pd.Timestamp, variables) -> Tuple[str, _Schema | None]:
        """The newest generation whose schema covers the run, the base if nothing is
        stored yet, or a new dated generation when the run has fields none of them hold."""
        prefixes = self.generations()
        populated = []
        for prefix in prefixes:
            repo = self._existing_repo(prefix)
            schema = _store_schema(repo) if repo is not None else None
            if schema is not None:
                populated.append((prefix, schema))
        if not populated:
            return self.product.store_prefix, None
        reasons: List[str] = []
        for prefix, schema in reversed(populated):
            misfits = _misfits(variables, schema)
            if not misfits:
                return prefix, schema
            reasons = reasons or misfits
        if len(prefixes) >= MAX_GENERATIONS:
            raise RuntimeError(
                f"{self.name}: {it} fits none of {len(prefixes)} generations; refusing to "
                "start another (is the schema splitting on noise?)"
            )
        prefix = prefix_for_date(self.product.store_prefix, it)
        logger.warning(
            f"{self.name}: {it} fits no generation ({len(reasons)} misfit(s), e.g. "
            f"{'; '.join(reasons[:3])}); starting {prefix}"
        )
        return prefix, None

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Reference one run. False when it is not published (or already stored)."""
        it = pd.Timestamp(it)
        if check_present and not self.missing_timesteps([it]):
            return False
        steps = _listed_steps(self.product, it)
        if not steps:
            logger.info(f"{self.name}: no files for {it}")
            return False
        registry = _registry(self.product.bucket)
        with ThreadPoolExecutor(self.product.parse_workers) as pool:
            results = list(
                pool.map(
                    lambda s: (s, _parse(self.product, self.product.url(it, s), registry)), steps
                )
            )
        parsed = {s: groups for s, groups in results if groups}
        if not parsed:
            return False
        variables, coords = _collect(parsed)
        prefix, schema = self.choose_generation(it, variables)
        return self._write(prefix, schema, it, variables, coords)

    def _extend(self, session, schema: _Schema, variables, coords) -> None:
        """Add the run's never-seen variables to the store, missing for every earlier run.

        Done in the run's own session, before its append, so the store never holds a
        variable shorter than the rest along ``init_time``. A new group gets its step,
        level and spatial coordinates; a new level dimension in an existing group gets its
        axis. Updates ``schema`` in place, so the append that follows includes them.
        """
        new = sorted(k for k in variables if k not in schema.arrays)
        if not new:
            return
        any_group = next(iter(schema.arrays))[0]
        history = xr.open_zarr(
            session.store, group=any_group.lstrip("/") or None, consolidated=False
        )["init_time"].values
        steps = self.product.steps
        known_groups = {g for g, _ in schema.arrays}
        by_group: Dict[str, Dict[str, Any]] = {}
        new_levels: Dict[Tuple[str, str], np.ndarray] = {}
        for group, name in new:
            var = variables[(group, name)]
            axes = []
            for dim in var.dims[:-2]:
                axis = schema.levels.get((group, dim))
                if axis is None:
                    axis = new_levels.setdefault((group, dim), np.sort(var.levels[dim]))
                axes.append(axis)
            ma = _manifest_array(
                name, var.dims, var.attrs, var.spatial, steps, axes, {}, runs=len(history)
            )
            by_group.setdefault(group, {})[name] = (ma, ("init_time", "step", *var.dims))
            schema.arrays[(group, name)] = {
                "dims": var.dims,
                "attrs": var.attrs,
                "spatial": var.spatial,
            }
        schema.levels.update(new_levels)
        for group, data in sorted(by_group.items()):
            ds = xr.Dataset(
                {name: xr.Variable(dims, ma) for name, (ma, dims) in data.items()},
                coords={"init_time": history},
            )
            for (g, dim), values in new_levels.items():
                if g == group:
                    ds = ds.assign_coords({dim: (dim, values)})
            if group not in known_groups:
                ds = ds.assign_coords(step=("step", pd.to_timedelta(steps, unit="h").values))
                if group in coords:
                    extra = coords[group].drop_vars(
                        [v for v in coords[group].variables if v in ds.variables]
                    )
                    ds = ds.assign_coords(
                        {
                            k: extra[k]
                            for k in extra.variables
                            if set(extra[k].dims) <= set(SPATIAL) or not extra[k].dims
                        }
                    )
            ds.vz.to_icechunk(session.store, group=group.lstrip("/") or None, mode="a")
        logger.info(
            f"{self.name}: added {len(new)} variable(s) first seen in this run, e.g. "
            f"{', '.join(f'{g}/{n}' for g, n in new[:3])}"
        )

    def _write(self, prefix, schema, it, variables, coords) -> bool:
        steps = self.product.steps
        first = schema is None
        if first:
            # The run defines the store: its own level axes, sorted.
            arrays = {
                k: {"dims": v.dims, "attrs": v.attrs, "spatial": v.spatial}
                for k, v in variables.items()
            }
            levels = {}
            for (group, _), var in variables.items():
                for dim, values in var.levels.items():
                    have = levels.get((group, dim))
                    levels[(group, dim)] = np.sort(
                        values if have is None else np.union1d(have, values)
                    )
            schema = _Schema(arrays=arrays, levels=levels)

        repo = self.repo(prefix)
        session = repo.writable_session("main")
        if not first:
            self._extend(session, schema, variables, coords)

        by_group: Dict[str, Dict[str, Any]] = {}
        for (group, name), meta in schema.arrays.items():
            var = variables.get((group, name))
            level_dims = meta["dims"][:-2]
            axes = [schema.levels[(group, d)] for d in level_dims]
            by_group.setdefault(group, {})[name] = (
                _manifest_array(
                    name,
                    meta["dims"],
                    meta["attrs"],
                    meta["spatial"],
                    steps,
                    axes,
                    var.cells if var is not None else {},
                ),
                ("init_time", "step", *meta["dims"]),
            )
        missing = sorted(
            {f"{g}/{n}" for (g, n) in schema.arrays} - {f"{g}/{n}" for (g, n) in variables}
        )
        if missing:
            logger.debug(f"{self.name}: {it} lacks {len(missing)} stored variable(s); left missing")

        for group, data in sorted(by_group.items()):
            ds = xr.Dataset(
                {name: xr.Variable(dims, ma) for name, (ma, dims) in data.items()},
                coords={"init_time": [np.datetime64(it, "ns")]},
            )
            if first:
                ds = ds.assign_coords(step=("step", pd.to_timedelta(steps, unit="h").values))
                for (g, dim), values in schema.levels.items():
                    if g == group:
                        ds = ds.assign_coords({dim: (dim, values)})
                if group in coords:
                    extra = coords[group].drop_vars(
                        [v for v in coords[group].variables if v in ds.variables]
                    )
                    ds = ds.assign_coords(
                        {
                            k: extra[k]
                            for k in extra.variables
                            if set(extra[k].dims) <= set(SPATIAL) or not extra[k].dims
                        }
                    )
                ds.vz.to_icechunk(session.store, group=group.lstrip("/") or None)
            else:
                ds.vz.to_icechunk(
                    session.store, group=group.lstrip("/") or None, append_dim="init_time"
                )
        root = zarr.open_group(session.store, mode="a")
        root.attrs[LEDGER_ATTR] = sorted(set(root.attrs.get(LEDGER_ATTR, [])) | {it.isoformat()})
        session.commit(f"{self.name}: run {it:%Y-%m-%dT%H}")
        logger.info(f"{self.name}: referenced run {it} into {prefix.rsplit('/', 1)[-1]}")
        return True
