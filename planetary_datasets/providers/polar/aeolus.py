"""ESA Aeolus: L1B, L2A, L2B and L2C, every product of the mission, in one store.

Aeolus (2018-08 to 2023-07) carried ALADIN, a Doppler wind lidar. Its products are served
as NetCDF, one field group at a time, by VirES for Aeolus (``VIRES_TOKEN``), which reads
ESA's Baseline 16 archive:

``l1b`` (ALD_U_N_1B)
    Observations (~12 s, ~87 km along track) and the measurements within them (~0.4 s),
    with their Mie and Rayleigh signals and wind speeds before any correction.
``l2a`` (ALD_U_N_2A)
    Aerosol and cloud optical properties: backscatter, extinction and lidar ratio per
    range bin, from the standard (SCA) and Mie (MCA) algorithms, and their groupings.
``l2b`` (ALD_U_N_2B)
    The wind product: horizontal line-of-sight wind per range bin, Mie (cloudy/aerosol)
    and Rayleigh (clear air) results, with their groupings, profiles and errors.
``l2c`` (ALD_U_N_2C)
    L2B winds as used by ECMWF's assimilation, with the model background and analysis
    winds alongside.

Store layout (``bkr/polar/aeolus.icechunk``): every variable and dimension is prefixed by
its level (``l2b_mie_wind_result_wind_velocity``, ``l2b_mie_wind_data``). Each group's rows
grow along their own dimension, product after product; ``<level>_product`` has one row
per product (its name, baseline and processor version), and each group's rows carry
``<dim>_product_index`` into it, so product-local ids (wind result ids, grouping ids) are
read within their product. Range-bin dimensions (``array_24``, ...) are fixed.

How products are fetched. VirES filters most groups by time but returns some (the L2B/L2C
groupings and measurements) whole, whatever the window. The time-filtered groups are asked
for over consecutive windows, each from a product's start to the next product's start, so
no row is fetched twice; the whole-product groups over one second in the product's
middle, which returns that product alone. Products overlap their neighbours by ~30 s, and
VirES serves the overlap once: rows there are indexed to the window's product, so for
under 1% of rows a product-local id (a wind result's grouping id, say) can belong to the
previous product.

Values are copied as VirES serves them: read without CF decoding, floats narrowed only
where every value survives (:mod:`planetary_datasets.common.lossless`), losslessly
compressed. Times are seconds since 2000-01-01, decoded by xarray from their ``units``.

Environment: ``VIRES_TOKEN``.

Source: https://aeolus.services
"""

from __future__ import annotations

import json
import pathlib
import re
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, List

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import naming
from planetary_datasets.common.multidim import append_partition, pad_value
from planetary_datasets.common.store import STORE_READ_ERRORS

VIRES_URL = "https://aeolus.services/ows"
#: First and last days of Baseline 16 and end-of-life data.
MISSION_START = "2018-09-03"
MISSION_END = "2023-07-05"
#: Fields in viresclient's catalogue that the server no longer serves.
REJECTED = {
    "ALD_U_N_2A": {
        "ica_mask",
        "ICA_time_obs",
        "ICA_QC_flag",
        "ICA_filling_case",
        "ICA_extinction",
        "ICA_backscatter",
        "ICA_LOD",
    },
    "ALD_U_N_2B": {
        "mie_wind_result_background_high",
        "rayleigh_wind_result_background_high",
        "rayleigh_wind_result_scattering_ratio_method",
    },
}
#: Attribute of ``<level>_product_name`` listing the level's complete days.
DAYS_ATTR = "days_complete"
NO_SUCH_FIELD = re.compile(r"No such field '([^']+)'")
PRODUCT_TIME = re.compile(r"_(\d{8}T\d{6})(\d{3})?_(\d{8}T\d{6}|\d{9})_")


@dataclass(frozen=True)
class Level:
    """One Aeolus product level and how VirES serves its groups."""

    prefix: str
    collection: str
    #: Groups VirES returns whole whatever the window.
    whole: tuple[str, ...] = ()
    #: A small field that lists the day's products cheaply.
    listing: tuple[str, str] = ("observation_fields", "time")


LEVELS = {
    level.prefix: level
    for level in (
        Level("l1b", "ALD_U_N_1B"),
        Level("l2a", "ALD_U_N_2A", listing=("observation_fields", "L1B_centroid_time_obs")),
        Level(
            "l2b",
            "ALD_U_N_2B",
            whole=("measurement_fields", "mie_grouping_fields", "rayleigh_grouping_fields"),
            listing=("mie_grouping_fields", "mie_grouping_id"),
        ),
        Level(
            "l2c",
            "ALD_U_N_2C",
            whole=("measurement_fields", "mie_grouping_fields", "rayleigh_grouping_fields"),
            listing=("mie_grouping_fields", "mie_grouping_id"),
        ),
    )
}


def catalogue_fields(collection: str) -> dict[str, list[str]]:
    """Each group's fields, from viresclient's catalogue less what the server rejects."""
    import viresclient

    path = pathlib.Path(viresclient.__file__).parent / "_data" / "config_aeolus.json"
    groups = json.loads(path.read_text())["collections"][collection]
    rejected = REJECTED.get(collection, set())
    out = {group: [f for f in fields if f not in rejected] for group, fields in groups.items()}
    return {group: fields for group, fields in out.items() if fields}


def product_span(name: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """A product's start and stop from its name.

    L1B/L2A names carry a start with milliseconds and a duration in milliseconds
    (``..._20201002T105035026_005399993_...``); L2B/L2C a start and a stop.
    """
    match = PRODUCT_TIME.search(name)
    if match is None:
        raise ValueError(f"not an Aeolus product name: {name}")
    start = pd.Timestamp(match.group(1)) + pd.Timedelta(int(match.group(2) or 0), "ms")
    end = match.group(3)
    if "T" in end:
        return start, pd.Timestamp(end)
    return start, start + pd.Timedelta(int(end), "ms")


def sources(ds: xr.Dataset) -> list[tuple[str, ...]]:
    """The products a VirES response was built from, in the order its rows come."""
    raw = ds.attrs.get("Sources", [])
    if isinstance(raw, str):
        import ast

        raw = ast.literal_eval(raw)
    raw = np.asarray(raw, dtype=object)
    if raw.ndim == 1:
        raw = raw[np.newaxis]
    return [tuple(str(v) for v in row) for row in raw] if raw.size else []


def split_whole(ds: xr.Dataset, listed: list[str], product: str) -> xr.Dataset:
    """``product``'s rows of a whole-product answer that returned several products.

    Whole products come one after another in ``Sources`` order, and their ids (grouping
    ids, observation numbers) restart with each, so a product starts wherever an id
    goes down.

    Raises:
        ValueError: No id along a dimension resets once per product.
    """
    keep = listed.index(product)
    selections = {}
    for dim in ds.dims:
        if dim.startswith("array_") or not ds.sizes[dim]:
            continue
        cuts = None
        for variable in ds.data_vars.values():
            if variable.dims != (dim,) or variable.dtype.kind not in "iu":
                continue
            resets = np.flatnonzero(np.diff(variable.values.astype("int64")) < 0) + 1
            if len(resets) == len(listed) - 1:
                cuts = [0, *resets.tolist(), ds.sizes[dim]]
                break
        if cuts is None:
            raise ValueError(f"{product}: no id along {dim} splits {listed}")
        selections[dim] = slice(cuts[keep], cuts[keep + 1])
    return ds.isel(selections)


def _units() -> dict[str, str]:
    """Units by field name, from viresclient's catalogue: VirES files carry none."""
    import viresclient

    path = pathlib.Path(viresclient.__file__).parent / "_data" / "config_aeolus.json"
    units: dict[str, str] = {}
    for collection in json.loads(path.read_text())["collections"].values():
        for fields in collection.values():
            for name, spec in fields.items():
                if isinstance(spec, dict) and spec.get("uom"):
                    units.setdefault(name, spec["uom"])
    return units


def _read_raw(path: pathlib.Path) -> list[xr.Dataset]:
    """A VirES NetCDF file's groups, without CF decoding.

    Each gets the file's source products as ``Sources`` (from its ``history``, as
    viresclient does) and its fields' units.
    """
    import netCDF4

    with _NETCDF_LOCK:
        return _read_raw_locked(path, netCDF4)


#: netCDF4/HDF5 are not thread-safe, and products are fetched on several threads.
_NETCDF_LOCK = threading.Lock()
#: NetCDF's default fill for floats, as float32 and float64: an unwritten value.
NETCDF_DEFAULT_FILLS = (float(np.float32(9.969209968386869e36)), 9.969209968386869e36)


def _read_raw_locked(path: pathlib.Path, netCDF4) -> list[xr.Dataset]:
    with netCDF4.Dataset(path) as nc:
        groups = list(nc.groups)
        history = json.loads(nc.getncattr("history")) if "history" in nc.ncattrs() else {}
    listed = list(
        zip(
            history.get("inputFiles", []),
            history.get("baselines", []),
            history.get("software_vers", []),
        )
    )
    units = _units()
    out = []
    for group in groups or [None]:
        with xr.open_dataset(
            path,
            group=group,
            engine="netcdf4",
            mask_and_scale=False,
            decode_times=False,
            decode_timedelta=False,
        ) as opened:
            ds = opened.load()
        for name, variable in ds.data_vars.items():
            if name in units and "units" not in variable.attrs:
                variable.attrs["units"] = units[name]
            if variable.dtype.kind == "f" and "_FillValue" not in variable.attrs:
                # VirES leaves unset values at NetCDF's default fill without declaring it,
                # which xarray cannot decode as a time (l2a SCA_time_obs, for one).
                present = [f for f in NETCDF_DEFAULT_FILLS if np.any(variable.values == f)]
                if present:
                    variable.attrs["_FillValue"] = present[0]
        ds.attrs["Sources"] = listed
        out.append(ds)
    if not out:
        out.append(xr.Dataset(attrs={"Sources": listed}))
    return out


def _with_fields(ds: xr.Dataset, templates: dict[str, xr.DataArray]) -> xr.Dataset:
    """``ds`` with each field of ``templates`` it lacks added as fill, sized to ``ds``."""
    added = {}
    for name, like in templates.items():
        if name in ds:
            continue
        shape = tuple(ds.sizes.get(d, like.sizes[d]) for d in like.dims)
        fill = pad_value(like.attrs, like.dtype)
        dtype = object if like.dtype.kind in "OUST" else like.dtype
        added[name] = xr.Variable(like.dims, np.full(shape, fill, dtype=dtype), like.attrs)
    return ds.assign(added) if added else ds


class AeolusProvider(BaseProvider):
    """Every Aeolus product of one or more levels, a day of products per partition."""

    name = "aeolus"
    append_dim = "time"
    store_prefix = "bkr/polar/aeolus.icechunk"

    def __init__(self, config=None, levels: Iterable[str] | None = None, workers: int = 8):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            levels: Level prefixes to ingest; all of :data:`LEVELS` by default.
            workers: VirES requests in flight. It answers concurrent requests about as
                fast as one, so this is most of the throughput.
        """
        super().__init__(config=config)
        names = list(levels) if levels is not None else list(LEVELS)
        unknown = set(names) - set(LEVELS)
        if unknown:
            raise ValueError(f"unknown Aeolus levels: {sorted(unknown)}")
        self.levels = [LEVELS[n] for n in names]
        self.workers = workers
        self._missing: dict[tuple[str, str, str], set[str]] = {}
        self._missing_lock = threading.Lock()
        self._fields = {level.prefix: catalogue_fields(level.collection) for level in self.levels}
        #: Field names in more than one group of a level (L1B's observation and measurement
        #: groups share ``time``, positions, ...): those take their group as a prefix.
        self._shared = {}
        for level, groups in self._fields.items():
            counts: dict[str, int] = {}
            for fields in groups.values():
                for name in fields:
                    counts[name] = counts.get(name, 0) + 1
            self._shared[level] = {name for name, n in counts.items() if n > 1}

    # ------------------------------------------------------------------ VirES

    def _get(self, level: Level, group: str, fields: list[str], start, end) -> xr.Dataset:
        """One group of one level over ``[start, end)``, read without CF decoding.

        Which fields exist depends on the product's processing baseline (early L2C lacks
        dozens), and the server refuses a whole request over one it lacks. Each one it
        names is dropped and the request retried; what was missing is remembered per
        level, group and month, so the round trips are paid once.
        """
        from viresclient import AeolusRequest

        (token,) = self.config.credentials.require("vires_token")
        key = (level.prefix, group, f"{pd.Timestamp(start):%Y-%m}")
        with self._missing_lock:
            missing = set(self._missing.get(key, ()))
        wanted = [f for f in fields if f not in missing]
        last: Exception | None = None
        failures = 0
        while wanted:
            request = AeolusRequest(token=token, url=VIRES_URL)
            request.set_collection(level.collection)
            request.set_fields(**{group: wanted})
            try:
                data = request.get_between(
                    f"{pd.Timestamp(start):%Y-%m-%dT%H:%M:%S.%f}"[:-3] + "Z",
                    f"{pd.Timestamp(end):%Y-%m-%dT%H:%M:%S.%f}"[:-3] + "Z",
                    filetype="nc",
                    asynchronous=False,
                    show_progress=False,
                )
                break
            except Exception as exc:  # noqa: BLE001 - VirES fails transiently in many ways
                reason = f"{exc.__context__ or ''} {exc}"
                absent = NO_SUCH_FIELD.search(reason)
                if absent:
                    name = absent.group(1)
                    # A field the product lacks, or one a requested field depends on
                    # (L2A's sca_mask needs SCA_time_obs): drop it, or its dependants.
                    stem = name.split("_")[0].lower()
                    drop = (
                        [name]
                        if name in wanted
                        else [f for f in wanted if f.lower().startswith(stem + "_")]
                    )
                    if not drop:
                        logger.debug(f"{self.name}: {level.prefix} {group} unavailable ({name})")
                        return xr.Dataset()
                    for field in drop:
                        wanted.remove(field)
                    with self._missing_lock:
                        self._missing.setdefault(key, set()).update(drop)
                    continue
                last, failures = exc, failures + 1
                logger.debug(
                    f"{self.name}: {level.prefix} {group} attempt {failures}: {reason[:200]}"
                )
                if failures == 5:
                    raise RuntimeError(
                        f"{self.name}: {level.prefix} {group} failed: {reason[:300]}"
                    ) from last
        else:
            return xr.Dataset()
        parts = []
        with tempfile.TemporaryDirectory(dir=self.config.scratch_dir) as tmp:
            # A long request comes back split into several files; each keeps its data in
            # groups. viresclient's own reader decodes them, so they are read here raw.
            for index, content in enumerate(getattr(data, "contents", [data])):
                path = pathlib.Path(tmp) / f"vires_{index}.nc"
                content.to_file(str(path), overwrite=True)
                parts.extend(_read_raw(path))
        if not parts:
            return xr.Dataset()
        merged = xr.merge(parts, compat="no_conflicts", combine_attrs="drop_conflicts")
        merged.attrs["Sources"] = [s for part in parts for s in sources(part)]
        return merged

    def products(self, level: Level, day: pd.Timestamp) -> list[tuple[str, pd.Timestamp]]:
        """The level's products starting on ``day``, each with where its window ends.

        A window ends where the next product starts (or, for the last of the mission, at
        its own stop), so consecutive windows tile the time axis with no row in two.
        """
        group, field = level.listing
        listing = self._get(level, group, [field], day, day + pd.Timedelta(30, "h"))
        names = sorted({s[0] for s in sources(listing)}, key=lambda n: product_span(n)[0])
        out = []
        for this, following in zip(names, [*names[1:], None]):
            start, stop = product_span(this)
            if start.normalize() != day:
                continue
            end = product_span(following)[0] if following else stop + pd.Timedelta(1, "s")
            out.append((this, end))
        return out

    def fetch_product(self, level: Level, product: str, end: pd.Timestamp) -> xr.Dataset:
        """Every group of one product, prefixed by level.

        Time-filtered groups cover ``[product start, end)``; whole-product groups are asked
        for over a second in the product's middle, which returns that product alone.

        Raises:
            ValueError: A whole-product request returned some other product.
        """
        start, stop = product_span(product)
        middle = start + (stop - start) / 2
        p = level.prefix
        variables: dict[str, xr.Variable] = {}
        baseline = version = ""
        for group, fields in self._fields[level.prefix].items():
            if group in level.whole:
                ds = self._get(level, group, fields, middle, middle + pd.Timedelta(1, "s"))
                listed = [s[0] for s in sources(ds)]
                if listed and product not in listed:
                    # A product inside another (early in the mission, some last seconds):
                    # it has no rows of its own in this group.
                    continue
                if len(listed) > 1:
                    ds = split_whole(ds, listed, product)
            else:
                ds = self._get(level, group, fields, start, end)
            for name, b, v in sources(ds):
                if name == product:
                    baseline, version = b, v
            for name, variable in ds.data_vars.items():
                dims = tuple(f"{p}_{d}" for d in variable.dims)
                label = name
                if name in self._shared[p]:
                    label = f"{group.removesuffix('_fields')}_{name}"
                variables[f"{p}_{label}"] = xr.Variable(dims, variable.values, dict(variable.attrs))
            for dim in ds.dims:
                if dim.startswith("array_"):
                    continue
                variables[f"{p}_{dim}_product_index"] = xr.Variable(
                    (f"{p}_{dim}",), np.zeros(ds.sizes[dim], dtype="uint32")
                )
        for name, value in (("name", product), ("baseline", baseline), ("processor", version)):
            variables[f"{p}_product_{name}"] = xr.Variable(
                (f"{p}_product",), np.array([value], dtype=object)
            )
        return xr.Dataset(variables)

    # ------------------------------------------------------------------ state

    def _group(self) -> zarr.Group | None:
        try:
            store = self.get_icechunk_repo().readonly_session("main").store
            return zarr.open_group(store, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    def _names(self, group: zarr.Group | None, level: Level) -> zarr.Array | None:
        name = f"{level.prefix}_product_name"
        return group[name] if group is not None and name in group else None

    def stored_products(self, level: Level) -> list[str]:
        """Product names already stored for ``level``, in store order."""
        names = self._names(self._group(), level)
        return [str(v) for v in names[:]] if names is not None else []

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Mission days some level has not been recorded complete for."""
        group = self._group()
        done = {}
        for level in self.levels:
            names = self._names(group, level)
            done[level.prefix] = set(names.attrs.get(DAYS_ATTR, [])) if names is not None else set()
        first, last = pd.Timestamp(MISSION_START), pd.Timestamp(MISSION_END)
        return [
            day
            for day in desired
            if first <= day <= last
            and any(f"{day:%Y-%m-%d}" not in done[lvl.prefix] for lvl in self.levels)
        ]

    # ------------------------------------------------------------------ writing

    def _write(self, level: Level, datasets: list[xr.Dataset], first_index: int) -> None:
        """Append products end to end, each row indexed to its product."""
        p = level.prefix
        indexed = []
        for offset, ds in enumerate(datasets):
            updates = {
                name: (ds[name].dims, np.full(ds[name].shape, first_index + offset, dtype="uint32"))
                for name in ds.data_vars
                if name.endswith("_product_index")
            }
            indexed.append(ds.assign(updates))
        stacked = []
        dims = list(dict.fromkeys(d for ds in indexed for d in ds.dims if f"{p}_array_" not in d))
        for dim in dims:
            # Products of different baselines carry different fields, and one with no rows
            # in a group has none of its fields at all: stack the products that have the
            # dimension, each given the fields it lacks as fill.
            having = [ds for ds in indexed if dim in ds.dims]
            names = list(
                dict.fromkeys(n for ds in having for n in ds.data_vars if ds[n].dims[:1] == (dim,))
            )
            if names:
                templates = {n: next(ds[n] for ds in having if n in ds) for n in names}
                stacked.append(
                    xr.concat(
                        [_with_fields(ds, templates)[names] for ds in having],
                        dim=dim,
                        data_vars="all",
                    )
                )
        ds = xr.merge(stacked, compat="no_conflicts", combine_attrs="override")
        repo = self.get_icechunk_repo()
        ds = naming.prepare_for_store(ds, self.store_prefix, naming.stored_arrays(repo), raw=True)
        append_partition(
            repo,
            ds,
            None,
            fixed_dims=[d for d in ds.dims if f"{p}_array_" in d],
            label=f"{self.name} {p}",
        )

    def _mark_day(self, level: Level, day: pd.Timestamp) -> None:
        name = f"{level.prefix}_product_name"
        for attempt in range(1, 6):
            session = self.get_icechunk_repo().writable_session("main")
            group = zarr.open_group(session.store, mode="a")
            if name not in group:
                return
            days = set(group[name].attrs.get(DAYS_ATTR, []))
            if f"{day:%Y-%m-%d}" in days:
                return
            group[name].attrs[DAYS_ATTR] = sorted(days | {f"{day:%Y-%m-%d}"})
            try:
                session.commit(
                    f"{self.name} {level.prefix}: {day:%Y-%m-%d} complete",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return
            except (icechunk.ConflictError, icechunk.RebaseFailedError):
                if attempt == 5:
                    raise

    def run_level_day(self, level: Level, day: pd.Timestamp, pool: ThreadPoolExecutor) -> int:
        """Write the day's products of one level not yet stored. Returns how many."""
        listed = self.products(level, day)
        stored = self.stored_products(level)
        todo = [(n, end) for n, end in listed if n not in set(stored)]
        fetched = [
            ds
            for ds in pool.map(lambda item: self._safe_fetch(level, *item), todo)
            if ds is not None
        ]
        if fetched:
            self._write(level, fetched, len(stored))
        if {n for n, _ in listed} <= set(self.stored_products(level)):
            self._mark_day(level, day)
        return len(fetched)

    def _safe_fetch(self, level: Level, product: str, end: pd.Timestamp) -> xr.Dataset | None:
        try:
            return self.fetch_product(level, product, end)
        except Exception as exc:  # noqa: BLE001 - one bad product must not stop the day
            logger.warning(f"{self.name}: {product} failed: {exc}")
            return None

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Every level's products starting on the day ``it``. True if any were written."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        written = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for level in self.levels:
                n = self.run_level_day(level, day, pool)
                if n:
                    logger.info(f"{self.name}: {level.prefix} {day:%Y-%m-%d}: {n} product(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` fetches and writes level by level."""
        raise NotImplementedError("AeolusProvider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("AeolusProvider writes through run_partition")


__all__ = [
    "AeolusProvider",
    "LEVELS",
    "Level",
    "catalogue_fields",
    "split_whole",
    "product_span",
    "sources",
]
