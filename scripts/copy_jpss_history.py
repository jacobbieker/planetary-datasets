"""Copy JPSS history out of the old per-product stores into ``bkr/polar/jpss.icechunk``.

The JPSS products moved into one store, a group per product with every spacecraft in it
(see :class:`~planetary_datasets.providers.polar._jpss.JpssProductProvider`). The lanes
fill each group from NOAA, but some history now exists only in the old stores:

==============  ===============================================  =========================
Group           Old stores (read only)                           Default window
==============  ===============================================  =========================
``atms``        ``bkr/polar/jpss_atms.icechunk``                 2022-11-07 - 2025-08-01
``atms_sfr``    ``bkr/polar/jpss_atms_sfr_{n20,n21,snpp}``       2026-07-17 - group start
``mirs_img``    ``bkr/polar/jpss_mirs_img_{n20,n21,snpp}``       2026-07-17 - group start
``mirs_snd``    ``bkr/polar/jpss_mirs_snd_{n20,n21,snpp}``       2026-07-17 - group start
``viirs_mband`` ``bkr/polar/jpss_viirs_mband_{n20,n21,snpp}``    2026-07-17 - group start
==============  ===============================================  =========================

"Group start" is the earliest ``time`` the group held before the copy first wrote to it;
NOAA deleted the rolling products before then, so the old stores are their only copy.
The first run records it as a repository tag, ``copy-jpss-history.<group>.end.<time>``,
which later runs read back: after a partial copy the group's earliest ``time`` is a
copied row, not the lane's start. The tag touches nothing the lanes write. From 2025-08-01 the ATMS
lane re-downloads the SDRs itself.

**What a row becomes.** Each old row (one granule along ``time``) becomes one row along
``granule``, exactly as the lane builds it: the id is :func:`granule_id` of its
spacecraft and its sensing start to a tenth of a second; ``time`` is recomputed as the
middle of ``start_time`` - ``end_time`` (the old rolling stores rounded it to the second;
when either end is missing the old ``time`` is kept); ``platform_name`` is the canonical
spacecraft name. Variables keep their names, packing and encodings because the rows go
through the providers' own renaming and writer (``JpssProductProvider.write_to_icechunk``
into the group). Old stores that were renamed in place are mapped back to source names
first, through each array's ``source_name`` attribute. A row whose sensing start is
missing gets an id from its ``time`` less half a nominal granule, logged.

The old ATMS store may have lost NOAA-20/NOAA-21 granules whose times coincided; what is
there is copied, nothing is reconstructed.

**Slices and commits.** The old ``time`` axis is walked forward in slices of
``--slice-hours``. Each slice reads only the rows it covers (lazily; VIIRS at one row per
dask chunk, so memory follows the dask threads, not the slice), takes the rows of every
spacecraft together, drops the ids already in the group, sorts by id and writes them in
one commit. A killed run loses at most its open slice; re-running skips everything
already copied, so the script is resumable and idempotent.

**The group ends up unsorted.** The copied rows are older than the lane's, and are
appended after them, so the group's ``granule`` axis (and its ``time``) is no longer in
order once this has run. Each slice is in order within itself. Sorting the axis is a
separate decision, and this script never does it.

**One writer per group.** Each invocation writes one group and assumes nothing else
writes it: stop that product's lane first. Other groups' lanes may keep running.
``--max-hours`` / ``--until`` end a run between commits, before a slice that would not
finish inside the budget (judged from the previous slice), so the copy can be
interleaved with a lane that has to keep up with NOAA's 90-day deletion.

The old stores are opened read only and never created, modified or deleted.

Usage::

    python scripts/copy_jpss_history.py --product mirs_img --dry-run
    python scripts/copy_jpss_history.py --product viirs_mband --max-hours 4
    python scripts/copy_jpss_history.py --product atms --start 2023-01-01 --end 2023-02-01
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common import naming
from planetary_datasets.common.dataset import coords_match
from planetary_datasets.common.store import STORE_READ_ERRORS, holds_data
from planetary_datasets.config import get_config
from planetary_datasets.providers.polar._jpss import (
    PLATFORM_NAMES,
    JpssProductProvider,
    granule_id,
)

#: Old-store spacecraft suffixes.
SAT_SUFFIXES = ("n20", "n21", "snpp")

#: Spellings of each spacecraft seen in ``platform_name``, normalised (upper case, no
#: separators), to the code the lanes use.
PLATFORM_ALIASES: Dict[str, str] = {
    "NOAA21": "n21",
    "J02": "n21",
    "JPSS2": "n21",
    "N21": "n21",
    "NOAA20": "n20",
    "J01": "n20",
    "JPSS1": "n20",
    "N20": "n20",
    "SUOMINPP": "snpp",
    "NPP": "snpp",
    "SNPP": "snpp",
}

#: Variables the conversion rebuilds rather than copies.
GRANULE_VARIABLES = ("time", "start_time", "end_time", "platform_name")


class MigrationError(RuntimeError):
    """The old rows cannot be written into the group without changing them."""


@dataclass(frozen=True)
class Product:
    """One group and where its history comes from.

    Attributes:
        provider: Builds the group's provider; its writer and encodings are reused.
        sources: Old store prefixes, each with the spacecraft it holds (None: several).
        start: Default first ``time`` copied.
        end: Default end (exclusive); None for the group's earliest stored ``time``.
        slice_hours: Default slice length.
        instrument: For the group attributes when the copy creates the group.
        nominal_seconds: Nominal granule length, for an id when the start is missing.
    """

    provider: Callable[[], JpssProductProvider]
    sources: Tuple[Tuple[str, Optional[str]], ...]
    start: str
    end: Optional[str]
    slice_hours: float
    instrument: str
    nominal_seconds: float


def _provider(module: str, cls: str) -> Callable[[], JpssProductProvider]:
    def build() -> JpssProductProvider:
        import importlib

        found = importlib.import_module(f"planetary_datasets.providers.polar.{module}")
        return getattr(found, cls)()

    return build


def _per_satellite(base: str) -> Tuple[Tuple[str, Optional[str]], ...]:
    return tuple((f"bkr/polar/{base}_{sat}.icechunk", sat) for sat in SAT_SUFFIXES)


#: First partition of the rolling Dagster assets; nothing older was ever stored.
ROLLING_START = "2026-07-17"

#: Tag (with the group and a time appended) recording where a group's lane rows begin.
BOUNDARY_TAG = "copy-jpss-history"
#: How the time is spelled in that tag.
BOUNDARY_FORMAT = "%Y%m%dT%H%M%S.%f"

PRODUCTS: Dict[str, Product] = {
    "atms": Product(
        _provider("jpss_atms", "JpssAtmsProvider"),
        (("bkr/polar/jpss_atms.icechunk", None),),
        "2022-11-07",
        "2025-08-01",
        6.0,
        "ATMS",
        31.6,
    ),
    "atms_sfr": Product(
        _provider("jpss_atms_l2", "JpssAtmsSfrProvider"),
        _per_satellite("jpss_atms_sfr"),
        ROLLING_START,
        None,
        6.0,
        "ATMS",
        31.6,
    ),
    "mirs_img": Product(
        _provider("jpss_atms_l2", "JpssMirsImgProvider"),
        _per_satellite("jpss_mirs_img"),
        ROLLING_START,
        None,
        6.0,
        "ATMS",
        31.6,
    ),
    "mirs_snd": Product(
        _provider("jpss_atms_l2", "JpssMirsSndProvider"),
        _per_satellite("jpss_mirs_snd"),
        ROLLING_START,
        None,
        6.0,
        "ATMS",
        31.6,
    ),
    "viirs_mband": Product(
        _provider("jpss_viirs", "JpssViirsMbandProvider"),
        _per_satellite("jpss_viirs_mband"),
        ROLLING_START,
        None,
        1.0,
        "VIIRS",
        85.5,
    ),
}


def satellite_of(platform: str, hint: Optional[str] = None) -> str:
    """The lanes' spacecraft code (``n20``, ``n21``, ``snpp``) for a ``platform_name``.

    Raises:
        MigrationError: The name is not a JPSS spacecraft, or contradicts the store's.
    """
    key = "".join(ch for ch in str(platform).upper() if ch.isalnum())
    satellite = PLATFORM_ALIASES.get(key)
    if satellite is None and not key and hint is not None:
        satellite = hint
    if satellite is None:
        raise MigrationError(f"unknown JPSS platform_name {platform!r}")
    if hint is not None and satellite != hint:
        raise MigrationError(f"platform_name {platform!r} in the {hint} store")
    return satellite


# ------------------------------------------------------------------ old stores


def _to_source_names(ds: xr.Dataset, prefix: str) -> xr.Dataset:
    """``ds`` with the names its old store's renames gave it mapped back to source names.

    Arrays renamed in place carry their source name in ``source_name``; dimensions are
    mapped back through the old store's dimension table.
    """
    renames = {}
    for name, var in ds.variables.items():
        source = var.attrs.get(naming.SOURCE_NAME_ATTR)
        if source and source != name and name not in ds.dims:
            renames[name] = source
    dims = {new: old for old, new in naming.store_dims(prefix).items() if new in ds.dims}
    if renames:
        ds = ds.rename_vars(renames)
    if dims:
        ds = ds.rename_dims(dims)
        ds = ds.rename_vars({new: old for new, old in dims.items() if new in ds.variables})
    for var in ds.variables.values():
        var.attrs.pop(naming.SOURCE_NAME_ATTR, None)
    return ds


class OldStore:
    """One old store, opened read only, with its ``time`` axis in memory.

    Attributes:
        prefix: Store prefix.
        satellite: Spacecraft the store holds, or None for a combined store.
        ds: The store, lazily, with source variable names.
    """

    def __init__(self, prefix: str, satellite: Optional[str]):
        """Open ``prefix`` read only (it is never created).

        Raises:
            FileNotFoundError: The store does not exist.
        """
        config = get_config()
        try:
            repo = icechunk.Repository.open(config.icechunk_storage(prefix))
        except icechunk.IcechunkError as exc:
            raise FileNotFoundError(f"no store at {config.store_path(prefix)}: {exc}") from exc
        self.prefix = prefix
        self.satellite = satellite
        raw = xr.open_zarr(
            repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
        )
        self.ds = _to_source_names(raw, prefix)
        self.times = self.ds["time"].values.astype("datetime64[ns]")
        self.order = np.argsort(self.times, kind="stable")
        self.sorted_times = self.times[self.order]

    def rows(self, start: pd.Timestamp, end: pd.Timestamp) -> np.ndarray:
        """Positions of the rows with ``start <= time < end``, ascending."""
        lo = np.searchsorted(self.sorted_times, np.datetime64(start), side="left")
        hi = np.searchsorted(self.sorted_times, np.datetime64(end), side="left")
        return np.sort(self.order[lo:hi])

    def granule_info(self, rows: np.ndarray, nominal_seconds: float) -> pd.DataFrame:
        """Per-row id, spacecraft and rebuilt times for ``rows``, without the swath data."""
        old_time = self.times[rows]
        missing = np.full(len(rows), np.datetime64("NaT"), dtype="datetime64[ns]")

        def column(name: str) -> np.ndarray:
            if name not in self.ds:
                return missing
            return self.ds[name].isel(time=rows).values.astype("datetime64[ns]")

        if "platform_name" in self.ds:
            platforms = [str(p) for p in self.ds["platform_name"].isel(time=rows).values]
        else:
            platforms = [""] * len(rows)
        start = column("start_time")
        end = column("end_time")
        both = ~(np.isnat(start) | np.isnat(end))
        mid = np.where(both, start + (end - start) / 2, old_time)
        half = np.timedelta64(int(nominal_seconds * 5e8), "ns")
        id_start = np.where(np.isnat(start), old_time - half, start)
        if (~both).any():
            logger.warning(
                f"{self.prefix}: {int((~both).sum())} row(s) lack a sensing start or end; "
                "their time is the stored one and their id is built from it"
            )
        sats = [satellite_of(p, self.satellite) for p in platforms]
        return pd.DataFrame(
            {
                "row": rows,
                "satellite": sats,
                "granule": [granule_id(s, pd.Timestamp(t)) for s, t in zip(sats, id_start)],
                "time": mid,
                "start_time": start,
                "end_time": end,
            }
        )


# ------------------------------------------------------------------ conversion


def convert(old: OldStore, info: pd.DataFrame) -> xr.Dataset:
    """The rows in ``info`` of an old store, on the ``granule`` axis as the lanes write."""
    ds = old.ds.isel(time=info["row"].to_numpy())
    ds = ds.drop_vars([v for v in GRANULE_VARIABLES if v in ds.variables])
    ds = ds.rename_dims({"time": "granule"})
    ds = ds.assign_coords(granule=np.array(info["granule"].tolist(), dtype=object))
    dim = "granule"
    ds["time"] = xr.DataArray(info["time"].to_numpy().astype("datetime64[ns]"), dims=dim)
    ds["start_time"] = xr.DataArray(
        info["start_time"].to_numpy().astype("datetime64[ns]"), dims=dim
    )
    ds["end_time"] = xr.DataArray(info["end_time"].to_numpy().astype("datetime64[ns]"), dims=dim)
    ds["platform_name"] = xr.DataArray(
        np.array([PLATFORM_NAMES[s] for s in info["satellite"]], dtype=object), dims=dim
    )
    return ds


def source_packing_of(ds: xr.Dataset) -> Dict[str, dict]:
    """The integer packing each variable of an old store carries, by source name."""
    from planetary_datasets.providers.polar.jpss_atms_l2 import source_packing

    out = {}
    for name, var in ds.data_vars.items():
        found = source_packing(var.variable)
        if found is not None:
            out[str(name)] = found
    return out


def _clear_encodings(ds: xr.Dataset) -> xr.Dataset:
    """Drop the old store's encodings, so the group's (or the provider's) apply."""
    for var in ds.variables.values():
        var.encoding = {}
    return ds


# ------------------------------------------------------------------ the copy


@dataclass
class Totals:
    """Counts over a run."""

    rows: int = 0
    present: int = 0
    copied: int = 0
    duplicates: int = 0
    slices: int = 0
    bytes: float = 0.0


class Copier:
    """Copies one product's history into its group.

    Args:
        product: Which group.
        start: First old ``time`` copied.
        end: End (exclusive).
        slice_hours: Slice length.
        deadline: ``clock()`` value after which no new slice is started; None for none.
        clock: Seconds on a monotonic clock; replaceable for tests.
    """

    def __init__(
        self,
        product: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        slice_hours: Optional[float] = None,
        deadline: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        """Open the group's provider and every old store that exists."""
        self.spec = PRODUCTS[product]
        self.product = product
        self.provider = self.spec.provider()
        self.repo = self.provider.get_icechunk_repo()
        self.olds: List[OldStore] = []
        for prefix, satellite in self.spec.sources:
            try:
                self.olds.append(OldStore(prefix, satellite))
            except FileNotFoundError as exc:
                logger.warning(f"{product}: skipping {prefix}: {exc}")
        if not self.olds:
            raise MigrationError(f"{product}: none of the old stores exist")
        self.group = self._group()
        self.existing = self._existing_ids()
        self.start = pd.Timestamp(start if start is not None else self.spec.start)
        self.end = self._end(end)
        self.slice = pd.Timedelta(slice_hours or self.spec.slice_hours, "h")
        self.deadline = deadline
        self.clock = clock
        self.check_compatible()

    # .............................................................. the group

    def _group(self) -> Optional[xr.Dataset]:
        try:
            return xr.open_zarr(
                self.repo.readonly_session("main").store,
                group=self.provider.group,
                consolidated=False,
                decode_timedelta=True,
            )
        except STORE_READ_ERRORS:
            if holds_data(self.repo, self.provider.group):
                raise
            return None

    def _existing_ids(self) -> set:
        if self.group is None or "granule" not in self.group.coords:
            return set()
        return {str(v) for v in self.group["granule"].values}

    @property
    def boundary_tag_prefix(self) -> str:
        """Prefix of the repository tag recording where the group's lane rows begin."""
        return f"{BOUNDARY_TAG}.{self.provider.group}.end."

    def recorded_boundary(self) -> Optional[pd.Timestamp]:
        """The boundary an earlier run recorded for this group, if any."""
        for tag in self.repo.list_tags():
            if tag.startswith(self.boundary_tag_prefix):
                stamp = tag[len(self.boundary_tag_prefix) :]
                return pd.Timestamp(pd.to_datetime(stamp, format=BOUNDARY_FORMAT))
        return None

    def record_boundary(self) -> None:
        """Tag the repository with the default end, once, before the first write.

        A tag rather than anything in the group: it touches no array or attribute the
        lane writes, so it cannot conflict with or be overwritten by the lane's commits.
        """
        if self._unrecorded is None:
            return
        name = f"{self.boundary_tag_prefix}{self._unrecorded.strftime(BOUNDARY_FORMAT)}"
        try:
            self.repo.create_tag(name, self.repo.lookup_branch("main"))
        except icechunk.IcechunkError as exc:
            if self.recorded_boundary() is None:
                raise
            logger.debug(f"{self.product}: boundary already recorded ({exc})")
        logger.info(f"{self.product}: recorded the group's lane start as tag {name}")
        self._unrecorded = None

    def _end(self, end: Optional[pd.Timestamp]) -> pd.Timestamp:
        """The end given, the product's, or where the lane's rows in the group begin.

        That is the group's earliest ``time`` *before the copy wrote anything*. Read
        afresh after a partial copy it would be the earliest copied row, and a resumed run
        would find nothing to do; so the first run records it as a repository tag
        (:meth:`record_boundary`) and later runs read it back.

        Raises:
            MigrationError: There is no end to default to.
        """
        self._unrecorded: Optional[pd.Timestamp] = None
        if self.spec.end is not None:
            return pd.Timestamp(end if end is not None else self.spec.end)
        boundary = self.recorded_boundary()
        if boundary is None and self.group is not None and "time" in self.group:
            # Recorded even when this run is given an explicit end: whatever it copies
            # makes the group's earliest time a copied row from then on.
            boundary = self._unrecorded = pd.Timestamp(np.nanmin(self.group["time"].values))
        elif boundary is None and end is not None:
            # The copy creates the group, so no lane start is known; the given end is the
            # only boundary a later default-end run can safely stop at.
            self._unrecorded = pd.Timestamp(end)
        if end is not None:
            return pd.Timestamp(end)
        if boundary is None:
            raise MigrationError(f"{self.product}: the group is empty; give --end")
        return boundary

    def slices(self) -> Iterator[Tuple[pd.Timestamp, pd.Timestamp]]:
        """``[start, end)`` of every slice, oldest first."""
        cursor = self.start
        while cursor < self.end:
            yield cursor, min(cursor + self.slice, self.end)
            cursor += self.slice

    # .............................................................. checks

    def check_compatible(self) -> None:
        """Refuse to copy from old stores the group would re-quantise or misalign.

        Every old store's packing and static coordinates are compared, before anything is
        written, with the group's, or, for a group the copy creates, with each other.

        Raises:
            MigrationError: An old store is packed differently or has other statics.
        """
        from planetary_datasets.providers.polar.jpss_atms_l2 import (
            STATIC_COORDS,
            JpssNdeL2Provider,
            same_packing,
        )

        if not isinstance(self.provider, JpssNdeL2Provider):
            return
        reference, statics = self.provider.stored_reference()
        for old in self.olds:
            ds = old.ds
            packing = source_packing_of(ds)
            for name, found in packing.items():
                if name in reference and not same_packing(reference[name], found):
                    raise MigrationError(
                        f"{old.prefix}: {name} is packed as {found}, the group as "
                        f"{reference[name]}; copying would re-quantise it"
                    )
                reference.setdefault(name, found)
            present = [c for c in STATIC_COORDS if c in ds.coords]
            if statics is None:
                statics = ds[present].coords.to_dataset() if present else None
                continue
            ok, bad = coords_match(statics, ds, STATIC_COORDS)
            if not ok:
                raise MigrationError(f"{old.prefix}: static coordinate {bad} differs")
        # The provider's encoding_for applies this packing if the copy creates the group.
        self.provider._packing = reference

    def _match_group_variables(self, ds: xr.Dataset) -> xr.Dataset:
        """Drop variables the group does not hold; refuse if it holds ones we lack."""
        if self.group is None:
            return ds
        stored = set(map(str, self.group.data_vars))
        have = set(map(str, ds.data_vars))
        missing = stored - have
        if missing:
            raise MigrationError(
                f"{self.product}: the old rows lack {sorted(missing)}, which the group holds"
            )
        extra = sorted(have - stored)
        if extra:
            logger.warning(f"{self.product}: dropping {extra}, which the group does not hold")
            ds = ds.drop_vars(extra)
        return ds

    def _attrs(self, ds: xr.Dataset) -> dict:
        if self.group is not None:
            return dict(self.group.attrs)
        kept = {
            k: v
            for k, v in ds.attrs.items()
            if "platform" not in k and k not in ("start_time", "end_time", "source")
        }
        return {**kept, **self.provider.group_attrs(self.spec.instrument)}

    # .............................................................. one slice

    def plan_slice(
        self, start: pd.Timestamp, end: pd.Timestamp, totals: Totals
    ) -> List[Tuple[OldStore, pd.DataFrame]]:
        """Rows of every old store in the slice that are not in the group yet."""
        planned = []
        seen: set = set()
        for old in self.olds:
            rows = old.rows(start, end)
            if rows.size == 0:
                continue
            info = old.granule_info(rows, self.spec.nominal_seconds)
            totals.rows += len(info)
            present = info["granule"].isin(self.existing)
            totals.present += int(present.sum())
            info = info[~present]
            dup = info["granule"].duplicated() | info["granule"].isin(seen)
            if dup.any():
                logger.warning(
                    f"{old.prefix}: {int(dup.sum())} row(s) repeat a granule id in "
                    f"{start} - {end}; the first is kept"
                )
                totals.duplicates += int(dup.sum())
                info = info[~dup]
            seen.update(info["granule"])
            if len(info):
                planned.append((old, info))
        return planned

    def build_slice(self, planned: List[Tuple[OldStore, pd.DataFrame]]) -> xr.Dataset:
        """One dataset of every planned row, sorted by id, encoded as the group expects."""
        converted = [convert(old, info) for old, info in planned]
        ds = (
            converted[0]
            if len(converted) == 1
            else xr.concat(
                converted,
                dim="granule",
                data_vars="minimal",
                coords="minimal",
                compat="override",
                join="exact",
            )
        )
        ds = _clear_encodings(ds.sortby("granule"))
        # Rows picked out of several old stores come in ragged dask chunks; even ones of
        # the group's chunk length line up with a new group's chunks, and the provider's
        # align_to_store shifts them onto an existing group's partial last chunk.
        ds = ds.chunk({"granule": self.provider.granule_chunk or 1})
        ds.attrs = self._attrs(converted[0])
        return ds

    def write_slice(self, ds: xr.Dataset) -> None:
        """Write one slice through the provider, in one commit.

        Raises:
            MigrationError: The writer refused it (variables or coordinates).
        """
        named = self.provider.prepare_names(ds, self.repo)
        named = self._match_group_variables(named)
        if not self.provider.write_to_icechunk(self.repo, named):
            raise MigrationError(
                f"{self.product}: the writer refused {ds.sizes['granule']} granule(s) "
                f"from {ds['granule'].values[0]}; see the error above"
            )
        if self.group is None:
            self.group = self._group()

    # .............................................................. the run

    def run(self, dry_run: bool = False) -> Totals:
        """Copy (or, with ``dry_run``, count) every slice; stop early on the deadline."""
        totals = Totals()
        began = self.clock()
        last = 0.0
        logger.info(
            f"{self.product}: {self.start} - {self.end} in {self.slice} slices from "
            f"{[o.prefix for o in self.olds]}; {len(self.existing)} granule(s) in the group"
        )
        for start, end in self.slices():
            if self.deadline is not None and self.clock() + last > self.deadline:
                logger.info(f"{self.product}: time budget reached before {start}; stopping")
                break
            started = self.clock()
            planned = self.plan_slice(start, end, totals)
            count = sum(len(info) for _, info in planned)
            if dry_run:
                for old, info in planned:
                    per_row = sum(
                        v.nbytes / max(old.ds.sizes["time"], 1)
                        for v in old.ds.data_vars.values()
                        if "time" in v.dims
                    )
                    totals.bytes += per_row * len(info)
                totals.copied += count
                continue
            if not count:
                continue
            ds = self.build_slice(planned)
            # Only once something is about to be written: until then the group is as the
            # lane left it, and a later run computes the same boundary afresh.
            self.record_boundary()
            nbytes = float(ds.nbytes)
            self.write_slice(ds)
            self.existing.update(str(v) for v in ds["granule"].values)
            totals.copied += count
            totals.slices += 1
            totals.bytes += nbytes
            last = self.clock() - started
            logger.info(
                f"{self.product}: {start} - {end}: {count} granule(s) in {last:.0f} s "
                f"({count / max(last, 1e-9):.1f}/s, {nbytes / 1e6 / max(last, 1e-9):.1f} MB/s "
                f"decoded); {totals.copied} copied, {totals.present} already there"
            )
        elapsed = self.clock() - began
        verb = "would copy" if dry_run else "copied"
        logger.info(
            f"{self.product}: {verb} {totals.copied} of {totals.rows} granule(s) "
            f"({totals.present} already in the group, {totals.duplicates} repeated ids, "
            f"{totals.bytes / 1e9:.1f} GB decoded) in {elapsed:.0f} s"
        )
        return totals


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point. Returns the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--product", required=True, choices=sorted(PRODUCTS))
    parser.add_argument("--start", help="first old time copied (default: per product)")
    parser.add_argument("--end", help="end, exclusive (default: per product / group start)")
    parser.add_argument("--slice-hours", type=float, help="hours of old rows per commit")
    parser.add_argument(
        "--max-hours", type=float, help="stop between commits after this many hours"
    )
    parser.add_argument("--until", help="stop between commits at this UTC time")
    parser.add_argument("--dry-run", action="store_true", help="count the work; write nothing")
    parser.add_argument(
        "--threads", type=int, default=8, help="dask threads reading and writing chunks"
    )
    args = parser.parse_args(argv)

    import dask

    deadline = None
    if args.max_hours is not None:
        deadline = time.monotonic() + args.max_hours * 3600
    if args.until is not None:
        until = pd.Timestamp(args.until)
        until = until.tz_localize("UTC") if until.tzinfo is None else until
        left = (until - pd.Timestamp.now(tz="UTC")).total_seconds()
        at = time.monotonic() + left
        deadline = at if deadline is None else min(deadline, at)

    # Scoped, so a caller of main() (a test) does not inherit the thread count.
    with dask.config.set(scheduler="threads", num_workers=args.threads):
        copier = Copier(
            args.product,
            start=pd.Timestamp(args.start) if args.start else None,
            end=pd.Timestamp(args.end) if args.end else None,
            slice_hours=args.slice_hours,
            deadline=deadline,
        )
        copier.run(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
