"""Roll a store to a new generation when the upstream model changes shape.

Operational models get upgraded. A resolution change, a new diagnostic, a variable that
stops being published — any of these make the new data un-appendable to the store holding
the old, because :func:`~planetary_datasets.common.store.write_to_icechunk` requires the
grid and the variable set to match what is already there.

Before this, an upgrade meant every partition after it failed, and someone noticed weeks
later and hand-made a ``..._2.icechunk``. That is exactly what happened to the Met Office
global wave store, which is why ``metoffice_global_wave_2`` exists alongside
``metoffice_global_wave``. This module makes that automatic: a provider names a *base*
prefix, and the generation actually written to is chosen by matching the incoming data's
schema against the stores that already exist.

    bkr/metoffice/metoffice_global_wave.icechunk           <- the series as first created
    bkr/metoffice/metoffice_global_wave_09042025.icechunk  <- the model changed on 9 Apr 2025
    bkr/metoffice/metoffice_global_wave_20092025.icechunk  <- ... and again on 20 Sep 2025

A new store is named after the *day the change appears*, ``DDMMYYYY``, rather than after a
counter. The date is the one thing a reader actually needs: it lines the break up against a
model-upgrade announcement without opening either store, and it stays meaningful when
generations are discovered out of order by a backfill. Stores created under the older
``_2``/``_3`` naming are still found and still written to — see
:func:`existing_generations`.

The rules, in order:

1. The first generation whose schema *matches* the incoming data is written to. Matching by
   schema rather than by recency is what lets a model revert, or a late backfill of
   pre-upgrade data, land back in the generation it belongs to.
2. Otherwise a new store is created, named for the first timestamp in the data that did not
   fit anywhere.
3. A generation that exists but cannot be read is skipped rather than written to. A store
   left inconsistent by a half-finished append cannot be fingerprinted, and appending to it
   would pile more on top of the damage.

What counts as "the same schema" is deliberately narrow — see :func:`schema_fingerprint`.
It is the set of things ``write_to_icechunk`` would refuse a mismatch on, and nothing else,
so a change in an attribute or in the data itself never splits a store.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import re

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

#: ``20092025`` for 20 September 2025: the day the upstream changed shape, which is
#: what a new store is named after.
GENERATION_DATE_FORMAT = "%d%m%Y"

#: How many generations to consider before giving up. A model that has been upgraded this
#: many times is more likely to be a fingerprint bug splitting a store on every partition.
MAX_GENERATIONS = 20

#: Coordinates whose *values* are part of the schema, not just their length. Two 1440-point
#: longitude axes that start in different places are not the same grid, and a store holding
#: both would silently interleave them.
GRID_COORDS = ("latitude", "longitude", "lat", "lon", "depth", "x", "y", "step", "station")

#: Significant figures grid coordinates are rounded to before hashing.
#:
#: *Significant* figures, not decimal places, and that distinction is the whole point. A
#: float32 carries about seven significant digits, so the spacing between representable
#: values at a latitude of 80 is around 8e-6 — and two runs of the same grid, one written
#: from a file that stored -80.0390625 and one from a file that stored -80.03905487,
#: differ by exactly that. Rounding to six *decimal places* preserves the difference and
#: reads it as a regridding; rounding to six *significant figures* absorbs it, because it
#: scales with the magnitude the way the float's own precision does.
#:
#: Six is comfortably finer than any real change: on a latitude it is about 1e-4 degrees,
#: roughly ten metres, so two grids that agree to this are the same grid. Used for the
#: degenerate axes :func:`_float_axis_digest` cannot describe by extent and spacing.
COORD_SIGNIFICANT_FIGURES = 6

#: Significant figures kept of an axis's start, end and spacing.
#:
#: Five, not six, and the difference matters: the global wave axis ends at 359.82421875 in
#: one construction and 359.82369995 in the other, which already disagree in the sixth
#: figure. Five absorbs that while still separating any real change — on that axis it is a
#: tolerance of about 0.005 degrees, seventy times finer than the 0.35 degree cell.
COORD_AXIS_FIGURES = 5


def _split_prefix(base_prefix: str) -> tuple[str, str]:
    """Split ``a/b/name.icechunk`` into ``("a/b/name", ".icechunk")``."""
    stem, dot, suffix = base_prefix.rpartition(".")
    return (stem, f".{suffix}") if dot else (base_prefix, "")


def prefix_for_generation(base_prefix: str, generation: int) -> str:
    """The store prefix for a numbered generation, counting from one.

    The original naming, kept because stores created under it still exist and have to go
    on being found — ``metoffice_global_wave_2`` among them. New generations are named by
    :func:`prefix_for_date` instead.
    """
    if generation < 1:
        raise ValueError(f"generation must be >= 1, got {generation}")
    if generation == 1:
        return base_prefix
    stem, suffix = _split_prefix(base_prefix)
    return f"{stem}_{generation}{suffix}"


def prefix_for_date(base_prefix: str, when, disambiguator: int = 0) -> str:
    """The store prefix for a generation that begins on ``when``.

    ``bkr/metoffice/metoffice_global_wave.icechunk`` plus 2025-09-20 gives
    ``bkr/metoffice/metoffice_global_wave_20092025.icechunk``. Naming the store after the
    day the upstream changed says what a bare counter cannot: *when* the break is, so the
    two halves of an archive can be lined up against a model-upgrade announcement without
    opening either of them.

    ``disambiguator`` covers the case of two different schemas appearing on one day, which
    would otherwise both want the same name.
    """
    stamp = pd.Timestamp(when).strftime(GENERATION_DATE_FORMAT)
    stem, suffix = _split_prefix(base_prefix)
    if disambiguator:
        return f"{stem}_{stamp}_{disambiguator}{suffix}"
    return f"{stem}_{stamp}{suffix}"


def generation_start(ds: xr.Dataset, append_dim: str):
    """The date a new generation built from ``ds`` should be named after.

    The first value along the append dimension: the earliest data that could not go in any
    existing store, which is as close as this can get to the moment the upstream changed.
    """
    values = ds[append_dim].values
    if values.size == 0:
        raise ValueError(f"{append_dim} is empty; cannot date a new generation")
    return pd.Timestamp(np.min(values))


def _suffix_sort_key(suffix: str) -> tuple:
    """Order generations: the base store, then numbered ones, then dated ones."""
    if suffix == "":
        return (0, 0)
    if suffix.isdigit() and len(suffix) <= 3:
        return (1, int(suffix))
    try:
        return (2, pd.Timestamp(dt.datetime.strptime(suffix[:8], GENERATION_DATE_FORMAT)).value)
    except ValueError:
        return (3, 0)


def existing_generations(config, base_prefix: str) -> list[str]:
    """Every store that is a generation of ``base_prefix``, in order.

    Found by *listing* rather than by counting upwards, because dated names cannot be
    guessed. Falls back to probing the numbered names when the listing is unavailable, so
    this still works against a store backend that will not enumerate.
    """
    stem, suffix = _split_prefix(base_prefix)
    parent, _, name = stem.rpartition("/")
    pattern = re.compile(rf"^{re.escape(name)}(?:_(?P<suffix>[0-9_]+))?{re.escape(suffix)}$")

    try:
        import fsspec

        root = config.store_path(parent) if parent else config.store_path("")
        filesystem, path = fsspec.core.url_to_fs(root, **config.fsspec_storage_options())
        # fsspec caches directory listings, and this is called repeatedly inside a process
        # that *creates* the very entries it is looking for. Without dropping the cache a
        # backfill never sees the generation its own previous partition started, so it
        # starts another one, and another, dating each after the day it happened to be
        # working on. That is how one schema came to own two stores.
        with contextlib.suppress(Exception):
            filesystem.invalidate_cache(path)
        entries = [str(e).rstrip("/").rpartition("/")[2] for e in filesystem.ls(path, detail=False)]
    except Exception as exc:  # noqa: BLE001 - a backend that will not list is not fatal
        logger.debug(f"could not list generations of {base_prefix} ({type(exc).__name__}: {exc})")
        return [
            prefix_for_generation(base_prefix, g)
            for g in range(1, MAX_GENERATIONS + 1)
            if store_exists(config, prefix_for_generation(base_prefix, g))
        ]

    found = []
    for entry in entries:
        match = pattern.match(entry)
        if match is None:
            continue
        found.append((_suffix_sort_key(match.group("suffix") or ""), entry))
    prefix = f"{parent}/" if parent else ""
    return [f"{prefix}{entry}" for _, entry in sorted(found)]


def round_significant(values: np.ndarray, figures: int) -> np.ndarray:
    """Round to ``figures`` significant digits, scaled by the array's own magnitude.

    One scale for the whole axis rather than per element, so that a coordinate running
    through zero does not have its small values rounded on a different scale from its
    large ones — which would make the digest depend on where the axis happened to be
    centred.
    """
    array = np.asarray(values, dtype="float64")
    finite = array[np.isfinite(array)]
    magnitude = np.max(np.abs(finite)) if finite.size else 0.0
    if magnitude == 0.0:
        return array
    decimals = figures - 1 - int(np.floor(np.log10(magnitude)))
    return np.round(array, decimals)


def _coord_digest(values: np.ndarray) -> str:
    """A stable digest of a coordinate, tolerant of how the axis was constructed.

    A float axis is *not* hashed element by element. Two runs of one model can describe
    the same grid with different float32 values: an axis written out exactly (0.17578125,
    ... 359.82421875) and one rebuilt as ``start + i * delta`` in float32 drift apart by up
    to 5e-4 degrees over 1024 points. That is 55 m against a 39 km cell — the same grid by
    any measure — but element-wise hashing calls them different, and quantising the
    elements only moves the problem to whichever values land near a bin boundary.

    So a float axis is reduced to the four things that actually define it — how many
    points, where it starts and ends, and how far apart the points are — each rounded to a
    small fraction of the axis's own span. Anything that genuinely regrids the data moves
    one of those by far more than :data:`COORD_SPAN_TOLERANCE`; float construction noise
    moves none of them.
    """
    array = np.asarray(values)
    if array.dtype.kind in "mM":
        array = array.astype("int64")
    elif array.dtype.kind == "f":
        return _float_axis_digest(array.astype("float64"))
    return hashlib.sha1(np.ascontiguousarray(array).tobytes()).hexdigest()[:16]


def _float_axis_digest(array: np.ndarray) -> str:
    """Digest of a floating point axis, from its shape, extent and spacing."""
    if array.size == 0:
        return "empty"
    if array.size == 1:
        return f"1@{round_significant(array, COORD_SIGNIFICANT_FIGURES)[0]!r}"

    span = float(np.abs(array[-1] - array[0]))
    if not np.isfinite(span) or span == 0.0:
        rounded = round_significant(array, COORD_SIGNIFICANT_FIGURES)
        return hashlib.sha1(np.ascontiguousarray(rounded).tobytes()).hexdigest()[:16]

    # Absolute significant figures, not a tolerance scaled by this axis's own span. Scaling
    # by the span would make the signature scale-invariant, so a -80..80 axis and a
    # -90..90 axis of the same length would normalise to the same integers and share a
    # store. Rounding the three summary numbers to a fixed precision keeps a shifted or
    # stretched domain distinguishable while still absorbing construction noise, which only
    # ever reaches the sixth significant figure.
    summary = np.array(
        [float(array[0]), float(array[-1]), float(np.median(np.diff(array)))],
        dtype="float64",
    )
    signature = (int(array.size), *round_significant(summary, COORD_AXIS_FIGURES).tolist())
    return hashlib.sha1(repr(signature).encode()).hexdigest()[:16]


def schema_fingerprint(ds: xr.Dataset, append_dim: str) -> str:
    """A digest of everything an append has to agree with the store about.

    Three things go in:

    * the data variables, by name and by the dimensions they are on — a new diagnostic, a
      dropped one, or one that gained a level dimension all change the store's shape;
    * the size of every dimension except ``append_dim``, which is the resolution;
    * the values of the grid coordinates in :data:`GRID_COORDS`, so that a regridding that
      happens to keep the point count still starts a new generation.

    Attributes, encoding, chunking and the data are all excluded: they vary run to run and
    splitting a store on them would create a generation per partition.
    """
    variables = sorted((str(name), tuple(map(str, ds[name].dims))) for name in ds.data_vars)
    sizes = sorted((str(dim), int(size)) for dim, size in ds.sizes.items() if dim != append_dim)
    coords = sorted(
        (str(name), _coord_digest(ds[name].values))
        for name in ds.coords
        if str(name) in GRID_COORDS and str(name) != append_dim and ds[name].ndim == 1
    )
    payload = json.dumps(
        {"variables": variables, "sizes": sizes, "coords": coords}, sort_keys=True
    )
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


#: What :func:`inspect_generation` found at a prefix.
EMPTY = "empty"
DAMAGED = "damaged"


def inspect_generation(config, prefix: str, append_dim: str) -> tuple[str, str | None]:
    """Classify the store at ``prefix`` as empty, damaged, or holding a schema.

    The distinction is the whole point and getting it wrong is expensive. An *empty* store
    is the normal way a generation begins and should be claimed. A *damaged* one — a store
    whose variables disagree about the length of the append dimension, which is what a
    half-finished append leaves behind — must be left alone: it cannot be fingerprinted, so
    there is no way to know whether the incoming data belongs in it, and writing to it
    piles more on top of the damage.

    Returns ``(EMPTY, None)``, ``(DAMAGED, None)`` or ``("ok", fingerprint)``.
    """
    try:
        repo = config.icechunk_repo(prefix)
        session = repo.readonly_session("main")
    except Exception as exc:  # noqa: BLE001 - no repository at all is an empty slot
        logger.debug(f"{prefix}: no repository ({type(exc).__name__}: {exc})")
        return EMPTY, None

    try:
        ds = xr.open_zarr(session.store, consolidated=False)
    except Exception as exc:  # noqa: BLE001
        # icechunk raises GroupNotFoundError for a repository with nothing committed to it,
        # which `icechunk_repo` creates as a side effect of merely looking. Anything else
        # means there *is* data and it cannot be read.
        if "GroupNotFound" in type(exc).__name__ or "NotFound" in type(exc).__name__:
            return EMPTY, None
        logger.warning(
            f"{prefix}: holds committed data that cannot be read "
            f"({type(exc).__name__}: {str(exc)[:160]}); leaving it alone"
        )
        return DAMAGED, None

    try:
        return "ok", schema_fingerprint(ds, append_dim)
    except Exception as exc:  # noqa: BLE001 - readable but un-fingerprintable is damaged
        logger.warning(f"{prefix}: cannot be fingerprinted ({type(exc).__name__}); leaving it")
        return DAMAGED, None


def store_exists(config, prefix: str) -> bool:
    """Whether a store at ``prefix`` holds anything at all.

    ``icechunk_repo`` creates an empty repository as a side effect of opening one, so
    "the repository exists" is not the question — "it has a root group" is.
    """
    try:
        repo = config.icechunk_repo(prefix)
        xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except Exception:  # noqa: BLE001 - anything unreadable is "not a usable store"
        return False
    return True


def resolve_generation(
    config,
    base_prefix: str,
    ds: xr.Dataset,
    append_dim: str,
    max_generations: int = MAX_GENERATIONS,
) -> str:
    """Return the store prefix ``ds`` should be written to.

    Walks the generations of ``base_prefix`` in order and returns the first that either
    matches ``ds``'s schema or does not exist yet. See the module docstring for the rules.

    Raises:
        RuntimeError: if ``max_generations`` are all present and none match, which means
            either a genuinely long upgrade history or a fingerprint that is splitting the
            store on something it should be ignoring.
    """
    wanted = schema_fingerprint(ds, append_dim)
    candidates = existing_generations(config, base_prefix)

    # The base store is written to even when it does not exist yet: a brand new series has
    # no change to date, so naming its first generation after a day would be misleading.
    if base_prefix not in candidates:
        candidates = [base_prefix, *candidates]

    empty: set[str] = set()
    for prefix in candidates[:max_generations]:
        state, found = inspect_generation(config, prefix, append_dim)

        if state == EMPTY:
            # The base store being empty means a brand new series; claim it. A *dated*
            # store being empty means an earlier run named one and then did not write —
            # interrupted, or its partition turned out to have no data. Claiming that one
            # would file today's change under the day that run happened to be working on,
            # so it is only reused below if its name is the one this change wants anyway.
            if prefix == base_prefix:
                logger.info(f"{base_prefix}: starting the series at {prefix}")
                return prefix
            empty.add(prefix)
            continue

        if state == DAMAGED:
            # Not ours to write to and not ours to judge: skip past it and let the data
            # land in a sound store. Writing here would turn an unreadable store into an
            # unreadable store with more in it.
            continue

        if found == wanted:
            if prefix != base_prefix:
                logger.debug(f"{base_prefix}: schema matches {prefix}")
            return prefix

        logger.debug(f"{prefix}: schema {found} does not match incoming {wanted}")

    # Nothing holds this schema, so the upstream has changed shape. Name the new store
    # after the day the change shows up rather than after a counter.
    start = generation_start(ds, append_dim)
    for disambiguator in range(0, max_generations):
        prefix = prefix_for_date(base_prefix, start, disambiguator)
        if prefix not in candidates or prefix in empty:
            logger.info(
                f"{base_prefix}: schema {wanted} is new; the model changed on "
                f"{start:%Y-%m-%d}, starting {prefix}"
            )
            return prefix

    raise RuntimeError(
        f"{base_prefix}: {len(candidates)} generations exist and none match the incoming "
        f"schema {wanted}, and every name for {start:%Y-%m-%d} is taken. Either the "
        f"upstream changes every partition, or schema_fingerprint is splitting on "
        f"something it should ignore."
    )


def snap_to_stored_coords(
    ds: xr.Dataset, stored: xr.Dataset, coords: tuple[str, ...] = GRID_COORDS
) -> xr.Dataset:
    """Adopt the store's coordinate values wherever they describe the same axis.

    Closes a gap between the two checks a write passes. :func:`schema_fingerprint` is
    deliberately *tolerant* — it has to be, or float32 construction noise would fork a
    store — while the alignment guard in
    :func:`~planetary_datasets.common.store.write_to_icechunk` is exact, because an append
    lands on the store's own axis and a coordinate that is off by anything is a different
    axis to zarr.

    Between the two sits a real case: an axis that the fingerprint calls the same and
    ``array_equal`` calls different. Without this the generation machinery would route such
    a partition to a store that then refuses it — and refuses it by returning False, so the
    partition would be reported as "nothing to do" and quietly never written.

    So where an incoming coordinate has the same digest as the store's, the store's values
    are adopted verbatim. The grid is the same; this just settles which of two spellings of
    it the store keeps — the one it already has.
    """
    for name in coords:
        if name not in ds.coords or name not in stored.coords:
            continue
        incoming, existing = np.asarray(ds[name].values), np.asarray(stored[name].values)
        if incoming.shape != existing.shape or np.array_equal(incoming, existing):
            continue
        if _coord_digest(incoming) != _coord_digest(existing):
            # A genuinely different axis. Leave it alone and let the alignment guard
            # reject it rather than silently bending the data onto the wrong grid.
            continue
        logger.debug(f"adopting the store's {name} values; same axis, different spelling")
        ds = ds.assign_coords({name: (ds[name].dims, existing)})
    return ds


class GenerationalStoreMixin:
    """Give a provider a store prefix chosen by :func:`resolve_generation`.

    The provider declares ``base_store_prefix`` instead of ``store_prefix``; the concrete
    prefix is settled once per partition, when the processed dataset is in hand and its
    schema can be read. ``store_path`` before that point reports generation 1, which is
    what logging and metadata want.
    """

    #: Logical name of the series, without a generation suffix.
    base_store_prefix: str

    #: Set once a partition has been processed; the generation actually written to.
    _resolved_prefix: str | None = None

    @property
    def store_prefix(self) -> str:  # type: ignore[override]
        """The generation being written to, or generation 1 before one is settled."""
        return self._resolved_prefix or self.base_store_prefix

    def resolve_store(self, processed: xr.Dataset) -> str:
        """Settle which generation ``processed`` belongs to, and remember it."""
        prefix = resolve_generation(
            self.config, self.base_store_prefix, processed, self.append_dim
        )
        if prefix != self._resolved_prefix:
            self._resolved_prefix = prefix
            # The base class caches the repository handle, and it is now the wrong one.
            self._repo = None
        return prefix

    def store_for(self, processed: xr.Dataset, repo):
        """Choose the generation for this partition, opening its repository."""
        self.resolve_store(processed)
        return self.get_icechunk_repo()

    def write_to_icechunk(self, repo, processed: xr.Dataset) -> bool:
        """Adopt the chosen store's grid spelling, then write.

        See :func:`snap_to_stored_coords`: without this a partition the fingerprint sends
        to a store can still be refused by that store's exact alignment check, and refused
        silently.
        """
        try:
            stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        except Exception:  # noqa: BLE001 - a store with nothing in it has nothing to adopt
            stored = None
        if stored is not None:
            processed = snap_to_stored_coords(processed, stored, self.alignment_coords)
        return super().write_to_icechunk(repo, processed)

    def generations(self) -> list[str]:
        """Every generation of this series that currently holds data, oldest first."""
        return [
            prefix
            for prefix in existing_generations(self.config, self.base_store_prefix)
            if store_exists(self.config, prefix)
        ]

    def describe_store(self) -> str:
        """The generations a presence check actually consults, not just the base name."""
        found = self.generations()
        if not found:
            return f"{self.config.store_path(self.base_store_prefix)} (empty)"
        return ", ".join(prefix.rpartition("/")[2] for prefix in found)

    def stored_times(self) -> set:
        """Every value along the append dimension, across all generations.

        Asked across every generation, not just the newest: a partition from before an
        upgrade lives in an older one, and treating it as missing would re-download and
        re-append the whole pre-upgrade archive on the next backfill.
        """
        import pandas as pd

        from planetary_datasets.common.store import existing_times

        stored: set = set()
        for prefix in self.generations():
            try:
                times = existing_times(
                    self.config.icechunk_repo(prefix), append_dim=self.append_dim
                )
            except Exception as exc:  # noqa: BLE001 - an unreadable generation holds nothing
                logger.debug(f"{prefix}: could not read {self.append_dim} ({type(exc).__name__})")
                continue
            if times is not None and len(times):
                stored.update(pd.DatetimeIndex(times))
        return stored

    def missing_timesteps(self, desired):
        """Timesteps in ``desired`` that no generation holds."""
        import pandas as pd

        stored = self.stored_times()
        return [t for t in pd.DatetimeIndex(desired) if t not in stored]


__all__ = [
    "GRID_COORDS",
    "generation_start",
    "prefix_for_date",
    "existing_generations",
    "snap_to_stored_coords",
    "GenerationalStoreMixin",
    "MAX_GENERATIONS",
    "prefix_for_generation",
    "resolve_generation",
    "schema_fingerprint",
    "store_exists",
]
