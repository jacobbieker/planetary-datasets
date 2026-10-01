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

    bkr/metoffice/metoffice_global_wave.icechunk      <- generation 1
    bkr/metoffice/metoffice_global_wave_2.icechunk    <- generation 2, after an upgrade
    bkr/metoffice/metoffice_global_wave_3.icechunk    <- ... and so on

The rules, in order:

1. The first generation whose schema *matches* the incoming data is written to. Matching by
   schema rather than by recency is what lets a model revert, or a late backfill of
   pre-upgrade data, land back in the generation it belongs to.
2. Otherwise the first generation that does not exist yet is created.
3. A generation that exists but cannot be read is skipped rather than written to. A store
   left inconsistent by a half-finished append cannot be fingerprinted, and appending to it
   would pile more on top of the damage.

What counts as "the same schema" is deliberately narrow — see :func:`schema_fingerprint`.
It is the set of things ``write_to_icechunk`` would refuse a mismatch on, and nothing else,
so a change in an attribute or in the data itself never splits a store.
"""

from __future__ import annotations

import hashlib
import json

import numpy as np
import xarray as xr
from loguru import logger

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


def prefix_for_generation(base_prefix: str, generation: int) -> str:
    """The store prefix for a generation, counting from one.

    Generation 1 is the base prefix unchanged, so an existing store keeps its name and
    nothing has to be migrated to adopt this.
    """
    if generation < 1:
        raise ValueError(f"generation must be >= 1, got {generation}")
    if generation == 1:
        return base_prefix
    stem, dot, suffix = base_prefix.rpartition(".")
    if not dot:
        return f"{base_prefix}_{generation}"
    return f"{stem}_{generation}.{suffix}"


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


def _stored_fingerprint(config, prefix: str, append_dim: str) -> str | None:
    """Fingerprint of the store at ``prefix``, or None when it is absent or unreadable.

    The two are not distinguished on purpose at this level; :func:`resolve_generation`
    handles them differently and logs which happened.
    """
    repo = config.icechunk_repo(prefix)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    return schema_fingerprint(ds, append_dim)


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

    for generation in range(1, max_generations + 1):
        prefix = prefix_for_generation(base_prefix, generation)
        try:
            found = _stored_fingerprint(config, prefix, append_dim)
        except Exception as exc:  # noqa: BLE001 - empty and damaged both land here
            # An empty store is the normal way a new generation begins, so this is only
            # worth a debug line; the interesting case is the one logged below.
            logger.debug(f"{prefix}: not readable as a store ({type(exc).__name__}), claiming it")
            logger.info(f"{base_prefix}: starting generation {generation} at {prefix}")
            return prefix

        if found == wanted:
            if generation > 1:
                logger.debug(f"{base_prefix}: schema matches generation {generation}")
            return prefix

        logger.info(
            f"{prefix}: schema {found} does not match incoming {wanted}; "
            f"the model has changed shape, trying generation {generation + 1}"
        )

    raise RuntimeError(
        f"{base_prefix}: {max_generations} generations exist and none match the incoming "
        f"schema {wanted}. Either the upstream changes every partition, or "
        f"schema_fingerprint is splitting on something it should ignore."
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
        found = []
        for generation in range(1, MAX_GENERATIONS + 1):
            prefix = prefix_for_generation(self.base_store_prefix, generation)
            if not store_exists(self.config, prefix):
                break
            found.append(prefix)
        return found

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
    "snap_to_stored_coords",
    "GenerationalStoreMixin",
    "MAX_GENERATIONS",
    "prefix_for_generation",
    "resolve_generation",
    "schema_fingerprint",
    "store_exists",
]
