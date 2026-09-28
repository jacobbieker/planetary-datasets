"""Writing datasets into icechunk stores.

Replaces the ``write_to_icechunk`` function that appeared in twenty files and the
encoding-dict builder that appeared in seventy-one. The append-or-create decision, the
"already written, skip" guard and the compression settings all live here.
"""

from __future__ import annotations

from typing import Iterable, Sequence

import icechunk
import numpy as np
import xarray as xr
import zarr.codecs
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.common.dataset import coords_match

# Coordinates that must line up with the existing store or an append will corrupt it.
ALIGNMENT_COORDS = ("latitude", "longitude", "level", "isobaricInhPa", "height")

# Errors that mean "this store has nothing in it yet" when the repository is also empty.
STORE_READ_ERRORS = (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError)


class StoreReadError(RuntimeError):
    """A store that is known to hold data could not be read.

    Distinct from an empty store. Treating this as empty would silently discard the
    archive, so it is raised rather than swallowed.
    """


def has_committed_data(repo: icechunk.Repository, branch: str = "main") -> bool:
    """True when the repository has at least one commit beyond initialisation.

    ``Repository.open_or_create`` leaves a single "initialized" snapshot behind, so a
    count above one means real data has been written. This is the authoritative
    emptiness check: a failed read must never be mistaken for an empty store, or the
    create-fresh path would overwrite an existing archive.
    """
    return sum(1 for _ in repo.ancestry(branch=branch)) > 1


def build_encoding(
    ds: xr.Dataset,
    append_dim: str = "time",
    clevel: int = 9,
) -> dict:
    """Build the zstd/bitshuffle encoding dict used for initial writes."""
    encoding: dict = {}
    for var in ds.data_vars:
        encoding[var] = {
            "compressors": zarr.codecs.BloscCodec(cname="zstd", clevel=clevel, shuffle="bitshuffle")
        }
    # Only a datetime append dimension gets CF time encoding. Applying it to, say, a
    # string station id fails in the encoder with "ufunc 'rint' not supported".
    if append_dim in ds.coords and np.issubdtype(ds.coords[append_dim].dtype, np.datetime64):
        encoding[append_dim] = {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        }
    return encoding


def existing_times(repo: icechunk.Repository, append_dim: str = "time") -> np.ndarray:
    """Return the values already present along ``append_dim``.

    An empty array is returned when the store does not exist yet or has no such
    coordinate, which is the normal state before the first write.
    """
    try:
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except STORE_READ_ERRORS as exc:
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but could not be read ({type(exc).__name__}: {exc}). "
                "Refusing to report it as empty."
            ) from exc
        logger.debug(f"store is empty ({type(exc).__name__})")
        return np.array([])
    if append_dim not in ds.coords:
        return np.array([])
    return ds.coords[append_dim].values


def has_timestep(repo: icechunk.Repository, timestep, append_dim: str = "time") -> bool:
    """True when ``timestep`` is already present in the store."""
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return False
    value = getattr(timestep, "to_numpy", lambda: timestep)()
    return bool(np.isin(value, times).any())


def missing_timesteps(
    repo: icechunk.Repository,
    desired: Sequence,
    append_dim: str = "time",
) -> list:
    """Return the subset of ``desired`` that is not yet in the store."""
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return list(desired)
    return [t for t in desired if not np.isin(getattr(t, "to_numpy", lambda: t)(), times).any()]


def missing_periods(
    repo: icechunk.Repository,
    desired: Sequence,
    unit: str = "D",
    append_dim: str = "time",
) -> list:
    """Return the timestamps in ``desired`` whose *period* is not represented in the store.

    :func:`missing_timesteps` asks whether an exact value is stored, which only works when
    the partition timestamp is itself one of the stored values. It is not for a swath
    archive keyed by granule time, or for a forecast archive whose initialisation dates
    fall inside the partition rather than on its first instant: there the question is
    "does the store already hold anything from this day/month?".

    Args:
        repo: Target repository.
        desired: Partition timestamps to test.
        unit: numpy datetime64 unit naming the period, e.g. ``D`` for a day or ``M`` for a
            month.
        append_dim: Dimension holding the stored timestamps.
    """
    times = existing_times(repo, append_dim=append_dim)
    if times.size == 0:
        return list(desired)
    # Truncated inside numpy: a Python set of Timestamps costs gigabytes once a swath
    # store holds tens of millions of scan times, and this runs before every partition.
    stored = np.unique(times.astype(f"datetime64[{unit}]"))
    wanted = np.array([np.datetime64(t, unit) for t in desired], dtype=f"datetime64[{unit}]")
    return [t for t, present in zip(desired, np.isin(wanted, stored)) if not present]


def write_to_icechunk(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    append_dim: str = "time",
    message: str | None = None,
    check_vars: bool = True,
    alignment_coords: Iterable[str] = ALIGNMENT_COORDS,
    require_monotonic: bool = True,
) -> bool:
    """Write ``ds`` to ``repo``, creating the store or appending along ``append_dim``.

    Returns True if data was committed, False if the write was skipped. Skipping is normal
    and not an error: it means the timestep is already stored, or the dataset does not line
    up with what is there.

    Args:
        repo: Target repository.
        ds: Dataset to write. Must have ``append_dim`` as a coordinate.
        append_dim: Dimension to append along.
        message: Commit message. A default naming the timestep is used when omitted.
        check_vars: Refuse to append when the variable set differs from the store's.
        alignment_coords: Coordinates that must match the store exactly before appending.
        require_monotonic: Drop incoming steps that fall at or before the last stored one.
            Icechunk only appends, so writing a late-arriving earlier timestep would leave
            ``append_dim`` unsorted and every later ``.sel(time=slice(...))`` silently
            wrong. The gap stays a gap, but it stays a *visible* gap.
    """
    if append_dim not in ds.coords:
        raise ValueError(f"dataset has no {append_dim!r} coordinate to append along")

    session = repo.writable_session("main")
    try:
        existing = xr.open_zarr(session.store, consolidated=False)
        first_write = append_dim not in existing.coords
    except STORE_READ_ERRORS as exc:
        # A read failure against a store that already holds data must not fall through to
        # the create-fresh path: that would overwrite the whole archive with one timestep.
        if has_committed_data(repo):
            raise StoreReadError(
                f"store holds committed data but could not be read ({type(exc).__name__}: {exc}). "
                "Refusing to overwrite it with a fresh write."
            ) from exc
        logger.debug(f"creating new store ({type(exc).__name__})")
        existing = None
        first_write = True

    # atleast_1d so a scalar append coordinate does not raise IndexError here.
    incoming = np.atleast_1d(ds[append_dim].values)

    if first_write:
        to_icechunk(ds, session, encoding=build_encoding(ds, append_dim=append_dim))
        session.commit(message or f"Initial write of {append_dim} {incoming[0]}")
        logger.info(f"created store with {ds.sizes.get(append_dim, 1)} step(s)")
        return True

    present = existing.coords[append_dim].values
    already = np.isin(incoming, present)
    if already.all():
        logger.debug(f"{incoming[0]} already in store, skipping write")
        return False

    if already.any():
        # Partial overlap: appending the whole batch would duplicate the steps that are
        # already stored. Append only the new ones.
        keep = np.flatnonzero(~already)
        logger.info(
            f"{int(already.sum())} of {incoming.size} steps already stored, appending the remaining {keep.size}"
        )
        ds = ds.isel({append_dim: keep})
        incoming = incoming[~already]

    if require_monotonic and present.size:
        # Icechunk appends; it cannot insert. A provider that revisits a partial hour (MRMS
        # and the UK radar composite both do, deliberately, while the upstream archive is
        # still filling in) can hand us a timestep that sorts before what is already there.
        # Appending it anyway leaves the coordinate unsorted, which no consumer checks for
        # and which breaks slicing over the whole store, not just the affected hour.
        stale = incoming <= present.max()
        if stale.any():
            logger.error(
                f"{int(stale.sum())} of {incoming.size} step(s) are at or before the last "
                f"stored {append_dim} ({present.max()}); dropping them rather than writing "
                f"an unsorted {append_dim}. First dropped: {incoming[stale][0]}"
            )
            keep = np.flatnonzero(~stale)
            if keep.size == 0:
                return False
            ds = ds.isel({append_dim: keep})
            incoming = incoming[~stale]

    if check_vars and set(ds.data_vars) != set(existing.data_vars):
        only_new = set(ds.data_vars) - set(existing.data_vars)
        only_old = set(existing.data_vars) - set(ds.data_vars)
        logger.error(
            f"variable mismatch, skipping write. only in new: {sorted(only_new)}; "
            f"only in store: {sorted(only_old)}"
        )
        return False

    ok, bad = coords_match(existing, ds, tuple(alignment_coords))
    if not ok:
        logger.error(f"coordinate {bad!r} does not match the store, skipping write")
        return False

    to_icechunk(ds, session, append_dim=append_dim)
    session.commit(
        message or f"Append {append_dim} {incoming[0]}",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"appended {append_dim} {incoming[0]}")
    return True
