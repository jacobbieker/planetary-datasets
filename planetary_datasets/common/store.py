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
    if append_dim in ds.coords:
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
    except (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError) as exc:
        logger.debug(f"store not readable yet ({type(exc).__name__}), treating as empty")
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


def write_to_icechunk(
    repo: icechunk.Repository,
    ds: xr.Dataset,
    append_dim: str = "time",
    message: str | None = None,
    check_vars: bool = True,
    alignment_coords: Iterable[str] = ALIGNMENT_COORDS,
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
    """
    if append_dim not in ds.coords:
        raise ValueError(f"dataset has no {append_dim!r} coordinate to append along")

    session = repo.writable_session("main")
    try:
        existing = xr.open_zarr(session.store, consolidated=False)
        first_write = append_dim not in existing.coords
    except (ValueError, KeyError, FileNotFoundError, icechunk.IcechunkError) as exc:
        logger.debug(f"creating new store ({type(exc).__name__})")
        existing = None
        first_write = True

    if first_write:
        to_icechunk(ds, session, encoding=build_encoding(ds, append_dim=append_dim))
        session.commit(message or f"Initial write of {append_dim} {ds[append_dim].values[0]}")
        logger.info(f"created store with {ds.sizes.get(append_dim, 1)} step(s)")
        return True

    incoming = np.atleast_1d(ds[append_dim].values)
    present = existing.coords[append_dim].values
    if np.isin(incoming, present).all():
        logger.debug(f"{incoming[0]} already in store, skipping write")
        return False

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
