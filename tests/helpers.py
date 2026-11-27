"""Small helpers shared by the test modules (import with ``from helpers import ...``).

Fixtures live in ``conftest.py``; these are plain functions, which read better called
inline than injected.
"""

from __future__ import annotations

import xarray as xr


def read_store(target) -> xr.Dataset:
    """Open an icechunk store's ``main`` branch for reading.

    ``target`` is a repository, or a provider, whose repository is used.
    """
    repo = target.get_icechunk_repo() if hasattr(target, "get_icechunk_repo") else target
    return xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
    )


def write_grib(path, fields, ni: int = 5, nj: int = 4):
    """Write a small regular lat-lon GRIB2 file, one message per field.

    ``fields`` holds ``(short_name, type_of_level, level, step_hours)`` tuples; ``level`` is
    None for a level type without one. Every value of a message is ``100 * step + level``,
    so a test can tell the messages apart after decoding. Returns ``path``.
    """
    import eccodes
    import numpy as np

    with open(path, "wb") as out:
        for short_name, level_type, level, step in fields:
            sample = (
                "regular_ll_pl_grib2" if level_type == "isobaricInhPa" else "regular_ll_sfc_grib2"
            )
            handle = eccodes.codes_grib_new_from_samples(sample)
            try:
                for key, value in (
                    ("Ni", ni),
                    ("Nj", nj),
                    ("latitudeOfFirstGridPointInDegrees", 1.0),
                    ("latitudeOfLastGridPointInDegrees", 1.0 - (nj - 1) * 0.5),
                    ("longitudeOfFirstGridPointInDegrees", 10.0),
                    ("longitudeOfLastGridPointInDegrees", 10.0 + (ni - 1) * 0.5),
                    ("iDirectionIncrementInDegrees", 0.5),
                    ("jDirectionIncrementInDegrees", 0.5),
                    ("dataDate", 20261126),
                    ("dataTime", 0),
                    ("typeOfLevel", level_type),
                ):
                    eccodes.codes_set(handle, key, value)
                if level is not None:
                    eccodes.codes_set(handle, "level", level)
                eccodes.codes_set(handle, "shortName", short_name)
                eccodes.codes_set(handle, "stepUnits", 1)
                eccodes.codes_set(handle, "forecastTime", step)
                eccodes.codes_set_values(handle, np.full(ni * nj, 100.0 * step + (level or 0)))
                eccodes.codes_write(handle, out)
            finally:
                eccodes.codes_release(handle)
    return path


def assert_pipes_accepts(metadata: dict) -> None:
    """Fail unless Dagster Pipes would accept ``metadata`` from a container.

    Pipes validates what an external process reports, and rejects an untagged dict
    (including an empty one) outright; a unit-test fake would accept anything.
    """
    from dagster_pipes import _normalize_param_metadata

    _normalize_param_metadata(metadata, "report_asset_materialization", "metadata")
