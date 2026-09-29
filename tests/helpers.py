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
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def assert_pipes_accepts(metadata: dict) -> None:
    """Fail unless Dagster Pipes would accept ``metadata`` from a container.

    Pipes validates what an external process reports, and rejects an untagged dict
    (including an empty one) outright; a unit-test fake would accept anything.
    """
    from dagster_pipes import _normalize_param_metadata

    _normalize_param_metadata(metadata, "report_asset_materialization", "metadata")
