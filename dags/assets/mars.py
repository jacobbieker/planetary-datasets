"""Dagster assets for the ECMWF MARS archive.

Thin wrappers around the entrypoints in
:mod:`planetary_datasets.providers.mars_pipeline` and
:mod:`planetary_datasets.providers.mars_icechunk`. The orchestration --
resumability, staging, disk accounting, multi-worker scheduling -- lives in
those modules and is deliberately not reimplemented here; these assets only
decide *when* it runs and record what it did.

Three assets, one per stage of the archive:

``ecmwf_mars_axis``
    Extend the stores' hourly time axes to cover the partition. This moves
    chunks, so it must not run while anything else writes -- which is why it
    is a separate asset the other two depend on, rather than a step inside
    them.
``ecmwf_mars_native``
    Retrieve a month from MARS and stream it into the native O1280 store,
    writing the regridded stores from the same decoded fields and deleting
    each GRIB file once every store has it.
``ecmwf_mars_regrid``
    Catch the regridded stores up on anything the native store has that they
    lack -- a resolution added later, or a timestep whose inline regrid lost
    a commit race.

These are the heaviest jobs in the repository: a month of model levels and
wave spectra is tens of terabytes of GRIB, MARS queues each request from tape,
and a single timestep takes minutes to decode. Hence the long
``dagster/max_runtime`` and a concurrency key of their own, so two months
never run at once and never contend with the other ingests for memory or for
the MARS queue.

Everything is configured through :func:`planetary_datasets.config.get_config`;
see the module docstring of ``mars_pipeline`` for which variables matter.
"""

# No ``from __future__ import annotations`` here: dagster inspects the real
# annotation on the ``context`` parameter and rejects the string a deferred
# annotation would leave behind.
from typing import TYPE_CHECKING, Dict, List, Tuple

import dagster as dg

if TYPE_CHECKING:  # pragma: no cover - typing only
    import datetime as dt

#: One asset run per calendar month. A month is the smallest unit worth the
#: fixed costs (opening three stores, building regridding weights, planning
#: the retrievals) and the largest that finishes inside the runtime limit.
partitions_def: dg.TimeWindowPartitionsDefinition = dg.MonthlyPartitionsDefinition(
    start_date="2026-01-01",
    end_offset=-1,
)

#: Every MARS asset takes this key, so only one of them runs at a time. They
#: share a MARS queue, a staging disk and a set of stores, and extending a time
#: axis is only safe with nothing else writing.
CONCURRENCY_KEY = "ecmwf-mars"

#: Three days. A month of retrievals is dominated by MARS queueing requests
#: from tape, which is not something the run can hurry along.
MAX_RUNTIME_SECONDS = 60 * 60 * 72

#: The regridded stores written alongside the native one.
RESOLUTIONS = (0.25, 1.0)

_COMMON_TAGS = {
    "dagster/max_runtime": str(MAX_RUNTIME_SECONDS),
    "dagster/concurrency_key": CONCURRENCY_KEY,
    "dagster/priority": "1",
}


def _month(context: dg.AssetExecutionContext) -> Tuple["dt.datetime", "dt.datetime"]:
    """The partition's first and last day, both inclusive and timezone-naive.

    MARS date ranges are inclusive at both ends, while a Dagster time window is
    half-open, so the last day is the one before the window closes.

    The window is timezone-aware UTC and the stores' time axis is naive. Mixing
    the two silently produces an empty intersection when the planned timesteps
    are checked against the axis, so the offset is dropped here rather than
    being carried into the pipeline.
    """
    import datetime as dt  # noqa: PLC0415

    window = context.partition_time_window
    start = window.start.astimezone(dt.timezone.utc).replace(tzinfo=None)
    end = window.end.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return start, end - dt.timedelta(days=1)


def _store_metadata(stores: List[str]) -> Dict[str, dg.MetadataValue]:
    return {
        "native_store": dg.MetadataValue.path(stores[0]),
        "regrid_stores": dg.MetadataValue.json(list(stores[1:])),
    }


@dg.asset(
    name="ecmwf_mars_axis",
    description=(
        "Extend the ECMWF MARS stores' hourly time axes to cover this month. "
        "Moves chunks, so nothing else may write while it runs."
    ),
    partitions_def=partitions_def,
    metadata={
        "source": dg.MetadataValue.text("ecmwf-mars"),
        "expected_runtime": dg.MetadataValue.text("minutes"),
    },
    compute_kind="python",
    tags=_COMMON_TAGS,
)
def ecmwf_mars_axis_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Create or grow the native and regridded stores so the month fits in them."""
    from planetary_datasets.providers.mars_icechunk import (  # noqa: PLC0415
        MARSRegridProvider,
        build_provider,
    )

    start, end = _month(context)
    native = build_provider()
    repo = native.get_icechunk_repo()
    try:
        native.store_times(repo)
    except Exception as exc:
        # `initialize_store` derives the schema -- the grid, the hybrid
        # coefficients, the variable set -- from a real GRIB message, so an
        # empty store cannot be created before anything has been retrieved.
        # This asset runs *before* the retrieval, so it can only extend.
        raise dg.Failure(
            description=(
                f"{native.icechunk_path} does not exist yet, and its schema is read "
                "from a GRIB message rather than declared. Create it once by hand from "
                "a retrieval already on disk -- `python -m "
                "planetary_datasets.providers.mars_icechunk --init-only`, then the same "
                "with `--regrid 0.25 1` -- and materialise this asset afterwards."
            )
        ) from exc

    # An hourly axis: the last analysis of the month is at 23Z on its last day.
    axis = native.ensure_time_axis(
        repo=repo, start=start, end=end.replace(hour=23, minute=0, second=0, microsecond=0)
    )
    targets = [MARSRegridProvider(native, resolution=r) for r in RESOLUTIONS]
    for target in targets:
        target.ensure_time_axis()

    stores = [native.icechunk_path, *(t.icechunk_path for t in targets)]
    context.log.info(f"Time axis now {axis[0]} to {axis[-1]} ({len(axis)} steps)")
    return dg.MaterializeResult(
        metadata={
            **_store_metadata(stores),
            "axis_start": dg.MetadataValue.text(str(axis[0])),
            "axis_end": dg.MetadataValue.text(str(axis[-1])),
            "timesteps": dg.MetadataValue.int(len(axis)),
        }
    )


@dg.asset(
    name="ecmwf_mars_native",
    description=(
        "Retrieve a month of ECMWF MARS model-level, wave and 2D wave spectra "
        "data and stream it into the native O1280 Icechunk store, writing the "
        "0.25 and 1 degree stores from the same decoded fields."
    ),
    deps=[dg.AssetDep("ecmwf_mars_axis", partition_mapping=dg.IdentityPartitionMapping())],
    partitions_def=partitions_def,
    metadata={
        "source": dg.MetadataValue.text("ecmwf-mars"),
        "grid": dg.MetadataValue.text("O1280 reduced Gaussian"),
        "expected_runtime": dg.MetadataValue.text("days"),
    },
    compute_kind="python",
    tags=_COMMON_TAGS,
)
def ecmwf_mars_native_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Run `MarsPipeline` over the partition, downloading and ingesting together."""
    from planetary_datasets.providers.mars_pipeline import MarsPipeline  # noqa: PLC0415

    start, end = _month(context)
    pipeline = MarsPipeline(start=start, end=end, download=True, resolutions=RESOLUTIONS)
    context.log.info(pipeline.describe())
    pipeline.run()

    stores = [pipeline.native.icechunk_path, *(t.icechunk_path for t in pipeline.targets)]
    return dg.MaterializeResult(
        metadata={
            **_store_metadata(stores),
            "start": dg.MetadataValue.text(f"{pipeline.start:%Y-%m-%d}"),
            "end": dg.MetadataValue.text(f"{pipeline.end:%Y-%m-%d}"),
            "retrievals_planned": dg.MetadataValue.int(len(pipeline.jobs)),
            "source_dir": dg.MetadataValue.path(str(pipeline.source_dir)),
            "staging_dir": dg.MetadataValue.path(str(pipeline.staging_dir)),
        }
    )


@dg.asset(
    name="ecmwf_mars_regrid",
    description=(
        "Resample whatever the native ECMWF MARS store has onto the 0.25 and 1 "
        "degree regular grids, for timesteps the inline regrid did not cover."
    ),
    deps=[dg.AssetDep("ecmwf_mars_native", partition_mapping=dg.IdentityPartitionMapping())],
    partitions_def=partitions_def,
    metadata={
        "source": dg.MetadataValue.text("ecmwf-mars"),
        "grid": dg.MetadataValue.text("regular latitude/longitude"),
        "expected_runtime": dg.MetadataValue.text("hours"),
    },
    compute_kind="python",
    tags=_COMMON_TAGS,
)
def ecmwf_mars_regrid_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Catch the regridded stores up with the native store over the partition."""
    import pandas as pd  # noqa: PLC0415

    from planetary_datasets.providers.mars_icechunk import (  # noqa: PLC0415
        MARSRegridProvider,
        build_provider,
        run_regrids,
    )

    start, end = _month(context)
    native = build_provider()
    targets = [MARSRegridProvider(native, resolution=r) for r in RESOLUTIONS]

    axis = pd.DatetimeIndex(targets[0].native_dataset()["time"].values)
    # `end` is midnight on the month's last day; the partition runs to 23:00
    # on it, and the next hour belongs to the next partition.
    stop = pd.Timestamp(end) + pd.Timedelta("1D")
    wanted = axis[(axis >= pd.Timestamp(start)) & (axis < stop)]
    context.log.info(f"Checking {len(wanted)} timestep(s) against {len(targets)} target store(s)")
    # No --follow: the asset covers a closed month, so there is nothing more
    # coming that a poll would pick up.
    run_regrids(targets, times=wanted)

    return dg.MaterializeResult(
        metadata={
            **_store_metadata([native.icechunk_path, *(t.icechunk_path for t in targets)]),
            "timesteps_checked": dg.MetadataValue.int(len(wanted)),
        }
    )
