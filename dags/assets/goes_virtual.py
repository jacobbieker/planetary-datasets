"""Dagster assets for the GOES ABI-L1b-RadF virtual-reference ingest.

One asset per satellite, partitioned by ABI channel. Each materialization runs
the backwards ingest for its channel: it anchors on the run's end date, walks
the archive back probing whether each day still combines with the anchor, and
writes each codec era into its own Icechunk store. ``--max-eras 1`` is the
default here, so a scheduled run keeps the newest era current and a backfill of
the older eras is an explicit, separately-tagged run.

These are by far the heaviest assets in the deployment — a single channel-era
can be years of full-disk scans — so they carry a long ``dagster/max_runtime``
and their own concurrency key rather than sharing the general satellite one.
The ingest manages its own memory (manifest splitting, per-batch repository
reopen, ``malloc_trim``), which is why it is driven directly rather than
through ``BaseProvider``.

Configuration comes from ``planetary_datasets.config``: the destination store
is ``ICECHUNK_BUCKET``/``ICECHUNK_PREFIX``, or a local directory when
``ICECHUNK_LOCAL_PATH`` is set. Pin ``GOES_VIRTUAL_END_DATE`` as well — the
anchor date names the newest era's store, so an unpinned deployment writes a
new store every day instead of extending yesterday's.
"""

# No `from __future__ import annotations` here: Dagster validates the
# `context` parameter against the *unevaluated* annotation, so stringified
# annotations make it reject the asset.

import datetime as dt
import os
from typing import Optional

import dagster as dg
from dagster import AssetExecutionContext

from planetary_datasets.config import get_config
from planetary_datasets.providers.virtualized import goes_radf_common as common
from planetary_datasets.providers.virtualized import ingest_goes_radf

SATELLITES = ("goes16", "goes17", "goes18", "goes19")

#: ABI channels, as the partition keys. Stored zero-padded so the partition
#: list sorts the way a human reads it.
CHANNELS = tuple(f"C{ch:02d}" for ch in range(1, 17))

CONCURRENCY_KEY = "goes-virtual-ingest"

#: 24h. An era spanning several years of full-disk scans commits a day at a
#: time and legitimately runs for hours; the ingest resumes from its last
#: commit, so a run cut short here costs at most one batch.
MAX_RUNTIME_SECONDS = 24 * 60 * 60

channel_partitions = dg.StaticPartitionsDefinition(list(CHANNELS))


def _channel_number(partition_key: str) -> int:
    """Parse an ABI channel number out of a ``C13``-style partition key."""
    try:
        channel = int(partition_key.lstrip("Cc"))
    except ValueError:
        raise ValueError(f"Bad channel partition key: {partition_key!r}") from None
    if not 1 <= channel <= 16:
        raise ValueError(f"Channel must be 1-16, got {channel}")
    return channel


def _pinned_anchor() -> Optional[dt.date]:
    """The anchor date pinned for this deployment, if there is one.

    The anchor names the newest era's store, so it has to stay the same from
    one run to the next: a run anchored on a new date mints a fresh set of
    stores and re-ingests the era from nothing instead of resuming. Pin it
    with ``GOES_VIRTUAL_END_DATE`` (``.env`` works, since the config loads it)
    and only move it when you intend to start a new set.
    """
    get_config()  # ensures .env has been loaded into the environment
    raw = os.environ.get("GOES_VIRTUAL_END_DATE", "").strip()
    return dt.date.fromisoformat(raw) if raw else None


def _run_options(tags: dict) -> tuple[Optional[dt.date], Optional[int], int]:
    """Read the per-run overrides off the run tags.

    ``end_date`` is None when neither the run nor the deployment pinned one;
    the caller decides what to do about that. ``max_eras`` defaults to 1 so a
    scheduled run only keeps the newest era current — backfilling the older
    ones is an explicit run tagged ``goes_virtual/max_eras: all``.
    """
    end_date = _pinned_anchor()
    max_eras: Optional[int] = 1
    batch_size = 1
    for key, value in (tags or {}).items():
        if key == "goes_virtual/end_date":
            end_date = dt.date.fromisoformat(value)
        elif key == "goes_virtual/max_eras":
            max_eras = None if value.strip().lower() in ("all", "none", "") else int(value)
        elif key == "goes_virtual/batch_size":
            batch_size = int(value)
    return end_date, max_eras, batch_size


def _run_ingest(
    context: AssetExecutionContext,
    satellite: str,
) -> dg.MaterializeResult:
    channel = _channel_number(context.partition_key)
    cfg = get_config()

    end_date, max_eras, batch_size = _run_options(getattr(context.run, "tags", {}))
    if end_date is None:
        end_date = dt.date.today()
        context.log.warning(
            "No anchor date pinned, falling back to today "
            f"({end_date.isoformat()}). The anchor names the newest era's "
            "store, so an unpinned run writes a new store every day and "
            "re-ingests the era from nothing. Set GOES_VIRTUAL_END_DATE, or "
            "tag the run goes_virtual/end_date, to resume the existing store."
        )

    args = ingest_goes_radf.build_args(
        satellite,
        end_date=end_date,
        max_eras=max_eras,
        batch_size=batch_size,
    )
    base_prefix = common.default_store_prefix(satellite)
    context.log.info(
        f"{satellite} C{channel:02d}: backwards from {end_date.isoformat()}, "
        f"max_eras={max_eras}, store base {cfg.store_path(base_prefix)}"
    )

    # Bound glibc arena growth before any subprocess: the long backfills leak
    # RSS into retained arenas otherwise. See goes_radf_common.REOPEN_REPO_EVERY.
    common.configure_ingest_process()

    suffixes = ingest_goes_radf.ingest_channel_from_args(args, channel)
    stores = [
        cfg.store_path(common.suffixed_prefix(base_prefix, channel=channel, era=s))
        for s in suffixes
        if s is not None
    ]

    return dg.MaterializeResult(
        metadata={
            "satellite": satellite,
            "channel": f"C{channel:02d}",
            "end_date": end_date.isoformat(),
            "eras_ingested": len(suffixes),
            "era_suffixes": ", ".join(str(s) for s in suffixes) or "none",
            "stores": dg.MetadataValue.md(
                "\n".join(f"- `{s}`" for s in stores) or "_no store written_"
            ),
        }
    )


def _build_asset(satellite: str):
    @dg.asset(
        name=f"{satellite}_radf_virtual",
        key_prefix=["satellite", "virtualized"],
        group_name="goes_virtual",
        description=(
            f"Virtual-reference Icechunk store of {satellite.upper()} "
            "ABI-L1b-RadF full-disk radiance, one store per codec era."
        ),
        partitions_def=channel_partitions,
        compute_kind="virtualizarr",
        op_tags={
            "dagster/concurrency_key": CONCURRENCY_KEY,
            "dagster/max_runtime": str(MAX_RUNTIME_SECONDS),
        },
    )
    def _asset(context: AssetExecutionContext) -> dg.MaterializeResult:
        return _run_ingest(context, satellite)

    return _asset


goes16_radf_virtual = _build_asset("goes16")
goes17_radf_virtual = _build_asset("goes17")
goes18_radf_virtual = _build_asset("goes18")
goes19_radf_virtual = _build_asset("goes19")

all_assets = [
    goes16_radf_virtual,
    goes17_radf_virtual,
    goes18_radf_virtual,
    goes19_radf_virtual,
]
