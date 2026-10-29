"""Launch Dagster backfills for the hours each store is actually missing.

    python _backfill_gaps.py                      # report only
    python _backfill_gaps.py --launch recent      # catch up to today
    python _backfill_gaps.py --launch historical  # the older interior gaps
    python _backfill_gaps.py --launch all

A backfill here is an explicit list of partition keys, not a range: these stores were
written out of order by the original scripts and are mostly full, so a range would spend
thousands of runs rediscovering hours that are already stored. The keys are the
intersection of two things — what the asset's own ``PartitionsDefinition`` currently
admits, and what the store does not already hold — so a key is never submitted that
Dagster would reject or that the provider would skip.

``recent`` is each store's trailing gap: everything after the last hour it holds. That is
the "catch up to today" half, and the data is certain to exist upstream. ``historical`` is
the interior gaps behind that point. They are split because they carry very different
risk: an OPERA hour the archive never published still costs the single download pool three
retries at a 30-minute delay before it gives up, and the interior gaps are the ones most
likely to be empty upstream — that is often why they are gaps.

Run from the repository root so `planetary_datasets` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

import argparse
import sys

import pandas as pd
import requests
import xarray as xr

import planetary_datasets

GRAPHQL = "http://127.0.0.1:3000/graphql"

LAUNCH = """
mutation($params: LaunchBackfillParams!) {
  launchPartitionBackfill(backfillParams: $params) {
    __typename
    ... on LaunchBackfillSuccess { backfillId }
    ... on PythonError { message }
    ... on InvalidStepError { invalidStepKey }
    ... on PartitionKeysNotFoundError { message }
    ... on UnauthorizedError { message }
  }
}
"""


def stored_times(provider) -> pd.DatetimeIndex:
    """Every step already in the provider's store."""
    repo = provider.get_icechunk_repo()
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    return pd.DatetimeIndex(ds[provider.append_dim].values)


def hourly_gaps(provider, partition_keys: list[str]) -> tuple[list[str], list[str]]:
    """Split an hourly asset's admissible keys into (recent, historical) missing ones.

    ``recent`` is the keys after the store's last stored hour, ``historical`` the keys
    before it that are absent.
    """
    times = stored_times(provider)
    stored = set(times)
    last = times.max()
    recent, historical = [], []
    for key in partition_keys:
        it = key_to_timestamp(key)
        if it in stored:
            continue
        (recent if it > last else historical).append(key)
    return recent, historical


def key_to_timestamp(key: str) -> pd.Timestamp:
    """Dagster's hourly key (``2026-07-22-15:00``) or daily key as a naive timestamp."""
    return pd.Timestamp(key[:10] + " " + key[11:]) if len(key) > 10 else pd.Timestamp(key)


def geos_day_gaps(provider, partition_keys: list[str]) -> tuple[list[str], list[str]]:
    """Split GEOS-CF daily keys into (recent, historical) days that are not yet complete.

    A day counts as missing when any of its 96 instants is absent; ``run_day`` fills only
    what is missing, so re-running a partly-filled day is cheap.
    """
    times = stored_times(provider)
    stored = set(times)
    last = times.max()
    recent, historical = [], []
    for key in partition_keys:
        day = pd.Timestamp(key)
        instants = pd.date_range(day, day + pd.Timedelta("1D"), freq="15min", inclusive="left")
        if all(instant in stored for instant in instants):
            continue
        (recent if day >= last.normalize() else historical).append(key)
    return recent, historical


def launch(title: str, asset_paths: list[list[str]], keys: list[str], dry_run: bool) -> None:
    """Submit one backfill over `keys` for `asset_paths`."""
    if not keys:
        print(f"  {title}: nothing to do")
        return
    print(f"  {title}: {len(keys)} partitions, {keys[0]} -> {keys[-1]}")
    if dry_run:
        return
    params = {
        "assetSelection": [{"path": path} for path in asset_paths],
        "partitionNames": keys,
        "fromFailure": False,
        "tags": [{"key": "planetary/backfill_reason", "value": "gap_fill"}],
        "title": title,
    }
    response = requests.post(GRAPHQL, json={"query": LAUNCH, "variables": {"params": params}})
    response.raise_for_status()
    payload = response.json()["data"]["launchPartitionBackfill"]
    if payload["__typename"] != "LaunchBackfillSuccess":
        print(f"    FAILED {payload['__typename']}: {str(payload.get('message'))[:300]}")
        return
    print(f"    launched backfill {payload['backfillId']}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", choices=["recent", "historical", "all"], default=None)
    parser.add_argument(
        "--only", action="append", default=None, help="restrict to these store labels (repeatable)"
    )
    parser.add_argument("--since", default=None, help="skip partitions before this timestamp")
    args = parser.parse_args(argv[1:])
    dry_run = args.launch is None
    want_recent = args.launch in ("recent", "all")
    want_historical = args.launch in ("historical", "all")

    from dags.assets.geos import v2_partitions_def
    from dags.assets.gmgsi import gmgsi_v3_partitions
    from dags.assets.radar import opera_dbz_partitions, opera_rainfall_partitions
    from planetary_datasets.providers.geos import GEOSProvider
    from planetary_datasets.providers.gmgsi import GMGSIProvider
    from planetary_datasets.providers.opera import (
        OPERARainfallProvider,
        OPERAReflectivityProvider,
    )

    print(f"package: {planetary_datasets.__file__}")
    print(f"now    : {pd.Timestamp.utcnow().tz_localize(None)}")
    print(f"mode   : {'report only' if dry_run else args.launch}\n")

    jobs = [
        ("gmgsi_v3", GMGSIProvider(), gmgsi_v3_partitions, [["gmgsi", "gmgsi_v3"]], hourly_gaps),
        (
            "opera_rainfall",
            OPERARainfallProvider(),
            opera_rainfall_partitions,
            [["radar", "opera_rainfall_download"], ["radar", "opera_rainfall"]],
            hourly_gaps,
        ),
        (
            "opera_dbz",
            OPERAReflectivityProvider(),
            opera_dbz_partitions,
            [["radar", "opera_dbz_download"], ["radar", "opera_dbz"]],
            hourly_gaps,
        ),
        (
            "geos_cf_v2_15min",
            GEOSProvider(version=2),
            v2_partitions_def,
            [["nwp", "geos_cf_v2_15min"]],
            geos_day_gaps,
        ),
    ]

    for label, provider, partitions_def, asset_paths, splitter in jobs:
        if args.only and label not in args.only:
            continue
        keys = list(partitions_def.get_partition_keys())
        if args.since:
            # OPERA's older gaps are holes in the upstream archive, not in our ingest: the
            # downloader cannot fill them and each attempt costs the single download pool
            # three retries at a 30 minute delay. `--since` aims a backfill at the gaps
            # that are an ingest outage instead, which do have data behind them.
            floor = pd.Timestamp(args.since)
            keys = [key for key in keys if key_to_timestamp(key) >= floor]
        recent, historical = splitter(provider, keys)
        print(f"=== {label}  ({len(keys)} admissible partitions, {keys[0]} -> {keys[-1]})")
        if want_recent or dry_run:
            launch(f"{label} catch-up", asset_paths, recent, dry_run or not want_recent)
        if want_historical or dry_run:
            launch(
                f"{label} historical gaps", asset_paths, historical, dry_run or not want_historical
            )
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
