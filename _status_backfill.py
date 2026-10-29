"""Report coverage of the OPERA, GMGSI v3 and GEOS-CF v2 stores.

    python _status_backfill.py

Read-only. For each store, print the append-dimension range, the step count, whether the
axis is sorted, and the hours/instants still missing between the store's first step and
now. Run from the repository root so `planetary_datasets` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

import pandas as pd
import xarray as xr

import planetary_datasets
from planetary_datasets.providers.geos import GEOSProvider
from planetary_datasets.providers.gmgsi import GMGSIProvider
from planetary_datasets.providers.opera import (
    OPERARainfallProvider,
    OPERAReflectivityProvider,
)

NOW = pd.Timestamp.utcnow().tz_localize(None).floor("h")


def report(label: str, provider, freq: str, lag_hours: int) -> None:
    """Print the coverage of one store, and the gaps up to `NOW` minus `lag_hours`."""
    print(f"\n=== {label}  ({provider.store_path})")
    try:
        repo = provider.get_icechunk_repo()
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
    except Exception as exc:  # noqa: BLE001 - a missing or damaged store is a finding
        print(f"  unreadable: {type(exc).__name__}: {str(exc)[:120]}")
        return

    dim = provider.append_dim
    times = pd.DatetimeIndex(ds[dim].values)
    sorted_note = "sorted" if times.is_monotonic_increasing else "UNSORTED"
    print(f"  {len(times):7d} steps  {times.min()} -> {times.max()}  ({sorted_note})")

    end = NOW - pd.Timedelta(hours=lag_hours)
    desired = pd.date_range(times.min(), end, freq=freq)
    stored = set(times)
    missing = pd.DatetimeIndex([t for t in desired if t not in stored])
    if len(missing) == 0:
        print(f"  no gaps through {end}")
        return

    interior = missing[missing < times.max()]
    trailing = missing[missing >= times.max()]
    print(f"  missing {len(missing)} of {len(desired)} steps through {end}")
    print(
        f"    interior gaps : {len(interior)}"
        + (f"  {interior.min()} -> {interior.max()}" if len(interior) else "")
    )
    print(
        f"    trailing (tail): {len(trailing)}"
        + (f"  {trailing.min()} -> {trailing.max()}" if len(trailing) else "")
    )

    # The contiguous runs of missing steps, so a backfill range can be read off directly.
    if len(missing):
        step = pd.Timedelta(freq)
        breaks = (missing.to_series().diff() != step).cumsum()
        runs = missing.to_series().groupby(breaks.values).agg(["min", "max", "count"])
        print(f"    {len(runs)} contiguous run(s); largest first:")
        for _, row in runs.sort_values("count", ascending=False).head(12).iterrows():
            print(f"      {row['min']} -> {row['max']}  ({row['count']} steps)")


if __name__ == "__main__":
    print(f"package: {planetary_datasets.__file__}")
    print(f"now    : {NOW}")
    # GMGSI lands ~40 min after the hour; the assets hold back two hours, so do the same.
    report("gmgsi_v3", GMGSIProvider(), "1h", lag_hours=2)
    report("opera_rainfall", OPERARainfallProvider(), "1h", lag_hours=1)
    report("opera_dbz", OPERAReflectivityProvider(), "1h", lag_hours=1)
    report("geos_cf_v2_15min", GEOSProvider(version=2), "15min", lag_hours=0)
