"""Report the last week of coverage for the SILAM, MEPS, HARMONIE and AROME stores.

    pixi run python _status_lam.py            # the last 8 days
    pixi run python _status_lam.py --days 14

Read-only. For each store: the append-dimension range and step count, whether the axis is
sorted, and which expected instants of the recent window are absent. Where the store also
carries an ``init_time`` coordinate alongside an hourly ``time`` axis, both are reported,
because a cycle can be present as an init time while some of its steps are missing.

Run from the repository root so `planetary_datasets` resolves here rather than to the
editable install's worktree; the resolved path is printed to prove it.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
from dataclasses import dataclass
from typing import Callable

import pandas as pd
import xarray as xr

import planetary_datasets
from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.arome import (
    OVERSEAS_REGIONS,
    AromeFranceHDProvider,
    AromeFranceProvider,
    AromeOverseasProvider,
)
from planetary_datasets.providers.dmi_harmonie import (
    DMIHarmonieModelLevelProvider,
    DMIHarmonieProvider,
)
from planetary_datasets.providers.meps import MEPSDetProvider
from planetary_datasets.providers.silam import SILAMAerosolProvider, SILAMDustProvider

NOW = pd.Timestamp.utcnow().tz_localize(None).floor("h")


@dataclass(frozen=True)
class Store:
    """One store to check.

    Attributes:
        label: Name used in the report.
        factory: Builds the provider. Construction touches neither network nor store.
        freq: Spacing the append dimension is expected to have.
        cycle_freq: Spacing of init times, when the store carries them.
        lag: Hours a cycle needs before it is published; the window ends there rather
            than at the present, so a run still being written is not called a gap.
    """

    label: str
    factory: Callable[[], BaseProvider]
    freq: str
    cycle_freq: str | None
    lag: int


STORES: tuple[Store, ...] = (
    Store("silam_dust", SILAMDustProvider, "1D", None, 24),
    Store("silam_aerosol", SILAMAerosolProvider, "1D", None, 24),
    Store("meps_model_level", MEPSDetProvider, "1h", "3h", 3),
    # HARMONIE's `time` is the cycle, not a valid time: the three forecast hours of each
    # run sit on a separate `step` dim, so the axis advances three-hourly, not hourly.
    Store("dmi_harmonie", DMIHarmonieProvider, "3h", None, 3),
    Store("dmi_harmonie_model_level", DMIHarmonieModelLevelProvider, "3h", None, 3),
    Store("arome_france_0025", AromeFranceProvider, "1h", "3h", 4),
    Store("arome_france_hd", AromeFranceHDProvider, "1h", "3h", 4),
    *(
        Store(
            f"arome_{OVERSEAS_REGIONS[region]}",
            lambda region=region: AromeOverseasProvider(region=region),
            "1h",
            "6h",
            4,
        )
        for region in OVERSEAS_REGIONS
    ),
)


def _runs(missing: pd.DatetimeIndex, step: pd.Timedelta) -> list[tuple]:
    """Collapse missing instants into contiguous runs, so a gap reads as one range."""
    if len(missing) == 0:
        return []
    breaks = (missing.to_series().diff() != step).cumsum()
    grouped = missing.to_series().groupby(breaks.values).agg(["min", "max", "count"])
    return [(row["min"], row["max"], int(row["count"])) for _, row in grouped.iterrows()]


def _axis_report(name: str, values, freq: str, start, end) -> list[str]:
    """Lines describing one axis: its extent, its order, and its gaps in [start, end]."""
    times = pd.DatetimeIndex(pd.Series(values).sort_values().unique())
    raw = pd.DatetimeIndex(values)
    order = "sorted" if raw.is_monotonic_increasing else "UNSORTED"
    lines = [f"  {name}: {len(raw)} steps  {raw.min()} -> {raw.max()}  ({order})"]

    window_start = max(start, times.min())
    if window_start > end:
        lines.append(f"  {name}: store ends before the window ({times.max()} < {start})")
        return lines
    expected = pd.date_range(window_start.ceil(freq), end, freq=freq)
    stored = set(times)
    missing = pd.DatetimeIndex([t for t in expected if t not in stored])
    if len(missing) == 0:
        lines.append(f"  {name}: complete, {len(expected)} of {len(expected)} in window")
        return lines
    lines.append(f"  {name}: MISSING {len(missing)} of {len(expected)} in window")
    for lo, hi, count in _runs(missing, pd.Timedelta(freq))[:12]:
        lines.append(f"      {lo} -> {hi}  ({count})")
    return lines


def report(store: Store, days: int) -> str:
    """Check one store and return its block of the report."""
    provider = store.factory()
    lines = [f"\n=== {store.label}  ({provider.store_path})"]
    try:
        session = provider.get_icechunk_repo().readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
    except Exception as exc:  # noqa: BLE001 - a missing or damaged store is a finding
        lines.append(f"  unreadable: {type(exc).__name__}: {str(exc)[:160]}")
        return "\n".join(lines)

    end = NOW - pd.Timedelta(hours=store.lag)
    start = end - pd.Timedelta(days=days)
    dim = provider.append_dim
    lines += _axis_report(dim, ds[dim].values, store.freq, start, end)

    # A cycle can be present as an init time while some of its steps are missing, so the
    # two axes are reported separately rather than one being inferred from the other.
    if store.cycle_freq and "init_time" in ds.coords and "init_time" != dim:
        cycle_end = end.floor(store.cycle_freq)
        lines += _axis_report(
            "init_time", ds["init_time"].values, store.cycle_freq, start, cycle_end
        )
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=8, help="how far back to check")
    parser.add_argument("--only", nargs="*", help="labels to check, default all")
    args = parser.parse_args()

    selected = [s for s in STORES if not args.only or s.label in args.only]
    print(f"package: {planetary_datasets.__file__}")
    print(f"now    : {NOW}   window: last {args.days} days")

    # Each check is network-bound and independent; serially this is minutes of waiting.
    with cf.ThreadPoolExecutor(max_workers=6) as pool:
        for block in pool.map(lambda s: report(s, args.days), selected):
            print(block)
