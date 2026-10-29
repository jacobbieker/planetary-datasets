"""Rebase PACE times stored as seconds of each granule's own day, and restore their names.

    python scripts/repair_pace_day_seconds.py [--dry-run] [PREFIX ...]

PACE stamps times such as ``nadir_view_time`` and ``att_time`` as seconds since the day
the granule starts on. Appended granule by granule under the first granule's units, every
row read as that one day. This rewrites each such array as seconds since
:data:`~planetary_datasets.providers.polar.pace.TIME_EPOCH`, taking each row's day from
its granule's ``time_coverage_start``, and moves arrays the naming pass called
``*_bin_was_viewed_at_nadir_view`` back to ``*_nadir_view_time``.

A value already past two days of seconds has been rebased (by a writer with the fix, or an
earlier run) and is left alone, as are fill values, so the script can be re-run. Stop the
stores' writers first: the arrays are rewritten whole.
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

from planetary_datasets.config import get_config
from planetary_datasets.providers.polar.pace import TIME_EPOCH, day_epoch

STORES = ("bkr/polar/pace_l2.icechunk", "bkr/polar/pace_virtual.icechunk")
#: PACE's own ``valid_max`` for these times: a granule crossing midnight runs past a day.
DAY_LIMIT = 2 * 86400
BAD_NAME = "_bin_was_viewed_at_nadir_view"
GOOD_NAME = "_nadir_view_time"


def _fill(array) -> float | None:
    from xarray.backends.zarr import FillValueCoder

    value = array.attrs.get("_FillValue")
    if value is None:
        return None
    if isinstance(value, str):
        return float(FillValueCoder.decode(value, array.dtype))
    return float(value)


def _row_days(root, array) -> np.ndarray:
    """Each row's granule day, as seconds from :data:`TIME_EPOCH`."""
    first = array.metadata.dimension_names[0]
    prefix, _, kind = first.rpartition("_")
    if first.endswith("_along_track"):
        prefix, kind = first[: -len("_along_track")], "along_track"
    starts = pd.to_datetime([str(s) for s in root[f"{prefix}_time_coverage_start"][:]], utc=True)
    days = (starts.normalize().tz_localize(None) - pd.Timestamp(TIME_EPOCH)).total_seconds()
    days = np.asarray(days, dtype="float64")
    if kind == "granule":
        return days
    if kind == "along_track":
        return days[root[f"{prefix}_granule_index"][:]]
    raise ValueError(f"{first}: neither a granule nor an along-track dimension")


def repair(prefix: str, dry_run: bool = False) -> list[str]:
    """Repair one store. Returns a line per array touched."""
    import zarr

    repo = get_config().icechunk_repo(prefix)
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="a")
    report, moves = [], {}
    for name, array in root.arrays():
        if name.endswith(BAD_NAME):
            moves[name] = name[: -len(BAD_NAME)] + GOOD_NAME
        if array.dtype.kind != "f" or day_epoch(dict(array.attrs)) is None:
            continue
        values = array[:]
        shift = _row_days(root, array).reshape((-1,) + (1,) * (values.ndim - 1))
        fill = _fill(array)
        stale = np.isfinite(values) & (np.abs(values) < DAY_LIMIT)
        if fill is not None:
            stale &= values != fill
        report.append(f"{prefix} {name}: {int(stale.sum()):,} of {values.size:,} value(s) rebased")
        if dry_run:
            continue
        array[:] = np.where(stale, values + shift, values)
        attrs = {k: v for k, v in array.attrs.items() if k not in ("valid_min", "valid_max")}
        attrs["units"] = f"seconds since {TIME_EPOCH}"
        array.attrs.clear()
        array.attrs.update(attrs)
    if moves:
        report += [f"{prefix} {old} -> {new}" for old, new in moves.items()]
        for old in moves:
            root[old].attrs.pop("source_name", None)
    if dry_run:
        return report
    if session.has_uncommitted_changes:
        session.commit("pace: day-relative times rebased to seconds since 1970-01-01")
    if moves:
        rearrange = repo.rearrange_session("main")
        for old, new in moves.items():
            rearrange.move(f"/{old}", f"/{new}")
        rearrange.commit(f"pace: {len(moves)} nadir view time(s) take their name back")
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("prefixes", nargs="*", default=list(STORES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    for prefix in args.prefixes:
        for line in repair(prefix, args.dry_run) or [f"{prefix}: nothing to repair"]:
            print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
