"""Timestamp normalisation shared by the providers in this package.

Dagster hands partition starts over as timezone-aware ``datetime`` objects, while the
times stored in icechunk are naive and implicitly UTC. Comparing the two never matches, so
a partition looks perpetually missing and is rewritten on every run. Every provider entry
point that accepts a partition timestamp puts it through :func:`to_naive_utc` first.
"""

from __future__ import annotations

import pandas as pd


def to_naive_utc(value) -> pd.Timestamp:
    """Return ``value`` as a timezone-naive UTC :class:`pandas.Timestamp`.

    Aware inputs are converted to UTC and then stripped of their offset; naive inputs are
    assumed to already be UTC and returned unchanged.
    """
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


class NaiveUTCPartitions:
    """Mixin that normalises partition timestamps before the base provider sees them.

    Mixed in ahead of :class:`~planetary_datasets.base.BaseProvider` so the
    "is this partition already stored?" check, which runs before ``fetch``, compares
    like with like.
    """

    def run_partition(self, it, check_present: bool = True) -> bool:
        return super().run_partition(to_naive_utc(it), check_present=check_present)

    def run_range(self, timestamps) -> int:
        return super().run_range(pd.DatetimeIndex([to_naive_utc(t) for t in timestamps]))
