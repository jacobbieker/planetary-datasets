"""Partitions that can never be written, because the source itself is broken.

A backfill retries a failed partition on every pass, which is right for an outage and
wrong for a defect upstream: an unreadable GRIB or a run with impossible times fails the
same way forever and fills the logs. Such partitions are listed, each with the reason, in
``planetary_datasets/known_bad.json``, keyed by lane (or ``<lane>/<collection>``), and the
backfills leave them out. Remove an entry to have it tried again.
"""

from __future__ import annotations

import functools
import json
import pathlib

import pandas as pd

KNOWN_BAD = pathlib.Path(__file__).resolve().parents[1] / "known_bad.json"


@functools.lru_cache(maxsize=None)
def _table(path: str) -> dict:
    p = pathlib.Path(path)
    return json.loads(p.read_text()) if p.is_file() else {}


def known_bad(name: str, path: pathlib.Path = KNOWN_BAD) -> dict[pd.Timestamp, str]:
    """Partition -> reason for the partitions of ``name`` that are never to be written.

    Args:
        name: The lane, or ``<lane>/<collection>`` for a lane writing several collections.
        path: The skip list; the checked-in one by default.

    Returns:
        Naive timestamps, as the stores hold them, mapped to why each is left out.
    """
    return {pd.Timestamp(when): why for when, why in _table(str(path)).get(name, {}).items()}


__all__ = ["KNOWN_BAD", "known_bad"]
