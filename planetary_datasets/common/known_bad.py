"""Partitions that can never be written, because the source itself is broken.

A backfill retries a failed partition on every pass, which is right for an outage and
wrong for a defect upstream: an unreadable GRIB or a run with impossible times fails the
same way forever and fills the logs. Such partitions are listed, each with the reason, in
``planetary_datasets/known_bad.json``, keyed by lane (or ``<lane>/<collection>``), and the
backfills leave them out. Remove an entry to have it tried again: the list is read afresh at
each pass, so a lane that follows its source picks the change up without a restart.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd

KNOWN_BAD = pathlib.Path(__file__).resolve().parents[1] / "known_bad.json"


def load(path: pathlib.Path = KNOWN_BAD) -> dict[str, dict[pd.Timestamp, str]]:
    """The whole skip list, read now: name -> partition -> reason.

    Take one snapshot per pass and look every name up in it, so a pass never mixes two
    versions of the list.

    Args:
        path: The skip list; the checked-in one by default.

    Returns:
        For each lane (or ``<lane>/<collection>``), naive timestamps, as the stores hold
        them, mapped to why each is left out. Empty if the file is absent.
    """
    if not path.is_file():
        return {}
    table = json.loads(path.read_text())
    return {
        name: {pd.Timestamp(when): why for when, why in entries.items()}
        for name, entries in table.items()
    }


def known_bad(name: str, path: pathlib.Path = KNOWN_BAD) -> dict[pd.Timestamp, str]:
    """Partition -> reason for the partitions of ``name`` that are never to be written.

    Args:
        name: The lane, or ``<lane>/<collection>`` for a lane writing several collections.
        path: The skip list; the checked-in one by default.

    Returns:
        Naive timestamps, as the stores hold them, mapped to why each is left out.
    """
    return load(path).get(name, {})


__all__ = ["KNOWN_BAD", "known_bad", "load"]
