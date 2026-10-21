"""``_backfill_kenda.discard_stored``: staged hours go once both KENDA stores hold them."""

from __future__ import annotations

import pandas as pd

import _backfill_kenda


class FakeStore:
    def __init__(self, missing):
        self.missing = {pd.Timestamp(t) for t in missing}

    def missing_timesteps(self, hours):
        return [h for h in hours if h in self.missing]


def stage(archive, stamp: str) -> list:
    paths = [archive / f"kenda-ch1-{stamp}-0-t_2m-ctrl{ext}" for ext in (".grib2", ".sha256")]
    for path in paths:
        path.write_bytes(b"x")
    return paths


def test_only_hours_in_both_stores_are_discarded(tmp_path):
    done = stage(tmp_path, "202610200000")
    half = stage(tmp_path, "202610200100")
    constants = tmp_path / "horizontal_constants_kenda-ch1.grib2"
    constants.write_bytes(b"x")

    stores = (FakeStore([]), FakeStore(["2026-10-20T01:00"]))
    assert _backfill_kenda.discard_stored(stores, tmp_path) == 2

    assert not any(p.exists() for p in done)
    assert all(p.exists() for p in half)
    assert constants.exists()
