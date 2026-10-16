"""Offline tests for the virtual HRRR Alaska / NAM Hawaii stores.

Parsing real GRIB needs NOAA's buckets, so these cover the pieces around it: file URLs,
placing messages on the store's fixed step and level axes, the schema check that decides a
generation, and listing generations without creating any.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from planetary_datasets.providers.virtualized import noaa_nests
from planetary_datasets.providers.virtualized.noaa_nests import (
    PRODUCTS,
    NestVirtualProvider,
    _manifest_array,
    _misfits,
    _Schema,
    _Var,
)


def test_product_urls():
    it = pd.Timestamp("2026-10-14T03:00")
    assert PRODUCTS["hrrr_alaska_sfc"].url(it, 7) == (
        "s3://noaa-hrrr-bdp-pds/hrrr.20261014/alaska/hrrr.t03z.wrfsfcf07.ak.grib2"
    )
    assert PRODUCTS["nam_hawaii"].url(it, 60) == (
        "s3://noaa-nam-pds/nam.20261014/nam.t03z.hawaiinest.hiresf60.tm00.grib2"
    )


def test_stores_sit_under_the_nwp_virtualized_prefix():
    assert {p.store_prefix for p in PRODUCTS.values()} == {
        "bkr/nwp/virtualized/hrrr_alaska_sfc.icechunk",
        "bkr/nwp/virtualized/hrrr_alaska_prs.icechunk",
        "bkr/nwp/virtualized/hrrr_alaska_nat.icechunk",
        "bkr/nwp/virtualized/nam_hawaii.icechunk",
    }
    assert PRODUCTS["hrrr_alaska_nat"].steps == tuple(range(49))
    assert PRODUCTS["nam_hawaii"].steps == tuple(range(61))


def test_messages_land_on_their_step_and_level_and_gaps_stay_empty():
    cells = {
        (0, (850.0,)): ("s3://b/f00", 10, 100),
        (2, (500.0,)): ("s3://b/f02", 20, 200),
        (99, (500.0,)): ("s3://b/f99", 30, 300),  # a step outside the store's axis
    }
    ma = _manifest_array(
        "tmp", ("isobar", "y", "x"), {}, (3, 4), (0, 1, 2), [np.array([500.0, 850.0])], cells
    )
    assert ma.shape == (1, 3, 2, 3, 4)
    assert ma.metadata.chunks == (1, 1, 1, 3, 4)
    paths = np.asarray(ma.manifest._paths)
    assert paths[0, 0, 1, 0, 0] == "s3://b/f00"  # step 0, 850 hPa
    assert paths[0, 2, 0, 0, 0] == "s3://b/f02"  # step 2, 500 hPa
    assert paths[0, 1, 0, 0, 0] == "" and paths[0, 1, 1, 0, 0] == ""  # step 1: missing
    assert int(np.asarray(ma.manifest._offsets)[0, 2, 0, 0, 0]) == 20


def _schema(levels=(500.0, 850.0)):
    return _Schema(
        arrays={
            ("/isobar/instant", "tmp"): {
                "dims": ("isobar", "y", "x"),
                "attrs": {},
                "spatial": (3, 4),
            }
        },
        levels={("/isobar/instant", "isobar"): np.array(levels)},
    )


def test_a_run_inside_the_schema_fits():
    var = _Var(
        dims=("isobar", "y", "x"), attrs={}, levels={"isobar": np.array([850.0])}, spatial=(3, 4)
    )
    assert _misfits({("/isobar/instant", "tmp"): var}, _schema()) == []


@pytest.mark.parametrize(
    ("key", "var", "reason"),
    [
        (
            ("/sfc/instant", "smoke"),
            _Var(dims=("y", "x"), attrs={}, levels={}, spatial=(3, 4)),
            "not in the store",
        ),
        (
            ("/isobar/instant", "tmp"),
            _Var(
                dims=("isobar", "y", "x"),
                attrs={},
                levels={"isobar": np.array([250.0])},
                spatial=(3, 4),
            ),
            "outside the store's axis",
        ),
        (
            ("/isobar/instant", "tmp"),
            _Var(
                dims=("isobar", "y", "x"),
                attrs={},
                levels={"isobar": np.array([500.0])},
                spatial=(5, 4),
            ),
            "dims",
        ),
    ],
    ids=["new-variable", "new-level", "new-grid"],
)
def test_a_run_with_something_new_does_not_fit(key, var, reason):
    misfits = _misfits({key: var}, _schema())
    assert len(misfits) == 1 and reason in misfits[0]


def test_generations_are_listed_without_creating_any(local_config):
    provider = NestVirtualProvider("hrrr_alaska_sfc", config=local_config)
    assert provider.generations() == ["bkr/nwp/virtualized/hrrr_alaska_sfc.icechunk"]
    parent = local_config.local_store_path("bkr/nwp/virtualized")
    assert not parent.exists() or not any(parent.iterdir())

    parent.mkdir(parents=True, exist_ok=True)
    for name in (
        "hrrr_alaska_sfc_02122020.icechunk",
        "hrrr_alaska_sfc_14102026.icechunk",
        "other.icechunk",
    ):
        (parent / name).mkdir()
    assert provider.generations() == [
        "bkr/nwp/virtualized/hrrr_alaska_sfc.icechunk",
        "bkr/nwp/virtualized/hrrr_alaska_sfc_02122020.icechunk",
        "bkr/nwp/virtualized/hrrr_alaska_sfc_14102026.icechunk",
    ]


def test_nothing_stored_means_every_run_is_missing(local_config):
    provider = NestVirtualProvider("nam_hawaii", config=local_config)
    runs = pd.date_range("2021-09-16", periods=3, freq="6h")
    assert provider.missing_timesteps(runs) == list(runs)
    parent = local_config.local_store_path("bkr/nwp/virtualized")
    assert not parent.exists() or not any(parent.iterdir())


def test_the_ledger_attribute_name_is_stable():
    assert noaa_nests.LEDGER_ATTR == "ingested_runs"
