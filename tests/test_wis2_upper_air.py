"""WIS2 upper-air soundings: decoding real cache messages and the ragged store.

The fixtures are real WIS2 Global Cache objects (WMO core data, unrestricted): a NOAA
TEMP part A, a Thai PILOT bulletin of twelve subsets, one Hong Kong PILOT relayed by both
DWD and JMA, the HKO radar wind profiler, and a JMA bulletin that is not a sounding.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from collections import Counter

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_upper_air as ua

FIXTURES = pathlib.Path(__file__).parent / "data" / "wis2_upper_air"
ARRIVED = dt.datetime(2026, 10, 28, 20, tzinfo=dt.timezone.utc)

TEMP = "data/us-noaa-nws/data/core/weather/surface-based-observations/temp/temp_01001.bufr4"
PILOT_12 = (
    "data/de-dwd-gts-to-wis2/data/core/I/U/J/C/01/VTBB/"
    "A_IUJC01VTBB280600_C_EDZW_20261028064602_72568844"
)
VHHH_DWD = (
    "data/de-dwd-gts-to-wis2/data/core/I/U/W/C/01/VHHH/"
    "A_IUWC01VHHH281800_C_EDZW_20261028193505_72839955"
)
VHHH_JMA = (
    "data/jp-jma-gts-to-wis2/data/core/I/U/W/C/01/VHHH/"
    "A_IUWC01VHHH281800_C_RJTD_20261028193538_131.bufr"
)
PROFILER = (
    "data/hk-hko/data/core/weather/surface-based-observations/wind-profile-radar/"
    "profiler_45004.bufr4"
)
NOT_SOUNDING = (
    "data/jp-jma-gts-to-wis2/data/core/I/U/C/C/10/RJTD/"
    "A_IUCC10RJTD270000_C_RJTD_20261027000423_28.bufr"
)


def body(key: str) -> bytes:
    return (FIXTURES / key.rsplit("/", 1)[-1]).read_bytes()


def decode(key: str, counts: Counter | None = None) -> list[ua.Sounding]:
    return ua.decode_object(key, body(key), ARRIVED, counts if counts is not None else Counter())


@pytest.mark.parametrize(
    ("key", "wanted"),
    [
        (TEMP, True),
        (PILOT_12, True),
        (PROFILER, True),
        ("data/de-dwd-gts-to-wis2/data/core/I/U/K/A/01/EIDB/A_IUKA01EIDB280000_C_EDZW_1", True),
        ("data/jp-jma-gts-to-wis2/data/core/I/U/A/A/01/RJTD/A_IUAA01RJTD_x", False),  # aircraft
        ("data/jp-jma-gts-to-wis2/data/core/I/U/F/A/01/EUMG/A_IUFA01EUMG_x", False),  # AMVs
        ("data/de-dwd-gts-to-wis2/data/core/U/S/A/A/01/EIDB/A_USAA01EIDB_x", False),  # TAC
        ("data/bn-bdmd/data/core/weather/experimental/upper-air-observations/pibal/a.txt", False),
        (
            "data/uk-metoffice/data/core/weather/experimental/upper-air-observations/lightning/x",
            False,
        ),
        ("data/fr-meteofrance/data/core/weather/surface-based-observations/synop/x.bufr4", False),
    ],
)
def test_objects_are_picked_by_key(key, wanted):
    assert ua.wanted(key) is wanted


def test_a_temp_decodes_to_levels_in_si_units():
    (sounding,) = decode(TEMP)
    header, levels = sounding.header, sounding.levels
    assert header["station_id"] == header["wmo_station_id"] == "01001"
    assert header["data_sub_category"] == 4
    assert header["launch_time"] == np.datetime64("2026-10-27T18:00:00")
    assert header["latitude"] == 70.93972 and header["longitude"] == -8.66778
    assert header["source_centre"] == "us-noaa-nws"
    assert sounding.level_count == 13
    temperature = levels["temperature"][np.isfinite(levels["temperature"])]
    assert 200 < temperature.min() and temperature.max() < 300  # kelvin
    assert np.nanmax(levels["pressure"]) > 80_000  # pascals
    assert np.nanmin(levels["pressure"]) == 10_000


def test_every_subset_of_a_bulletin_is_a_sounding_and_nil_reports_are_dropped():
    counts = Counter()
    soundings = decode(PILOT_12, counts)
    assert counts["messages"] == 1
    assert len(soundings) + counts["no_levels"] == 12
    assert len({s.header["station_id"] for s in soundings}) == len(soundings)
    for sounding in soundings:
        assert sounding.header["data_sub_category"] == 1
        assert sounding.header["bulletin_heading"] == "IUJC01VTBB280600"
        assert np.isnan(sounding.levels["temperature"]).all()  # PILOT: winds only
        assert np.isfinite(sounding.levels["wind_speed"]).any()


def test_a_profiler_keeps_heights_and_winds():
    (sounding,) = decode(PROFILER)
    assert sounding.header["data_sub_category"] == 10
    assert sounding.header["wigos_station_id"] == "0-20000-0-45004"
    assert np.isfinite(sounding.levels["geopotential_height"]).all()
    assert np.isnan(sounding.levels["pressure"]).all()


def test_messages_that_are_not_soundings_are_counted_and_skipped():
    counts = Counter()
    assert decode(NOT_SOUNDING, counts) == []
    assert counts["other_category"] == 1


def test_garbage_never_raises():
    counts = Counter()
    truncated = body(TEMP)[:200]
    assert ua.decode_object("x", b"BUFR\x00\x00\x30\x04" + b"\xff" * 40, None, counts) == []
    assert ua.decode_object("y", b"not bufr at all", None, counts) == []
    assert ua.decode_object("z", truncated, None, counts) == []
    assert counts["not_bufr"] == 2  # the truncated message's length runs past the object
    assert counts["messages"] == 1


def test_relayed_copies_collapse_to_one_and_stored_ones_are_skipped():
    counts = Counter()
    dwd, jma = decode(VHHH_DWD), decode(VHHH_JMA)
    assert dwd[0].identity == jma[0].identity
    kept = ua.deduplicate(dwd + jma, set(), counts)
    assert len(kept) == 1 and counts["duplicate"] == 1
    assert ua.deduplicate(jma, {dwd[0].identity}, counts) == []
    assert counts["already_stored"] == 1


def test_parts_and_reissues_with_different_levels_are_not_duplicates():
    (sounding,) = decode(TEMP)
    other = ua.Sounding(dict(sounding.header), {k: v.copy() for k, v in sounding.levels.items()})
    other.levels["temperature"][0] += 0.5  # a corrected value
    assert other.identity != sounding.identity


def test_the_store_is_ragged_lossless_and_written_once_per_window(local_config):
    target = ua.UpperAirTarget(local_config)
    for key in (TEMP, PILOT_12, VHHH_DWD, VHHH_JMA, PROFILER, NOT_SOUNDING):
        # Fed regardless of wants(): the decoder must reject what is not a sounding itself.
        target.feed(key, body(key), ARRIVED)
    end = dt.datetime(2026, 10, 29, 0, tzinfo=dt.timezone.utc)
    window = (end - dt.timedelta(hours=12), end)
    summary = target.finish(*window, "wis2_20261029T0000Z")
    assert summary["duplicate"] == 1 and summary["other_category"] == 1

    repo = local_config.icechunk_repo(ua.STORE_PREFIX)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes["sounding"] == summary["soundings"]
    assert ds.sizes["level"] == int(ds["level_count"].sum())
    # Each sounding's levels are where first_level/level_count say, and point back to it.
    for row in range(ds.sizes["sounding"]):
        first, count = int(ds["first_level"][row]), int(ds["level_count"][row])
        assert (ds["sounding_index"].values[first : first + count] == row).all()
    temp = int(np.flatnonzero(ds["station_id"].values == "01001")[0])
    first = int(ds["first_level"][temp])
    (decoded,) = decode(TEMP)
    # Lossless to the BUFR decimal: float32 where it reads back as the same decimal.
    places = ds["temperature"].attrs["decimal_places"]
    np.testing.assert_array_equal(
        np.round(ds["temperature"].values[first : first + 13].astype("float64"), places),
        decoded.levels["temperature"],
    )
    assert ds["launch_time"].dtype.kind == "M"
    assert ds["latitude"].values[temp] == pytest.approx(70.93972, abs=1e-9)

    # The same window again, and a later window carrying the same objects: nothing new.
    again = ua.UpperAirTarget(local_config)
    again.feed(TEMP, body(TEMP), ARRIVED)
    assert again.finish(*window, "wis2_20261029T0000Z")["window_already_written"] == 1
    later = ua.UpperAirTarget(local_config)
    for key in (TEMP, VHHH_JMA):
        later.feed(key, body(key), ARRIVED)
    result = later.finish(end, end + dt.timedelta(hours=12), "wis2_20261029T1200Z")
    assert result["soundings"] == 0 and result["already_stored"] == 2
    # ...and that window is ledgered all the same, so a re-run skips it outright.
    from planetary_datasets.common.multidim import ledger

    assert {"wis2_20261029T0000Z", "wis2_20261029T1200Z"} <= ledger(repo)


def test_the_target_registers_with_the_archive():
    from planetary_datasets.providers.observations.wis2_targets import load_targets

    assert load_targets()["upper_air"] is ua.UpperAirTarget
