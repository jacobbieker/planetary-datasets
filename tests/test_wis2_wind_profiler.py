"""Tests for the wind-profiler group of the WIS2 upper-air store.

The fixtures are real GTS ``IUP*`` bulletins from the WIS2 Global Cache (2026-11-05), one
per template family, and the DWD and JMA relays' copies of three of them.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from collections import Counter

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_upper_air as ua
from planetary_datasets.providers.observations import wis2_wind_profiler as wp

FIXTURES = pathlib.Path(__file__).parent / "data" / "wis2_wind_profiler"
UPPER_AIR = pathlib.Path(__file__).parent / "data" / "wis2_upper_air"
ARRIVED = dt.datetime(2026, 11, 5, 11, 50, tzinfo=dt.timezone.utc)
DWD = "data/de-dwd-gts-to-wis2/data/core/I/U/P"
JMA = "data/jp-jma-gts-to-wis2/data/core/I/U/P"

LFPW_DWD = f"{DWD}/N/58/LFPW/A_IUPN58LFPW051140_C_EDZW_20261105115001_76673368"
LFPW_JMA = f"{JMA}/N/58/LFPW/A_IUPN58LFPW051140_C_RJTD_20261105114950_16.bufr"
AMMC_DWD = f"{DWD}/K/07/AMMC/A_IUPK07AMMC051130_C_EDZW_20261105114702_76672325"
AMMC_JMA = f"{JMA}/K/07/AMMC/A_IUPK07AMMC051130_C_RJTD_20261105115115_50.bufr"
WINDAS_DWD = f"{DWD}/C/47/RJTD/A_IUPC47RJTD051100_C_EDZW_20261105111701_76666593"
WINDAS_JMA = f"{JMA}/C/47/RJTD/A_IUPC47RJTD051100_C_RJTD_20261105111741_3.bufr"
LPMG = f"{JMA}/A/41/LPMG/A_IUPA41LPMG050157_C_RJTD_20261105020445_243.bufr"
VHHH = f"{DWD}/C/01/VHHH/A_IUPC01VHHH050900_C_EDZW_20261105092704_76622784"
ENMI = f"{DWD}/D/40/ENMI/IUPD40ENMI050706_C_EDZW_20261105071202_76590900.03831"
EGRR = f"{DWD}/D/67/EGRR/A_IUPD67EGRR050920_C_EDZW_20261105092704_76622795"
NZKL = f"{DWD}/K/42/NZKL/A_IUPK42NZKL050600_C_EDZW_20261105060808_76570726"
EMPTY_EGRR = f"{DWD}/A/75/EGRR/A_IUPA75EGRR051151_C_EDZW_20261105115404_76674818"
EMPTY_EDZW = f"{DWD}/D/84/EDZW/A_IUPD84EDZW050940_C_EDZW_20261105094411_76637157"
ALL = (
    LFPW_DWD,
    LFPW_JMA,
    AMMC_DWD,
    AMMC_JMA,
    WINDAS_DWD,
    WINDAS_JMA,
    LPMG,
    VHHH,
    ENMI,
    EGRR,
    NZKL,
    EMPTY_EGRR,
    EMPTY_EDZW,
)
TEMP = "data/us-noaa-nws/data/core/weather/surface-based-observations/temp/temp_01001.bufr4"
WINDOW_END = dt.datetime(2026, 11, 5, 12, tzinfo=dt.timezone.utc)
WINDOW = (WINDOW_END - dt.timedelta(hours=12), WINDOW_END)
PARTITION = "wis2_20261105T1200Z"


def body(key: str) -> bytes:
    """The fixture bytes of an object key."""
    name = key.rsplit("/", 1)[-1]
    path = FIXTURES / name
    return (path if path.exists() else UPPER_AIR / name).read_bytes()


def decode(key: str, counts: Counter | None = None) -> list[wp.Profile]:
    """The profiles of a fixture object."""
    return wp.decode_object(key, body(key), ARRIVED, counts if counts is not None else Counter())


def stored(local_config, group: str = ua.GROUP) -> xr.Dataset:
    """A group of the local store (the soundings by default), as a dataset."""
    repo = local_config.icechunk_repo(ua.STORE_PREFIX)
    return xr.open_zarr(repo.readonly_session("main").store, group=group, consolidated=False)


@pytest.mark.parametrize(
    ("key", "profiler", "sounding"),
    [
        (LFPW_DWD, True, False),
        (WINDAS_JMA, True, False),
        (ENMI, True, False),
        (
            "data/de-dwd-gts-to-wis2/data/core/I/U/K/A/01/EIDB/A_IUKA01EIDB280000_C_EDZW_1",
            False,
            True,
        ),
        ("data/jp-jma-gts-to-wis2/data/core/I/U/C/C/10/RJTD/A_IUCC10RJTD_x.bufr", False, False),
        ("data/de-dwd-gts-to-wis2/data/core/U/P/A/A/01/EGRR/A_UPAA01EGRR_x", False, False),
        (
            "data/hk-hko/data/core/weather/surface-based-observations/wind-profile-radar/p.bufr4",
            False,
            True,
        ),
    ],
)
def test_wind_profile_bulletins_are_picked_by_key_and_not_taken_as_soundings(
    key, profiler, sounding
):
    """IUP* bulletins go to the profiler, never to the soundings."""
    assert wp.wanted(key) is profiler
    assert ua.wanted(key) is sounding


def test_a_radar_profile_keeps_heights_winds_and_both_qualities():
    """A category 6 radar profile decodes with both quality flags."""
    (profile,) = decode(LPMG)
    header, levels = profile.header, profile.levels
    assert header["station_id"] == header["wmo_station_id"] == "08550"
    assert header["data_category"] == 6
    assert header["time"] == np.datetime64("2026-11-05T01:57:00")
    assert header["station_elevation"] == 193.0
    assert header["mean_frequency"] == 5.6e9
    assert header["bulletin_heading"] == "IUPA41LPMG050157"
    assert levels["height"][:2].tolist() == [750.0, 1250.0]
    assert levels["wind_direction"][0] == 336
    assert levels["wind_speed"][0] == 4.1
    assert levels["upward_air_velocity"][0] == -1.41
    # 0 33 002 after the wind speed is the horizontal wind's, after w the vertical's.
    assert levels["wind_quality"][:2].tolist() == [0, 0]
    assert levels["vertical_wind_quality"][:2].tolist() == [0, 1]
    assert np.isnan(levels["height_above_station"]).all()


def test_a_profiler_with_associated_quality_fields_and_averaging_period():
    """Associated-field qualities and the averaging period are kept."""
    (profile,) = decode(EGRR)
    header, levels = profile.header, profile.levels
    assert header["station_id"] == "07625"
    assert header["data_category"] == 2
    assert header["time_significance"] == 2
    assert header["time_period"] == -22 * 60
    assert header["range_gate_length"] == 187.0
    assert header["beamwidth"] == 8.5
    assert header["data_sub_category"] is None  # BUFR edition 3
    assert header["local_data_sub_category"] == 1
    assert np.isfinite(levels["signal_to_noise_ratio"]).all()
    assert (levels["wind_quality_significance"] == 1).all()
    assert (levels["vertical_wind_quality"] >= 0).all()


def test_windas_bulletins_hold_ten_minute_profiles_of_three_stations():
    """JMA WINDAS bulletins split into one profile per station and time."""
    profiles = decode(WINDAS_DWD)
    stations = Counter(p.header["station_id"] for p in profiles)
    assert len(stations) == 3
    assert set(stations.values()) == {6}
    first = profiles[0]
    assert first.header["time_period"] == -600
    assert np.isfinite(first.levels["height_above_station"]).all()
    assert np.isnan(first.levels["height"]).all()
    times = [
        p.header["time"]
        for p in profiles
        if p.header["station_id"] == profiles[0].header["station_id"]
    ]
    assert np.diff(np.array(times)).astype(int).tolist() == [600] * 5


def test_the_pilot_template_profiler_reads_as_a_profile():
    """Hong Kong's PILOT-template profiler becomes a profile."""
    (profile,) = decode(VHHH)
    assert profile.header["station_id"] == "45004"
    assert profile.header["data_category"] == 2
    assert np.isfinite(profile.levels["geopotential_height"]).all()
    assert np.isnan(profile.levels["height"]).all()
    assert np.isfinite(profile.levels["wind_speed"]).any()


def test_empty_bulletins_and_unprefixed_names():
    """Empty bulletins are counted; headings without A_ are still read."""
    counts = Counter()
    assert decode(EMPTY_EGRR, counts) == []
    assert decode(EMPTY_EDZW, counts) == []
    assert counts["no_levels"] == 2
    (profile,) = decode(ENMI)
    assert profile.header["bulletin_heading"] == "IUPD40ENMI050706"
    (profile,) = decode(NZKL)
    assert profile.level_count == 3


def test_garbage_never_raises():
    """Undecodable input is counted, never raised."""
    counts = Counter()
    assert wp.decode_object("x", b"BUFR\x00\x00\x30\x04" + b"\xff" * 40, None, counts) == []
    assert wp.decode_object("y", b"not bufr", None, counts) == []
    assert counts["not_bufr"] == 1
    assert counts["messages"] == 1


def test_relayed_copies_collapse_to_one():
    """The DWD and JMA copies collapse to one profile."""
    counts = Counter()
    copies = [p for key in (LFPW_DWD, LFPW_JMA, AMMC_DWD, AMMC_JMA) for p in decode(key)]
    windas = decode(WINDAS_DWD) + decode(WINDAS_JMA)
    kept = wp.deduplicate(copies + windas, set(), counts)
    assert len(kept) == 2 + 18
    assert counts["duplicate"] == 2 + 18
    stored = {p.identity for p in decode(LFPW_DWD)}
    assert wp.deduplicate(decode(LFPW_JMA), stored, counts) == []
    assert counts["already_stored"] == 1


def _run(local_config, keys, partition=PARTITION, prepare=True) -> dict:
    """Run the upper_air target over ``keys`` as one window."""
    target = ua.UpperAirTarget(local_config)
    if prepare:
        target.prepare(partition)
    for key in keys:
        if target.wants(key, 0):
            target.feed(key, body(key), ARRIVED)
    return target.finish(*WINDOW, partition)


def test_the_group_is_ragged_lossless_and_written_once_per_window(local_config):
    """The group is ragged, lossless and written once per window."""
    summary = _run(local_config, (*ALL, TEMP))
    assert summary["soundings"] == 1
    assert summary["profiles"] == 1 + 1 + 18 + 1 + 1 + 1 + 1 + 1
    assert summary["profiler_duplicate"] == 2 + 18
    assert summary["profiler_no_levels"] == 2

    ds = stored(local_config, wp.GROUP)
    assert ds.sizes["profile"] == summary["profiles"]
    assert ds.sizes["profile_level"] == int(ds["level_count"].sum())
    for row in range(ds.sizes["profile"]):
        first, count = int(ds["first_level"][row]), int(ds["level_count"][row])
        assert (ds["profile_index"].values[first : first + count] == row).all()
    assert (np.diff(ds["time"].values).astype(int) >= 0).all()
    row = int(np.flatnonzero(ds["station_id"].values == "08550")[0])
    first = int(ds["first_level"][row])
    (decoded,) = decode(LPMG)
    places = ds["upward_air_velocity"].attrs["decimal_places"]
    np.testing.assert_array_equal(
        np.round(ds["upward_air_velocity"].values[first : first + 29].astype("float64"), places),
        decoded.levels["upward_air_velocity"],
    )
    import zarr

    repo = local_config.icechunk_repo(ua.STORE_PREFIX)
    raw = zarr.open_group(repo.readonly_session("main").store, path=wp.GROUP, mode="r")
    assert raw["wind_quality"].dtype.kind == "i"
    assert raw["wind_quality"].attrs["_FillValue"] == -1
    # The soundings at the root are as before.
    root = stored(local_config)
    assert root.sizes["sounding"] == 1
    assert "profile" not in root.sizes

    from planetary_datasets.common.multidim import ledger

    assert PARTITION in ledger(repo, group=ua.GROUP)
    assert PARTITION in ledger(repo, group=wp.GROUP)

    # The same window again: both groups decline their objects and nothing is written.
    again = _run(local_config, (*ALL, TEMP))
    assert again["objects"] == 0
    assert again["soundings"] == 0
    assert again["profiles"] == 0
    assert stored(local_config, wp.GROUP).sizes["profile"] == summary["profiles"]
    assert stored(local_config).sizes["sounding"] == 1
    # ...and without prepare(), the ledgers still stop a second write.
    unprepared = _run(local_config, (*ALL, TEMP), prepare=False)
    assert unprepared["window_already_written"] == 1
    assert unprepared["profiler_window_already_written"] == 1
    # A later window carrying the same bulletins: nothing new, but ledgered.
    later = _run(local_config, (LFPW_JMA, WINDAS_JMA), partition="wis2_20261106T0000Z")
    assert later["profiles"] == 0
    assert later["profiler_already_stored"] == 1 + 18
    assert "wis2_20261106T0000Z" in ledger(repo, group=wp.GROUP)


def test_a_window_already_written_for_soundings_still_gets_its_profiles(local_config):
    """A window holding soundings gets its profiles; the soundings are untouched."""
    # The window as written before the profiler group existed: soundings only.
    ua.write(
        ua.decode_object(TEMP, body(TEMP), ARRIVED, Counter()), local_config, PARTITION, Counter()
    )
    before = stored(local_config)

    target = ua.UpperAirTarget(local_config)
    target.prepare(PARTITION)
    assert not target.wants(TEMP, 0)
    assert target.wants(LFPW_DWD, 0)
    summary = _run(local_config, (*ALL, TEMP))
    assert summary["window_already_written"] == 1
    assert summary["soundings"] == 0
    assert summary["profiles"] == 25
    after = stored(local_config)
    xr.testing.assert_identical(before.load(), after.load())


def test_profiles_first_then_soundings_in_a_new_store(local_config):
    """A fresh store can take the group first and root arrays later."""
    assert _run(local_config, (LFPW_DWD,), partition="a")["profiles"] == 1
    assert _run(local_config, (TEMP,), partition="b")["soundings"] == 1
    assert stored(local_config).sizes["sounding"] == 1
    assert stored(local_config, wp.GROUP).sizes["profile"] == 1


def test_the_group_records_arrival_like_the_soundings():
    """Arrival times are described as in the soundings."""
    names = ("arrival_time", "first_arrival_time")
    assert {n: wp.PROFILE_ATTRS[n]["long_name"] for n in names} == {
        n: ua.SOUNDING_ATTRS[n]["long_name"] for n in names
    }
