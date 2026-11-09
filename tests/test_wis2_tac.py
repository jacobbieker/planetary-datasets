"""WIS2 TAC SYNOP/SHIP text bulletins into the land and ocean stores.

The bulletins are real GTS text relayed to WIS2 by JMA and DWD (copied from the WIS2
Global Cache); the BUFR SYNOP is the land tests' ``ism_synop.bufr`` (stations 07005 and
07015 at 2026-10-28T06Z).
"""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pytest
import xarray as xr

pytest.importorskip("pymetdecoder")

from planetary_datasets.common.multidim import ledger  # noqa: E402
from planetary_datasets.providers.observations import wis2_land as land  # noqa: E402
from planetary_datasets.providers.observations import wis2_tac as tac  # noqa: E402

ARRIVED = dt.datetime(2026, 10, 28, 7, 30, tzinfo=dt.timezone.utc)
PARTITION = "wis2_20261028T1200Z"
BUFR = pathlib.Path(__file__).parent / "data" / "wis2_land" / "ism_synop.bufr"

CANADA_KEY = (
    "data/jp-jma-gts-to-wis2/data/core/S/M/C/N/03/CWAO/"
    "A_SMCN03CWAO280600RRA_C_RJTD_20261028062551_28.txt"
)
CANADA = b"""SMCN03 CWAO 280600 RRA
AAXX 28064
71421 16/// /1903 11088 21114 39491 40198 50001 60001
333 11032 21091 70008=
71472 16/// /1623 10106 20070 30079 40097 58014 60001
333 10112 7////=
"""
# The same bulletin from the DWD relay: no extension, a sequence number line.
CANADA_DWD_KEY = (
    "data/de-dwd-gts-to-wis2/data/core/S/M/C/N/03/CWAO/A_SMCN03CWAO280600RRA_C_EDZW_202610280626"
)
CANADA_DWD = b"\x01\r\r\n123\r\r\n" + CANADA.replace(b"\n", b"\r\r\n") + b"\x03"
FRANCE_KEY = (
    "data/jp-jma-gts-to-wis2/data/core/S/M/F/R/01/LFPW/"
    "A_SMFR01LFPW280600_C_RJTD_20261028060918_7.txt"
)
# Station 07005 at 06Z on the 28th is also in the BUFR fixture; 07999 is not.
FRANCE = b"""SMFR01 LFPW 280600
AAXX 28061
07005 12970 61208 10110 20087 30150 40210 52010 60012 70282 85870=
07999 12970 61208 10110 20087 30150 40210 52010 60012 70282 85870=
07998 NIL=
"""
SHIP_KEY = (
    "data/jp-jma-gts-to-wis2/data/core/S/N/V/B/03/LFPW/"
    "A_SNVB03LFPW280600_C_RJTD_20261028061549_7.txt"
)
SHIP = b"""SNVB03 LFPW 280600
BBXX
ZKF2BNJ 28061 99299 11222 46/// ///// 10254 20186 30096 40143 52001
22200 00230=
SXZPW9C 28061 99820 11637 46/// /3107 11045 21047 39992 49992 54000
22240 01005
  333 91010=
"""


def objects():
    later = ARRIVED + dt.timedelta(minutes=3)
    return [
        (CANADA_KEY, CANADA, ARRIVED),
        (CANADA_DWD_KEY, CANADA_DWD, later),
        (FRANCE_KEY, FRANCE, ARRIVED),
        (SHIP_KEY, SHIP, ARRIVED),
    ]


def run(config, items, partition=PARTITION):
    target = tac.TacTarget(config)
    target.prepare(partition)
    for key, body, arrived in items:
        if target.wants(key, len(body)):
            target.feed(key, body, arrived)
    return target, target.finish(None, None, partition)


def write_bufr(config):
    target = land.LandTarget(config, workers=1)
    target.feed(
        "data/x/data/core/weather/surface-based-observations/synop/a", BUFR.read_bytes(), ARRIVED
    )
    target.finish(None, None, PARTITION)


def rows_of(ds, name, value):
    """The rows of ``ds`` whose ``name`` is ``value``.

    Selected by position: CI's numpy reads the strings as StringDType, which neither
    ``where`` nor ``astype(str)`` take.
    """
    return ds.isel(observation=[i for i, v in enumerate(ds[name].values) if str(v) == value])


def open_group(config, prefix, group):
    repo = config.icechunk_repo(prefix)
    return xr.open_zarr(repo.readonly_session("main").store, group=group, consolidated=False)


@pytest.mark.parametrize(
    ("key", "wanted"),
    [
        (CANADA_KEY, True),
        (CANADA_DWD_KEY, True),
        (SHIP_KEY, True),
        ("data/jp-jma-gts-to-wis2/data/core/S/I/I/D/20/WIIX/20261031000901_65.30686", True),
        ("data/jp-jma-gts-to-wis2/data/core/S/A/U/S/01/KWBC/A_SAUS01KWBC.txt", False),
        ("data/de-dwd-gts-to-wis2/data/core/S/Z/C/I/01/BABJ/A_SZCI01BABJ050020_C_EDZW", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/S/M/N/07/LFPW/A_ISMN07LFPW280600_C_EDZW", False),
        ("data/jp-jma-gts-to-wis2/data/core/U/S/U/K/02/EGRR/A_USUK02EGRR.txt", False),
    ],
)
def test_wants_relayed_synop_text_only(key, wanted):
    assert tac.wants(key) is wanted


def test_a_bulletin_splits_into_reports_under_their_section():
    title, reports = tac.split_bulletin(CANADA_DWD)
    assert title == "SMCN03 CWAO 280600 RRA"
    assert [r[0] for r in reports] == ["AAXX 28064", "AAXX 28064"]
    assert reports[0][1].startswith("71421 16/// /1903") and reports[0][1].endswith("70008")
    assert tac.split_bulletin(CANADA)[1] == reports
    _, ships = tac.split_bulletin(SHIP)
    assert [r[0] for r in ships] == ["BBXX", "BBXX"] and ships[1][1].endswith("333 91010")


def test_a_land_report_is_converted_to_the_bufr_groups_units():
    row = tac.decode_report(
        "AAXX 28061", "07999 12970 61208 10110 20087 30150 40210 52010 60012 70282 85870", ARRIVED
    )
    assert row["station_id"] == "0-20000-0-07999"
    assert row["time"] == np.datetime64("2026-10-28T06:00:00")
    assert row["temperature"] == pytest.approx(284.15)
    assert row["dewpoint"] == pytest.approx(281.85)
    assert row["mean_sea_level_pressure"] == pytest.approx(102100.0)
    assert row["pressure_change_3h"] == pytest.approx(100.0)
    assert row["wind_speed"] == 8 and row["wind_direction"] == 120  # m s-1 (iw = 1)
    assert row["total_precipitation_12h"] == 1.0  # 60012: 1 mm, tR = 2 (12 h)
    assert row["present_weather"] == 2 and row["past_weather_1"] == 8
    assert row["total_cloud_cover_oktas"] == 6
    canada = tac.decode_report(
        "AAXX 28064", "71421 16/// /1903 11088 21114 39491 40198 50001 60001", ARRIVED
    )
    assert canada["wind_speed"] == pytest.approx(3 * 1852 / 3600)  # knots (iw = 4)
    assert canada["temperature"] == pytest.approx(273.15 - 8.8)
    assert tac.decode_report("AAXX 28061", "07998 NIL", ARRIVED) is None


def test_a_day_after_the_arrival_is_last_month():
    reference = dt.datetime(2026, 11, 1, 0, 30, tzinfo=dt.timezone.utc)
    assert tac.resolve_time(31, 18, 0, reference) == np.datetime64("2026-10-31T18:00:00")
    assert tac.resolve_time(1, 0, 0, reference) == np.datetime64("2026-11-01T00:00:00")
    assert np.isnat(tac.resolve_time(31, 0, 0, dt.datetime(2026, 12, 1, tzinfo=dt.timezone.utc)))


def test_reports_held_as_bufr_are_dropped_and_the_rest_written(local_config):
    write_bufr(local_config)
    _, summary = run(local_config, objects())
    assert summary[tac.LAND_GROUP] == {
        "reports": 4,  # 71421, 71472, 07005, 07999 (07998 is NIL)
        "already_stored_as_tac": 0,
        "held_as_bufr": 1,  # 07005 at 06Z
        "rows_written": 3,
    }
    assert summary["copies"] == 2  # the DWD copy of the Canadian bulletin
    synop = open_group(local_config, tac.STORE_PREFIX, tac.LAND_GROUP).compute()
    assert sorted(synop["station_id"].values) == [
        "0-20000-0-07999",
        "0-20000-0-71421",
        "0-20000-0-71472",
    ]
    canada = rows_of(synop, "station_id", "0-20000-0-71421")
    assert str(canada["report"].values[0]).startswith("AAXX 28064 71421 16///")
    assert str(canada["bulletin"].values[0]) == "SMCN03 CWAO 280600 RRA"
    assert canada["arrival_time"].values[0] == np.datetime64("2026-10-28T07:30:00", "ns")
    ships = open_group(local_config, tac.STORE_PREFIX, tac.SHIP_GROUP).compute()
    assert sorted(ships["call_sign"].values) == ["SXZPW9C", "ZKF2BNJ"]
    sxz = rows_of(ships, "call_sign", "SXZPW9C")
    assert float(sxz["latitude"][0]) == 82.0 and float(sxz["longitude"][0]) == 163.7
    assert float(sxz["sea_surface_temperature"][0]) == pytest.approx(273.15 - 0.5)
    # The BUFR root and its ledger are untouched.
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    root = xr.open_zarr(repo.readonly_session("main").store, group=land.GROUP, consolidated=False)
    assert root.sizes["observation"] == 2


def test_a_second_run_over_the_window_writes_nothing(local_config):
    run(local_config, objects())
    land_repo = local_config.icechunk_repo(tac.STORE_PREFIX)
    ocean_repo = local_config.icechunk_repo(tac.STORE_PREFIX)
    assert PARTITION in ledger(land_repo, group=tac.LAND_GROUP)
    assert PARTITION in ledger(ocean_repo, group=tac.SHIP_GROUP)
    tips = [next(iter(r.ancestry(branch="main"))).id for r in (land_repo, ocean_repo)]
    again = tac.TacTarget(local_config)
    again.prepare(PARTITION)
    assert not any(again.wants(key, len(body)) for key, body, _ in objects())
    target, summary = run(local_config, objects())
    assert (
        summary[tac.LAND_GROUP] == "already written"
        and summary[tac.SHIP_GROUP] == "already written"
    )
    assert [next(iter(r.ancestry(branch="main"))).id for r in (land_repo, ocean_repo)] == tips


def test_a_later_window_repeating_reports_adds_only_corrections(local_config):
    run(local_config, objects())
    corrected = CANADA.replace(b"RRA", b"CCA").replace(b"11088", b"11090")
    _, summary = run(
        local_config,
        [(CANADA_KEY.replace("RRA", "CCA"), corrected, ARRIVED + dt.timedelta(hours=12))],
        "wis2_20261029T0000Z",
    )
    assert summary[tac.LAND_GROUP]["already_stored_as_tac"] == 1  # 71472 unchanged
    assert summary[tac.LAND_GROUP]["rows_written"] == 1  # 71421, corrected
    synop = open_group(local_config, tac.STORE_PREFIX, tac.LAND_GROUP).compute()
    station = rows_of(synop, "station_id", "0-20000-0-71421")
    assert sorted(station["2m_temperature"].values.round(2).tolist()) == [264.15, 264.35]


def test_land_reports_take_their_position_from_the_bufr_reports(local_config):
    write_bufr(local_config)
    later = FRANCE.replace(b"280600", b"280900").replace(b"28061", b"28091")
    run(local_config, [(FRANCE_KEY, later, ARRIVED + dt.timedelta(hours=3))])
    synop = open_group(local_config, tac.STORE_PREFIX, tac.LAND_GROUP).compute()
    held = rows_of(synop, "station_id", "0-20000-0-07005")
    assert float(held["latitude"][0]) == pytest.approx(50.136)
    unknown = rows_of(synop, "station_id", "0-20000-0-07999")
    assert np.isnan(unknown["latitude"].values[0])


def test_the_target_is_registered():
    from planetary_datasets.providers.observations import wis2_targets

    assert wis2_targets.load_targets()["tac"] is tac.TacTarget


def test_a_failure_in_one_store_does_not_stop_the_other(local_config, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("land store unreadable")

    monkeypatch.setattr(tac, "land_positions", broken)
    with pytest.raises(tac.IncompleteWindow, match="synop_tac not written"):
        run(local_config, objects())
    assert PARTITION in ledger(local_config.icechunk_repo(tac.STORE_PREFIX), group=tac.SHIP_GROUP)
    assert PARTITION not in ledger(
        local_config.icechunk_repo(tac.STORE_PREFIX), group=tac.LAND_GROUP
    )
    ships = open_group(local_config, tac.STORE_PREFIX, tac.SHIP_GROUP)
    assert ships.sizes["observation"] == 2
