"""WIS2 ocean observations: decoding real BUFR samples and the store they build.

The samples in ``tests/wis2_ocean_samples`` are real WIS2 Global Cache objects (WMO core
data, a few hundred bytes each): a NOAA moored buoy, seven ship reports in one message,
an IMD drifting buoy reporting below the surface, an Ifremer Argo profile, a GTS buoy
bulletin with its heading, and a bulletin ecCodes cannot unpack.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_ocean as ocean
from planetary_datasets.providers.observations.wis2_targets import build_targets

SAMPLES = pathlib.Path(__file__).parent / "wis2_ocean_samples"
TOPIC = "data/{centre}/data/core/{topic}/{name}"
MOORED = TOPIC.format(
    centre="us-noaa-nws",
    topic="ocean/surface-based-observations/moored-buoys-and-moorings",
    name="BUOY_0-20002-0-1300130_x.bufr4",
)
SHIP = TOPIC.format(
    centre="uk-metoffice", topic="weather/surface-based-observations/ship", name="ship"
)
DRIFTER = TOPIC.format(
    centre="in-imd", topic="ocean/surface-based-observations/drifting-buoys", name="drifter"
)
ARGO = TOPIC.format(
    centre="fr-ifremer-argo",
    topic="ocean/surface-based-observations/drifting-ocean-profilers",
    name="argo",
)
GTS_BUOY = "data/de-dwd-gts-to-wis2/data/core/I/O/B/A/01/LEMM/A_IOBA01LEMM270000_C_EDZW_1"
GTS_BAD = "data/de-dwd-gts-to-wis2/data/core/I/O/D/B/01/EBUM/A_IODB01EBUM262340_C_EDZW_2"


def sample(name: str) -> bytes:
    return (SAMPLES / name).read_bytes()


@pytest.mark.parametrize(
    ("key", "kind"),
    [
        (MOORED, "moored_buoy"),
        (SHIP, "ship"),
        (ARGO, "profiling_float"),
        (GTS_BUOY, "buoy"),
        ("data/jp-jma-gts-to-wis2/data/core/I/S/S/A/01/RJTD/x", "ship"),
        ("data/de-dwd-gts-to-wis2/data/core/I/O/P/B/01/AMMC/x", "profiling_float"),
        # Tsunameters, upper air, land synop and gridded ocean forecasts are not this store's.
        ("data/de-dwd-gts-to-wis2/data/core/I/O/Z/K/01/AMMC/x", None),
        ("data/jp-jma-gts-to-wis2/data/core/I/U/A/X/01/RJTD/x", None),
        ("data/de-dwd-gts-to-wis2/data/core/I/S/M/D/01/EDZW/x", None),
        (
            TOPIC.format(centre="x", topic="weather/surface-based-observations/synop", name="a"),
            None,
        ),
        (
            TOPIC.format(
                centre="uk-metoffice-globalwave",
                topic="ocean/experimental/prediction/forecast/medium-range",
                name="a.grib2",
            ),
            None,
        ),
    ],
)
def test_objects_are_told_apart_by_topic_and_bulletin(key, kind):
    assert ocean.kind_of(key) == kind


def test_a_moored_buoy_reports_surface_values_and_what_it_measures_below():
    collector = ocean.collect([(MOORED, sample("moored_buoy_315008.msg"))])
    ((row,),) = [list(collector.surface.values())]
    assert row["wmo_platform_id"] == "1300130" and row["station_name"] == "GRAN CANARIA"
    assert row["time"] == np.datetime64("2026-10-28T17:00:00")
    assert row["mean_sea_level_pressure"] == 101780.0  # Pa
    assert row["air_temperature"] == pytest.approx(298.44)  # K
    assert row["significant_height_of_combined_wind_waves_and_swell"] == pytest.approx(1.17)
    assert row["peak_wave_period"] == pytest.approx(11.9)
    assert row["mean_wave_direction"] == 340.0
    assert row["data_buoy_type"] == 25
    ((meta, levels),) = collector.profiles.values()
    assert meta["wmo_platform_id"] == "1300130"
    assert levels == [
        {"depth": 3.0, "sea_water_temperature": pytest.approx(297.81), "sea_water_salinity": 36.79},
        {"depth": 3.2, "current_direction": 201, "current_speed": pytest.approx(0.13)},
    ]


def test_every_subset_of_a_ship_bulletin_is_a_report():
    collector = ocean.collect([(SHIP, sample("ship_308009_7_subsets.msg"))])
    rows = list(collector.surface.values())
    assert len(rows) == 7
    assert {r["call_sign"] for r in rows} >= {"62305", "AMOUK44", "AMOUK17"}
    first = next(r for r in rows if r["call_sign"] == "62305")
    # A ship's water temperature, at no stated depth, is its sea surface temperature.
    assert first["sea_surface_temperature"] == pytest.approx(289.35)
    assert first["air_temperature"] == pytest.approx(288.75)
    assert not collector.profiles


def test_an_argo_profile_merges_its_oxygen_onto_its_levels():
    collector = ocean.collect([(ARGO, sample("argo_315003.msg"))])
    assert not collector.surface, "a float reports nothing at the surface"
    ((meta, levels),) = collector.profiles.values()
    assert meta["wmo_platform_id"] == "6903822" and meta["float_cycle_number"] == 318
    assert len(levels) == 111
    assert levels[0] == {
        "sea_water_pressure": 1000.0,
        "sea_water_temperature": pytest.approx(292.688),
        "sea_water_salinity": pytest.approx(38.962),
        "dissolved_oxygen": pytest.approx(228.189),
    }
    pressures = [lv["sea_water_pressure"] for lv in levels]
    assert pressures == sorted(pressures)


def test_a_drifter_reports_its_surface_and_its_subsurface():
    collector = ocean.collect([(DRIFTER, sample("drifting_buoy_with_profile.msg"))])
    ((row,),) = [list(collector.surface.values())]
    assert row["sea_surface_temperature"] == pytest.approx(303.17)
    ((_, levels),) = collector.profiles.values()
    assert levels[0]["depth"] == 1.0 and len(levels) > 1


def test_a_bad_message_is_counted_not_raised():
    collector = ocean.collect([(GTS_BAD, sample("gts_IODB01_undecodable.msg"))])
    assert collector.counts.bad_messages == 1 and not collector.surface


def test_one_report_through_two_centres_is_kept_once():
    body = sample("moored_buoy_315008.msg")
    relayed = "data/jp-jma-gts-to-wis2/data/core/I/O/B/A/01/KWBC/A_IOBA01KWBC281700_C_RJTD_1"
    collector = ocean.collect([(MOORED, body), (relayed, b"IOBA01 KWBC 281700\r\r\n" + body)])
    assert len(collector.surface) == 1 and collector.duplicates["surface"] == 1
    assert len(collector.profiles) == 1


def test_a_gts_bulletin_heading_is_skipped():
    collector = ocean.collect([(GTS_BUOY, sample("gts_IOBA01_LEMM.msg"))])
    assert collector.counts.messages == 1 and len(collector.surface) == 1


def _run_window(config, objects, end):
    (target,) = build_targets(config, ["ocean"])
    for key, body in objects:
        if target.wants(key, len(body)):
            target.feed(key, body, None)
    return target.finish(end - dt.timedelta(hours=12), end, f"wis2_{end:%Y%m%dT%H%M}Z")


def test_the_store_opens_as_a_tree_and_skips_reports_it_holds(local_config):
    objects = [
        (MOORED, sample("moored_buoy_315008.msg")),
        (SHIP, sample("ship_308009_7_subsets.msg")),
        (ARGO, sample("argo_315003.msg")),
        (GTS_BAD, sample("gts_IODB01_undecodable.msg")),
    ]
    end = dt.datetime(2026, 10, 29, 12, tzinfo=dt.timezone.utc)
    first = _run_window(local_config, objects, end)
    assert first["surface_written"] == 8 and first["profiles_written"] == 2
    assert first["levels_written"] == 113 and first["bad_messages"] == 1

    store = local_config.icechunk_repo(ocean.STORE_PREFIX).readonly_session("main").store
    tree = xr.open_datatree(store, engine="zarr", consolidated=False)
    assert set(tree.children) == {"surface", "profiles"}
    surface = xr.open_zarr(store, group="surface", consolidated=False)
    assert surface.sizes["observation"] == 8
    assert surface["time"].dtype.kind == "M"
    assert surface["air_temperature"].attrs["units"] == "K"
    assert (surface["time"].values[1:] >= surface["time"].values[:-1]).all()
    profiles = xr.open_zarr(store, group="profiles", consolidated=False)
    assert int(profiles["level_count"].sum()) == profiles.sizes["level"] == 113
    argo = int(np.flatnonzero(profiles["wmo_platform_id"].values == "6903822")[0])
    start = int(profiles["level_count"].values[:argo].sum())
    assert profiles["sea_water_pressure"].values[start] == 1000.0
    assert profiles["data_buoy_type"].values[argo] == 26

    # The next window brings the same reports again (another centre's copy, say).
    later = _run_window(local_config, objects, end + dt.timedelta(hours=12))
    assert later["surface_written"] == 0 and later["surface_already_stored"] == 8
    assert later["profiles_written"] == 0
    again = xr.open_zarr(store, group="surface", consolidated=False)
    assert again.sizes["observation"] == 8
