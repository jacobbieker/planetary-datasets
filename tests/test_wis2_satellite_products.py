"""WIS2 satellite products beside the radiances, offline against real cache objects.

The fixtures in ``data/wis2_satellite_products`` are real WIS2 Global Cache objects (WMO
core data): radio occultations from Metop-SG A1, Spire and FY-3 (via the JMA GTS relay),
Sentinel-1 SAR ocean winds, Sentinel-3 SRAL and Jason altimetry (via the DWD relay),
Metop AVHRR and Meteosat AMVs, Metop-C ASCAT winds, and a Norwegian ``ISX`` bulletin that
is not satellite data at all. ``<name>.key`` holds the cache key each came from.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_satellite as sat
from planetary_datasets.providers.observations import wis2_satellite_products as sp

DATA = pathlib.Path(__file__).parent / "data" / "wis2_satellite_products"
ARRIVED = dt.datetime(2026, 11, 1, 6, tzinfo=dt.timezone.utc)
GROUPS = {
    "ro_metop_sga1": "gnss_ro",
    "ro_spire": "gnss_ro",
    "ro_gts_iut": "gnss_ro",
    "sar_sentinel1c": "sar_ocean",
    "altimetry_sral": "altimetry",
    "altimetry_gts_isz": "altimetry",
    "amv_avhrr": "amv",
    "amv_gts_iev": "amv",
    "scatterometer_gts_isx": "scatterometer",
}


def key(name: str) -> str:
    return (DATA / f"{name}.key").read_text().strip()


def body(name: str) -> bytes:
    return (DATA / f"{name}.bufr").read_bytes()


def store(config):
    return config.icechunk_repo(sat.STORE_PREFIX).readonly_session("main").store


@pytest.mark.parametrize("name", sorted(GROUPS))
def test_each_object_decodes_into_its_group(name):
    group, arrays, counts = sat.decode_object(key(name), body(name), ARRIVED)
    assert group == GROUPS[name] and arrays is not None, counts
    rows = arrays["time"].size
    assert rows > 0 and not np.isnat(arrays["time"]).any()
    assert np.isfinite(
        np.asarray(arrays["latitude"], dtype="float64").reshape(rows, -1)[:, 0]
    ).all()
    assert (arrays["arrival_time"] == np.datetime64("2026-11-01T06:00:00")).all()


def test_a_relayed_bulletin_that_is_not_satellite_data_is_left_out():
    name = "not_satellite_gts_isx"
    group, arrays, counts = sat.decode_object(key(name), body(name), ARRIVED)
    assert group == "scatterometer" and arrays is None
    assert counts["scatterometer_messages_of_other_products"] == 1


def test_occultations_keep_every_level_in_message_order():
    _, arrays, _ = sat.decode_object(key("ro_metop_sga1"), body("ro_metop_sga1"), ARRIVED)
    bending, impact = arrays["bending_angle"], arrays["impact_parameter"]
    assert bending.ndim == 2 and impact.ndim == 2 and bending.shape[0] == 1
    # Each impact parameter carries a bending angle per frequency in its replication.
    assert bending.shape[1] % impact.shape[1] == 0 and bending.shape[1] > impact.shape[1]
    levels = impact[0][np.isfinite(impact[0])]
    assert levels.size > 100 and 6.3e6 < levels.min() < levels.max() < 6.5e6


def test_scatterometer_winds_are_ascat():
    _, arrays, _ = sat.decode_object(
        key("scatterometer_gts_isx"), body("scatterometer_gts_isx"), ARRIVED
    )
    assert set(np.asarray(arrays["satellite_instruments"]).ravel().tolist()) == {190}
    speeds = np.asarray(arrays["wind_speed_at10_m"], dtype="float64")
    assert np.nanmax(speeds) < 60 and np.isfinite(speeds).any()


def test_the_target_writes_a_group_per_product_with_first_arrivals(local_config):
    target = sat.SatelliteTarget(local_config, workers=2)
    for name in sorted(GROUPS) + ["not_satellite_gts_isx"]:
        target.feed(key(name), body(name), ARRIVED)
    # The FY-3 occultation relayed again, earlier, by another centre: one row, whose first
    # arrival is the earlier copy's.
    earlier = ARRIVED - dt.timedelta(minutes=7)
    target.feed(key("ro_gts_iut").replace("jp-jma", "de-dwd"), body("ro_gts_iut"), earlier)
    summary = target.finish(None, None, "wis2_20261101T1200Z")
    assert set(summary["rows"]) == set(GROUPS.values())
    assert summary["gnss_ro_duplicates"] == 1

    tree = xr.open_datatree(store(local_config), engine="zarr", consolidated=False)
    assert set(GROUPS.values()) <= set(tree.children)
    ro = xr.open_zarr(store(local_config), group="gnss_ro", consolidated=False)
    assert ro.sizes["observation"] == 3
    assert ro["bending_angle"].dims == ("observation", "bending_angle_n")
    relayed = ro["source_key"].values.tolist().index(key("ro_gts_iut").replace("jp-jma", "de-dwd"))
    assert ro["first_arrival_time"].values[relayed] == np.datetime64(earlier.replace(tzinfo=None))
    assert ro["arrival_time"].values[relayed] == np.datetime64(earlier.replace(tzinfo=None))
    assert ro["first_arrival_time"].attrs["long_name"].startswith("earliest time any copy")

    amv = xr.open_zarr(store(local_config), group="amv", consolidated=False)
    expected = sum(
        sat.decode_object(key(n), body(n), ARRIVED)[1]["time"].size
        for n in ("amv_avhrr", "amv_gts_iev")
    )
    assert amv.sizes["observation"] == expected
    altimetry = xr.open_zarr(store(local_config), group="altimetry", consolidated=False)
    templates = set(altimetry["bufr_template"].values.tolist())
    assert len(templates) == 2, "SRAL (340017) and Jason (340010) side by side"

    again = sat.SatelliteTarget(local_config, workers=1)
    again.feed(key("amv_avhrr"), body("amv_avhrr"), ARRIVED)
    assert again.finish(None, None, "wis2_20261101T1200Z")["rows"] == {"amv": 0}
    later = sat.SatelliteTarget(local_config, workers=1)
    later.feed(key("amv_avhrr"), body("amv_avhrr"), ARRIVED)
    assert later.finish(None, None, "wis2_20261102T0000Z")["rows"] == {"amv": 0}


def test_a_later_window_with_a_different_template_widens_the_group(local_config):
    first = sat.SatelliteTarget(local_config, workers=1)
    first.feed(key("altimetry_sral"), body("altimetry_sral"), ARRIVED)
    first.finish(None, None, "a")
    stored = xr.open_zarr(store(local_config), group="altimetry", consolidated=False)
    before, names_before = stored.sizes["observation"], set(stored.data_vars)

    second = sat.SatelliteTarget(local_config, workers=1)
    second.feed(key("altimetry_gts_isz"), body("altimetry_gts_isz"), ARRIVED)
    second.finish(None, None, "b")
    stored = xr.open_zarr(store(local_config), group="altimetry", consolidated=False)
    added = sat.decode_object(key("altimetry_gts_isz"), body("altimetry_gts_isz"), ARRIVED)[1]
    assert stored.sizes["observation"] == before + added["time"].size
    assert names_before < set(stored.data_vars), "Jason's elements join SRAL's"
    assert set(stored.attrs["partitions_written"]) == {"a", "b"}


def test_element_names_are_snake_case():
    assert sp.snake("bendingAngle") == "bending_angle"
    assert sp.snake("DistanceFromEarthCentreInDirectionOfNorthPole") == (
        "distance_from_earth_centre_in_direction_of_north_pole"
    )
    assert sp.snake("windSpeedAt10M") == "wind_speed_at10_m"
