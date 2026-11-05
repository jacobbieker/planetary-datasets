"""WIS2 satellite products beside the radiances, offline against real cache objects.

The fixtures in ``data/wis2_satellite_products`` are real WIS2 Global Cache objects (WMO
core data): radio occultations from Metop-SG A1, Spire and FY-3 (via the JMA GTS relay),
Sentinel-1 SAR ocean winds, Sentinel-3 SRAL and Jason altimetry (via the DWD relay),
Metop AVHRR and Meteosat AMVs, Metop-C ASCAT winds, and a Norwegian ``ISX`` bulletin that
is not satellite data at all; Meteosat-9 SEVIRI all-sky radiances (DWD relay), Himawari-9
AMVs (JMA relay), a CFOSAT SWIM nadir altimetry message, and the first 60 subsets of an
MTG-I1 FCI all-sky radiance bulletin (JMA relay; cut with ecCodes' subset extraction, the
values untouched). ``<name>.key`` holds the cache key each came from.
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
    "asr_fci_gts_iuf": "fci_asr",
    "asr_seviri_gts_ixf": "seviri_asr",
    "amv_ahi_gts_ixc": "ahi_amv",
    "swim_nadir": "swim_nadir",
}
#: Groups added after the store began, backfilled over windows the others already hold.
ADDED = ("asr_fci_gts_iuf", "asr_seviri_gts_ixf", "amv_ahi_gts_ixc", "swim_nadir")


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


def test_the_earliest_copy_of_each_identity_is_kept(tmp_path):
    from collections import Counter

    def spool(name, ids, arrivals):
        path = tmp_path / f"{name}.npz"
        np.savez(
            path,
            observation_identity=np.array(ids, dtype="int64"),
            arrival_time=np.array(arrivals, dtype="datetime64[s]"),
        )
        return str(path)

    a = spool("a", [1, 2, 3], ["2026-11-01T06:05", "2026-11-01T06:05", "NaT"])
    b = spool(
        "b", [2, 3, 4, 9], ["2026-11-01T06:01", "2026-11-01T06:09", "2026-11-01T06:02", "NaT"]
    )
    counts = Counter()
    keep = sat._earliest_copies([a, b], stored={4}, group="amv", counts=counts)
    # 1 from a; 2 from b (earlier); 3 from b (a's arrival is unknown); 4 already stored;
    # 9 only from b.
    assert {k: v.tolist() for k, v in keep.items()} == {0: [0], 1: [0, 1, 3]}
    assert counts["amv_decoded"] == 7 and counts["amv_duplicates"] == 3


SPACE = "data/int-eumetsat/data/core/weather/space-based-observations"


@pytest.mark.parametrize(
    "path, group",
    [
        (
            f"{SPACE}/meteosat-12/fci/W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-2-ASR--FD---",
            "fci_asr",
        ),
        (f"{SPACE}/meteosat-12/fci/W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-2-AMV--FD---", "amv"),
        (
            f"{SPACE}/meteosat-9/seviri/W_XX-EUMETSAT-Darmstadt,SING+LEV+SAT,MET09+ASR_C_EUMG_1",
            "seviri_asr",
        ),
        (
            f"{SPACE}/meteosat-9/seviri/W_XX-EUMETSAT-Darmstadt,SING+LEV+SAT,MET09+AMV_C_EUMG_1",
            "amv",
        ),
        (
            f"{SPACE}/cfosat/swim/W_XX-EUMETSAT-Darmstadt,SURFACE+SATELLITE,CFO+NRT+SWIM_OGDR.bin",
            "swim_nadir",
        ),
        (
            "data/jp-jma-gts-to-wis2/data/core/I/U/F/E/14/EUMT/A_IUFE14EUMT051130_C_RJTD_1.bufr",
            "fci_asr",
        ),
        (
            "data/de-dwd-gts-to-wis2/data/core/I/U/F/E/20/EUMG/A_IUFE20EUMG051145_C_EDZW_1",
            "seviri_asr",
        ),
        (
            "data/de-dwd-gts-to-wis2/data/core/I/X/F/H/19/EUMG/A_IXFH19EUMG051145_C_EDZW_1",
            "seviri_asr",
        ),
        (
            "data/jp-jma-gts-to-wis2/data/core/I/X/C/S/40/RJTD/A_IXCS40RJTD051100_C_RJTD_1.bufr",
            "ahi_amv",
        ),
        # The same headings from originators not known to carry these products.
        ("data/de-dwd-gts-to-wis2/data/core/I/U/F/A/03/LFPW/A_IUFA03LFPW051145_C_EDZW_1", None),
        ("data/jp-jma-gts-to-wis2/data/core/I/X/C/N/01/KWBC/A_IXCN01KWBC051100_C_RJTD_1", None),
        ("data/de-dwd-gts-to-wis2/data/core/I/X/V/D/01/EUMG/A_IXVD01EUMG051145_C_EDZW_1", "amv"),
    ],
)
def test_objects_are_routed_by_file_name_and_originator(path, group):
    product = sp.product_for(path)
    assert (product.group if product else None) == group


def test_the_asr_groups_keep_every_channel_and_category():
    _, fci, _ = sat.decode_object(key("asr_fci_gts_iuf"), body("asr_fci_gts_iuf"), ARRIVED)
    _, seviri, _ = sat.decode_object(key("asr_seviri_gts_ixf"), body("asr_seviri_gts_ixf"), ARRIVED)
    assert set(fci["satellite_identifier"].tolist()) == {71}, "Meteosat-12 (MTG-I1)"
    assert set(seviri["satellite_identifier"].tolist()) == {56}, "Meteosat-9 (IODC)"
    for arrays in (fci, seviri):
        assert arrays["satellite_channel_centre_frequency"].shape[1] == 11
        # Per channel: all-sky, clear and cloudy values and their spreads.
        bt = np.asarray(arrays["brightness_temperature"], dtype="float64")
        assert bt.shape[1] == 132 and np.nanmax(bt) < 350 and np.isfinite(bt).any()


def test_new_groups_are_backfilled_into_a_window_the_others_hold(local_config):
    window = "wis2_20261105T1200Z"
    first = sat.SatelliteTarget(local_config, workers=1)
    for name in ("amv_avhrr", "altimetry_sral"):
        first.feed(key(name), body(name), ARRIVED)
    assert set(first.finish(None, None, window)["rows"]) == {"amv", "altimetry"}

    backfill = sat.SatelliteTarget(local_config, workers=1)
    backfill.prepare(window)
    assert not backfill.wants(key("amv_avhrr"), 1), "amv holds the window"
    assert all(backfill.wants(key(name), 1) for name in ADDED)
    for name in ADDED:
        backfill.feed(key(name), body(name), ARRIVED)
    # The same SEVIRI bulletin through the JMA relay, later: one copy kept.
    relayed = key("asr_seviri_gts_ixf").replace("de-dwd", "jp-jma") + ".bufr"
    backfill.feed(relayed, body("asr_seviri_gts_ixf"), ARRIVED + dt.timedelta(minutes=3))
    summary = backfill.finish(None, None, window)
    expected = {
        GROUPS[name]: sat.decode_object(key(name), body(name), ARRIVED)[1]["time"].size
        for name in ADDED
    }
    assert summary["rows"] == expected
    assert summary["seviri_asr_duplicates"] == expected["seviri_asr"]

    tree = xr.open_datatree(store(local_config), engine="zarr", consolidated=False)
    assert {"amv", "altimetry", *expected} <= set(tree.children)
    for group, rows in expected.items():
        ds = xr.open_zarr(store(local_config), group=group, consolidated=False)
        assert ds.sizes["observation"] == rows
        assert ds.attrs["partitions_written"] == [window]
    seviri = xr.open_zarr(store(local_config), group="seviri_asr", consolidated=False)
    assert not seviri["source_key"].isin([relayed]).any(), "the earlier copy is kept"
    amv = xr.open_zarr(store(local_config), group="amv", consolidated=False)
    assert amv.sizes["observation"] == first.counts["amv_observations"]

    again = sat.SatelliteTarget(local_config, workers=1)
    again.prepare(window)
    assert not any(again.wants(key(name), 1) for name in ADDED), "written once"
    for name in ADDED:
        again.feed(key(name), body(name), ARRIVED)
    assert set(again.finish(None, None, window)["rows"].values()) == {0}
