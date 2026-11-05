"""WIS2 satellite sounder target, offline against one real message per instrument.

The fixtures are real WIS2 Global Cache objects (WMO core data, unrestricted), each cut
down to a single BUFR message: EUMETSAT ATMS, AMSU-A, MHS, FY-3D MWHS-2 and MWTS-2, the
reduced-set CrIS SDR, and a Météo-France DBNet IASI L1C subset. ``<name>.key`` holds the
cache key each came from.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import time

import eccodes
import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_satellite as sat
from planetary_datasets.providers.observations.wis2_targets import build_targets, load_targets

DATA = pathlib.Path(__file__).parent / "data" / "wis2_satellite"
NAMES = ("atms", "amsua", "mhs", "mwhs2", "mwts2", "cris", "iasi")
ARRIVED = dt.datetime(2026, 10, 29, 12, tzinfo=dt.timezone.utc)


def key(name: str) -> str:
    return (DATA / f"{name}.key").read_text().strip()


def body(name: str) -> bytes:
    return (DATA / f"{name}.bufr").read_bytes()


def decoded(name: str) -> dict:
    group, arrays, counts = sat.decode_object(key(name), body(name), ARRIVED)
    assert group == name and arrays is not None, counts
    return arrays


@pytest.mark.parametrize(
    ("path", "wanted"),
    [
        ("data/int-eumetsat/data/core/weather/space-based-observations/noaa-21/atms/x.bin", True),
        ("data/us-cimss/data/core/weather/space-based-observations/metop-b/iasi/x.bin", True),
        (
            "data/fr-meteofrance/data/core/weather/space-based-observations/metop-c/iasi/x.bufr",
            True,
        ),
        (
            "data/int-eumetsat/data/core/weather/experimental/space-based-observations/noaa-20/cris/x",
            True,
        ),
        # IASI level-2 products: neither radiances nor a product group.
        (
            "data/int-eumetsat/data/core/weather/space-based-observations/metop-b/iasi/"
            "W_XX-EUMETSAT-Darmstadt,SOUNDING+SATELLITE,METOPB+IASI_C_EUMP_1_73198_eps_o_trg_l2.bin",
            False,
        ),
        # Product groups: AMVs, radio occultation, AMI, and their GTS relays.
        (
            "data/int-eumetsat/data/core/weather/space-based-observations/metop-c/avhrr-3/a_amv_l2t.bin",
            True,
        ),
        ("data/int-eumetsat/data/core/weather/space-based-observations/metop-c/gras/x.bufr", True),
        (
            "data/kr-meteofrance/data/core/weather/space-based-observations/geo-kompsat-2a/ami/x",
            True,
        ),
        (
            "data/jp-jma-gts-to-wis2/data/core/I/U/T/A/14/BAWX/A_IUTA14BAWX270834_C_RJTD_1.bufr",
            True,
        ),
        (
            "data/de-dwd-gts-to-wis2/data/core/I/S/X/X/01/EHDB/A_ISXX01EHDB010048_C_EDZW_1.bin",
            True,
        ),
        # GTS radiance bulletins: those that repeat the topics are skipped, those with
        # fields of view of their own taken; upper-air bulletins are another target's.
        ("data/de-dwd-gts-to-wis2/data/core/I/E/I/X/01/EUMP/A_IEIX01EUMP010001_C_EDZW_1", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/N/C/X/01/EUMP/A_INCX01EUMP010001_C_EDZW_1", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/E/Q/X/01/EUMP/A_IEQX01EUMP010001_C_EDZW_1", False),
        (
            "data/jp-jma-gts-to-wis2/data/core/I/N/S/T/07/KWBC/A_INST07KWBC050650_C_RJTD_1.bufr",
            True,
        ),
        ("data/de-dwd-gts-to-wis2/data/core/I/N/M/X/08/SBBR/A_INMX08SBBR051035_C_EDZW_1", True),
        (
            "data/jp-jma-gts-to-wis2/data/core/I/N/Q/X/01/SBBR/A_INQX01SBBR050109_C_RJTD_1.bufr",
            True,
        ),
        ("data/de-dwd-gts-to-wis2/data/core/I/N/A/X/01/SBBR/A_INAX01SBBR010001_C_EDZW_1", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/U/K/X/01/EGRR/A_IUKX01EGRR010000_C_EDZW_1", False),
        ("data/de-dwd/data/core/weather/surface-based-observations/synop/x.bufr", False),
    ],
)
def test_wants_radiances_and_satellite_products(path, wanted):
    assert sat.SatelliteTarget.wants(None, path, 1) is wanted


@pytest.mark.parametrize("name", NAMES)
def test_every_instrument_decodes_with_sane_values(name):
    arrays = decoded(name)
    rows = arrays["time"].size
    assert rows > 0
    assert np.isfinite(arrays["latitude"]).all() and np.isfinite(arrays["longitude"]).all()
    assert (np.abs(arrays["latitude"]) <= 90).all()
    assert arrays["time"].dtype == np.dtype("datetime64[ms]")
    assert (arrays["time"] > np.datetime64("2026-10-01")).all()
    value = sat.INSTRUMENTS[name].values[0][1]
    values = arrays[value]
    assert values.shape == (rows, arrays["channel_number"].size)
    if name in ("cris", "iasi"):
        present = (
            values[~np.isnan(values)]
            if values.dtype.kind == "f"
            else values[values != sat.MISSING_LONG]
        )
        assert present.size and (present > 0).all()
    else:
        bt = values[np.isfinite(values)]
        assert bt.size and 100 < bt.min() and bt.max() < 350, (bt.min(), bt.max())
    assert len(np.unique(arrays["observation_identity"])) == rows


def test_atovs_channels_are_numbered_from_one_with_the_atovs_number_kept():
    amsua, mhs = decoded("amsua"), decoded("mhs")
    assert amsua["channel_number"].tolist() == list(range(1, 16))
    assert amsua["channel_atovs_number"].tolist() == list(range(28, 43))
    assert mhs["channel_number"].tolist() == list(range(1, 6))
    assert mhs["channel_atovs_number"].tolist() == list(range(43, 48))


def test_cris_takes_only_the_channel_loop_not_the_viirs_cluster_statistics():
    arrays = decoded("cris")
    handle = eccodes.codes_new_from_message(body("cris"))
    try:
        eccodes.codes_set(handle, "unpack", 1)
        loop = int(eccodes.codes_get(handle, "#1#extendedDelayedDescriptorReplicationFactor"))
        for occurrence in (1, 200, loop):
            number = int(
                np.atleast_1d(eccodes.codes_get_array(handle, f"#{occurrence}#channelNumber"))[0]
            )
            expected = np.atleast_1d(
                eccodes.codes_get_array(handle, f"#{occurrence}#channelRadiance")
            )
            expected = np.where(expected <= -1e99, np.nan, expected)
            got = arrays["radiance"][:, number - 1]
            np.testing.assert_array_equal(np.broadcast_to(expected, got.shape), got)
    finally:
        eccodes.codes_release(handle)
    assert arrays["channel_number"].size == loop == 2211


def test_iasi_scale_factors_follow_the_band_table():
    arrays = decoded("iasi")
    scales, scaled = arrays["radiance_scale_factor"], arrays["scaled_radiance"]
    present = scaled[0] != sat.MISSING_LONG
    channels = arrays["channel_number"][present]
    expected = np.select(
        [channels <= 3340, channels <= 6428, channels <= 6960, channels <= 8140],
        [7, 8, 9, 8],
        default=9,
    )
    np.testing.assert_array_equal(scales[0][present], expected)
    radiance = scaled[0][present] * 10.0 ** -scales[0][present]
    assert 1e-8 < radiance.min() and radiance.max() < 0.01  # W m-2 sr-1 m; band 3 ~5e-7


def test_the_target_writes_one_group_per_instrument_and_drops_relayed_copies(local_config):
    target = sat.SatelliteTarget(local_config, workers=2)
    for name in NAMES:
        target.feed(key(name), body(name), ARRIVED)
    # The same IASI field of view relayed by another centre, and ATMS again.
    target.feed(key("iasi").replace("fr-meteofrance", "hk-hko"), body("iasi"), ARRIVED)
    earlier = ARRIVED - dt.timedelta(minutes=5)
    target.feed(key("atms").replace("int-eumetsat", "us-noaa-nws"), body("atms"), earlier)
    summary = target.finish(None, None, "wis2_20261029T1200Z")
    assert summary["iasi_duplicates"] == 120 and summary["atms_duplicates"] == 207
    assert set(summary["rows"]) == set(NAMES)

    store = local_config.icechunk_repo(sat.STORE_PREFIX).readonly_session("main").store
    tree = xr.open_datatree(store, engine="zarr", consolidated=False)
    assert set(tree.children) == set(NAMES)
    atms = xr.open_zarr(store, group="atms", consolidated=False)
    assert atms.sizes == {"observation": 207, "channel": 22}
    assert atms["brightness_temperature"].dtype == np.float32
    expected = np.round(decoded("atms")["brightness_temperature"], 2)
    np.testing.assert_array_equal(
        np.round(atms["brightness_temperature"].values.astype("float64"), 2), expected
    )
    assert atms["channel_centre_frequency"].values[0] == 23.8e9
    # Every field of view first reached the cache with the relayed copy.
    first = np.datetime64(earlier.replace(tzinfo=None))
    assert (atms["first_arrival_time"].values == first).all()
    assert atms["first_arrival_time"].attrs["long_name"].startswith("earliest time any copy")
    amsua = xr.open_zarr(store, group="amsua", consolidated=False)
    assert amsua["channel_atovs_number"].values.tolist() == list(range(28, 43))
    iasi = xr.open_zarr(store, group="iasi", consolidated=False, mask_and_scale=False)
    assert iasi["scaled_radiance"].dtype == np.int16
    assert iasi["radiance_scale_factor"].dtype == np.int8
    assert iasi.sizes["observation"] == 120
    assert iasi["source_centre"].values[0] in ("fr-meteofrance", "hk-hko")

    again = sat.SatelliteTarget(local_config, workers=1)
    again.feed(key("atms"), body("atms"), ARRIVED)
    assert again.finish(None, None, "wis2_20261029T1200Z")["rows"] == {"atms": 0}
    later = sat.SatelliteTarget(local_config, workers=1)
    later.feed(key("atms"), body("atms"), ARRIVED)
    assert later.finish(None, None, "wis2_20261030T0000Z")["rows"] == {"atms": 0}
    assert xr.open_zarr(store, group="atms", consolidated=False).sizes["observation"] == 207


def test_a_wider_channel_set_later_widens_the_group(local_config):
    first = sat.SatelliteTarget(local_config, workers=1)
    arrays = decoded("iasi")
    narrow_width = arrays["channel_number"].size
    first.feed(key("iasi"), body("iasi"), ARRIVED)
    first.finish(None, None, "a")
    store_group = "iasi"
    paths = []
    widened = dict(arrays)
    extra = 8700 - narrow_width
    for name in ("scaled_radiance", "radiance_scale_factor"):
        widened[name] = np.concatenate(
            [arrays[name], np.full((arrays[name].shape[0], extra), 7, dtype="int64")], axis=1
        )
    widened["channel_number"] = np.arange(1, 8701)
    widened["observation_identity"] = arrays["observation_identity"] + 1
    path = local_config.data_dir / "wide.npz"
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(
        path.with_suffix(""),
        **{k.replace(".", "__"): v for k, v in widened.items()},
        allow_pickle=True,
    )
    paths.append(str(path))
    repo = local_config.icechunk_repo(sat.STORE_PREFIX)
    from collections import Counter

    assert sat.write_group(repo, sat.INSTRUMENTS[store_group], paths, "b", Counter()) == 120
    iasi = xr.open_zarr(repo.readonly_session("main").store, group="iasi", consolidated=False)
    assert iasi.sizes == {"observation": 240, "channel": 8700}
    assert iasi["channel_number"].values[-1] == 8700
    assert iasi["channel_central_wavenumber"].values[0] == 64500.0


def test_feed_returns_at_spool_speed_and_never_raises(local_config, monkeypatch):
    target = sat.SatelliteTarget(local_config, workers=1)
    started = time.perf_counter()
    for _ in range(20):
        target.feed(key("cris"), body("cris"), ARRIVED)
    assert time.perf_counter() - started < 5
    target.finish(None, None, "x")

    broken = sat.SatelliteTarget(local_config, workers=1)
    monkeypatch.setattr(broken, "_start", lambda: (_ for _ in ()).throw(OSError("disk full")))
    broken.feed(key("atms"), body("atms"), ARRIVED)
    assert broken.counts["unspooled_objects"] == 1


def test_a_corrupt_object_is_counted_not_fatal(local_config):
    target = sat.SatelliteTarget(local_config, workers=1)
    target.feed(key("atms"), b"BUFR\x00\x00\x20\x04" + b"\x00" * 24, ARRIVED)
    target.feed(key("mhs"), body("mhs"), ARRIVED)
    summary = target.finish(None, None, "x")
    # ecCodes crashes outright on this one: the worker dies, the object is isolated.
    assert summary.get("crashed_objects", 0) + summary.get("undecodable_messages", 0) == 1
    assert summary["rows"] == {"mhs": 1890}


def test_the_target_is_registered():
    assert "satellite" in load_targets()
    assert any(isinstance(t, sat.SatelliteTarget) for t in build_targets(None, ["satellite"]))


def test_channel_numbers_out_of_range_are_dropped_not_allocated(monkeypatch):
    """A missing channel number (2**31 - 1) once sized an IASI axis to 2 billion."""
    monkeypatch.setattr(sat, "MAX_CHANNEL", 3000)
    arrays = decoded("iasi")
    assert arrays["channel_number"].max() <= 3000
    assert arrays["scaled_radiance"].shape[1] == arrays["channel_number"].max()


def test_workers_hand_back_compact_types():
    arrays = sat._compact(decoded("iasi"))
    assert arrays["scaled_radiance"].dtype == np.int32
    assert sat._compact(decoded("atms"))["brightness_temperature"].dtype == np.float32


def test_first_arrival_joins_a_group_written_before_it_existed(local_config):
    from planetary_datasets.common.multidim import append_partition

    repo = local_config.icechunk_repo(sat.STORE_PREFIX)
    sat.ensure_root(repo)
    full = decoded("mhs")
    half = full["time"].size // 2
    old = {
        k: v if k.startswith("channel_") or k.endswith(".decimals") else v[:half]
        for k, v in full.items()
        if not k.startswith("channel_")
    }
    ds = sat.to_dataset(
        [sat._compact(old)], old["brightness_temperature"].shape[1], sat.INSTRUMENTS["mhs"]
    )
    assert "first_arrival_time" not in ds
    append_partition(repo, ds, "before", fixed_dims=["channel"], label="old", group="mhs")

    target = sat.SatelliteTarget(local_config, workers=1)
    later = ARRIVED + dt.timedelta(hours=12)
    target.feed(key("mhs"), body("mhs"), later)
    target.finish(None, None, "after")
    mhs = xr.open_zarr(repo.readonly_session("main").store, group="mhs", consolidated=False)
    assert mhs.sizes["observation"] == full["time"].size
    first = mhs["first_arrival_time"].values
    assert np.isnat(first[:half]).all(), "rows written before the variable existed"
    assert (first[half:] == np.datetime64(later.replace(tzinfo=None))).all()


def test_groups_already_holding_the_window_are_not_fed_again(local_config):
    first = sat.SatelliteTarget(local_config, workers=1)
    first.feed(key("atms"), body("atms"), ARRIVED)
    first.finish(None, None, "wis2_20261029T1200Z")

    again = sat.SatelliteTarget(local_config, workers=1)
    again.prepare("wis2_20261029T1200Z")
    assert not again.wants(key("atms"), 1), "atms holds the window already"
    assert again.wants(key("mhs"), 1)
    gras = "data/int-eumetsat/data/core/weather/space-based-observations/metop-c/gras/x.bufr"
    assert again.wants(gras, 1), "a product group not written yet"
    fresh = sat.SatelliteTarget(local_config, workers=1)
    fresh.prepare("wis2_20261030T0000Z")
    assert fresh.wants(key("atms"), 1)


GTS = {"atms": "gts_atms", "mhs": "gts_mhs", "iasi": "gts_iasi"}


def as_gts(name: str, heading: str) -> str:
    """The topic fixture ``name`` as if relayed under the GTS ``heading`` (``INST KWBC``)."""
    letters, centre = heading.split()
    return f"data/de-dwd-gts-to-wis2/data/core/{'/'.join(letters)}/01/{centre}/A_{name}_relayed"


@pytest.mark.parametrize(("group", "fixture"), sorted(GTS.items()))
def test_gts_bulletins_decode_into_their_instrument_group(group, fixture):
    assert sat.gts_heading(key(fixture)) in sat.INSTRUMENTS[group].gts
    found, arrays, counts = sat.decode_object(key(fixture), body(fixture), ARRIVED)
    assert found == group and arrays is not None, counts
    assert arrays["source_centre"][0].endswith("-gts-to-wis2")
    assert np.isfinite(arrays["latitude"]).all()
    assert (arrays["time"] > np.datetime64("2026-11-01")).all()
    reference = decoded(group)
    assert arrays["channel_number"].size <= max(reference["channel_number"].size, 8700)
    if group == "mhs":
        assert arrays["channel_atovs_number"].tolist() == list(range(43, 48))
    if group == "atms":
        assert arrays["channel_number"].tolist() == list(range(1, 23))


def test_a_gts_bulletin_of_another_template_is_not_taken_into_the_group():
    # AMSU-A (310009) under an MHS heading must not land in mhs with 15 channels.
    group, arrays, counts = sat.decode_object(as_gts("x", "INMX SBBR"), body("amsua"), ARRIVED)
    assert group == "mhs" and arrays is None
    assert counts["messages_of_other_templates"] == 1


def test_ledger_entries_keep_a_windows_topics_and_gts_bulletins_apart():
    gts = key("gts_atms")
    assert sat.ledger_entry("w", gts) == "w:gts" and sat.ledger_entry("w", key("atms")) == "w"
    assert sat.window_entries(sat.INSTRUMENTS["atms"], "w") == ["w", "w:gts"]
    assert sat.window_entries(sat.INSTRUMENTS["cris"], "w") == ["w"]


def feed_wanted(target: sat.SatelliteTarget, objects: list[tuple[str, bytes]]) -> list[str]:
    """Feed what the target wants, as the archiver's router does; the keys fed."""
    fed = []
    for name, data in objects:
        if target.wants(name, len(data)):
            target.feed(name, data, ARRIVED)
            fed.append(name)
    return fed


def test_a_rerun_adds_only_the_gts_rows_missing_from_a_window_written_before(
    local_config, monkeypatch
):
    window = "wis2_20261029T1200Z"
    repo = local_config.icechunk_repo(sat.STORE_PREFIX)
    # The window as written before the groups took GTS bulletins: topics only, ledgered
    # under the bare window name.
    with monkeypatch.context() as before:
        before.setattr(sat, "window_entries", lambda instrument, partition: [partition])
        first = sat.SatelliteTarget(local_config, workers=1)
        first.prepare(window)
        for name in ("atms", "mhs", "iasi"):
            first.feed(key(name), body(name), ARRIVED)
        written = first.finish(None, None, window)["rows"]
    # A later window, so the first one's rows are not the latest the group holds, and no
    # "recent identities": only the scan of stored rows by minute can find repeats.
    later = sat.SatelliteTarget(local_config, workers=1)
    later.feed(key("gts_mhs"), body("gts_mhs"), ARRIVED)
    later.finish(None, None, "wis2_20261030T0000Z")
    monkeypatch.setattr(sat, "RECENT_IDENTITIES", 0)

    objects = [(key(name), body(name)) for name in ("atms", "mhs", "iasi")]
    # GTS copies of fields of view the topics already gave, and GTS-only ones.
    objects += [(as_gts("atms", "INST KWBC"), body("atms"))]
    objects += [(as_gts("iasi", "INQX SBBR"), body("iasi"))]
    objects += [(key(fixture), body(fixture)) for fixture in GTS.values()]
    rerun = sat.SatelliteTarget(local_config, workers=1)
    rerun.prepare(window)
    fed = feed_wanted(rerun, objects)
    assert fed and all(sat.gts_heading(k) for k in fed), "the topics are written already"
    summary = rerun.finish(None, None, window)

    new = {group: decoded_gts(fixture)["time"].size for group, fixture in GTS.items()}
    assert summary["rows"] == {"atms": new["atms"], "iasi": new["iasi"], "mhs": 0}
    assert summary["atms_duplicates"] == written["atms"]
    assert summary["iasi_duplicates"] == written["iasi"]
    # The MHS bulletin's rows were stored by the later window, wherever they now are.
    assert summary["mhs_duplicates"] == new["mhs"]
    store = repo.readonly_session("main").store
    for group in GTS:
        rows = xr.open_zarr(store, group=group, consolidated=False)["observation_identity"]
        assert len(np.unique(rows.values)) == rows.size, f"{group} holds a row twice"
    from planetary_datasets.common.multidim import ledger

    assert {window, f"{window}:gts"} <= ledger(repo, group="atms")

    again = sat.SatelliteTarget(local_config, workers=1)
    again.prepare(window)
    assert feed_wanted(again, objects) == []
    assert again.finish(None, None, window) == {"objects": 0}
    # Fed regardless of the ledger (a run racing the first), nothing is written twice.
    forced = sat.SatelliteTarget(local_config, workers=1)
    for name, data in objects:
        forced.feed(name, data, ARRIVED)
    assert set(forced.finish(None, None, window)["rows"].values()) == {0}


def decoded_gts(fixture: str) -> dict:
    group, arrays, counts = sat.decode_object(key(fixture), body(fixture), ARRIVED)
    assert arrays is not None, counts
    return arrays


def test_a_window_written_with_gts_bulletins_is_ledgered_for_both(local_config):
    window = "wis2_20261105T1200Z"
    target = sat.SatelliteTarget(local_config, workers=1)
    target.feed(key("atms"), body("atms"), ARRIVED)
    target.feed(key("gts_atms"), body("gts_atms"), ARRIVED)
    summary = target.finish(None, None, window)
    assert (
        summary["rows"]["atms"]
        == decoded("atms")["time"].size + decoded_gts("gts_atms")["time"].size
    )
    again = sat.SatelliteTarget(local_config, workers=1)
    again.prepare(window)
    assert not again.wants(key("atms"), 1) and not again.wants(key("gts_atms"), 1)
