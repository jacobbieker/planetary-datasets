"""WIS2 land surface reports: decoding real BUFR and appending to an Icechunk store.

The fixtures are real WIS2 Global Cache objects (WMO core data, free and unrestricted):

* ``isn_13_subsets.bufr``: a GTS bulletin of 13 Australian SYNOPs (3 07 080, uncompressed);
* ``ism_synop.bufr``: two French SYNOPs (3 07 096) with precipitation over five periods;
* ``isa_compressed.bufr``: four Canadian hourly automatic-station reports (3 07 091,
  compressed);
* ``jma_single.bufr``: one Japanese SYNOP from the JMA national topic;
* ``ism_cloud.bufr``: a corrected (CCB) Argentine SYNOP reporting 25 % cloud cover;
* ``isx_scatterometer.bufr``: a satellite (category 12) message, not land.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.providers.observations import wis2_land as land
from planetary_datasets.providers.observations import wis2_targets

DATA = pathlib.Path(__file__).parent / "data" / "wis2_land"
ARRIVED = dt.datetime(2026, 10, 28, 7, 30, tzinfo=dt.timezone.utc)


def body(name: str) -> bytes:
    return (DATA / name).read_bytes()


def by_station(rows):
    return {r["station_id"]: r for r in rows}


@pytest.mark.parametrize(
    ("key", "wanted"),
    [
        ("data/jp-jma/data/core/weather/surface-based-observations/synop/WIGOS_x.bufr4", True),
        ("data/de-dwd-gts-to-wis2/data/core/I/S/M/N/07/LFPW/A_ISMN07LFPW280600_C_EDZW", True),
        ("data/jp-jma-gts-to-wis2/data/core/I/S/A/I/15/SBBR/A_ISAI15SBBR272100_C_RJTD", True),
        ("data/de-dwd-gts-to-wis2/data/core/I/S/X/X/14/EUSR/A_ISXX14EUSR270031_C_EDZW", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/S/S/X/01/KWBC/A_ISSX01KWBC", False),
        ("data/us-noaa-nws/data/core/weather/surface-based-observations/ship/x.bufr", False),
        ("data/de-dwd-gts-to-wis2/data/core/I/U/A/X/01/EGRR/A_IUAX01EGRR", False),
    ],
)
def test_wants_land_topics_and_land_bulletins_only(key, wanted):
    assert land.wants(key) is wanted


def test_an_uncompressed_bulletin_is_split_into_its_stations():
    decoded = land.decode(body("isn_13_subsets.bufr"), "de-dwd-gts-to-wis2", ARRIVED)
    assert decoded.messages == 1 and decoded.failed == 0
    # One of the 13 subsets has no position, so it cannot be stored.
    assert len(decoded.rows) == 12 and decoded.incomplete == 1
    rows = by_station(decoded.rows)
    darwin = rows["0-20000-0-95122"]
    assert darwin["station_name"] == "DARWIN NTC AWS"
    assert darwin["mean_sea_level_pressure"] == 101320.0
    broome = rows["0-20000-0-95202"]
    assert broome["2m_temperature"] == pytest.approx(299.85)
    assert broome["10m_wind_speed"] == pytest.approx(4.1)
    assert all(r["time"] == np.datetime64("2026-10-27T22:00:00") for r in decoded.rows)
    assert all(r["arrived"] == np.datetime64("2026-10-28T07:30:00") for r in decoded.rows)


def test_a_compressed_message_gives_each_subset_its_own_values():
    decoded = land.decode(body("isa_compressed.bufr"), "ca-eccc-msc")
    rows = by_station(decoded.rows)
    assert len(rows) == 4
    mistook = rows["0-20000-0-71381"]
    assert mistook["station_name"] == "MISTOOK"
    assert mistook["2m_temperature"] == pytest.approx(268.45)
    assert mistook["bufr_template"] == 307091
    # Hourly extremes: the period is given once, in minutes (-60), so it reads 1 h.
    assert mistook["maximum_2m_temperature_period"] == 1.0
    kuujjuarapik = rows["0-20000-0-71615"]
    assert kuujjuarapik["10m_wind_gust"] == 7.0
    assert kuujjuarapik["10m_wind_gust_period"] == 10.0
    assert "2m_temperature" not in rows["0-20000-0-71522"]


def test_precipitation_lands_in_the_variable_of_its_period():
    rows = by_station(land.decode(body("ism_synop.bufr")).rows)
    abbeville = rows["0-20000-0-07005"]
    assert abbeville["total_precipitation_6h"] == pytest.approx(0.1)
    assert abbeville["total_precipitation_12h"] == pytest.approx(0.4)
    assert abbeville["total_precipitation_24h"] == pytest.approx(3.0)
    assert abbeville["temperature_sensor_height"] == 1.5
    lille = rows["0-20000-0-07015"]
    assert lille["present_weather"] == 44 and lille["horizontal_visibility"] == 420.0


def test_a_national_single_station_report():
    (sendai,) = land.decode(body("jma_single.bufr")).rows
    assert sendai["station_id"] == "0-20000-0-47590" and sendai["station_name"] == "SENDAI"
    assert sendai["2m_dewpoint_temperature"] == pytest.approx(279.8)
    assert sendai["sunshine_duration_1h"] == 0.0


def test_other_categories_and_garbage_are_counted_not_raised():
    assert land.decode(body("isx_scatterometer.bufr")).other_category == 1
    real = body("ism_synop.bufr")
    start = real.find(b"BUFR")
    length = int.from_bytes(real[start + 4 : start + 7], "big")
    # Cut short: not even a message.
    assert land.decode(real[: start + 60]).messages == 0
    # A message whose data section is scrambled fails to decode, and is counted.
    scrambled = bytearray(real[start : start + length])
    for i in range(60, len(scrambled) - 4):
        scrambled[i] ^= 0x5A
    decoded = land.decode(b"ISMN07 LFPW 280600\r\r\n" + bytes(scrambled))
    assert (decoded.messages, decoded.failed, decoded.rows) == (1, 1, [])


def test_deduplication_keeps_the_fullest_copy_and_corrections():
    (sendai,) = land.decode(body("jma_single.bufr")).rows
    sparse = {k: v for k, v in sendai.items() if k not in ("2m_temperature", "station_name")}
    correction = {**sendai, "update_sequence_number": 1}
    kept = land.deduplicate([sparse, sendai, correction])
    assert len(kept) == 2
    assert {r["update_sequence_number"] for r in kept} == {0, 1}
    assert next(r for r in kept if r["update_sequence_number"] == 0)["station_name"] == "SENDAI"
    seen = {land.identity(sendai["station_id"], sendai["time"], 0)}
    assert land.deduplicate([sendai], seen) == []


def feed_all(target):
    for name, key in [
        ("isn_13_subsets.bufr", "data/de-dwd-gts-to-wis2/data/core/I/S/N/K/15/AMMC/A"),
        ("ism_synop.bufr", "data/de-dwd-gts-to-wis2/data/core/I/S/M/N/07/LFPW/A"),
        (
            "isa_compressed.bufr",
            "data/ca-eccc-msc/data/core/weather/surface-based-observations/synop/I",
        ),
        ("jma_single.bufr", "data/jp-jma/data/core/weather/surface-based-observations/synop/W"),
        # The same report again, relayed by a second centre.
        ("jma_single.bufr", "data/jp-jma-gts-to-wis2/data/core/I/S/M/N/01/RJTD/A"),
        ("isx_scatterometer.bufr", "data/de-dwd-gts-to-wis2/data/core/I/S/M/X/14/EUSR/A"),
    ]:
        assert target.wants(key, 0)
        target.feed(key, body(name), ARRIVED)


def test_windows_append_once_and_relays_and_repeats_are_dropped(local_config):
    target = land.LandTarget(local_config, workers=1)
    feed_all(target)
    first = target.finish(None, None, "wis2_20261028T1200Z")
    assert first["reports"] == 12 + 2 + 4 + 1 + 1
    assert first["rows_written"] == 19
    assert first["other_category_messages"] == 1

    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes["observation"] == 19
    assert ds["time"].dtype.kind == "M"
    assert set(ds["station_id"].values) >= {"0-20000-0-47590", "0-20000-0-95122"}
    (index,) = np.flatnonzero(ds["station_id"].values == "0-20000-0-47590")
    assert float(ds["2m_temperature"][index]) == pytest.approx(284.9)

    # A later window carrying the same reports adds nothing; the same window is a no-op.
    again = land.LandTarget(local_config, workers=1)
    feed_all(again)
    assert again.finish(None, None, "wis2_20261029T0000Z")["rows_written"] == 0
    once_more = land.LandTarget(local_config, workers=1)
    once_more.feed("data/x/data/core/I/S/M/A", body("jma_single.bufr"), ARRIVED)
    assert once_more.finish(None, None, "wis2_20261028T1200Z")["rows_written"] == 0
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes["observation"] == 19


def test_cloud_cover_is_stored_as_percent_and_reads_as_a_fraction(local_config):
    target = land.LandTarget(local_config, workers=1)
    target.feed(
        "data/de-dwd-gts-to-wis2/data/core/I/S/M/I/02/SABM/A", body("ism_cloud.bufr"), ARRIVED
    )
    assert target.finish(None, None, "w")["rows_written"] == 1
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds["station_name"].values[0] == "MENDOZA AERO"
    assert float(ds["total_cloud_cover"][0]) == pytest.approx(0.25)
    assert ds["total_cloud_cover"].attrs["units"] == "(0 - 1)"
    raw = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert int(raw["total_cloud_cover"][0]) == 25
    # A corrected bulletin (CCB): its update sequence number is kept.
    assert int(ds["update_sequence_number"][0]) == 2


def test_a_report_without_a_position_is_kept_and_filled_from_its_station():
    (sendai,) = land.decode(body("jma_single.bufr")).rows
    bare = {k: v for k, v in sendai.items() if k not in ("latitude", "longitude", "station_name")}
    bare["time"] = np.datetime64("2026-10-29T15:00:00")
    stranger = {**bare, "station_id": "0-20000-0-99999"}
    assert land.fill_positions([sendai, bare, stranger]) == 1
    assert bare["latitude"] == sendai["latitude"] and bare["station_name"] == "SENDAI"
    assert "latitude" not in stranger
    ds = land.to_dataset([sendai, bare, stranger])
    (index,) = np.flatnonzero(ds["station_id"].values == "0-20000-0-99999")
    assert np.isnan(ds["latitude"].values[index])


def test_nil_reports_are_skipped_and_a_missing_date_comes_from_the_message():
    nil = land._Report()
    for name, value in [
        ("blockNumber", 85),
        ("stationNumber", 140),
        ("year", 2026),
        ("month", 10),
        ("day", 29),
        ("hour", 15),
    ]:
        nil.add(name, value)
    assert nil.finish() is None

    undated = land._Report()
    for name, value in [
        ("blockNumber", 11),
        ("stationNumber", 567),
        ("hour", 15),
        ("airTemperature", 280.15),
    ]:
        undated.add(name, value)
    row = undated.finish(typical=(2026, 10, 29, 12, 0))
    assert row["time"] == np.datetime64("2026-10-29T15:00:00")
    assert row["station_id"] == "0-20000-0-11567" and row["2m_temperature"] == 280.15


def test_decoding_over_processes_matches_one_process(local_config, monkeypatch):
    monkeypatch.setattr(land, "DECODE_BATCH", 1)
    serial, parallel = (
        land.LandTarget(local_config, workers=1),
        land.LandTarget(local_config, workers=2),
    )
    feed_all(serial)
    feed_all(parallel)
    one, many = serial.decode_pending(), parallel.decode_pending()
    key = lambda r: (r["station_id"], r["centre"])  # noqa: E731
    assert sorted(map(key, one.rows)) == sorted(map(key, many.rows))
    assert len(many.rows) == 20


def test_an_archived_tar_backfills_the_store_through_the_archiver(local_config, monkeypatch):
    import io
    import tarfile

    from planetary_datasets.providers.observations import wis2_archive

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for name, key in [
            ("ism_synop.bufr", "data/de-dwd-gts-to-wis2/data/core/I/S/M/N/07/LFPW/A_ISMN07"),
            ("jma_single.bufr", "data/jp-jma/data/core/weather/surface-based-observations/synop/W"),
            ("isx_scatterometer.bufr", "data/de-dwd-gts-to-wis2/data/core/I/S/X/X/14/EUSR/A"),
        ]:
            info = tarfile.TarInfo(key)
            info.size, info.mtime = len(body(name)), ARRIVED.timestamp()
            tar.addfile(info, io.BytesIO(body(name)))

    class Dest:
        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(buffer.getvalue())}

        def head_object(self, Bucket, Key):
            from botocore.exceptions import ClientError

            if not Key.endswith(".tar"):
                raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
            return {}

    monkeypatch.setattr(wis2_archive, "dest_client", lambda config: Dest())
    key = "bkr/obs/wis2/wis2_20261028T1200Z.tar"
    summary = wis2_archive.targets_from_tar(key, config=local_config, targets=["land"])
    assert summary["land"]["rows_written"] == 3
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.attrs["partitions_written"] == [wis2_archive.partition_name(key)]
    assert sorted(ds["station_id"].values) == [
        "0-20000-0-07005",
        "0-20000-0-07015",
        "0-20000-0-47590",
    ]


def test_the_target_registers_with_the_archiver():
    targets = wis2_targets.load_targets()
    assert targets["land"] is land.LandTarget


def stored_times(repo, name: str) -> np.ndarray:
    """A stored time column as datetime64[s], NaT for its fill, without xarray's decoding.

    (xarray before 2026.9 cannot decode an integer time column holding NaT.)
    """
    import zarr

    array = zarr.open_group(repo.readonly_session("main").store, mode="r")[name]
    assert str(array.attrs["units"]).startswith("nanoseconds since 1970-01-01")
    return array[:].astype("int64").view("datetime64[ns]").astype("datetime64[s]")


def test_first_arrival_is_the_earliest_copy_and_arrived_the_kept_ones():
    early, late = ARRIVED - dt.timedelta(minutes=6), ARRIVED + dt.timedelta(minutes=3)
    (sendai,) = land.decode(body("jma_single.bufr"), "jp-jma", ARRIVED).rows
    (relayed,) = land.decode(body("jma_single.bufr"), "jp-jma-gts-to-wis2", early).rows
    (sparse,) = land.decode(body("jma_single.bufr"), "de-dwd-gts-to-wis2", late).rows
    del sparse["2m_temperature"]
    (kept,) = land.deduplicate([sparse, sendai, relayed])
    # The fullest copy read first is kept; the relay's identical copy arrived earliest.
    assert kept["centre"] == "jp-jma"
    assert kept["arrived"] == np.datetime64("2026-10-28T07:30:00")
    assert kept["first_arrival_time"] == np.datetime64("2026-10-28T07:24:00")
    assert "first_arrival_time" not in sendai, "the decoded rows are left as they were"
    ds = land.to_dataset([kept])
    assert ds["first_arrival_time"].dtype == "datetime64[s]"
    assert ds["first_arrival_time"].attrs == land.COLUMNS["first_arrival_time"].attrs
    assert ds["arrived"].attrs["long_name"] == (
        "time the copy of this observation kept here arrived in the WIS2 Global Cache"
    )


def test_first_arrival_is_added_to_a_store_written_without_it(local_config):
    import zarr

    from planetary_datasets.common.multidim import append_partition

    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    # A window written before the variable existed, its "arrived" described the old way.
    (old,) = land.decode(body("ism_cloud.bufr"), "de-dwd-gts-to-wis2", ARRIVED).rows
    legacy = land.to_dataset([old]).drop_vars("first_arrival_time")
    legacy["arrived"].attrs = {"long_name": "time the copy kept reached the cache"}
    append_partition(repo, legacy, "old", label="wis2_land")

    target = land.LandTarget(local_config, workers=1)
    for key, arrived in [
        ("data/jp-jma/data/core/weather/surface-based-observations/synop/W", ARRIVED),
        ("data/jp-jma-gts-to-wis2/data/core/I/S/M/N/01/RJTD/A", ARRIVED - dt.timedelta(minutes=2)),
    ]:
        target.feed(key, body("jma_single.bufr"), arrived)
    assert target.finish(None, None, "new")["rows_written"] == 1

    assert stored_times(repo, "arrived").tolist() == [ARRIVED.replace(tzinfo=None)] * 2
    first = stored_times(repo, "first_arrival_time")
    assert np.isnat(first[0]) and first[1] == np.datetime64("2026-10-28T07:28:00")
    group = zarr.open_group(repo.readonly_session("main").store, mode="r")
    assert group["arrived"].attrs["long_name"] == land.ARRIVAL_ATTRS["arrived"]["long_name"]
    assert group["first_arrival_time"].attrs["comment"].endswith("have no value (NaT)")
