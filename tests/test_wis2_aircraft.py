"""Aircraft reports from WIS2 IUA bulletins into one Icechunk store."""

from __future__ import annotations

import datetime as dt

import numpy as np
import pytest
import xarray as xr
from wis2_bufr import bulletin, encode_amdar, report

from planetary_datasets.providers.observations import wis2_aircraft as aircraft
from planetary_datasets.providers.observations.wis2_aircraft import (
    AircraftTarget,
    Batch,
    is_aircraft,
    to_dataset,
)
from planetary_datasets.providers.observations.wis2_targets import Router

KEY = "data/jp-jma-gts-to-wis2/data/core/I/U/A/G/04/VHHH/A_IUAG04VHHH270642_C_RJTD_20261027064754_178.bufr"
OTHER = (
    "data/de-dwd-gts-to-wis2/data/core/I/U/A/D/01/EGRR/A_IUAD01EGRR271341_C_EDZW_2026102713441.bufr"
)
ARRIVED = dt.datetime(2026, 10, 27, 6, 47, 54, tzinfo=dt.timezone.utc)


def test_only_iua_bulletins_are_aircraft():
    assert is_aircraft(KEY) and is_aircraft(OTHER)
    assert not is_aircraft("data/jp-jma-gts-to-wis2/data/core/I/U/P/A/01/KWBC/A_IUPA01.bufr")
    assert not is_aircraft("data/cn-cma/data/core/weather/surface-based-observations/synop/x.bufr")


def test_a_bulletin_decodes_to_reports_with_provenance():
    batch = Batch()
    message = encode_amdar(
        [report("HK0013", edr=[(0.1, 0.2)]), report("HK0014", minute=37, temperature=250.15)]
    )
    batch.add(KEY, bulletin(message), ARRIVED)
    assert (batch.bulletins, batch.messages, len(batch.rows)) == (1, 1, 2)
    first = batch.rows[0]
    assert first["time"] == np.datetime64("2026-10-27T06:36:00")
    assert first["originating_centre"] == "VHHH" and first["relay"] == "jp-jma-gts-to-wis2"
    assert first["bulletin"] == "A_IUAG04VHHH270642"
    assert first["meanTurbulenceIntensityEddyDissipationRate"] == pytest.approx(0.1)


def test_a_bad_message_is_counted_and_the_rest_kept():
    batch = Batch()
    good = encode_amdar([report()])
    broken = b"BUFR" + len(good).to_bytes(3, "big") + b"\x04" + b"\x00" * (len(good) - 8)
    batch.add(KEY, bulletin(broken, good), ARRIVED)
    assert batch.failed == 1 and len(batch.rows) == 1


def test_the_dataset_is_lossless_and_typed():
    batch = Batch()
    batch.add(KEY, bulletin(encode_amdar([report(temperature=291.85), report(minute=40)])), ARRIVED)
    for row in batch.rows:
        row["report_id"] = aircraft.report_id(row)
    ds = to_dataset(batch.rows)
    assert ds["temperature"].dtype == np.float32
    assert ds["temperature"].values.tolist() == pytest.approx([291.85, 291.85])
    assert str(np.float32(ds["temperature"].values[0])) == "291.85"
    assert ds["latitude"].values[0] == pytest.approx(22.13333, abs=1e-9)
    assert ds["flight_level"].dtype == np.int32 and ds["flight_level"].values[0] == 1058
    assert ds["phase_of_flight"].dtype == np.int16
    assert ds["detailed_phase_of_flight"].values.tolist() == [-1, -1], "absent from the layout"
    assert np.isnan(ds["dewpoint_temperature"].values).all()
    assert ds["aircraft_identifier"].values.tolist() == ["HK0013", "HK0013"]


def _store(local_config):
    repo = local_config.icechunk_repo(aircraft.STORE_PREFIX)
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


def test_windows_append_once_and_duplicates_are_dropped(local_config):
    one, two = report("HK0013"), report("HK0014", minute=37)
    first = Batch()
    # The same report relayed twice in one window, and the same bulletin by two relays.
    first.add(KEY, bulletin(encode_amdar([one, two])), ARRIVED)
    first.add(OTHER, bulletin(encode_amdar([one])), ARRIVED)
    assert aircraft.write(first, "wis2_20261027T1200Z", local_config) == 2
    assert aircraft.write(first, "wis2_20261027T1200Z", local_config) == 0, "ledgered"

    second = Batch()
    # The next window repeats one report and brings a new one.
    second.add(KEY, bulletin(encode_amdar([two, report("HK0015", minute=50)])), ARRIVED)
    assert aircraft.write(second, "wis2_20261028T0000Z", local_config) == 1

    ds = _store(local_config)
    assert ds.sizes["observation"] == 3
    assert sorted(ds["aircraft_identifier"].values.tolist()) == ["HK0013", "HK0014", "HK0015"]
    assert ds["time"].dtype.kind == "M" and ds["arrival_time"].dtype.kind == "M"
    assert ds["temperature"].attrs["units"] == "K"
    assert len(set(ds["report_id"].values.tolist())) == 3


def test_a_window_with_no_new_reports_is_still_ledgered(local_config):
    batch = Batch()
    batch.add(KEY, bulletin(encode_amdar([report()])), ARRIVED)
    assert aircraft.write(batch, "a", local_config) == 1
    assert aircraft.write(batch, "b", local_config) == 0
    from planetary_datasets.common.multidim import ledger

    assert {"a", "b"} <= ledger(local_config.icechunk_repo(aircraft.STORE_PREFIX))


def test_the_target_decodes_as_the_archive_streams(local_config):
    target = AircraftTarget(local_config)
    route = Router([target])
    route(KEY, 10, bulletin(encode_amdar([report(), report(minute=41)])), ARRIVED)
    route("data/cn-cma/data/core/weather/x.nc", 10, b"not aircraft", ARRIVED)
    summary = target.finish(None, None, "wis2_20261027T1200Z")
    assert summary["rows"] == 2 and summary["bulletins"] == 1 and summary["objects"] == 1
    assert _store(local_config).sizes["observation"] == 2
