"""WIS2 land store: the hydrometric and radar groups, and re-running archived windows.

Fixtures: two real Water Survey of Canada real-time CSVs from the WIS2 Global Cache
(``ca-eccc-msc`` hydrology topic, cut to their first rows), a real SYNOP bulletin from
``tests/data/wis2_land`` and small ODIM_H5 polar volumes built here with the structure of
RMI's (``what``/``where``/``how``, ``datasetN/dataN``).
"""

from __future__ import annotations

import datetime as dt
import io
import pathlib

import h5py
import numpy as np
import pytest
import xarray as xr
import zarr

from planetary_datasets.common.multidim import ledger
from planetary_datasets.providers.observations import wis2_land as land
from planetary_datasets.providers.observations import wis2_land_hydrometric as hydrometric

DATA = pathlib.Path(__file__).parent / "data"
HYDRO = DATA / "wis2_land_hydrometric"
ARRIVED = dt.datetime(2026, 11, 1, 3, 0, tzinfo=dt.timezone.utc)
PARTITION = "wis2_20261101T1200Z"

HYDRO_KEY = "data/ca-eccc-msc/data/core/hydrology/experimental/{}"
RADAR_KEY = "data/be-rmib/data/core/weather/experimental/bel-radar/{}"
SYNOP_KEY = "data/de-dwd-gts-to-wis2/data/core/I/S/M/N/07/LFPW/A_ISMN07LFPW280600_C_EDZW"


def odim_volume(
    time: str = "000000",
    quantity: str = "DBZH",
    sweeps: int = 2,
    bins: int = 6,
    node: str = "bewid",
    dtype: str = "uint8",
    seed: int = 0,
) -> bytes:
    """A small ODIM_H5 polar volume, laid out as RMI's are."""
    rng = np.random.default_rng(seed)
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as f:
        f.attrs["Conventions"] = np.bytes_("ODIM_H5/V2_0")
        what = f.create_group("what")
        for key, value in {
            "object": "PVOL",
            "source": f"WMO:06477,RAD:BE41,NOD:{node},PLC:Wideumont,CTY:605",
            "date": "20261101",
            "time": time,
            "version": "H5rad 2.0",
        }.items():
            what.attrs[key] = np.bytes_(value)
        where = f.create_group("where")
        where.attrs.update({"lat": 49.9136, "lon": 5.5044, "height": 585.0})
        how = f.create_group("how")
        how.attrs.update({"startepochs": 1793577618, "endepochs": 1793577867, "beamwidth": 0.951})
        how.attrs["task"] = np.bytes_("bewid_scan_v3_Z.vol")
        for i in range(sweeps):
            ds = f.create_group(f"dataset{i + 1}")
            ds.create_group("what").attrs.update(
                {
                    "product": np.bytes_("SCAN"),
                    "startdate": np.bytes_("20261101"),
                    "starttime": np.bytes_(f"0000{10 * i:02d}"),
                    "enddate": np.bytes_("20261101"),
                    "endtime": np.bytes_(f"0000{10 * i + 9:02d}"),
                }
            )
            ds.create_group("where").attrs.update(
                {
                    "elangle": 0.3 + i,
                    "nbins": bins - i,
                    "nrays": 4,
                    "rscale": 250.0,
                    "rstart": 0.0,
                    "a1gate": 2,
                }
            )
            ds.create_group("how").attrs.update(
                {"NI": 7.95, "startazA": np.arange(4) * 90.0, "stopazA": np.arange(4) * 90.0 + 1}
            )
            data = ds.create_group("data1")
            data.create_dataset(
                "data",
                data=rng.integers(0, 255, size=(4, bins - i)).astype(dtype),
                compression="gzip",
            )
            data.create_group("what").attrs.update(
                {
                    "quantity": np.bytes_(quantity),
                    "gain": 0.5,
                    "offset": -32.0,
                    "nodata": 255.0,
                    "undetect": 0.0,
                }
            )
    return buffer.getvalue()


def feed_all(target, objects):
    for key, body, arrived in objects:
        if target.wants(key, len(body)):
            target.feed(key, body, arrived)


def window_objects():
    """A window's worth of objects for the land store.

    A SYNOP bulletin, hydrometric CSVs (one sent twice) and radar volumes (one under both
    of RMI's names).
    """
    later = ARRIVED + dt.timedelta(minutes=5)
    first = odim_volume("000000")
    return [
        (SYNOP_KEY, (DATA / "wis2_land" / "ism_synop.bufr").read_bytes(), ARRIVED),
        (
            HYDRO_KEY.format("AB_05AA004_hourly_hydrometric.csv"),
            (HYDRO / "AB_05AA004_hourly_hydrometric.csv").read_bytes(),
            ARRIVED,
        ),
        (
            HYDRO_KEY.format("AB_05AA004_hourly_hydrometric-1793866936.0.csv"),
            (HYDRO / "AB_05AA004_hourly_hydrometric.csv").read_bytes(),
            later,
        ),
        (
            HYDRO_KEY.format("AB_05AA008_hourly_hydrometric_D-20261102023620Z.csv"),
            (HYDRO / "AB_05AA008_hourly_hydrometric_D.csv").read_bytes(),
            later,
        ),
        (RADAR_KEY.format("T_PAGW41_C_EBUM_20261101000018.hdf"), first, ARRIVED),
        (RADAR_KEY.format("be-rmib_be-radar_T_PAGW41_C_EBUM_20261101000018.hdf"), first, later),
        (
            RADAR_KEY.format("T_PAGW41_C_EBUM_20261101000518.hdf"),
            odim_volume("000500", sweeps=3, bins=8, seed=1),
            later,
        ),
        (
            RADAR_KEY.format("T_PAHZ41_C_EBUM_20261101000018.hdf"),
            odim_volume("000000", quantity="VRAD", seed=2),
            ARRIVED,
        ),
    ]


def run(config, objects, partition=PARTITION):
    target = land.LandTarget(config, workers=1)
    target.prepare(partition)
    feed_all(target, objects)
    return target, target.finish(None, None, partition)


def sizes(repo) -> dict[str, int]:
    """Rows of the root and of each new group."""
    store = repo.readonly_session("main").store
    root = zarr.open_group(store, mode="r")
    out = {"root": root["station_id"].shape[0] if "station_id" in root else 0}
    for group, dim_array in (
        ("hydrometric", "station_id"),
        ("radar/bewid_dbzh", "time"),
        ("radar/bewid_vrad", "time"),
    ):
        try:
            out[group] = zarr.open_group(store, path=group, mode="r")[dim_array].shape[0]
        except (KeyError, FileNotFoundError, zarr.errors.GroupNotFoundError):
            out[group] = 0
    return out


@pytest.mark.parametrize(
    ("key", "part"),
    [
        (HYDRO_KEY.format("AB_05AA004_daily_hydrometric-1793866936.0.csv"), "hydrometric"),
        (HYDRO_KEY.format("AB_daily_hydrometric.csv"), "hydrometric"),
        (RADAR_KEY.format("20261105052000.rad.behel.pvol.vrad.scanz.hdf"), "radar"),
        (RADAR_KEY.format("T_PAGY41_C_EBUM_20261102000018.hdf"), "radar"),
        (
            "data/xx-centre/data/core/weather/surface-based-observations/radar/a.pvol.dbzh.h5",
            "radar",
        ),
        (
            "data/cy-dom/data/core/weather/experimental/surface-based-observations/radar/"
            "LONGRANGE_PFO_20261105001038_00.h5",
            None,
        ),
        (
            "data/uk-metoffice/data/core/weather/experimental/surface-based-observations/"
            "weather-radar/A_PAAL21EGRR050345_C_EGRR_20261105035026.929.000.bufr",
            None,
        ),
        (SYNOP_KEY, "reports"),
        ("data/ca-eccc-msc/data/core/weather/experimental/x_MSC_CitypageWeather_en.xml", None),
    ],
)
def test_objects_go_to_their_part_of_the_store(key, part):
    assert land.LandTarget.part(key) == part


def test_local_times_are_converted_to_utc_exactly():
    assert hydrometric.utc("2026-10-31T00:05:00-07:00") == np.datetime64("2026-10-31T07:05:00")
    assert hydrometric.utc("2026-10-31T00:05:00+05:30") == np.datetime64("2026-10-30T18:35:00")
    assert hydrometric.utc("2026-10-31T00:05:00Z") == np.datetime64("2026-10-31T00:05:00")


def test_a_csv_line_is_read_as_given():
    row = hydrometric.parse_line("05AA008,2026-10-31T00:00:00-07:00,1.338,,B,1,2.10,,,2")
    assert row["station_id"] == "05AA008" and row["time"] == np.datetime64("2026-10-31T07:00")
    assert row["water_level"] == 1.338 and row["discharge"] == 2.10
    assert row["water_level_symbol"] == "B" and row["water_level_qa_qc"] == 1
    assert row["discharge_qa_qc"] == 2
    empty = hydrometric.parse_line("05AH056,2026-10-02T00:00:00-07:00,1185.647,,,2,,,,")
    assert np.isnan(empty["discharge"]) and empty["discharge_qa_qc"] == -1
    assert hydrometric.lines(b"not,a,hydrometric,csv\n1,2,3,4\n") is None
    # QA/QC codes go past a byte (500 occurs).
    coded = hydrometric.parse_line("05AH056,2026-10-02T00:00:00-07:00,1185.647,,,500,,,,")
    coded.update(arrival_time=np.datetime64("2026-11-01T00:00:00"), observation_identity=1)
    assert int(hydrometric.to_dataset([coded])["water_level_qa_qc"][0]) == 500


def test_a_window_writes_each_group_with_copies_collapsed(local_config):
    _, summary = run(local_config, window_objects())
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    assert summary["rows_written"] > 0
    # 24 lines of one station (sent twice) and 12 of another.
    assert summary["hydrometric"]["rows_written"] == 36
    assert summary["radar"]["duplicate_copies"] == 1
    assert summary["radar"]["volumes_written"] == {"radar/bewid_dbzh": 2, "radar/bewid_vrad": 1}

    hydro = xr.open_zarr(
        repo.readonly_session("main").store, group="hydrometric", consolidated=False
    )
    assert sorted(set(hydro["station_id"].values)) == ["05AA004", "05AA008"]
    first = hydro.isel(observation=0)
    assert first["time"].values == np.datetime64("2026-10-31T07:00:00")
    assert float(first["water_level"]) == 0.245 and float(first["discharge"]) == 0.315
    # The earliest copy's arrival is kept.
    assert hydro["arrival_time"].values.min() == np.datetime64("2026-11-01T03:00:00")

    dbzh = xr.open_zarr(
        repo.readonly_session("main").store, group="radar/bewid_dbzh", consolidated=False
    )
    assert dict(dbzh["counts"].sizes) == {"volume": 2, "sweep": 3, "azimuth": 4, "range": 8}
    raw = zarr.open_group(repo.readonly_session("main").store, path="radar/bewid_dbzh", mode="r")
    expected = h5py.File(io.BytesIO(odim_volume("000000")), "r")["dataset2/data1/data"][()]
    np.testing.assert_array_equal(raw["counts"][0, 1, :, : expected.shape[1]], expected)
    # The smaller first volume is padded with nodata, its extent recorded.
    assert (raw["counts"][0, 2] == 255).all() and (raw["counts"][0, 0, :, 6:] == 255).all()
    assert raw["nbins"][0].tolist() == [6, 5, -1] and raw["nbins"][1].tolist() == [8, 7, 6]
    assert list(dbzh["time"].values) == [
        np.datetime64("2026-11-01T00:00:00", "ns"),
        np.datetime64("2026-11-01T00:05:00", "ns"),
    ]
    assert dbzh["sweep_start_time"].values[0, 1] == np.datetime64("2026-11-01T00:00:10", "ns")
    assert float(dbzh["gain"][0, 0]) == 0.5 and float(dbzh["elevation"][1, 2]) == 2.3
    assert dbzh["azimuth_start"].values[0, 0].tolist() == [0.0, 90.0, 180.0, 270.0]
    assert str(dbzh["centre"].values[0]) == "be-rmib"


def test_a_second_run_over_the_same_window_writes_nothing(local_config):
    run(local_config, window_objects())
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    before = sizes(repo)
    snapshot = next(iter(repo.ancestry(branch="main"))).id

    again = land.LandTarget(local_config, workers=1)
    again.prepare(PARTITION)
    assert not any(again.wants(key, len(body)) for key, body, _ in window_objects())
    feed_all(again, window_objects())
    summary = again.finish(None, None, PARTITION)
    assert summary["rows_written"] == 0 and "hydrometric" not in summary
    assert sizes(repo) == before
    assert next(iter(repo.ancestry(branch="main"))).id == snapshot


def test_a_window_already_written_for_the_reports_still_gets_the_new_groups(local_config):
    objects = window_objects()
    # The window as the archiver wrote it before the new groups existed.
    run(local_config, [o for o in objects if land.LandTarget.part(o[0]) == "reports"])
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    reports = sizes(repo)["root"]
    assert reports > 0 and sizes(repo)["hydrometric"] == 0
    assert PARTITION in ledger(repo)

    target, summary = run(local_config, objects)
    assert "reports" in target._written
    assert summary["objects"] == 0  # no report object was kept, so none was decoded
    after = sizes(repo)
    assert after["root"] == reports
    assert after["hydrometric"] == 36 and after["radar/bewid_dbzh"] == 2
    assert PARTITION in ledger(repo, group="hydrometric")
    assert PARTITION in ledger(repo, group="radar")
    assert PARTITION in ledger(repo, group="radar/bewid_vrad")


def test_a_later_window_adds_only_new_and_revised_lines(local_config):
    run(local_config, window_objects())
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    text = (HYDRO / "AB_05AA004_hourly_hydrometric.csv").read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    # The next publication: the same lines, one revised (discharge filled in differently)
    # and one new time.
    revised = lines[5].rsplit(",", 4)[0] + ",0.999,,,2"
    new = "05AA004,2026-10-31T02:00:00-07:00,0.250,,,1,0.330,,,1"
    body = "﻿" + "\n".join([*lines[:5], revised, *lines[6:], new]) + "\n"
    later = [
        (
            HYDRO_KEY.format("AB_05AA004_hourly_hydrometric-1793900000.0.csv"),
            body.encode(),
            ARRIVED + dt.timedelta(hours=12),
        )
    ]
    _, summary = run(local_config, later, "wis2_20261102T0000Z")
    assert summary["hydrometric"]["rows_written"] == 2
    hydro = xr.open_zarr(
        repo.readonly_session("main").store, group="hydrometric", consolidated=False
    ).compute()
    at = hydro.where(
        (hydro["station_id"] == "05AA004")
        & (hydro["time"] == hydrometric.utc(lines[5].split(",")[1])),
        drop=True,
    )
    assert sorted(at["discharge"].values.tolist()) == [0.315, 0.999]


def test_a_run_cut_short_before_its_ledger_is_finished_without_duplicates(local_config):
    run(local_config, window_objects())
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    before = sizes(repo)
    # As if the process died after the appends but before the ledgers: the window is not
    # listed, so its objects come again, and the stored identities stop them.
    session = repo.writable_session("main")
    for group in ("hydrometric", "radar", "radar/bewid_dbzh", "radar/bewid_vrad"):
        zarr.open_group(session.store, path=group, mode="a").attrs["partitions_written"] = []
    session.commit("forget the window")
    _, summary = run(local_config, window_objects())
    assert summary["hydrometric"]["rows_written"] == 0
    assert summary["radar"]["volumes_written"] == {"radar/bewid_dbzh": 0, "radar/bewid_vrad": 0}
    assert sizes(repo) == before
    assert PARTITION in ledger(repo, group="hydrometric")
    assert PARTITION in ledger(repo, group="radar")


def test_spooled_objects_are_removed_after_the_window(local_config):
    target, _ = run(local_config, window_objects())
    spool = pathlib.Path(local_config.data_dir) / "wis2_land_spool"
    assert not any(p.is_file() for p in spool.rglob("*"))
    assert target._spools == {}


def test_objects_that_are_not_odim_or_csv_are_counted_not_fatal(local_config):
    objects = [
        (HYDRO_KEY.format("AB_05AA004_hourly_hydrometric.csv"), b"garbage", ARRIVED),
        (RADAR_KEY.format("T_PAGW41_C_EBUM_20261101000018.hdf"), b"not hdf5", ARRIVED),
    ]
    _, summary = run(local_config, objects)
    assert summary["hydrometric"]["unrecognised_objects"] == 1
    assert summary["radar"]["not_odim_polar_volumes"] == 1


def test_later_volumes_with_fewer_or_more_sweeps_and_bins_are_padded(local_config):
    def volume(time, sweeps, bins, partition):
        key = RADAR_KEY.format(f"T_PAGW41_C_EBUM_20261101{time}18.hdf")
        run(local_config, [(key, odim_volume(time, sweeps=sweeps, bins=bins), ARRIVED)], partition)

    volume("000000", 3, 8, "wis2_20261101T1200Z")
    volume("000500", 2, 5, "wis2_20261102T0000Z")
    volume("001000", 4, 10, "wis2_20261102T1200Z")
    repo = local_config.icechunk_repo(land.STORE_PREFIX)
    raw = zarr.open_group(repo.readonly_session("main").store, path="radar/bewid_dbzh", mode="r")
    assert raw["counts"].shape == (3, 4, 4, 10)
    assert raw["nbins"][:].tolist() == [[8, 7, 6, -1], [5, 4, -1, -1], [10, 9, 8, 7]]
    assert (raw["counts"][1, 2:] == 255).all() and (raw["counts"][0, :, :, 8:] == 255).all()
    ds = xr.open_zarr(
        repo.readonly_session("main").store, group="radar/bewid_dbzh", consolidated=False
    )
    assert np.isnat(ds["sweep_start_time"].values[1, 2])
    assert np.isnan(ds["elevation"].values[0, 3])


def test_a_spool_left_by_a_dead_process_is_removed(local_config, tmp_path):
    root = tmp_path / "spool"
    orphan = root / "hydrometric" / "999999999-abc"
    orphan.mkdir(parents=True)
    (orphan / "00000001").write_bytes(b"x")
    spool = hydrometric.Spool(root, "hydrometric")
    spool.add("key", b"body", ARRIVED)
    assert not orphan.exists() and len(spool.items) == 1
    spool.clear()
