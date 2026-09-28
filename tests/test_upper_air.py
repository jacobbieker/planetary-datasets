"""Offline tests for the upper-air and aircraft observation providers.

Nothing here touches the network or the MET toolkit: the pb2nc shell-out is exercised
through a stub executable, the Sondehub archive through JSON written to tmp_path, and the
store round-trips through a local icechunk directory.
"""

from __future__ import annotations

import json
import pathlib
import stat

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.observations import igra as igra_module
from planetary_datasets.providers.observations import sondehub as sondehub_module
from planetary_datasets.providers.observations.amdar import (
    AMDAR_SCHEMA,
    AMDARProvider,
    MissingToolError,
    convert_many,
    find_pb2nc,
    pb2nc_available,
    pb2nc_to_table,
    rename_by_long_name,
    run_pb2nc,
    slugify,
    write_pb2nc_config,
)
from planetary_datasets.providers.observations.igra import (
    IGRA_CDS_SCHEMA,
    IGRACDSProvider,
    IGRAStationArchive,
    cds_to_table,
    igra_cds_request,
    parse_station_list,
    read_station,
    select_stations,
)
from planetary_datasets.providers.observations.sondehub import (
    SONDEHUB_SCHEMA,
    SondehubProvider,
    flight_summary,
    frames_to_table,
    read_frames,
)
from planetary_datasets.providers.observations.upper_air_common import (
    MISSING_STRING,
    PointObservationProvider,
    concat_tables,
    table_to_dataset,
)

# --- table_to_dataset -------------------------------------------------------------------


def test_table_to_dataset_fills_the_schema_and_sorts():
    table = pd.DataFrame(
        {
            "time": ["2026-01-01T06:00:00Z", "2026-01-01T03:00:00Z"],
            "observation": ["1.5", "2.5"],
            "station_id": ["AAA", None],
        }
    )
    schema = {"observation": "float32", "station_id": "str", "pressure": "float32"}
    ds = table_to_dataset(table, schema)

    assert list(ds.data_vars) == ["observation", "station_id", "pressure"]
    assert pd.DatetimeIndex(ds["time"].values).tolist() == pd.DatetimeIndex(
        ["2026-01-01T03:00", "2026-01-01T06:00"]
    ).tolist()
    # Sorted by time, so the 03:00 row comes first.
    assert ds["observation"].values.tolist() == [2.5, 1.5]
    assert ds["station_id"].values.tolist() == [MISSING_STRING, "AAA"]
    # A schema field absent from the table becomes all-missing, not an error.
    assert np.isnan(ds["pressure"].values).all()


def test_table_to_dataset_time_is_naive_utc():
    ds = table_to_dataset(
        pd.DataFrame({"time": ["2026-01-01T12:00:00+02:00"]}), {"x": "float32"}
    )
    assert ds["time"].dtype == np.dtype("datetime64[ns]")
    assert pd.Timestamp(ds["time"].values[0]) == pd.Timestamp("2026-01-01T10:00")


def test_table_to_dataset_drops_unparseable_times():
    table = pd.DataFrame({"time": ["not a time", "2026-01-01T00:00:00Z"], "x": [1.0, 2.0]})
    ds = table_to_dataset(table, {"x": "float32"})
    assert ds.sizes["time"] == 1
    assert ds["x"].values.tolist() == [2.0]


def test_table_to_dataset_rejects_an_unsupported_dtype():
    with pytest.raises(ValueError, match="unsupported dtypes"):
        table_to_dataset(pd.DataFrame({"time": []}), {"x": "int8"})


def test_table_to_dataset_requires_a_time_column():
    with pytest.raises(ValueError, match="no 'time' column"):
        table_to_dataset(pd.DataFrame({"x": [1]}), {"x": "float32"})


def test_concat_tables_ignores_empties():
    assert concat_tables([]).empty
    assert len(concat_tables([pd.DataFrame(), pd.DataFrame({"a": [1]})])) == 1
    joined = concat_tables([pd.DataFrame({"a": [1]}), pd.DataFrame({"b": [2]})])
    assert sorted(joined.columns) == ["a", "b"]
    assert len(joined) == 2


# --- PointObservationProvider -------------------------------------------------------------


class StubPointProvider(PointObservationProvider):
    """A point provider whose observations are invented, for exercising the base logic."""

    name = "stub_points"
    store_prefix = "test/stub_points.icechunk"
    partition_freq = "1D"

    def __init__(self, config=None, offsets=("1h", "23h"), extra_offsets=()):
        super().__init__(config=config)
        self.offsets = list(offsets)
        self.extra_offsets = list(extra_offsets)

    def fetch(self, it, temp_dir=None, **kwargs):
        return ["stub"]

    def process(self, input_files, it, temp_dir=None, **kwargs):
        times = [pd.Timestamp(it) + pd.Timedelta(o) for o in self.offsets + self.extra_offsets]
        table = pd.DataFrame({"time": times, "value": np.arange(len(times), dtype="float64")})
        return self.trim_to_window(table_to_dataset(table, {"value": "float32"}), it)


@pytest.fixture
def stub_provider(local_config):
    return StubPointProvider(config=local_config)


def test_partition_window_is_half_open(stub_provider):
    start, end = stub_provider.partition_window(pd.Timestamp("2026-03-04"))
    assert start == pd.Timestamp("2026-03-04")
    assert end == pd.Timestamp("2026-03-05")


def test_monthly_partition_window():
    class Monthly(StubPointProvider):
        partition_freq = "MS"

    start, end = Monthly().partition_window(pd.Timestamp("2026-01-01"))
    assert end == pd.Timestamp("2026-02-01")


def test_trim_to_window_drops_the_overspill(stub_provider):
    provider = StubPointProvider(config=stub_provider.config, extra_offsets=("25h",))
    ds = provider.process(["stub"], pd.Timestamp("2026-03-04"))
    assert ds.sizes["time"] == 2
    assert pd.Timestamp(ds["time"].values[-1]) == pd.Timestamp("2026-03-04T23:00")


def test_partition_is_missing_until_an_observation_lands_in_it(stub_provider):
    day = pd.Timestamp("2026-03-04")
    assert stub_provider.missing_timesteps([day]) == [day]

    assert stub_provider.run_partition(day) is True

    # The partition timestamp itself is never a stored value; the window is what counts.
    assert stub_provider.missing_timesteps([day]) == []
    assert stub_provider.missing_timesteps([day + pd.Timedelta("1D")]) == [day + pd.Timedelta("1D")]


def test_round_trip_through_a_local_store(stub_provider):
    stub_provider.run_partition(pd.Timestamp("2026-03-04"))
    stub_provider.run_partition(pd.Timestamp("2026-03-05"))

    repo = stub_provider.get_icechunk_repo()
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert stored.sizes["time"] == 4
    assert pd.Timestamp(stored["time"].values[0]) == pd.Timestamp("2026-03-04T01:00")
    assert pd.Timestamp(stored["time"].values[-1]) == pd.Timestamp("2026-03-05T23:00")


def test_sub_second_times_survive_a_store_created_from_whole_seconds(stub_provider):
    """The store's time units are fixed by the first write; later partitions must fit.

    Without flooring, a partition of microsecond timestamps appended to a store created
    at second resolution reads back a thousandfold out.
    """
    whole = StubPointProvider(config=stub_provider.config, offsets=("1h",))
    assert whole.run_partition(pd.Timestamp("2026-03-04")) is True

    fine = StubPointProvider(config=stub_provider.config, offsets=("1h 0.1s",))
    assert fine.run_partition(pd.Timestamp("2026-03-05")) is True

    stored = xr.open_zarr(
        stub_provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert pd.DatetimeIndex(stored["time"].values).tolist() == [
        pd.Timestamp("2026-03-04T01:00"),
        pd.Timestamp("2026-03-05T01:00"),
    ]


def test_an_empty_partition_is_not_written(stub_provider):
    empty = StubPointProvider(config=stub_provider.config, offsets=())
    assert empty.run_partition(pd.Timestamp("2026-03-04")) is False


# --- AMDAR ------------------------------------------------------------------------------


def test_slugify_handles_descriptions_and_units():
    assert slugify("Air Temperature (K)") == "air_temperature_k"
    assert slugify("m/s", unit=True) == "m_per_s"
    assert slugify("m/s") == "m/s"


def test_rename_by_long_name_keeps_the_first_winner_on_a_collision():
    ds = xr.Dataset(
        {
            "a": ("x", [1.0], {"long_name": "Air Temperature"}),
            "b": ("x", [2.0], {"long_name": "Air Temperature"}),
            "c": ("x", [3.0]),
        }
    )
    renamed = rename_by_long_name(ds)
    assert "air_temperature" in renamed.data_vars
    # The second variable with the same long_name keeps its own name rather than raising.
    assert "b" in renamed.data_vars
    assert "c" in renamed.data_vars


def test_find_pb2nc_names_the_missing_toolkit(monkeypatch):
    monkeypatch.setenv("PB2NC_BINARY", "definitely-not-installed-pb2nc")
    with pytest.raises(MissingToolError) as excinfo:
        find_pb2nc()
    message = str(excinfo.value)
    assert "pb2nc" in message
    assert "MET" in message
    assert not pb2nc_available()


def _stub_pb2nc(tmp_path: pathlib.Path, body: str) -> pathlib.Path:
    """Write an executable stand-in for pb2nc so the shell-out can be tested offline."""
    script = tmp_path / "pb2nc"
    script.write_text("#!/bin/sh\n" + body)
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def test_run_pb2nc_raises_with_the_tools_own_error(tmp_path, monkeypatch):
    script = _stub_pb2nc(tmp_path, 'echo "bad bufr record" >&2\nexit 3\n')
    monkeypatch.setenv("PB2NC_BINARY", str(script))
    with pytest.raises(RuntimeError, match="bad bufr record"):
        run_pb2nc(tmp_path / "in.bufr", tmp_path / "out.nc", tmp_path / "cfg")
    assert not (tmp_path / "out.nc").exists()
    assert not (tmp_path / "out.nc.part").exists()


def test_run_pb2nc_rejects_a_success_that_wrote_nothing(tmp_path, monkeypatch):
    script = _stub_pb2nc(tmp_path, "exit 0\n")
    monkeypatch.setenv("PB2NC_BINARY", str(script))
    with pytest.raises(RuntimeError, match="wrote no output"):
        run_pb2nc(tmp_path / "in.bufr", tmp_path / "out.nc", tmp_path / "cfg")


def test_run_pb2nc_renames_only_a_complete_output(tmp_path, monkeypatch):
    script = _stub_pb2nc(tmp_path, 'printf data > "$2"\nexit 0\n')
    monkeypatch.setenv("PB2NC_BINARY", str(script))
    out = run_pb2nc(tmp_path / "in.bufr", tmp_path / "out.nc", tmp_path / "cfg")
    assert out.read_text() == "data"
    assert not (tmp_path / "out.nc.part").exists()


def test_convert_many_skips_a_failing_file(tmp_path, monkeypatch):
    script = _stub_pb2nc(
        tmp_path,
        'case "$1" in *bad*) exit 1;; esac\nprintf data > "$2"\nexit 0\n',
    )
    monkeypatch.setenv("PB2NC_BINARY", str(script))
    for name in ("good.bufr", "bad.bufr"):
        (tmp_path / name).write_text("x")
    outputs = convert_many(
        [tmp_path / "good.bufr", tmp_path / "bad.bufr"], tmp_path / "nc", tmp_path / "cfg"
    )
    assert [p.name for p in outputs] == ["good.nc"]


def test_write_pb2nc_config_has_no_absolute_paths_baked_in(tmp_path):
    path = write_pb2nc_config(tmp_path / "cfg.txt", tmp_dir=tmp_path / "scratch")
    text = path.read_text()
    assert f'tmp_dir = "{tmp_path / "scratch"}"' in text
    assert "AIRCAR" in text
    # The mask polygon the original config referenced never existed in the repository.
    assert "mask.poly" not in text


def _met_point_dataset() -> xr.Dataset:
    """A miniature MET point-observation file: two headers, three observations."""
    return xr.Dataset(
        {
            "hdr_typ": ("nhdr", np.array([0, 0]), {"long_name": "index of message type"}),
            "hdr_sid": (
                "nhdr",
                np.array([0, 1]),
                {"long_name": "index of station identification"},
            ),
            "hdr_vld": ("nhdr", np.array([0, 1]), {"long_name": "index of valid time"}),
            "hdr_lat": ("nhdr", np.array([69.1, 10.0]), {"long_name": "latitude"}),
            "hdr_lon": ("nhdr", np.array([15.8, -20.0]), {"long_name": "longitude"}),
            "hdr_elv": ("nhdr", np.array([10.0, 3000.0]), {"long_name": "elevation"}),
            "hdr_typ_table": (
                "nhdr_typ",
                np.array([b"AIRCFT"]),
                {"long_name": "message type"},
            ),
            "hdr_sid_table": (
                "nhdr_sid",
                np.array([b"ABC123", b"DEF456"]),
                {"long_name": "station identification"},
            ),
            "hdr_vld_table": (
                "nhdr_vld",
                np.array([b"20260304_010000", b"20260304_230000"]),
                {"long_name": "valid time"},
            ),
            "obs_hid": (
                "nobs",
                np.array([0, 1, 1]),
                {"long_name": "index of matching header data"},
            ),
            "obs_vid": (
                "nobs",
                np.array([0, 0, 1]),
                {
                    "long_name": (
                        "index of BUFR variable corresponding to the observation type"
                    )
                },
            ),
            "obs_lvl": (
                "nobs",
                np.array([850.0, 500.0, 500.0]),
                {"long_name": "pressure level (hPa) or accumulation interval (sec)"},
            ),
            "obs_hgt": (
                "nobs",
                np.array([1500.0, 5000.0, 5000.0]),
                {"long_name": "height in meters above sea level (MSL)"},
            ),
            "obs_val": (
                "nobs",
                np.array([273.0, 250.0, 7.0]),
                {"long_name": "observation value"},
            ),
            "obs_qty": ("nobs", np.array([2.0, 2.0, 2.0]), {"long_name": "quality flag"}),
            "obs_var": (
                "nobs_var",
                np.array([b"TMP", b"NUM"]),
                {"long_name": "variable names"},
            ),
            "obs_unit": (
                "nobs_var",
                np.array([b"K", b"NUMERIC"]),
                {"long_name": "variable units"},
            ),
            "obs_desc": (
                "nobs_var",
                np.array([b"Temperature", b"Report Count"]),
                {"long_name": "variable descriptions"},
            ),
        }
    )


def test_pb2nc_to_table_joins_each_observation_to_its_own_header():
    table = pb2nc_to_table(_met_point_dataset())

    # The code-table observation (__numeric) is dropped, leaving two.
    assert len(table) == 2
    assert table["observation"].tolist() == [273.0, 250.0]
    # The bug in the original: every observation took header 0's time and position.
    assert table["station_id"].tolist() == ["ABC123", "DEF456"]
    assert table["time"].tolist() == [
        pd.Timestamp("2026-03-04T01:00"),
        pd.Timestamp("2026-03-04T23:00"),
    ]
    assert table["latitude"].tolist() == [69.1, 10.0]
    assert table["observation_type"].unique().tolist() == ["temperature__k"]
    assert table["message_type"].tolist() == ["AIRCFT", "AIRCFT"]


def test_pb2nc_to_table_reads_the_met_12_quality_lookup_table():
    """MET 12 stores the quality mark as an index into a table of distinct marks."""
    ds = _met_point_dataset()
    ds = ds.drop_vars("obs_qty").assign(
        obs_qty=("nobs", np.array([0, 1, 0]), {"long_name": "index of quality flag"}),
        obs_qty_table=("nobs_qty", np.array([b"2", b"9"]), {"long_name": "quality flag"}),
    )
    table = pb2nc_to_table(ds)
    # Two observations survive the code-table filter; the third was the numeric one.
    assert table["quality_flag"].tolist() == [2.0, 9.0]


def test_pb2nc_to_table_drops_a_quality_column_of_the_wrong_length():
    ds = _met_point_dataset()
    ds = ds.drop_vars("obs_qty").assign(
        obs_qty=("nobs_var", np.array([2.0, 9.0]), {"long_name": "quality flag"})
    )
    table = pb2nc_to_table(ds)
    assert "quality_flag" not in table.columns


def test_pb2nc_to_table_rejects_a_file_that_is_not_met_point_output():
    with pytest.raises(ValueError, match="not a MET point-observation file"):
        pb2nc_to_table(xr.Dataset({"temperature": ("x", [1.0])}))


def test_amdar_fetch_raises_when_the_directory_is_missing(local_config, tmp_path):
    provider = AMDARProvider(config=local_config, bufr_dir=tmp_path / "nope")
    with pytest.raises(FileNotFoundError, match="AMDAR_BUFR_DIR"):
        provider.fetch(pd.Timestamp("2026-03-04"))


def test_amdar_fetch_matches_on_the_partition_date(local_config, tmp_path):
    raw = tmp_path / "amdar"
    raw.mkdir()
    for name in ("gdas.20260304.t00z.48h", "gdas.20260305.t00z.48h"):
        (raw / name).write_text("x")
    provider = AMDARProvider(config=local_config, bufr_dir=raw)
    found = provider.fetch(pd.Timestamp("2026-03-04"))
    assert [pathlib.Path(p).name for p in found] == ["gdas.20260304.t00z.48h"]
    assert provider.fetch(pd.Timestamp("2026-03-06")) == []


def test_amdar_process_converts_trims_and_writes(local_config, tmp_path, monkeypatch):
    """The whole AMDAR leg with pb2nc stubbed out by a script that writes a MET file."""
    raw = tmp_path / "amdar"
    raw.mkdir()
    (raw / "gdas.20260304.t00z.48h").write_text("x")

    sample = tmp_path / "sample.nc"
    _met_point_dataset().to_netcdf(sample)
    script = _stub_pb2nc(tmp_path, f'cp "{sample}" "$2"\nexit 0\n')
    monkeypatch.setenv("PB2NC_BINARY", str(script))

    provider = AMDARProvider(config=local_config, bufr_dir=raw)
    assert provider.run_partition(pd.Timestamp("2026-03-04")) is True

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert stored.sizes["time"] == 2
    assert set(stored.data_vars) == set(AMDAR_SCHEMA)
    assert stored["station_id"].values.tolist() == ["ABC123", "DEF456"]


def test_amdar_bbox_crops(local_config, tmp_path, monkeypatch):
    raw = tmp_path / "amdar"
    raw.mkdir()
    (raw / "gdas.20260304.t00z.48h").write_text("x")
    sample = tmp_path / "sample.nc"
    _met_point_dataset().to_netcdf(sample)
    monkeypatch.setenv("PB2NC_BINARY", str(_stub_pb2nc(tmp_path, f'cp "{sample}" "$2"\nexit 0\n')))

    provider = AMDARProvider(config=local_config, bufr_dir=raw, bbox=(0.0, 60.0, 30.0, 80.0))
    raw_file = str(raw / "gdas.20260304.t00z.48h")
    ds = provider.process([raw_file], pd.Timestamp("2026-03-04"), tmp_path)
    assert ds.sizes["time"] == 1
    assert ds["station_id"].values.tolist() == ["ABC123"]


def test_amdar_bufr_dir_comes_from_the_environment(local_config, tmp_path, monkeypatch):
    monkeypatch.setenv("AMDAR_BUFR_DIR", str(tmp_path / "from-env"))
    assert AMDARProvider(config=local_config).bufr_dir == tmp_path / "from-env"


def test_amdar_pb2nc_config_env_must_exist(local_config, tmp_path, monkeypatch):
    monkeypatch.setenv("AMDAR_PB2NC_CONFIG", str(tmp_path / "absent.cfg"))
    with pytest.raises(FileNotFoundError, match="AMDAR_PB2NC_CONFIG"):
        AMDARProvider(config=local_config).pb2nc_config_path(tmp_path)


# --- IGRA -------------------------------------------------------------------------------


def test_igra_cds_request_shape():
    request = igra_cds_request(2020, [1])
    assert request["year"] == ["2020"]
    assert request["month"] == ["01"]
    assert len(request["day"]) == 31
    assert request["data_format"] == "netcdf"
    assert "air_temperature" in request["variable"]


def test_cds_to_table_renames_and_decodes():
    ds = xr.Dataset(
        {
            "observation_value": ("index", np.array([273.0, 274.0])),
            "observed_variable": ("index", np.array([b"air_temperature", b"air_temperature"])),
            "z_coordinate": ("index", np.array([85000.0, 70000.0])),
            "report_timestamp": (
                "index",
                pd.to_datetime(["2020-01-01T00:00", "2020-01-01T12:00"]).values,
            ),
            "primary_station_id": ("index", np.array([b"AGM00060355", b"AGM00060355"])),
        },
        coords={"index": [0, 1]},
    )
    table = cds_to_table(ds)
    assert "time" in table.columns
    assert table["observation"].tolist() == [273.0, 274.0]
    assert table["station_id"].tolist() == ["AGM00060355", "AGM00060355"]
    assert table["observation_type"].tolist() == ["air_temperature", "air_temperature"]


def test_cds_to_table_says_what_it_looked_for():
    ds = xr.Dataset({"observation_value": ("index", [1.0])}, coords={"index": [0]})
    with pytest.raises(ValueError, match="no recognised time column"):
        cds_to_table(ds)


def test_igra_cds_provider_processes_a_local_file(local_config, tmp_path):
    ds = xr.Dataset(
        {
            "observation_value": ("index", np.array([273.0, 274.0, 275.0])),
            "observed_variable": ("index", np.array([b"air_temperature"] * 3)),
            "z_coordinate": ("index", np.array([85000.0, 70000.0, 50000.0])),
            "report_timestamp": (
                "index",
                pd.to_datetime(
                    ["2020-01-01T00:00", "2020-01-15T12:00", "2020-02-03T00:00"]
                ).values,
            ),
        },
        coords={"index": [0, 1, 2]},
    )
    path = tmp_path / "igra_202001.nc"
    ds.to_netcdf(path)

    provider = IGRACDSProvider(config=local_config, raw_dir=tmp_path)
    processed = provider.process([str(path)], pd.Timestamp("2020-01-01"))
    # The February row is outside the monthly window and is trimmed.
    assert processed.sizes["time"] == 2
    assert set(processed.data_vars) == set(IGRA_CDS_SCHEMA)


def test_cds_to_table_survives_two_sources_for_one_target():
    """A real CDS file carries both station columns; renaming both would collide."""
    ds = xr.Dataset(
        {
            "observation_value": ("index", np.array([273.0])),
            "observed_variable": ("index", np.array([b"air_temperature"])),
            "primary_station_id": ("index", np.array([b"AGM00060355"])),
            "station_name": ("index", np.array([b"ANNABA"])),
            "z_coordinate": ("index", np.array([85000.0])),
            "air_pressure": ("index", np.array([85000.0])),
            "report_timestamp": ("index", pd.to_datetime(["2020-01-01T00:00"]).values),
        },
        coords={"index": [0]},
    )
    table = cds_to_table(ds)
    assert list(table.columns).count("station_id") == 1
    assert list(table.columns).count("pressure") == 1
    # The identifier wins over the human-readable name.
    assert table["station_id"].tolist() == ["AGM00060355"]


def test_igra_cds_provider_reuses_the_downloaded_file(local_config, tmp_path, monkeypatch):
    path = tmp_path / "igra_202001.nc"
    xr.Dataset({"x": ("index", [1.0])}, coords={"index": [0]}).to_netcdf(path)

    def _explode(*args, **kwargs):
        raise AssertionError("the CDS should not be contacted when the file is present")

    monkeypatch.setattr("planetary_datasets.providers.observations.igra.cds_client", _explode)
    provider = IGRACDSProvider(config=local_config, raw_dir=tmp_path)
    assert provider.fetch(pd.Timestamp("2020-01-01")) == [str(path)]


def test_select_stations_filters_on_recency_reports_and_position():
    stations = pd.DataFrame(
        {
            "end": [2026, 2010, 2026, 2026],
            "total": [500, 500, 10, 500],
            "lat": [1.0, 2.0, 3.0, np.nan],
            "lon": [1.0, 2.0, 3.0, 4.0],
        },
        index=["keep", "too_old", "too_few", "no_position"],
    )
    assert list(select_stations(stations).index) == ["keep"]


def _station_line(ident, lat, lon, elev, state, name, start, end, total):
    """One line in NCEI's fixed-width station-list layout."""
    line = (
        f"{ident:<11} {lat:>8} {lon:>9} {elev:>6} {state:<2} {name:<30} "
        f"{start:>4} {end:>4} {total:>6}"
    )
    assert len(line) == 88, len(line)
    return line


# Two real stations plus one with the blank position fields that make the igra package's
# own reader raise part-way through the file.
STATION_LIST_SAMPLE = "\n".join(
    [
        _station_line(
            "ACM00078861", "17.1170", "-61.7830", "10.0", "", "COOLIDGE FIELD", 1947, 1993, 13896
        ),
        _station_line(
            "AEM00041217", "24.4333", "54.6500", "16.0", "", "ABU DHABI INTL", 1983, 2026, 41271
        ),
        _station_line("ZZXXXXXXXXX", "", "", "-999.9", "", "NO POSITION", 1900, 1901, 1),
    ]
) + "\n"


def test_parse_station_list_handles_blank_and_sentinel_fields(tmp_path):
    path = tmp_path / "igra2-station-list.txt"
    path.write_text(STATION_LIST_SAMPLE)
    stations = parse_station_list(path)

    assert list(stations.index) == ["ACM00078861", "AEM00041217", "ZZXXXXXXXXX"]
    assert stations.loc["AEM00041217", "lat"] == pytest.approx(24.4333)
    assert stations.loc["AEM00041217", "total"] == 41271
    assert np.isnan(stations.loc["ZZXXXXXXXXX", "lat"])
    # -999.9 is the elevation sentinel, not a real depth.
    assert np.isnan(stations.loc["ZZXXXXXXXXX", "alt"])
    assert list(select_stations(stations, min_end_year=2020, min_reports=100).index) == [
        "AEM00041217"
    ]


def test_every_ncei_missing_value_sentinel_becomes_nan(tmp_path):
    """Regression: the sentinel tuple listed -98.8888 twice and missed two others.

    NCEI's longitude sentinel (-998.8888) and its second elevation sentinel (-998.8) were
    absent, so a station with an unknown longitude was placed at -998.8888 degrees east
    rather than dropped.
    """
    path = tmp_path / "igra2-station-list.txt"
    path.write_text(
        "\n".join(
            [
                _station_line(
                    "NOLONXXXXXX", "51.5000", "-998.8888", "10.0", "", "NO LONGITUDE", 1950, 2020, 5
                ),
                _station_line(
                    "NOLATXXXXXX", "-98.8888", "0.0000", "10.0", "", "NO LATITUDE", 1950, 2020, 5
                ),
                _station_line(
                    "NOALTXXXXXX", "51.5000", "0.0000", "-998.8", "", "NO ALTITUDE", 1950, 2020, 5
                ),
            ]
        )
        + "\n"
    )

    stations = parse_station_list(path)

    assert np.isnan(stations.loc["NOLONXXXXXX", "lon"])
    assert np.isnan(stations.loc["NOLATXXXXXX", "lat"])
    assert np.isnan(stations.loc["NOALTXXXXXX", "alt"])
    # The fields that are present are untouched.
    assert stations.loc["NOLONXXXXXX", "lat"] == pytest.approx(51.5)


def _fake_station_table():
    """Two soundings on two standard levels, in the shape ascii_to_dataframe returns."""
    dates = pd.to_datetime(["2020-01-01T00:00", "2020-01-01T00:00", "2020-01-02T12:00"])
    frame = pd.DataFrame(
        {
            "pres": [85000.0, 50000.0, 85000.0],
            "gph": [1500.0, 5600.0, 1490.0],
            "temp": [280.0, 250.0, 281.0],
            "rhumi": [80.0, 40.0, 75.0],
            "windd": [180.0, 250.0, 190.0],
            "winds": [5.0, 25.0, 6.0],
            "dpd": [2.0, 10.0, 3.0],
        },
        index=pd.Index(dates, name="date"),
    )
    metadata = pd.DataFrame(
        {"numlev": [2, 2, 1], "lat": [36.9] * 3, "lon": [6.9] * 3},
        index=pd.Index(dates, name="date"),
    )
    return frame, metadata


def test_read_station_builds_a_station_time_level_cube(monkeypatch):
    monkeypatch.setattr(igra_module, "station_table", lambda path: _fake_station_table())
    ds = read_station("AGM00060355", "ignored", levels=[85000.0, 50000.0, 10000.0])

    assert dict(ds.sizes) == {"station": 1, "time": 2, "level": 3}
    assert ds["station"].values.tolist() == ["AGM00060355"]
    # The level never reported is still present, so every station shares one level axis.
    assert np.isnan(ds["temperature"].sel(level=10000.0).values).all()
    assert ds["temperature"].sel(time="2020-01-01", level=85000.0).values.item() == 280.0
    assert ds["latitude"].values.ravel().tolist() == pytest.approx([36.9, 36.9])
    assert ds["temperature"].dtype == np.float32


def _staged_archive(local_config, monkeypatch, tmp_path, **kwargs):
    """An archive whose downloads and parsing are stubbed, staging into tmp_path."""
    monkeypatch.setattr(igra_module, "station_table", lambda path: _fake_station_table())
    monkeypatch.setattr(
        IGRAStationArchive,
        "download",
        lambda self, idents, overwrite=False: {i: tmp_path / i for i in idents},
    )
    return IGRAStationArchive(
        config=local_config,
        levels=[85000.0, 50000.0],
        stage_dir=tmp_path / "staged",
        **kwargs,
    )


def test_station_archive_stages_each_station_on_its_own_time_axis(
    local_config, monkeypatch, tmp_path
):
    archive = _staged_archive(local_config, monkeypatch, tmp_path)
    staged = archive.stage({"AAA": tmp_path / "AAA", "BBB": tmp_path / "BBB"})

    assert [p.name for p in staged] == ["AAA.nc", "BBB.nc"]
    with xr.open_dataset(staged[0]) as ds:
        assert dict(ds.sizes) == {"station": 1, "time": 2, "level": 2}
    # A second pass reuses what is already staged rather than re-parsing.
    assert archive.stage({"AAA": tmp_path / "AAA"}) == [staged[0]]


def test_station_archive_writes_in_time_slices_and_is_idempotent(
    local_config, monkeypatch, tmp_path
):
    archive = _staged_archive(local_config, monkeypatch, tmp_path)

    # slice_size of 1 forces more than one commit, exercising the append path.
    assert archive.build(idents=["AAA", "BBB"], slice_size=1) == 2
    # Re-running must not duplicate sounding times already in the store.
    assert archive.build(idents=["AAA", "BBB"], slice_size=1) == 0

    repo = local_config.icechunk_repo(archive.store_prefix)
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert stored["station"].values.tolist() == ["AAA", "BBB"]
    assert dict(stored.sizes) == {"station": 2, "time": 2, "level": 2}
    assert stored["level"].values.tolist() == [85000.0, 50000.0]


def test_station_archive_stages_each_level_set_separately(local_config, monkeypatch, tmp_path):
    """Two level sets must not reuse each other's staged files."""
    sixteen = _staged_archive(local_config, monkeypatch, tmp_path)
    two = IGRAStationArchive(
        config=local_config, levels=[85000.0], stage_dir=tmp_path / "staged"
    )
    assert sixteen.stage_dir != two.stage_dir


def test_station_archive_refresh_redownloads(local_config, monkeypatch, tmp_path):
    archive = _staged_archive(local_config, monkeypatch, tmp_path)
    seen = []
    monkeypatch.setattr(
        IGRAStationArchive,
        "download",
        lambda self, idents, overwrite=False: seen.append(overwrite) or {},
    )
    archive.build(idents=["AAA"], refresh=True)
    assert seen == [True]


def test_station_archive_with_nothing_to_download(local_config, monkeypatch, tmp_path):
    archive = _staged_archive(local_config, monkeypatch, tmp_path)
    monkeypatch.setattr(IGRAStationArchive, "download", lambda self, idents, overwrite=False: {})
    assert archive.build(idents=["AAA"]) == 0


# --- Sondehub ---------------------------------------------------------------------------


SAMPLE_FRAMES = [
    {
        "datetime": "2018-10-01T20:54:00.760406Z",
        "lat": "50.10301",
        "lon": "4.79712",
        "alt": "517",
        "vel_h": "10.8",
        "temp": "6.4",
        "serial": "DFM09-17027735",
        "type": "DFM",
        "launch_site": "06458",
    },
    {
        "datetime": "2018-10-01T21:47:00.112691Z",
        "lat": "50.10301",
        "lon": "4.92834",
        "alt": "20069",
        "vel_h": "8.6",
        "temp": "-56.4",
        "serial": "DFM09-17027735",
        "type": "DFM",
    },
    {"lat": "1.0", "alt": "1"},  # no datetime: unusable, and must not raise
]


def test_frames_to_table_maps_fields_and_drops_undated_frames():
    table = frames_to_table(SAMPLE_FRAMES)
    assert len(table) == 2
    assert set(["time", "latitude", "longitude", "altitude", "horizontal_velocity"]) <= set(
        table.columns
    )
    assert table["serial"].tolist() == ["DFM09-17027735"] * 2


def test_frames_to_table_on_nothing():
    assert frames_to_table([]).empty


def test_flight_summary_reports_the_flight_envelope():
    summary = flight_summary("DFM09-17027735", SAMPLE_FRAMES)
    assert summary["serial"] == "DFM09-17027735"
    assert summary["frames"] == 2
    assert summary["max_altitude"] == 20069.0
    assert summary["start"] < summary["end"]


def test_flight_summary_without_any_altitude():
    """A receiver that never decoded altitude leaves the column out entirely."""
    frames = [{"datetime": "2018-10-01T20:54:00Z", "lat": "50.1", "lon": "4.8"}]
    summary = flight_summary("no-alt", frames)
    assert summary["frames"] == 1
    assert summary["max_altitude"] is None


def test_flight_summary_on_an_empty_flight():
    assert flight_summary("nothing", []) == {
        "serial": "nothing",
        "frames": 0,
        "start": None,
        "end": None,
        "max_altitude": None,
    }


def test_read_frames_normalises_a_single_object(tmp_path):
    path = tmp_path / "one.json"
    path.write_text(json.dumps(SAMPLE_FRAMES[0]))
    assert len(read_frames(str(path))) == 1


def test_read_frames_reads_a_list(tmp_path):
    path = tmp_path / "many.json"
    path.write_text(json.dumps(SAMPLE_FRAMES))
    assert len(read_frames(str(path))) == 3


def test_sondehub_process_builds_the_full_schema(local_config, tmp_path):
    path = tmp_path / "DFM09-17027735.json"
    path.write_text(json.dumps(SAMPLE_FRAMES))

    provider = SondehubProvider(config=local_config)
    ds = provider.process([str(path)], pd.Timestamp("2018-10-01"))
    assert ds.sizes["time"] == 2
    assert set(ds.data_vars) == set(SONDEHUB_SCHEMA)
    # Not reported by these frames, so present and all-missing rather than absent.
    assert np.isnan(ds["pressure"].values).all()
    assert ds["sonde_type"].values.tolist() == ["DFM", "DFM"]


def test_sondehub_process_trims_to_the_day(local_config, tmp_path):
    path = tmp_path / "s.json"
    path.write_text(json.dumps(SAMPLE_FRAMES))
    provider = SondehubProvider(config=local_config)
    ds = provider.process([str(path)], pd.Timestamp("2018-10-02"))
    assert ds.sizes["time"] == 0


def test_sondehub_write_round_trip(local_config, tmp_path, monkeypatch):
    path = tmp_path / "DFM09-17027735.json"
    path.write_text(json.dumps(SAMPLE_FRAMES))
    monkeypatch.setattr(
        sondehub_module, "list_day_keys", lambda *args, **kwargs: [str(path)]
    )

    provider = SondehubProvider(config=local_config)
    assert provider.run_partition(pd.Timestamp("2018-10-01")) is True
    # Re-running the same partition is a no-op, not a duplicate.
    assert provider.run_partition(pd.Timestamp("2018-10-01")) is False

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert stored.sizes["time"] == 2
    assert stored["serial"].values.tolist() == ["DFM09-17027735"] * 2


def test_sondehub_process_survives_one_unreadable_object(local_config, tmp_path):
    good = tmp_path / "good.json"
    good.write_text(json.dumps(SAMPLE_FRAMES))
    bad = tmp_path / "bad.json"
    bad.write_text("{ not json")

    provider = SondehubProvider(config=local_config)
    ds = provider.process([str(good), str(bad)], pd.Timestamp("2018-10-01"))
    assert ds.sizes["time"] == 2


def test_max_sondes_limits_the_day(local_config, monkeypatch):
    monkeypatch.setattr(
        sondehub_module, "list_day_keys", lambda *args, **kwargs: ["a", "b", "c"]
    )
    provider = SondehubProvider(config=local_config, max_sondes=2)
    assert provider.fetch(pd.Timestamp("2018-10-01")) == ["a", "b"]


# --- Dagster assets ----------------------------------------------------------------------


def test_the_asset_module_defines_the_expected_assets():
    import dagster as dg

    import dags.assets.upper_air as upper_air

    assets = [
        upper_air.amdar_observations,
        upper_air.igra_cds_raw,
        upper_air.igra_cds_observations,
        upper_air.igra_station_archive,
        upper_air.sondehub_observations,
    ]
    # Constructing Definitions is the check that dagster accepts the annotations and the
    # dependency between the two IGRA stages.
    dg.Definitions(assets=assets)

    keys = {key.to_user_string() for asset in assets for key in asset.keys}
    assert keys == {
        "amdar_observations",
        "igra_cds_raw",
        "igra_cds_observations",
        "igra_station_archive",
        "sondehub_observations",
    }


def test_the_icechunk_stage_depends_on_the_download_stage():
    import dags.assets.upper_air as upper_air

    dependencies = upper_air.igra_cds_observations.asset_deps
    depends_on = {key.to_user_string() for keys in dependencies.values() for key in keys}
    assert "igra_cds_raw" in depends_on
