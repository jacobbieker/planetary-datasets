"""Offline tests for the surface observation providers.

Nothing here touches the network: every provider is driven with monkeypatched fetch and
parse steps, or with fixture text captured from the real services.
"""

from __future__ import annotations

import gzip
import io
import json
import pathlib
import zipfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets import config as config_module
from planetary_datasets.providers.observations.base import (
    NoStationDataError,
    Station,
    StationObservationProvider,
    align_to_grid,
    frames_to_dataset,
    partition_time_index,
    safe_filename,
)


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """The shared local_config, plus a redirected data directory.

    Several of these providers cache a station roster under ``data_dir``, which the
    shared fixture leaves pointing at the checkout. Overriding it here keeps the tests
    from dropping cache files into the repository.
    """
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    config_module.reset_config_cache()
    return config_module.load_config(env_file=tmp_path / "nonexistent.env")


# --------------------------------------------------------------------------------------
# Reshape helpers
# --------------------------------------------------------------------------------------


def test_partition_time_index_is_half_open():
    times = partition_time_index(pd.Timestamp("2026-03-01"), "D", "1h")
    assert times[0] == pd.Timestamp("2026-03-01T00:00")
    assert times[-1] == pd.Timestamp("2026-03-01T23:00")
    assert len(times) == 24


def test_consecutive_partitions_do_not_overlap():
    first = partition_time_index(pd.Timestamp("2026-01-01"), "MS", "1h")
    second = partition_time_index(pd.Timestamp("2026-02-01"), "MS", "1h")
    assert first[-1] < second[0]
    assert len(first) == 31 * 24


def test_align_to_grid_exact_drops_off_grid_readings():
    index = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T00:17", "2026-01-01T01:00"])
    df = pd.DataFrame({"t": [1.0, 2.0, 3.0]}, index=index)
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    out = align_to_grid(df, times, "1h", how="exact")
    assert out["t"].iloc[0] == 1.0
    assert out["t"].iloc[1] == 3.0
    assert out["t"].iloc[2:].isna().all()


def test_align_to_grid_last_keeps_the_final_report_in_each_hour():
    index = pd.DatetimeIndex(["2026-01-01T00:20", "2026-01-01T00:50", "2026-01-01T01:10"])
    df = pd.DataFrame({"t": [1.0, 2.0, 3.0]}, index=index)
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    out = align_to_grid(df, times, "1h", how="last")
    assert out["t"].iloc[0] == 2.0
    assert out["t"].iloc[1] == 3.0


def test_align_to_grid_converts_tz_aware_input_to_naive_utc():
    index = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00"], tz="America/Denver")
    df = pd.DataFrame({"t": [1.0, 2.0]}, index=index)
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    out = align_to_grid(df, times, "1h", how="exact")
    # 00:00 MST is 07:00 UTC.
    assert out["t"].iloc[7] == 1.0
    assert out["t"].iloc[8] == 2.0


def test_align_to_grid_keeps_the_later_of_duplicate_timestamps():
    index = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T00:00"])
    df = pd.DataFrame({"t": [1.0, 5.0]}, index=index)
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    assert align_to_grid(df, times, "1h")["t"].iloc[0] == 5.0


def test_align_to_grid_rejects_an_unknown_mode():
    df = pd.DataFrame({"t": [1.0]}, index=pd.DatetimeIndex(["2026-01-01"]))
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    with pytest.raises(ValueError, match="unknown alignment mode"):
        align_to_grid(df, times, "1h", how="median")


STATIONS = [
    Station(id="AAA", latitude=1.0, longitude=2.0, elevation=3.0, name="Alpha"),
    Station(id="BBB", latitude=4.0, longitude=5.0, elevation=6.0, name="Beta"),
]


def test_frames_to_dataset_builds_a_dense_cube():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    frames = {"AAA": pd.DataFrame({"temp": np.arange(24.0)}, index=times)}
    ds = frames_to_dataset(frames, STATIONS, times, variables=["temp"])

    assert ds["temp"].dims == ("time", "station")
    assert ds.sizes == {"time": 24, "station": 2}
    assert ds["temp"].sel(station="AAA").values[3] == 3.0
    # A station that did not report still occupies its column.
    assert bool(np.isnan(ds["temp"].sel(station="BBB").values).all())


def test_frames_to_dataset_carries_station_metadata_as_coords():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = frames_to_dataset(
        {"AAA": pd.DataFrame({"temp": np.zeros(24)}, index=times)},
        STATIONS,
        times,
        variables=["temp"],
    )
    assert list(ds["latitude"].values) == [1.0, 4.0]
    assert list(ds["longitude"].values) == [2.0, 5.0]
    assert list(ds["elevation"].values) == [3.0, 6.0]
    assert list(ds["station_name"].values) == ["Alpha", "Beta"]


def test_frames_to_dataset_writes_declared_variables_even_when_absent():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = frames_to_dataset(
        {"AAA": pd.DataFrame({"temp": np.zeros(24)}, index=times)},
        STATIONS,
        times,
        variables=["temp", "pressure"],
    )
    # The variable set must not depend on what a given day happened to contain, or the
    # next append is refused for a variable mismatch.
    assert set(ds.data_vars) == {"temp", "pressure"}
    assert bool(np.isnan(ds["pressure"].values).all())


def test_frames_to_dataset_infers_variables_when_not_declared():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = frames_to_dataset(
        {"AAA": pd.DataFrame({"b": np.zeros(24), "a": np.zeros(24)}, index=times)},
        STATIONS,
        times,
    )
    assert list(ds.data_vars) == ["a", "b"]


def test_frames_to_dataset_rejects_an_unknown_station():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    with pytest.raises(ValueError, match="missing from the canonical list"):
        frames_to_dataset(
            {"ZZZ": pd.DataFrame({"temp": np.zeros(24)}, index=times)},
            STATIONS,
            times,
            variables=["temp"],
        )


def test_frames_to_dataset_rejects_duplicate_station_ids():
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    with pytest.raises(ValueError, match="must be unique"):
        frames_to_dataset({}, [STATIONS[0], STATIONS[0]], times, variables=["temp"])


def test_safe_filename_strips_path_separators():
    assert safe_filename("a/b c") == "a_b_c"
    assert Station(id="010010-99999").safe_id == "010010-99999"


# --------------------------------------------------------------------------------------
# StationObservationProvider lifecycle
# --------------------------------------------------------------------------------------


class FakeNetwork(StationObservationProvider):
    """A two-station network backed by an in-memory table."""

    name = "fake_network"
    store_prefix = "test/fake_network.icechunk"
    partition_freq = "D"
    sample_freq = "1h"
    variables = ("temp",)
    max_workers = 2

    def __init__(self, config=None, stations=None, absent=(), broken=()):
        super().__init__(config=config, stations=stations)
        self.absent = set(absent)
        self.broken = set(broken)

    def all_stations(self):
        return list(STATIONS)

    def fetch_station(self, station, it, temp_dir):
        if station.id in self.broken:
            raise RuntimeError("upstream is down")
        if station.id in self.absent:
            return None
        path = temp_dir / self.station_filename(station, it)
        times = self.partition_times(it)
        pd.DataFrame({"temp": np.arange(len(times), dtype=float)}, index=times).to_csv(
            path, index_label="time"
        )
        return path

    def read_station(self, path, station, it):
        df = pd.read_csv(path, index_col="time", parse_dates=True)
        return df


@pytest.fixture
def fake_network(local_config):
    return FakeNetwork(config=local_config)


def test_run_partition_writes_a_station_cube(fake_network, tmp_path):
    stamp = pd.Timestamp("2026-01-01")
    assert fake_network.run_partition(stamp) is True

    repo = fake_network.get_icechunk_repo()
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes == {"time": 24, "station": 2}
    assert list(ds["station"].values) == ["AAA", "BBB"]
    assert float(ds["temp"].isel(time=5, station=0)) == 5.0


def test_consecutive_partitions_append(fake_network):
    assert fake_network.run_partition(pd.Timestamp("2026-01-01")) is True
    assert fake_network.run_partition(pd.Timestamp("2026-01-02")) is True

    repo = fake_network.get_icechunk_repo()
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert ds.sizes["time"] == 48
    assert ds.sizes["station"] == 2


def test_a_station_absent_from_the_archive_is_still_a_column(local_config):
    provider = FakeNetwork(config=local_config, absent={"BBB"})
    provider.run_partition(pd.Timestamp("2026-01-01"))
    ds = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert list(ds["station"].values) == ["AAA", "BBB"]
    assert bool(np.isnan(ds["temp"].sel(station="BBB").values).all())


def test_every_station_failing_raises_rather_than_reporting_an_empty_partition(
    local_config, tmp_path
):
    # An empty fetch is recorded as "nothing to do" and never retried, so a total
    # upstream outage must surface as a failure.
    provider = FakeNetwork(config=local_config, broken={"AAA", "BBB"})
    with pytest.raises(RuntimeError, match="nothing fetched"):
        provider.fetch(pd.Timestamp("2026-01-01"), temp_dir=tmp_path)


def test_nothing_fetched_plus_one_error_is_a_failure_not_an_empty_partition(
    local_config, tmp_path
):
    # The rest of the roster being legitimately absent must not disguise the one station
    # whose request blew up: nothing was written, so the partition is unproven.
    provider = FakeNetwork(config=local_config, absent={"AAA"}, broken={"BBB"})
    with pytest.raises(RuntimeError, match="nothing fetched"):
        provider.fetch(pd.Timestamp("2026-01-01"), temp_dir=tmp_path)


def test_a_wholly_absent_partition_is_reported_empty(local_config, tmp_path):
    # No errors at all: the archive simply has nothing here, which is not a failure.
    provider = FakeNetwork(config=local_config, absent={"AAA", "BBB"})
    assert provider.fetch(pd.Timestamp("2026-01-01"), temp_dir=tmp_path) == []


def test_partial_paths_are_unique_per_process_and_call():
    from planetary_datasets.providers.observations.base import partial_path

    dest = pathlib.Path("/tmp/010010-99999-2023.gz")
    first, second = partial_path(dest), partial_path(dest)
    # Twelve monthly partitions of one year share the cached file; a fixed .part name
    # would let two concurrent backfill runs interleave writes into it.
    assert first != second
    assert first.parent == dest.parent
    assert first.name.startswith(dest.name)


def test_some_stations_failing_still_writes_the_rest(local_config, tmp_path):
    provider = FakeNetwork(config=local_config, broken={"BBB"})
    assert provider.run_partition(pd.Timestamp("2026-01-01")) is True


def test_process_raises_when_nothing_parses(local_config, tmp_path):
    provider = FakeNetwork(config=local_config)
    empty = tmp_path / "AAA.dat"
    empty.write_text("time,temp\n")
    with pytest.raises(NoStationDataError):
        provider.process([str(empty)], pd.Timestamp("2026-01-01"))


def test_station_subset_narrows_the_axis(local_config):
    provider = FakeNetwork(config=local_config, stations=["BBB"])
    assert [s.id for s in provider.station_list()] == ["BBB"]


def test_unknown_station_subset_is_rejected(local_config):
    provider = FakeNetwork(config=local_config, stations=["NOPE"])
    with pytest.raises(ValueError, match="unknown station id"):
        provider.station_list()


def test_store_path_stays_local_under_the_test_config(fake_network, tmp_path):
    assert fake_network.store_path.startswith(str(tmp_path))


# --------------------------------------------------------------------------------------
# ASOS
# --------------------------------------------------------------------------------------

ASOS_CSV = (
    "station,station_name,lat,lon,valid(UTC),tmpf,dwpf\n"
    "DSM,Des Moines,41.5340,-93.6531,2026-09-20 00:00,71,60\n"
    "DSM,Des Moines,41.5340,-93.6531,2026-09-20 00:01,71,60\n"
)


def test_asos_url_covers_exactly_one_utc_day(local_config):
    from planetary_datasets.providers.observations.asos import ASOSOneMinuteProvider

    provider = ASOSOneMinuteProvider(config=local_config)
    url = provider.station_url(Station(id="DSM"), pd.Timestamp("2026-09-20"))
    assert "sts=2026-09-20T00:00Z" in url
    assert "ets=2026-09-21T00:00Z" in url
    assert "station=DSM" in url
    assert "vars=tmpf" in url


def test_asos_parses_the_iem_csv(local_config, tmp_path):
    from planetary_datasets.providers.observations.asos import ASOSOneMinuteProvider

    path = tmp_path / "DSM.csv"
    path.write_text(ASOS_CSV)
    provider = ASOSOneMinuteProvider(config=local_config)
    df = provider.read_station(path, Station(id="DSM"), pd.Timestamp("2026-09-20"))
    assert list(df.columns) == ["tmpf", "dwpf"]
    assert df.index[0] == pd.Timestamp("2026-09-20T00:00")


def test_asos_treats_a_header_only_response_as_no_data(local_config, tmp_path, monkeypatch):
    from planetary_datasets.providers.observations import asos as asos_module

    provider = asos_module.ASOSOneMinuteProvider(config=local_config)
    monkeypatch.setattr(provider, "_get_text", lambda url: "station,valid(UTC),tmpf\n")
    assert provider.fetch_station(Station(id="DSM"), pd.Timestamp("2026-09-20"), tmp_path) is None


def test_asos_raises_after_exhausting_retries(local_config, monkeypatch):
    from planetary_datasets.providers.observations import asos as asos_module

    class Response:
        status_code = 200
        text = "ERROR: too many requests"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(asos_module.requests, "get", lambda *a, **k: Response())
    monkeypatch.setattr(asos_module.time, "sleep", lambda _s: None)
    provider = asos_module.ASOSOneMinuteProvider(config=local_config)
    provider.max_attempts = 2
    with pytest.raises(RuntimeError, match="2 attempts failed"):
        provider._get_text("https://example.invalid/asos")


def test_asos_roster_cache_is_reused(local_config, monkeypatch):
    from planetary_datasets.providers.observations import asos as asos_module

    provider = asos_module.ASOSOneMinuteProvider(config=local_config)
    provider.roster_cache.parent.mkdir(parents=True, exist_ok=True)
    provider.roster_cache.write_text(
        json.dumps([{"id": "DSM", "latitude": 41.5, "longitude": -93.6}])
    )

    def _boom(*_args, **_kwargs):
        raise AssertionError("the roster cache should have been used")

    monkeypatch.setattr(asos_module, "fetch_network_stations", _boom)
    assert [s.id for s in provider.all_stations()] == ["DSM"]


# --------------------------------------------------------------------------------------
# ISD
# --------------------------------------------------------------------------------------

# The first line of noaa-isd-pds/data/2023/010010-99999-2023.gz, verbatim.
ISD_LINE = (
    "0104010010999992023010100004+70939-008669FM-12+001099999V0202671N0142199999999"
    "999999999-01001-01241097251ADDAA199999999KA1120M-00991KA2120N-01081MA199999909"
    "7131MD1310301+9999OC102641OD199902361999REMSYN004BUFR"
)


def test_isd_parses_a_real_record():
    from planetary_datasets.providers.observations.isd import parse_isd_text

    df = parse_isd_text(ISD_LINE)
    assert len(df) == 1
    assert df.index[0] == pd.Timestamp("2023-01-01T00:00")
    assert df["air_temperature"].iloc[0] == pytest.approx(-10.0)
    assert df["sea_level_pressure"].iloc[0] == pytest.approx(972.5)


def test_isd_masks_the_missing_value_sentinels():
    from planetary_datasets.providers.observations.isd import MISSING_SENTINELS, parse_isd_text

    df = parse_isd_text(ISD_LINE)
    # ceiling is 99999 in this record, which is the sentinel, not a 99 km cloud base.
    assert MISSING_SENTINELS["ceiling"] == 99999
    assert bool(pd.isna(df["ceiling"].iloc[0]))


def test_isd_skips_unparseable_lines():
    from planetary_datasets.providers.observations.isd import parse_isd_text

    df = parse_isd_text("too short\n" + ISD_LINE + "\n\n")
    assert len(df) == 1


def test_isd_read_station_cuts_the_year_file_down_to_the_month(local_config, tmp_path):
    from planetary_datasets.providers.observations.isd import ISDProvider

    path = tmp_path / "010010-99999-2023.gz"
    path.write_bytes(gzip.compress(ISD_LINE.encode()))

    provider = ISDProvider(config=local_config, stations=[Station(id="010010-99999")])
    january = provider.read_station(path, Station(id="010010-99999"), pd.Timestamp("2023-01-01"))
    assert january is not None and len(january) == 1
    february = provider.read_station(path, Station(id="010010-99999"), pd.Timestamp("2023-02-01"))
    assert february is None


def test_isd_history_parses_into_stations(monkeypatch):
    from planetary_datasets.providers.observations import isd as isd_module

    csv = (
        "USAF,WBAN,STATION NAME,CTRY,STATE,ICAO,LAT,LON,ELEV(M),BEGIN,END\n"
        "010010,99999,JAN MAYEN,NO,,ENJA,70.939,-8.669,9.0,19310101,20260101\n"
        "010014,99999,SORSTOKKEN,NO,,ENSO,59.792,5.341,9999.0,19861120,20260101\n"
    )

    class FakeHandle(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(isd_module.fsspec, "open", lambda *a, **k: FakeHandle(csv.encode()))
    stations = isd_module.read_isd_history()
    assert [s.id for s in stations] == ["010010-99999", "010014-99999"]
    assert stations[0].latitude == pytest.approx(70.939)
    # 9999 m is the elevation sentinel, not a station in the stratosphere.
    assert stations[1].elevation is None


# --------------------------------------------------------------------------------------
# GHCN
# --------------------------------------------------------------------------------------


def test_ghcn_station_list_reads_latitude_before_longitude(monkeypatch):
    from planetary_datasets.providers.observations import ghcn as ghcn_module

    text = (
        "ACL000BARA9  17.5910  -61.8210    5.0 TX BARBUDA        AG\n"
        "ACM00078861  17.1167  -61.7833   10.0    COOLIDGE FIELD AG\n"
    )

    class FakeHandle(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(ghcn_module.fsspec, "open", lambda *a, **k: FakeHandle(text.encode()))
    df = ghcn_module.get_list_of_stations()
    assert len(df) == 2
    # The original script read column 2 as latitude and column 1 as longitude, putting
    # every station in the wrong hemisphere.
    assert df["latitude"].iloc[0] == pytest.approx(17.5910)
    assert df["longitude"].iloc[0] == pytest.approx(-61.8210)


def _meteora_frames(tmp_path):
    """The two parquet files GHCNHourlyProvider.fetch leaves behind."""
    index = pd.MultiIndex.from_product(
        [
            ["USC00111577", "USW00094846"],
            pd.date_range("2023-01-01", periods=8, freq="15min", tz="UTC"),
        ],
        names=["station_id", "time"],
    )
    observations = tmp_path / "obs.parquet"
    pd.DataFrame({"temperature": np.arange(16.0)}, index=index).to_parquet(observations)

    stations = tmp_path / "stations.parquet"
    pd.DataFrame(
        {
            "LATITUDE": [41.7372, 41.9603],
            "LONGITUDE": [-87.7775, -87.9317],
            "ELEVATION": [189.0, 204.8],
            "NAME": ["CHICAGO MIDWAY", "CHICAGO OHARE"],
        },
        index=pd.Index(["USC00111577", "USW00094846"], name="station_id"),
    ).to_parquet(stations)
    return [str(observations), str(stations)]


def test_ghcn_meteora_route_reshapes_into_a_station_cube(local_config, tmp_path):
    from planetary_datasets.providers.observations.ghcn import GHCNHourlyProvider

    provider = GHCNHourlyProvider(config=local_config, variables=["temperature"])
    ds = provider.process(_meteora_frames(tmp_path), pd.Timestamp("2023-01-01"))

    # A quarter of hourly steps, not the sub-hourly resolution meteora returns.
    assert ds.sizes == {"time": 90 * 24, "station": 2}
    assert list(ds["station"].values) == ["USC00111577", "USW00094846"]
    # 00:00, 00:15, 00:30, 00:45 collapse to the last reading in the hour.
    assert float(ds["temperature"].isel(time=0, station=0)) == pytest.approx(3.0)
    assert float(ds["latitude"].isel(station=1)) == pytest.approx(41.9603)


def test_ghcn_meteora_route_rejects_a_station_missing_from_the_table(local_config, tmp_path):
    from planetary_datasets.providers.observations.ghcn import GHCNHourlyProvider

    files = _meteora_frames(tmp_path)
    stations = pd.read_parquet(files[1]).iloc[:1]
    stations.to_parquet(files[1])

    provider = GHCNHourlyProvider(config=local_config, variables=["temperature"])
    with pytest.raises(NoStationDataError, match="absent from the station table"):
        provider.process(files, pd.Timestamp("2023-01-01"))


def test_ghcn_meteora_partition_is_a_calendar_quarter(local_config):
    from planetary_datasets.providers.observations.ghcn import GHCNHourlyProvider

    times = GHCNHourlyProvider(config=local_config).partition_times(pd.Timestamp("2023-01-01"))
    assert times[0] == pd.Timestamp("2023-01-01T00:00")
    assert times[-1] == pd.Timestamp("2023-03-31T23:00")


def test_ghcn_station_provider_demands_an_explicit_station_list(local_config):
    from planetary_datasets.providers.observations.ghcn import GHCNHourlyStationProvider

    with pytest.raises(ValueError, match="explicit station list"):
        GHCNHourlyStationProvider(config=local_config)


def test_ghcn_station_provider_reads_the_parquet_date_column(local_config, tmp_path):
    from planetary_datasets.providers.observations.ghcn import GHCNHourlyStationProvider

    path = tmp_path / "USW00094846-2023.parquet"
    pd.DataFrame(
        {
            "DATE": ["2023-01-01T00:51:00", "2023-02-01T00:51:00"],
            "temperature": [2.2, 3.3],
            "wind_speed": [1.0, 2.0],
        }
    ).to_parquet(path)

    provider = GHCNHourlyStationProvider(
        config=local_config, stations=[Station(id="USW00094846")]
    )
    df = provider.read_station(path, Station(id="USW00094846"), pd.Timestamp("2023-01-01"))
    assert len(df) == 1
    assert df["temperature"].iloc[0] == pytest.approx(2.2)


# --------------------------------------------------------------------------------------
# Meteostat
# --------------------------------------------------------------------------------------

METEOSTAT_CSV = (
    "year,month,day,hour,temp,temp_source,rhum,rhum_source,wspd,wspd_source\n"
    "2023,1,1,0,10.9,dwd,76,dwd,10.1,dwd\n"
    "2023,1,1,1,12.4,dwd,73,dwd,15.5,dwd\n"
    "2023,6,1,0,20.0,dwd,50,dwd,5.0,dwd\n"
)


def test_meteostat_reads_the_bulk_csv_for_the_partition_month(local_config, tmp_path):
    from planetary_datasets.providers.observations.meteostat import MeteostatHourlyProvider

    path = tmp_path / "10637-2023.csv.gz"
    path.write_bytes(gzip.compress(METEOSTAT_CSV.encode()))

    provider = MeteostatHourlyProvider(config=local_config, stations=[Station(id="10637")])
    df = provider.read_station(path, Station(id="10637"), pd.Timestamp("2023-01-01"))
    assert len(df) == 2
    assert "temp_source" not in df.columns
    assert df["temp"].iloc[1] == pytest.approx(12.4)


def test_meteostat_inventory_window_excludes_stations_with_no_overlap(local_config):
    from planetary_datasets.providers.observations.meteostat import MeteostatHourlyProvider

    provider = MeteostatHourlyProvider(config=local_config, stations=[Station(id="X")])
    provider._inventory = {"X": (pd.Timestamp("2020-01-01"), pd.Timestamp("2021-01-01"))}
    assert provider.covers(Station(id="X"), pd.Timestamp("2020-06-01")) is True
    assert provider.covers(Station(id="X"), pd.Timestamp("2023-06-01")) is False


def test_meteostat_roster_entry_reads_the_hourly_inventory():
    from planetary_datasets.providers.observations.meteostat import _station_entry

    station, start, end = _station_entry(
        {
            "id": "00FAY",
            "name": {"en": "Holden Agdm"},
            "location": {"latitude": 53.19, "longitude": -112.25, "elevation": 688},
            "inventory": {"hourly": {"start": "2020-01-01", "end": "2024-12-07"}},
        }
    )
    assert station.id == "00FAY"
    assert station.latitude == pytest.approx(53.19)
    assert start == pd.Timestamp("2020-01-01")
    assert end == pd.Timestamp("2024-12-07")


# --------------------------------------------------------------------------------------
# AERONET
# --------------------------------------------------------------------------------------

AERONET_RESPONSE = "\n".join(
    [
        "AERONET Data Download (Version 3 Direct Sun)",
        "AERONET Version 3",
        "Version 3: AOD Level 1.5",
        "The following data are cloud cleared...",
        "Contact: PI=Someone; PI Email=someone@example.org",
        "AERONET_Site,Date(dd:mm:yyyy),Time(hh:mm:ss),AOD_500nm,AOD_440nm",
        "GSFC,01:06:2023,12:00:00,0.257342,0.299547",
        "GSFC,01:06:2023,12:30:00,0.357342,0.399547",
        "Tucson,01:06:2023,01:20:58,0.030078,-999.000000",
    ]
)


def test_aeronet_parses_the_web_service_response():
    from planetary_datasets.providers.observations.aeronet import parse_web_data

    df = parse_web_data(AERONET_RESPONSE)
    assert len(df) == 3
    assert df.index[0] == pd.Timestamp("2023-06-01T12:00:00")
    assert bool(pd.isna(df["AOD_440nm"].iloc[2]))


def test_aeronet_preamble_only_response_is_empty():
    from planetary_datasets.providers.observations.aeronet import parse_web_data

    assert parse_web_data("AERONET Data Download\nline2\nline3\n").empty


def test_aeronet_averages_onto_the_hourly_grid(local_config, tmp_path, monkeypatch):
    from planetary_datasets.providers.observations import aeronet as aeronet_module

    provider = aeronet_module.AeronetProvider(config=local_config, sites=["GSFC", "Tucson"])
    monkeypatch.setattr(
        provider,
        "station_list",
        lambda: [
            Station(id="GSFC", latitude=38.99, longitude=-76.84),
            Station(id="Tucson", latitude=32.23, longitude=-110.95),
        ],
    )
    path = tmp_path / "aeronet.csv"
    path.write_text(AERONET_RESPONSE)

    ds = provider.process([str(path)], pd.Timestamp("2023-06-01"))
    assert ds.sizes == {"time": 24, "station": 2}
    # 0.257342 and 0.357342 both fall in the 12:00 hour.
    assert float(ds["AOD_500nm"].sel(station="GSFC").isel(time=12)) == pytest.approx(
        0.307342, abs=1e-6
    )


def test_aeronet_rejects_an_unknown_quality_level(local_config):
    from planetary_datasets.providers.observations.aeronet import AeronetProvider

    with pytest.raises(ValueError, match="quality must be one of"):
        AeronetProvider(config=local_config, quality="AOD30")


def test_aeronet_request_url_is_a_single_day(local_config):
    from planetary_datasets.providers.observations.aeronet import AeronetProvider

    url = AeronetProvider(config=local_config).request_url(pd.Timestamp("2023-06-01"))
    assert "year=2023&month=6&day=1" in url
    assert "year2=2023&month2=6&day2=1" in url
    assert "AOD15=1" in url


# --------------------------------------------------------------------------------------
# SOLRAD / SURFRAD / MIDC
# --------------------------------------------------------------------------------------


def test_surfrad_file_url_uses_two_digit_year_and_day_of_year():
    from planetary_datasets.providers.observations.surfrad import file_url

    assert file_url("bon", pd.Timestamp("2020-04-24")).endswith("bon/2020/bon20115.dat")


def test_solrad_filename_keeps_the_upstream_name():
    from planetary_datasets.providers.observations.solrad import SolradProvider

    # pvlib's reader chooses the Madison column layout by looking for "msn" in the path,
    # so the local name has to keep the station prefix.
    name = SolradProvider.station_filename(
        SolradProvider, Station(id="msn"), pd.Timestamp("2020-04-24")
    )
    assert name == "msn20115.dat"


def test_solrad_roster_is_the_published_site_list(local_config):
    from planetary_datasets.providers.observations.solrad import SITES, SolradProvider

    assert [s.id for s in SolradProvider(config=local_config).all_stations()] == list(SITES)


def test_surfrad_keeps_retired_sites_on_the_axis(local_config):
    from planetary_datasets.providers.observations.surfrad import SurfradProvider

    ids = [s.id for s in SurfradProvider(config=local_config).all_stations()]
    assert {"red", "rut", "slv"}.issubset(ids)


def test_surfrad_variables_are_the_names_pvlib_actually_returns():
    from pvlib.iotools.surfrad import SURFRAD_COLUMNS, VARIABLE_MAP

    from planetary_datasets.providers.observations.surfrad import VARIABLES

    # read_surfrad renames by default, so the raw file column names ("dw_solar",
    # "diffuse", "zen") never appear in the frame and asking for them yields an
    # entirely NaN store.
    produced = {VARIABLE_MAP.get(column, column) for column in SURFRAD_COLUMNS}
    assert set(VARIABLES) <= produced


def test_solrad_variables_are_the_names_pvlib_actually_returns():
    from pvlib.iotools.solrad import HEADERS, MADISON_HEADERS

    from planetary_datasets.providers.observations.solrad import VARIABLES

    assert set(VARIABLES) <= set(HEADERS) | set(MADISON_HEADERS)


def test_midc_variables_come_from_the_pvlib_variable_map(local_config):
    from pvlib.iotools.midc import MIDC_VARIABLE_MAP

    from planetary_datasets.providers.observations.midc import MIDCProvider

    produced = {name for site in MIDC_VARIABLE_MAP.values() for name in site.values()}
    assert set(MIDCProvider(config=local_config).variables) == produced


def test_midc_url_points_at_nrel_not_pvlibs_misspelling(local_config):
    from planetary_datasets.providers.observations.midc import MIDCProvider

    provider = MIDCProvider(config=local_config)
    url = provider.station_url(Station(id="BMS"), pd.Timestamp("2023-06-01"))
    assert url.startswith("https://midcdmz.nrel.gov/apps/data_api.pl")
    assert "begin=20230601&end=20230601" in url


def test_midc_roster_matches_what_pvlib_can_map(local_config):
    from planetary_datasets.providers.observations.midc import MIDCProvider, midc_sites

    assert [s.id for s in MIDCProvider(config=local_config).all_stations()] == midc_sites()


def test_download_or_none_returns_none_on_404(tmp_path, monkeypatch):
    from planetary_datasets.providers.observations import base as base_module

    class Response:
        status_code = 404

        def raise_for_status(self):
            raise AssertionError("must not be called for a 404")

    monkeypatch.setattr("requests.get", lambda *a, **k: Response())
    assert base_module.download_or_none("https://example.invalid/x", tmp_path / "x") is None


def test_download_or_none_raises_after_retries(tmp_path, monkeypatch):
    import requests

    from planetary_datasets.providers.observations import base as base_module

    def _boom(*_a, **_k):
        raise requests.ConnectionError("no route to host")

    monkeypatch.setattr("requests.get", _boom)
    monkeypatch.setattr(base_module.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="attempts failed"):
        base_module.download_or_none("https://example.invalid/x", tmp_path / "x", retries=2)


# --------------------------------------------------------------------------------------
# PV Live
# --------------------------------------------------------------------------------------


# Column names and the ``tng000NN_YYYY-MM.tsv`` layout are taken from the real
# pvlive_2020-09.zip on Zenodo.
PVLIVE_ROWS = (
    "datetime\tGg_pyr\tflag_Gg_pyr\tT_pyr\tflag_T_pyr\n"
    "2026-01-01T00:00\t0.0\t0\t1.5\t0\n"
    "2026-01-01T00:01\t0.0\t0\t1.6\t0\n"
)
PVLIVE_METADATA = (
    "LocationID\tName\tLatitude [deg]\tLongitude [deg]\tAltitude from EU-DEM [m]\n"
    "tng00001\tWendlingen\t48.667\t9.399\t276\n"
    "tng00002\tStuttgart\t48.830\t9.196\t294\n"
)


def _pvlive_zip(path: pathlib.Path) -> pathlib.Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("2026-01/tng00001_2026-01.tsv", PVLIVE_ROWS)
        archive.writestr("2026-01/tng00002_2026-01.tsv", PVLIVE_ROWS)
        archive.writestr("2026-01/metadata_stations_2026-01.tsv", PVLIVE_METADATA)
    return path


def test_pvlive_reads_every_station_out_of_a_month_archive(tmp_path):
    from planetary_datasets.providers.observations.pvlive import read_month_zip

    frames, stations = read_month_zip(_pvlive_zip(tmp_path / "pvlive.zip"))
    assert sorted(frames) == ["tng00001", "tng00002"]
    assert list(frames["tng00001"].columns) == ["Gg_pyr", "flag_Gg_pyr", "T_pyr", "flag_T_pyr"]
    assert stations["tng00001"].latitude == pytest.approx(48.667)
    assert stations["tng00001"].name == "Wendlingen"


def test_pvlive_process_builds_the_forty_station_axis(local_config, tmp_path):
    from planetary_datasets.providers.observations.pvlive import PVLiveProvider

    provider = PVLiveProvider(config=local_config)
    ds = provider.process(
        [str(_pvlive_zip(tmp_path / "pvlive.zip"))], pd.Timestamp("2026-01-01")
    )
    assert ds.sizes["station"] == 40
    assert ds.sizes["time"] == 31 * 24 * 60
    assert float(ds["T_pyr"].sel(station="tng00001").isel(time=1)) == pytest.approx(1.6)
    # Declared variables only: the quality flags are dropped.
    assert "flag_Gg_pyr" not in ds.data_vars
    # Coordinates come from the archive's own metadata table.
    assert float(ds["latitude"].sel(station="tng00002")) == pytest.approx(48.830)


def test_pvlive_reuses_cached_metadata_when_an_archive_omits_it(local_config, tmp_path):
    from planetary_datasets.providers.observations.pvlive import PVLiveProvider

    provider = PVLiveProvider(config=local_config)

    # First month carries the table and seeds the cache.
    with_metadata = provider.process(
        [str(_pvlive_zip(tmp_path / "a.zip"))], pd.Timestamp("2026-01-01")
    )
    assert float(with_metadata["latitude"].sel(station="tng00001")) == pytest.approx(48.667)

    # Second month's archive has no metadata table; the coordinate set must not change.
    bare = tmp_path / "b.zip"
    with zipfile.ZipFile(bare, "w") as archive:
        archive.writestr("2026-02/tng00001_2026-02.tsv", PVLIVE_ROWS)
    without_metadata = provider.process([str(bare)], pd.Timestamp("2026-02-01"))
    assert set(without_metadata.coords) == set(with_metadata.coords)
    assert float(without_metadata["latitude"].sel(station="tng00001")) == pytest.approx(48.667)


def test_pvlive_empty_archive_raises(local_config, tmp_path):
    from planetary_datasets.providers.observations.pvlive import PVLiveProvider

    path = tmp_path / "empty.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("readme.txt", "nothing here")
    with pytest.raises(NoStationDataError):
        PVLiveProvider(config=local_config).process([str(path)], pd.Timestamp("2026-01-01"))


# --------------------------------------------------------------------------------------
# CDS providers
# --------------------------------------------------------------------------------------


def _long_table() -> xr.Dataset:
    stamps = pd.to_datetime(
        [
            "2026-01-01T00:10",
            "2026-01-01T00:40",
            "2026-01-01T01:00",
            "2026-01-01T00:00",
        ]
    )
    return xr.Dataset(
        {
            "report_timestamp": ("index", stamps),
            "primary_station_id": ("index", ["AAAA", "AAAA", "AAAA", "BBBB"]),
            "observed_variable": (
                "index",
                [
                    "total_column_water_vapour",
                    "total_column_water_vapour",
                    "total_column_water_vapour",
                    "total_column_water_vapour",
                ],
            ),
            "observation_value": ("index", [10.0, 20.0, 30.0, 5.0]),
            "latitude": ("index", [1.0, 1.0, 1.0, 2.0]),
            "longitude": ("index", [3.0, 3.0, 3.0, 4.0]),
        },
        coords={"index": np.arange(4)},
    )


def test_long_table_to_cube_bins_onto_the_time_grid():
    from planetary_datasets.providers.observations.cds import long_table_to_cube

    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = long_table_to_cube(_long_table(), times, stations=["AAAA", "BBBB"])

    assert ds.sizes == {"time": 24, "station": 2}
    # 00:10 and 00:40 both fall in the 00:00 bin and are averaged.
    assert float(ds["total_column_water_vapour"].sel(station="AAAA").isel(time=0)) == pytest.approx(
        15.0
    )
    assert float(ds["total_column_water_vapour"].sel(station="AAAA").isel(time=1)) == pytest.approx(
        30.0
    )
    assert list(ds["latitude"].values) == [1.0, 2.0]


def test_long_table_to_cube_writes_declared_variables_the_response_lacks():
    from planetary_datasets.providers.observations.cds import long_table_to_cube

    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = long_table_to_cube(
        _long_table(),
        times,
        stations=["AAAA", "BBBB"],
        variables=("total_column_water_vapour", "zenith_total_delay"),
    )
    # A month where nobody reported one of the two variables must still write both, or
    # write_to_icechunk refuses the append for a variable mismatch.
    assert set(ds.data_vars) == {"total_column_water_vapour", "zenith_total_delay"}
    assert bool(np.isnan(ds["zenith_total_delay"].values).all())


def test_long_table_to_cube_pins_the_level_axis():
    from planetary_datasets.providers.observations.cds import long_table_to_cube

    ds_in = _long_table().assign(z_coordinate=("index", [100000.0] * 4))
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = long_table_to_cube(
        ds_in, times, stations=["AAAA", "BBBB"], levels=(100000.0, 85000.0, 50000.0)
    )
    # Only one level was reported; all three must still appear.
    assert list(ds["level"].values) == [100000.0, 85000.0, 50000.0]


def test_cds_station_axis_follows_the_store_once_one_exists(local_config):
    from planetary_datasets.providers.observations.cds import long_table_to_cube
    from planetary_datasets.providers.observations.gnss import GNSSProvider

    provider = GNSSProvider(config=local_config)
    assert provider.station_axis() is None

    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    first = long_table_to_cube(_long_table(), times, variables=("total_column_water_vapour",))
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), first) is True

    # The second month must reuse the first month's axis rather than whatever reported.
    assert provider.station_axis() == ["AAAA", "BBBB"]


def test_long_table_to_cube_names_the_columns_it_could_not_find():
    from planetary_datasets.providers.observations.cds import long_table_to_cube

    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    bare = xr.Dataset({"something": ("index", [1.0])}, coords={"index": [0]})
    with pytest.raises(NoStationDataError, match="not a recognised long table"):
        long_table_to_cube(bare, times)


def test_long_table_to_cube_adds_a_level_axis_when_levels_are_given():
    from planetary_datasets.providers.observations.cds import long_table_to_cube

    ds_in = _long_table().assign(
        z_coordinate=("index", [100000.0, 85000.0, 85000.0, 100000.0])
    )
    times = partition_time_index(pd.Timestamp("2026-01-01"), "D", "1h")
    ds = long_table_to_cube(
        ds_in, times, stations=["AAAA", "BBBB"], levels=(100000.0, 85000.0)
    )
    assert "level" in ds.dims
    assert sorted(ds["level"].values) == [85000.0, 100000.0]


def test_cds_provider_raises_missing_credential_without_a_key(local_config, monkeypatch):
    from planetary_datasets.config import MissingCredential
    from planetary_datasets.providers.observations.gnss import GNSSProvider

    provider = GNSSProvider(config=local_config)
    # Pretend there is no ~/.cdsapirc either.
    monkeypatch.setattr(pathlib.Path, "is_file", lambda _self: False)
    with pytest.raises(MissingCredential, match="CDSAPI_URL"):
        provider.client()


def test_gnss_request_asks_for_one_month():
    from planetary_datasets.providers.observations.gnss import GNSSProvider

    request = GNSSProvider().build_request(pd.Timestamp("2023-04-01"))
    assert request["year"] == "2023"
    assert request["month"] == "04"
    assert len(request["day"]) == 31
    assert request["network_type"] == "igs_daily"


def test_rahm_request_carries_the_harmonisation_archive():
    from planetary_datasets.providers.observations.rahm import ARCHIVE, RAHMProvider

    request = RAHMProvider().build_request(pd.Timestamp("1985-11-01"))
    assert request["archive"] == ARCHIVE
    assert request["year"] == "1985"
    assert request["month"] == "11"
    assert "air_temperature" in request["variable"]


# --------------------------------------------------------------------------------------
# Dagster wiring
# --------------------------------------------------------------------------------------


def test_every_network_is_exposed_as_a_dagster_asset():
    import dagster as dg

    from dags.assets import surface_obs

    assets = dg.load_assets_from_modules([surface_obs])
    names = {key.path[-1] for asset in assets for key in asset.keys}
    assert names == {
        "aeronet_aod",
        "asos_one_minute",
        "ghcn_hourly",
        "gnss_water_vapour",
        "isd_hourly",
        "meteostat_hourly",
        "midc",
        "pvlive",
        "rahm_radiosonde",
        "solrad",
        "surfrad",
    }
    dg.Definitions(assets=assets)
