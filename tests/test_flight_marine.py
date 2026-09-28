"""Offline tests for the flight-tracking and marine-float providers.

Nothing here touches the network: OpenSky archives are built in tmp_path, the ERDDAP
response is synthesised, and every store is a local icechunk repository.
"""

from __future__ import annotations

import gzip
import io
import pathlib
import tarfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.observations import _points, eurocontrol, opensky, osmc

# --------------------------------------------------------------------------- helpers


def make_states_frame(start="2020-05-11T00:00:00", rows=6, camel=False) -> pd.DataFrame:
    """A small OpenSky state-vector frame, in either column spelling."""
    times = pd.date_range(start, periods=rows, freq="10min")
    # Not `.view("int64") // 1e9`: pandas 3 defaults date_range to microsecond
    # resolution, so that silently divides microseconds and lands in 1970.
    epoch = times.to_numpy().astype("datetime64[s]").astype("int64")
    frame = pd.DataFrame(
        {
            "time": epoch,
            "icao24": ["abc123", "def456"] * (rows // 2),
            "lat": np.linspace(50.0, 52.0, rows),
            "lon": np.linspace(-1.0, 1.0, rows),
            "velocity": np.linspace(200.0, 240.0, rows),
            "heading": np.linspace(0.0, 90.0, rows),
            "callsign": ["BAW1   ", "EZY2   "] * (rows // 2),
        }
    )
    if camel:
        frame["geoAltitude"] = np.linspace(9000.0, 11000.0, rows)
        frame["baroAltitude"] = np.linspace(8900.0, 10900.0, rows)
        frame["vertRate"] = np.zeros(rows)
        frame["onGround"] = [False] * rows
    else:
        frame["geoaltitude"] = np.linspace(9000.0, 11000.0, rows)
        frame["baroaltitude"] = np.linspace(8900.0, 10900.0, rows)
        frame["vertrate"] = np.zeros(rows)
        frame["onground"] = [False] * rows
    frame["alert"] = [False] * rows
    return frame


def write_states_archive(path: pathlib.Path, frame: pd.DataFrame, compress=True) -> pathlib.Path:
    """Write a frame as an OpenSky-style ``.csv.tar`` archive.

    Real archives carry LICENSE.txt and README.txt beside the data, so they are included
    here: reading them as CSV is exactly the bug this shape guards against.
    """
    csv = frame.to_csv(index=False).encode()
    payload = gzip.compress(csv) if compress else csv
    inner = "states_inner.csv.gz" if compress else "states_inner.csv"
    with tarfile.open(path, "w") as tar:
        for name, body in ((inner, payload), ("LICENSE.txt", b"CC BY-SA 4.0\n"), ("README.txt", b"hi\n")):
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
    return path


def make_erddap_dataset(start="2012-01-01", rows=8) -> xr.Dataset:
    """A synthetic OSMC ERDDAP response: one ``row`` dimension, time as a variable."""
    times = pd.date_range(start, periods=rows, freq="3h")
    return xr.Dataset(
        {
            "platform_id": ("row", np.arange(rows, dtype="float64") % 2),
            "platform_type": ("row", np.array(["DRIFTING BUOYS (GENERIC)"] * rows, dtype=object)),
            "time": ("row", times.to_numpy()),
            "latitude": ("row", np.linspace(-10, 10, rows)),
            "longitude": ("row", np.linspace(20, 40, rows)),
            "sst": ("row", np.linspace(280.0, 290.0, rows)),
        }
    )


# ----------------------------------------------------------------------- opensky urls


def test_states_archive_url_carries_the_dotted_date():
    url = opensky.states_archive_url(pd.Timestamp("2020-05-11T07:00"))
    assert url == (
        "https://s3.opensky-network.org/data-samples/states/.2020-05-11/07/"
        "states_2020-05-11-07.csv.tar"
    )


def test_sample_hours_covers_mondays_only():
    hours = opensky.sample_hours("2016-06-06", "2016-06-20")
    assert len(hours) == 3 * 24
    assert set(pd.DatetimeIndex(hours).dayofweek) == {0}


# ------------------------------------------------------------------- opensky reading


@pytest.mark.parametrize("compress", [True, False])
def test_read_states_archive_handles_gzipped_and_plain_members(tmp_path, compress):
    frame = make_states_frame()
    path = write_states_archive(tmp_path / "states.csv.tar", frame, compress=compress)
    read = opensky.read_states_archive(path)
    assert len(read) == len(frame)
    assert "icao24" in read.columns


def test_read_states_archive_rejects_an_archive_with_no_csv(tmp_path):
    path = tmp_path / "empty.csv.tar"
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("README.txt")
        info.size = 3
        tar.addfile(info, io.BytesIO(b"hi\n"))
    with pytest.raises(ValueError, match="no CSV members"):
        opensky.read_states_archive(path)


@pytest.mark.parametrize("camel", [False, True])
def test_normalise_states_accepts_both_column_spellings(camel):
    out = opensky.normalise_states(make_states_frame(camel=camel))
    assert {"latitude", "longitude", "altitude", "vertical_rate", "on_ground"} <= set(out.columns)
    assert pd.api.types.is_datetime64_any_dtype(out["time"])


def test_normalise_states_is_idempotent():
    once = opensky.normalise_states(make_states_frame())
    twice = opensky.normalise_states(once)
    pd.testing.assert_frame_equal(once, twice)


def test_normalise_states_drops_rows_without_a_position():
    frame = make_states_frame()
    frame.loc[2, "lat"] = np.nan
    out = opensky.normalise_states(frame)
    assert len(out) == len(frame) - 1


def test_normalise_states_requires_the_core_columns():
    with pytest.raises(ValueError, match="missing required columns"):
        opensky.normalise_states(pd.DataFrame({"time": [0], "lat": [1.0]}))


def test_states_to_dataset_shapes_and_dtypes():
    ds = opensky.states_to_dataset(make_states_frame())
    assert ds.sizes["time"] == 6
    assert ds["latitude"].dtype == np.dtype("float32")
    assert ds["on_ground"].dtype == np.dtype("bool")
    assert ds["icao24"].dtype.kind == "U"
    # callsigns arrive space-padded upstream.
    assert set(np.unique(ds["callsign"].values)) == {"BAW1", "EZY2"}


def test_states_to_dataset_emits_every_variable_even_when_absent():
    """The store schema is fixed by the first hour written, so it must not vary."""
    frame = make_states_frame().drop(columns=["alert", "callsign", "geoaltitude"])
    ds = opensky.states_to_dataset(frame)
    expected = set(opensky.FLOAT_VARIABLES) | set(opensky.BOOL_VARIABLES) | set(opensky.STRING_VARIABLES)
    assert set(ds.data_vars) == expected
    assert bool(np.isnan(ds["altitude"].values).all())
    assert not ds["alert"].values.any()
    assert set(np.unique(ds["callsign"].values)) == {""}


def test_states_to_dataset_schema_is_stable_across_differing_archives(local_config):
    """An hour missing a column must still append, not be rejected forever."""
    repo = local_config.icechunk_repo("test/schema.icechunk")
    full = opensky.states_to_dataset(make_states_frame(start="2020-05-11T00:00"))
    partial = opensky.states_to_dataset(
        make_states_frame(start="2020-05-11T01:00").drop(columns=["alert"])
    )
    assert _points.append_point_observations(repo, full) is True
    assert _points.append_point_observations(repo, partial) is True


def test_states_to_dataset_keeps_duplicate_timestamps():
    frame = make_states_frame()
    frame["time"] = frame["time"].iloc[0]
    ds = opensky.states_to_dataset(frame)
    assert ds.sizes["time"] == len(frame)


# --------------------------------------------------------------- opensky trajectories


def test_iter_trajectories_splits_by_airframe():
    trajectories = dict(opensky.iter_trajectories(make_states_frame()))
    assert sorted(trajectories) == ["abc123", "def456"]
    one = trajectories["abc123"]
    assert one.sizes["time"] == 3
    assert str(one.coords["icao24"].values) == "abc123"
    # icao24 is constant for a trajectory, so it is a coordinate, not a per-sample var.
    assert "icao24" not in one.data_vars


def test_iter_trajectories_honours_min_points():
    assert list(opensky.iter_trajectories(make_states_frame(), min_points=4)) == []


def test_write_trajectories_writes_one_file_per_airframe(tmp_path):
    paths = opensky.write_trajectories(make_states_frame(), tmp_path, label="2020-05-11")
    assert len(paths) == 2
    assert not list(tmp_path.glob("*.part")), "partial files must be renamed away"
    reopened = xr.open_dataset(paths[0])
    assert "latitude" in reopened
    # A second call skips what is already written.
    assert opensky.write_trajectories(make_states_frame(), tmp_path, label="2020-05-11") == []


# -------------------------------------------------------------------- opensky hazards


def test_hazards_near_trajectory_filters_in_space_and_time():
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import box

    hazards = gpd.GeoDataFrame(
        {
            "VALID_FM": pd.to_datetime(
                ["2020-05-11T00:00Z", "2021-01-01T00:00Z", "2020-05-11T00:00Z"], utc=True
            ),
            "VALID_TO": pd.to_datetime(
                ["2020-05-11T06:00Z", "2021-01-01T06:00Z", "2020-05-11T06:00Z"], utc=True
            ),
        },
        geometry=[box(-2, 49, 2, 53), box(-2, 49, 2, 53), box(100, 10, 110, 20)],
        crs="EPSG:4326",
    )
    ds = opensky.states_to_dataset(make_states_frame())
    kept = opensky.hazards_near_trajectory(hazards, ds, "VALID_FM", "VALID_TO")
    # Only the first: the second is out of time, the third out of area.
    assert list(kept.index) == [0]


def test_hazards_near_trajectory_on_an_empty_trajectory():
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import box

    hazards = gpd.GeoDataFrame(
        {"VALID_FM": pd.to_datetime(["2020-05-11T00:00Z"]), "VALID_TO": pd.to_datetime(["2020-05-11T06:00Z"])},
        geometry=[box(-2, 49, 2, 53)],
        crs="EPSG:4326",
    )
    empty = opensky.states_to_dataset(make_states_frame()).isel(time=slice(0, 0))
    assert len(opensky.hazards_near_trajectory(hazards, empty, "VALID_FM", "VALID_TO")) == 0


def test_within_bounds_clips_to_a_box():
    ds = opensky.states_to_dataset(make_states_frame())
    clipped = opensky.within_bounds(ds, 50.5, 55.0, -180, 180)
    assert clipped.sizes["time"] < ds.sizes["time"]
    assert float(clipped["latitude"].min()) >= 50.5


# ------------------------------------------------------------------ point store logic


def test_partition_bounds_hourly_and_monthly():
    assert _points.partition_bounds("2020-05-11T03:00", "h") == (
        pd.Timestamp("2020-05-11T03:00"),
        pd.Timestamp("2020-05-11T04:00"),
    )
    assert _points.partition_bounds("2012-01-01", "MS") == (
        pd.Timestamp("2012-01-01"),
        pd.Timestamp("2012-02-01"),
    )


def test_window_has_data_is_half_open():
    times = pd.DatetimeIndex(["2020-05-11T04:00"]).to_numpy()
    assert not _points.window_has_data(times, "2020-05-11T03:00", "2020-05-11T04:00")
    assert _points.window_has_data(times, "2020-05-11T04:00", "2020-05-11T05:00")
    assert not _points.window_has_data(np.array([]), "2020-05-11T04:00", "2020-05-11T05:00")


def test_clip_to_window_drops_stray_rows():
    ds = opensky.states_to_dataset(make_states_frame(rows=6))
    clipped = _points.clip_to_window(ds, "2020-05-11T00:00", "2020-05-11T00:30")
    assert clipped.sizes["time"] == 3


def test_naive_utc_strips_a_timezone():
    aware = pd.Timestamp("2020-05-11T03:00", tz="UTC")
    assert _points.naive_utc(aware) == pd.Timestamp("2020-05-11T03:00")
    assert _points.naive_utc(pd.Timestamp("2020-05-11T03:00")) == pd.Timestamp("2020-05-11T03:00")


def test_partition_bounds_accepts_a_tz_aware_partition_key():
    """Dagster hands partition windows over as tz-aware UTC timestamps."""
    start, end = _points.partition_bounds(pd.Timestamp("2020-05-11T03:00", tz="UTC"), "h")
    assert (start, end) == (pd.Timestamp("2020-05-11T03:00"), pd.Timestamp("2020-05-11T04:00"))


def test_stored_windows_on_an_empty_store(local_config):
    repo = local_config.icechunk_repo("test/scan-empty.icechunk")
    windows = [_points.partition_bounds("2020-05-11T00:00", "h")]
    assert _points.stored_windows(repo, windows) == [False]


def test_stored_windows_answers_many_windows_in_one_pass(local_config):
    repo = local_config.icechunk_repo("test/scan.icechunk")
    _points.append_point_observations(repo, opensky.states_to_dataset(make_states_frame()))
    windows = [
        _points.partition_bounds(t, "h")
        for t in pd.date_range("2020-05-10T23:00", periods=3, freq="h")
    ]
    assert _points.stored_windows(repo, windows) == [False, True, False]


def test_stored_windows_scans_in_blocks(local_config):
    """A store larger than one block is still answered correctly."""
    repo = local_config.icechunk_repo("test/scan-blocks.icechunk")
    for hour in range(3):
        ds = opensky.states_to_dataset(make_states_frame(start=f"2020-05-11T0{hour}:00"))
        _points.append_point_observations(repo, ds)
    windows = [
        _points.partition_bounds(t, "h")
        for t in pd.date_range("2020-05-11T00:00", periods=4, freq="h")
    ]
    assert _points.stored_windows(repo, windows, block=7) == [True, True, True, False]


def test_stored_windows_returns_empty_for_no_windows(local_config):
    repo = local_config.icechunk_repo("test/scan-none.icechunk")
    assert _points.stored_windows(repo, []) == []


def test_widen_string_vars_pins_the_width():
    ds = xr.Dataset(
        {"tag": ("time", np.array(["a", "bb"], dtype=object))},
        coords={"time": pd.date_range("2020-01-01", periods=2)},
    )
    out = _points.widen_string_vars(ds, width=16)
    assert out["tag"].dtype == np.dtype("<U16")


def test_append_point_observations_creates_then_appends(local_config):
    repo = local_config.icechunk_repo("test/points.icechunk")
    first = opensky.states_to_dataset(make_states_frame(start="2020-05-11T00:00"))
    second = opensky.states_to_dataset(make_states_frame(start="2020-05-11T01:00"))

    assert _points.append_point_observations(repo, first) is True
    assert _points.append_point_observations(repo, second) is True

    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert stored.sizes["time"] == first.sizes["time"] + second.sizes["time"]


def test_append_point_observations_keeps_repeated_timestamps(local_config):
    """The gridded writer would drop these; a point store must not."""
    repo = local_config.icechunk_repo("test/dupes.icechunk")
    frame = make_states_frame()
    ds = opensky.states_to_dataset(frame)
    assert _points.append_point_observations(repo, ds) is True
    assert _points.append_point_observations(repo, ds) is True
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert stored.sizes["time"] == 2 * ds.sizes["time"]


def test_append_point_observations_rejects_a_missing_append_dim(local_config):
    repo = local_config.icechunk_repo("test/nodim.icechunk")
    ds = xr.Dataset({"x": ("row", [1, 2])})
    with pytest.raises(ValueError, match="no 'time' coordinate"):
        _points.append_point_observations(repo, ds)


def test_append_point_observations_skips_an_empty_dataset(local_config):
    repo = local_config.icechunk_repo("test/empty.icechunk")
    ds = opensky.states_to_dataset(make_states_frame()).isel(time=slice(0, 0))
    assert _points.append_point_observations(repo, ds) is False


def test_append_point_observations_refuses_a_variable_mismatch(local_config):
    repo = local_config.icechunk_repo("test/mismatch.icechunk")
    ds = opensky.states_to_dataset(make_states_frame())
    assert _points.append_point_observations(repo, ds) is True
    assert _points.append_point_observations(repo, ds.drop_vars("velocity")) is False


# ------------------------------------------------------------ opensky provider (local)


def test_opensky_provider_missing_timesteps_is_window_based(local_config, tmp_path):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    hours = pd.date_range("2020-05-11T00:00", periods=3, freq="h")
    assert provider.missing_timesteps(hours) == list(hours)

    archive = write_states_archive(tmp_path / "states.csv.tar", make_states_frame())
    processed = provider.process([str(archive)], hours[0])
    provider.write_to_icechunk(provider.get_icechunk_repo(), processed)

    # The hour's samples start at 00:00 but never land exactly on 01:00 or 02:00, so a
    # plain "is this timestamp stored?" check would be wrong here.
    assert provider.missing_timesteps(hours) == list(hours[1:])


def test_opensky_provider_process_clips_and_chunks(local_config, tmp_path):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    archive = write_states_archive(tmp_path / "states.csv.tar", make_states_frame(rows=12))
    ds = provider.process([str(archive)], pd.Timestamp("2020-05-11T00:00"))
    # 12 samples ten minutes apart span two hours; only the first hour belongs here.
    assert ds.sizes["time"] == 6
    assert ds["icao24"].dtype == np.dtype(f"<U{_points.STRING_WIDTH}")


def test_opensky_provider_process_raises_when_content_mismatches_the_hour(local_config, tmp_path):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    archive = write_states_archive(tmp_path / "states.csv.tar", make_states_frame())
    with pytest.raises(ValueError, match="does not match its filename"):
        provider.process([str(archive)], pd.Timestamp("2021-01-01T00:00"))


def test_opensky_provider_fetch_reports_an_unpublished_hour_as_empty(local_config, monkeypatch):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    monkeypatch.setattr(opensky, "download_one", lambda *a, **k: None)
    monkeypatch.setattr(opensky, "_url_exists", lambda url: False)
    assert provider.fetch(pd.Timestamp("2020-05-12T00:00")) == []


def test_url_exists_only_treats_404_as_absent(monkeypatch):
    """A transport error must propagate, not be reported as 'no such archive'."""
    monkeypatch.setattr(opensky.requests, "head", lambda *a, **k: _FakeResponse(404))
    assert opensky._url_exists("https://example.invalid/x") is False

    monkeypatch.setattr(opensky.requests, "head", lambda *a, **k: _FakeResponse(200))
    assert opensky._url_exists("https://example.invalid/x") is True

    monkeypatch.setattr(opensky.requests, "head", lambda *a, **k: _FakeResponse(503))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        opensky._url_exists("https://example.invalid/x")


def test_opensky_provider_fetch_raises_on_a_transient_failure(local_config, monkeypatch):
    """A download that failed while the archive exists must not look like 'no data'."""
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    monkeypatch.setattr(opensky, "download_one", lambda *a, **k: None)
    monkeypatch.setattr(opensky, "_url_exists", lambda url: True)
    with pytest.raises(RuntimeError, match="failed to download"):
        provider.fetch(pd.Timestamp("2020-05-11T00:00"))


def test_opensky_run_partition_round_trip(local_config, tmp_path, monkeypatch):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    archive = write_states_archive(tmp_path / "states.csv.tar", make_states_frame())
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: [str(archive)])

    it = pd.Timestamp("2020-05-11T00:00")
    assert provider.run_partition(it) is True
    # Second run is a no-op because the window is already covered.
    assert provider.run_partition(it) is False

    stored = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store, consolidated=False)
    assert pd.Timestamp(stored.time.values[0]) == it


def test_opensky_extract_trajectories(local_config, tmp_path):
    provider = opensky.OpenSkyStatesProvider(config=local_config)
    archive = write_states_archive(tmp_path / "states.csv.tar", make_states_frame())
    paths = provider.extract_trajectories([str(archive)], label="2020-05-11", out_dir=tmp_path / "traj")
    assert len(paths) == 2


# ------------------------------------------------------------------------------ osmc


def test_osmc_url_matches_the_erddap_encoding():
    url = osmc.osmc_url("2012-01-01", "2012-01-31T23:59:59", platform_type="VOSCLIM")
    assert url.startswith(osmc.ERDDAP_BASE + "?platform_id%2Cplatform_code%2C")
    assert "&platform_type=%22VOSCLIM%22" in url
    assert "&time%3E=2012-01-01T00%3A00%3A00Z" in url
    assert "&time%3C=2012-01-31T23%3A59%3A59Z" in url


def test_osmc_url_without_a_platform_filter():
    url = osmc.osmc_url("2012-01-01", "2012-01-31")
    assert "platform_type=%22" not in url


def test_osmc_url_escapes_parentheses_the_way_erddap_does():
    url = osmc.osmc_url("2012-01-01", "2012-01-31", platform_type="DRIFTING BUOYS (GENERIC)")
    assert "%22DRIFTING%20BUOYS%20(GENERIC)%22" in url


def test_store_prefix_for_rejects_an_unknown_group():
    assert osmc.store_prefix_for("drifters") == "bkr/aoml/aoml_drifters.icechunk"
    with pytest.raises(ValueError, match="unknown OSMC dataset"):
        osmc.store_prefix_for("submarines")


def test_platform_categories_buckets_every_type():
    categories = osmc.platform_categories()
    assert "DRIFTING BUOYS (GENERIC)" in categories["buoys"]
    assert "VOSCLIM" in categories["ships"]
    assert "C-MAN WEATHER STATIONS" in categories["stations"]
    assert "PROFILING FLOATS AND GLIDERS" in categories["gliders"]


def test_rows_to_time_dim_swaps_the_row_dimension():
    out = osmc.rows_to_time_dim(make_erddap_dataset())
    assert out.sizes["time"] == 8
    assert list(out.coords) == ["time"]
    # latitude and friends stay variables: they vary per observation, they are not an index.
    assert {"latitude", "longitude", "platform_id"} <= set(out.data_vars)


def test_rows_to_time_dim_is_idempotent():
    once = osmc.rows_to_time_dim(make_erddap_dataset())
    twice = osmc.rows_to_time_dim(once)
    assert twice.sizes["time"] == once.sizes["time"]
    assert "time" in twice.coords


def test_rows_to_time_dim_needs_a_time_variable():
    with pytest.raises(ValueError, match="no 'time' variable"):
        osmc.rows_to_time_dim(xr.Dataset({"sst": ("row", [1.0, 2.0])}))


def test_osmc_provider_rejects_an_unknown_dataset():
    with pytest.raises(ValueError, match="unknown OSMC dataset"):
        osmc.OSMCProvider("submarines")


def test_osmc_provider_url_ends_one_second_before_the_next_month(local_config):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    url = provider.url_for(pd.Timestamp("2012-01-01"))
    assert "&time%3C=2012-01-31T23%3A59%3A59Z" in url


class _FakeResponse:
    """Minimal stand-in for a streamed ``requests`` response."""

    def __init__(self, status_code, body=b"", text=""):
        self.status_code = status_code
        self._body = body
        self.text = text
        self.closed = False

    def iter_content(self, chunk_size=1):
        yield self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def close(self):
        self.closed = True


def test_osmc_fetch_treats_no_matching_results_as_an_empty_partition(local_config, monkeypatch, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    body = 'Error {\n code=404;\n message="Not Found: Your query produced no matching results. (nRows = 0)";\n}'
    monkeypatch.setattr(
        osmc.requests, "get", lambda *a, **k: _FakeResponse(404, text=body)
    )
    assert provider.fetch(pd.Timestamp("2012-01-01"), temp_dir=tmp_path) == []


def test_osmc_fetch_raises_on_a_server_error(local_config, monkeypatch, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    monkeypatch.setattr(osmc.requests, "get", lambda *a, **k: _FakeResponse(500, text="boom"))
    with pytest.raises(RuntimeError, match="HTTP 500"):
        provider.fetch(pd.Timestamp("2012-01-01"), temp_dir=tmp_path)


def test_osmc_fetch_raises_on_an_empty_body(local_config, monkeypatch, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    monkeypatch.setattr(osmc.requests, "get", lambda *a, **k: _FakeResponse(200, body=b""))
    with pytest.raises(RuntimeError, match="empty body"):
        provider.fetch(pd.Timestamp("2012-01-01"), temp_dir=tmp_path)
    assert not list(tmp_path.glob("*.part"))


def test_osmc_fetch_writes_the_payload(local_config, monkeypatch, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    payload = (tmp_path / "src.nc")
    make_erddap_dataset().to_netcdf(payload)
    monkeypatch.setattr(
        osmc.requests, "get", lambda *a, **k: _FakeResponse(200, body=payload.read_bytes())
    )
    files = provider.fetch(pd.Timestamp("2012-01-01"), temp_dir=tmp_path)
    assert len(files) == 1
    assert xr.open_dataset(files[0]).sizes["row"] == 8
    assert not list(tmp_path.glob("*.part"))


def test_osmc_provider_process_and_write(local_config, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    path = tmp_path / "osmc.nc"
    make_erddap_dataset().to_netcdf(path)

    it = pd.Timestamp("2012-01-01")
    ds = provider.process([str(path)], it)
    assert ds.sizes["time"] == 8
    assert ds["platform_type"].dtype == np.dtype(f"<U{_points.STRING_WIDTH}")

    assert provider.write_to_icechunk(provider.get_icechunk_repo(), ds) is True
    assert provider.missing_timesteps(pd.DatetimeIndex(["2012-01-01"])) == []
    assert provider.missing_timesteps(pd.DatetimeIndex(["2012-02-01"])) == [pd.Timestamp("2012-02-01")]


def test_osmc_provider_process_clips_to_the_month(local_config, tmp_path):
    provider = osmc.OSMCProvider("drifters", config=local_config)
    path = tmp_path / "osmc.nc"
    # Eight three-hourly samples starting on the last day of January spill into February.
    make_erddap_dataset(start="2012-01-31T12:00").to_netcdf(path)
    ds = provider.process([str(path)], pd.Timestamp("2012-01-01"))
    assert ds.sizes["time"] == 4
    assert pd.Timestamp(ds.time.values[-1]) < pd.Timestamp("2012-02-01")


def test_reorganise_by_platform_id():
    flat = osmc.rows_to_time_dim(make_erddap_dataset())
    out = osmc.reorganise_by_platform_id(flat)
    assert out.sizes["platform_id"] == 2
    assert out.sizes["time"] == 8
    assert "sst" in out.data_vars


def test_reorganise_by_platform_id_keeps_type_tied_to_the_platform():
    """platform_type must follow platform_id, not become a dimension of its own."""
    raw = make_erddap_dataset()
    types = np.array(["DRIFTING BUOYS (GENERIC)", "SHIPS (GENERIC)"] * 4, dtype=object)
    raw["platform_type"] = ("row", types)
    out = osmc.reorganise_by_platform_id(osmc.rows_to_time_dim(raw))
    assert "platform_type" not in out.dims
    assert out["platform_type"].dims == ("platform_id",)
    assert out.sizes["platform_id"] == 2
    assert out["sst"].dims == ("platform_id", "time")


def test_reorganise_by_platform_id_needs_the_variable():
    flat = osmc.rows_to_time_dim(make_erddap_dataset()).drop_vars("platform_id")
    with pytest.raises(ValueError, match="no 'platform_id'"):
        osmc.reorganise_by_platform_id(flat)


# ----------------------------------------------------------------------- eurocontrol


def _write_eurocontrol_month(base: pathlib.Path, year_month: str) -> None:
    import gzip as _gzip

    folder = base / year_month
    folder.mkdir(parents=True)
    flights = pd.DataFrame(
        {
            "ECTRL ID": [1, 2],
            "AC Operator": ["BAW", "EZY"],
            "AC Type": ["A320", "A319"],
            "AC Registration": ["G-AAA", "G-BBB"],
            "ICAO Flight Type": ["S", "S"],
            "STATFOR Market Segment": ["Traditional Scheduled"] * 2,
            "Unused": [0, 0],
        }
    )
    points = pd.DataFrame(
        {"ECTRL ID": [1, 1, 2], "Latitude": [50.0, 51.0, 48.0], "Longitude": [0.0, 1.0, 2.0]}
    )
    end = f"{year_month}28"
    for stem, frame in (
        ("Flights", flights),
        ("Flight_Points_Actual", points),
        ("Flight_Points_Filed", points),
    ):
        path = folder / f"{stem}_{year_month}01_{end}.csv.gz"
        path.write_bytes(_gzip.compress(frame.to_csv(index=False).encode()))


def test_month_files_finds_the_three_csvs(tmp_path):
    _write_eurocontrol_month(tmp_path, "202312")
    found = eurocontrol.month_files(tmp_path, "202312")
    assert sorted(found) == ["filed", "flights", "flown"]


def test_month_files_on_a_missing_month(tmp_path):
    assert eurocontrol.month_files(tmp_path, "202312") == {}


def test_combine_flight_points_joins_metadata(tmp_path):
    pytest.importorskip("polars")
    _write_eurocontrol_month(tmp_path, "202312")
    written = eurocontrol.combine_flight_points(["202312", "202401"], base_dir=tmp_path, out_dir=tmp_path)
    assert len(written) == 2
    import polars as pl

    filed = pl.read_parquet(tmp_path / "eurocontrol_flight_points_filed.parquet")
    assert filed.height == 3
    assert "AC Type" in filed.columns
    assert "Unused" not in filed.columns


def test_combine_parquets_streams_several_files(tmp_path):
    pl = pytest.importorskip("polars")
    paths = []
    for i in range(3):
        path = tmp_path / f"part-{i}.parquet"
        pl.DataFrame({"icao24": ["abc"], "lat": [float(i)]}).write_parquet(path)
        paths.append(path)
    out = eurocontrol.combine_parquets(paths, tmp_path / "combined.parquet")
    assert pl.read_parquet(out).height == 3


def test_combine_parquets_rejects_an_empty_list(tmp_path):
    pytest.importorskip("polars")
    with pytest.raises(ValueError, match="no parquet files"):
        eurocontrol.combine_parquets([], tmp_path / "out.parquet")


# --------------------------------------------------------------------- dagster assets


def test_dagster_assets_build_a_definitions_object():
    import dagster as dg

    from dags.assets import flight_marine

    assets = [flight_marine.opensky_states_asset, *flight_marine.osmc_assets]
    defs = dg.Definitions(assets=assets)
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "opensky_states" in keys
    assert "osmc_drifters" in keys
    assert len(keys) == 1 + len(osmc.PLATFORM_TYPES)


def test_opensky_partitions_cover_the_sample_set_and_stop_there():
    from dags.assets import flight_marine

    keys = flight_marine.opensky_partitions.get_partition_keys()
    assert pd.Timestamp(keys[0]) == opensky.SAMPLE_START
    assert {pd.Timestamp(k).dayofweek for k in keys} == {0}
    # The sample set is closed; no partition may fall past its last Monday.
    assert pd.Timestamp(keys[-1]) == opensky.SAMPLE_END


def test_osmc_partitions_reach_the_last_complete_month():
    from dags.assets import flight_marine

    keys = flight_marine.osmc_partitions.get_partition_keys()
    last = pd.Timestamp(keys[-1])
    now = pd.Timestamp.now().normalize().replace(day=1)
    # Only the in-progress month is excluded, not the one before it as well.
    assert last == now - pd.offsets.MonthBegin(1)
