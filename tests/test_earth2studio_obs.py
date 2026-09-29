"""Tests for the earth2studio observation pipeline: registry, downloader, publishers, assets.

earth2studio is only installed in the downloader image, so its data sources are replaced
by small fakes with the same call signature, output shape and ``SCHEMA`` attribute.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import pathlib
import sys

import dagster as dg
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xarray as xr

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from planetary_datasets.common.parquet import ParquetSink  # noqa: E402
from planetary_datasets.providers import earth2studio_download as dl  # noqa: E402
from planetary_datasets.providers import earth2studio_obs as obs  # noqa: E402

#: Every source listed under "Direct Observations" in the earth2studio catalog, less OPERA
#: (which has its own provider).
CATALOG_OBSERVATION_SOURCES = {
    "GHCNDaily", "GHCNHourly", "GOES", "GOESGLM", "GOESGLMGrid", "HimawariAHI", "IBTrACS",
    "IEM_ASOS", "ISD", "JPSS", "JPSS_ATMS", "JPSS_CRIS", "MRMS", "MeteosatFCI", "MeteosatLI",
    "MetOpAMSUA", "MetOpAVHRR", "MetOpIASI", "MetOpMHS", "NClimGridDaily", "NNJAObsConv",
    "NNJAObsSat", "NNJAObsSatwnd", "NomadsGDASObsConv", "PlanetaryComputerGOES",
    "PlanetaryComputerMODISFire", "PlanetaryComputerOISST", "PlanetaryComputerSentinel3AOD",
    "UFSObsConv", "UFSObsSat",
}  # fmt: skip

TABLE_SCHEMA = pa.schema(
    [
        pa.field("time", pa.timestamp("ns")),
        pa.field("lat", pa.float32()),
        pa.field("lon", pa.float32()),
        pa.field("station", pa.string()),
        pa.field("observation", pa.float32()),
        pa.field("variable", pa.string()),
    ]
)


class FakeTable:
    """A DataFrame source: one row per 15 minutes, including rows outside the window."""

    SCHEMA = TABLE_SCHEMA
    instances: list["FakeTable"] = []

    def __init__(self, time_tolerance, cache=True, verbose=True, async_timeout=600, **kwargs):
        self.tolerance = time_tolerance
        self.kwargs = kwargs
        FakeTable.instances.append(self)

    def __call__(self, time, variable):
        (start,) = time
        lower, upper = self.tolerance
        times = pd.date_range(start + lower - pd.Timedelta("15min"), start + upper, freq="15min")
        stations = self.kwargs.get("stations") or ["A"]
        rows = [
            {"time": t, "lat": 1.0, "lon": 2.0, "station": s, "observation": 3.0, "variable": v}
            for t in times
            for s in stations
            for v in variable
        ]
        df = pd.DataFrame(rows)
        df["station"] = df["station"].astype("category")
        return df


class EmptyTable(FakeTable):
    def __call__(self, time, variable):
        return pd.DataFrame(columns=self.SCHEMA.names)


class FakeGrid:
    """A fixed-grid source returning ``[time, variable, y, x]`` with 2-D ``_lat``/``_lon``."""

    fail_at: set = set()
    calls: list = []

    def __init__(self, cache=True, verbose=True, async_timeout=600, **kwargs):
        self.kwargs = kwargs

    def __call__(self, time, variable):
        FakeGrid.calls.append(list(time))
        if any(t in self.fail_at for t in time):
            raise FileNotFoundError(f"no scan at {time}")
        data = np.stack([np.full((len(variable), 4, 3), t.minute, dtype="float64") for t in time])
        lat, lon = np.meshgrid(np.linspace(10, 20, 4), np.linspace(-5, 5, 3), indexing="ij")
        return xr.DataArray(
            data,
            dims=["time", "variable", "y", "x"],
            coords={
                "time": list(time),
                "variable": list(variable),
                "y": np.arange(4.0),
                "x": np.arange(3.0),
            },
        ).assign_coords(_lat=(("y", "x"), lat), _lon=(("y", "x"), lon))


class FakeTiles(FakeGrid):
    """A tiled Planetary Computer-like source: one flaky tile, one never published."""

    instances = 0

    def __init__(self, tile, **kwargs):
        super().__init__(**kwargs)
        # The tiler swaps this filter between tiles instead of building new instances.
        self._search_kwargs = {"filter": {"op": "iLike", "args": [{}, f"%{tile}%"]}}
        FakeTiles.instances += 1

    @property
    def tile(self) -> str:
        return self._search_kwargs["filter"]["args"][1].strip("%")

    def __call__(self, time, variable):
        if self.tile.endswith("h00v09"):
            raise OSError("connection reset")
        if self.tile.endswith("h00v10"):
            raise FileNotFoundError("No Planetary Computer item found")
        return super().__call__(time, variable).drop_vars(["_lat", "_lon"])


def table_dataset(**overrides) -> dl.Dataset:
    base = dict(
        name="fake_table",
        source="FakeTable",
        kind="table",
        freq="1h",
        start="2020-01-01",
        variables=("t2m", "tp"),
        description="fake",
        lead="30min",
    )
    base.update(overrides)
    return dl.Dataset(**base)


def grid_dataset(**overrides) -> dl.Dataset:
    base = dict(
        name="fake_grid",
        source="FakeGrid",
        kind="grid",
        freq="1h",
        start="2020-01-01",
        variables=("b1", "b2"),
        description="fake",
        step="10min",
        frames_per_call=2,
    )
    base.update(overrides)
    return dl.Dataset(**base)


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """Stores and staging under tmp_path, with a generous memory budget."""
    from planetary_datasets import config as config_module

    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MEMORY_CEILING_GB", "64")
    monkeypatch.delenv(obs.ARCHIVE_ENV, raising=False)
    for name in ("EUMETSAT_CONSUMER_KEY", "EUMETSAT_CONSUMER_SECRET"):
        monkeypatch.delenv(name, raising=False)
    config_module.reset_config_cache()
    FakeTable.instances.clear()
    FakeGrid.calls.clear()
    FakeGrid.fail_at = set()
    yield tmp_path
    config_module.reset_config_cache()


@pytest.fixture
def registered(monkeypatch):
    """Register extra datasets for the duration of a test."""

    def add(*datasets: dl.Dataset):
        for dataset in datasets:
            monkeypatch.setitem(dl.DATASETS, dataset.name, dataset)

    return add


def staging(tmp_path) -> pathlib.Path:
    return tmp_path / "data" / "earth2studio"


HOUR = dt.datetime(2020, 1, 1, 6)


# --- registry ---------------------------------------------------------------------------


def test_every_catalog_observation_source_has_a_dataset():
    assert set(dl.SOURCES) == CATALOG_OBSERVATION_SOURCES


def test_dataset_definitions_are_consistent():
    from dags.assets.earth2studio_obs import PARTITIONINGS

    for dataset in dl.DATASETS.values():
        assert dataset.kind in {"table", "grid", "granules"}, dataset.name
        assert dataset.freq in PARTITIONINGS, dataset.name
        dl.partition_window(dataset, dt.datetime.fromisoformat(dataset.start))
        if dataset.kind == "grid":
            assert dataset.step, dataset.name
        if dataset.kind == "granules":
            assert dataset.source in dl.GRANULE_LISTERS, dataset.name
        eumetsat = dataset.source.startswith(("MetOp", "Meteosat"))
        assert (dataset.credentials == dl.EUMETSAT) == eumetsat, dataset.name


def test_the_modis_tile_list_is_the_published_one():
    tiles = dl.MODIS_TILES
    assert len(tiles) == len(set(tiles)) == 294
    assert tiles == tuple(sorted(tiles))
    # Spot checks against the STAC listing: present, and never published.
    assert {"h08v11", "h14v00", "h35v10"} <= set(tiles)
    assert not {"h17v15", "h23v16"} & set(tiles)


def test_every_store_goes_under_obs_without_clashing_with_an_existing_one():
    assert obs.provider_for("iem_asos").store_prefix == "bkr/obs/iem_asos.parquet"
    assert obs.provider_for("mrms_conus").store_prefix == "bkr/obs/mrms_conus.icechunk"

    # bkr/obs already holds the native observation stores; no prefix may be reused.
    import re

    source = "\n".join(
        path.read_text()
        for path in (REPO_ROOT / "planetary_datasets").rglob("*.py")
        if path.name not in {"earth2studio_obs.py", "parquet.py"}
    )
    existing = set(re.findall(r"[\"'](bkr/[\w./-]+\.(?:icechunk|parquet|zarr))[\"']", source))
    ours = {obs.provider_for(name).store_prefix for name in dl.DATASETS}
    assert len(ours) == len(dl.DATASETS)
    assert not ours & existing
    stem = lambda prefix: prefix.rsplit("/", 1)[-1].split(".")[0]  # noqa: E731
    obs_stems = {stem(p) for p in existing if p.startswith("bkr/obs/")}
    assert not {stem(p) for p in ours} & obs_stems, "a name shared with a native store misleads"


def test_satellite_changeovers_apply_from_their_date():
    east = dl.DATASETS["goes_east_conus"]
    assert east.kwargs_for(dt.datetime(2025, 4, 6, 23))["satellite"] == "goes16"
    assert east.kwargs_for(dt.datetime(2025, 4, 7))["satellite"] == "goes19"
    assert dl.DATASETS["goes_west_fd"].kwargs_for(dt.datetime(2024, 1, 1))["satellite"] == "goes18"


def test_viirs_requests_do_not_ask_for_geolocation():
    for name in ("jpss_viirs_noaa20_i", "jpss_viirs_snpp_m"):
        assert not any(v.startswith("_") for v in dl.DATASETS[name].variables)


def test_partition_windows_reject_a_time_off_the_partitioning():
    with pytest.raises(ValueError, match="not the start"):
        dl.partition_window(dl.DATASETS["nnja_conv"], dt.datetime(2020, 1, 1, 3))
    start, end = dl.partition_window(dl.DATASETS["ghcn_daily"], dt.datetime(2020, 2, 1))
    assert end == dt.datetime(2020, 3, 1)


def test_viirs_granule_times_come_from_the_filename():
    path = "VIIRS-I1-SDR/2026/09/20/SVI01_j01_d20260920_t0001033_e0002261_b45792_c2026.h5"
    assert dl.viirs_granule_time(path) == dt.datetime(2026, 9, 20, 0, 1, 3, 300000)
    assert dl.viirs_granule_time("unrelated.h5") is None


# --- tables -----------------------------------------------------------------------------


def test_a_table_partition_is_widened_then_cut_to_its_window(local_config, registered):
    dataset = table_dataset()
    registered(dataset)
    summary = dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)

    (source,) = FakeTable.instances
    assert source.tolerance == (-dt.timedelta(minutes=30), dt.timedelta(hours=1))
    table = pq.read_table(
        staging(local_config) / "fake_table/2020/01/01/fake_table_202001010600.parquet"
    )
    times = pd.DatetimeIndex(table.column("time").to_pandas())
    assert times.min() == pd.Timestamp(HOUR)
    assert times.max() < pd.Timestamp(HOUR) + pd.Timedelta("1h")
    assert summary["rows"] == table.num_rows == 4 * 2
    assert table.schema.field("station").type == pa.string(), "categoricals are cast to the schema"


def test_an_empty_result_is_still_a_typed_partition(local_config, registered):
    dataset = table_dataset(name="fake_empty")
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=EmptyTable)
    table = pq.read_table(next(staging(local_config).rglob("*.parquet")))
    assert table.num_rows == 0
    assert table.schema.field("time").type == pa.timestamp("ns")


def test_stations_are_enumerated_and_chunked(local_config, registered, monkeypatch):
    dataset = table_dataset(
        name="fake_stations", stations="ghcnd", station_chunk=2, freq="1D", lead="0s"
    )
    registered(dataset)
    monkeypatch.setattr(dl, "list_stations", lambda network, start, end: ["S1", "S2", "S3"])
    summary = dl.download(
        dataset.name, dt.datetime(2020, 1, 1), staging(local_config), factory=FakeTable
    )

    assert [s.kwargs["stations"] for s in FakeTable.instances] == [["S1", "S2"], ["S3"]]
    assert summary["rows"] == 96 * 3 * 2


def test_credentials_are_required_before_anything_is_fetched(local_config, registered):
    dataset = table_dataset(name="fake_secure", credentials=dl.EUMETSAT)
    registered(dataset)
    with pytest.raises(RuntimeError, match="EUMETSAT_CONSUMER_KEY"):
        dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    assert FakeTable.instances == []


def test_a_table_partition_is_published_once_and_its_staging_cleared(local_config, registered):
    dataset = table_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    publisher = obs.provider_for(dataset.name)

    assert publisher.discard_staged(HOUR) == [], "nothing is deleted before it is published"
    assert publisher.run_partition(HOUR)
    assert not publisher.run_partition(HOUR), "a published partition is skipped"
    assert publisher.partition_stored(HOUR)
    assert len(publisher.discard_staged(HOUR)) == 2
    assert not any(staging(local_config).rglob("*.*"))

    published = pd.read_parquet(publisher.store_path)
    assert len(published) == 8
    assert publisher.sink.partitions()[0].endswith("date=2020-01-01/part-202001010600.parquet")


def test_a_table_partition_is_published_byte_for_byte(local_config, registered):
    dataset = table_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    staged = next(staging(local_config).rglob("*.parquet")).read_bytes()
    publisher = obs.provider_for(dataset.name)
    assert publisher.run_partition(HOUR)
    assert pathlib.Path(publisher.sink.path(pd.Timestamp(HOUR))).read_bytes() == staged


def test_time_bounds_come_from_the_parquet_footer(tmp_path):
    path = tmp_path / "x.parquet"
    times = pd.date_range("2020-01-01", periods=5, freq="1h")
    pq.write_table(pa.table({"time": times, "v": range(5)}), path, row_group_size=2)
    assert obs.time_bounds(path) == (times[0], times[-1])
    pq.write_table(pa.table({"time": pa.array([], pa.timestamp("ns"))}), path)
    assert obs.time_bounds(path) == (None, None)


def test_parquet_and_icechunk_share_one_s3_configuration(monkeypatch):
    from planetary_datasets import config as config_module

    monkeypatch.setenv("ICECHUNK_ENDPOINT_URL", "https://data.source.coop")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    cfg = config_module.load_config(env_file="/nonexistent.env")
    options = cfg.fsspec_storage_options()
    assert options["endpoint_url"] == "https://data.source.coop"
    # The icechunk side forces path-style for a custom endpoint; so must this.
    assert options["config_kwargs"] == {"s3": {"addressing_style": "path"}}
    assert (options["key"], options["secret"]) == ("k", "s")


def test_a_staged_file_with_rows_outside_its_window_is_refused(local_config, registered):
    dataset = table_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    path = next(staging(local_config).rglob("*.parquet"))
    table = pq.read_table(path)
    shifted = table.set_column(
        0, "time", pa.array(pd.DatetimeIndex(table.column("time").to_pandas()) - pd.Timedelta("2h"))
    )
    pq.write_table(shifted, path)
    with pytest.raises(obs.StagedPartitionError, match="outside"):
        obs.provider_for(dataset.name).run_partition(HOUR)


def test_nothing_is_published_without_a_manifest(local_config, registered):
    dataset = table_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    next(staging(local_config).rglob("*.json")).unlink()
    assert not obs.provider_for(dataset.name).run_partition(HOUR)


# --- grids ------------------------------------------------------------------------------


def test_a_grid_partition_is_staged_in_batches(local_config, registered):
    dataset = grid_dataset()
    registered(dataset)
    summary = dl.download(dataset.name, HOUR, staging(local_config), factory=FakeGrid)

    assert [len(c) for c in FakeGrid.calls] == [2, 2, 2]
    assert len(summary["files"]) == 3 and summary["missing_frames"] == []
    with xr.open_dataset(
        staging(local_config) / "fake_grid/2020/01/01" / summary["files"][0]
    ) as ds:
        assert set(ds.data_vars) == {"b1", "b2"}
        assert ds["b1"].dtype == np.float32
        assert ds["latitude"].dims == ("y", "x") and "_lat" not in ds.variables


def test_a_failed_frame_fails_the_partition_unless_partial_is_allowed(local_config, registered):
    dataset = grid_dataset()
    registered(dataset)
    FakeGrid.fail_at = {HOUR + dt.timedelta(minutes=20)}
    with pytest.raises(dl.IncompletePartition, match="06:20"):
        dl.download(dataset.name, HOUR, staging(local_config), factory=FakeGrid)
    assert not any(staging(local_config).rglob("*.*")), "nothing is left staged"

    summary = dl.download(
        dataset.name, HOUR, staging(local_config), allow_partial=True, factory=FakeGrid
    )
    assert len(summary["files"]) == 2
    assert summary["missing_frames"] == ["2020-01-01T06:20:00", "2020-01-01T06:30:00"]


def test_grid_partitions_append_frame_batches_and_keep_static_geolocation_once(
    local_config, registered
):
    dataset = grid_dataset()
    registered(dataset)
    publisher = obs.provider_for(dataset.name)
    second = HOUR + dt.timedelta(hours=1)
    for hour in (HOUR, second):
        dl.download(dataset.name, hour, staging(local_config), factory=FakeGrid)
        assert publisher.run_partition(hour)
        assert publisher.discard_staged(hour)

    store = xr.open_zarr(publisher.get_icechunk_repo().readonly_session("main").store)
    assert store.sizes == {"time": 12, "y": 4, "x": 3}
    assert pd.DatetimeIndex(store["time"].values).is_monotonic_increasing
    assert store["latitude"].dims == ("y", "x")
    assert not publisher.run_partition(HOUR), "a stored partition is skipped"
    assert not publisher.appendable(HOUR), "and one before the store's end cannot be filled"
    assert publisher.appendable(second + dt.timedelta(hours=1))


def test_a_missing_tile_fails_the_day_unless_partial_is_allowed(
    local_config, registered, monkeypatch
):
    dataset = grid_dataset(
        name="fake_tiles",
        source="FakeTiles",
        freq="1D",
        step="1D",
        variables=("fmask",),
        tiles=("h00v08", "h00v09", "h00v10"),
        platform="MOD14A1",
        dtype="uint8",
        frames_per_call=0,
    )
    registered(dataset)
    monkeypatch.setattr(dl._TiledSource, "attempts", 1)
    monkeypatch.setattr(FakeTiles, "instances", 0)
    day = dt.datetime(2020, 1, 1)
    with pytest.raises(dl.IncompletePartition, match="h00v09"):
        dl.download(dataset.name, day, staging(local_config), factory=FakeTiles)
    assert not any(staging(local_config).rglob("*.*"))

    summary = dl.download(
        dataset.name, day, staging(local_config), allow_partial=True, factory=FakeTiles
    )
    assert summary["missing_tiles"] == ["h00v09"]
    assert FakeTiles.instances == 2, "one source instance per run, not one per tile"
    assert summary["absent_tiles"] == ["h00v10"], "an unpublished tile is not a failure"
    with xr.open_dataset(next(staging(local_config).rglob("*.nc"))) as ds:
        assert ds["fmask"].dims == ("time", "tile", "y", "x")
        assert ds["fmask"].dtype == np.uint8
        assert list(ds["tile"].values) == ["h00v08", "h00v09", "h00v10"]
        assert int(ds["fmask"].sel(tile="h00v09").max()) == 0


def test_sentinel3_granules_are_selected_by_nearest_nominal_time():
    @dataclasses.dataclass
    class Item:
        datetime: dt.datetime

    class Base:
        pass

    exact = dl._granule_source(dataclasses.replace(dl.DATASETS["pc_sentinel3_aod"]), Base)
    assert exact.SEARCH_TOLERANCE == dt.timedelta(seconds=5)
    when = dt.datetime(2026, 9, 20, 1, 35, 2)
    items = [
        Item(dt.datetime(2026, 9, 20, 0, 56, tzinfo=dt.timezone.utc)),
        Item(dt.datetime(2026, 9, 20, 1, 35, 2, 555714, tzinfo=dt.timezone.utc)),
    ]
    assert exact._select_item(exact(), items, when) is items[1]
    aware = when.replace(tzinfo=dt.timezone.utc)
    assert exact._select_item(exact(), items, aware) is items[1], "earth2studio passes tz-aware"
    assert dl._granule_source(dl.DATASETS["jpss_viirs_noaa20_i"], Base) is Base


def test_modis_items_are_pinned_to_one_platform_and_the_covering_period():
    @dataclasses.dataclass
    class Item:
        id: str
        properties: dict

    class Base:
        pass

    terra = dl._platform_source(Base, "MOD14A1")
    period = lambda start, end: {"start_datetime": start, "end_datetime": end}  # noqa: E731
    items = [
        Item("MYD14A1.A2023161.h35v10.061", period("2023-06-10T00:00:00Z", "2023-06-17T23:59:59Z")),
        Item("MOD14A1.A2023169.h35v10.061", period("2023-06-18T00:00:00Z", "2023-06-25T23:59:59Z")),
        Item("MOD14A1.A2023161.h35v10.061", period("2023-06-10T00:00:00Z", "2023-06-17T23:59:59Z")),
    ]
    when = dt.datetime(2023, 6, 17, tzinfo=dt.timezone.utc)
    assert terra._select_item(terra(), items, when) is items[2]
    with pytest.raises(FileNotFoundError, match="MOD14A1"):
        terra._select_item(terra(), items[:1], when)


def test_sentinel_values_below_the_valid_minimum_become_nan():
    array = FakeGrid()([HOUR], ["refc"])
    array[0, 0, 0, 0] = -999.0
    array[0, 0, 0, 1] = -99.0
    ds = dl.to_dataset(array, valid_min=-90.0)
    assert int(ds["refc"].isnull().sum()) == 2
    assert float(ds["refc"].max()) == 0.0


def test_pipes_accepts_the_summaries():
    from dagster_pipes import _normalize_param_metadata

    summary = {"files": [], "missing_frames": [], "missing_tiles": [], "rows": 0, "start": "x"}
    _normalize_param_metadata(
        dl.pipes_metadata(summary), "report_asset_materialization", "metadata"
    )


def test_the_parquet_sink_layout(local_config):
    sink = ParquetSink("bkr/obs/x.parquet")
    it = pd.Timestamp("2026-09-29T06:00")
    assert sink.key(it) == "date=2026-09-29/part-202609290600.parquet"
    assert not sink.exists(it)
    sink.write(it, pd.DataFrame({"a": [1, 2]}))
    assert sink.exists(it)
    assert not list(pathlib.Path(sink.root).rglob("*.part"))


# --- Dagster assets ---------------------------------------------------------------------


class FakeInvocation:
    def get_materialize_result(self):
        return dg.MaterializeResult(metadata={"fake": True})


class FakeDockerClient:
    def __init__(self):
        self.calls: list[dict] = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return FakeInvocation()


def test_the_download_asset_runs_the_image_with_only_the_credentials_it_needs(
    local_config, monkeypatch
):
    from dags.assets import earth2studio_obs as assets
    from planetary_datasets import config as config_module

    monkeypatch.setenv("EUMETSAT_CONSUMER_KEY", "key")
    monkeypatch.setenv("EUMETSAT_CONSUMER_SECRET", "secret")
    config_module.reset_config_cache()
    dataset = dl.DATASETS["metop_amsua"]
    client = FakeDockerClient()
    result = dg.materialize(
        [assets.build_download_asset(dataset)],
        partition_key="2026-09-01",
        resources={"pipes_docker_client": client},
    )

    assert result.success
    (call,) = client.calls
    assert call["command"] == [
        "obs", "--dataset", "metop_amsua", "--time", "2026-09-01T00:00", "--target", "/data/earth2studio",
    ]  # fmt: skip
    assert call["env"]["EUMETSAT_CONSUMER_KEY"] == "key"
    assert "EARTH2STUDIO_CACHE" in call["env"]
    root = str((local_config / "data" / "earth2studio").resolve())
    assert call["container_kwargs"]["volumes"] == {
        root: {"bind": "/data/earth2studio", "mode": "rw"}
    }

    iem = FakeDockerClient()
    dg.materialize(
        [assets.build_download_asset(dl.DATASETS["iem_asos"])],
        partition_key="2026-09-01",
        resources={"pipes_docker_client": iem},
    )
    assert "EUMETSAT_CONSUMER_KEY" not in iem.calls[0]["env"]


def test_the_download_asset_fails_fast_without_credentials(local_config):
    from dags.assets import earth2studio_obs as assets

    client = FakeDockerClient()
    result = dg.materialize(
        [assets.build_download_asset(dl.DATASETS["metop_mhs"])],
        partition_key="2026-09-01",
        resources={"pipes_docker_client": client},
        raise_on_error=False,
    )
    assert not result.success
    assert client.calls == []


def test_the_download_asset_skips_a_published_partition(local_config, registered):
    from dags.assets import earth2studio_obs as assets

    dataset = table_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    assert obs.provider_for(dataset.name).run_partition(HOUR)

    client = FakeDockerClient()
    result = dg.materialize(
        [assets.build_download_asset(dataset)],
        partition_key="2020-01-01-06:00",
        resources={"pipes_docker_client": client},
    )
    assert result.success and client.calls == []


def test_the_publish_asset_fails_when_nothing_was_staged(local_config, registered):
    from dags.assets import earth2studio_obs as assets

    dataset = table_dataset()
    registered(dataset)
    download = assets.build_download_asset(dataset)
    publish = assets.build_publish_asset(dataset, download)
    result = dg.materialize([publish], partition_key="2020-01-01-06:00", raise_on_error=False)
    assert not result.success

    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeTable)
    result = dg.materialize([publish], partition_key="2020-01-01-06:00")
    (materialization,) = result.asset_materializations_for_node(dataset.name)
    assert materialization.metadata["written"].value is True
    assert materialization.metadata["staged_files_removed"].value == 2


def test_manual_datasets_are_left_out_of_the_scheduled_jobs():
    from dags import loader
    from dags.assets import earth2studio_obs as assets

    loaded, failures = loader.load_assets([assets])
    assert not failures
    jobs, schedules = loader.build_memory_class_jobs(loaded)
    scheduled = {
        key.path[-1]
        for job in jobs
        for key in job.selection.resolve(
            dg.Definitions(assets=loaded, resources=loader.build_resources()).resolve_asset_graph()
        )
    }
    for dataset in dl.DATASETS.values():
        assert (dataset.name in scheduled) == dataset.scheduled, dataset.name
    graph = {key: set(a.asset_deps[key]) for a in loaded for key in a.keys}
    assert graph[dg.AssetKey(["earth2studio_obs", "iem_asos"])] == {
        dg.AssetKey(["earth2studio_obs", "iem_asos_download"])
    }


def test_the_manifest_is_the_last_thing_written(local_config, registered):
    dataset = grid_dataset()
    registered(dataset)
    dl.download(dataset.name, HOUR, staging(local_config), factory=FakeGrid)
    manifest = dl.manifest_path(staging(local_config), dataset, HOUR)
    listed = json.loads(manifest.read_text())["files"]
    newest = max(manifest.parent.iterdir(), key=lambda p: p.stat().st_mtime_ns)
    assert newest == manifest and len(listed) == 3
