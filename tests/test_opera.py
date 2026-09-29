"""Tests for the OPERA downloader, the providers that read what it stages, and the assets.

``earth2studio`` is only installed in the downloader image, so its OPERA source is
replaced by a fake that returns arrays of the same shape and coordinates.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re
import sys

import dagster as dg
import numpy as np
import pandas as pd
import pytest
import xarray as xr

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from planetary_datasets.providers import opera, opera_download  # noqa: E402
from planetary_datasets.providers.opera_download import (  # noqa: E402
    PRODUCTS,
    IncompleteHour,
    download_opera,
    staged_path,
)

HOUR = dt.datetime(2026, 9, 29, 6)
NY, NX = 6, 5


class FakeOPERA:
    """Mimics ``earth2studio.data.OPERA.__call__``, which fails whole if any frame does."""

    def __init__(self, missing: set[dt.datetime] = frozenset()):
        self.missing = set(missing)
        self.calls: list[tuple[list[dt.datetime], list[str]]] = []

    def __call__(self, time, variable):
        self.calls.append((list(time), list(variable)))
        absent = [t for t in time if t in self.missing]
        if absent:
            raise FileNotFoundError(f"no composite at {absent[0]}")
        data = np.stack(
            [
                np.stack(
                    [np.full((NY, NX), t.minute + i, dtype="float32") for i in range(len(variable))]
                )
                for t in time
            ]
        )
        lat, lon = np.meshgrid(np.linspace(70, 32, NY), np.linspace(-30, 62, NX), indexing="ij")
        return xr.DataArray(
            data,
            dims=["time", "variable", "y", "x"],
            coords={
                "time": list(time),
                "variable": list(variable),
                "y": np.arange(NY),
                "x": np.arange(NX),
            },
        ).assign_coords(
            _lat=(("y", "x"), lat.astype("float32")), _lon=(("y", "x"), lon.astype("float32"))
        )


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """Stores and staging under tmp_path, with a generous memory budget."""
    from planetary_datasets import config as config_module

    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MEMORY_CEILING_GB", "64")
    monkeypatch.delenv(opera.ARCHIVE_ENV, raising=False)
    config_module.reset_config_cache()
    yield tmp_path
    config_module.reset_config_cache()


def stage(tmp_path: pathlib.Path, product: str, hour: dt.datetime, **kwargs) -> dict:
    root = tmp_path / "data" / "opera"
    return download_opera(product, hour, root, source=FakeOPERA(), **kwargs)


# --- products ---------------------------------------------------------------------------


def test_products_match_the_original_scripts():
    rainfall, dbz = PRODUCTS["rainfall"], PRODUCTS["dbz"]
    assert rainfall.variables == {"rainfall_rate": "tprate", "accumulated_rainfall_1hour": "tp01"}
    assert len(rainfall.frame_times(HOUR)) == 4
    assert dbz.variables == {"dbz": "refc"}
    assert len(dbz.frame_times(HOUR)) == 12
    assert opera.OPERARainfallProvider.store_prefix == "bkr/precipradar/opera_rainfall.icechunk"
    assert opera.OPERAReflectivityProvider.store_prefix == "bkr/precipradar/opera_dbz.icechunk"


def test_staged_paths_nest_by_day_and_carry_the_hour_stamp(tmp_path):
    path = staged_path(tmp_path, "dbz", HOUR)
    assert path == tmp_path / "dbz/2026/09/29/opera_dbz_202609290600.nc"


def test_no_credentials_in_the_opera_modules():
    """The original scripts had a Source Cooperative key pair inline."""
    for module in (opera, opera_download):
        source = pathlib.Path(module.__file__).read_text()
        assert not re.search(r"AKIA[0-9A-Z]{16}", source)
        assert "secret_access_key=" not in source


# --- downloader -------------------------------------------------------------------------


def test_an_hour_is_fetched_in_one_call_and_named_like_the_store(tmp_path):
    source = FakeOPERA()
    summary = download_opera("rainfall", HOUR, tmp_path, source=source)

    ((times, variables),) = source.calls
    assert [t.minute for t in times] == [0, 15, 30, 45]
    assert variables == ["tprate", "tp01"]
    with xr.open_dataset(summary["path"]) as ds:
        assert set(ds.data_vars) == {"rainfall_rate", "accumulated_rainfall_1hour"}
        assert ds["rainfall_rate"].dims == ("time", "y", "x")
        assert ds["latitude"].dims == ("y", "x")
        assert "_lat" not in ds.coords and "variable" not in ds.coords
        assert ds["accumulated_rainfall_1hour"].attrs["units"] == "m"
        np.testing.assert_array_equal(ds["rainfall_rate"].isel(y=0, x=0), [0, 15, 30, 45])
        np.testing.assert_array_equal(
            ds["accumulated_rainfall_1hour"].isel(y=0, x=0), [1, 16, 31, 46]
        )
    assert summary["frames"] == 4 and summary["missing_frames"] == []
    assert not list(tmp_path.rglob("*.part"))


def test_a_missing_frame_fails_the_hour_unless_allowed(tmp_path):
    gap = HOUR + dt.timedelta(minutes=30)
    with pytest.raises(IncompleteHour, match="06:30"):
        download_opera("rainfall", HOUR, tmp_path, source=FakeOPERA(missing={gap}))
    assert not list(tmp_path.rglob("*.nc"))

    source = FakeOPERA(missing={gap})
    summary = download_opera("rainfall", HOUR, tmp_path, source=source, allow_partial=True)
    assert summary["frames"] == 3 and summary["missing_frames"] == ["06:30"]
    assert len(source.calls) == 1 + 4, "the whole hour, then frame by frame to find the gap"


def test_an_hour_with_no_frames_at_all_fails_even_when_partial_is_allowed(tmp_path):
    frames = set(PRODUCTS["dbz"].frame_times(HOUR))
    with pytest.raises(IncompleteHour, match="no frames"):
        download_opera("dbz", HOUR, tmp_path, source=FakeOPERA(missing=frames), allow_partial=True)


def test_reflectivity_before_the_1km_grid_is_refused(tmp_path):
    with pytest.raises(ValueError, match="only staged from 2024-07-01"):
        download_opera("dbz", dt.datetime(2024, 6, 30, 23), tmp_path, source=FakeOPERA())


def test_a_time_that_is_not_on_the_hour_is_refused(tmp_path):
    with pytest.raises(ValueError, match="start of an hour"):
        download_opera("rainfall", "2026-09-29T06:15", tmp_path, source=FakeOPERA())


def test_tz_aware_times_are_converted_to_naive_utc():
    assert opera_download.to_naive_utc("2026-09-29T08:00+02:00") == HOUR


def test_pipes_accepts_the_summary():
    from dagster_pipes import _normalize_param_metadata

    summary = {"frames": 4, "missing_frames": [], "grid": [2200, 1900], "path": "x.nc"}
    _normalize_param_metadata(
        opera_download.pipes_metadata(summary), "report_asset_materialization", "metadata"
    )


# --- providers --------------------------------------------------------------------------


def test_the_provider_finds_and_reads_a_staged_hour(local_config):
    stage(local_config, "dbz", HOUR)
    provider = opera.OPERAReflectivityProvider()
    it = pd.Timestamp(HOUR)

    files = provider.fetch(it)
    ds = provider.process(files, it)

    assert provider.archive_dir == local_config / "data" / "opera" / "dbz"
    assert len(files) == 1
    assert ds.sizes == {"time": 12, "y": NY, "x": NX}
    assert list(ds.data_vars) == ["dbz"]


def test_the_archive_root_honours_the_environment(tmp_path, local_config, monkeypatch):
    monkeypatch.setenv(opera.ARCHIVE_ENV, str(tmp_path / "elsewhere"))
    provider = opera.OPERARainfallProvider()
    assert provider.archive_dir == tmp_path / "elsewhere" / "rainfall"


def test_an_absent_hour_is_nothing_to_do(local_config):
    (local_config / "data" / "opera" / "rainfall").mkdir(parents=True)
    assert opera.OPERARainfallProvider().fetch(pd.Timestamp(HOUR)) == []


def test_a_partial_staged_hour_is_rejected_by_default(local_config):
    gap = HOUR + dt.timedelta(minutes=15)
    root = local_config / "data" / "opera"
    download_opera("rainfall", HOUR, root, source=FakeOPERA(missing={gap}), allow_partial=True)
    provider = opera.OPERARainfallProvider()
    it = pd.Timestamp(HOUR)

    with pytest.raises(ValueError, match="06:15"):
        provider.process(provider.fetch(it), it)
    assert (
        opera.OPERARainfallProvider(allow_partial=True)
        .process(provider.fetch(it), it)
        .sizes["time"]
        == 3
    )


def test_a_staged_file_of_the_wrong_product_is_rejected(local_config):
    summary = stage(local_config, "dbz", HOUR)
    provider = opera.OPERARainfallProvider()
    with pytest.raises(ValueError, match="expected"):
        provider.process([summary["path"]], pd.Timestamp(HOUR))


def test_hours_append_to_the_store_and_staging_is_cleared(local_config):
    provider = opera.OPERARainfallProvider()
    first, second = pd.Timestamp(HOUR), pd.Timestamp(HOUR) + pd.Timedelta("1h")
    for hour in (first, second):
        stage(local_config, "rainfall", hour.to_pydatetime())

    assert provider.discard_staged(first) == [], "nothing is deleted before it is stored"
    assert provider.run_partition(first)
    assert provider.run_partition(second)
    assert not provider.run_partition(first), "a stored hour is skipped"

    removed = provider.discard_staged(first) + provider.discard_staged(second)
    assert len(removed) == 2
    assert not list((local_config / "data" / "opera").rglob("*.nc"))

    stored = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store)
    assert stored.sizes["time"] == 8
    assert pd.DatetimeIndex(stored["time"].values).is_monotonic_increasing
    assert set(stored.data_vars) == {"rainfall_rate", "accumulated_rainfall_1hour"}
    assert stored["latitude"].dims == ("y", "x")


def test_hours_before_the_store_end_are_not_appendable_and_are_discarded(local_config):
    """The existing stores have gaps before their latest time that cannot be filled."""
    provider = opera.OPERARainfallProvider()
    early, late = pd.Timestamp(HOUR), pd.Timestamp(HOUR) + pd.Timedelta("2h")
    assert provider.appendable(early), "an empty store accepts anything"

    stage(local_config, "rainfall", late.to_pydatetime())
    assert provider.run_partition(late)
    stage(local_config, "rainfall", early.to_pydatetime())

    assert not provider.appendable(early)
    assert provider.appendable(late + pd.Timedelta("1h"))
    assert not provider.run_partition(early), "the writer drops it"
    assert len(provider.discard_staged(early)) == 1, "so it must not stay staged"


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


def test_the_download_asset_runs_the_image_for_a_missing_hour(local_config, monkeypatch):
    from dags.assets import radar

    monkeypatch.setenv(radar.OPERA_IMAGE_ENV, "example/opera:test")
    client = FakeDockerClient()
    result = dg.materialize(
        [radar.opera_dbz_download],
        partition_key="2026-09-29-06:00",
        resources={"pipes_docker_client": client},
    )

    assert result.success
    (call,) = client.calls
    assert call["image"] == "example/opera:test"
    assert call["command"] == [
        "--product", "dbz", "--time", "2026-09-29T06:00", "--target", "/data/opera",
    ]  # fmt: skip
    root = str((local_config / "data" / "opera").resolve())
    assert call["container_kwargs"]["volumes"] == {root: {"bind": "/data/opera", "mode": "rw"}}


def test_the_download_asset_skips_an_hour_already_in_the_store(local_config):
    from dags.assets import radar

    stage(local_config, "dbz", HOUR)
    assert opera.OPERAReflectivityProvider().run_partition(pd.Timestamp(HOUR))

    client = FakeDockerClient()
    result = dg.materialize(
        [radar.opera_dbz_download],
        partition_key="2026-09-29-06:00",
        resources={"pipes_docker_client": client},
    )
    assert result.success
    assert client.calls == []


def test_the_download_asset_skips_an_hour_the_store_cannot_accept(local_config):
    from dags.assets import radar

    later = HOUR + dt.timedelta(hours=3)
    stage(local_config, "dbz", later)
    assert opera.OPERAReflectivityProvider().run_partition(pd.Timestamp(later))

    client = FakeDockerClient()
    result = dg.materialize(
        [radar.opera_dbz_download],
        partition_key="2026-09-29-06:00",
        resources={"pipes_docker_client": client},
    )
    assert result.success
    assert client.calls == []


def test_the_processing_asset_writes_and_clears_the_staged_hour(local_config):
    from dags.assets import radar

    stage(local_config, "rainfall", HOUR)
    result = dg.materialize(
        [radar.opera_rainfall],
        partition_key="2026-09-29-06:00",
    )

    assert result.success
    (materialization,) = result.asset_materializations_for_node("opera_rainfall")
    metadata = materialization.metadata
    assert metadata["written"].value is True
    assert metadata["staged_files_removed"].value == 1


def test_opera_stores_depend_on_their_downloads_after_key_prefixing():
    from dags import loader
    from dags.assets import radar

    assets, failures = loader.load_assets([radar])
    assert not failures
    graph = {key: set(a.asset_deps[key]) for a in assets for key in a.keys}

    for name in ("opera_rainfall", "opera_dbz"):
        download = dg.AssetKey(["radar", f"{name}_download"])
        assert graph[dg.AssetKey(["radar", name])] == {download}
        assert graph[download] == set()
