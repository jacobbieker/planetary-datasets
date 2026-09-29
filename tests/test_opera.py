"""Tests for the OPERA downloader, the providers that read what it stages, and the assets.

``earth2studio`` is only installed in the downloader image, so its OPERA source is
replaced by a fake that returns arrays of the same shape and coordinates.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import re

import dagster as dg
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import assert_pipes_accepts, read_store
from planetary_datasets.providers import opera, opera_download
from planetary_datasets.providers.opera_download import (
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
def opera_root(local_config, tmp_path) -> pathlib.Path:
    """Where the providers look for staged hours under the local data directory."""
    return tmp_path / "data" / "opera"


@pytest.fixture
def stage(opera_root):
    """Stage an hour where the providers look for it, as the downloader image would."""

    def stage(product: str, hour: dt.datetime, source=None, **kwargs) -> dict:
        return download_opera(product, hour, opera_root, source=source or FakeOPERA(), **kwargs)

    return stage


def materialize_dbz_download(client):
    from dags.assets import radar

    return dg.materialize(
        [radar.opera_dbz_download],
        partition_key="2026-09-29-06:00",
        resources={"pipes_docker_client": client},
    )


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


@pytest.mark.parametrize(
    ("product", "hour", "missing", "error", "match"),
    [
        ("dbz", HOUR, set(PRODUCTS["dbz"].frame_times(HOUR)), IncompleteHour, "no frames"),
        ("dbz", dt.datetime(2024, 6, 30, 23), set(), ValueError, "only staged from 2024-07-01"),
        ("rainfall", "2026-09-29T06:15", set(), ValueError, "start of an hour"),
    ],
    ids=["no-frames-at-all", "reflectivity-before-the-1km-grid", "not-on-the-hour"],
)
def test_an_hour_is_refused_even_when_partial_is_allowed(
    tmp_path, product, hour, missing, error, match
):
    with pytest.raises(error, match=match):
        download_opera(
            product, hour, tmp_path, source=FakeOPERA(missing=missing), allow_partial=True
        )


def test_tz_aware_times_are_converted_to_naive_utc():
    assert opera_download.to_naive_utc("2026-09-29T08:00+02:00") == HOUR


def test_pipes_accepts_the_summary():
    summary = {"frames": 4, "missing_frames": [], "grid": [2200, 1900], "path": "x.nc"}
    assert_pipes_accepts(opera_download.pipes_metadata(summary))


# --- providers --------------------------------------------------------------------------


def test_the_provider_finds_and_reads_a_staged_hour(stage, opera_root):
    stage("dbz", HOUR)
    provider = opera.OPERAReflectivityProvider()
    it = pd.Timestamp(HOUR)

    files = provider.fetch(it)
    ds = provider.process(files, it)

    assert provider.archive_dir == opera_root / "dbz"
    assert len(files) == 1
    assert ds.sizes == {"time": 12, "y": NY, "x": NX}
    assert list(ds.data_vars) == ["dbz"]


def test_the_archive_root_honours_the_environment(tmp_path, local_config, monkeypatch):
    monkeypatch.setenv(opera.ARCHIVE_ENV, str(tmp_path / "elsewhere"))
    provider = opera.OPERARainfallProvider()
    assert provider.archive_dir == tmp_path / "elsewhere" / "rainfall"


def test_an_absent_hour_is_nothing_to_do(opera_root):
    (opera_root / "rainfall").mkdir(parents=True)
    assert opera.OPERARainfallProvider().fetch(pd.Timestamp(HOUR)) == []


def test_a_partial_staged_hour_is_rejected_by_default(stage):
    gap = HOUR + dt.timedelta(minutes=15)
    stage("rainfall", HOUR, source=FakeOPERA(missing={gap}), allow_partial=True)
    provider = opera.OPERARainfallProvider()
    it = pd.Timestamp(HOUR)

    with pytest.raises(ValueError, match="06:15"):
        provider.process(provider.fetch(it), it)
    partial = opera.OPERARainfallProvider(allow_partial=True).process(provider.fetch(it), it)
    assert partial.sizes["time"] == 3


def test_a_staged_file_of_the_wrong_product_is_rejected(stage):
    summary = stage("dbz", HOUR)
    provider = opera.OPERARainfallProvider()
    with pytest.raises(ValueError, match="expected"):
        provider.process([summary["path"]], pd.Timestamp(HOUR))


def test_hours_append_to_the_store_and_staging_is_cleared(stage, opera_root):
    provider = opera.OPERARainfallProvider()
    first, second = pd.Timestamp(HOUR), pd.Timestamp(HOUR) + pd.Timedelta("1h")
    for hour in (first, second):
        stage("rainfall", hour.to_pydatetime())

    assert provider.discard_staged(first) == [], "nothing is deleted before it is stored"
    assert provider.run_partition(first)
    assert provider.run_partition(second)
    assert not provider.run_partition(first), "a stored hour is skipped"

    removed = provider.discard_staged(first) + provider.discard_staged(second)
    assert len(removed) == 2
    assert not list(opera_root.rglob("*.nc"))

    stored = read_store(provider)
    assert stored.sizes["time"] == 8
    assert pd.DatetimeIndex(stored["time"].values).is_monotonic_increasing
    assert set(stored.data_vars) == {"rainfall_rate", "accumulated_rainfall_1hour"}
    assert stored["latitude"].dims == ("y", "x")


def test_hours_before_the_store_end_are_not_appendable_and_are_discarded(stage):
    """The existing stores have gaps before their latest time that cannot be filled."""
    provider = opera.OPERARainfallProvider()
    early, late = pd.Timestamp(HOUR), pd.Timestamp(HOUR) + pd.Timedelta("2h")
    assert provider.appendable(early), "an empty store accepts anything"

    stage("rainfall", late.to_pydatetime())
    assert provider.run_partition(late)
    stage("rainfall", early.to_pydatetime())

    assert not provider.appendable(early)
    assert provider.appendable(late + pd.Timedelta("1h"))
    assert not provider.run_partition(early), "the writer drops it"
    assert len(provider.discard_staged(early)) == 1, "so it must not stay staged"


# --- Dagster assets ---------------------------------------------------------------------


def test_the_download_asset_runs_the_image_for_a_missing_hour(
    opera_root, monkeypatch, fake_docker_client
):
    from dags.assets import radar

    monkeypatch.setenv(radar.OPERA_IMAGE_ENV, "example/opera:test")
    assert materialize_dbz_download(fake_docker_client).success

    (call,) = fake_docker_client.calls
    assert call["image"] == "example/opera:test"
    assert call["command"] == [
        "opera", "--product", "dbz", "--time", "2026-09-29T06:00", "--target", "/data/opera",
    ]  # fmt: skip
    root = str(opera_root.resolve())
    assert call["container_kwargs"]["volumes"] == {root: {"bind": "/data/opera", "mode": "rw"}}


@pytest.mark.parametrize(
    "stored_hour",
    [HOUR, HOUR + dt.timedelta(hours=3)],
    ids=["already-in-the-store", "before-the-store-end"],
)
def test_the_download_asset_skips_an_hour_the_store_will_not_take(
    stage, fake_docker_client, stored_hour
):
    stage("dbz", stored_hour)
    assert opera.OPERAReflectivityProvider().run_partition(pd.Timestamp(stored_hour))

    assert materialize_dbz_download(fake_docker_client).success
    assert fake_docker_client.calls == []


def test_the_processing_asset_writes_and_clears_the_staged_hour(stage):
    from dags.assets import radar

    stage("rainfall", HOUR)
    result = dg.materialize([radar.opera_rainfall], partition_key="2026-09-29-06:00")

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
