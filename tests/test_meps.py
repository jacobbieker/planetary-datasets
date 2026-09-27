"""The met.no Nordic providers, exercised offline against synthetic inputs.

No test here touches the network: every provider's ``process`` is fed NetCDF files written
into ``tmp_path`` that mimic the shape of the real archive files, and the store round-trips
go to a local icechunk repository.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.config import Config
from planetary_datasets.providers.meps import (
    ANDOYA,
    ANDOYA_BUCKET_ENV,
    PROVIDERS,
    AndoyaMEPSPostProcessedProvider,
    AndoyaMEPSProvider,
    AndoyaReflectivityProvider,
    Box,
    BucketNotConfigured,
    MEPSAnalysisProvider,
    MEPSDetProvider,
    MEPSPostProcessedProvider,
    NordicReflectivityProvider,
    meps_analysis_urls,
    meps_det_opendap_url,
    nordic_analysis_url,
    nordic_reflectivity_url,
)

NY, NX = 4, 5


def _grid() -> dict:
    """2D latitude/longitude coordinates centred on the Andoya box."""
    lat = np.linspace(ANDOYA.latitude - 1, ANDOYA.latitude + 1, NY)
    lon = np.linspace(ANDOYA.longitude - 1, ANDOYA.longitude + 1, NX)
    lon2d, lat2d = np.meshgrid(lon, lat)
    return {
        "latitude": (("y", "x"), lat2d),
        "longitude": (("y", "x"), lon2d),
    }


def write_reflectivity(path, time: pd.Timestamp, steps: int = 3):
    """A stand-in for a daily ``yrwms-nordic`` radar mosaic file."""
    ds = xr.Dataset(
        {
            "equivalent_reflectivity_factor": (
                ("time", "Yc", "Xc"),
                np.zeros((steps, NY, NX), dtype="float32"),
            )
        },
        coords={
            "time": pd.date_range(time, periods=steps, freq="1h"),
            "Yc": np.arange(NY, dtype="float32"),
            "Xc": np.arange(NX, dtype="float32"),
            "lat": (("Yc", "Xc"), _grid()["latitude"][1]),
            "lon": (("Yc", "Xc"), _grid()["longitude"][1]),
        },
    )
    ds.to_netcdf(path)
    return str(path)


def write_meps_step(tmp_path, stamp: str, step: int) -> list[str]:
    """The three files (height, pressure, surface) making up one MEPS lead time."""
    time = pd.DatetimeIndex([pd.Timestamp(stamp) + pd.Timedelta(hours=step)])
    coords = {"time": time, **_grid()}

    hl = xr.Dataset(
        {"air_temperature_hl": (("time", "height0", "y", "x"), np.zeros((1, 1, NY, NX), "float32"))},
        coords={**coords, "height0": np.array([2.0])},
    )
    pl = xr.Dataset(
        {"air_temperature_pl": (("time", "pressure", "y", "x"), np.zeros((1, 3, NY, NX), "float32"))},
        coords={**coords, "pressure": np.array([1000.0, 500.0, 850.0])},
    )
    sfc = xr.Dataset(
        {
            "icing_index": (("time", "height1", "y", "x"), np.zeros((1, 2, NY, NX), "float32")),
            "surface_air_pressure": (
                ("time", "height2", "y", "x"),
                np.zeros((1, 1, NY, NX), "float32"),
            ),
        },
        coords={**coords, "height1": np.array([3000.0, 1000.0]), "height2": np.array([0.0])},
    )

    paths = []
    for kind, ds in (("hl", hl), ("pl", pl), ("sfc", sfc)):
        path = tmp_path / f"meps_{kind}_{step:02}_{pd.Timestamp(stamp).strftime('%Y%m%dT%HZ')}.nc"
        ds.to_netcdf(path)
        paths.append(str(path))
    return paths


def write_nordic_analysis(tmp_path, time: pd.Timestamp) -> str:
    """A stand-in for one hour of ``met_analysis_1_0km_nordic``."""
    ds = xr.Dataset(
        {"air_temperature_2m": (("time", "y", "x"), np.zeros((1, NY, NX), "float32"))},
        coords={"time": pd.DatetimeIndex([time]), **_grid()},
    )
    path = tmp_path / f"met_analysis_1_0km_nordic_{time.strftime('%Y%m%dT%HZ')}.nc"
    ds.to_netcdf(path)
    return str(path)


def write_det_model_level(tmp_path, time: pd.Timestamp, steps: int = 3) -> str:
    """A stand-in for the OPeNDAP ``meps_det_2_5km`` model-level dataset."""
    ds = xr.Dataset(
        {
            "air_temperature_ml": (
                ("time", "hybrid", "y", "x"),
                np.full((steps, 2, NY, NX), 273.0, "float32"),
            ),
            # ap and b are the hybrid-level coefficients, one value per level.
            "ap": (("hybrid",), np.array([20000.75, 0.0])),
            "b": (("hybrid",), np.array([0.0, 1.0])),
            "surface_air_pressure": (("time", "y", "x"), np.zeros((steps, NY, NX), "float32")),
        },
        coords={
            "time": pd.date_range(time, periods=steps, freq="1h"),
            "hybrid": np.array([0.9, 1.0]),
            **_grid(),
        },
    )
    path = tmp_path / "meps_det_2_5km.nc"
    ds.to_netcdf(path)
    return str(path)


# --- URL construction ------------------------------------------------------------------


def test_reflectivity_url_is_month_nested():
    url = nordic_reflectivity_url(pd.Timestamp("2026-01-31"))
    assert url.endswith("nordiclcc-1000.20260131.nc")
    assert "/reflectivity-nordic/2026/01/" in url


def test_analysis_urls_cover_every_step_and_level_type():
    urls = meps_analysis_urls(pd.Timestamp("2026-04-05T03:00"))
    assert len(urls) == 9
    assert all("/2026/04/05/03/member_00/" in u for u in urls)
    assert sum("meps_sfc_" in u for u in urls) == 3
    assert "meps_hl_02_20260405T03Z.nc" in urls[-3]


def test_nordic_analysis_url_is_day_nested():
    url = nordic_analysis_url(pd.Timestamp("2026-04-05T20:00"))
    assert url.endswith("met_analysis_1_0km_nordic_20260405T20Z.nc")
    assert "/metpparchive/2026/04/05/" in url


def test_det_url_uses_opendap_endpoint():
    url = meps_det_opendap_url(pd.Timestamp("2026-04-01T00:00"))
    assert "/dodsC/" in url
    assert url.endswith("meps_det_2_5km_20260401T00Z.nc")


# --- cropping --------------------------------------------------------------------------


def test_box_clips_to_its_padding():
    lat = np.linspace(60, 80, 21)
    lon = np.linspace(0, 40, 41)
    lon2d, lat2d = np.meshgrid(lon, lat)
    ds = xr.Dataset(
        {"v": (("y", "x"), np.zeros((21, 41), "float32"))},
        coords={"latitude": (("y", "x"), lat2d), "longitude": (("y", "x"), lon2d)},
    )
    clipped = Box(latitude=70.0, longitude=20.0, lat_pad=2.0, lon_pad=3.0).clip(ds)
    assert float(clipped.latitude.min()) >= 68.0
    assert float(clipped.latitude.max()) <= 72.0
    assert float(clipped.longitude.min()) >= 17.0
    assert float(clipped.longitude.max()) <= 23.0
    assert clipped.sizes["y"] < ds.sizes["y"]


def test_box_clip_leaves_the_grid_mapping_variable_alone():
    """``Dataset.where`` would broadcast the scalar CRS sentinel into a full float grid."""
    lat = np.linspace(60, 80, 21)
    lon = np.linspace(0, 40, 41)
    lon2d, lat2d = np.meshgrid(lon, lat)
    ds = xr.Dataset(
        {
            "v": (("y", "x"), np.zeros((21, 41), "float32")),
            "projection_lambert": ((), np.int32(-2147483647)),
        },
        coords={"latitude": (("y", "x"), lat2d), "longitude": (("y", "x"), lon2d)},
    )
    clipped = Box(latitude=70.0, longitude=20.0).clip(ds)
    assert clipped["projection_lambert"].dims == ()
    assert clipped["projection_lambert"].dtype == np.dtype("int32")
    assert clipped["v"].dims == ("y", "x")


def test_box_clip_rejects_a_grid_it_does_not_touch():
    ds = xr.Dataset(
        {"v": (("y", "x"), np.zeros((2, 2), "float32"))},
        coords={
            "latitude": (("y", "x"), np.full((2, 2), -40.0)),
            "longitude": (("y", "x"), np.full((2, 2), 120.0)),
        },
    )
    with pytest.raises(ValueError, match="does not intersect"):
        Box(latitude=70.0, longitude=20.0).clip(ds)


# --- process ---------------------------------------------------------------------------


def test_reflectivity_process_renames_axes(tmp_path, local_config):
    path = write_reflectivity(tmp_path / "radar.nc", pd.Timestamp("2026-01-31"))
    ds = NordicReflectivityProvider(config=local_config).process([path], pd.Timestamp("2026-01-31"))
    assert set(ds.dims) == {"time", "y", "x"}
    assert "latitude" in ds.coords and "longitude" in ds.coords
    assert ds.chunksizes["time"][0] == 1


def test_analysis_process_merges_levels_and_concatenates_steps(tmp_path, local_config):
    files = []
    for step in range(3):
        files += write_meps_step(tmp_path, "2026-04-05T03:00", step)
    ds = MEPSAnalysisProvider(config=local_config).process(files, pd.Timestamp("2026-04-05T03:00"))

    assert ds.sizes["time"] == 3
    # The icing levels survive as a real height axis; the other degenerate axes are gone.
    assert ds.sizes["height"] == 2
    assert "height0" not in ds.dims and "height2" not in ds.dims
    assert {"air_temperature_hl", "air_temperature_pl", "icing_index"} <= set(ds.data_vars)
    # Vertical coordinates are sorted so later appends line up with the store.
    assert list(ds.pressure.values) == sorted(ds.pressure.values)
    assert list(ds.height.values) == sorted(ds.height.values)


def test_fetch_skips_a_partition_the_archive_does_not_have(tmp_path, local_config, monkeypatch):
    provider = MEPSAnalysisProvider(config=local_config)
    monkeypatch.setattr(provider, "download", lambda urls, temp_dir: [])
    assert provider.fetch(pd.Timestamp("2026-04-05T03:00"), temp_dir=tmp_path) == []


def test_fetch_refuses_to_write_a_half_downloaded_partition(tmp_path, local_config, monkeypatch):
    """A partial set must raise: committing it would mark the partition permanently done."""
    provider = MEPSAnalysisProvider(config=local_config)
    monkeypatch.setattr(provider, "download", lambda urls, temp_dir: ["only-one.nc"])
    with pytest.raises(RuntimeError, match="partly available"):
        provider.fetch(pd.Timestamp("2026-04-05T03:00"), temp_dir=tmp_path)


def test_postprocessed_fetch_refuses_a_partial_day(tmp_path, local_config, monkeypatch):
    provider = MEPSPostProcessedProvider(config=local_config)
    monkeypatch.setattr(provider, "download", lambda urls, temp_dir: list(urls[:19]))
    with pytest.raises(RuntimeError, match="19/24"):
        provider.fetch(pd.Timestamp("2026-04-05"), temp_dir=tmp_path)


def test_postprocessed_process_concatenates_hours(tmp_path, local_config):
    start = pd.Timestamp("2026-04-05T00:00")
    files = [write_nordic_analysis(tmp_path, start + pd.Timedelta(hours=h)) for h in range(3)]
    ds = MEPSPostProcessedProvider(config=local_config).process(files, start)
    assert ds.sizes["time"] == 3
    assert list(pd.DatetimeIndex(ds.time.values)) == [start + pd.Timedelta(hours=h) for h in range(3)]


def test_postprocessed_partitions_do_not_overlap(local_config):
    provider = MEPSPostProcessedProvider(config=local_config)
    day = provider.hourly_range(pd.Timestamp("2026-04-05"))
    nxt = provider.hourly_range(pd.Timestamp("2026-04-06"))
    assert len(day) == 24
    assert not set(day) & set(nxt)


def test_det_process_keeps_hybrid_vars_and_downcasts(tmp_path, local_config):
    path = write_det_model_level(tmp_path, pd.Timestamp("2026-04-01T00:00"))
    ds = MEPSDetProvider(config=local_config).process([path], pd.Timestamp("2026-04-01T00:00"))
    assert set(ds.data_vars) == {"air_temperature_ml", "ap", "b"}
    assert ds["air_temperature_ml"].dtype == np.dtype("float16")
    # ap and b define the hybrid levels; float16 would blur the vertical coordinate.
    assert ds["b"].dtype == np.dtype("float64")
    assert ds["ap"].dtype == np.dtype("float64")
    assert float(ds["ap"].max()) == 20000.75


def test_det_process_finds_levels_without_a_hybrid_coordinate(tmp_path, local_config):
    """Some THREDDS aggregations expose hybrid as a bare dimension with no values."""
    ds = xr.Dataset(
        {"air_temperature_ml": (("time", "hybrid", "y", "x"), np.zeros((1, 2, NY, NX), "float32"))},
        coords={"time": pd.DatetimeIndex(["2026-04-01"]), **_grid()},
    )
    path = tmp_path / "nocoord.nc"
    ds.to_netcdf(path)
    out = MEPSDetProvider(config=local_config).process([str(path)], pd.Timestamp("2026-04-01"))
    assert "air_temperature_ml" in out.data_vars


def test_det_process_rejects_a_file_with_no_model_levels(tmp_path, local_config):
    ds = xr.Dataset(
        {"v": (("time", "y", "x"), np.zeros((1, NY, NX), "float32"))},
        coords={"time": pd.DatetimeIndex(["2026-04-01"]), **_grid()},
    )
    path = tmp_path / "nolevels.nc"
    ds.to_netcdf(path)
    with pytest.raises(ValueError, match="hybrid"):
        MEPSDetProvider(config=local_config).process([str(path)], pd.Timestamp("2026-04-01"))


# --- the Andoya subsets ------------------------------------------------------------------


def test_andoya_variants_crop(tmp_path, local_config):
    files = []
    for step in range(3):
        files += write_meps_step(tmp_path, "2026-04-05T03:00", step)
    full = MEPSAnalysisProvider(config=local_config).process(files, pd.Timestamp("2026-04-05T03:00"))
    cropped = AndoyaMEPSProvider(config=local_config).process(files, pd.Timestamp("2026-04-05T03:00"))
    # The synthetic grid sits inside the box, so nothing is dropped, but the crop must run.
    assert cropped.sizes["y"] <= full.sizes["y"]
    assert float(cropped.latitude.max()) <= ANDOYA.latitude + ANDOYA.lat_pad


def test_andoya_variants_need_their_bucket_configured(monkeypatch):
    monkeypatch.delenv(ANDOYA_BUCKET_ENV, raising=False)
    with pytest.raises(BucketNotConfigured, match=ANDOYA_BUCKET_ENV):
        AndoyaMEPSProvider(config=Config()).config  # noqa: B018 - the property raises


def test_andoya_variants_use_the_configured_bucket(monkeypatch):
    monkeypatch.setenv(ANDOYA_BUCKET_ENV, "some-private-bucket")
    provider = AndoyaMEPSPostProcessedProvider(config=Config())
    assert provider.config.bucket == "some-private-bucket"
    assert provider.store_path == "s3://some-private-bucket/andoya_meps_postprocessed.icechunk"


def test_a_local_store_does_not_need_the_private_bucket(monkeypatch, local_config, tmp_path):
    monkeypatch.delenv(ANDOYA_BUCKET_ENV, raising=False)
    assert AndoyaReflectivityProvider(config=local_config).store_path.startswith(str(tmp_path))


def test_the_public_providers_never_read_the_private_bucket_variable():
    for cls in (NordicReflectivityProvider, MEPSAnalysisProvider, MEPSPostProcessedProvider, MEPSDetProvider):
        assert cls.bucket_env is None


def test_every_variant_has_a_distinct_store():
    prefixes = [cls.store_prefix for cls in PROVIDERS.values()]
    assert len(prefixes) == len(set(prefixes)) == 7


# --- store round-trip ----------------------------------------------------------------------


def test_run_partition_writes_and_reopens(tmp_path, local_config):
    """The full fetch/process/write path, with the download replaced by a local file."""
    stamp = pd.Timestamp("2026-01-31")
    path = write_reflectivity(tmp_path / "radar.nc", stamp)

    class LocalReflectivity(NordicReflectivityProvider):
        def fetch(self, it, temp_dir=None, **kwargs):
            return [path]

    provider = LocalReflectivity(config=local_config)
    assert provider.run_partition(stamp) is True
    assert provider.run_partition(stamp) is False

    stored = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store, consolidated=False)
    assert stamp.to_datetime64() in stored.time.values
    assert "equivalent_reflectivity_factor" in stored.data_vars


def test_run_partition_skips_when_nothing_was_fetched(local_config):
    class EmptyFetch(MEPSAnalysisProvider):
        def fetch(self, it, temp_dir=None, **kwargs):
            return []

    assert EmptyFetch(config=local_config).run_partition(pd.Timestamp("2026-04-05T03:00")) is False
