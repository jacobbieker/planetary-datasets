"""Offline tests for the IFS HRES analysis provider. Nothing here touches the network."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import requests
import xarray as xr

from planetary_datasets.providers.ifs import (
    ATMOSPHERE_VAR_CODES,
    KEEP_FLOAT32_VARS,
    SURFACE_VAR_CODES,
    IFSAnalysisProvider,
    IFSRegriddedAnalysisProvider,
    _analysis_stamp,
    _regridded_path,
    atmosphere_url,
    merge_surface_and_atmosphere,
    surface_url,
    to_float16_except,
    url_is_absent,
)

DAY = pd.Timestamp("2026-01-02")


def _grid(long_name: str, times, values, level: bool = False):
    lat = np.linspace(-10, 10, 4)
    lon = np.linspace(0, 350, 5)
    if level:
        levels = np.array([1000.0, 500.0])
        data = np.full((len(times), len(levels), lat.size, lon.size), values, dtype="float32")
        da = xr.DataArray(
            data,
            dims=("time", "level", "latitude", "longitude"),
            coords={"time": times, "level": levels, "latitude": lat, "longitude": lon},
        )
    else:
        data = np.full((len(times), lat.size, lon.size), values, dtype="float32")
        da = xr.DataArray(
            data,
            dims=("time", "latitude", "longitude"),
            coords={"time": times, "latitude": lat, "longitude": lon},
        )
    da.attrs["long_name"] = long_name
    return da


@pytest.fixture
def analysis_pair():
    """A tiny stand-in for the merged surface and pressure-level analyses."""
    times = pd.date_range(DAY, periods=4, freq="6h")
    surface = xr.Dataset(
        {
            "2t": _grid("2 metre temperature", times, 280.0),
            "sp": _grid("Surface pressure", times, 101325.0),
            "lsm": _grid("Land-sea mask", times, 1.0),
            "utc_date": _grid("UTC date", times, 0.0),
        }
    )
    atmos = xr.Dataset(
        {
            "t": _grid("Temperature", times, 250.0, level=True),
            "q": _grid("Specific humidity", times, 0.004, level=True),
            "d": _grid("Divergence", times, 0.0, level=True),
        }
    )
    return surface, atmos


def test_surface_url_uses_table_228_for_100m_winds():
    assert surface_url(DAY, "246", "100u").endswith(
        "ec.oper.an.sfc/202601/ec.oper.an.sfc.228_246_100u.regn1280sc.20260102.nc"
    )
    assert "128_167_2t" in surface_url(DAY, "167", "2t")


def test_atmosphere_url_uses_the_vector_grid_for_winds():
    assert atmosphere_url(DAY, "12", "131", "u", suffix="grb").endswith(
        "ec.oper.an.pl/202601/ec.oper.an.pl.128_131_u.regn1280uv.2026010212.grb"
    )
    assert "regn1280sc" in atmosphere_url(DAY, "00", "130", "t")


def test_urls_cover_every_variable_and_hour():
    provider = IFSAnalysisProvider()
    urls = provider.urls(DAY)
    assert len(urls) == len(SURFACE_VAR_CODES) + 4 * len(ATMOSPHERE_VAR_CODES)
    assert len(set(urls)) == len(urls)


def test_analysis_stamp_survives_the_regrid_suffix():
    raw = "ec.oper.an.pl.128_130_t.regn1280sc.2026010218.grb"
    assert _analysis_stamp(raw) == "2026010218"
    regridded = _regridded_path(pathlib.Path(raw), [1.0, 1.0])
    assert regridded.name.endswith(".2026010218.regrid_1.0_1.0.grb")
    assert _analysis_stamp(regridded) == "2026010218"


def test_regridded_path_distinguishes_anisotropic_grids():
    raw = pathlib.Path("ec.oper.an.pl.128_130_t.regn1280sc.2026010218.grb")
    assert _regridded_path(raw, [1.0, 0.5]) != _regridded_path(raw, [1.0, 1.0])


def test_to_float16_except_keeps_the_named_variables(analysis_pair):
    surface, _ = analysis_pair
    out = to_float16_except(surface, keep={"sp"})
    assert out["sp"].dtype == np.float32
    assert out["2t"].dtype == np.float16


def test_merge_renames_by_long_name_and_drops_static_and_metadata(analysis_pair):
    surface, atmos = analysis_pair
    ds = merge_surface_and_atmosphere(surface, atmos)
    assert "2_metre_temperature" in ds.data_vars
    assert "temperature" in ds.data_vars
    # land-sea mask is static, divergence is dropped, utc_date is bookkeeping.
    assert "land-sea_mask" not in ds.data_vars
    assert "divergence" not in ds.data_vars
    assert not any("utc_date" in v for v in ds.data_vars)


def test_merge_applies_the_precision_policy(analysis_pair):
    surface, atmos = analysis_pair
    ds = merge_surface_and_atmosphere(surface, atmos)
    assert "specific_humidity" in KEEP_FLOAT32_VARS
    assert ds["specific_humidity"].dtype == np.float32
    assert ds["surface_pressure"].dtype == np.float32
    assert ds["temperature"].dtype == np.float16

    ds32 = merge_surface_and_atmosphere(surface, atmos, float16=False)
    assert ds32["temperature"].dtype == np.float32


def test_merge_normalises_longitudes(analysis_pair):
    surface, atmos = analysis_pair
    ds = merge_surface_and_atmosphere(surface, atmos)
    assert float(ds.longitude.min()) >= -180
    assert float(ds.longitude.max()) <= 180
    assert (ds.longitude.diff("longitude") > 0).all()


def test_surface_suffix_disambiguates_colliding_long_names():
    times = pd.date_range(DAY, periods=1, freq="6h")
    surface = xr.Dataset({"z": _grid("Geopotential", times, 1.0)})
    atmos = xr.Dataset({"z": _grid("Geopotential", times, 2.0, level=True)})
    ds = merge_surface_and_atmosphere(surface, atmos, surface_suffix="_sfc")
    assert "geopotential_sfc" in ds.data_vars
    assert "geopotential" in ds.data_vars
    # Both are on the keep list, so neither is downcast.
    assert ds["geopotential_sfc"].dtype == np.float32
    assert ds["geopotential"].dtype == np.float32


def test_static_and_dropped_vars_go_even_with_a_surface_suffix(analysis_pair):
    surface, atmos = analysis_pair
    ds = merge_surface_and_atmosphere(surface, atmos, surface_suffix="_sfc")
    assert "land-sea_mask_sfc" not in ds.data_vars
    assert "2_metre_temperature_sfc" in ds.data_vars


def test_levels_are_sorted_descending(analysis_pair):
    surface, atmos = analysis_pair
    ds = merge_surface_and_atmosphere(surface, atmos.sortby("level"))
    assert list(ds.level.values) == [1000.0, 500.0]


def test_process_splits_inputs_by_archive_marker(tmp_path, analysis_pair, local_config):
    surface, atmos = analysis_pair
    sfc_path = tmp_path / "ec.oper.an.sfc.128_167_2t.regn1280sc.20260102.nc"
    pl_path = tmp_path / "ec.oper.an.pl.128_130_t.regn1280sc.2026010200.nc"
    surface[["2t"]].to_netcdf(sfc_path)
    atmos[["t"]].to_netcdf(pl_path)

    provider = IFSAnalysisProvider(config=local_config)
    ds = provider.process([str(sfc_path), str(pl_path)], DAY)
    assert "2_metre_temperature" in ds.data_vars
    assert "temperature" in ds.data_vars


def test_process_rejects_a_partial_day(local_config):
    provider = IFSAnalysisProvider(config=local_config)
    with pytest.raises(ValueError, match="surface and pressure-level"):
        provider.process(["ec.oper.an.sfc.128_167_2t.regn1280sc.20260102.nc"], DAY)


def test_fetch_skips_a_day_the_archive_does_not_have(monkeypatch, tmp_path, local_config):
    provider = IFSAnalysisProvider(config=local_config)
    monkeypatch.setattr(
        "planetary_datasets.providers.ifs.download_one", lambda url, dest, **kw: None
    )
    monkeypatch.setattr("planetary_datasets.providers.ifs.url_is_absent", lambda url: True)
    assert provider.fetch(DAY, temp_dir=tmp_path) == []


def test_fetch_raises_when_a_download_fails_but_the_file_exists(
    monkeypatch, tmp_path, local_config
):
    provider = IFSAnalysisProvider(config=local_config)
    monkeypatch.setattr(
        "planetary_datasets.providers.ifs.download_one", lambda url, dest, **kw: None
    )
    monkeypatch.setattr("planetary_datasets.providers.ifs.url_is_absent", lambda url: False)
    with pytest.raises(RuntimeError, match="failed to download"):
        provider.fetch(DAY, temp_dir=tmp_path)


def test_url_is_absent_treats_a_failed_probe_as_present(monkeypatch):
    def boom(*args, **kwargs):
        raise requests.ConnectionError("no route to host")

    monkeypatch.setattr(requests, "head", boom)
    assert url_is_absent("https://example.invalid/file.nc") is False


def test_fetch_returns_every_downloaded_file(monkeypatch, tmp_path, local_config):
    provider = IFSAnalysisProvider(config=local_config)

    def fake_download(url, dest, **kwargs):
        dest.write_bytes(b"x")
        return dest

    monkeypatch.setattr("planetary_datasets.providers.ifs.download_one", fake_download)
    paths = provider.fetch(DAY, temp_dir=tmp_path)
    assert len(paths) == len(provider.urls(DAY))


def test_run_partition_round_trips_through_a_local_store(
    monkeypatch, tmp_path, analysis_pair, local_config
):
    surface, atmos = analysis_pair
    provider = IFSAnalysisProvider(config=local_config)
    monkeypatch.setattr(
        IFSAnalysisProvider, "fetch", lambda self, it, temp_dir=None, **kw: ["sfc", "pl"]
    )
    monkeypatch.setattr(
        IFSAnalysisProvider,
        "process",
        lambda self, files, it, temp_dir=None, **kw: merge_surface_and_atmosphere(
            surface, atmos
        ),
    )
    assert provider.run_partition(DAY) is True

    stored = xr.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert DAY.to_numpy() in stored.time.values
    assert "temperature" in stored.data_vars
    # Second run is a no-op, the timestep is already there.
    assert provider.run_partition(DAY) is False


def test_regrid_provider_bucket_comes_from_the_environment(monkeypatch, local_config):
    monkeypatch.setenv("IFS_REGRID_BUCKET", "some-other-bucket")
    monkeypatch.setenv("IFS_REGRID_REGION", "eu-west-1")
    provider = IFSRegriddedAnalysisProvider(config=local_config)
    assert provider.config.bucket == "some-other-bucket"
    assert provider.config.region == "eu-west-1"
    assert provider.suffix == "grb"
    assert provider.grid == [1.0, 1.0]
    assert provider.surface_suffix == "_sfc"


def test_regrid_provider_falls_back_to_the_configured_bucket(monkeypatch, local_config):
    monkeypatch.delenv("IFS_REGRID_BUCKET", raising=False)
    monkeypatch.delenv("IFS_REGRID_REGION", raising=False)
    provider = IFSRegriddedAnalysisProvider(config=local_config)
    assert provider.config.bucket == local_config.bucket


def test_provider_rejects_regridding_netcdf():
    with pytest.raises(ValueError, match="only supported for the 'grb'"):
        IFSAnalysisProvider(suffix="nc", grid=(1.0, 1.0))


def test_provider_rejects_an_unknown_suffix():
    with pytest.raises(ValueError, match="must be 'nc' or 'grb'"):
        IFSAnalysisProvider(suffix="zarr")
