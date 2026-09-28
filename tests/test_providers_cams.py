"""Offline tests for the CAMS providers.

Nothing here touches the Copernicus ADS. Retrievals are either replaced with a fixture
bundle or asserted to fail loudly when no credentials are configured.
"""

from __future__ import annotations

import pathlib
import zipfile

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.config import MissingCredential
from planetary_datasets.providers import cams

RUN = pd.Timestamp("2024-03-01T12:00")
LEAD_HOURS = 3


@pytest.fixture(autouse=True)
def _no_cds_env(monkeypatch):
    """Never pick up a developer's real ADS credentials during these tests."""
    for var in ("CDSAPI_KEY", "CDSAPI_URL", "CDSAPI_RC"):
        monkeypatch.delenv(var, raising=False)


def _sfc_dataset(run: pd.Timestamp, lead: int) -> xr.Dataset:
    data = np.linspace(0, 1, 2 * 3, dtype="float32").reshape(1, 1, 2, 3)
    return xr.Dataset(
        {
            "aod550": (
                ("forecast_reference_time", "forecast_period", "latitude", "longitude"),
                data,
                {"long_name": "Total Aerosol Optical Depth at 550nm"},
            ),
        },
        coords={
            "forecast_reference_time": pd.DatetimeIndex([run]),
            "forecast_period": np.array([lead], dtype="timedelta64[h]"),
            "latitude": np.array([10.0, -10.0], dtype="float32"),
            "longitude": np.array([0.0, 120.0, 300.0], dtype="float32"),
        },
    )


def _plev_dataset(run: pd.Timestamp, lead: int) -> xr.Dataset:
    data = np.ones((1, 1, 2, 2, 3), dtype="float32")
    return xr.Dataset(
        {
            "go3": (
                (
                    "forecast_reference_time",
                    "forecast_period",
                    "pressure_level",
                    "latitude",
                    "longitude",
                ),
                data,
                {"long_name": "Dust Aerosol (0.03 - 0.55 um) Mixing Ratio"},
            ),
        },
        coords={
            "forecast_reference_time": pd.DatetimeIndex([run]),
            "forecast_period": np.array([lead], dtype="timedelta64[h]"),
            "pressure_level": np.array([500.0, 1000.0], dtype="float32"),
            "latitude": np.array([10.0, -10.0], dtype="float32"),
            "longitude": np.array([0.0, 120.0, 300.0], dtype="float32"),
        },
    )


@pytest.fixture
def cams_zip(tmp_path: pathlib.Path) -> pathlib.Path:
    """A ``.nc.zip`` bundle shaped like a single-lead ADS download."""
    raw = tmp_path / "raw"
    raw.mkdir()
    sfc = raw / "data_sfc.nc"
    plev = raw / "data_plev.nc"
    _sfc_dataset(RUN, LEAD_HOURS).to_netcdf(sfc)
    _plev_dataset(RUN, LEAD_HOURS).to_netcdf(plev)

    bundle = tmp_path / "cams_composition_20240301_1500.nc.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.write(sfc, "data_sfc.nc")
        archive.write(plev, "data_plev.nc")
    return bundle


# --------------------------------------------------------------------------- naming


def test_slugify_long_name_matches_store_naming():
    assert cams.slugify_long_name("Total Aerosol Optical Depth at 550nm") == (
        "total_aerosol_optical_depth_at_550nm"
    )
    assert cams.slugify_long_name("Dust Aerosol (0.03 - 0.55 um) Mixing Ratio") == (
        "dust_aerosol_0.03_to_0.55_um_mixing_ratio"
    )


def test_colliding_long_names_do_not_raise():
    ds = xr.Dataset(
        {
            "a": ("x", [1.0], {"long_name": "Ozone"}),
            "b": ("x", [2.0], {"long_name": "Ozone"}),
        },
        coords={"x": [0]},
    )
    out = cams.preproc_cams(ds)
    assert "ozone" in out.data_vars
    assert len(out.data_vars) == 2


def test_preproc_standard_name_uses_standard_name():
    ds = xr.Dataset({"tmp": ("x", [1.0], {"standard_name": "air_temperature"})}, coords={"x": [0]})
    assert "air_temperature" in cams.preproc_standard_name(ds).data_vars


# --------------------------------------------------------------------------- zip


def test_extract_zip_returns_members(cams_zip):
    members = cams.extract_zip(cams_zip)
    assert set(members) == {"data_sfc.nc", "data_plev.nc"}
    assert all(isinstance(v, bytes) and v for v in members.values())


def test_extract_zip_to_dir_prefixes_members(cams_zip, tmp_path):
    out = cams.extract_zip_to_dir(cams_zip, tmp_path / "unzipped")
    assert [p.name for p in out] == [
        "cams_composition_20240301_1500__data_plev.nc",
        "cams_composition_20240301_1500__data_sfc.nc",
    ]


def test_extract_zip_to_dir_cannot_escape(tmp_path):
    bundle = tmp_path / "evil.nc.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("../../escaped.nc", b"nope")
    written = cams.extract_zip_to_dir(bundle, tmp_path / "out")
    assert [p.parent for p in written] == [tmp_path / "out"]
    assert not (tmp_path.parent / "escaped.nc").exists()


def test_open_cams_zip_from_disk_and_memory(cams_zip, tmp_path):
    from_disk = cams.open_cams_zip(cams_zip, temp_dir=tmp_path / "scratch")
    in_memory = cams.open_cams_zip(cams_zip)
    for ds in (from_disk, in_memory):
        assert set(ds.data_vars) == {
            "total_aerosol_optical_depth_at_550nm",
            "dust_aerosol_0.03_to_0.55_um_mixing_ratio",
        }
        assert ds["time"].values[0] == np.datetime64(RUN + pd.Timedelta(hours=LEAD_HOURS))
        assert "forecast_period" not in ds.variables


def test_open_cams_zip_without_netcdf_members(tmp_path):
    bundle = tmp_path / "empty.nc.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("readme.txt", b"no data here")
    with pytest.raises(ValueError, match="no .nc members"):
        cams.open_cams_zip(bundle)


def test_preproc_cams_rejects_multiple_lead_times():
    ds = _sfc_dataset(RUN, LEAD_HOURS).reindex(
        forecast_period=np.array([0, 1], dtype="timedelta64[h]")
    )
    with pytest.raises(ValueError, match="single lead time"):
        cams.preproc_cams(ds)


def test_preproc_cams_forecast_keeps_run_and_lead_axes():
    ds = _sfc_dataset(RUN, LEAD_HOURS).reindex(
        forecast_period=np.array([0, 1, 2], dtype="timedelta64[h]")
    )
    out = cams.preproc_cams_forecast(ds)
    assert out.sizes["init_time"] == 1
    assert out.sizes["step"] == 3
    assert out["init_time"].values[0] == np.datetime64(RUN)


def test_finalise_normalises_coords(cams_zip, tmp_path):
    ds = cams.finalise(cams.open_cams_zip(cams_zip, temp_dir=tmp_path / "scratch"))
    assert list(ds["longitude"].values) == [-60.0, 0.0, 120.0]
    assert list(ds["latitude"].values) == [-10.0, 10.0]
    assert list(ds["pressure_level"].values) == [1000.0, 500.0]


def test_finalise_keeps_full_precision_by_default(cams_zip, tmp_path):
    """float16 would flush CAMS trace-gas columns of order 1e-9 to zero."""
    opened = cams.open_cams_zip(cams_zip, temp_dir=tmp_path / "scratch")
    assert all(cams.finalise(opened)[v].dtype == np.float32 for v in opened.data_vars)

    lossy = cams.finalise(opened, float16=True)
    assert all(lossy[v].dtype == np.float16 for v in lossy.data_vars)
    # Coordinates keep full precision even when the data does not.
    assert lossy["latitude"].dtype != np.float16


# --------------------------------------------------------------------------- creds


def test_cdsapi_credentials_defaults_to_ads(local_config):
    from dataclasses import replace

    from planetary_datasets.config import Credentials

    assert cams.cdsapi_credentials(local_config) is None

    keyed = replace(local_config, credentials=Credentials(cdsapi_key="abc123"))
    assert cams.cdsapi_credentials(keyed) == (cams.DEFAULT_ADS_URL, "abc123")

    explicit = replace(
        local_config,
        credentials=Credentials(cdsapi_key="abc123", cdsapi_url="https://example.invalid/api"),
    )
    assert cams.cdsapi_credentials(explicit) == ("https://example.invalid/api", "abc123")


def test_cds_client_raises_without_credentials(monkeypatch, local_config, tmp_path):
    monkeypatch.delenv("CDSAPI_RC", raising=False)
    monkeypatch.setattr(pathlib.Path, "home", classmethod(lambda cls: tmp_path))
    with pytest.raises(MissingCredential, match="Copernicus ADS credentials"):
        cams.cds_client(local_config)


def test_cds_client_accepts_rc_file(monkeypatch, local_config, tmp_path):
    rc = tmp_path / "cdsapirc"
    rc.write_text("url: https://example.invalid/api\nkey: x\n")
    monkeypatch.setenv("CDSAPI_RC", str(rc))
    assert cams._cdsapirc_exists() is True


# --------------------------------------------------------------------------- requests


def test_run_and_leadtime_picks_the_freshest_run():
    provider = cams.CAMSGlobalCompositionProvider()
    assert provider.run_and_leadtime(pd.Timestamp("2024-03-01T00:00")) == (
        pd.Timestamp("2024-03-01T00:00"),
        0,
    )
    assert provider.run_and_leadtime(pd.Timestamp("2024-03-01T11:00")) == (
        pd.Timestamp("2024-03-01T00:00"),
        11,
    )
    assert provider.run_and_leadtime(pd.Timestamp("2024-03-01T15:00")) == (
        pd.Timestamp("2024-03-01T12:00"),
        3,
    )


def test_composition_request_shape():
    provider = cams.CAMSGlobalCompositionProvider(variables=["total_column_ozone"])
    request = provider.build_request(pd.Timestamp("2024-03-01T15:00"))
    assert request["date"] == ["2024-03-01/2024-03-01"]
    assert request["time"] == ["12:00"]
    assert request["leadtime_hour"] == ["3"]
    assert request["variable"] == ["total_column_ozone"]
    assert request["data_format"] == "netcdf_zip"


def test_composition_fetch_skips_leads_beyond_the_archive(local_config, tmp_path):
    provider = cams.CAMSGlobalCompositionProvider(
        config=local_config, archive_dir=tmp_path / "archive"
    )
    provider.max_leadtime_hour = 2
    assert provider.fetch(pd.Timestamp("2024-03-01T05:00")) == []


def test_composition_fetch_reuses_an_existing_download(local_config, tmp_path, cams_zip):
    provider = cams.CAMSGlobalCompositionProvider(
        config=local_config, archive_dir=tmp_path / "archive"
    )
    it = pd.Timestamp("2024-03-01T15:00")
    target = provider.target_path(it)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(cams_zip.read_bytes())
    # No credentials are configured, so reaching the ADS at all would raise.
    assert provider.fetch(it) == [str(target)]


def test_weekly_request_shapes():
    europe = cams.CAMSEuropeAirQualityProvider(variables=["ozone"])
    request = europe.build_request(pd.Timestamp("2024-03-04"), "ozone")
    assert request["model"] == ["ensemble"]
    assert request["level"] == list(cams.CAMS_EUROPE_LEVELS)
    assert len(request["leadtime_hour"]) == 97

    aod = cams.CAMSGlobalAODProvider()
    request = aod.build_request(pd.Timestamp("2024-03-04"), "total_aerosol_optical_depth_550nm")
    assert request["time"] == ["00:00", "12:00"]
    assert len(request["leadtime_hour"]) == 121
    assert aod.append_dim == "init_time"


def test_weekly_request_dates_do_not_overlap_the_next_partition():
    """The ADS date range is inclusive, so consecutive weeks must not share a day."""
    provider = cams.CAMSGlobalAODProvider()
    first = provider.build_request(pd.Timestamp("2024-03-04"), "dust")["date"]
    second = provider.build_request(pd.Timestamp("2024-03-11"), "dust")["date"]
    assert first == ["2024-03-04/2024-03-10"]
    assert second == ["2024-03-11/2024-03-17"]

    # The archive file name still spans the half-open Dagster window.
    assert provider.target_path(pd.Timestamp("2024-03-04"), "dust").name == (
        "20240304-20240311_dust.nc.zip"
    )


def test_a_week_missing_a_variable_fails_rather_than_writing_a_subset(local_config, tmp_path):
    """Regression: one failed ADS request was enough to write a partial week.

    On the week that creates the store that bakes in a partial variable set and locks every
    complete week out afterwards; on a later week the write is silently refused and the asset
    still reports success. Neither is visible without someone reading the logs.
    """
    provider = cams.CAMSGlobalAODProvider(
        config=local_config,
        archive_dir=tmp_path / "archive",
        variables=["dust", "sea_salt"],
    )
    provider._retrieve = lambda request, dst: None if "sea_salt" in request["variable"] else dst

    with pytest.raises(cams.IncompleteWeek, match="1 of 2 variable"):
        provider.fetch(pd.Timestamp("2024-03-04"))


def test_a_week_with_nothing_at_all_is_a_skip_not_a_failure(local_config, tmp_path):
    """Nothing published is "no work to do"; half published is an error."""
    provider = cams.CAMSGlobalAODProvider(
        config=local_config, archive_dir=tmp_path / "archive", variables=["dust"]
    )
    provider._retrieve = lambda request, dst: None

    assert provider.fetch(pd.Timestamp("2024-03-04")) == []


def test_archive_dir_defaults_under_the_data_dir(local_config):
    provider = cams.CAMSGlobalCompositionProvider(config=local_config)
    assert provider.archive_dir == local_config.data_dir / "cams" / "composition"


def test_store_paths_are_local_under_test(local_config):
    provider = cams.CAMSGlobalCompositionProvider(config=local_config)
    assert provider.store_path.endswith("bkr/cams/cams_analysis_and_forecast.icechunk")
    assert not provider.store_path.startswith("s3://")


# --------------------------------------------------------------------------- store


def test_run_partition_writes_and_is_idempotent(local_config, tmp_path, cams_zip, monkeypatch):
    provider = cams.CAMSGlobalCompositionProvider(
        config=local_config, archive_dir=tmp_path / "archive"
    )
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: [str(cams_zip)])

    it = RUN + pd.Timedelta(hours=LEAD_HOURS)
    assert provider.run_partition(it) is True
    assert provider.run_partition(it) is False

    stored = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store,
                          consolidated=False)
    assert np.datetime64(it) in stored["time"].values
    assert "total_aerosol_optical_depth_at_550nm" in stored.data_vars
    assert stored["total_aerosol_optical_depth_at_550nm"].dtype == np.float32
