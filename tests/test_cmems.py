"""Offline tests for the consolidated Copernicus Marine providers.

Nothing here touches the network: downloads are exercised through monkeypatched toolbox
functions, and processing runs on synthetic NetCDF files.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets import config as config_module
from planetary_datasets.config import Config, MissingCredential
from planetary_datasets.providers import cmems
from planetary_datasets.providers.cmems import client as cmems_client
from planetary_datasets.providers.cmems.global_ocean import _day_files


def _wave_day(day: str, n_time: int = 3, descending_lat: bool = True) -> xr.Dataset:
    """A tiny stand-in for one day of a regional wave product."""
    times = pd.date_range(day, periods=n_time, freq="1h")
    lat = np.array([3.0, 2.0, 1.0]) if descending_lat else np.array([1.0, 2.0, 3.0])
    lon = np.array([10.0, 11.0])
    return xr.Dataset(
        {
            "VHM0": (
                ("time", "latitude", "longitude"),
                np.arange(n_time * 3 * 2, dtype="float32").reshape(n_time, 3, 2),
            )
        },
        coords={"time": times, "latitude": lat, "longitude": lon},
    )


@pytest.fixture
def credentialed_config(local_config, monkeypatch):
    """``local_config`` with Copernicus Marine credentials set."""
    monkeypatch.setenv("COPERNICUSMARINE_SERVICE_USERNAME", "someone")
    monkeypatch.setenv("COPERNICUSMARINE_SERVICE_PASSWORD", "secret")
    return config_module.load_config()


# --------------------------------------------------------------------------- region table


def test_region_table_is_self_consistent():
    assert set(cmems.WAVE_REGIONS) == {"arctic", "baltic", "ibi", "medsea", "nwshelf"}
    dataset_ids = set()
    for key, region in cmems.WAVE_REGIONS.items():
        assert region.key == key
        assert region.name == f"cmems_wave_{key}"
        assert region.store_prefix == f"bkr/cmems-wave/{key}.icechunk"
        assert region.variables == cmems.WAVE_VARIABLES
        pd.Timestamp(region.start_date)
        dataset_ids.add(region.dataset_id)
    assert len(dataset_ids) == len(cmems.WAVE_REGIONS)


@pytest.mark.parametrize(
    ("key", "dataset_id"),
    [
        # The Arctic product keeps its legacy dataset id.
        ("arctic", "dataset-wam-arctic-1hr3km-be"),
        # The pre-consolidation module had NWSHELF pointing at the IBI dataset id.
        ("nwshelf", "cmems_mod_nws_wav_anfc_1.5km_PT1H-i"),
        ("medsea", "cmems_mod_med_wav_anfc_4.2km_PT1H-i"),
    ],
)
def test_upstream_dataset_ids(key, dataset_id):
    assert cmems.CMEMSWaveProvider(key).dataset_id == dataset_id


def test_wave_provider_accepts_key_or_region():
    by_key = cmems.CMEMSWaveProvider("medsea")
    by_region = cmems.CMEMSWaveProvider(cmems.WAVE_REGIONS["medsea"])
    assert by_key.name == by_region.name == "cmems_wave_medsea"


def test_unknown_region_names_the_known_ones():
    with pytest.raises(KeyError, match="nwshelf"):
        cmems.CMEMSWaveProvider("atlantis")


def test_wave_providers_covers_every_region():
    assert {p.region.key for p in cmems.wave_providers()} == set(cmems.WAVE_REGIONS)


def test_store_path_uses_config(local_config):
    provider = cmems.CMEMSWaveProvider("baltic", config=local_config)
    assert provider.store_path.endswith("bkr/cmems-wave/baltic.icechunk")
    assert not provider.store_path.startswith("s3://")


def test_store_path_defaults_to_the_public_bucket():
    provider = cmems.CMEMSWaveProvider("arctic", config=Config())
    assert provider.store_path == (
        "s3://us-west-2.opendata.source.coop/bkr/cmems-wave/arctic.icechunk"
    )


# ----------------------------------------------------------------------------- credentials


def test_credentials_required(local_config):
    with pytest.raises(MissingCredential, match="COPERNICUSMARINE_SERVICE_USERNAME"):
        cmems.credentials(local_config)


def test_credentials_returned_when_set(credentialed_config):
    assert cmems.credentials(credentialed_config) == ("someone", "secret")


def test_fetch_without_credentials_raises(local_config, tmp_path):
    provider = cmems.CMEMSWaveProvider("ibi", config=local_config)
    with pytest.raises(MissingCredential):
        provider.fetch(pd.Timestamp("2026-01-01"), temp_dir=tmp_path)


# ------------------------------------------------------------------------------- download


def test_resolve_dataset_id_passes_through_raw_ids():
    assert cmems.resolve_dataset_id("global_wave_reanalysis") == (
        "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
    )
    assert cmems.resolve_dataset_id("cmems_mod_glo_wav_my_0.2deg_PT3H-i") == (
        "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
    )


def test_dataset_dir_lives_under_the_configured_data_dir(local_config):
    path = cmems.dataset_dir("global_phy_hourly", local_config)
    assert path == local_config.data_dir / "cmems" / "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"


def test_download_dataset_passes_credentials_and_config(
    monkeypatch, tmp_path, credentialed_config
):
    seen: dict = {}

    class _File:
        def __init__(self, path):
            self.file_path = path

    class _Response:
        files = [_File(tmp_path / "a.nc")]

    def fake_get(**kwargs):
        seen.update(kwargs)
        return _Response()

    monkeypatch.setattr("copernicusmarine.get", fake_get)

    paths = cmems.download_dataset(
        "global_phy_hourly", file_filter="*2026*", config=credentialed_config
    )

    assert [p.name for p in paths] == ["a.nc"]
    assert seen["dataset_id"] == "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"
    assert seen["username"] == "someone"
    assert seen["password"] == "secret"
    assert seen["filter"] == "*2026*"
    assert seen["output_directory"].startswith(str(tmp_path / "data"))


def test_download_datasets_continues_past_failures(monkeypatch, tmp_path):
    def boom(dataset, config=None, **kwargs):
        if dataset == "global_phy_hourly":
            raise RuntimeError("service unavailable")
        return [tmp_path / "ok.nc"]

    monkeypatch.setattr(cmems_client, "download_dataset", boom)
    results = cmems_client.download_datasets(["global_phy_hourly", "global_phy_sea_level"])

    assert results["cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"] == []
    assert len(results["cmems_mod_glo_phy_anfc_merged-sl_PT1H-i"]) == 1


def _no_data_for_this_day(**kwargs):
    raise RuntimeError("no data for this day")


@pytest.mark.parametrize(
    "subset",
    [lambda **kwargs: None, _no_data_for_this_day],
    ids=["missing_output", "toolbox_error"],
)
def test_subset_day_reports_a_missing_day_as_none(
    monkeypatch, tmp_path, credentialed_config, subset
):
    monkeypatch.setattr("copernicusmarine.subset", subset)
    assert (
        cmems_client.subset_day(
            "some-dataset",
            ["VHM0"],
            pd.Timestamp("2026-01-01"),
            tmp_path,
            "out.nc",
            credentialed_config,
        )
        is None
    )


def test_subset_day_reraises_malformed_requests(monkeypatch, tmp_path, credentialed_config):
    """An unknown variable is a bug, not a gap: it must not look like a missing day."""
    import copernicusmarine

    def boom(**kwargs):
        raise copernicusmarine.VariableDoesNotExistInTheDataset("NOPE")

    monkeypatch.setattr("copernicusmarine.subset", boom)
    with pytest.raises(copernicusmarine.VariableDoesNotExistInTheDataset):
        cmems_client.subset_day(
            "some-dataset",
            ["NOPE"],
            pd.Timestamp("2026-01-01"),
            tmp_path,
            "out.nc",
            credentialed_config,
        )


def test_wave_fetch_returns_empty_when_download_fails(monkeypatch, tmp_path, local_config):
    monkeypatch.setattr(
        "planetary_datasets.providers.cmems.wave.subset_day",
        lambda **kwargs: None,
    )
    provider = cmems.CMEMSWaveProvider("nwshelf", config=local_config)
    assert provider.fetch(pd.Timestamp("2026-01-01"), temp_dir=tmp_path) == []


# -------------------------------------------------------------------------------- process


def test_wave_process_sorts_and_chunks(tmp_path, local_config):
    path = tmp_path / "day.nc"
    _wave_day("2026-01-01", descending_lat=True).to_netcdf(path)

    provider = cmems.CMEMSWaveProvider("medsea", config=local_config)
    ds = provider.process([str(path)], pd.Timestamp("2026-01-01"))

    assert list(ds.latitude.values) == [1.0, 2.0, 3.0]
    assert ds.chunksizes["time"][0] == 1
    assert ds.chunksizes["longitude"] == (2,)


def test_wave_process_leaves_2d_coordinates_alone(tmp_path, local_config):
    """The Arctic grid is rotated: x/y have no 1-D coordinate to sort by."""
    lat2d = np.array([[1.0, 2.0], [3.0, 4.0]])
    ds = xr.Dataset(
        {"VHM0": (("time", "y", "x"), np.ones((2, 2, 2), dtype="float32"))},
        coords={
            "time": pd.date_range("2026-01-01", periods=2, freq="1h"),
            "latitude": (("y", "x"), lat2d),
            "longitude": (("y", "x"), lat2d + 10),
        },
    )
    path = tmp_path / "arctic.nc"
    ds.to_netcdf(path)

    provider = cmems.CMEMSWaveProvider("arctic", config=local_config)
    out = provider.process([str(path)], pd.Timestamp("2026-01-01"))

    assert out["latitude"].dims == ("y", "x")
    assert out.chunksizes["time"][0] == 1
    assert out.chunksizes["x"] == (2,)


def test_rename_to_standard_names():
    ds = xr.Dataset(
        {
            "VHM0": ("time", np.zeros(2, dtype="float32")),
            "unnamed": ("time", np.zeros(2, dtype="float32")),
        },
        coords={"time": pd.date_range("2026-01-01", periods=2)},
    )
    ds["VHM0"].attrs["standard_name"] = "sea_surface_wave_significant_height"
    out = cmems.rename_to_standard_names(ds)
    assert "sea_surface_wave_significant_height" in out.data_vars
    assert "unnamed" in out.data_vars


def test_rename_to_standard_names_skips_collisions():
    ds = xr.Dataset(
        {
            "a": ("time", np.zeros(2, dtype="float32")),
            "b": ("time", np.zeros(2, dtype="float32")),
        },
        coords={"time": pd.date_range("2026-01-01", periods=2)},
    )
    ds["a"].attrs["standard_name"] = "shared"
    ds["b"].attrs["standard_name"] = "shared"
    out = cmems.rename_to_standard_names(ds)
    assert set(out.data_vars) == {"shared", "b"}


def test_reanalysis_process_downcasts_and_deduplicates(tmp_path, local_config):
    ds = _wave_day("2026-01-01", n_time=3, descending_lat=False)
    ds["VHM0"].attrs["standard_name"] = "sea_surface_wave_significant_height"
    first = tmp_path / "a.nc"
    second = tmp_path / "b.nc"
    ds.isel(time=slice(0, 2)).to_netcdf(first)
    # Overlapping timesteps: the archive publishes analyses that overlap at day edges.
    ds.isel(time=slice(1, 3)).to_netcdf(second)

    provider = cmems.CMEMSGlobalWaveReanalysisProvider(config=local_config)
    out = provider.process([str(first), str(second)], pd.Timestamp("2026-01-01"))

    assert out.sizes["time"] == 3
    assert out["sea_surface_wave_significant_height"].dtype == np.float16
    assert out.time.to_index().is_monotonic_increasing


def test_reanalysis_fetch_finds_mirrored_files(local_config):
    provider = cmems.CMEMSGlobalWaveReanalysisProvider(config=local_config)
    day_dir = provider.source_dir / "2026" / "01"
    day_dir.mkdir(parents=True)
    wanted = day_dir / "mfwamglocep_2026010100_R20260102.nc"
    wanted.touch()
    (day_dir / "mfwamglocep_2026010200_R20260103.nc").touch()

    assert provider.fetch(pd.Timestamp("2026-01-01")) == [str(wanted)]


def test_day_files_ignores_the_production_date_stamp(tmp_path):
    """``..._R20260102.nc`` holds 1 January's data; it must not match 2 January."""
    first = tmp_path / "mfwamglocep_2026010100_R20260102.nc"
    second = tmp_path / "mfwamglocep_2026010200_R20260103.nc"
    first.touch()
    second.touch()

    assert _day_files(tmp_path, pd.Timestamp("2026-01-01")) == [first]
    assert _day_files(tmp_path, pd.Timestamp("2026-01-02")) == [second]
    assert _day_files(tmp_path, pd.Timestamp("2026-01-03")) == []


def test_day_files_on_a_missing_directory(tmp_path):
    assert _day_files(tmp_path / "nope", pd.Timestamp("2026-01-01")) == []


def test_reanalysis_fetch_skips_download_when_disabled(local_config):
    provider = cmems.CMEMSGlobalWaveReanalysisProvider(config=local_config)
    provider.download_if_missing = False
    assert provider.fetch(pd.Timestamp("2026-01-01")) == []


@pytest.fixture
def forecast(local_config):
    """A forecast provider with one synthetic day for each of its three datasets mirrored.

    The variable sets mirror the real catalogue, where all three products publish some of
    the same fields: the base product serves ``thetao``/``so``/``uo``/``vo``/``zos``, the
    currents product ``uo``/``vo`` and the sea-level product its own sea surface height.
    Returns the provider, the mirror root and the written paths (base, currents, sea level).
    """
    provider = cmems.CMEMSGlobalOceanForecastProvider(config=local_config)
    root = local_config.data_dir / "cmems"
    times = pd.date_range("2026-01-01", periods=2, freq="1h")
    lat = np.array([1.0, 2.0])
    lon = np.array([10.0, 11.0])
    fields = {
        provider.base_dataset_id: {
            "thetao": "sea_water_potential_temperature",
            "uo": "eastward_sea_water_velocity",
            "zos": "sea_surface_height_above_geoid",
        },
        provider.currents_dataset_id: {"uo": "eastward_sea_water_velocity"},
        provider.sea_level_dataset_id: {
            "total_sea_level": "sea_surface_height_above_geoid"
        },
    }
    paths = []
    for dataset_id, variables in fields.items():
        ds = xr.Dataset(
            {
                var: (
                    ("time", "latitude", "longitude"),
                    np.full((2, 2, 2), value, dtype="float32"),
                )
                for value, var in enumerate(variables, start=1)
            },
            coords={"time": times, "latitude": lat, "longitude": lon},
        )
        for var, standard_name in variables.items():
            ds[var].attrs["standard_name"] = standard_name
        directory = root / dataset_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{dataset_id}_2026010100.nc"
        ds.to_netcdf(path)
        paths.append(path)
    return provider, root, paths


def test_forecast_fetch_requires_every_dataset(forecast):
    provider, root, _ = forecast
    assert len(provider.fetch(pd.Timestamp("2026-01-01"))) == 3

    # Remove one dataset's file: the day is no longer complete.
    (root / provider.currents_dataset_id).rename(root / "unused")
    assert provider.fetch(pd.Timestamp("2026-01-01")) == []


def test_forecast_fetch_skips_ambiguous_days(forecast):
    """Two production runs for one day: there is no way to tell which to trust."""
    provider, root, _ = forecast
    duplicate = root / provider.base_dataset_id / "rerun_2026010100_R20260105.nc"
    duplicate.touch()
    assert provider.fetch(pd.Timestamp("2026-01-01")) == []


def test_forecast_process_merges_onto_the_sea_level_grid(forecast):
    provider, _, paths = forecast
    out = provider.process([str(p) for p in reversed(paths)], pd.Timestamp("2026-01-01"))

    assert set(out.data_vars) == {
        "sea_water_potential_temperature",
        "eastward_sea_water_velocity",
        "sea_surface_height_above_geoid",
    }
    assert out["sea_water_potential_temperature"].dtype == np.float32
    assert out["eastward_sea_water_velocity"].dtype == np.float16
    assert out.chunksizes["time"][0] == 1


def test_forecast_process_prefers_the_most_specific_product(forecast):
    """Overlapping fields must not raise, and must come from the specialised product."""
    provider, _, paths = forecast
    out = provider.process([str(p) for p in paths], pd.Timestamp("2026-01-01"))

    # The currents product is the only source with a single variable, written with value
    # 1; the base product's own eastward velocity was written with value 2.
    assert float(out["eastward_sea_water_velocity"].isel(time=0).max()) == 1.0
    # Sea level comes from the sea-level product (value 1), not the base product (3).
    assert float(out["sea_surface_height_above_geoid"].isel(time=0).max()) == 1.0


def test_forecast_process_rejects_incomplete_inputs(forecast):
    provider, _, paths = forecast
    with pytest.raises(ValueError, match="no input file"):
        provider.process([str(paths[0])], pd.Timestamp("2026-01-01"))


# ------------------------------------------------------------------------- store round trip


def test_run_partition_writes_and_skips(tmp_path, local_config, monkeypatch):
    """A full partition through fetch, process and the store, with the download faked."""
    day = pd.Timestamp("2026-01-01")
    source = tmp_path / "day.nc"
    _wave_day("2026-01-01", n_time=2).to_netcdf(source)

    monkeypatch.setattr(
        "planetary_datasets.providers.cmems.wave.subset_day",
        lambda **kwargs: source,
    )
    provider = cmems.CMEMSWaveProvider("baltic", config=local_config)

    assert provider.run_partition(day) is True
    # The timestep is in the store now, so a second run is a no-op.
    assert provider.run_partition(day) is False

    stored = read_store(provider)
    assert pd.Timestamp(stored.time.values[0]) == day
    assert "VHM0" in stored.data_vars


def test_dagster_assets_load():
    import dagster as dg

    from dags.assets import cmems as cmems_assets

    defs = dg.Definitions(assets=cmems_assets.all_assets)
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert "ocean/cmems_wave_medsea" in keys
    assert "ocean/cmems_global_ocean_forecast" in keys
    assert len(cmems_assets.all_assets) == len(cmems.WAVE_REGIONS) + 3
