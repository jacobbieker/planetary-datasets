"""Offline tests for the regional limited-area model providers.

Nothing here touches the network or the real archives: the GRIB engine is exercised with
hand-built xarray datasets shaped like what cfgrib returns, and the store round-trip runs
against a local icechunk directory.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import dmi_harmonie as dmi
from planetary_datasets.providers import hawaii_nam, hrrr_alaska, kenda
from planetary_datasets.providers.regional_lam_common import (
    GribMergeSpec,
    chunk_present,
    clean_grib_subset,
    download_with_filesystem,
    init_time_download_dir,
    long_name_slug,
    rename_present,
    resolve_renames,
    soil_level_count,
)

ALL_PROVIDERS = [
    dmi.DMIHarmonieProvider,
    dmi.DMIHarmonieModelLevelProvider,
    hrrr_alaska.AlaskaHRRRProvider,
    hawaii_nam.HawaiiNAMProvider,
    kenda.KENDAAnalysisProvider,
    kenda.KENDAForecastProvider,
]


# --------------------------------------------------------------------------- helpers


def _surface_subset(**coords) -> xr.Dataset:
    """A cfgrib-shaped sub-dataset with one surface field on a y/x grid."""
    ds = xr.Dataset(
        {"t": (("y", "x"), np.zeros((2, 3), dtype="float32"))},
        coords={"y": np.arange(2), "x": np.arange(3), "surface": 0.0, "time": pd.Timestamp("2026-01-01")},
    )
    ds["t"].attrs["long_name"] = "Temperature"
    return ds.assign_coords(coords) if coords else ds


def _alaska_step(step_hours: int) -> xr.Dataset:
    ds = xr.Dataset(
        {"temperature_at_surface": (("isobaricInhPa", "y", "x"), np.ones((2, 2, 3), dtype="float32"))},
        coords={
            "isobaricInhPa": np.array([500.0, 1000.0]),
            "y": np.arange(2),
            "x": np.arange(3),
            "step": pd.Timedelta(hours=step_hours),
            "time": pd.Timestamp("2026-01-01T00:00"),
        },
    )
    return ds


def _hawaii_step(valid_time: str) -> xr.Dataset:
    return xr.Dataset(
        {
            "temperature_at_surface": (("isobaricInhPa", "values"), np.ones((2, 3), dtype="float32")),
            "soil_temperature": (("depthBelowLandLayer", "values"), np.ones((2, 3), dtype="float32")),
        },
        coords={
            "isobaricInhPa": np.array([500.0, 1000.0]),
            "depthBelowLandLayer": np.array([0.4, 0.1]),
            "values": np.arange(3),
            "valid_time": pd.Timestamp(valid_time),
        },
    )


# ------------------------------------------------------------------ provider contract


@pytest.mark.parametrize("provider_cls", ALL_PROVIDERS)
def test_providers_declare_the_base_contract(provider_cls):
    assert provider_cls.name
    assert provider_cls.append_dim == "time"
    assert provider_cls.store_prefix.startswith("bkr/dmi/")
    assert provider_cls.store_prefix.endswith(".icechunk")


def test_store_prefixes_are_distinct():
    prefixes = [cls.store_prefix for cls in ALL_PROVIDERS]
    assert len(set(prefixes)) == len(prefixes)


def test_kenda_store_prefixes_match_the_original_scripts():
    assert kenda.KENDAAnalysisProvider.store_prefix == "bkr/dmi/kenda_switzerland.icechunk"
    assert kenda.KENDAForecastProvider.store_prefix == "bkr/dmi/kenda_forecast_switzerland.icechunk"
    # KENDAProvider used to expose icechunk_path as a property; it now resolves to the
    # analysis store via a plain class attribute like every other provider.
    assert kenda.KENDAProvider.store_prefix == kenda.KENDAAnalysisProvider.store_prefix
    assert isinstance(type(kenda.KENDAProvider).__dict__.get("store_prefix", None), type(None))


@pytest.mark.parametrize("provider_cls", ALL_PROVIDERS)
def test_no_hardcoded_credentials_in_provider_sources(provider_cls):
    import inspect

    source = inspect.getsource(inspect.getmodule(provider_cls))
    assert "AKIA" not in source
    assert "access_key" not in source


def test_store_path_follows_the_local_override(local_config):
    provider = hrrr_alaska.AlaskaHRRRProvider(config=local_config)
    assert provider.store_path.endswith("bkr/dmi/alaska_hrrr.icechunk")
    assert not provider.store_path.startswith("s3://")


# ------------------------------------------------------------------------- file naming


def test_alaska_urls_cover_every_level_type_and_step():
    urls = hrrr_alaska.AlaskaHRRRProvider().expected_urls(pd.Timestamp("2024-01-10T06:00"))
    assert len(urls) == 9
    assert urls[0] == (
        "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20240110/alaska/"
        "hrrr.t06z.wrfsfcf00.ak.grib2"
    )
    assert urls[-1].endswith("hrrr.t06z.wrfprsf02.ak.grib2")
    assert all(url.startswith("https://") for url in urls)


def test_hawaii_urls_cover_six_forecast_hours():
    urls = hawaii_nam.HawaiiNAMProvider().expected_urls(pd.Timestamp("2025-05-17T06:00"))
    assert len(urls) == 6
    assert urls[0] == (
        "https://noaa-nam-pds.s3.amazonaws.com/nam.20250517/"
        "nam.t06z.hawaiinest.hiresf00.tm00.grib2"
    )
    assert urls[-1].endswith("hiresf05.tm00.grib2")


def test_harmonie_keys_pair_init_time_with_valid_time():
    provider = dmi.DMIHarmonieProvider()
    key = provider.remote_key(pd.Timestamp("2026-03-28T00:00"), 2, "PL")
    assert key == (
        "dmi-opendata/forecastdata/HARMONIE_IG_PL/"
        "HARMONIE_IG_PL_2026-03-28T000000Z_2026-03-28T020000Z.grib"
    )
    ml = dmi.DMIHarmonieModelLevelProvider().remote_key(pd.Timestamp("2026-03-28T00:00"), 0, "ML")
    assert ml.endswith("HARMONIE_IG_ML_2026-03-28T000000Z_2026-03-28T000000Z.grib")


# ------------------------------------------------------------------------ grib engine


def test_long_name_slug_only_strips_parens_when_asked():
    assert long_name_slug("Best (4-layer) Lifted Index") == "best_(4-layer)_lifted_index"
    assert (
        long_name_slug("Best (4-layer) Lifted Index", strip_parens=True)
        == "best_4-layer_lifted_index"
    )


def test_soil_level_count_treats_a_scalar_as_no_profile():
    scalar = xr.DataArray(0.1)
    profile = xr.DataArray(np.linspace(0, 2, 9), dims="depthBelowLandLayer")
    assert soil_level_count(scalar) == 0
    assert soil_level_count(profile) == 9


def test_unwanted_level_types_are_dropped_whole():
    spec = GribMergeSpec()
    assert clean_grib_subset(_surface_subset(tropopause=0.0), spec) is None
    assert clean_grib_subset(_surface_subset(potentialVorticity=2e-6), spec) is None


def test_height_above_ground_as_a_dimension_is_dropped():
    ds = xr.Dataset(
        {"t": (("heightAboveGround", "y", "x"), np.zeros((2, 2, 3), dtype="float32"))},
        coords={"heightAboveGround": [2.0, 10.0], "y": np.arange(2), "x": np.arange(3)},
    )
    assert clean_grib_subset(ds, GribMergeSpec()) is None


def test_soil_profile_threshold_differs_between_the_two_nests():
    ds = xr.Dataset(
        {"st": (("depthBelowLandLayer", "y", "x"), np.zeros((4, 2, 3), dtype="float32"))},
        coords={"depthBelowLandLayer": np.linspace(0, 2, 4), "y": np.arange(2), "x": np.arange(3)},
    )
    ds["st"].attrs["long_name"] = "Soil Temperature"
    # HRRR Alaska only wants the full 9-layer profile; the Hawaii nest keeps this one.
    assert clean_grib_subset(ds, hrrr_alaska.ALASKA_MERGE_SPEC) is None
    kept = clean_grib_subset(ds, hawaii_nam.HAWAII_MERGE_SPEC)
    assert kept is not None
    assert "soil_temperature" in kept.data_vars


def test_variables_are_renamed_by_long_name_with_a_level_suffix():
    kept = clean_grib_subset(_surface_subset(), GribMergeSpec())
    assert list(kept.data_vars) == ["temperature_at_surface"]
    assert "surface" not in kept.coords


def test_height_above_ground_scalar_becomes_a_name_suffix():
    ds = _surface_subset()
    ds = ds.drop_vars("surface").assign_coords(heightAboveGround=2.0)
    kept = clean_grib_subset(ds, GribMergeSpec())
    assert list(kept.data_vars) == ["temperature_at_2.0m"]
    assert "heightAboveGround" not in kept.coords


def test_undecodable_and_simulated_imagery_variables_are_dropped():
    ds = xr.Dataset(
        {
            "unknown": (("y", "x"), np.zeros((2, 3), dtype="float32")),
            "SBT124": (("y", "x"), np.zeros((2, 3), dtype="float32")),
            "refc": (("y", "x"), np.zeros((2, 3), dtype="float32")),
        },
        coords={"y": np.arange(2), "x": np.arange(3), "surface": 0.0},
    )
    assert clean_grib_subset(ds, GribMergeSpec()) is None


def test_deprecated_fields_are_dropped():
    ds = _surface_subset()
    ds["t"].attrs["long_name"] = "Something deprecated"
    assert clean_grib_subset(ds, GribMergeSpec()) is None


def test_a_lone_wind_component_is_dropped():
    """A wind component with no partner is dropped; it arrives again on a kept level."""
    ds = xr.Dataset(
        {"u": (("isobaricInhPa", "y", "x"), np.zeros((2, 2, 3), dtype="float32"))},
        coords={"isobaricInhPa": [500.0, 1000.0], "y": np.arange(2), "x": np.arange(3)},
    )
    ds["u"].attrs["long_name"] = "U component of wind"
    assert clean_grib_subset(ds, GribMergeSpec()) is None


def test_a_bare_geopotential_height_field_is_dropped():
    ds = xr.Dataset(
        {"gh": (("y", "x"), np.zeros((2, 3), dtype="float32"))},
        coords={"y": np.arange(2), "x": np.arange(3)},
    )
    ds["gh"].attrs["long_name"] = "Geopotential Height"
    assert clean_grib_subset(ds, GribMergeSpec()) is None


def test_hawaii_drops_its_extra_duplicate_blocks():
    spec = hawaii_nam.HAWAII_MERGE_SPEC
    pressure_only = xr.Dataset(
        {"pres": (("y", "x"), np.zeros((2, 3), dtype="float32"))},
        coords={"y": np.arange(2), "x": np.arange(3)},
    )
    pressure_only["pres"].attrs["long_name"] = "Pressure"
    assert clean_grib_subset(pressure_only, spec) is None

    flux_block = xr.Dataset(
        {
            name: (("y", "x"), np.zeros((2, 3), dtype="float32"))
            for name in ("prate", "dswrf", "gflux")
        },
        coords={"y": np.arange(2), "x": np.arange(3), "surface": 0.0},
    )
    flux_block["prate"].attrs["long_name"] = "Precipitation rate"
    flux_block["dswrf"].attrs["long_name"] = "Surface downward short-wave radiation flux"
    flux_block["gflux"].attrs["long_name"] = "Ground heat flux"
    assert clean_grib_subset(flux_block, spec) is None


# ------------------------------------------------------------------------- combining


def test_alaska_combine_stacks_steps_under_one_init_time():
    provider = hrrr_alaska.AlaskaHRRRProvider()
    it = pd.Timestamp("2026-01-01T00:00")
    ds = provider.combine([_alaska_step(0), _alaska_step(1), _alaska_step(2)], it)

    assert ds.sizes["time"] == 1
    assert ds.sizes["step"] == 3
    assert pd.Timestamp(ds.time.values[0]) == it
    # Pressure levels are stored descending, as the store was created.
    assert list(ds.level.values) == [1000.0, 500.0]
    assert ds["temperature_at_surface"].dtype == np.dtype("float16")


def test_hawaii_combine_turns_forecast_hours_into_a_time_axis():
    provider = hawaii_nam.HawaiiNAMProvider()
    steps = [_hawaii_step(f"2026-01-01T{hour:02d}:00") for hour in range(3)]
    ds = provider.combine(steps, pd.Timestamp("2026-01-01T00:00"))

    assert ds.sizes["time"] == 3
    assert list(pd.DatetimeIndex(ds.time.values).hour) == [0, 1, 2]
    assert list(ds.level.values) == [1000.0, 500.0]
    assert list(ds.depth.values) == [0.1, 0.4]
    assert ds["soil_temperature"].dtype == np.dtype("float16")


def test_nest_process_rejects_an_incomplete_file_set():
    provider = hrrr_alaska.AlaskaHRRRProvider()
    with pytest.raises(ValueError, match="expected 9 files"):
        provider.process(["a.grib2"], pd.Timestamp("2026-01-01T00:00"))


def test_harmonie_process_rejects_an_incomplete_file_set():
    provider = dmi.DMIHarmonieProvider()
    with pytest.raises(ValueError, match="expected 6 files"):
        provider.process(["a.grib", "b.grib"], pd.Timestamp("2026-01-01T00:00"))


# ------------------------------------------------------------------- dataset helpers


def test_rename_present_ignores_names_that_are_not_there():
    ds = xr.Dataset({"a": ("isobaricInhPa", np.zeros(2))}, coords={"isobaricInhPa": [1.0, 2.0]})
    renamed = rename_present(ds, {"isobaricInhPa": "level", "depthBelowLandLayer": "depth"})
    assert "level" in renamed.dims
    assert "depth" not in renamed.dims


def test_resolve_renames_drops_sources_that_are_not_there():
    ds = xr.Dataset({"a": ("x", np.zeros(2))})
    assert resolve_renames(ds, {"a": "alpha", "twater": "total_water"}) == {"a": "alpha"}


def test_resolve_renames_lets_the_first_claim_on_a_name_win():
    """GRIB reuses a long_name across level types; renaming both would raise."""
    ds = xr.Dataset({"a": ("x", np.zeros(2)), "b": ("x", np.zeros(2))})
    assert resolve_renames(ds, {"a": "shared", "b": "shared"}) == {"a": "shared"}
    assert ds.rename(resolve_renames(ds, {"a": "shared", "b": "shared"})) is not None


def test_resolve_renames_will_not_collide_with_a_variable_left_alone():
    ds = xr.Dataset({"a": ("x", np.zeros(2)), "keep": ("x", np.zeros(2))})
    assert resolve_renames(ds, {"a": "keep"}) == {}


def test_resolve_renames_rechecks_after_dropping_a_rename():
    """Dropping b->c leaves b in place, so a->b must be dropped too, not just b->c."""
    ds = xr.Dataset({name: ("x", np.zeros(2)) for name in ("a", "b", "c")})
    resolved = resolve_renames(ds, {"a": "b", "b": "c"})
    assert resolved == {}
    ds.rename(resolved)  # would raise if the mapping still conflicted


def test_resolve_renames_allows_a_simultaneous_swap():
    ds = xr.Dataset({"a": ("x", np.zeros(2)), "b": ("x", np.ones(2))})
    resolved = resolve_renames(ds, {"a": "b", "b": "a"})
    assert resolved == {"a": "b", "b": "a"}
    swapped = ds.rename(resolved)
    assert list(swapped["a"].values) == [1.0, 1.0]


def test_a_duplicated_long_name_drops_the_loser_rather_than_the_timestep():
    """Two messages sharing a long_name would make Dataset.rename raise."""
    ds = xr.Dataset(
        {
            "t": (("y", "x"), np.zeros((2, 3), dtype="float32")),
            "t2": (("y", "x"), np.ones((2, 3), dtype="float32")),
            "r": (("y", "x"), np.ones((2, 3), dtype="float32")),
        },
        coords={"y": np.arange(2), "x": np.arange(3), "surface": 0.0},
    )
    ds["t"].attrs["long_name"] = "Temperature"
    ds["t2"].attrs["long_name"] = "Temperature"
    ds["r"].attrs["long_name"] = "Relative humidity"

    kept = clean_grib_subset(ds, GribMergeSpec())
    assert sorted(kept.data_vars) == ["relative_humidity_at_surface", "temperature_at_surface"]


def test_download_dir_separates_init_times_when_no_temp_dir_is_given(tmp_path):
    first = init_time_download_dir(tmp_path, "alaska_hrrr", pd.Timestamp("2026-01-01T06:00"))
    second = init_time_download_dir(tmp_path, "alaska_hrrr", pd.Timestamp("2026-01-02T06:00"))
    assert first != second
    assert first.parent == second.parent == tmp_path / "alaska_hrrr"


def test_download_dir_honours_an_explicit_temp_dir(tmp_path):
    assert init_time_download_dir(
        tmp_path / "scratch", "alaska_hrrr", pd.Timestamp("2026-01-01T06:00"), tmp_path / "given"
    ) == (tmp_path / "given")


def test_chunk_present_ignores_dimensions_that_are_not_there():
    ds = xr.Dataset({"a": (("time", "x"), np.zeros((2, 3)))})
    chunked = chunk_present(ds, {"time": 1, "x": -1, "level": -1})
    assert chunked.chunksizes["time"] == (1, 1)


def test_harmonie_surface_height_split_keeps_the_unsuffixed_pressure_name():
    ds = xr.Dataset(
        {"pres": (("y", "x"), np.zeros((2, 3), dtype="float32"))},
        coords={"y": np.arange(2), "x": np.arange(3), "heightAboveGround": 0.0},
    )
    split = dmi._split_by_height(ds, "heightAboveGround", "height_above_ground", rename_scalar=False)
    assert list(split.data_vars) == ["pres"]
    assert "heightAboveGround" not in split.coords


def test_harmonie_surface_height_split_fans_out_a_height_stack():
    ds = xr.Dataset(
        {"t": (("heightAboveGround", "y", "x"), np.zeros((2, 2, 3), dtype="float32"))},
        coords={"heightAboveGround": [2.0, 10.0], "y": np.arange(2), "x": np.arange(3)},
    )
    split = dmi._split_by_height(ds, "heightAboveGround", "height_above_ground")
    assert sorted(split.data_vars) == [
        "t_at_height_above_ground_10.0",
        "t_at_height_above_ground_2.0",
    ]
    assert "heightAboveGround" not in split.dims


# ------------------------------------------------------------------------------ KENDA


def test_kenda_archive_path_defaults_under_the_data_dir(local_config, tmp_path, monkeypatch):
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    from planetary_datasets import config as config_module

    config_module.reset_config_cache()
    provider = kenda.KENDAAnalysisProvider()
    assert provider.archive_path == tmp_path / "data" / "meteoswiss"


def test_kenda_archive_path_can_be_overridden(tmp_path):
    provider = kenda.KENDAForecastProvider(archive_path=tmp_path)
    assert provider.archive_path == tmp_path


def test_kenda_fetch_returns_nothing_when_the_feed_is_empty(tmp_path):
    provider = kenda.KENDAAnalysisProvider(archive_path=tmp_path)
    assert provider.fetch(pd.Timestamp("2026-06-20T02:00")) == []


def test_kenda_fetch_requires_the_constants(tmp_path):
    it = pd.Timestamp("2026-06-20T02:00")
    (tmp_path / f"kenda-ch1-{it.strftime('%Y%m%d%H00')}-0-t-ctrl.grib2").write_bytes(b"")
    provider = kenda.KENDAAnalysisProvider(archive_path=tmp_path)
    assert provider.fetch(it) == []

    (tmp_path / kenda.HORIZONTAL_CONSTANTS).write_bytes(b"")
    (tmp_path / kenda.VERTICAL_CONSTANTS).write_bytes(b"")
    found = provider.fetch(it)
    assert len(found) == 3
    assert sum("constants" in f for f in found) == 2


def test_kenda_analysis_and_forecast_read_different_steps(tmp_path):
    it = pd.Timestamp("2026-06-20T02:00")
    stamp = it.strftime("%Y%m%d%H00")
    (tmp_path / kenda.HORIZONTAL_CONSTANTS).write_bytes(b"")
    (tmp_path / kenda.VERTICAL_CONSTANTS).write_bytes(b"")
    (tmp_path / f"kenda-ch1-{stamp}-0-t-ctrl.grib2").write_bytes(b"")
    (tmp_path / f"kenda-ch1-{stamp}-1-t-ctrl.grib2").write_bytes(b"")

    analysis = kenda.KENDAAnalysisProvider(archive_path=tmp_path).fetch(it)
    forecast = kenda.KENDAForecastProvider(archive_path=tmp_path).fetch(it)
    assert any(f"-{stamp}-0-" in f for f in analysis)
    assert not any(f"-{stamp}-1-" in f for f in analysis)
    assert any(f"-{stamp}-1-" in f for f in forecast)


def test_kenda_load_constants_needs_both_files():
    with pytest.raises(FileNotFoundError):
        kenda.load_constants(["horizontal_constants_kenda-ch1.grib2"])


# ---------------------------------------------------------------------------- download


class _FlakyFilesystem:
    """Filesystem that fails a fixed number of times before succeeding."""

    def __init__(self, failures: int):
        self.failures = failures
        self.calls = 0

    def get(self, remote, local):
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient")
        with open(local, "wb") as out:
            out.write(b"grib")


def test_download_with_filesystem_retries_then_succeeds(tmp_path):
    fs = _FlakyFilesystem(failures=2)
    dest = download_with_filesystem(fs, "bucket/key.grib", tmp_path / "key.grib", backoff=0)
    assert dest is not None
    assert dest.read_bytes() == b"grib"
    assert fs.calls == 3


def test_download_with_filesystem_leaves_no_partial_file(tmp_path):
    fs = _FlakyFilesystem(failures=99)
    assert download_with_filesystem(fs, "bucket/key.grib", tmp_path / "key.grib", backoff=0) is None
    assert list(tmp_path.iterdir()) == []


def test_download_with_filesystem_skips_an_existing_file(tmp_path):
    dest = tmp_path / "key.grib"
    dest.write_bytes(b"already here")
    fs = _FlakyFilesystem(failures=99)
    assert download_with_filesystem(fs, "bucket/key.grib", dest) == dest
    assert fs.calls == 0


def test_nest_fetch_skips_an_init_time_with_missing_files(tmp_path, monkeypatch):
    provider = hrrr_alaska.AlaskaHRRRProvider()

    def _only_two(urls, dest_dir, **kwargs):
        return [tmp_path / "a", tmp_path / "b"]

    monkeypatch.setattr(hrrr_alaska.GribNestProvider.__module__ + ".download_many", _only_two)
    from planetary_datasets.providers import regional_lam_common

    monkeypatch.setattr(regional_lam_common, "download_many", _only_two)
    assert provider.fetch(pd.Timestamp("2026-01-01T00:00"), temp_dir=tmp_path) == []


# --------------------------------------------------------------------- store roundtrip


def test_run_partition_writes_and_then_skips(local_config, monkeypatch):
    """A full pass through BaseProvider.run_partition against a local store."""
    provider = hrrr_alaska.AlaskaHRRRProvider(config=local_config)
    it = pd.Timestamp("2026-01-01T00:00")

    monkeypatch.setattr(type(provider), "fetch", lambda self, it, temp_dir=None, **kw: ["a"] * 9)
    monkeypatch.setattr(
        type(provider),
        "process",
        lambda self, files, it, temp_dir=None, **kw: self.combine(
            [_alaska_step(0), _alaska_step(1), _alaska_step(2)], it
        ),
    )

    assert provider.run_partition(it) is True
    assert provider.run_partition(it) is False

    import xarray as xr_

    stored = xr_.open_zarr(
        provider.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    assert pd.Timestamp(stored.time.values[0]) == it
    assert stored.sizes["step"] == 3
