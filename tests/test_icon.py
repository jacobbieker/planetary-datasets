"""Offline tests for the DWD ICON providers. Nothing here touches the network."""

from __future__ import annotations

import dataclasses
import datetime as dt

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.config import MissingCredential
from planetary_datasets.providers import icon


@pytest.fixture
def eu_subset():
    """A tiny ICON-EU variant, small enough to enumerate its URLs in a test."""
    return dataclasses.replace(
        icon.EUROPE_CONFIG,
        store_prefix="bkr/icon/test_eu.icechunk",
        vars_2d=["t_2m"],
        vars_3d=["t@500", "t@850"],
        f_steps=[0, 1],
    )


@pytest.fixture
def art_subset():
    """A tiny ICON-ART variant, which uses the newer path-based layout."""
    return dataclasses.replace(
        icon.GLOBAL_ART_ANALYSIS_CONFIG,
        store_prefix="bkr/icon/test_art.icechunk",
        vars_2d=["T_2M"],
        vars_3d=["FI@50000"],
        vars_model=["T@120"],
        vars_soil=["T_SO@0.0"],
        vars_wavelength=["CEIL_BSC_DUST@532@120"],
        vars_invariant=["clat"],
        f_steps=[0],
    )


# -- configuration ----------------------------------------------------------------------


def test_every_variant_is_valid():
    for name, config in icon.VARIANTS.items():
        assert config.store_prefix.startswith("bkr/icon/"), name
        assert config.store_prefix.endswith(".icechunk"), name
        assert config.url_style in ("filename", "path"), name


def test_variant_store_prefixes_are_unique():
    prefixes = [c.store_prefix for c in icon.VARIANTS.values()]
    assert len(prefixes) == len(set(prefixes))


def test_config_rejects_unknown_url_style():
    with pytest.raises(ValueError, match="url_style"):
        dataclasses.replace(icon.EUROPE_CONFIG, url_style="ftp")


def test_config_rejects_empty_variable_lists():
    with pytest.raises(ValueError, match="at least one"):
        dataclasses.replace(icon.EUROPE_CONFIG, vars_2d=[], vars_3d=[])


def test_case_follows_the_variant():
    assert icon.GLOBAL_CONFIG.case("t_2m") == "T_2M"
    assert icon.GLOBAL_ENSEMBLE_CONFIG.case("t_2m") == "t_2m"


# -- URL construction -------------------------------------------------------------------


def test_filename_urls(eu_subset):
    urls = icon.build_urls(eu_subset, "00", dt.date(2026, 9, 27))
    # 1 single-level and 2 pressure-level variables over 2 steps, no invariants for EU.
    assert len(urls) == 6
    assert urls[0] == (
        "https://opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/"
        "icon-eu_europe_regular-lat-lon_single-level_2026092700_000_T_2M.grib2.bz2",
        "icon-eu_europe_regular-lat-lon_single-level_2026092700_000_T_2M.grib2",
    )


def test_filename_urls_include_one_invariant_per_variable_not_per_step():
    urls = icon.build_urls(
        dataclasses.replace(icon.GLOBAL_CONFIG, vars_2d=["t_2m"], vars_3d=[], f_steps=[0, 1, 2]),
        "12",
        dt.date(2026, 1, 2),
    )
    invariant = [name for _, name in urls if "time-invariant" in name]
    assert sorted(invariant) == [
        "icon_global_icosahedral_time-invariant_2026010212_CLAT.grib2",
        "icon_global_icosahedral_time-invariant_2026010212_CLON.grib2",
    ]


def test_ensemble_urls_use_lowercase_variable_names():
    urls = icon.build_urls(
        dataclasses.replace(icon.GLOBAL_ENSEMBLE_CONFIG, vars_2d=["t_2m"], vars_invariant=[]),
        "00",
        dt.date(2026, 9, 27),
    )
    assert urls[0][1].endswith("_000_t_2m.grib2")


def test_path_urls(art_subset):
    urls = dict(
        (name, url) for url, name in icon.build_urls(art_subset, "00", dt.date(2026, 9, 27))
    )
    assert urls["T_2M__single__step-000.grib2"].endswith(
        "/icon-art/p/T_2M/r/2026-09-27T00:00/s/PT000H00M.grib2"
    )
    assert "/lvt1/100/lv1/50000/" in urls["FI__pressure-50000__step-000.grib2"]
    assert "/lvt1/150/lv1/120/" in urls["T__model-120__step-000.grib2"]
    assert "/lvt1/106/lv1/0.0/" in urls["T_SO__soil-0.0__step-000.grib2"]
    assert "/wvl1/532/lvt1/150/lv1/120/" in urls[
        "CEIL_BSC_DUST__wavelength-532-120__step-000.grib2"
    ]


def test_half_height_urls_cover_every_level():
    urls = icon.model_level_half_height_urls(dt.date(2026, 9, 27), levels=[1, 2, 121])
    assert [name for _, name in urls] == [
        "icon_global_icosahedral_time-invariant_2026092700_1_HHL.grib2",
        "icon_global_icosahedral_time-invariant_2026092700_2_HHL.grib2",
        "icon_global_icosahedral_time-invariant_2026092700_121_HHL.grib2",
    ]
    assert urls[0][0].endswith("/icon/grib/00/hhl/" + urls[0][1] + ".bz2")


# -- filename parsing -------------------------------------------------------------------


@pytest.mark.parametrize("variant", ["eu", "global", "global_model", "global_ensemble"])
def test_every_built_filename_parses_back(variant):
    config = dataclasses.replace(icon.VARIANTS[variant], f_steps=[0, 78])
    urls = icon.build_urls(config, "00", dt.date(2026, 9, 27))
    for _, name in urls:
        assert icon._parse_name(config, name) is not None, name


def test_parse_name_keeps_underscores_in_variable_names():
    name = "icon-eu_europe_regular-lat-lon_single-level_2026092700_000_T_2M.grib2"
    assert icon._parse_name(icon.EUROPE_CONFIG, name) == (icon.KIND_SINGLE, "t_2m")


def test_parse_name_separates_level_from_variable():
    name = "icon_global_icosahedral_model-level_2026092700_012_121_QV.grib2"
    assert icon._parse_name(icon.GLOBAL_CONFIG, name) == (icon.KIND_MODEL, "qv")


def test_parse_name_handles_leveled_invariants():
    name = "icon_global_icosahedral_time-invariant_2026092700_121_HHL.grib2"
    assert icon._parse_name(icon.GLOBAL_CONFIG, name) == (icon.KIND_INVARIANT, "hhl")


def test_parse_name_rejects_files_from_another_variant():
    name = "icon-eu_europe_regular-lat-lon_single-level_2026092700_000_T_2M.grib2"
    assert icon._parse_name(icon.GLOBAL_CONFIG, name) is None


def test_step_extraction():
    assert icon._step_of("x/icon_global_icosahedral_single-level_2026092700_078_T_2M.grib2") == 78
    assert icon._step_of("x/icon_global_icosahedral_model-level_2026092712_006_121_T.grib2") == 6
    assert icon._step_of("x/T__model-120__step-042.grib2") == 42
    assert icon._step_of("x/icon_global_icosahedral_time-invariant_2026092700_CLAT.grib2") == 0


def test_index_files_groups_by_kind_and_variable(eu_subset):
    names = [name for _, name in icon.build_urls(eu_subset, "00", dt.date(2026, 9, 27))]
    grouped = icon.index_files(eu_subset, names)
    assert set(grouped) == {(icon.KIND_SINGLE, "t_2m"), (icon.KIND_PRESSURE, "t")}
    assert len(grouped[(icon.KIND_PRESSURE, "t")]) == 4


def test_index_files_ignores_unrecognised_names(eu_subset):
    assert icon.index_files(eu_subset, ["README.txt"]) == {}


def test_index_files_splits_wavelengths_but_not_levels(art_subset):
    names = [
        "CEIL_BSC_DUST__wavelength-532-119__step-000.grib2",
        "CEIL_BSC_DUST__wavelength-532-120__step-000.grib2",
        "CEIL_BSC_DUST__wavelength-1064-120__step-000.grib2",
    ]
    grouped = icon.index_files(art_subset, names)
    assert set(grouped) == {
        (icon.KIND_WAVELENGTH, "ceil_bsc_dust@532"),
        (icon.KIND_WAVELENGTH, "ceil_bsc_dust@1064"),
    }
    assert len(grouped[(icon.KIND_WAVELENGTH, "ceil_bsc_dust@532")]) == 2


# -- dataset shaping --------------------------------------------------------------------


def _leveled_dataset(level_values):
    """A dataset shaped like a level-concatenated open_mfdataset result."""
    n = len(level_values)
    ds = xr.Dataset(
        {"t": (("level", "values"), np.zeros((n, 3), dtype="float32"))},
    )
    if n == 1:
        # open_mfdataset leaves the coordinate scalar when only one file is concatenated.
        return ds.assign_coords(isobaricInhPa=level_values[0])
    return ds.assign_coords(isobaricInhPa=("level", np.array(level_values)))


def test_promote_to_dim_handles_a_scalar_coordinate():
    ds = icon._promote_to_dim(_leveled_dataset([500.0]), "isobaricInhPa", "level")
    assert ds.sizes["isobaricInhPa"] == 1
    assert "level" not in ds.dims
    assert ds.isobaricInhPa.values.tolist() == [500.0]


def test_promote_to_dim_handles_an_already_promoted_coordinate():
    ds = icon._promote_to_dim(_leveled_dataset([500.0, 850.0]), "isobaricInhPa", "level")
    assert ds.isobaricInhPa.values.tolist() == [500.0, 850.0]


def test_promote_to_dim_can_rename_the_dimension():
    ds = icon._promote_to_dim(
        _leveled_dataset([60.0, 61.0]), "isobaricInhPa", "level", rename_to="model_level"
    )
    assert "model_level" in ds.dims
    assert "isobaricInhPa" not in ds.coords


def test_promote_to_dim_leaves_a_mismatched_coordinate_alone():
    # Three levels' worth of coordinate values against a dimension of two: refuse rather
    # than write a mislabelled axis.
    ds = _leveled_dataset([500.0, 850.0])
    ds = ds.drop_vars("isobaricInhPa").assign_coords(
        isobaricInhPa=("other", np.array([500.0, 850.0, 1000.0]))
    )
    out = icon._promote_to_dim(ds, "isobaricInhPa", "level")
    assert "level" in out.dims
    assert out.isobaricInhPa.dims == ("other",)


def test_drop_scalar_coords_keeps_time_and_dimensions():
    ds = xr.Dataset(
        {"t": (("step",), np.zeros(2, dtype="float32"))},
        coords={
            "step": np.arange(2),
            "time": pd.Timestamp("2026-09-27"),
            "surface": 0.0,
        },
    )
    out = icon._drop_scalar_coords(ds)
    assert "surface" not in out.coords
    assert "time" in out.coords
    assert "step" in out.coords


def test_clean_ruc_dataset_renames_valid_time():
    ds = xr.Dataset(
        {"unknown": (("valid_time", "values"), np.zeros((2, 3), dtype="float32"))},
        coords={
            "valid_time": pd.to_datetime(["2026-09-27T14:00", "2026-09-27T14:15"]),
            "time": pd.Timestamp("2026-09-27T14:00"),
            "surface": 0.0,
        },
    )
    out = icon._clean_ruc_dataset(ds)
    assert "time" in out.dims
    assert out.sizes["time"] == 2
    # The GRIB init time and the per-file scalar coordinates are gone, so variables from
    # different files can be merged.
    assert "surface" not in out.coords
    assert "valid_time" not in out.coords


# -- providers --------------------------------------------------------------------------


def test_provider_names_and_stores_come_from_the_variant(local_config):
    provider = icon.ICONProvider("eu", config=local_config)
    assert provider.name == "icon_eu"
    assert provider.store_prefix == icon.EUROPE_CONFIG.store_prefix
    assert provider.append_dim == "init_time"
    assert provider.store_path.startswith(str(local_config.icechunk_local_path))


def test_provider_rejects_an_unknown_variant():
    with pytest.raises(ValueError, match="unknown ICON variant"):
        icon.ICONProvider("mars")


def test_provider_accepts_an_explicit_config(eu_subset, local_config):
    provider = icon.ICONProvider("eu", config=local_config, icon_config=eu_subset)
    assert provider.store_prefix == "bkr/icon/test_eu.icechunk"


def test_store_prefix_override(local_config):
    provider = icon.ICONProvider("eu", config=local_config, store_prefix="bkr/icon/other.icechunk")
    assert provider.store_prefix == "bkr/icon/other.icechunk"


def test_download_dir_is_per_variant_and_run(tmp_path, local_config):
    provider = icon.ICONProvider("eu", config=local_config)
    path = provider._download_dir(pd.Timestamp("2026-09-27T12:00"), tmp_path)
    assert path == tmp_path / "eu" / "20260927" / "12"


def test_hf_repo_id_defaults_to_the_variant(local_config):
    assert icon.ICONProvider("global", config=local_config).hf_repo_id == (
        "openclimatefix/dwd-icon-global"
    )
    assert icon.ICONProvider("eu", config=local_config).hf_repo_id == "openclimatefix/dwd-icon-eu"


def test_hf_repo_id_honours_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path))
    monkeypatch.setenv("HF_REPO_ID", "someone/else")
    from planetary_datasets import config as config_module

    cfg = config_module.load_config(env_file=tmp_path / "nonexistent.env")
    assert icon.ICONProvider("global", config=cfg).hf_repo_id == "someone/else"


def test_hf_repo_id_raises_when_unconfigured(local_config, eu_subset):
    provider = icon.ICONProvider(
        "eu", config=local_config, icon_config=dataclasses.replace(eu_subset, hf_repo_id=None)
    )
    with pytest.raises(ValueError, match="HF_REPO_ID"):
        _ = provider.hf_repo_id


def test_publish_dry_run_does_not_need_a_token(tmp_path, local_config):
    folder = tmp_path / "store"
    folder.mkdir()
    provider = icon.ICONProvider("global", config=local_config)
    assert provider.publish(folder, dry_run=True) == "openclimatefix/dwd-icon-global"


def test_publish_rejects_a_missing_directory(tmp_path, local_config):
    provider = icon.ICONProvider("global", config=local_config)
    with pytest.raises(FileNotFoundError):
        provider.publish(tmp_path / "nope")


def test_publish_rejects_an_object_store_uri(local_config):
    provider = icon.ICONProvider("global", config=local_config)
    with pytest.raises(ValueError, match="local directory"):
        provider.publish("s3://us-west-2.opendata.source.coop/bkr/icon/icon_global.icechunk")


def test_publish_requires_a_token(tmp_path, local_config):
    folder = tmp_path / "store"
    folder.mkdir()
    provider = icon.ICONProvider("global", config=local_config)
    with pytest.raises(MissingCredential, match="HF_TOKEN"):
        provider.publish(folder)


def test_process_refuses_an_empty_run(eu_subset, local_config):
    provider = icon.ICONProvider("eu", config=local_config, icon_config=eu_subset)
    with pytest.raises(ValueError, match="no usable GRIB files"):
        provider.process([], pd.Timestamp("2026-09-27T00:00"))


# -- ICON-D2-RUC ------------------------------------------------------------------------


def test_ruc_provider_has_one_store_per_cadence(local_config):
    prefixes = {
        ts: icon.ICOND2RUCProvider(ts, config=local_config).store_prefix
        for ts in icon.ICOND2RUCProvider.TIMESTEPS
    }
    assert prefixes == {
        "60min": "bkr/icon/icon_d2_ruc_60min.icechunk",
        "15min": "bkr/icon/icon_d2_ruc_15min.icechunk",
        "5min": "bkr/icon/icon_d2_ruc_5min.icechunk",
    }


def test_ruc_store_suffix(local_config):
    provider = icon.ICOND2RUCProvider("5min", config=local_config, store_suffix="_2")
    assert provider.store_prefix == "bkr/icon/icon_d2_ruc_5min_2.icechunk"


def test_ruc_provider_rejects_an_unknown_cadence():
    with pytest.raises(ValueError, match="unknown timescale"):
        icon.ICOND2RUCProvider("30min")


def test_ruc_grib_dir_precedence(tmp_path, local_config, monkeypatch):
    explicit = icon.ICOND2RUCProvider("60min", config=local_config, grib_dir=tmp_path / "a")
    assert explicit.grib_dir == tmp_path / "a"

    monkeypatch.setenv("ICON_D2_RUC_GRIB_DIR", str(tmp_path / "b"))
    from_env = icon.ICOND2RUCProvider("60min", config=local_config)
    assert from_env.grib_dir == tmp_path / "b"

    monkeypatch.delenv("ICON_D2_RUC_GRIB_DIR")
    default = icon.ICOND2RUCProvider("60min", config=local_config)
    assert default.grib_dir == local_config.data_dir / "dwd-ruc" / default.MIRROR_SUBPATH


def test_ruc_fetch_returns_nothing_when_the_mirror_is_absent(tmp_path, local_config):
    provider = icon.ICOND2RUCProvider("60min", config=local_config, grib_dir=tmp_path / "missing")
    assert provider.fetch(pd.Timestamp("2026-09-27T14:00")) == []


def test_ruc_fetch_finds_analysis_files_in_the_mirror(tmp_path, local_config):
    mirror = tmp_path / "mirror"
    wanted = mirror / "T_2M" / "r" / "2026-09-27T14:00" / "s" / "PT000H00M.grib2"
    wanted.parent.mkdir(parents=True)
    wanted.write_bytes(b"")
    other = mirror / "T_2M" / "r" / "2026-09-27T15:00" / "s" / "PT000H00M.grib2"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"")
    forecast = mirror / "T_2M" / "r" / "2026-09-27T14:00" / "s" / "PT003H00M.grib2"
    forecast.write_bytes(b"")

    provider = icon.ICOND2RUCProvider("60min", config=local_config, grib_dir=mirror)
    assert provider.fetch(pd.Timestamp("2026-09-27T14:00")) == [str(wanted)]


def _ruc_dataset(minutes):
    times = pd.Timestamp("2026-09-27T14:00") + pd.to_timedelta(minutes, unit="m")
    return xr.Dataset(
        {"cape_ml": (("time", "values"), np.zeros((len(times), 2), dtype="float32"))},
        coords={"time": times},
    )


def test_ruc_cadence_accepts_correctly_spaced_times(local_config):
    provider = icon.ICOND2RUCProvider("15min", config=local_config)
    assert provider._matches_cadence(_ruc_dataset([0, 15, 30, 45]), "CAPE_ML") is True


def test_ruc_cadence_rejects_a_partially_mirrored_five_minute_variable(local_config):
    # Four valid times, but five minutes apart: this is a half-mirrored 5-minute variable,
    # not a 15-minute one.
    provider = icon.ICOND2RUCProvider("15min", config=local_config)
    assert provider._matches_cadence(_ruc_dataset([0, 5, 10, 15]), "TOT_PREC") is False


def test_ruc_cadence_rejects_the_wrong_number_of_times(local_config):
    provider = icon.ICOND2RUCProvider("5min", config=local_config)
    assert provider._matches_cadence(_ruc_dataset([0, 5]), "TOT_PREC") is False


def test_ruc_cadence_accepts_a_single_hourly_time(local_config):
    provider = icon.ICOND2RUCProvider("60min", config=local_config)
    assert provider._matches_cadence(_ruc_dataset([0]), "PMSL") is True


def test_ruc_process_refuses_an_empty_run(local_config):
    provider = icon.ICOND2RUCProvider("60min", config=local_config)
    with pytest.raises(ValueError, match="no 60min variables"):
        provider.process([], pd.Timestamp("2026-09-27T14:00"))


# -- completeness -----------------------------------------------------------------------


def _icon_run(init: pd.Timestamp, variables: list[str], steps: int = 3) -> xr.Dataset:
    """A minimal ICON run: one init time, ``steps`` lead times, the named variables."""
    return xr.Dataset(
        {
            name: (("init_time", "step", "values"), np.zeros((1, steps, 4), dtype="float32"))
            for name in variables
        },
        coords={
            "init_time": pd.DatetimeIndex([init]),
            "step": pd.to_timedelta(np.arange(steps), unit="h"),
        },
    )


def test_a_fresh_run_does_not_create_the_store(local_config):
    """Regression: a half-uploaded run is indistinguishable from a sparse one.

    A truncated first run fixed the store's schema and locked every complete run out afterwards,
    while the asset went on reporting success.
    """
    provider = icon.ICONProvider("eu", config=local_config, store_prefix="bkr/icon/fresh.icechunk")
    just_now = pd.Timestamp.utcnow().tz_localize(None).floor("h")

    with pytest.raises(icon.IncompleteRun, match="possibly still uploading"):
        provider.write_to_icechunk(
            provider.get_icechunk_repo(), _icon_run(just_now, ["t_2m", "u_10m"])
        )


def test_a_settled_run_creates_the_store(local_config):
    provider = icon.ICONProvider(
        "eu", config=local_config, store_prefix="bkr/icon/settled.icechunk"
    )
    old = pd.Timestamp.utcnow().tz_localize(None).floor("h") - pd.Timedelta(days=1)

    assert (
        provider.write_to_icechunk(
            provider.get_icechunk_repo(), _icon_run(old, ["t_2m", "u_10m"])
        )
        is True
    )


def test_a_run_short_of_the_stores_variables_is_refused(local_config):
    provider = icon.ICONProvider("eu", config=local_config, store_prefix="bkr/icon/short.icechunk")
    repo = provider.get_icechunk_repo()
    first = pd.Timestamp.utcnow().tz_localize(None).floor("h") - pd.Timedelta(days=2)
    provider.write_to_icechunk(repo, _icon_run(first, ["t_2m", "u_10m"]))

    with pytest.raises(icon.IncompleteRun, match="1 variable"):
        provider.write_to_icechunk(repo, _icon_run(first + pd.Timedelta(hours=6), ["t_2m"]))


def test_a_run_with_a_short_step_axis_is_refused(local_config):
    provider = icon.ICONProvider("eu", config=local_config, store_prefix="bkr/icon/steps.icechunk")
    repo = provider.get_icechunk_repo()
    first = pd.Timestamp.utcnow().tz_localize(None).floor("h") - pd.Timedelta(days=2)
    provider.write_to_icechunk(repo, _icon_run(first, ["t_2m"], steps=4))

    with pytest.raises(icon.IncompleteRun, match="2 of 4 step"):
        provider.write_to_icechunk(
            repo, _icon_run(first + pd.Timedelta(hours=6), ["t_2m"], steps=2)
        )


# -- static heights ---------------------------------------------------------------------


def test_static_heights_write_is_skipped_when_already_stored(local_config):
    from icechunk.xarray import to_icechunk

    repo = local_config.icechunk_repo(icon.STATIC_HEIGHTS_PREFIX)
    ds = xr.Dataset(
        {"HHL": (("model_level_half", "values"), np.zeros((2, 3), dtype="float32"))},
        coords={"model_level_half": [1, 2]},
    )
    session = repo.writable_session("main")
    to_icechunk(ds, session)
    session.commit("test")

    # Returns False without downloading anything.
    assert icon.write_model_level_half_heights(config=local_config) is False


def test_open_model_level_half_heights_rejects_an_empty_list():
    with pytest.raises(ValueError, match="no HHL files"):
        icon.open_model_level_half_heights([])


def test_static_heights_refuses_a_partial_field(local_config, monkeypatch):
    # Only two of the three requested levels came back; writing that would cache an
    # incomplete field forever.
    monkeypatch.setattr(icon, "download_run", lambda urls, dest, **kw: ["a.grib2", "b.grib2"])
    assert (
        icon.write_model_level_half_heights(
            date=dt.date(2026, 9, 27), config=local_config, levels=[1, 2, 3]
        )
        is False
    )


def test_static_heights_reports_no_data(local_config, monkeypatch):
    monkeypatch.setattr(icon, "download_run", lambda urls, dest, **kw: [])
    assert (
        icon.write_model_level_half_heights(
            date=dt.date(2026, 9, 27), config=local_config, levels=[1]
        )
        is False
    )


# -- Dagster ----------------------------------------------------------------------------


def test_dagster_assets_load():
    import dagster as dg

    from dags.assets import icon as icon_assets

    defs = dg.Definitions(assets=icon_assets.icon_assets)
    keys = {a.key.to_user_string() for a in icon_assets.icon_assets}
    assert "icon_global" in keys
    assert "icon_d2_ruc_5min" in keys
    assert "icon_global_model_level_half_heights" in keys
    assert defs is not None
