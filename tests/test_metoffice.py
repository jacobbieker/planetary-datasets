"""Met Office providers, exercised offline against synthetic published files."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store as open_store
from planetary_datasets.providers.metoffice import (
    PROVIDERS,
    MetOfficeGlobal10km6Hourly24HourProvider,
    MetOfficeGlobal10kmProvider,
    MetOfficeGlobalWaveProvider,
    MetOfficeOceanDepthProvider,
    MetOfficeOceanSurfaceProvider,
    MetOfficeUK2kmProvider,
    drop_grid_variables,
    group_files_by_variable,
    parse_filename,
    preprocess,
    slugify_long_names,
    to_float16,
    variable_suffix,
)

INIT = pd.Timestamp("2026-01-01T00:00")


def surface_file(valid: pd.Timestamp, variable: str, height: float | None = 1.5) -> xr.Dataset:
    """One published surface file: a single field on a lat/lon grid at one valid time."""
    ds = xr.Dataset(
        {
            "air_temperature": (("latitude", "longitude"), np.full((3, 4), 280.0, dtype="float32")),
            "latitude_longitude": ((), np.int32(0), {"grid_mapping_name": "latitude_longitude"}),
            "latitude_bnds": (("latitude", "bnds"), np.zeros((3, 2), dtype="float32")),
            "flag": (("latitude", "longitude"), np.zeros((3, 4), dtype="int8")),
        },
        coords={
            "latitude": np.linspace(-1, 1, 3, dtype="float32"),
            "longitude": np.linspace(0, 3, 4, dtype="float32"),
            "time": valid,
            "forecast_period": np.int32(0),
            "forecast_reference_time": INIT,
        },
    )
    if height is not None:
        ds = ds.assign_coords(height=np.float32(height))
    ds = ds.rename({"air_temperature": variable}) if variable != "air_temperature" else ds
    return ds


def write_archive(root, model: str, init: pd.Timestamp, steps, variables=("temperature_at_screen_level",)):
    """Write synthetic published files into an archive directory, one per step per variable."""
    directory = root / model / init.strftime("%Y%m%dT%H%MZ")
    directory.mkdir(parents=True, exist_ok=True)
    for step in steps:
        valid = init + pd.Timedelta(step, "h")
        for variable in variables:
            name = f"{valid.strftime('%Y%m%dT%H%MZ')}-PT{step:04d}H00M-{variable}.nc"
            surface_file(valid, "air_temperature").to_netcdf(directory / name)
    return root / model




# --------------------------------------------------------------------------- filenames


def test_parse_filename_splits_valid_time_step_and_variable():
    parsed = parse_filename("20260925T0300Z-PT0003H00M-temperature_at_screen_level.nc")
    assert parsed["valid_time"] == pd.Timestamp("2026-09-25T03:00")
    assert (parsed["hours"], parsed["minutes"]) == (3, 0)
    assert parsed["variable"] == "temperature_at_screen_level"


def test_parse_filename_keeps_the_aggregation_period_in_the_variable():
    parsed = parse_filename("20260925T0300Z-PT0003H00M-wind_gust_at_10m_max-PT01H.nc")
    assert parsed["variable"] == "wind_gust_at_10m_max-PT01H"


def test_parse_filename_rejects_anything_else():
    assert parse_filename("index.html") is None
    assert parse_filename("/tmp/notes.txt") is None


@pytest.mark.parametrize(
    "variable,expected",
    [
        ("temperature_at_screen_level", ""),
        ("wind_gust_at_10m_max-PT01H", "_max_1h"),
        ("temperature_at_screen_level_min-PT01H", "_min_1h"),
        ("precipitation_accumulation-PT01H", "_1h"),
        ("precipitation_accumulation-PT06H", "_6h"),
        ("CAPE_most_unstable_below_500hPa", "_below_500hPa"),
        ("CAPE_mixed_layer_lowest_500m", "_lowest_500m"),
    ],
)
def test_variable_suffix_distinguishes_fields_that_share_a_cf_name(variable, expected):
    assert variable_suffix(variable) == expected


def test_group_files_by_variable_ignores_the_lead_time():
    files = [
        "20260925T0000Z-PT0000H00M-rainfall_rate.nc",
        "20260925T0100Z-PT0001H00M-rainfall_rate.nc",
        "20260925T0100Z-PT0001H00M-wind_gust_at_10m_max-PT01H.nc",
        "some-other-file.txt",
    ]
    groups = group_files_by_variable(files)
    assert sorted(groups) == ["rainfall_rate", "wind_gust_at_10m_max-PT01H"]
    assert len(groups["rainfall_rate"]) == 2


# ------------------------------------------------------------------------ preprocessing


def test_drop_grid_variables_removes_the_grid_mapping_and_bounds():
    ds = drop_grid_variables(surface_file(INIT, "air_temperature"))
    assert list(ds.data_vars) == ["air_temperature"]
    assert "bnds" not in ds.dims
    assert "forecast_period" not in ds.coords


def test_preprocess_folds_a_single_height_into_the_variable_name():
    ds = preprocess(surface_file(INIT, "air_temperature", height=1.5))
    assert list(ds.data_vars) == ["air_temperature_1.5m"]
    # The coordinate has to go, or merging 1.5 m with 10 m fields conflicts on it.
    assert "height" not in ds.coords


def test_preprocess_keeps_multiple_heights_as_a_dimension():
    ds = surface_file(INIT, "air_temperature", height=None).expand_dims(height=[5.0, 10.0])
    out = preprocess(ds)
    assert list(out.data_vars) == ["air_temperature_on_height_levels"]
    assert out.sizes["height"] == 2


def test_to_float16_leaves_the_fields_that_need_the_precision():
    ds = xr.Dataset(
        {
            "air_temperature": ("x", np.ones(3, dtype="float32")),
            "air_pressure_at_sea_level": ("x", np.ones(3, dtype="float32")),
        },
        coords={"x": np.arange(3)},
    )
    out = to_float16(ds)
    assert out["air_temperature"].dtype == np.float16
    assert out["air_pressure_at_sea_level"].dtype == np.float32


def test_slugify_long_names_uses_the_long_name():
    ds = xr.Dataset({"ssh": ("x", np.zeros(2), {"long_name": "Sea Surface Height (m)"})})
    assert list(slugify_long_names(ds).data_vars) == ["sea_surface_height_m"]


# ------------------------------------------------------------------------- the variants


DOCUMENTED_STORES = {
    MetOfficeGlobal10kmProvider: ("metoffice_global_deterministic_10km", "init_time"),
    MetOfficeGlobal10km6Hourly24HourProvider: (
        "metoffice_global_deterministic_10km_6hourly_24hr",
        "init_time",
    ),
    MetOfficeUK2kmProvider: ("metoffice_uk_deterministic_2km", "time"),
    MetOfficeOceanSurfaceProvider: ("metoffice_global_hourly_ocean_surface_analysis", "init_time"),
    MetOfficeOceanDepthProvider: ("metoffice_global_hourly_ocean_depth_analysis", "init_time"),
    MetOfficeGlobalWaveProvider: ("metoffice_global_wave", "time"),
}


def test_the_providers_target_the_documented_stores():
    assert set(PROVIDERS.values()) == set(DOCUMENTED_STORES)
    for cls, (store, append_dim) in DOCUMENTED_STORES.items():
        assert cls.store_prefix == f"bkr/metoffice/{store}.icechunk", cls.__name__
        assert cls.append_dim == append_dim, cls.__name__


def test_store_paths_resolve_through_the_config(local_config, tmp_path):
    provider = MetOfficeGlobal10kmProvider(config=local_config)
    assert provider.store_path.startswith(str(tmp_path))
    assert "s3://" not in provider.store_path


def test_wanted_keeps_only_the_configured_whole_hour_steps():
    provider = MetOfficeGlobal10km6Hourly24HourProvider()
    assert provider.wanted("20260925T0600Z-PT0006H00M-rainfall_rate.nc")
    assert not provider.wanted("20260925T0100Z-PT0001H00M-rainfall_rate.nc")
    assert not provider.wanted("20260925T0015Z-PT0000H15M-rainfall_rate.nc")


# -------------------------------------------------------------------- end to end, local


@pytest.fixture
def archive(tmp_path):
    return tmp_path / "archive"


def test_fetch_prefers_the_local_archive_over_the_network(local_config, archive):
    root = write_archive(archive, "global-deterministic-10km", INIT, range(6))
    provider = MetOfficeGlobal10kmProvider(config=local_config, archive_dir=root)
    assert len(provider.fetch(INIT)) == 6


def test_fetch_returns_nothing_when_the_bucket_is_missing_a_step(local_config, archive, monkeypatch):
    provider = MetOfficeGlobal10km6Hourly24HourProvider(config=local_config, archive_dir=archive)
    published = [
        f"global-deterministic-10km/20260101T0000Z/20260101T{h:02d}00Z-PT{h:04d}H00M-rainfall_rate.nc"
        for h in (0, 6, 12, 18)
    ]
    monkeypatch.setattr(provider, "list_remote", lambda it: published)
    assert provider.fetch(INIT) == [], "an incomplete run must be left for a later attempt"


def test_fetch_falls_back_to_the_bucket_when_the_archive_is_incomplete(
    local_config, archive, monkeypatch
):
    # A half-finished bulk download must not be mistaken for the whole run: appending a
    # short partition to a store built from complete ones fails on the step axis.
    root = write_archive(archive, "global-deterministic-10km", INIT, (0, 1, 2))
    provider = MetOfficeGlobal10kmProvider(config=local_config, archive_dir=root)
    monkeypatch.setattr(provider, "list_remote", lambda it: [])
    assert provider.fetch(INIT) == []


def test_missing_steps_lists_what_the_variant_still_needs():
    provider = MetOfficeGlobal10km6Hourly24HourProvider()
    paths = ["20260101T0000Z-PT0000H00M-rainfall_rate.nc", "20260101T0600Z-PT0006H00M-rainfall_rate.nc"]
    assert provider.missing_steps(paths) == [12, 18, 24]
    assert provider.missing_steps([]) == [0, 6, 12, 18, 24]


def test_global_partition_round_trips_through_the_store(local_config, archive):
    root = write_archive(archive, "global-deterministic-10km", INIT, range(6))
    provider = MetOfficeGlobal10kmProvider(config=local_config, archive_dir=root)

    assert provider.run_partition(INIT) is True
    assert provider.run_partition(INIT) is False, "a stored partition must not be redone"

    ds = open_store(provider)
    assert list(ds.init_time.values) == [INIT.to_numpy()]
    assert ds.sizes["step"] == 6
    assert ds.step.values[-1] == np.timedelta64(5, "h")
    assert list(ds.data_vars) == ["air_temperature_1.5m"]
    assert ds["air_temperature_1.5m"].dtype == np.float16


def test_uk_partition_keeps_the_valid_times(local_config, archive):
    root = write_archive(archive, "uk-deterministic-2km", INIT, range(6))
    provider = MetOfficeUK2kmProvider(config=local_config, archive_dir=root)

    assert provider.run_partition(INIT) is True

    ds = open_store(provider)
    assert "init_time" not in ds.dims
    assert list(ds.time.values) == [
        (INIT + pd.Timedelta(h, "h")).to_numpy() for h in range(6)
    ]


def test_process_rejects_a_partition_with_nothing_recognisable(local_config, archive):
    provider = MetOfficeGlobal10kmProvider(config=local_config, archive_dir=archive)
    with pytest.raises(ValueError, match="no recognisable files"):
        provider.process(["notes.txt"], INIT)


# ------------------------------------------------------------------------ ocean and wave


def ocean_file(times, depths=None, init: pd.Timestamp = INIT) -> xr.Dataset:
    """One ocean product file, shaped like the ORCA025 archive."""
    shape = (len(times), 2, 3) if depths is None else (len(times), len(depths), 2, 3)
    dims = ("time", "lat", "lon") if depths is None else ("time", "depth", "lat", "lon")
    ds = xr.Dataset(
        {"var": (dims, np.zeros(shape, dtype="float32"), {"long_name": "Sea Surface Height"})},
        coords={
            "time": times,
            "lat": np.linspace(-1, 1, 2),
            "lon": np.linspace(0, 1, 3),
            "forecast_reference_time": init,
            "forecast_period": np.int32(0),
        },
    )
    if depths is not None:
        ds = ds.assign_coords(depth=list(depths))
    return ds


def write_ocean(root, init: pd.Timestamp, with_depth: bool = True):
    directory = root / "global-ocean-ORCA025" / init.strftime("%Y/%m/%d/T%H%MZ")
    directory.mkdir(parents=True, exist_ok=True)
    hourly = pd.date_range(init - pd.Timedelta(24, "h"), periods=24, freq="1h")
    ocean_file(hourly, init=init).to_netcdf(directory / f"b{init:%Y%m%d}T0000Z_hi-SSH.nc")
    if with_depth:
        depth = ocean_file([init], depths=[0.0, 10.0], init=init).rename({"var": "temperature"})
        depth["temperature"].attrs["long_name"] = "Sea Water Potential Temperature"
        depth.to_netcdf(directory / f"b{init:%Y%m%d}T0000Z_dm-TEM.nc")
    return root


def test_ocean_surface_and_depth_go_to_separate_stores(local_config, tmp_path):
    root = write_ocean(tmp_path / "ocean", INIT)
    surface = MetOfficeOceanSurfaceProvider(config=local_config, archive_root=root)
    depth = MetOfficeOceanDepthProvider(config=local_config, archive_root=root)

    assert surface.run_partition(INIT) is True
    assert depth.run_partition(INIT) is True
    assert surface.store_path != depth.store_path

    surface_ds = open_store(surface)
    depth_ds = open_store(depth)
    assert list(surface_ds.init_time.values) == [INIT.to_numpy()]
    assert "depth" not in surface_ds.dims
    assert depth_ds.sizes["depth"] == 2
    assert "latitude" in surface_ds.coords and "longitude" in surface_ds.coords


def test_ocean_runs_are_stored_as_offsets_so_a_second_run_cannot_relabel_the_first(
    local_config, tmp_path
):
    # `time` as a dimension coordinate under `init_time` holds one set of values for the
    # whole store, so appending a second run would rewrite the first run's hours.
    root = tmp_path / "ocean"
    later = INIT + pd.Timedelta(1, "D")
    write_ocean(root, INIT)
    write_ocean(root, later)
    provider = MetOfficeOceanSurfaceProvider(config=local_config, archive_root=root)

    assert provider.run_partition(INIT) is True
    assert provider.run_partition(later) is True

    ds = open_store(provider)
    assert list(ds.init_time.values) == [INIT.to_numpy(), later.to_numpy()]
    assert "time" not in ds.dims
    assert ds.step.values[0] == np.timedelta64(-24, "h")
    assert ds.step.values[-1] == np.timedelta64(-1, "h")


def test_ocean_depth_store_copes_with_a_run_that_has_no_depth_levels(local_config, tmp_path):
    # Daily means land before the depth products do; the run is not an error, just empty.
    root = write_ocean(tmp_path / "ocean", INIT, with_depth=False)
    provider = MetOfficeOceanDepthProvider(config=local_config, archive_root=root)
    surface, depth = provider.split(provider.fetch(INIT), INIT)
    assert depth is None and surface is not None
    with pytest.raises(ValueError, match="no depth-level fields"):
        provider.process(provider.fetch(INIT), INIT)


def test_ocean_fetch_is_empty_when_the_archive_has_no_such_run(local_config, tmp_path):
    provider = MetOfficeOceanSurfaceProvider(config=local_config, archive_root=tmp_path / "empty")
    assert provider.fetch(INIT) == []


def write_wave(root, day: pd.Timestamp, first_step_hours: int = 0):
    stamp = day.strftime("%Y%m%d")
    for run_index, run in enumerate(("T0000Z", "T0600Z", "T1200Z", "T1800Z")):
        directory = root / "global-wave" / day.strftime("%Y/%m/%d") / run
        directory.mkdir(parents=True, exist_ok=True)
        start = day + pd.Timedelta(6 * run_index + first_step_hours, "h")
        # Eight steps are published; only the six that tile the day are kept.
        times = pd.date_range(start, periods=8, freq="1h")
        ds = xr.Dataset(
            {
                "hs": (
                    ("time", "latitude", "longitude"),
                    np.zeros((8, 2, 3), dtype="float32"),
                    {"long_name": "Significant Wave Height (m)"},
                )
            },
            coords={
                "time": times,
                "latitude": np.linspace(-1, 1, 2),
                "longitude": np.linspace(0, 1, 3),
            },
        )
        ds.to_netcdf(directory / f"b{stamp}{run}_hi{stamp}{run}-wave_global_standard_v1.nc")
    return root


def test_wave_day_tiles_four_runs_into_an_hourly_series(local_config, tmp_path):
    day = pd.Timestamp("2026-01-01")
    root = write_wave(tmp_path / "wave", day)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(day) is True
    assert provider.run_partition(day) is False

    ds = open_store(provider)
    assert ds.sizes["time"] == 24
    assert list(ds.data_vars) == ["significant_wave_height_m"]
    assert ds.time.values[0] == day.to_numpy()


def test_a_wave_day_whose_steps_start_an_hour_late_is_still_recognised(local_config, tmp_path):
    day = pd.Timestamp("2026-01-01")
    write_wave(tmp_path, day, first_step_hours=1)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=tmp_path)

    assert provider.run_partition(day) is True
    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []
    # The last step of the day lands on the next midnight; that must not make the next day
    # look stored, or every second day would be silently skipped.
    following = pd.Timestamp("2026-01-02")
    assert provider.missing_timesteps(pd.DatetimeIndex([following])) == [following]


def test_a_partly_written_wave_day_can_still_be_backfilled(local_config, tmp_path):
    day = pd.Timestamp("2026-01-01")
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=tmp_path)
    write_wave(tmp_path, day)
    # Only the first run has arrived, so the day holds six of its twenty-four hours.
    partial = provider.process(
        [f for f in provider.fetch(day) if "T0000Z_hi" in pathlib.Path(f).name], day
    )
    provider.write_to_icechunk(provider.get_icechunk_repo(), partial)

    assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
