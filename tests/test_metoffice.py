"""Met Office providers, exercised offline against synthetic published files."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store as open_store
from planetary_datasets.common.store import (
    bitround,
    bitround_dataset,
    keepbits_for_tolerance,
)
from planetary_datasets.providers.metoffice import (
    KEEPBITS_BY_PATTERN,
    KEEPBITS_EXACT,
    METOFFICE_KEEPBITS,
    PROVIDERS,
    MetOfficeGlobal10km6Hourly24HourProvider,
    MetOfficeGlobal10kmProvider,
    MetOfficeGlobalOceanHourlyProvider,
    MetOfficeGlobalWaveProvider,
    MetOfficeNWSOceanDepthHourlyProvider,
    MetOfficeNWSOceanSurfaceHourlyProvider,
    MetOfficeNWSWaveProvider,
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
    MetOfficeGlobalOceanHourlyProvider: ("metoffice_global_ocean_hourly", "time"),
    MetOfficeGlobalWaveProvider: ("metoffice_global_wave", "time"),
    MetOfficeNWSWaveProvider: ("metoffice_nws_wave", "time"),
    MetOfficeNWSOceanSurfaceHourlyProvider: ("metoffice_nws_ocean_surface_hourly", "time"),
    MetOfficeNWSOceanDepthHourlyProvider: ("metoffice_nws_ocean_depth_hourly", "time"),
}


def test_the_providers_target_the_documented_stores():
    assert set(PROVIDERS.values()) == set(DOCUMENTED_STORES)
    for cls, (store, append_dim) in DOCUMENTED_STORES.items():
        assert cls.base_store_prefix == f"bkr/metoffice/{store}.icechunk", cls.__name__
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
#
# The buckets and a local archive have the same layout, so every test here builds a tree
# under tmp_path and passes it as `archive_root`. Nothing touches the network.


def wave_file(times, nlat: int = 2, nlon: int = 3) -> xr.Dataset:
    """One wave variable over ``times``, shaped like a published file."""
    return xr.Dataset(
        {
            "hs": (
                ("time", "latitude", "longitude"),
                np.zeros((len(times), nlat, nlon), dtype="float32"),
                {"long_name": "Significant Wave Height (m)"},
            )
        },
        coords={
            "time": pd.DatetimeIndex(times),
            "latitude": np.linspace(-1, 1, nlat),
            "longitude": np.linspace(0, 1, nlon),
        },
    )


def write_wave(root, day: pd.Timestamp, product="global-wave", marker="wave_global_standard_v1",
               nlat: int = 2, runs=("T0000Z", "T0600Z", "T1200Z", "T1800Z")):
    """A day of wave runs, each publishing 24 steps of which six are kept."""
    stamp = day.strftime("%Y%m%d")
    for index, run in enumerate(runs):
        directory = root / product / day.strftime("%Y/%m/%d") / run
        directory.mkdir(parents=True, exist_ok=True)
        start = day + pd.Timedelta(6 * index, "h")
        ds = wave_file(pd.date_range(start, periods=24, freq="1h"), nlat=nlat)
        ds.to_netcdf(directory / f"b{stamp}{run}_hi{stamp}{run}-{marker}-significant_height.nc")
    return root


def ocean_hourly_file(times, name="zos", long_name="Sea Surface Height Above Geoid",
                      depths=None) -> xr.Dataset:
    """One ocean product's hourly file, with or without depth levels."""
    dims = ("time", "latitude", "longitude") if depths is None else (
        "time", "depth", "latitude", "longitude"
    )
    shape = (len(times), 2, 3) if depths is None else (len(times), len(depths), 2, 3)
    coords = {
        "time": pd.DatetimeIndex(times),
        "latitude": np.linspace(-1, 1, 2),
        "longitude": np.linspace(0, 1, 3),
    }
    if depths is not None:
        coords["depth"] = np.asarray(depths, dtype="float32")
    return xr.Dataset(
        {name: (dims, np.zeros(shape, dtype="float32"), {"long_name": long_name})},
        coords=coords,
    )


def write_ocean(root, day: pd.Timestamp, product="global-ocean-ORCA025",
                prefix="level1_coupled_orca025_GL4", with_depth: bool = False):
    """One daily ocean run: 24 hourly steps, plus the neighbouring forecast days."""
    directory = root / product / day.strftime("%Y/%m/%d/T0000Z")
    directory.mkdir(parents=True, exist_ok=True)
    stamp = day.strftime("%Y%m%d")

    # The run's own day, which is what a partition keeps.
    hours = pd.date_range(day + pd.Timedelta(1, "h"), periods=24, freq="1h")
    ocean_hourly_file(hours).to_netcdf(directory / f"{prefix}_SSH_b{stamp}_hi{stamp}.nc")
    if with_depth:
        ocean_hourly_file(
            hours, name="thetao", long_name="Sea Water Potential Temperature",
            depths=[0.0, 10.0],
        ).to_netcdf(directory / f"{prefix}_TEM_b{stamp}_hi{stamp}.nc")

    # The rest of the forecast, and a daily mean. Neither belongs to this partition.
    other = (day + pd.Timedelta(1, "D")).strftime("%Y%m%d")
    ocean_hourly_file(pd.date_range(day + pd.Timedelta(25, "h"), periods=24, freq="1h")).to_netcdf(
        directory / f"{prefix}_SSH_b{stamp}_hi{other}.nc"
    )
    ocean_hourly_file([day + pd.Timedelta(12, "h")]).to_netcdf(
        directory / f"{prefix}_SSH_b{stamp}_dm{stamp}.nc"
    )
    return root


DAY = pd.Timestamp("2026-01-01")


# ------------------------------------------------------------------------------- wave


def test_wave_day_tiles_four_runs_into_a_continuous_hourly_series(local_config, tmp_path):
    """Each run publishes 24 steps; only the six before the next analysis are kept."""
    root = write_wave(tmp_path / "wave", DAY)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert provider.run_partition(DAY) is False

    ds = open_store(provider)
    times = pd.DatetimeIndex(ds.time.values)
    assert ds.sizes["time"] == 24
    assert list(ds.data_vars) == ["significant_wave_height_m"]
    assert times[0] == DAY
    assert times[-1] == DAY + pd.Timedelta(23, "h")
    assert times.is_unique and times.is_monotonic_increasing


def test_consecutive_wave_days_tile_without_overlapping(local_config, tmp_path):
    root = tmp_path / "wave"
    write_wave(root, DAY)
    write_wave(root, DAY + pd.Timedelta(1, "D"))
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert provider.run_partition(DAY + pd.Timedelta(1, "D")) is True

    times = pd.DatetimeIndex(open_store(provider).time.values)
    assert len(times) == 48
    assert times.is_unique, "the analysis window let two runs claim one hour"
    assert (times.to_series().diff().dropna() == pd.Timedelta(1, "h")).all()


def test_the_nws_wave_provider_reads_its_own_product(local_config, tmp_path):
    """Both wave models share a layout; only the product marker tells them apart."""
    root = write_wave(tmp_path / "w", DAY, product="nws-wave", marker="wave_uk_standard_v1")
    provider = MetOfficeNWSWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert open_store(provider).sizes["time"] == 24


def test_a_wave_run_that_never_arrived_is_skipped_not_fatal(local_config, tmp_path):
    root = write_wave(tmp_path / "wave", DAY, runs=("T0000Z", "T0600Z"))
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert open_store(provider).sizes["time"] == 12


def test_wave_fetch_is_empty_when_the_day_was_never_published(local_config, tmp_path):
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=tmp_path / "empty")
    assert provider.fetch(DAY) == []
    assert provider.run_partition(DAY) is False


# ------------------------------------------------------------------------------ ocean


def test_ocean_keeps_its_own_day_and_ignores_the_rest_of_the_forecast(local_config, tmp_path):
    """A run directory holds nine forecast days; a partition is one of them."""
    root = write_ocean(tmp_path / "ocean", DAY)
    provider = MetOfficeGlobalOceanHourlyProvider(config=local_config, archive_root=root)

    fetched = [pathlib.Path(f).name for f in provider.fetch(DAY)]
    assert all(f"_hi{DAY:%Y%m%d}" in f for f in fetched), fetched
    assert not any("_dm" in f for f in fetched), "a daily mean cannot sit on an hourly axis"

    assert provider.run_partition(DAY) is True
    times = pd.DatetimeIndex(open_store(provider).time.values)
    assert len(times) == 24
    assert times[0] == DAY + pd.Timedelta(1, "h")
    assert times[-1] == DAY + pd.Timedelta(24, "h")


def test_consecutive_ocean_days_tile_without_overlapping(local_config, tmp_path):
    root = tmp_path / "ocean"
    write_ocean(root, DAY)
    write_ocean(root, DAY + pd.Timedelta(1, "D"))
    provider = MetOfficeGlobalOceanHourlyProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert provider.run_partition(DAY + pd.Timedelta(1, "D")) is True

    times = pd.DatetimeIndex(open_store(provider).time.values)
    assert len(times) == 48
    assert times.is_unique
    assert (times.to_series().diff().dropna() == pd.Timedelta(1, "h")).all()


def test_nws_ocean_splits_surface_from_depth(local_config, tmp_path):
    """A day is 2.3 GB of surface fields and 58 GB of depth ones; they get a store each."""
    root = write_ocean(
        tmp_path / "o", DAY, product="nws-ocean", prefix="metoffice_foam1_amm15_NWS",
        with_depth=True,
    )
    surface = MetOfficeNWSOceanSurfaceHourlyProvider(config=local_config, archive_root=root)
    depth = MetOfficeNWSOceanDepthHourlyProvider(config=local_config, archive_root=root)

    assert surface.run_partition(DAY) is True
    assert depth.run_partition(DAY) is True
    assert surface.store_path != depth.store_path

    surface_ds, depth_ds = open_store(surface), open_store(depth)
    assert "depth" not in surface_ds.dims
    assert depth_ds.sizes["depth"] == 2
    assert set(surface_ds.data_vars).isdisjoint(depth_ds.data_vars)


def test_the_nws_wave_files_in_the_ocean_bucket_are_left_to_the_wave_store(local_config, tmp_path):
    """``nws-ocean`` also carries ``level1_wave_amm15_NWS_WAV_*``; it is not ocean data."""
    root = write_ocean(
        tmp_path / "o", DAY, product="nws-ocean", prefix="metoffice_foam1_amm15_NWS"
    )
    directory = root / "nws-ocean" / DAY.strftime("%Y/%m/%d/T0000Z")
    stamp = DAY.strftime("%Y%m%d")
    ocean_hourly_file(pd.date_range(DAY + pd.Timedelta(1, "h"), periods=24, freq="1h")).to_netcdf(
        directory / f"level1_wave_amm15_NWS_WAV_b{stamp}_hi{stamp}.nc"
    )
    provider = MetOfficeNWSOceanSurfaceHourlyProvider(config=local_config, archive_root=root)

    assert not any("_WAV_" in pathlib.Path(f).name for f in provider.fetch(DAY))


def test_products_that_share_a_long_name_are_disambiguated(local_config, tmp_path):
    """``CUR`` and ``MAXCURU`` both call themselves eastward current velocity."""
    root = tmp_path / "o"
    directory = root / "nws-ocean" / DAY.strftime("%Y/%m/%d/T0000Z")
    directory.mkdir(parents=True, exist_ok=True)
    stamp = DAY.strftime("%Y%m%d")
    hours = pd.date_range(DAY + pd.Timedelta(1, "h"), periods=24, freq="1h")
    for code in ("CUR", "MAXCURU"):
        ocean_hourly_file(hours, name="uo", long_name="Eastward Current Velocity").to_netcdf(
            directory / f"metoffice_foam1_amm15_NWS_{code}_b{stamp}_hi{stamp}.nc"
        )
    provider = MetOfficeNWSOceanSurfaceHourlyProvider(config=local_config, archive_root=root)

    ds = provider.process(provider.fetch(DAY), DAY)
    assert set(ds.data_vars) == {
        "eastward_current_velocity_cur",
        "eastward_current_velocity_maxcuru",
    }


def test_ocean_fetch_is_empty_when_the_run_was_never_published(local_config, tmp_path):
    provider = MetOfficeGlobalOceanHourlyProvider(config=local_config, archive_root=tmp_path)
    assert provider.fetch(DAY) == []
    assert provider.run_partition(DAY) is False


# ------------------------------------------------------------------------- generations


def test_a_resolution_change_starts_a_new_generation(local_config, tmp_path):
    """The Met Office upgrades these models; an upgrade must not fail every partition."""
    root = tmp_path / "wave"
    write_wave(root, DAY, nlat=2)
    write_wave(root, DAY + pd.Timedelta(1, "D"), nlat=4)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    first = provider.store_prefix
    assert provider.run_partition(DAY + pd.Timedelta(1, "D")) is True
    second = provider.store_prefix

    assert first == MetOfficeGlobalWaveProvider.base_store_prefix
    # Named after the day the upgrade appears, not after a counter.
    assert second == "bkr/metoffice/metoffice_global_wave_02012026.icechunk"


def test_pre_upgrade_data_backfills_into_the_generation_it_belongs_to(local_config, tmp_path):
    """Matching on schema rather than recency is what makes a late backfill land right."""
    root = tmp_path / "wave"
    write_wave(root, DAY, nlat=2)
    write_wave(root, DAY + pd.Timedelta(1, "D"), nlat=4)
    write_wave(root, DAY + pd.Timedelta(2, "D"), nlat=2)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    provider.run_partition(DAY)
    provider.run_partition(DAY + pd.Timedelta(1, "D"))
    provider.run_partition(DAY + pd.Timedelta(2, "D"))

    assert provider.store_prefix == MetOfficeGlobalWaveProvider.base_store_prefix
    assert open_store(provider).sizes["time"] == 48


def test_a_partition_already_in_an_older_generation_is_not_rewritten(local_config, tmp_path):
    """`missing_timesteps` has to look across generations, or a backfill redoes them all."""
    root = tmp_path / "wave"
    write_wave(root, DAY, nlat=2)
    write_wave(root, DAY + pd.Timedelta(1, "D"), nlat=4)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    provider.run_partition(DAY)
    provider.run_partition(DAY + pd.Timedelta(1, "D"))

    assert provider.missing_timesteps(pd.DatetimeIndex([DAY])) == []
    assert provider.run_partition(DAY) is False


# -------------------------------------------------------------------------- bitrounding


def test_every_metoffice_provider_bitrounds():
    for cls in PROVIDERS.values():
        assert cls.keepbits == METOFFICE_KEEPBITS, cls.__name__
        assert cls.keepbits_exact == KEEPBITS_EXACT, cls.__name__


def test_the_derived_table_gives_each_family_the_precision_its_units_justify():
    """Angles need fewer bits than salinity; that is the point of a per-variable table."""
    assert KEEPBITS_BY_PATTERN["direction"] < KEEPBITS_BY_PATTERN["salinity"]
    assert KEEPBITS_BY_PATTERN["direction"] < KEEPBITS_BY_PATTERN["temperature"]
    assert all(6 <= bits <= 23 for bits in KEEPBITS_BY_PATTERN.values())


@pytest.mark.parametrize(
    ("magnitude", "tolerance", "expected_error_below"),
    [(360.0, 0.1, 0.1), (350.0, 0.01, 0.01), (50.0, 0.001, 0.001), (40.0, 0.001, 0.001)],
    ids=["direction", "temperature", "salinity", "wave-height"],
)
def test_the_derived_keepbits_actually_hold_their_tolerance(
    magnitude, tolerance, expected_error_below
):
    """The table is only meaningful if the bits it picks really deliver the tolerance."""
    bits = keepbits_for_tolerance(magnitude, absolute_tolerance=tolerance)
    values = np.linspace(-magnitude, magnitude, 20001, dtype="float32")
    error = np.max(np.abs(bitround(values, bits) - values))

    assert error <= expected_error_below, f"{bits} bits gave {error} > {expected_error_below}"


def test_bitrounding_stays_inside_its_declared_error(local_config, tmp_path):
    """The saving is only worth having if the error is smaller than the model's own."""
    values = np.linspace(-40.0, 40.0, 4001, dtype="float32")
    rounded = bitround(values, METOFFICE_KEEPBITS)
    relative = np.abs(rounded - values) / np.maximum(np.abs(values), 1e-6)

    assert np.nanmax(relative) < 2.0 ** -(METOFFICE_KEEPBITS - 1)
    assert rounded.dtype == values.dtype


def test_bitrounding_records_itself_and_leaves_coordinates_alone(local_config, tmp_path):
    root = write_wave(tmp_path / "wave", DAY)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)
    provider.run_partition(DAY)

    ds = open_store(provider)
    variable = ds["significant_wave_height_m"]
    assert variable.attrs["bitround_keepbits"] == KEEPBITS_BY_PATTERN["wave_height"]
    # A rounded latitude would fail the alignment check on the next append.
    assert "bitround_keepbits" not in ds["latitude"].attrs


@pytest.mark.parametrize(
    ("variable", "expected"),
    [
        ("surface_air_pressure", None),
        ("air_pressure_at_sea_level", None),
        ("tropopause_air_pressure", None),
        ("eastward_wind_at_10m", None),
        ("northward_wind_at_10m", None),
        ("wind_speed_10.0m", KEEPBITS_BY_PATTERN["wind_speed"]),
        ("sea_water_potential_temperature", KEEPBITS_BY_PATTERN["temperature"]),
        ("mean_wave_direction_from_mdir", KEEPBITS_BY_PATTERN["direction"]),
        ("a_brand_new_diagnostic", METOFFICE_KEEPBITS),
    ],
    ids=lambda v: str(v),
)
def test_rounding_is_decided_per_variable(variable, expected):
    """Winds and pressures are stored exactly; everything else is rounded."""
    provider = MetOfficeGlobalWaveProvider()
    assert provider.keepbits_for(variable) == expected


def test_an_exempt_variable_is_stored_bit_for_bit():
    """A pressure sits on a ~100000 Pa offset, where one part in 4096 is ~25 Pa."""
    pressure = np.array([100123.456, 98765.432], dtype="float32")
    wind = np.array([12.3456789, -0.0031415], dtype="float32")
    ds = xr.Dataset(
        {
            "surface_air_pressure": ("x", pressure.copy()),
            "eastward_wind_at_10m": ("x", wind.copy()),
            "sea_water_potential_temperature": ("x", wind.copy()),
        }
    )
    provider = MetOfficeGlobalWaveProvider()
    out = bitround_dataset(ds, provider.keepbits_for)

    assert np.array_equal(out["surface_air_pressure"].values, pressure)
    assert np.array_equal(out["eastward_wind_at_10m"].values, wind)
    assert "bitround_keepbits" not in out["surface_air_pressure"].attrs
    assert "bitround_keepbits" not in out["eastward_wind_at_10m"].attrs
    # ... while a field that is not exempt still is rounded.
    assert (
        out["sea_water_potential_temperature"].attrs["bitround_keepbits"]
        == KEEPBITS_BY_PATTERN["temperature"]
    )


def test_the_exempt_patterns_are_documented_substrings():
    assert "pressure" in KEEPBITS_EXACT
    assert {"eastward_wind", "northward_wind"} <= set(KEEPBITS_EXACT)


def test_float32_axis_noise_does_not_split_a_store(local_config, tmp_path):
    """Regression: two float32 descriptions of one grid forked a generation.

    The Met Office global wave store holds longitudes written out exactly
    (0.17578125 ... 359.82421875); today's files rebuild the same axis as
    ``start + i * delta`` in float32, which drifts up to 5e-4 degrees over 1024 points.
    That is 55 m against a 39 km cell — the same grid — but an element-wise digest called
    them different and started a needless generation 3 on a store with 14418 steps in it.
    """
    from planetary_datasets.common.generations import schema_fingerprint

    exact = np.linspace(0.17578125, 359.82421875, 1024, dtype="float64")
    # The measured drift between the two axes grows along the axis and reaches 5.2e-4,
    # which is what float32 accumulation looks like; reproduce that magnitude directly
    # rather than a particular float path that a numpy version might optimise away.
    drifted = exact - np.linspace(0.0, 5.2e-4, 1024)
    assert np.abs(exact - drifted).max() > 5e-4, "fixture no longer reproduces the drift"

    def grid(longitude):
        return xr.Dataset(
            {"hs": (("time", "longitude"), np.zeros((1, 1024), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([DAY]), "longitude": longitude},
        )

    assert schema_fingerprint(grid(exact), "time") == schema_fingerprint(grid(drifted), "time")


def test_a_genuine_regrid_still_splits_a_store():
    """The tolerance must not be so loose that a real resolution change slips through."""
    from planetary_datasets.common.generations import schema_fingerprint

    def grid(n):
        return xr.Dataset(
            {"hs": (("time", "longitude"), np.zeros((1, n), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([DAY]), "longitude": np.linspace(0, 360, n)},
        )

    assert schema_fingerprint(grid(1024), "time") != schema_fingerprint(grid(2048), "time")


def test_a_shifted_domain_of_the_same_size_still_splits_a_store():
    """Same point count, different extent: a different grid, and the digest must see it."""
    from planetary_datasets.common.generations import schema_fingerprint

    def grid(start, stop):
        return xr.Dataset(
            {"hs": (("time", "latitude"), np.zeros((1, 684), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([DAY]), "latitude": np.linspace(start, stop, 684)},
        )

    assert schema_fingerprint(grid(-80, 80), "time") != schema_fingerprint(grid(-90, 90), "time")


def test_a_tolerated_axis_difference_is_snapped_to_the_store(local_config, tmp_path):
    """The fingerprint is tolerant but the store's alignment guard is exact.

    An axis the fingerprint calls the same and ``array_equal`` calls different would be
    routed to a store that then refuses it — and refuses it by returning False, so the
    partition would be reported as "nothing to do" and silently never written. The store's
    own spelling of the axis is adopted instead.
    """
    from planetary_datasets.common.generations import snap_to_stored_coords

    exact = np.linspace(0.17578125, 359.82421875, 64, dtype="float64")
    drifted = exact - np.linspace(0.0, 5.2e-4, 64)

    def grid(longitude, when):
        return xr.Dataset(
            {"hs": (("time", "longitude"), np.zeros((1, 64), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([when]), "longitude": longitude},
        )

    stored = grid(exact, DAY)
    incoming = grid(drifted, DAY + pd.Timedelta(1, "D"))
    assert not np.array_equal(incoming["longitude"].values, stored["longitude"].values)

    snapped = snap_to_stored_coords(incoming, stored, ("longitude",))
    assert np.array_equal(snapped["longitude"].values, stored["longitude"].values)


def test_a_genuinely_different_axis_is_left_alone_to_be_rejected(local_config):
    """Snapping a real regrid onto the store's axis would mislabel where the data is."""
    from planetary_datasets.common.generations import snap_to_stored_coords

    def grid(start, stop):
        return xr.Dataset(
            {"hs": (("time", "latitude"), np.zeros((1, 64), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([DAY]), "latitude": np.linspace(start, stop, 64)},
        )

    stored, incoming = grid(-80, 80), grid(-90, 90)
    snapped = snap_to_stored_coords(incoming, stored, ("latitude",))

    assert np.array_equal(snapped["latitude"].values, incoming["latitude"].values)


# --------------------------------------------------------- dating a new generation


def test_a_new_generation_is_named_after_the_day_the_model_changed(local_config, tmp_path):
    """A counter says a break happened; a date says when, which is the useful half."""
    root = tmp_path / "wave"
    write_wave(root, DAY, nlat=2)
    changed = pd.Timestamp("2025-09-20")
    write_wave(root, changed, nlat=4)
    provider = MetOfficeGlobalWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert provider.store_prefix == MetOfficeGlobalWaveProvider.base_store_prefix

    assert provider.run_partition(changed) is True
    assert provider.store_prefix.endswith("metoffice_global_wave_20092025.icechunk")


def test_the_date_suffix_is_day_month_year():
    from planetary_datasets.common.generations import prefix_for_date

    assert prefix_for_date("a/b/wave.icechunk", "2025-09-20") == "a/b/wave_20092025.icechunk"
    # A January date keeps its leading zeros, or the names would not sort or parse.
    assert prefix_for_date("a/b/wave.icechunk", "2026-01-05") == "a/b/wave_05012026.icechunk"


def test_a_store_without_a_suffix_is_still_handled():
    from planetary_datasets.common.generations import prefix_for_date

    assert prefix_for_date("a/b/wave", "2025-09-20") == "a/b/wave_20092025"


def test_two_changes_on_one_day_get_distinct_names():
    """Unlikely, but two names that collide would silently merge two schemas."""
    from planetary_datasets.common.generations import prefix_for_date

    first = prefix_for_date("a/b/wave.icechunk", "2025-09-20")
    second = prefix_for_date("a/b/wave.icechunk", "2025-09-20", disambiguator=1)
    assert first != second
    assert second == "a/b/wave_20092025_1.icechunk"


def test_legacy_numbered_generations_are_still_found_and_matched(local_config, tmp_path):
    """``metoffice_global_wave_2`` was made by hand and must go on being used."""
    from planetary_datasets.common.generations import (
        existing_generations,
        prefix_for_generation,
        resolve_generation,
    )
    from planetary_datasets.common.store import write_to_icechunk

    base = "bkr/test/legacy.icechunk"
    numbered = prefix_for_generation(base, 2)

    def grid(nlat, when):
        return xr.Dataset(
            {"hs": (("time", "latitude"), np.zeros((1, nlat), dtype="float32"))},
            coords={"time": pd.DatetimeIndex([when]), "latitude": np.linspace(0, 10, nlat)},
        )

    write_to_icechunk(local_config.icechunk_repo(base), grid(4, DAY), append_dim="time", message="x")
    write_to_icechunk(
        local_config.icechunk_repo(numbered), grid(8, DAY), append_dim="time", message="x"
    )

    assert existing_generations(local_config, base) == [base, numbered]
    # The old store is matched on schema, not skipped because its name is the old style.
    later = DAY + pd.Timedelta(1, "D")
    assert resolve_generation(local_config, base, grid(8, later), "time") == numbered
    assert resolve_generation(local_config, base, grid(4, later), "time") == base


def test_a_brand_new_series_starts_at_the_base_name_not_a_dated_one(local_config, tmp_path):
    """There is no change to date yet, so dating the first store would be a lie."""
    root = write_wave(tmp_path / "wave", DAY)
    provider = MetOfficeNWSWaveProvider(config=local_config, archive_root=root)
    # NWS files, so point it at the right product.
    root = write_wave(tmp_path / "w2", DAY, product="nws-wave", marker="wave_uk_standard_v1")
    provider = MetOfficeNWSWaveProvider(config=local_config, archive_root=root)

    assert provider.run_partition(DAY) is True
    assert provider.store_prefix == MetOfficeNWSWaveProvider.base_store_prefix
