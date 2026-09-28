"""Offline tests for the GFS provider. Nothing here touches the network."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import gfs
from planetary_datasets.providers.gfs import GFSProvider


@pytest.fixture
def provider(local_config):
    return GFSProvider(config=local_config, forecast_steps=(0, 6))


# --- URL construction ------------------------------------------------------------------


def test_gfs_url_uses_the_two_gdex_datasets():
    it = pd.Timestamp("2016-01-01T00:00")
    assert gfs.gfs_url(it, 6) == (
        "https://osdf-director.osg-htc.org/ncar/gdex/d084001/2016/20160101/"
        "gfs.0p25.2016010100.f006.grib2"
    )
    assert gfs.gfs_url(it, 6, gfs.GFS_EXTRA) == (
        "https://osdf-director.osg-htc.org/ncar/gdex/d084003/2016/20160101/"
        "gfs.0p25b.2016010100.f006.grib2"
    )


def test_gfs_urls_pairs_both_datasets_per_step():
    pairs = gfs.gfs_urls(pd.Timestamp("2020-07-04T18:00"), steps=(0, 12))
    assert len(pairs) == 2
    for main, extra in pairs:
        assert "d084001" in main and "d084003" in extra
        assert main.endswith(".grib2") and extra.endswith(".grib2")
    assert ".f000." in pairs[0][0]
    assert ".f012." in pairs[1][0]


def test_prepbufr_urls_cover_every_cycle():
    urls = gfs.ncep_prepbufr_urls(pd.date_range("2016-01-01", periods=2))
    assert len(urls) == 8
    assert urls[0].endswith("prepbufr.gdas.20160101.t00z.nr.48h")
    assert urls[-1].endswith("prepbufr.gdas.20160102.t18z.nr.48h")


# --- Pairing downloaded files ----------------------------------------------------------


def test_group_files_by_step_pairs_main_and_extra():
    grouped = gfs.group_files_by_step(
        [
            "/tmp/gfs.0p25.2016010100.f000.grib2",
            "/tmp/gfs.0p25b.2016010100.f000.grib2",
            "/tmp/gfs.0p25.2016010100.f006.grib2",
            "/tmp/gfs.0p25b.2016010100.f006.grib2",
        ]
    )
    assert sorted(grouped) == [0, 6]
    assert grouped[6]["main"].endswith("gfs.0p25.2016010100.f006.grib2")
    assert grouped[6]["extra"].endswith("gfs.0p25b.2016010100.f006.grib2")


def test_group_files_by_step_survives_the_regrid_suffix():
    grouped = gfs.group_files_by_step(
        [
            "/tmp/gfs.0p25.2016010100.f018.regrid_1.0.grib2",
            "/tmp/gfs.0p25b.2016010100.f018.regrid_1.0.grib2",
        ]
    )
    assert list(grouped) == [18]
    assert set(grouped[18]) == {"main", "extra"}


def test_group_files_by_step_rejects_an_unparseable_name():
    with pytest.raises(ValueError, match="forecast step"):
        gfs.group_files_by_step(["/tmp/something-else.grib2"])


# --- Dataset filtering and naming ------------------------------------------------------


def _single_var(name, coords, long_name=None):
    attrs = {"long_name": long_name} if long_name else {}
    return xr.Dataset(
        {name: (("latitude",), np.zeros(3, dtype="float32"), attrs)},
        coords={"latitude": np.array([-1.0, 0.0, 1.0]), **coords},
    )


def test_filter_datasets_drops_excluded_level_types():
    keep = _single_var("t", {"surface": 0.0}, "Temperature")
    drop = _single_var("t", {"tropopause": 0.0}, "Temperature")
    assert len(gfs.filter_datasets([keep, drop])) == 1


def test_filter_datasets_suffixes_by_level_type():
    surface = gfs.filter_datasets([_single_var("t", {"surface": 0.0}, "Temperature")])[0]
    assert "temperature_at_surface" in surface.data_vars

    height = gfs.filter_datasets([_single_var("t2", {"heightAboveGround": 2.0}, "Temperature")])[0]
    assert "temperature_at_2m" in height.data_vars
    # The scalar height coordinate is consumed by the suffix and must not linger.
    assert "heightAboveGround" not in height.coords


def test_height_suffix_does_not_depend_on_the_level_dtype():
    as_float = _single_var("t2", {"heightAboveGround": np.float64(10.0)}, "Wind")
    as_int = _single_var("t2", {"heightAboveGround": np.int64(10)}, "Wind")
    assert gfs._level_suffix(as_float) == gfs._level_suffix(as_int) == "_at_10m"


def test_filter_datasets_drops_multi_height_datasets():
    ds = xr.Dataset(
        {"t": (("heightAboveGround", "latitude"), np.zeros((2, 3), dtype="float32"))},
        coords={"heightAboveGround": [2.0, 10.0], "latitude": [-1.0, 0.0, 1.0]},
    )
    assert gfs.filter_datasets([ds]) == []


def test_strip_height_prefixes():
    ds = xr.Dataset({"2m_temperature": ("x", [1.0]), "pressure": ("x", [2.0])})
    stripped = gfs._strip_height_prefixes(ds)
    assert set(stripped.data_vars) == {"temperature", "pressure"}


def test_strip_height_prefixes_keeps_a_colliding_name():
    # Both GDEX files can spell the same field differently; renaming one onto the other
    # would raise and take the whole init time down.
    ds = xr.Dataset({"2m_temperature": ("x", [1.0]), "temperature": ("x", [2.0])})
    stripped = gfs._strip_height_prefixes(ds)
    assert set(stripped.data_vars) == {"2m_temperature", "temperature"}


def test_temperature_levels_defines_the_vertical_grid():
    warm = xr.Dataset(
        {"temperature": (("isobaricInhPa",), np.zeros(3, dtype="float32"))},
        coords={"isobaricInhPa": [1000.0, 850.0, 500.0]},
    )
    humid = xr.Dataset(
        {"relative_humidity": (("isobaricInhPa",), np.zeros(2, dtype="float32"))},
        coords={"isobaricInhPa": [1000.0, 100.0]},
    )
    assert gfs._temperature_levels([warm, humid]) == [500.0, 850.0, 1000.0]


# --- Regridding ------------------------------------------------------------------------


def test_regrid_falls_back_when_metview_is_missing(tmp_path, monkeypatch):
    monkeypatch.setitem(__import__("sys").modules, "metview", None)
    grib = tmp_path / "gfs.0p25.2016010100.f000.grib2"
    grib.write_bytes(b"GRIB")
    # A None entry in sys.modules makes the import raise ImportError.
    assert gfs.regrid_grib(grib, 1.0) is None


def test_regrid_reuses_an_existing_output(tmp_path):
    grib = tmp_path / "gfs.0p25.2016010100.f000.grib2"
    grib.write_bytes(b"GRIB")
    done = tmp_path / "gfs.0p25.2016010100.f000.regrid_1.0.grib2"
    done.write_bytes(b"GRIB")
    assert gfs.regrid_grib(grib, 1.0) == done


# --- Fetch -----------------------------------------------------------------------------


def test_fetch_downloads_both_datasets_for_every_step(provider, tmp_path, monkeypatch):
    def fake_download_one(url, dest, **kwargs):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"GRIB")
        return dest

    monkeypatch.setattr(gfs, "download_one", fake_download_one)
    provider.regrid_degrees = None
    files = provider.fetch(pd.Timestamp("2016-01-01T00:00"), temp_dir=tmp_path)
    assert len(files) == 4
    assert sorted(gfs.group_files_by_step(files)) == [0, 6]


def test_fetch_returns_nothing_when_a_file_is_unavailable(provider, tmp_path, monkeypatch):
    def fake_download_one(url, dest, **kwargs):
        if ".f006." in url:
            return None
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"GRIB")
        return dest

    monkeypatch.setattr(gfs, "download_one", fake_download_one)
    # A partial init time must be skipped entirely, not written with holes.
    assert provider.fetch(pd.Timestamp("2016-01-01T00:00"), temp_dir=tmp_path) == []


def test_regridding_is_all_or_nothing(provider, tmp_path, monkeypatch):
    native = [tmp_path / f"gfs.0p25.2016010100.f{step:03d}.grib2" for step in (0, 6)]
    for path in native:
        path.write_bytes(b"GRIB")

    # One file regrids, the other does not: falling back for only one of them would mix a
    # 1 degree grid with a 0.25 degree one inside the same init time.
    def half_regrid(path, degrees):
        return pathlib.Path(f"{path}.regridded") if ".f000." in str(path) else None

    monkeypatch.setattr(gfs, "regrid_grib", half_regrid)
    assert provider._regrid_all(native) == [str(p) for p in native]

    monkeypatch.setattr(gfs, "regrid_grib", lambda path, degrees: pathlib.Path(f"{path}.rg"))
    assert provider._regrid_all(native) == [f"{p}.rg" for p in native]


# --- Process ---------------------------------------------------------------------------


def _step_dataset(step_hours: int) -> xr.Dataset:
    """A stand-in for one merged forecast step, with GFS-like coordinate conventions."""
    return xr.Dataset(
        {
            "temperature": (
                ("level", "latitude", "longitude"),
                np.zeros((2, 3, 4), dtype="float32"),
            ),
            "surface_pressure_at_surface": (
                ("latitude", "longitude"),
                np.zeros((3, 4), dtype="float32"),
            ),
        },
        coords={
            "level": [850.0, 1000.0],
            # GRIB order: latitude descending, longitude on 0-360.
            "latitude": [1.0, 0.0, -1.0],
            "longitude": [0.0, 90.0, 180.0, 270.0],
            "step": pd.Timedelta(hours=step_hours),
        },
    )


def test_process_builds_a_time_and_step_cube(provider, monkeypatch):
    monkeypatch.setattr(
        gfs, "merge_step", lambda main, extra: _step_dataset(int(main.split(".f")[1][:3]))
    )
    it = pd.Timestamp("2016-01-01T00:00")
    ds = provider.process(
        [
            "/tmp/gfs.0p25.2016010100.f000.grib2",
            "/tmp/gfs.0p25b.2016010100.f000.grib2",
            "/tmp/gfs.0p25.2016010100.f006.grib2",
            "/tmp/gfs.0p25b.2016010100.f006.grib2",
        ],
        it,
    )
    assert ds.sizes["time"] == 1
    assert ds.sizes["step"] == 2
    assert ds.time.values[0] == np.datetime64(it)
    # Longitudes normalised onto -180..180, both spatial axes ascending.
    assert ds.longitude.min() < 0 and ds.longitude.max() <= 180
    assert (ds.latitude.diff("latitude") > 0).all()
    assert (ds.longitude.diff("longitude") > 0).all()


def test_process_rejects_a_partial_init_time(provider, monkeypatch):
    monkeypatch.setattr(gfs, "merge_step", lambda main, extra: _step_dataset(0))
    with pytest.raises(ValueError, match="expected forecast steps"):
        provider.process(
            [
                "/tmp/gfs.0p25.2016010100.f000.grib2",
                "/tmp/gfs.0p25b.2016010100.f000.grib2",
            ],
            pd.Timestamp("2016-01-01T00:00"),
        )


def test_process_rejects_a_step_missing_its_companion_file(provider, monkeypatch):
    monkeypatch.setattr(gfs, "merge_step", lambda main, extra: _step_dataset(0))
    with pytest.raises(ValueError, match="missing a GDEX file"):
        provider.process(
            [
                "/tmp/gfs.0p25.2016010100.f000.grib2",
                "/tmp/gfs.0p25b.2016010100.f000.grib2",
                "/tmp/gfs.0p25.2016010100.f006.grib2",
            ],
            pd.Timestamp("2016-01-01T00:00"),
        )


# --- Writing ---------------------------------------------------------------------------


def _cube(it: str, levels=(850.0, 1000.0), extra_var: bool = False) -> xr.Dataset:
    data = {
        "temperature": (
            ("time", "level", "latitude"),
            np.zeros((1, len(levels), 3), dtype="float32"),
        )
    }
    if extra_var:
        data["experimental"] = (("time", "latitude"), np.zeros((1, 3), dtype="float32"))
    return xr.Dataset(
        data,
        coords={
            "time": pd.DatetimeIndex([it]),
            "level": list(levels),
            "latitude": [-1.0, 0.0, 1.0],
        },
    )


def test_write_appends_a_second_init_time(provider):
    repo = provider.get_icechunk_repo()
    assert provider.write_to_icechunk(repo, _cube("2016-01-01T00:00")) is True
    assert provider.write_to_icechunk(repo, _cube("2016-01-01T06:00")) is True
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert list(pd.DatetimeIndex(stored.time.values).strftime("%H")) == ["00", "06"]


def test_write_drops_levels_and_variables_the_store_does_not_have(provider):
    repo = provider.get_icechunk_repo()
    provider.write_to_icechunk(repo, _cube("2016-01-01T00:00"))
    # A later init time with an extra level and an extra variable must still land.
    richer = _cube("2016-01-01T06:00", levels=(700.0, 850.0, 1000.0), extra_var=True)
    assert provider.write_to_icechunk(repo, richer) is True
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert list(stored.level.values) == [850.0, 1000.0]
    assert set(stored.data_vars) == {"temperature"}
    assert stored.sizes["time"] == 2


def test_write_refuses_an_init_time_with_a_different_step_list(provider):
    def cube_with_steps(it, steps):
        return xr.Dataset(
            {
                "temperature": (
                    ("time", "step", "latitude"),
                    np.zeros((1, len(steps), 3), dtype="float32"),
                )
            },
            coords={
                "time": pd.DatetimeIndex([it]),
                "step": [pd.Timedelta(hours=h) for h in steps],
                "latitude": [-1.0, 0.0, 1.0],
            },
        )

    repo = provider.get_icechunk_repo()
    provider.write_to_icechunk(repo, cube_with_steps("2016-01-01T00:00", (0, 6)))
    # A short forecast must be refused cleanly rather than failing inside the append.
    assert provider.write_to_icechunk(repo, cube_with_steps("2016-01-01T06:00", (0,))) is False


def test_write_refuses_an_init_time_that_is_missing_a_stored_level(provider):
    repo = provider.get_icechunk_repo()
    provider.write_to_icechunk(repo, _cube("2016-01-01T00:00"))
    thinner = _cube("2016-01-01T06:00", levels=(1000.0,))
    assert provider.write_to_icechunk(repo, thinner) is False
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert stored.sizes["time"] == 1


# --- Lifecycle -------------------------------------------------------------------------


def test_run_partition_skips_an_init_time_already_stored(provider, monkeypatch):
    it = pd.Timestamp("2016-01-01T00:00")
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: ["a", "b"])
    monkeypatch.setattr(provider, "process", lambda files, it, temp_dir=None, **kw: _cube(str(it)))
    assert provider.run_partition(it) is True
    assert provider.run_partition(it) is False


def test_store_prefix_is_configurable_and_never_hardcodes_a_bucket(provider, tmp_path):
    assert provider.store_prefix == "bkr/gfs/gfs_forecast_6hr_1deg.icechunk"
    assert provider.store_path.startswith(str(tmp_path))


# --- Dagster asset ---------------------------------------------------------------------


def _stub_provider(monkeypatch, written: bool, still_missing: bool):
    """Point the asset at a provider that reports a fixed outcome."""
    from dags.assets import gfs as asset_module

    class StubProvider:
        store_path = "/tmp/stub.icechunk"

        def __init__(self, *args, **kwargs):
            pass

        def run_partition(self, it):
            return written

        def missing_timesteps(self, desired):
            return list(desired) if still_missing else []

    monkeypatch.setattr(asset_module, "GFSProvider", StubProvider)
    return asset_module


def test_asset_reports_a_written_init_time(monkeypatch):
    module = _stub_provider(monkeypatch, written=True, still_missing=False)
    result = module.run_gfs_partition(pd.Timestamp("2016-01-01T00:00"))
    assert result.metadata["written"].value is True


def test_asset_accepts_an_init_time_that_was_already_stored(monkeypatch):
    module = _stub_provider(monkeypatch, written=False, still_missing=False)
    result = module.run_gfs_partition(pd.Timestamp("2016-01-01T00:00"))
    assert result.metadata["written"].value is False


def test_asset_fails_when_nothing_was_written_and_the_init_time_is_still_absent(monkeypatch):
    import dagster as dg

    module = _stub_provider(monkeypatch, written=False, still_missing=True)
    # Reporting success here would mark the partition done and leave a permanent hole.
    with pytest.raises(dg.Failure):
        module.run_gfs_partition(pd.Timestamp("2016-01-01T00:00"))


def test_the_asset_definitions_load(monkeypatch):
    import dagster as dg
    from dagster_docker import PipesDockerClient

    from dags.assets import gfs as asset_module

    defs = dg.Definitions(
        assets=asset_module.assets,
        resources={"pipes_docker_client": PipesDockerClient()},
    )
    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert keys == {"gfs-icechunk", "ncep-gfs-global"}
