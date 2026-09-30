"""The SILAM providers, exercised offline against a local store.

No test touches the network: ``fetch`` is replaced by synthetic NetCDF files written
to ``tmp_path``, which is enough to exercise the real ``process`` and the real
append-or-create write through :class:`~planetary_datasets.base.BaseProvider`.
"""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets import config as config_module
from planetary_datasets.providers.silam import (
    IncompleteForecast,
    SILAMAerosolProvider,
    SILAMDustProvider,
    _species_of,
)

INIT = pd.Timestamp("2026-09-25T00:00")


def _grid(n_lat: int = 4, n_lon: int = 6) -> dict:
    return {
        "lat": np.linspace(-80.0, 80.0, n_lat),
        "lon": np.linspace(-170.0, 170.0, n_lon),
    }


def write_dust_step(directory, init: pd.Timestamp, step: int) -> str:
    """Write a file shaped like one SILAM dust forecast step."""
    grid = _grid()
    valid = init + pd.Timedelta(step, "h")
    shape = (1, 1, grid["lat"].size, grid["lon"].size)
    ds = xr.Dataset(
        {
            "cnc_dust": (("time", "hybrid", "lat", "lon"), np.full(shape, float(step), "float32")),
            "wd_dust": (("time", "lat", "lon"), np.full(shape[:1] + shape[2:], 1.0, "float32")),
            # Hybrid level coefficients: dropped by the provider, and deliberately only a
            # subset of the names the older SILAM versions used.
            "a": (("hybrid",), np.zeros(1, "float32")),
            "b": (("hybrid",), np.zeros(1, "float32")),
            "a_half": (("hybrid_half",), np.zeros(2, "float32")),
        },
        coords={
            "time": pd.DatetimeIndex([valid]),
            "hybrid": np.array([10.0], "float32"),
            "lat": grid["lat"],
            "lon": grid["lon"],
        },
    )
    path = directory / f"SILAM-dust-glob01_v5_7_2_{init:%Y%m%d%H}_{step:03d}.nc4"
    ds.to_netcdf(path)
    return str(path)


def write_aerosol_file(
    directory, init: pd.Timestamp, species: str, day: int, units: str = "ug/m3 (EU)"
) -> str:
    """Write a file shaped like one species/day slice of the surface aerosol forecast."""
    grid = _grid()
    valid = pd.DatetimeIndex([init + pd.Timedelta(day * 24 + h, "h") for h in range(1, 25)])
    values = np.ones((24, grid["lat"].size, grid["lon"].size), "float32")
    ds = xr.Dataset(
        {species: (("time", "lat", "lon"), values, {"units": units})},
        coords={"time": valid, "lat": grid["lat"], "lon": grid["lon"]},
    )
    path = directory / f"silam_glob_v6_1_{init:%Y%m%d}_{species}_d{day}.nc"
    ds.to_netcdf(path)
    return str(path)


class _OfflineDust(SILAMDustProvider):
    """Dust provider whose inputs are written locally instead of downloaded."""

    def __init__(self, directory, **kwargs):
        super().__init__(max_step=3, **kwargs)
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory

    def fetch(self, it, temp_dir=None, **kwargs):
        return [write_dust_step(self.directory, it, step) for step in range(1, self.max_step + 1)]


class _OfflineAerosol(SILAMAerosolProvider):
    """Aerosol provider whose inputs are written locally instead of downloaded."""

    def __init__(self, directory, units=None, **kwargs):
        super().__init__(forecast_days=2, **kwargs)
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.units = units or {}

    def fetch(self, it, temp_dir=None, **kwargs):
        return [
            write_aerosol_file(
                self.directory, it, species, day, units=self.units.get(species, "ug/m3 (EU)")
            )
            for species in ("PM25", "SO2")
            for day in range(self.forecast_days)
        ]


@pytest.fixture
def dust(local_config, tmp_path):
    return _OfflineDust(tmp_path / "dust_in", config=local_config)


@pytest.fixture
def aerosol(local_config, tmp_path):
    return _OfflineAerosol(tmp_path / "aerosol_in", config=local_config)


@pytest.mark.parametrize("cls", [SILAMDustProvider, SILAMAerosolProvider])
def test_store_prefix_is_a_relative_icechunk_path(cls):
    """The prefix is resolved against the configured bucket, so it must not carry its own."""
    assert cls.store_prefix.endswith(".icechunk")
    assert not cls.store_prefix.startswith("s3://")


def test_dust_urls_cover_every_step():
    urls = SILAMDustProvider(max_step=4).urls_for(INIT)
    assert len(urls) == 4
    assert urls[0].endswith("SILAM-dust-glob01_v5_7_2_2026092500_001.nc4")
    assert urls[-1].endswith("_004.nc4")


def test_dust_process_reshapes_to_init_time_and_step(dust):
    files = dust.fetch(INIT)
    processed = dust.process(files, INIT)

    assert processed.sizes["init_time"] == 1
    assert processed.sizes["step"] == 3
    assert processed["init_time"].values[0] == np.datetime64(INIT)
    assert list(processed["step"].values) == [np.timedelta64(h, "h") for h in (1, 2, 3)]
    # Hybrid level machinery is removed rather than carried through.
    assert "hybrid" not in processed.dims
    assert "hybrid_half" not in processed.dims
    assert set(processed.data_vars) == {"cnc_dust", "wd_dust"}
    assert {"latitude", "longitude"} <= set(processed.coords)


def test_dust_run_partition_writes_then_skips(dust):
    assert dust.run_partition(INIT) is True
    assert dust.run_partition(INIT) is False

    stored = read_store(dust)
    assert stored["init_time"].values[0] == np.datetime64(INIT)
    assert stored.sizes["step"] == 3


def test_aerosol_process_merges_species_and_days(aerosol):
    files = aerosol.fetch(INIT)
    processed = aerosol.process(files, INIT)

    assert set(processed.data_vars) == {"PM25", "SO2"}
    assert processed.sizes["step"] == 48
    assert processed["step"].values[0] == np.timedelta64(1, "h")
    assert processed["PM25"].dtype == np.dtype("float16")
    # The forecast is indexed by run time, not by the first valid time.
    assert processed["init_time"].values[0] == np.datetime64(INIT)


def test_aerosol_run_partition_round_trips(aerosol):
    assert aerosol.run_partition(INIT) is True

    stored = read_store(aerosol)
    assert stored["init_time"].values[0] == np.datetime64(INIT)
    assert set(stored.data_vars) == {"PM25", "SO2"}


@pytest.fixture
def listed_aerosol(local_config, monkeypatch):
    """A real aerosol provider whose bucket listing is replaced by ``list_keys``."""

    def make(list_keys):
        monkeypatch.setattr(SILAMAerosolProvider, "list_keys", list_keys)
        return SILAMAerosolProvider(config=local_config)

    return make


def test_aerosol_fetch_returns_nothing_when_the_day_is_absent(listed_aerosol):
    provider = listed_aerosol(lambda self, it: [])
    assert provider.fetch(INIT) == []
    # An empty fetch is "nothing to do", not a failure.
    assert provider.run_partition(INIT) is False


def test_species_is_read_from_the_object_key():
    assert _species_of("global/20260925/silam_glob_v6_1_20260925_PM25_d0.nc") == "PM25"
    assert _species_of("global/20260925/silam_glob_v6_1_20260925_airdens_d4.nc") == "airdens"


def test_aerosol_keeps_source_precision_when_units_would_underflow(local_config, tmp_path):
    provider = _OfflineAerosol(
        tmp_path / "aerosol_units", config=local_config, units={"SO2": "kg/m3"}
    )
    processed = provider.process(provider.fetch(INIT), INIT)
    assert processed["PM25"].dtype == np.dtype("float16")
    # 1e-8 kg/m3 is below float16's smallest subnormal; casting would zero the field.
    assert processed["SO2"].dtype == np.dtype("float32")


def test_aerosol_fetch_fails_on_a_half_published_run(listed_aerosol):
    provider = listed_aerosol(
        lambda self, it: [
            "global/20260925/silam_glob_v6_1_20260925_SO2_d0.nc",
            "global/20260925/silam_glob_v6_1_20260925_SO2_d1.nc",
        ]
    )
    with pytest.raises(IncompleteForecast):
        provider.fetch(INIT)


def test_aerosol_listing_errors_are_not_mistaken_for_an_empty_day(listed_aerosol):
    def boom(self, it):
        raise OSError("S3 is having a day")

    provider = listed_aerosol(boom)
    with pytest.raises(OSError):
        provider.fetch(INIT)


def test_dust_fetch_fails_when_only_some_steps_download(local_config, monkeypatch):
    provider = SILAMDustProvider(config=local_config, max_step=3)
    monkeypatch.setattr(
        "planetary_datasets.providers.silam.download_many",
        lambda urls, dest, **kw: [pathlib.Path(dest) / "one.nc4"],
    )
    with pytest.raises(IncompleteForecast):
        provider.fetch(INIT)


def test_tempdir_is_taken_from_the_configured_scratch_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("PLANETARY_DATASETS_SCRATCH_DIR", str(tmp_path / "scratch"))
    provider = SILAMDustProvider(config=config_module.load_config(env_file=tmp_path / "none.env"))
    with provider.local_tempdir() as td:
        assert str(td).startswith(str(tmp_path / "scratch"))


def test_write_is_refused_loudly_when_the_step_axis_changes(local_config, tmp_path):
    short = _OfflineDust(tmp_path / "dust_short", config=local_config)
    assert short.run_partition(INIT) is True

    longer = _OfflineDust(tmp_path / "dust_long", config=local_config)
    longer.max_step = 4
    with pytest.raises(IncompleteForecast):
        longer.run_partition(INIT + pd.Timedelta(1, "D"))
