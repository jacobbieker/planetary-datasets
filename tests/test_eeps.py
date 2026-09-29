"""Offline tests for the EEPS blended rain-rate provider."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets.providers import eeps

ATTRS = {
    "time_coverage_start": "2024-01-01T12:00:00Z",
    "time_coverage_end": "2024-01-01T12:10:00Z",
    "geospatial_lat_min": 0.0,
    "geospatial_lat_max": 0.3,
    "geospatial_lon_min": 0.0,
    "geospatial_lon_max": 0.4,
    "geospatial_lat_resolution": 0.1,
    "geospatial_lon_resolution": 0.1,
}


def write_fake_eeps(directory, timestamp="202401011200", end="202401011210", attrs=None):
    """Write a netCDF shaped like an EEPS RainRate-Blend granule."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"RRQPE-BLEND-GLB-5km_v1r0_blend_s{timestamp}_e{end}_c{end}.nc"
    ds = xr.Dataset(
        {
            "RRQPE": (("Rows", "Columns"), np.full((4, 5), 2.5, dtype="float32")),
            "DQF": (("Rows", "Columns"), np.zeros((4, 5), dtype="int16")),
            "quality_information": ((), np.int32(0)),
            "MonitoringMetaData": ((), np.int32(0)),
        },
        attrs=attrs if attrs is not None else dict(ATTRS),
    )
    ds["MonitoringMetaData"].attrs = {"Number_of_Input_Files": 7, "Missing_Inputs": "GOES-18"}
    ds.to_netcdf(path)
    ds.close()
    return path


@pytest.fixture
def source_dir(tmp_path):
    """A source directory holding two consecutive granules, 12:00 and 12:10."""
    directory = tmp_path / "EEPS"
    write_fake_eeps(directory)
    write_fake_eeps(directory, timestamp="202401011210", end="202401011220")
    return directory


def test_timestamp_from_filename():
    name = "RRQPE-BLEND-GLB-5km_v1r0_blend_s202401011200_e202401011210_c202401011215.nc"
    assert eeps.timestamp_from_filename(name) == pd.Timestamp("2024-01-01T12:00:00")


class TestGrid:
    def test_latitudes_run_high_to_low(self):
        latitudes, longitudes = eeps._grid_from_attrs(ATTRS)
        assert latitudes.size == 4
        assert longitudes.size == 5
        assert latitudes[0] > latitudes[-1]
        assert longitudes[0] < longitudes[-1]

    def test_real_global_grid_has_the_expected_size(self):
        """The 5 km global grid is the case where float accumulation bites."""
        attrs = {
            "geospatial_lat_min": -60.0,
            "geospatial_lat_max": 60.0,
            "geospatial_lon_min": -180.0,
            "geospatial_lon_max": 180.0,
            "geospatial_lat_resolution": 0.05,
            "geospatial_lon_resolution": 0.05,
        }
        latitudes, longitudes = eeps._grid_from_attrs(attrs)
        assert latitudes.size == 2401
        assert longitudes.size == 7201
        assert latitudes[0] == 60.0
        assert latitudes[-1] == -60.0


class TestOpen:
    def test_georeferences_and_casts(self, tmp_path):
        path = write_fake_eeps(tmp_path)

        ds = eeps.open_eeps_file(path)

        assert ds.sizes == {"time": 1, "latitude": 4, "longitude": 5}
        assert ds["RRQPE"].dtype == np.dtype("float16")
        assert ds["DQF"].dtype == np.dtype("uint8")
        assert pd.Timestamp(ds.time.values[0]) == pd.Timestamp("2024-01-01T12:00:00")
        assert pd.Timestamp(ds["time_end"].values[0]) == pd.Timestamp("2024-01-01T12:10:00")
        assert int(ds["number_of_inputs"].values[0]) == 7
        assert ds.attrs["missing_inputs"] == "GOES-18"
        assert "MonitoringMetaData" not in ds
        assert "quality_information" not in ds

    def test_grid_mismatch_is_an_error(self, tmp_path):
        bad = dict(ATTRS, geospatial_lat_max=0.9)
        path = write_fake_eeps(tmp_path, attrs=bad)
        with pytest.raises(ValueError, match="grid from attributes"):
            eeps.open_eeps_file(path)


class TestProvider:
    @pytest.mark.parametrize("tz", [None, "UTC"], ids=["naive", "aware"])
    def test_fetch_matches_on_timestamp(self, source_dir, tz):
        """Dagster hands over tz-aware starts; a naive comparison would silently miss."""
        provider = eeps.EEPSProvider(source_dir=source_dir)

        found = provider.fetch(pd.Timestamp("2024-01-01T12:10", tz=tz))

        assert len(found) == 1
        assert "s202401011210" in found[0]

    def test_fetch_returns_nothing_when_absent(self, tmp_path):
        provider = eeps.EEPSProvider(source_dir=tmp_path / "EEPS")
        assert provider.fetch(pd.Timestamp("2024-01-01T12:10")) == []

    def test_available_timestamps(self, source_dir):
        provider = eeps.EEPSProvider(source_dir=source_dir)
        assert list(provider.available_timestamps()) == [
            pd.Timestamp("2024-01-01T12:00"),
            pd.Timestamp("2024-01-01T12:10"),
        ]

    def test_round_trip_through_the_store(self, tmp_path, local_config):
        write_fake_eeps(tmp_path / "EEPS")
        provider = eeps.EEPSProvider(source_dir=tmp_path / "EEPS", config=local_config)
        it = pd.Timestamp("2024-01-01T12:00")

        # A tz-aware partition start, as Dagster passes it, must land on the naive timestep.
        assert provider.run_partition(it.tz_localize("UTC")) is True
        # A second run must find the timestep already stored and do nothing.
        assert provider.run_partition(it) is False

        stored = read_store(provider)
        assert pd.Timestamp(stored.time.values[0]) == it
        assert set(stored.data_vars) >= {"RRQPE", "DQF"}
