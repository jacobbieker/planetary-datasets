"""Shared helpers: downloads, dataset shaping, store writes."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.common import dataset as ds_helpers
from planetary_datasets.common import download as dl
from planetary_datasets.common import store as store_helpers


class TestReducePrecision:
    def test_matching_variables_are_downcast(self):
        ds = xr.Dataset(
            {
                "wind_speed": ("x", np.ones(3, dtype="float32")),
                "pressure": ("x", np.ones(3, dtype="float32")),
            }
        )
        out = ds_helpers.reduce_precision(ds)
        assert out["wind_speed"].dtype == np.float16
        assert out["pressure"].dtype == np.float32

    def test_input_is_not_modified(self):
        ds = xr.Dataset({"wind_speed": ("x", np.ones(3, dtype="float32"))})
        ds_helpers.reduce_precision(ds)
        assert ds["wind_speed"].dtype == np.float32


class TestCoordinateNormalisation:
    def test_longitude_is_converted_to_minus_180(self):
        ds = xr.Dataset({"v": ("longitude", np.arange(4))}, coords={"longitude": [0, 90, 180, 270]})
        out = ds_helpers.lon_to_m180(ds)
        assert out.longitude.min() >= -180
        assert out.longitude.max() < 180
        assert list(out.longitude.values) == sorted(out.longitude.values)

    def test_descending_latitude_is_sorted(self):
        ds = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [10, 0, -10]})
        out = ds_helpers.make_spatial_coords_increasing(ds)
        assert list(out.latitude.values) == [-10, 0, 10]

    def test_missing_coords_are_tolerated(self):
        ds = xr.Dataset({"v": ("x", np.arange(3))})
        assert ds_helpers.make_lat_lon_coords_consistent(ds).equals(ds)


class TestCoordsMatch:
    def test_identical_coords_match(self):
        a = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [1, 2, 3]})
        assert ds_helpers.coords_match(a, a, ("latitude",)) == (True, None)

    def test_differing_values_are_reported(self):
        a = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [1, 2, 3]})
        b = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [1, 2, 4]})
        assert ds_helpers.coords_match(a, b, ("latitude",)) == (False, "latitude")

    def test_differing_lengths_are_reported_not_raised(self):
        a = xr.Dataset({"v": ("latitude", np.arange(3))}, coords={"latitude": [1, 2, 3]})
        b = xr.Dataset({"v": ("latitude", np.arange(2))}, coords={"latitude": [1, 2]})
        assert ds_helpers.coords_match(a, b, ("latitude",)) == (False, "latitude")


class TestDownloadOne:
    def test_file_is_downloaded(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"payload")
        dest = tmp_path / "out" / "dest.bin"
        assert dl.download_one(str(src), dest) == dest
        assert dest.read_bytes() == b"payload"

    def test_existing_file_is_skipped(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"old")
        dl.download_one(str(src), dest)
        assert dest.read_bytes() == b"old"

    def test_overwrite_forces_a_redownload(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"old")
        dl.download_one(str(src), dest, overwrite=True)
        assert dest.read_bytes() == b"new"

    def test_failure_returns_none_and_leaves_no_partial(self, tmp_path):
        dest = tmp_path / "dest.bin"
        assert dl.download_one(str(tmp_path / "missing.bin"), dest, retries=1, backoff=0) is None
        assert not dest.exists()
        assert not dest.with_name("dest.bin.part").exists()

    def test_zero_byte_file_is_not_treated_as_downloaded(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"payload")
        dest = tmp_path / "dest.bin"
        dest.touch()
        dl.download_one(str(src), dest)
        assert dest.read_bytes() == b"payload"


class TestCleanupFiles:
    def test_files_and_index_sidecars_are_removed(self, tmp_path):
        grib = tmp_path / "a.grib2"
        grib.write_bytes(b"x")
        idx = tmp_path / "a.grib2.5b7b6.idx"
        idx.write_bytes(b"y")
        assert dl.cleanup_files(grib) == 2
        assert not grib.exists() and not idx.exists()

    def test_missing_file_is_tolerated(self, tmp_path):
        assert dl.cleanup_files(tmp_path / "nope.grib2") == 0


class TestBuildEncoding:
    def test_every_variable_gets_a_compressor(self, sample_dataset):
        enc = store_helpers.build_encoding(sample_dataset)
        assert set(enc) == {"temperature", "pressure", "time"}
        assert "compressors" in enc["temperature"]

    def test_append_dim_gets_an_integer_time_encoding(self, sample_dataset):
        enc = store_helpers.build_encoding(sample_dataset)
        assert enc["time"]["dtype"] == "int64"


class TestStoreRoundTrip:
    def test_first_write_creates_the_store(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/roundtrip.icechunk")
        assert store_helpers.write_to_icechunk(repo, sample_dataset) is True
        assert store_helpers.existing_times(repo).size == 1

    def test_second_write_of_the_same_time_is_skipped(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/skip.icechunk")
        store_helpers.write_to_icechunk(repo, sample_dataset)
        assert store_helpers.write_to_icechunk(repo, sample_dataset) is False
        assert store_helpers.existing_times(repo).size == 1

    def test_a_new_time_is_appended(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/append.icechunk")
        store_helpers.write_to_icechunk(repo, sample_dataset)
        later = sample_dataset.assign_coords(time=pd.DatetimeIndex(["2026-01-01T01:00"]))
        assert store_helpers.write_to_icechunk(repo, later) is True
        assert store_helpers.existing_times(repo).size == 2

    def test_mismatched_variables_are_refused(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/vars.icechunk")
        store_helpers.write_to_icechunk(repo, sample_dataset)
        different = sample_dataset.drop_vars("pressure").assign_coords(
            time=pd.DatetimeIndex(["2026-01-01T01:00"])
        )
        assert store_helpers.write_to_icechunk(repo, different) is False
        assert store_helpers.existing_times(repo).size == 1

    def test_mismatched_coords_are_refused(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/coords.icechunk")
        store_helpers.write_to_icechunk(repo, sample_dataset)
        shifted = sample_dataset.assign_coords(
            time=pd.DatetimeIndex(["2026-01-01T01:00"]),
            latitude=sample_dataset.latitude.values + 1,
        )
        assert store_helpers.write_to_icechunk(repo, shifted) is False

    def test_has_timestep_reflects_what_was_written(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/has.icechunk")
        stamp = pd.Timestamp("2026-01-01T00:00")
        assert store_helpers.has_timestep(repo, stamp) is False
        store_helpers.write_to_icechunk(repo, sample_dataset)
        assert store_helpers.has_timestep(repo, stamp) is True

    def test_missing_timesteps_filters_what_is_stored(self, local_config, sample_dataset):
        repo = local_config.icechunk_repo("test/missing.icechunk")
        store_helpers.write_to_icechunk(repo, sample_dataset)
        wanted = pd.DatetimeIndex(["2026-01-01T00:00", "2026-01-01T01:00"])
        assert store_helpers.missing_timesteps(repo, list(wanted)) == [pd.Timestamp("2026-01-01T01:00")]

    def test_existing_times_is_empty_for_a_fresh_store(self, local_config):
        repo = local_config.icechunk_repo("test/fresh.icechunk")
        assert store_helpers.existing_times(repo).size == 0

    def test_dataset_without_append_dim_is_rejected(self, local_config):
        repo = local_config.icechunk_repo("test/nodim.icechunk")
        ds = xr.Dataset({"v": ("x", np.arange(3))})
        with pytest.raises(ValueError, match="no 'time' coordinate"):
            store_helpers.write_to_icechunk(repo, ds)
