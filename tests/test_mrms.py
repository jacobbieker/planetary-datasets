"""Offline tests for the MRMS provider.

Nothing here touches the network: file discovery runs against a fake archive tree on disk,
and the GRIB loader is substituted for a synthetic one so ``process`` and the store
round-trip can be exercised without shipping binary fixtures.
"""

from __future__ import annotations

import gzip

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import mrms


def make_step(variable: str, dtype: str, timestamp: pd.Timestamp) -> xr.Dataset:
    """A one-timestep dataset shaped like a loaded MRMS GRIB."""
    return xr.Dataset(
        {variable: (("time", "latitude", "longitude"), np.ones((1, 3, 4), dtype=dtype))},
        coords={
            "time": pd.DatetimeIndex([timestamp]),
            "latitude": np.linspace(20.0, 55.0, 3),
            "longitude": np.linspace(230.0, 300.0, 4),
        },
    )


@pytest.fixture
def fake_loader(monkeypatch):
    """Replace the GRIB loader with one that synthesises a field from the filename."""

    def _load(filepath, variable, dtype, temp_dir=None):
        return make_step(variable, dtype, mrms.timestamp_from_filename(filepath))

    monkeypatch.setattr(mrms, "load_mrms_grib", _load)
    return _load


def write_fake_grib(path):
    """Create a plausible-looking gzipped file so glob-based discovery has something."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(b"GRIB")
    return path


class TestFilenameParsing:
    def test_modern_name(self):
        name = "MRMS_PrecipRate_00.00_20260920-001400.grib2.gz"
        assert mrms.timestamp_from_filename(name) == pd.Timestamp("2026-09-20T00:14:00")

    def test_legacy_name(self):
        name = "MRMS_PrecipRate_20160122-194600.grib2.gz"
        assert mrms.timestamp_from_filename(name) == pd.Timestamp("2016-01-22T19:46:00")

    def test_full_path(self, tmp_path):
        path = tmp_path / "CONUS" / "MRMS_PrecipFlag_00.00_20201014-120200.grib2.gz"
        assert mrms.timestamp_from_filename(path) == pd.Timestamp("2020-10-14T12:02:00")

    def test_product_match(self):
        products = ("PrecipFlag", "PrecipRate")
        assert (
            mrms.product_from_filename("MRMS_PrecipFlag_00.00_20260920-000000.grib2.gz", products)
            == "PrecipFlag"
        )
        assert mrms.product_from_filename("MRMS_Reflectivity_00.00_x.grib2.gz", products) is None

    def test_product_match_prefers_longest(self):
        """Pass1 must not be matched by a shorter name that is a prefix of it."""
        products = ("MultiSensor_QPE_01H", "MultiSensor_QPE_01H_Pass1")
        name = "MRMS_MultiSensor_QPE_01H_Pass1_00.00_20260920-000000.grib2.gz"
        assert mrms.product_from_filename(name, products) == "MultiSensor_QPE_01H_Pass1"


class TestConfiguration:
    def test_default_is_conus_flag_and_rate(self):
        provider = mrms.MRMSProvider()
        assert provider.region == "CONUS"
        assert provider.products == ("PrecipFlag", "PrecipRate")
        assert provider.store_prefix == "bkr/mrms/mrms.icechunk"
        assert provider.freq == "2min"

    @pytest.mark.parametrize(
        "region,expected",
        [
            ("ALASKA", "bkr/mrms/mrms_ALASKA.icechunk"),
            ("CARIB", "bkr/mrms/mrms_CARIB.icechunk"),
            ("GUAM", "bkr/mrms/mrms_GUAM.icechunk"),
            ("HAWAII", "bkr/mrms/mrms_HAWAII.icechunk"),
        ],
    )
    def test_region_store_prefix(self, region, expected):
        assert mrms.MRMSProvider(region=region).store_prefix == expected

    def test_preciprate_store_prefix(self):
        assert mrms.MRMSPrecipRateProvider().store_prefix == "bkr/mrms/mrms_preciprate.icechunk"

    def test_regional_rate_only_does_not_collide_with_conus(self):
        """The rate-only store name belongs to CONUS; a region must get its own."""
        regional = mrms.MRMSPrecipRateProvider(region="HAWAII")
        assert regional.store_prefix != "bkr/mrms/mrms_preciprate.icechunk"
        assert "HAWAII" in regional.store_prefix

    def test_explicit_store_prefix_wins(self):
        provider = mrms.MRMSProvider(store_prefix="bkr/mrms/custom.icechunk")
        assert provider.store_prefix == "bkr/mrms/custom.icechunk"

    def test_hourly_product_sets_the_pace(self):
        provider = mrms.MRMSProvider(products=("MultiSensor_QPE_01H_Pass2",))
        assert provider.freq == "1h"
        assert len(provider.partition_timestamps(pd.Timestamp("2026-01-01T00:00"))) == 1

    def test_slowest_product_sets_the_pace_when_mixed(self):
        provider = mrms.MRMSProvider(products=("PrecipRate", "RadarOnly_QPE_01H"))
        assert provider.freq == "1h"

    def test_partition_is_thirty_two_minute_steps(self):
        stamps = mrms.MRMSProvider().partition_timestamps(pd.Timestamp("2026-09-20T01:00"))
        assert len(stamps) == 30
        assert stamps[0] == pd.Timestamp("2026-09-20T01:00")
        assert stamps[-1] == pd.Timestamp("2026-09-20T01:58")

    def test_unknown_region_rejected(self):
        with pytest.raises(ValueError, match="unknown MRMS region"):
            mrms.MRMSProvider(region="ATLANTIS")

    def test_unknown_product_rejected(self):
        with pytest.raises(ValueError, match="unknown MRMS product"):
            mrms.MRMSProvider(products=("Sunshine",))

    def test_default_providers_cover_every_store(self):
        prefixes = {p.store_prefix for p in mrms.default_providers()}
        assert prefixes == {
            "bkr/mrms/mrms.icechunk",
            "bkr/mrms/mrms_preciprate.icechunk",
            "bkr/mrms/mrms_ALASKA.icechunk",
            "bkr/mrms/mrms_CARIB.icechunk",
            "bkr/mrms/mrms_GUAM.icechunk",
            "bkr/mrms/mrms_HAWAII.icechunk",
        }


class TestLocalArchive:
    def test_finds_aws_mirror_layout(self, tmp_path):
        target = write_fake_grib(
            tmp_path
            / "aws"
            / "HAWAII"
            / "20260920"
            / "MRMS_PrecipRate_00.00_20260920-000200.grib2.gz"
        )
        provider = mrms.MRMSProvider(region="HAWAII", archive_dir=tmp_path)
        found = provider.local_file("PrecipRate", pd.Timestamp("2026-09-20T00:02"))
        assert found == str(target)

    def test_finds_iastate_mirror_layout(self, tmp_path):
        target = write_fake_grib(
            tmp_path
            / mrms.IASTATE_MIRROR_DIR
            / "2016"
            / "01"
            / "22"
            / "mrms"
            / "ncep"
            / "PrecipFlag"
            / "MRMS_PrecipFlag_00.00_20160122-194600.grib2.gz"
        )
        provider = mrms.MRMSProvider(archive_dir=tmp_path)
        found = provider.local_file("PrecipFlag", pd.Timestamp("2016-01-22T19:46"))
        assert found == str(target)

    def test_missing_file_returns_none(self, tmp_path):
        provider = mrms.MRMSProvider(archive_dir=tmp_path)
        assert provider.local_file("PrecipFlag", pd.Timestamp("2016-01-22T19:46")) is None

    def test_fetch_prefers_local_and_never_hits_the_network(self, tmp_path, monkeypatch):
        it = pd.Timestamp("2026-09-20T01:00")
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)

        def _no_network(*args, **kwargs):
            raise AssertionError("fetch went to the network despite a local archive hit")

        monkeypatch.setattr(provider, "remote_url", _no_network)

        expected = []
        for stamp in provider.partition_timestamps(it):
            for product in provider.products:
                expected.append(
                    str(
                        write_fake_grib(
                            tmp_path
                            / "aws"
                            / "GUAM"
                            / "20260920"
                            / f"MRMS_{product}_00.00_{stamp:%Y%m%d-%H%M%S}.grib2.gz"
                        )
                    )
                )

        assert sorted(provider.fetch(it, temp_dir=tmp_path / "work")) == sorted(expected)

    def test_fetch_drops_timesteps_missing_a_product(self, tmp_path, monkeypatch):
        """An hour where one product never published must fetch nothing, not half of it."""
        it = pd.Timestamp("2026-09-20T01:00")
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        monkeypatch.setattr(provider, "remote_url", lambda product, timestamp: None)
        for stamp in provider.partition_timestamps(it):
            write_fake_grib(
                tmp_path
                / "aws"
                / "GUAM"
                / "20260920"
                / f"MRMS_PrecipFlag_00.00_{stamp:%Y%m%d-%H%M%S}.grib2.gz"
            )

        assert provider.fetch(it, temp_dir=tmp_path / "work") == []

    def test_remote_url_skips_regions_before_the_aws_archive(self):
        provider = mrms.MRMSProvider(region="HAWAII")
        assert provider.remote_url("PrecipRate", pd.Timestamp("2016-01-01T00:00")) is None

    def test_remote_url_uses_iastate_for_old_conus(self):
        provider = mrms.MRMSProvider()
        url = provider.remote_url("PrecipFlag", pd.Timestamp("2016-01-22T19:46"))
        assert url == (
            "https://mtarchive.geol.iastate.edu/2016/01/22/mrms/ncep/PrecipFlag/"
            "MRMS_PrecipFlag_00.00_20160122-194600.grib2.gz"
        )


class TestProcess:
    def test_merges_products_and_concatenates(self, tmp_path, fake_loader):
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        it = pd.Timestamp("2026-09-20T01:00")
        files = [
            f"MRMS_{product}_00.00_{stamp:%Y%m%d-%H%M%S}.grib2.gz"
            for stamp in provider.partition_timestamps(it)[:3]
            for product in provider.products
        ]

        ds = provider.process(files, it)

        assert ds.sizes["time"] == 3
        assert set(ds.data_vars) == {"precipitation_flag", "precipitation_rate"}
        assert ds["precipitation_flag"].dtype == np.dtype("int8")
        assert ds["precipitation_rate"].dtype == np.dtype("float16")
        assert pd.Timestamp(ds.time.values[0]) == it

    def test_drops_timesteps_missing_a_product(self, tmp_path, fake_loader):
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        it = pd.Timestamp("2026-09-20T01:00")
        files = [
            "MRMS_PrecipFlag_00.00_20260920-010000.grib2.gz",
            "MRMS_PrecipRate_00.00_20260920-010000.grib2.gz",
            # 01:02 only has the flag, so it must not reach the store half-empty.
            "MRMS_PrecipFlag_00.00_20260920-010200.grib2.gz",
        ]

        ds = provider.process(files, it)

        assert ds.sizes["time"] == 1
        assert pd.Timestamp(ds.time.values[0]) == it

    def test_no_complete_timesteps_raises(self, tmp_path, fake_loader):
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        with pytest.raises(FileNotFoundError, match="no complete MRMS timesteps"):
            provider.process(
                ["MRMS_PrecipFlag_00.00_20260920-010000.grib2.gz"],
                pd.Timestamp("2026-09-20T01:00"),
            )

    def test_unrelated_files_are_ignored(self, tmp_path, fake_loader):
        provider = mrms.MRMSPrecipRateProvider(region="GUAM", archive_dir=tmp_path)
        ds = provider.process(
            [
                "MRMS_PrecipRate_00.00_20260920-010000.grib2.gz",
                "MRMS_SyntheticPrecipRateID_00.00_20260920-010000.grib2.gz",
            ],
            pd.Timestamp("2026-09-20T01:00"),
        )
        assert set(ds.data_vars) == {"precipitation_rate"}


class TestStoreRoundTrip:
    def _provider(self, local_config, tmp_path):
        return mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path, config=local_config)

    def _dataset(self, stamps):
        return xr.concat(
            [
                xr.merge(
                    [
                        make_step("precipitation_flag", "int8", s),
                        make_step("precipitation_rate", "float16", s),
                    ]
                )
                for s in stamps
            ],
            dim="time",
        )

    def test_write_then_read_back(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        stamps = pd.date_range("2026-09-20T01:00", periods=3, freq="2min")
        repo = provider.get_icechunk_repo()

        assert provider.write_to_icechunk(repo, self._dataset(stamps)) is True

        stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        assert list(pd.DatetimeIndex(stored.time.values)) == list(stamps)
        assert set(stored.data_vars) == {"precipitation_flag", "precipitation_rate"}

    def test_rewriting_the_same_hour_is_a_no_op(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        stamps = pd.date_range("2026-09-20T01:00", periods=3, freq="2min")
        repo = provider.get_icechunk_repo()
        provider.write_to_icechunk(repo, self._dataset(stamps))

        assert provider.write_to_icechunk(repo, self._dataset(stamps)) is False

        stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        assert stored.sizes["time"] == 3

    def test_partial_overlap_appends_only_the_new_steps(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        repo = provider.get_icechunk_repo()
        provider.write_to_icechunk(
            repo, self._dataset(pd.date_range("2026-09-20T01:00", periods=3, freq="2min"))
        )

        overlapping = pd.date_range("2026-09-20T01:04", periods=3, freq="2min")
        assert provider.write_to_icechunk(repo, self._dataset(overlapping)) is True

        stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        times = pd.DatetimeIndex(stored.time.values)
        assert list(times) == list(pd.date_range("2026-09-20T01:00", periods=5, freq="2min"))
        assert times.is_unique

    def test_store_path_is_local_under_test_config(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        assert not provider.store_path.startswith("s3://")
        assert provider.store_path.endswith("bkr/mrms/mrms_GUAM.icechunk")

    def test_fully_stored_partition_is_not_missing(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        it = pd.Timestamp("2026-09-20T01:00")
        provider.write_to_icechunk(
            provider.get_icechunk_repo(), self._dataset(provider.partition_timestamps(it))
        )
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == []

    def test_recent_partial_partition_is_retried(self, local_config, tmp_path):
        """An hour written while the archive was still filling in must not be sealed."""
        provider = self._provider(local_config, tmp_path)
        it = mrms.naive_utc(pd.Timestamp.now("UTC")).floor("h") - pd.Timedelta(hours=2)
        provider.write_to_icechunk(
            provider.get_icechunk_repo(), self._dataset(provider.partition_timestamps(it)[:5])
        )
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == [it]

    def test_settled_partial_partition_is_left_alone(self, local_config, tmp_path):
        """Old gaps are real gaps; re-downloading them on every pass is waste."""
        provider = self._provider(local_config, tmp_path)
        it = pd.Timestamp("2021-01-01T01:00")
        provider.write_to_icechunk(
            provider.get_icechunk_repo(), self._dataset(provider.partition_timestamps(it)[:5])
        )
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == []

    def test_empty_store_reports_everything_missing(self, local_config, tmp_path):
        provider = self._provider(local_config, tmp_path)
        wanted = pd.DatetimeIndex(["2026-09-20T01:00", "2026-09-20T02:00"])
        assert provider.missing_timesteps(wanted) == list(wanted)


class TestTimezoneHandling:
    """Dagster hands partition starts over as tz-aware UTC; the archive is naive."""

    def test_naive_utc_normalises(self):
        aware = pd.Timestamp("2026-09-20T01:00", tz="UTC")
        assert mrms.naive_utc(aware) == pd.Timestamp("2026-09-20T01:00")
        assert mrms.naive_utc(aware).tzinfo is None

    def test_naive_utc_converts_other_offsets(self):
        assert mrms.naive_utc(pd.Timestamp("2026-09-20T03:00", tz="Europe/Berlin")) == pd.Timestamp(
            "2026-09-20T01:00"
        )

    def test_partition_timestamps_accept_aware_input(self):
        provider = mrms.MRMSProvider()
        stamps = provider.partition_timestamps(pd.Timestamp("2026-09-20T01:00", tz="UTC"))
        assert stamps.tz is None
        assert stamps[0] == pd.Timestamp("2026-09-20T01:00")

    def test_remote_url_accepts_aware_input(self):
        provider = mrms.MRMSProvider()
        url = provider.remote_url("PrecipFlag", pd.Timestamp("2016-01-22T19:46", tz="UTC"))
        assert url is not None and url.endswith("MRMS_PrecipFlag_00.00_20160122-194600.grib2.gz")

    def test_local_file_accepts_aware_input(self, tmp_path):
        target = write_fake_grib(
            tmp_path
            / "aws"
            / "GUAM"
            / "20260920"
            / "MRMS_PrecipRate_00.00_20260920-010000.grib2.gz"
        )
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        found = provider.local_file("PrecipRate", pd.Timestamp("2026-09-20T01:00", tz="UTC"))
        assert found == str(target)

    def test_run_partition_accepts_aware_input(self, local_config, tmp_path, monkeypatch):
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path, config=local_config)
        seen = []

        def _fetch(it, temp_dir=None, **kwargs):
            seen.append(it)
            return []

        monkeypatch.setattr(provider, "fetch", _fetch)
        assert provider.run_partition(pd.Timestamp("2026-09-20T01:00", tz="UTC")) is False
        assert seen == [pd.Timestamp("2026-09-20T01:00")]


class TestFactories:
    def test_region_providers_rejects_region(self):
        with pytest.raises(TypeError, match="one provider per region"):
            mrms.region_providers(region="GUAM")

    def test_default_providers_rejects_per_store_arguments(self):
        with pytest.raises(TypeError, match="per store"):
            mrms.default_providers(products=("PrecipRate",))
