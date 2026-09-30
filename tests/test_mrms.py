"""Offline tests for the MRMS provider.

Nothing here touches the network: file discovery runs against a fake archive tree on disk,
and the GRIB loader is substituted for a synthetic one so ``process`` and the store
round-trip can be exercised without shipping binary fixtures.
"""

from __future__ import annotations

import gzip
import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store as stored
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


def write_aws_grib(root, region: str, product: str, stamp: pd.Timestamp):
    """Write a fake GRIB where the AWS mirror layout puts ``product`` at ``stamp``."""
    name = f"MRMS_{product}_00.00_{stamp:%Y%m%d-%H%M%S}.grib2.gz"
    return write_fake_grib(root / "aws" / region / f"{stamp:%Y%m%d}" / name)


class TestFilenameParsing:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("MRMS_PrecipRate_00.00_20260920-001400.grib2.gz", "2026-09-20T00:14:00"),
            ("MRMS_PrecipRate_20160122-194600.grib2.gz", "2016-01-22T19:46:00"),
            (
                pathlib.Path("/archive/CONUS/MRMS_PrecipFlag_00.00_20201014-120200.grib2.gz"),
                "2020-10-14T12:02:00",
            ),
        ],
        ids=["modern_name", "legacy_name", "full_path"],
    )
    def test_timestamp(self, name, expected):
        assert mrms.timestamp_from_filename(name) == pd.Timestamp(expected)

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
        assert provider.freq == "2min"

    @pytest.mark.parametrize(
        ("cls", "kwargs", "expected"),
        [
            (mrms.MRMSProvider, {}, "bkr/mrms/mrms.icechunk"),
            (mrms.MRMSProvider, {"region": "ALASKA"}, "bkr/mrms/mrms_ALASKA.icechunk"),
            (mrms.MRMSProvider, {"region": "CARIB"}, "bkr/mrms/mrms_CARIB.icechunk"),
            (mrms.MRMSProvider, {"region": "GUAM"}, "bkr/mrms/mrms_GUAM.icechunk"),
            (mrms.MRMSProvider, {"region": "HAWAII"}, "bkr/mrms/mrms_HAWAII.icechunk"),
            (mrms.MRMSPrecipRateProvider, {}, "bkr/mrms/mrms_preciprate.icechunk"),
            (
                mrms.MRMSProvider,
                {"store_prefix": "bkr/mrms/custom.icechunk"},
                "bkr/mrms/custom.icechunk",
            ),
        ],
        ids=["conus", "alaska", "carib", "guam", "hawaii", "preciprate", "explicit_prefix_wins"],
    )
    def test_store_prefix(self, cls, kwargs, expected):
        assert cls(**kwargs).store_prefix == expected

    def test_regional_rate_only_does_not_collide_with_conus(self):
        """The rate-only store name belongs to CONUS; a region must get its own."""
        regional = mrms.MRMSPrecipRateProvider(region="HAWAII")
        assert regional.store_prefix != "bkr/mrms/mrms_preciprate.icechunk"
        assert "HAWAII" in regional.store_prefix

    @pytest.mark.parametrize(
        "products",
        [("MultiSensor_QPE_01H_Pass2",), ("PrecipRate", "RadarOnly_QPE_01H")],
        ids=["hourly", "mixed"],
    )
    def test_slowest_product_sets_the_pace(self, products):
        provider = mrms.MRMSProvider(products=products)
        assert provider.freq == "1h"
        assert len(provider.partition_timestamps(pd.Timestamp("2026-01-01T00:00"))) == 1

    def test_partition_is_thirty_two_minute_steps(self):
        stamps = mrms.MRMSProvider().partition_timestamps(pd.Timestamp("2026-09-20T01:00"))
        assert len(stamps) == 30
        assert stamps[0] == pd.Timestamp("2026-09-20T01:00")
        assert stamps[-1] == pd.Timestamp("2026-09-20T01:58")

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"region": "ATLANTIS"}, "unknown MRMS region"),
            ({"products": ("Sunshine",)}, "unknown MRMS product"),
        ],
        ids=["region", "product"],
    )
    def test_unknown_values_rejected(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            mrms.MRMSProvider(**kwargs)

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
    @pytest.mark.parametrize("tz", [None, "UTC"], ids=["naive", "tz_aware"])
    def test_finds_aws_mirror_layout(self, tmp_path, tz):
        stamp = pd.Timestamp("2026-09-20T00:02")
        target = write_aws_grib(tmp_path, "HAWAII", "PrecipRate", stamp)
        provider = mrms.MRMSProvider(region="HAWAII", archive_dir=tmp_path)
        found = provider.local_file("PrecipRate", stamp.tz_localize(tz))
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

        expected = [
            str(write_aws_grib(tmp_path, "GUAM", product, stamp))
            for stamp in provider.partition_timestamps(it)
            for product in provider.products
        ]

        assert sorted(provider.fetch(it, temp_dir=tmp_path / "work")) == sorted(expected)

    def test_fetch_drops_timesteps_missing_a_product(self, tmp_path, monkeypatch):
        """An hour where one product never published must fetch nothing, not half of it."""
        it = pd.Timestamp("2026-09-20T01:00")
        provider = mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)
        monkeypatch.setattr(provider, "remote_url", lambda product, timestamp: None)
        for stamp in provider.partition_timestamps(it):
            write_aws_grib(tmp_path, "GUAM", "PrecipFlag", stamp)

        assert provider.fetch(it, temp_dir=tmp_path / "work") == []

    def test_remote_url_skips_regions_before_the_aws_archive(self):
        provider = mrms.MRMSProvider(region="HAWAII")
        assert provider.remote_url("PrecipRate", pd.Timestamp("2016-01-01T00:00")) is None

    @pytest.mark.parametrize("tz", [None, "UTC"], ids=["naive", "tz_aware"])
    def test_remote_url_uses_iastate_for_old_conus(self, tz):
        provider = mrms.MRMSProvider()
        url = provider.remote_url("PrecipFlag", pd.Timestamp("2016-01-22T19:46", tz=tz))
        assert url == (
            "https://mtarchive.geol.iastate.edu/2016/01/22/mrms/ncep/PrecipFlag/"
            "MRMS_PrecipFlag_00.00_20160122-194600.grib2.gz"
        )


class TestProcess:
    @pytest.fixture
    def provider(self, tmp_path, fake_loader):
        return mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path)

    def test_merges_products_and_concatenates(self, provider):
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

    def test_drops_timesteps_missing_a_product(self, provider):
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

    def test_no_complete_timesteps_raises(self, provider):
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




def make_partition(stamps) -> xr.Dataset:
    """A flag-and-rate dataset covering ``stamps``, as ``process`` would return it."""
    return xr.concat(
        [
            xr.merge(
                [
                    make_step("precipitation_flag", "int8", s),
                    make_step("precipitation_rate", "float16", s),
                ], compat="no_conflicts"
            )
            for s in stamps
        ],
        dim="time",
    )


FIRST_STEPS = pd.date_range("2026-09-20T01:00", periods=3, freq="2min")


class TestStoreRoundTrip:
    @pytest.fixture
    def provider(self, local_config, tmp_path):
        return mrms.MRMSProvider(region="GUAM", archive_dir=tmp_path, config=local_config)

    @pytest.fixture
    def repo(self, provider):
        return provider.get_icechunk_repo()

    def test_write_then_read_back(self, provider, repo):
        assert provider.write_to_icechunk(repo, make_partition(FIRST_STEPS)) is True

        ds = stored(repo)
        assert list(pd.DatetimeIndex(ds.time.values)) == list(FIRST_STEPS)
        assert set(ds.data_vars) == {"precipitation_flag", "precipitation_rate"}

    def test_rewriting_the_same_hour_is_a_no_op(self, provider, repo):
        provider.write_to_icechunk(repo, make_partition(FIRST_STEPS))

        assert provider.write_to_icechunk(repo, make_partition(FIRST_STEPS)) is False
        assert stored(repo).sizes["time"] == 3

    def test_partial_overlap_appends_only_the_new_steps(self, provider, repo):
        provider.write_to_icechunk(repo, make_partition(FIRST_STEPS))

        overlapping = pd.date_range("2026-09-20T01:04", periods=3, freq="2min")
        assert provider.write_to_icechunk(repo, make_partition(overlapping)) is True

        times = pd.DatetimeIndex(stored(repo).time.values)
        assert list(times) == list(pd.date_range("2026-09-20T01:00", periods=5, freq="2min"))
        assert times.is_unique

    def test_store_path_is_local_under_test_config(self, provider):
        assert not provider.store_path.startswith("s3://")
        assert provider.store_path.endswith("bkr/mrms/mrms_GUAM.icechunk")

    def test_fully_stored_partition_is_not_missing(self, provider, repo):
        it = pd.Timestamp("2026-09-20T01:00")
        provider.write_to_icechunk(repo, make_partition(provider.partition_timestamps(it)))
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == []

    def test_recent_partial_partition_is_retried(self, provider, repo):
        """An hour written while the archive was still filling in must not be sealed."""
        it = mrms.naive_utc(pd.Timestamp.now("UTC")).floor("h") - pd.Timedelta(2, "h")
        provider.write_to_icechunk(repo, make_partition(provider.partition_timestamps(it)[:5]))
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == [it]

    def test_settled_partial_partition_is_left_alone(self, provider, repo):
        """Old gaps are real gaps; re-downloading them on every pass is waste."""
        it = pd.Timestamp("2021-01-01T01:00")
        provider.write_to_icechunk(repo, make_partition(provider.partition_timestamps(it)[:5]))
        assert provider.missing_timesteps(pd.DatetimeIndex([it])) == []

    def test_empty_store_reports_everything_missing(self, provider):
        wanted = pd.DatetimeIndex(["2026-09-20T01:00", "2026-09-20T02:00"])
        assert provider.missing_timesteps(wanted) == list(wanted)


class TestTimezoneHandling:
    """Dagster hands partition starts over as tz-aware UTC; the archive is naive."""

    @pytest.mark.parametrize(
        "aware",
        [
            pd.Timestamp("2026-09-20T01:00", tz="UTC"),
            pd.Timestamp("2026-09-20T03:00", tz="Europe/Berlin"),
        ],
        ids=["utc", "other_offset"],
    )
    def test_naive_utc_normalises(self, aware):
        naive = mrms.naive_utc(aware)
        assert naive == pd.Timestamp("2026-09-20T01:00")
        assert naive.tzinfo is None

    def test_partition_timestamps_accept_aware_input(self):
        provider = mrms.MRMSProvider()
        stamps = provider.partition_timestamps(pd.Timestamp("2026-09-20T01:00", tz="UTC"))
        assert stamps.tz is None
        assert stamps[0] == pd.Timestamp("2026-09-20T01:00")

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
