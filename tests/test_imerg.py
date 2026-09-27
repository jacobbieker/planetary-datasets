"""Offline tests for the IMERG provider. Nothing here touches the network."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import requests
import xarray as xr

from planetary_datasets.config import MissingCredential
from planetary_datasets.providers.imerg import (
    DEFAULT_VARIABLES,
    GRANULES_PER_DAY,
    PRODUCTS,
    EarthdataSession,
    IMERGProvider,
    IncompleteDay,
    day_range,
    get_product,
)

LAT = np.arange(-89.75, 90.0, 5.0, dtype="float32")
LON = np.arange(-179.75, 180.0, 5.0, dtype="float32")


def write_granule(path: pathlib.Path, time: pd.Timestamp, variables=DEFAULT_VARIABLES) -> pathlib.Path:
    """Write a miniature stand-in for an IMERG HDF5 granule."""
    ds = xr.Dataset(
        {
            v: (("time", "lat", "lon"), np.full((1, LAT.size, LON.size), 1.0, dtype="float32"))
            for v in variables
        },
        coords={"time": [time], "lat": LAT, "lon": LON},
    )
    ds["lat_bnds"] = (("lat", "latv"), np.stack([LAT - 2.5, LAT + 2.5], -1))
    ds["lon_bnds"] = (("lon", "lonv"), np.stack([LON - 2.5, LON + 2.5], -1))
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path, group="/Grid", engine="h5netcdf")
    return path


@pytest.fixture(autouse=True)
def isolated_scratch(tmp_path, monkeypatch):
    """Keep granule staging inside the test's tmp_path.

    Autouse, and declared before ``local_config`` is built, so the config the provider
    sees already points at the isolated directory rather than the shared ``/tmp``.
    """
    monkeypatch.setenv("PLANETARY_DATASETS_SCRATCH_DIR", str(tmp_path / "scratch"))


@pytest.fixture
def day_of_granules(tmp_path):
    """Four half-hourly granules for 2024-01-01, written out of order."""
    day = pd.Timestamp("2024-01-01")
    paths = []
    for i in (2, 0, 3, 1):
        t = day + pd.Timedelta(minutes=30 * i)
        name = f"3B-HHR.MS.MRG.3IMERG.20240101-S{i:02d}0000-E000000.{i * 30:04d}.V07B.HDF5"
        paths.append(str(write_granule(tmp_path / "granules" / name, t)))
    return day, paths


class TestProducts:
    def test_three_runs_are_registered(self):
        assert set(PRODUCTS) == {"early", "late", "final"}

    def test_each_run_has_its_own_store(self):
        prefixes = {p.store_prefix for p in PRODUCTS.values()}
        assert len(prefixes) == len(PRODUCTS)

    @pytest.mark.parametrize(
        ("code", "collection"),
        [("early", "GPM_3IMERGHHE.07"), ("late", "GPM_3IMERGHHL.07"), ("final", "GPM_3IMERGHH.07")],
    )
    def test_collections(self, code, collection):
        assert PRODUCTS[code].collection == collection

    def test_lookup_is_case_insensitive(self):
        assert get_product("FINAL") is PRODUCTS["final"]

    def test_product_objects_pass_through(self):
        assert get_product(PRODUCTS["early"]) is PRODUCTS["early"]

    def test_unknown_product_is_rejected(self):
        with pytest.raises(ValueError, match="unknown IMERG product"):
            get_product("nowcast")


class TestProviderWiring:
    def test_store_prefix_follows_the_product(self, local_config):
        assert IMERGProvider("early", config=local_config).store_prefix.endswith("imerg_early.icechunk")
        assert IMERGProvider("final", config=local_config).store_prefix.endswith("imerg_final.icechunk")

    def test_store_path_is_local_under_test_config(self, local_config, tmp_path):
        path = IMERGProvider("late", config=local_config).store_path
        assert path.startswith(str(tmp_path))

    def test_no_absolute_paths_are_hardcoded(self, local_config, tmp_path, monkeypatch):
        monkeypatch.setenv("PLANETARY_DATASETS_SCRATCH_DIR", str(tmp_path / "scratch"))
        from planetary_datasets import config as config_module

        cfg = config_module.load_config(env_file=tmp_path / "nonexistent.env")
        provider = IMERGProvider("final", config=cfg)
        assert str(provider.staging_dir).startswith(str(tmp_path / "scratch"))

    def test_listing_url_uses_day_of_year(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        url = provider.listing_url(pd.Timestamp("2024-03-01"))
        assert url.endswith("/GPM_3IMERGHH.07/2024/061/")

    def test_listing_url_pads_day_of_year(self, local_config):
        provider = IMERGProvider("early", config=local_config)
        assert provider.listing_url(pd.Timestamp("2021-01-05")).endswith("/2021/005/")

    def test_staging_dir_is_per_day(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        assert provider.staging_dir_for(pd.Timestamp("2024-01-01T12:00")).name == "20240101"


class TestCredentials:
    def test_session_requires_earthdata_credentials(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        with pytest.raises(MissingCredential, match="EARTHDATA_USERNAME"):
            provider.session()

    def test_listing_requires_credentials(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        with pytest.raises(MissingCredential):
            provider.list_remote_granules(pd.Timestamp("2024-01-01"))

    def test_session_is_reused(self, local_config, monkeypatch):
        monkeypatch.setenv("EARTHDATA_USERNAME", "user")
        monkeypatch.setenv("EARTHDATA_PASSWORD", "pass")
        from planetary_datasets import config as config_module

        cfg = config_module.load_config(env_file=pathlib.Path("/nonexistent.env"))
        provider = IMERGProvider("final", config=cfg)
        assert provider.session() is provider.session()
        assert provider.session().auth == ("user", "pass")


class TestEarthdataSession:
    """The redirect to urs.earthdata.nasa.gov must keep the header, nothing else may."""

    @staticmethod
    def _rebuild(from_url: str, to_url: str) -> bool:
        session = EarthdataSession()
        prepared = requests.Request("GET", to_url).prepare()
        prepared.headers["Authorization"] = "Basic secret"
        response = requests.Response()
        response.request = requests.Request("GET", from_url).prepare()
        session.rebuild_auth(prepared, response)
        return "Authorization" in prepared.headers

    def test_header_kept_across_nasa_hosts(self):
        assert self._rebuild(
            "https://gpm1.gesdisc.eosdis.nasa.gov/data/x.HDF5",
            "https://urs.earthdata.nasa.gov/oauth/authorize",
        )

    def test_header_kept_on_same_host(self):
        assert self._rebuild("https://gpm1.gesdisc.eosdis.nasa.gov/a", "https://gpm1.gesdisc.eosdis.nasa.gov/b")

    def test_header_stripped_for_third_parties(self):
        assert not self._rebuild(
            "https://gpm1.gesdisc.eosdis.nasa.gov/data/x.HDF5",
            "https://evil.example.com/collect",
        )

    def test_header_stripped_on_https_downgrade(self):
        assert not self._rebuild(
            "https://gpm1.gesdisc.eosdis.nasa.gov/data/x.HDF5",
            "http://urs.earthdata.nasa.gov/oauth/authorize",
        )

    def test_absent_header_is_not_invented(self):
        session = EarthdataSession()
        prepared = requests.Request("GET", "https://urs.earthdata.nasa.gov/").prepare()
        response = requests.Response()
        response.request = requests.Request("GET", "https://example.com/").prepare()
        session.rebuild_auth(prepared, response)
        assert "Authorization" not in prepared.headers


class FakeResponse:
    def __init__(self, text: str = "", status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


class TestListing:
    LISTING = """
    <html><body>
    <a href="?C=N;O=D">Name</a>
    <a href="/data/GPM_L3/">Parent Directory</a>
    <a href="3B-HHR.MS.MRG.3IMERG.20240101-S003000-E005959.0030.V07B.HDF5">b</a>
    <a href="3B-HHR.MS.MRG.3IMERG.20240101-S000000-E002959.0000.V07B.HDF5">a</a>
    <a href="3B-HHR.MS.MRG.3IMERG.20240101-S000000-E002959.0000.V07B.HDF5.xml">xml</a>
    </body></html>
    """

    def _provider(self, local_config, response):
        provider = IMERGProvider("final", config=local_config)
        provider._session = type("S", (), {"get": lambda self, url, **kw: response})()
        return provider

    def test_only_hdf5_links_are_returned(self, local_config):
        granules = self._provider(local_config, FakeResponse(self.LISTING)).list_remote_granules(
            pd.Timestamp("2024-01-01")
        )
        assert len(granules) == 2
        assert all(g.endswith(".HDF5") for g in granules)

    def test_granules_are_absolute_and_sorted(self, local_config):
        granules = self._provider(local_config, FakeResponse(self.LISTING)).list_remote_granules(
            pd.Timestamp("2024-01-01")
        )
        assert granules == sorted(granules)
        assert granules[0].startswith("https://gpm1.gesdisc.eosdis.nasa.gov/")
        assert "S000000" in granules[0]

    def test_absolute_hrefs_are_not_appended_to_the_directory(self, local_config):
        listing = '<a href="/data/GPM_L3/GPM_3IMERGHH.07/2024/001/granule.HDF5">g</a>'
        granules = self._provider(local_config, FakeResponse(listing)).list_remote_granules(
            pd.Timestamp("2024-01-01")
        )
        assert granules == [
            "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/2024/001/granule.HDF5"
        ]

    def test_missing_day_returns_empty(self, local_config):
        granules = self._provider(local_config, FakeResponse("not found", 404)).list_remote_granules(
            pd.Timestamp("1999-01-01")
        )
        assert granules == []

    def test_server_error_raises(self, local_config):
        with pytest.raises(requests.HTTPError):
            self._provider(local_config, FakeResponse("boom", 503)).list_remote_granules(
                pd.Timestamp("2024-01-01")
            )


class TestProcess:
    def test_granule_is_tidied(self, local_config, day_of_granules):
        _, paths = day_of_granules
        data = IMERGProvider("final", config=local_config).open_granule(paths[0])
        assert "latitude" in data.coords and "longitude" in data.coords
        assert not {"lat", "lon", "latv", "lonv"} & set(data.dims)
        assert set(data.data_vars) == set(DEFAULT_VARIABLES)

    def test_data_is_float16_but_coords_are_not(self, local_config, day_of_granules):
        _, paths = day_of_granules
        data = IMERGProvider("final", config=local_config).open_granule(paths[0])
        assert all(data[v].dtype == np.dtype("float16") for v in data.data_vars)
        assert data.latitude.dtype != np.dtype("float16")
        # The 0.1 degree grid does not survive float16; guard against a regression.
        assert np.allclose(data.latitude.values, LAT)

    def test_missing_variable_is_an_error(self, local_config, tmp_path):
        path = write_granule(
            tmp_path / "short.HDF5", pd.Timestamp("2024-01-01"), variables=("precipitation",)
        )
        with pytest.raises(ValueError, match="missing"):
            IMERGProvider("final", config=local_config).open_granule(path)

    def test_variable_subset_is_honoured(self, local_config, day_of_granules):
        _, paths = day_of_granules
        provider = IMERGProvider("final", config=local_config, variables=["precipitation"])
        assert list(provider.open_granule(paths[0]).data_vars) == ["precipitation"]

    def test_day_is_concatenated_in_time_order(self, local_config, day_of_granules):
        day, paths = day_of_granules
        data = IMERGProvider("final", config=local_config).process(paths, day)
        assert data.sizes["time"] == 4
        assert list(data.time.values) == sorted(data.time.values)
        assert data.time.values[0] == np.datetime64("2024-01-01T00:00")

    def test_process_records_provenance(self, local_config, day_of_granules):
        day, paths = day_of_granules
        data = IMERGProvider("late", config=local_config).process(paths, day)
        assert data.attrs["imerg_product"] == "late"
        assert data.attrs["imerg_collection"] == "GPM_3IMERGHHL.07"

    def test_empty_input_is_an_error(self, local_config):
        with pytest.raises(ValueError, match="no input files"):
            IMERGProvider("final", config=local_config).process([], pd.Timestamp("2024-01-01"))


def stage_day(provider, day, count=3):
    """Stage ``count`` granules for ``day`` and return their paths."""
    staged = provider.staging_dir_for(day)
    return [
        write_granule(
            staged / f"3B-HHR.MS.MRG.3IMERG.20240101-S{i:02d}0000.{i:04d}.V07B.HDF5",
            day + pd.Timedelta(minutes=30 * i),
        )
        for i in range(count)
    ]


class TestStoreRoundTrip:
    @pytest.fixture(autouse=True)
    def no_network(self, monkeypatch):
        """Stub ``fetch`` out: the archive has nothing beyond what is already staged."""
        calls: list = []

        def fetch(self, it, temp_dir=None, **kwargs):
            calls.append(pd.Timestamp(it))
            return []

        monkeypatch.setattr(IMERGProvider, "fetch", fetch)
        return calls

    def test_write_staged_appends_and_cleans_up(self, local_config, no_network):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=GRANULES_PER_DAY)

        assert provider.write_staged(day) is True
        assert no_network == [], "a complete day must not be re-listed"
        assert provider.staged_files(day) == []

        repo = provider.get_icechunk_repo()
        stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        assert stored.sizes["time"] == GRANULES_PER_DAY
        assert np.datetime64("2024-01-01T00:00") in stored.time.values

    def test_a_short_day_is_refused(self, local_config):
        """A partial write would mark the day done and leave a permanent gap."""
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=3)

        with pytest.raises(IncompleteDay, match="3/48"):
            provider.write_staged(day)
        assert provider.missing_timesteps(pd.DatetimeIndex([day])) == [day]
        # The inputs survive so the retry only has to fetch what is missing.
        assert len(provider.staged_files(day)) == 3

    def test_a_short_day_can_be_forced(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=3)
        assert provider.write_staged(day, allow_partial=True) is True

    def test_second_run_is_a_no_op(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=1)
        assert provider.write_staged(day, allow_partial=True) is True
        assert provider.missing_timesteps(pd.DatetimeIndex([day])) == []

        stage_day(provider, day, count=1)
        assert provider.write_staged(day, allow_partial=True) is False
        # Nothing was written, so the re-staged file is left where it is.
        assert provider.staged_files(day) != []

    def test_staged_files_survive_a_rejected_write(self, local_config, monkeypatch):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=GRANULES_PER_DAY)
        monkeypatch.setattr(IMERGProvider, "write_to_icechunk", lambda self, repo, ds: False)

        assert provider.write_staged(day) is False
        assert len(provider.staged_files(day)) == GRANULES_PER_DAY

    def test_run_partition_uses_the_staging_path(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=GRANULES_PER_DAY)
        assert provider.run_partition(day) is True
        assert provider.staged_files(day) == []

    def test_publish_reports_a_local_store_as_unpublished(self, local_config):
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=1)
        provider.write_staged(day, allow_partial=True)

        info = provider.publish()
        assert info["published"] is False
        assert info["timesteps"] == 1
        assert info["store_path"] == provider.store_path

    def test_publish_on_an_empty_store(self, local_config):
        info = IMERGProvider("early", config=local_config).publish()
        assert info["timesteps"] == 0
        assert info["first_time"] is None

    def test_nothing_staged_and_nothing_remote_is_skipped(self, local_config, monkeypatch):
        provider = IMERGProvider("final", config=local_config)
        monkeypatch.setattr(IMERGProvider, "fetch", lambda self, *a, **k: [])
        assert provider.write_staged(pd.Timestamp("2024-01-01")) is False

    def test_gaps_are_refilled_before_writing(self, local_config, monkeypatch):
        """A day left short by a timed-out download run is topped up, not written short."""
        provider = IMERGProvider("final", config=local_config)
        day = pd.Timestamp("2024-01-01")
        stage_day(provider, day, count=10)

        def fake_fetch(self, it, temp_dir=None, **kwargs):
            stage_day(self, pd.Timestamp(it).normalize(), count=GRANULES_PER_DAY)
            return self.staged_files(it)

        monkeypatch.setattr(IMERGProvider, "fetch", fake_fetch)
        assert provider.write_staged(day) is True


class TestDownload:
    def test_existing_file_is_not_redownloaded(self, local_config, tmp_path):
        provider = IMERGProvider("final", config=local_config)
        dest = tmp_path / "granule.HDF5"
        dest.write_bytes(b"already here")

        def explode(self):
            raise AssertionError("session should not be needed")

        provider.session = explode.__get__(provider)
        assert provider.download_granule("https://example.invalid/x.HDF5", dest) == dest
        assert dest.read_bytes() == b"already here"

    def test_empty_file_is_redownloaded(self, local_config, tmp_path, monkeypatch):
        provider = IMERGProvider("final", config=local_config)
        dest = tmp_path / "granule.HDF5"
        dest.touch()
        calls = []
        monkeypatch.setattr(
            IMERGProvider, "session", lambda self: calls.append(1) or pytest.fail("stop")
        )
        with pytest.raises(BaseException):
            provider.download_granule("https://example.invalid/x.HDF5", dest, retries=1)
        assert calls


class TestHelpers:
    def test_day_range_is_inclusive_and_daily(self):
        days = day_range("2024-01-01", "2024-01-04")
        assert len(days) == 4
        assert days[-1] == pd.Timestamp("2024-01-04")

    def test_day_range_normalises(self):
        assert day_range("2024-01-01T13:00", "2024-01-01T23:00")[0] == pd.Timestamp("2024-01-01")
