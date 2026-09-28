"""Offline tests for the NASA GEOS-CF provider.

Nothing here touches the network: ``fetch`` is exercised by pointing it at a synthetic
NetCDF4 file written to a temporary directory, and every store write goes to a local
Icechunk repository under ``tmp_path``.
"""

from __future__ import annotations

import importlib
import os

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import geos as geos_module
from planetary_datasets.providers.geos import (
    STORE_PREFIXES,
    GEOSProvider,
    build_urls,
    preprocess_geos,
)

STAMP = pd.Timestamp("2026-01-01T00:15")
RAW_DIMS = ("time", "lev", "lat", "lon")
RAW_SHAPE = (1, 1, 3, 4)


def _raw_geos_dataset(it: pd.Timestamp = STAMP) -> xr.Dataset:
    """A miniature stand-in for a GEOS-CF file: lon/lat names and a length-one ``lev``."""
    return xr.Dataset(
        {
            "SLP": (RAW_DIMS, np.full(RAW_SHAPE, 101325.0, dtype="float32")),
            "T2M": (RAW_DIMS, np.full(RAW_SHAPE, 288.0, dtype="float32")),
        },
        coords={
            "time": pd.DatetimeIndex([it]),
            "lev": [1.0],
            "lat": np.linspace(-10, 10, 3),
            "lon": np.linspace(0, 30, 4),
        },
    )


@pytest.fixture(autouse=True)
def no_checksum_env(monkeypatch):
    """Opening a store writes this variable, so keep it from leaking between tests."""
    monkeypatch.delenv("AWS_REQUEST_CHECKSUM_CALCULATION", raising=False)


@pytest.fixture
def geos_file(tmp_path):
    """Write a synthetic GEOS-CF file and return its path."""
    path = tmp_path / "GEOS.cf.ana.20260101_0015z.R0.nc4"
    _raw_geos_dataset().to_netcdf(path)
    return path


# --- URL construction -------------------------------------------------------------------


def test_v1_url_matches_the_published_pattern():
    """The v1 collection publishes exactly one file per instant."""
    (url,) = build_urls(STAMP, version=1)
    assert url == (
        "https://portal.nccs.nasa.gov/datashare/gmao/geos-cf/v1/ana/Y2026/M01/D01/"
        "GEOS-CF.v01.rpl.htf_inst_15mn_g1440x721_x1.20260101_0015z.nc4"
    )


def test_v2_offers_every_revision_with_r0_first():
    """v2 revisions are offered in the order R0, R1, unsuffixed."""
    urls = build_urls(STAMP, version=2)
    assert [u.rsplit("z", 1)[-1] for u in urls] == [".R0.nc4", ".R1.nc4", ".nc4"]
    assert all("/v2/ana/Y2026/M01/D01/" in u for u in urls)


def test_unknown_version_is_rejected():
    """Only GEOS-CF 1 and 2 exist, so anything else fails immediately."""
    with pytest.raises(ValueError):
        build_urls(STAMP, version=3)
    with pytest.raises(ValueError):
        GEOSProvider(version=99)


# --- preprocessing ----------------------------------------------------------------------


def test_preprocess_renames_coords_and_drops_the_level_dim():
    """lon/lat become longitude/latitude and the degenerate lev axis disappears."""
    ds = preprocess_geos(_raw_geos_dataset())
    assert set(ds.dims) == {"time", "latitude", "longitude"}
    assert "lev" not in ds.variables


def test_preprocess_is_idempotent_on_already_renamed_data():
    """Preprocessing an already-preprocessed dataset is harmless."""
    ds = preprocess_geos(preprocess_geos(_raw_geos_dataset()))
    assert set(ds.dims) == {"time", "latitude", "longitude"}


def test_float16_downcast_spares_sea_level_pressure():
    """SLP needs the float32 range; the other variables do not."""
    ds = preprocess_geos(_raw_geos_dataset(), to_float16=True)
    assert ds["T2M"].dtype == np.float16
    assert ds["SLP"].dtype == np.float32


def test_float16_downcast_is_off_by_default():
    """Downcasting is opt-in, so callers cannot lose precision by accident."""
    ds = preprocess_geos(_raw_geos_dataset())
    assert ds["T2M"].dtype == np.float32


# --- provider configuration -------------------------------------------------------------


def test_each_version_targets_its_own_store(local_config):
    """v1 and v2 have incompatible variable sets and must never share a store."""
    v1 = GEOSProvider(version=1, config=local_config)
    v2 = GEOSProvider(version=2, config=local_config)
    assert v1.store_prefix == STORE_PREFIXES[1]
    assert v2.store_prefix == STORE_PREFIXES[2]
    assert v1.store_path != v2.store_path
    assert "s3://" not in v1.store_path


def test_v1_downcasts_and_v2_does_not():
    """The precision defaults follow how each store was originally created."""
    assert GEOSProvider(version=1).to_float16 is True
    assert GEOSProvider(version=2).to_float16 is False
    assert GEOSProvider(version=2, to_float16=True).to_float16 is True


def test_day_timestamps_covers_the_whole_day_at_quarter_hours():
    """A day is 96 instants, from midnight inclusive to the next midnight exclusive."""
    stamps = GEOSProvider.day_timestamps(pd.Timestamp("2026-01-01T13:07"))
    assert len(stamps) == 96
    assert stamps[0] == pd.Timestamp("2026-01-01T00:00")
    assert stamps[-1] == pd.Timestamp("2026-01-01T23:45")


def test_checksum_env_is_not_set_at_import_time(monkeypatch):
    """Importing the module must not change process-wide S3 behaviour."""
    monkeypatch.delenv("AWS_REQUEST_CHECKSUM_CALCULATION", raising=False)
    importlib.reload(geos_module)
    assert "AWS_REQUEST_CHECKSUM_CALCULATION" not in os.environ


def test_opening_the_store_sets_the_checksum_env(local_config, monkeypatch):
    """The source.coop checksum workaround is applied when a store is opened."""
    monkeypatch.delenv("AWS_REQUEST_CHECKSUM_CALCULATION", raising=False)
    GEOSProvider(version=1, config=local_config).get_icechunk_repo()
    assert os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] == "WHEN_REQUIRED"


def test_checksum_env_overrides_an_inherited_value(local_config, monkeypatch):
    """An inherited WHEN_SUPPORTED would break every commit, so it must be overridden."""
    monkeypatch.setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_SUPPORTED")
    GEOSProvider(version=1, config=local_config).get_icechunk_repo()
    assert os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] == "WHEN_REQUIRED"


# --- fetch ------------------------------------------------------------------------------


def test_fetch_returns_the_first_revision_that_downloads(
    local_config, tmp_path, monkeypatch, geos_file
):
    """Later revisions are not requested once an earlier one succeeds."""
    tried: list[str] = []

    def fake_download(url, dest, **kwargs):
        tried.append(url)
        # Pretend only the .R1 revision was published for this instant.
        return geos_file if ".R1.nc4" in url else None

    monkeypatch.setattr(geos_module, "download_one", fake_download)
    files = GEOSProvider(version=2, config=local_config).fetch(STAMP, temp_dir=tmp_path)

    assert files == [str(geos_file)]
    assert len(tried) == 2


def test_fetch_probes_candidates_cheaply_before_spending_retries(
    local_config, tmp_path, monkeypatch, geos_file
):
    """The first pass over the v2 revisions uses one attempt each, not the retry budget."""
    budgets: list[int] = []

    def fake_download(url, dest, retries=3, **kwargs):
        budgets.append(retries)
        return geos_file if ".R1.nc4" in url else None

    monkeypatch.setattr(geos_module, "download_one", fake_download)
    GEOSProvider(version=2, retries=5, config=local_config).fetch(STAMP, temp_dir=tmp_path)

    assert budgets == [1, 1]


def test_fetch_uses_the_full_retry_budget_for_a_single_candidate(
    local_config, tmp_path, monkeypatch
):
    """v1 has nothing to probe, so its one URL gets the retries straight away."""
    budgets: list[int] = []

    def fake_download(url, dest, retries=3, **kwargs):
        budgets.append(retries)
        return None

    monkeypatch.setattr(geos_module, "download_one", fake_download)
    GEOSProvider(version=1, retries=5, config=local_config).fetch(STAMP, temp_dir=tmp_path)

    assert budgets == [5]


def test_fetch_retries_properly_when_every_candidate_fails_the_cheap_pass(
    local_config, tmp_path, monkeypatch
):
    """A blip that kills the probe pass still gets the full retry budget afterwards."""
    budgets: list[int] = []

    def fake_download(url, dest, retries=3, **kwargs):
        budgets.append(retries)
        return None

    monkeypatch.setattr(geos_module, "download_one", fake_download)
    GEOSProvider(version=2, retries=4, config=local_config).fetch(STAMP, temp_dir=tmp_path)

    assert budgets == [1, 1, 1, 4, 4, 4]


def test_fetch_returns_nothing_when_no_revision_exists(local_config, tmp_path, monkeypatch):
    """An instant the portal has not published is nothing to do, not a failure."""
    monkeypatch.setattr(geos_module, "download_one", lambda url, dest, **kwargs: None)
    assert GEOSProvider(version=2, config=local_config).fetch(STAMP, temp_dir=tmp_path) == []


# --- process and write ------------------------------------------------------------------


def test_process_shapes_the_file_for_the_store(local_config, geos_file):
    """Processing yields the store's dimensions and one chunk per timestep."""
    ds = GEOSProvider(version=2, config=local_config).process([str(geos_file)], STAMP)
    assert set(ds.dims) == {"time", "latitude", "longitude"}
    assert ds.chunksizes["time"] == (1,)
    assert pd.Timestamp(ds["time"].values[0]) == STAMP


def test_process_rejects_a_file_whose_time_is_not_the_partition(local_config, tmp_path):
    """A mismatched timestamp must not be written under the partition's key."""
    path = tmp_path / "wrong.nc4"
    _raw_geos_dataset(pd.Timestamp("2026-01-01T12:00")).to_netcdf(path)
    with pytest.raises(ValueError, match="reports time"):
        GEOSProvider(version=2, config=local_config).process([str(path)], STAMP)


def test_run_partition_writes_then_skips(local_config, monkeypatch, geos_file):
    """The first run writes the instant; the second finds it already stored."""
    monkeypatch.setattr(geos_module, "download_one", lambda url, dest, **kwargs: geos_file)
    provider = GEOSProvider(version=2, config=local_config)

    assert provider.run_partition(STAMP) is True
    assert provider.run_partition(STAMP) is False

    session = provider.get_icechunk_repo().readonly_session("main")
    stored = xr.open_zarr(session.store, consolidated=False)
    assert pd.Timestamp(stored["time"].values[0]) == STAMP
    assert set(stored.data_vars) == {"SLP", "T2M"}


def test_run_partition_skips_when_nothing_is_published(local_config, monkeypatch):
    """An unavailable instant returns False without writing anything."""
    monkeypatch.setattr(geos_module, "download_one", lambda url, dest, **kwargs: None)
    assert GEOSProvider(version=2, config=local_config).run_partition(STAMP) is False


# --- day-level results ------------------------------------------------------------------


def test_run_day_reports_a_wholly_unpublished_day_as_neither_written_nor_failed(
    local_config, monkeypatch
):
    """A day the portal has no data for must not look like an error."""
    monkeypatch.setattr(geos_module, "download_one", lambda url, dest, **kwargs: None)
    result = GEOSProvider(version=2, config=local_config).run_day(pd.Timestamp("2026-01-01"))
    assert (result.attempted, result.written, result.failed) == (96, 0, 0)
    assert result.unavailable == 96


def test_run_day_distinguishes_failures_from_missing_data(local_config, monkeypatch):
    """Exceptions are counted separately so a total outage is visible to the caller."""

    def boom(url, dest, **kwargs):
        raise OSError("the portal is down")

    monkeypatch.setattr(geos_module, "download_one", boom)
    result = GEOSProvider(version=2, config=local_config).run_day(pd.Timestamp("2026-01-01"))
    assert result.written == 0
    assert result.failed == 96
    assert result.unavailable == 0


def test_run_day_counts_the_instants_it_writes(local_config, monkeypatch, geos_file):
    """Only the one instant our synthetic file matches is written; the rest are absent."""
    def only_the_one_instant(url, dest, **kwargs):
        return geos_file if "0015z" in url else None

    monkeypatch.setattr(geos_module, "download_one", only_the_one_instant)
    result = GEOSProvider(version=2, config=local_config).run_day(STAMP)
    assert (result.attempted, result.written, result.failed) == (96, 1, 0)


def test_run_days_sums_across_days(local_config, monkeypatch):
    """Running several days aggregates into a single result."""
    monkeypatch.setattr(geos_module, "download_one", lambda url, dest, **kwargs: None)
    days = pd.date_range("2026-01-01", periods=2, freq="1D")
    result = GEOSProvider(version=2, config=local_config).run_days(days)
    assert result.attempted == 192
    assert result.written == 0
