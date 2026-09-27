"""Non-GOES geostationary providers: store naming, credentials and definitions.

Every test here is offline. Nothing touches S3, the EUMETSAT Data Store or the
Hugging Face Hub.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pytest

from planetary_datasets.common import hub
from planetary_datasets.config import MissingCredential, load_config
from planetary_datasets.providers.eumetsat import mtg
from planetary_datasets.providers.virtualized import (
    gk2a_ami_fd,
    himawari_isatss,
    virtual_repo,
)

EXTRA_CREDENTIAL_VARS = [
    "EUMETSAT_CONSUMER_KEY",
    "EUMETSAT_CONSUMER_SECRET",
    "HF_TOKEN",
    "HF_REPO_ID",
]


@pytest.fixture
def bare_config(tmp_path, monkeypatch):
    """A config with no credentials at all, writing stores under tmp_path."""
    for var in EXTRA_CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    return load_config(env_file=tmp_path / "absent.env")


# =============================================================================
# Store naming
# =============================================================================
def test_store_prefix_appends_discriminators():
    assert (
        virtual_repo.store_prefix("bkr/geo/gk2a", "ir087", "2026-01-01")
        == "bkr/geo/gk2a_ir087_2026-01-01.icechunk"
    )


def test_store_prefix_drops_empty_parts():
    assert virtual_repo.store_prefix("bkr/geo/gk2a", "ir087", None) == "bkr/geo/gk2a_ir087.icechunk"
    assert virtual_repo.store_prefix("bkr/geo/gk2a", "", "") == "bkr/geo/gk2a.icechunk"


def test_store_prefix_does_not_double_the_suffix():
    assert virtual_repo.store_prefix("bkr/geo/gk2a.icechunk", "ir087") == (
        "bkr/geo/gk2a_ir087.icechunk"
    )


def test_gk2a_store_prefix_lowercases_the_band():
    assert gk2a_ami_fd.store_prefix_for("IR087") == "bkr/geo/gk2a_ami_fd_ir087.icechunk"


def test_himawari_store_prefix_separates_the_satellites():
    h8 = himawari_isatss.store_prefix_for("himawari8", "c13")
    h9 = himawari_isatss.store_prefix_for("himawari9", "c13")
    assert h8 != h9
    assert h8.endswith("_himawari8_C13.icechunk")


@pytest.mark.parametrize("bucket", ["noaa-gk2a-pds", "s3://noaa-gk2a-pds", "s3://noaa-gk2a-pds/"])
def test_bucket_urls_are_normalised(bucket):
    assert virtual_repo._as_url_prefix(bucket) == "s3://noaa-gk2a-pds/"


# =============================================================================
# Virtual repositories
# =============================================================================
def test_open_virtual_repo_writes_to_the_local_store(bare_config):
    repo = virtual_repo.open_virtual_repo(
        "bkr/geo/test.icechunk",
        virtual_buckets=gk2a_ami_fd.BUCKET,
        config=bare_config,
    )
    assert (bare_config.icechunk_local_path / "bkr/geo/test.icechunk").is_dir()
    # A store with no commits yet describes as empty rather than raising.
    assert virtual_repo.describe_store(repo) in ({}, {"timesteps": 0})


def test_open_virtual_repo_rejects_an_empty_bucket_list(bare_config):
    with pytest.raises(ValueError, match="at least one virtual source bucket"):
        virtual_repo.open_virtual_repo(
            "bkr/geo/test.icechunk", virtual_buckets=[], config=bare_config
        )


def test_check_stores_counts_unreadable_stores(bare_config):
    failed = virtual_repo.check_stores(
        ["bkr/geo/never-written.icechunk"],
        virtual_buckets=gk2a_ami_fd.BUCKET,
        config=bare_config,
    )
    assert failed == 1


# =============================================================================
# Archive geometry
# =============================================================================
def test_gk2a_band_resolutions():
    assert gk2a_ami_fd.grid_size("vi006") == 22000
    assert gk2a_ami_fd.grid_size("VI004") == 11000
    assert gk2a_ami_fd.grid_size("ir087") == 5500


def test_gk2a_slot_is_parsed_from_the_filename():
    url = "s3://noaa-gk2a-pds/AMI/L1B/FD/202503/06/05/gk2a_ami_le1b_wv063_fd020ge_202503060510.nc"
    assert gk2a_ami_fd.parse_slot_to_datetime(url) == np.datetime64("2025-03-06T05:10", "ns")
    assert gk2a_ami_fd.band_from_filename(url) == "wv063"


def test_gk2a_rejects_a_malformed_slot_token():
    with pytest.raises(ValueError, match="Unexpected GK-2A slot token"):
        gk2a_ami_fd.parse_slot_to_datetime("gk2a_ami_le1b_wv063_fd020ge_nonsense.nc")


def test_himawari_tiles_divide_the_grid_exactly():
    for band in himawari_isatss.BANDS:
        assert himawari_isatss.tile_size(band) * himawari_isatss.TILE_GRID == (
            himawari_isatss.grid_size(band)
        )


# =============================================================================
# EUMETSAT MTG
# =============================================================================
def test_mtg_collection_ids():
    assert mtg.collection_id("fdhi") == "EO:EUM:DAT:0665"
    assert mtg.collection_id("FDLR") == "EO:EUM:DAT:0662"


def test_mtg_rejects_an_unknown_product():
    with pytest.raises(ValueError, match="Unknown MTG product"):
        mtg.collection_id("fdxx")


def test_mtg_keeps_only_netcdf_entries():
    entries = ["a_chunk.nc", "trailer.xml", "b_chunk.nc", "index.idx"]
    assert mtg.select_netcdf(entries) == ["a_chunk.nc", "b_chunk.nc"]


def test_mtg_without_credentials_fails_loudly(bare_config):
    with pytest.raises(MissingCredential) as exc:
        mtg.open_datastore(config=bare_config)
    assert "EUMETSAT_CONSUMER_KEY" in str(exc.value)
    assert "EUMETSAT_CONSUMER_SECRET" in str(exc.value)


def test_mtg_archive_dir_is_under_the_configured_data_dir(bare_config):
    path = mtg.archive_dir("fdhi", dt.datetime(2026, 1, 2, 3), config=bare_config)
    assert path == bare_config.data_dir / mtg.DATA_SUBDIR / "2026010203" / "FDHI"


# =============================================================================
# Hugging Face publishing
# =============================================================================
def test_hub_upload_needs_a_repo_id(tmp_path, bare_config):
    folder = tmp_path / "store.zarr"
    folder.mkdir()
    with pytest.raises(ValueError, match="No Hugging Face repo id"):
        hub.upload_folder(folder, config=bare_config)


def test_hub_upload_needs_a_token(tmp_path, bare_config):
    folder = tmp_path / "store.zarr"
    folder.mkdir()
    with pytest.raises(MissingCredential, match="HF_TOKEN"):
        hub.upload_folder(folder, repo_id="someone/some-dataset", config=bare_config)


def test_hub_upload_needs_the_folder_to_exist(tmp_path, bare_config):
    with pytest.raises(FileNotFoundError):
        hub.upload_folder(tmp_path / "absent", repo_id="someone/some-dataset", config=bare_config)


def test_hub_upload_dry_run_uploads_nothing(tmp_path, monkeypatch):
    for var in EXTRA_CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HF_TOKEN", "not-a-real-token")
    monkeypatch.setenv("HF_REPO_ID", "someone/some-dataset")
    cfg = load_config(env_file=tmp_path / "absent.env")

    folder = tmp_path / "store.zarr"
    folder.mkdir()
    assert hub.upload_folder(folder, config=cfg, dry_run=True) == "someone/some-dataset"


# =============================================================================
# Dagster definitions
# =============================================================================
def test_definitions_build_from_the_assets():
    import dagster as dg
    from dagster_docker import PipesDockerClient

    from dags.assets import geo_satellites

    defs = dg.Definitions(
        assets=geo_satellites.ASSETS,
        resources={"pipes_docker_client": PipesDockerClient()},
    )
    assert defs is not None
    names = {asset.key.to_user_string() for asset in geo_satellites.ASSETS}
    assert names == {
        "gk2a_ami_fd_virtual",
        "himawari8_isatss_virtual",
        "himawari9_isatss_virtual",
        "mtg_fdhi_download",
        "mtg_fdlr_download",
        "mtg_zarr_hub_upload",
        "eumetsat_iodc_lrv",
    }


def test_partitions_start_at_the_archive_starts():
    from dags.assets import geo_satellites

    assert geo_satellites.gk2a_dates.start.date() == gk2a_ami_fd.ARCHIVE_START_DATE
    assert (
        geo_satellites.himawari8_dates.start.date()
        == himawari_isatss.ARCHIVE_START_DATE["himawari8"]
    )
    assert set(geo_satellites.GK2A_BANDS.get_partition_keys()) == set(gk2a_ami_fd.BANDS)
    assert set(geo_satellites.AHI_BANDS.get_partition_keys()) == set(himawari_isatss.BANDS)
