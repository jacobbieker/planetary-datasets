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



@pytest.fixture
def repo(local_config):
    """An empty virtual-reference store under the test's data directory."""
    return virtual_repo.open_virtual_repo(
        "bkr/geo/test.icechunk", virtual_buckets=gk2a_ami_fd.BUCKET, config=local_config
    )


# =============================================================================
# Store naming
# =============================================================================
def test_published_store_prefixes():
    """Store names are persisted, so each discriminator rule is pinned here."""
    cases = [
        (
            virtual_repo.store_prefix("bkr/geo/gk2a", "ir087", "2026-01-01"),
            "bkr/geo/gk2a_ir087_2026-01-01.icechunk",
        ),
        # Empty discriminators are dropped.
        (virtual_repo.store_prefix("bkr/geo/gk2a", "ir087", None), "bkr/geo/gk2a_ir087.icechunk"),
        (virtual_repo.store_prefix("bkr/geo/gk2a", "", ""), "bkr/geo/gk2a.icechunk"),
        # The suffix is not doubled.
        (
            virtual_repo.store_prefix("bkr/geo/gk2a.icechunk", "ir087"),
            "bkr/geo/gk2a_ir087.icechunk",
        ),
        # The band is lowercased for GK-2A, and the store sits under
        # bkr/geo/virtualized with the other virtualized geostationary stores...
        (
            gk2a_ami_fd.store_prefix_for("IR087"),
            "bkr/geo/virtualized/gk2a_ami_fd_ir087.icechunk",
        ),
        # ...and the Himawari satellites get stores of their own, there too,
        # each named for its own spacecraft rather than sharing one base.
        (
            himawari_isatss.store_prefix_for("himawari8", "c13"),
            "bkr/geo/virtualized/himawari8_isatss_C13.icechunk",
        ),
        (
            himawari_isatss.store_prefix_for("himawari9", "c13"),
            "bkr/geo/virtualized/himawari9_isatss_C13.icechunk",
        ),
        # An explicit base is used as given, so a redirected run does not get
        # the satellite spliced into the middle of the name.
        (
            himawari_isatss.store_prefix_for("himawari9", "c13", "2025-12-31", "tmp/ahi"),
            "tmp/ahi_C13_2025-12-31.icechunk",
        ),
    ]
    for prefix, expected in cases:
        assert prefix == expected


@pytest.mark.parametrize("bucket", ["noaa-gk2a-pds", "s3://noaa-gk2a-pds", "s3://noaa-gk2a-pds/"])
def test_bucket_urls_are_normalised(bucket):
    assert virtual_repo._as_url_prefix(bucket) == "s3://noaa-gk2a-pds/"


# =============================================================================
# Virtual repositories
# =============================================================================
def test_open_virtual_repo_writes_to_the_local_store(repo, local_config):
    assert (local_config.icechunk_local_path / "bkr/geo/test.icechunk").is_dir()
    # A store with no commits yet describes as empty rather than raising.
    assert virtual_repo.describe_store(repo) in ({}, {"timesteps": 0})


def test_open_virtual_repo_rejects_an_empty_bucket_list(local_config):
    with pytest.raises(ValueError, match="at least one virtual source bucket"):
        virtual_repo.open_virtual_repo(
            "bkr/geo/test.icechunk", virtual_buckets=[], config=local_config
        )


def test_check_stores_counts_unreadable_stores_without_creating_them(local_config):
    prefix = "bkr/geo/never-written.icechunk"
    failed = virtual_repo.check_stores(
        [prefix], virtual_buckets=gk2a_ami_fd.BUCKET, config=local_config
    )
    assert failed == 1
    assert not (local_config.icechunk_local_path / prefix).exists()


# =============================================================================
# Append-order and commit guards
# =============================================================================
def _write_days(repo, days: list[str], group: str | None = None) -> None:
    """Commit one timestep per named day into an empty virtual-reference store."""
    import xarray as xr
    from icechunk.xarray import to_icechunk

    ds = xr.Dataset(
        {"image_pixel_values": (("t",), np.arange(len(days), dtype="int16"))},
        coords={"t": np.array(days, dtype="datetime64[ns]")},
    )
    session = repo.writable_session("main")
    to_icechunk(ds, session, group=group)
    session.commit("test data")


def test_day_coverage_reports_steps_and_the_newest_timestep(repo):
    assert virtual_repo.day_coverage(repo, dt.date(2026, 1, 1)) == (0, None)

    _write_days(repo, ["2026-01-01T00:00", "2026-01-01T00:10", "2026-01-02T00:00"])
    steps, newest = virtual_repo.day_coverage(repo, dt.date(2026, 1, 1))
    assert steps == 2
    assert newest == np.datetime64("2026-01-02T00:00", "ns")


def test_guard_append_order_reports_a_day_already_stored(repo):
    _write_days(repo, ["2026-01-02T00:00"])
    assert virtual_repo.guard_append_order(repo, dt.date(2026, 1, 2), "test") == 1
    # A newer day is still appendable.
    assert virtual_repo.guard_append_order(repo, dt.date(2026, 1, 3), "test") == 0


def test_guard_append_order_refuses_a_day_behind_the_store(repo):
    _write_days(repo, ["2026-01-05T00:00"])
    with pytest.raises(virtual_repo.OutOfOrderPartition, match="2026-01-04"):
        virtual_repo.guard_append_order(repo, dt.date(2026, 1, 4), "test")


def test_require_committed_raises_when_the_day_is_absent(repo):
    _write_days(repo, ["2026-01-05T00:00"])
    assert virtual_repo.require_committed(repo, dt.date(2026, 1, 5), "test") == 1
    with pytest.raises(virtual_repo.NothingCommitted, match="2026-01-06"):
        virtual_repo.require_committed(repo, dt.date(2026, 1, 6), "test")


def test_the_guards_read_the_subgroup_the_ingest_writes_to(repo):
    """Regression: the ingest wrote into a subgroup while the guards read the root.

    A successful ingest therefore raised NothingCommitted, and a re-run found nothing to skip.
    """
    group = f"{gk2a_ami_fd.PRODUCT_LABEL}/vi006"
    _write_days(repo, ["2026-01-05T00:00"], group=group)

    # Reading the root reports the day as absent, which is what used to happen.
    assert virtual_repo.day_coverage(repo, dt.date(2026, 1, 5)) == (0, None)

    assert virtual_repo.require_committed(repo, dt.date(2026, 1, 5), "t", group=group) == 1
    assert virtual_repo.guard_append_order(repo, dt.date(2026, 1, 5), "t", group=group) == 1


def test_gk2a_ingest_day_uses_one_group_for_the_write_and_the_guards(monkeypatch):
    """The group resolved for the write must be the one the guards are handed."""
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        virtual_repo, "guard_append_order",
        lambda repo, date, what, **kw: seen.__setitem__("guard", kw.get("group")) or 0,
    )
    monkeypatch.setattr(
        gk2a_ami_fd, "list_day_files", lambda *a, **k: ["s3://bucket/one.nc"]
    )
    monkeypatch.setattr(gk2a_ami_fd, "_store", lambda: None)
    monkeypatch.setattr(
        gk2a_ami_fd, "ingest_all_days",
        lambda repo, band, **kw: seen.__setitem__("write", kw.get("group")),
    )
    monkeypatch.setattr(
        virtual_repo, "require_committed",
        lambda repo, date, what, **kw: seen.__setitem__("committed", kw.get("group")) or 1,
    )

    gk2a_ami_fd.ingest_day(dt.date(2026, 1, 5), "vi006", repo=object())

    expected = f"{gk2a_ami_fd.PRODUCT_LABEL}/{gk2a_ami_fd._band_label('vi006')}"
    assert seen["guard"] == expected
    assert seen["write"] == expected
    assert seen["committed"] == expected


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


def _isatss_url(slot: str, tile: int) -> str:
    return f"s3://bucket/OR_HFD-020-B12-M1C13-T{tile:03d}_GH9_s{slot}_e0_c0.nc"


def test_himawari_slots_are_grouped_in_time_order():
    urls = [_isatss_url("20260010010000", t) for t in (3, 1, 2)]
    urls += [_isatss_url("20260010000000", t) for t in (1, 2)]

    slots = himawari_isatss.group_by_slot(urls)

    assert [len(s) for s in slots] == [2, 3]
    assert "s20260010000000" in slots[0][0]


def test_a_redelivered_himawari_tile_does_not_cost_the_whole_scene():
    """Regression: the lister did not dedupe the way the GOES and GK-2A ones do.

    A tile delivered twice reached the mosaic as an 89th entry claiming an occupied grid
    cell, and the whole scene was discarded over a duplicate carrying the same data.
    """
    urls = [_isatss_url("20260010000000", t) for t in (1, 2, 3)]
    urls.append(_isatss_url("20260010000000", 2))

    (slot,) = himawari_isatss.group_by_slot(urls)

    assert len(slot) == 3
    assert len(set(slot)) == 3


TWO_SCENES = [_isatss_url(slot, 1) for slot in ("20260010000000", "20260010010000")]


def test_a_batch_with_a_failed_scene_is_refused(monkeypatch):
    """Regression: the failed scene was logged and dropped and the day committed as complete.

    The store only appends along `t`, so the gap could never be filled.
    """
    def stitch(slot_urls, **kwargs):
        if "s20260010010000" in slot_urls[0]:
            raise ValueError("Expected 88 tiles for a full scene, got 40")
        return object()

    monkeypatch.setattr(himawari_isatss, "stitch_slot", stitch)

    with pytest.raises(himawari_isatss.IncompleteBatch, match="1 of 2 scene"):
        himawari_isatss.build_batch(TWO_SCENES, registry=None, parser=object())


def test_a_batch_with_a_failed_scene_can_be_forced(monkeypatch):
    """For a day the archive genuinely never published in full."""
    concatenated = []

    def stitch(slot_urls, **kwargs):
        if "s20260010010000" in slot_urls[0]:
            raise ValueError("never published")
        return "scene"

    monkeypatch.setattr(himawari_isatss, "stitch_slot", stitch)
    monkeypatch.setattr(
        himawari_isatss.xr, "concat", lambda scenes, **kw: concatenated.append(scenes) or "out"
    )

    assert (
        himawari_isatss.build_batch(
            TWO_SCENES, registry=None, parser=object(), allow_missing_scenes=True
        )
        == "out"
    )
    assert concatenated == [["scene"]]


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


def test_mtg_without_credentials_fails_loudly(local_config):
    with pytest.raises(MissingCredential) as exc:
        mtg.open_datastore(config=local_config)
    assert "EUMETSAT_CONSUMER_KEY" in str(exc.value)
    assert "EUMETSAT_CONSUMER_SECRET" in str(exc.value)


def test_mtg_archive_dir_is_under_the_configured_data_dir(local_config):
    path = mtg.archive_dir("fdhi", dt.datetime(2026, 1, 2, 3), config=local_config)
    assert path == local_config.data_dir / mtg.DATA_SUBDIR / "2026010203" / "FDHI"


# =============================================================================
# Hugging Face publishing
# =============================================================================
@pytest.fixture
def store_folder(tmp_path):
    folder = tmp_path / "store.zarr"
    folder.mkdir()
    return folder


def test_hub_upload_needs_a_repo_id(store_folder, local_config):
    with pytest.raises(ValueError, match="No Hugging Face repo id"):
        hub.upload_folder(store_folder, config=local_config)


def test_hub_upload_needs_a_token(store_folder, local_config):
    with pytest.raises(MissingCredential, match="HF_TOKEN"):
        hub.upload_folder(store_folder, repo_id="someone/some-dataset", config=local_config)


def test_hub_upload_needs_the_folder_to_exist(tmp_path, local_config):
    with pytest.raises(FileNotFoundError):
        hub.upload_folder(tmp_path / "absent", repo_id="someone/some-dataset", config=local_config)


def test_hub_upload_dry_run_uploads_nothing(store_folder, tmp_path, monkeypatch):
    monkeypatch.setenv("HF_TOKEN", "not-a-real-token")
    monkeypatch.setenv("HF_REPO_ID", "someone/some-dataset")
    cfg = load_config(env_file=tmp_path / "absent.env")
    assert hub.upload_folder(store_folder, config=cfg, dry_run=True) == "someone/some-dataset"


# =============================================================================
# Dagster definitions
# =============================================================================
def test_definitions_build_from_the_assets():
    import dagster as dg
    from dagster_docker import PipesDockerClient

    from dags.assets import geo_satellites

    dg.Definitions(
        assets=geo_satellites.ASSETS,
        resources={"pipes_docker_client": PipesDockerClient()},
    )
    names = {asset.key.to_user_string() for asset in geo_satellites.ASSETS}
    assert names == {
        "gk2a_ami_fd_virtual",
        "himawari8_isatss_virtual",
        "himawari9_isatss_virtual",
        "mtg_fdhi_download",
        "mtg_fdlr_download",
        "mtg_hub_upload",
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


def test_himawari8_partitions_cover_the_overlap_with_himawari9():
    """Himawari-8 stayed operational for a fortnight after Himawari-9 started."""
    from dags.assets import geo_satellites

    keys = set(geo_satellites.himawari8_dates.get_partition_keys())
    assert "2022-12-01" in keys
    assert "2022-12-13" in keys
    assert "2022-12-14" not in keys
