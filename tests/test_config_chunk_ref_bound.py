"""Handles from ``Config.icechunk_repo`` bound icechunk's chunk-reference cache."""

from __future__ import annotations

import icechunk

from planetary_datasets import config as config_module

PREFIX = "bkr/test/bound.icechunk"


def persisted(local_config, prefix: str = PREFIX) -> icechunk.RepositoryConfig:
    """The configuration stored in the repository itself."""
    return icechunk.Repository.fetch_config(local_config.icechunk_storage(prefix))


def test_a_handle_gets_the_default_bound(local_config):
    """Both the creating and a later open get DEFAULT_CACHED_CHUNK_REFS."""
    repo = local_config.icechunk_repo(PREFIX)
    assert repo.config.caching.num_chunk_refs == config_module.DEFAULT_CACHED_CHUNK_REFS
    reopened = local_config.icechunk_repo(PREFIX)
    assert reopened.config.caching.num_chunk_refs == config_module.DEFAULT_CACHED_CHUNK_REFS


def test_the_bound_is_never_persisted(local_config):
    """Neither the default nor a caller's own caching is written to the repository."""
    own = icechunk.RepositoryConfig(
        caching=icechunk.CachingConfig(num_chunk_refs=500_000), inline_chunk_threshold_bytes=7
    )
    local_config.icechunk_repo(PREFIX, config=own)
    stored = persisted(local_config)
    assert stored is None or stored.caching is None
    assert stored is not None and stored.inline_chunk_threshold_bytes == 7, (
        "the rest of the caller's config is still persisted on create"
    )
    # A plain open afterwards sees icechunk's default, not a bound left behind.
    plain = icechunk.Repository.open(local_config.icechunk_storage(PREFIX))
    assert plain.config.caching is None or plain.config.caching.num_chunk_refs is None


def test_a_callers_own_caching_wins(local_config):
    """A provider's own bound (NCEP's 500,000) overrides the default."""
    own = icechunk.RepositoryConfig(caching=icechunk.CachingConfig(num_chunk_refs=500_000))
    repo = local_config.icechunk_repo(PREFIX, config=own)
    assert repo.config.caching.num_chunk_refs == 500_000
    assert own.caching.num_chunk_refs == 500_000, "the caller's config is not modified"


def test_a_callers_other_settings_are_kept_and_bounded(local_config):
    """Settings other than caching pass through; the bound is added."""
    own = icechunk.RepositoryConfig(inline_chunk_threshold_bytes=7)
    repo = local_config.icechunk_repo(PREFIX, config=own)
    assert repo.config.inline_chunk_threshold_bytes == 7
    assert repo.config.caching.num_chunk_refs == config_module.DEFAULT_CACHED_CHUNK_REFS
    assert own.caching is None, "the caller's config is not modified"


def test_a_store_persisted_with_caching_is_still_bounded(local_config):
    """A store created before caching was stripped gets the handle's bound."""
    storage = local_config.icechunk_storage(PREFIX)
    icechunk.Repository.create(
        storage,
        config=icechunk.RepositoryConfig(caching=icechunk.CachingConfig(num_chunk_refs=15)),
    )
    repo = local_config.icechunk_repo(PREFIX)
    assert repo.config.caching.num_chunk_refs == config_module.DEFAULT_CACHED_CHUNK_REFS


def test_stores_with_huge_manifests_keep_icechunks_default(local_config):
    """The IFS native store's 6M-reference manifests would never fit the bound."""
    repo = local_config.icechunk_repo("bkr/ifs/ifs_native.icechunk")
    assert repo.config.caching is None or repo.config.caching.num_chunk_refs is None
    own = icechunk.RepositoryConfig(caching=icechunk.CachingConfig(num_chunk_refs=9))
    assert local_config.icechunk_repo("bkr/mrms/mrms_GUAM.icechunk", config=own).config.caching
    assert config_module.cached_chunk_refs_for("/bkr/mrms/mrms_CARIB.icechunk") is None
    assert config_module.cached_chunk_refs_for("bkr/geo/mtg_irs_l1_lac4_hourly.icechunk") is None
    assert (
        config_module.cached_chunk_refs_for("bkr/geo/goes19_l2_points.icechunk")
        == config_module.DEFAULT_CACHED_CHUNK_REFS
    )


def test_without_caching_leaves_none_alone():
    """Nothing to strip returns the config as given."""
    assert config_module.without_caching(None) is None
    bare = icechunk.RepositoryConfig(inline_chunk_threshold_bytes=3)
    assert config_module.without_caching(bare) is bare
