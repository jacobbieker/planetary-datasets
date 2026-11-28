"""Two writers creating the same store at once both end up with it open."""

from __future__ import annotations

import icechunk
import pytest


def test_losing_the_creation_race_opens_the_winners_store(local_config, monkeypatch):
    """A create that fails because the store now exists opens it instead."""
    local_config.icechunk_repo("bkr/test/race.icechunk")  # the winner creates it

    real_exists = icechunk.Repository.exists
    calls = []

    def not_yet(storage):
        """The loser looked before the winner had created it."""
        calls.append(storage)
        return len(calls) > 1 and real_exists(storage)

    monkeypatch.setattr(icechunk.Repository, "exists", staticmethod(not_yet))
    opened = local_config.icechunk_repo("bkr/test/race.icechunk")
    assert list(opened.list_branches()) == ["main"]


def test_a_failed_create_with_no_store_raises(local_config, monkeypatch):
    """A create that fails while nothing exists is an error, not a lost race."""

    def broken(storage, config=None):
        raise icechunk.IcechunkError("storage refused")

    monkeypatch.setattr(icechunk.Repository, "create", staticmethod(broken))
    with pytest.raises(icechunk.IcechunkError):
        local_config.icechunk_repo("bkr/test/broken.icechunk")
