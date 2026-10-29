"""Two writers creating the same store at once both end up with it open."""

from __future__ import annotations

import icechunk


def test_losing_the_creation_race_opens_the_winners_store(local_config, monkeypatch):
    local_config.icechunk_repo("bkr/test/race.icechunk")  # the winner creates it

    def lost(storage, config=None):
        raise icechunk.ConflictError.__new__(icechunk.ConflictError)

    monkeypatch.setattr(icechunk.Repository, "open_or_create", lost)
    opened = local_config.icechunk_repo("bkr/test/race.icechunk")
    assert list(opened.list_branches()) == ["main"]
