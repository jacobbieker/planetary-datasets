"""``scripts/icon_v1_backfill.plan``: which runs to write, and in what order."""

from __future__ import annotations

import importlib.util
import pathlib

import pandas as pd

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "icon_v1_backfill.py"


def load_script():
    spec = importlib.util.spec_from_file_location("icon_v1_backfill", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Provider:
    model = "icon"

    def __init__(self, stored):
        self.stored = {pd.Timestamp(t) for t in stored}

    def missing_timesteps(self, runs):
        return [r for r in runs if r not in self.stored]


def runs(*times):
    return [pd.Timestamp(t) for t in times]


def test_runs_only_dwd_still_has_come_first(monkeypatch):
    script = load_script()
    listings = {
        "archive": runs("2026-10-19T00:00", "2026-10-19T06:00", "2026-10-20T00:00"),
        "live": runs("2026-10-20T00:00", "2026-10-21T12:00", "2026-10-21T18:00"),
    }
    monkeypatch.setattr(script, "list_runs", lambda model, source: listings[source])
    available, missing = script.plan(Provider(["2026-10-19T06:00"]), None, None)
    assert len(available) == 5
    assert missing == runs(
        "2026-10-21T12:00", "2026-10-21T18:00", "2026-10-19T00:00", "2026-10-20T00:00"
    )


def test_archive_only_ignores_the_live_server(monkeypatch):
    script = load_script()
    listings = {"archive": runs("2026-10-19T00:00"), "live": runs("2026-10-21T12:00")}
    monkeypatch.setattr(script, "list_runs", lambda model, source: listings[source])
    _, missing = script.plan(Provider([]), None, None, live=False)
    assert missing == runs("2026-10-19T00:00")
