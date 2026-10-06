"""The GK-2A live append: offline, against a local store and a faked listing.

Nothing here touches S3. The archive listing is replaced by an in-memory one
and the virtual write by a stand-in that commits each scan's slot as `t`, so
what is exercised is the selection (which scans are newer), the windowing
across hours and days, the commit-conflict retry, and the CLI's summary.
"""

from __future__ import annotations

import datetime as dt
import json
import types

import numpy as np
import pytest

pytest.importorskip("obstore", reason="virtualizarr 2.x stack not installed")
pytest.importorskip("obspec_utils", reason="virtualizarr 2.x stack not installed")

from planetary_datasets.providers.virtualized import gk2a_ami_fd as mod  # noqa: E402
from planetary_datasets.providers.virtualized import ingest_gk2a_fd as cli  # noqa: E402

BAND = "ir105"


def _path(band: str, slot: dt.datetime) -> str:
    return (
        f"{mod.PRODUCT}{slot:%Y%m}/{slot:%d}/{slot:%H}/"
        f"gk2a_ami_le1b_{band}_fd{mod.BAND_RESOLUTION[band]}ge_{slot:%Y%m%d%H%M}.nc"
    )


@pytest.fixture
def archive(monkeypatch):
    """An in-memory stand-in for the NOAA listing; add slots with ``archive.add``."""
    objects: list[dict] = []
    prefixes: list[str] = []

    def add(*slots: dt.datetime, band: str = BAND, size: int = 1000) -> None:
        objects.extend({"path": _path(band, s), "size": size} for s in slots)

    def fake_list(store, prefix):
        prefixes.append(prefix)
        return [[o for o in objects if o["path"].startswith(prefix)]]

    monkeypatch.setattr(mod, "obs", types.SimpleNamespace(list=fake_list))
    return types.SimpleNamespace(add=add, prefixes=prefixes)


def _write(repo, times, group: str = "") -> None:
    """Commit one timestep per value of ``times``, creating or appending."""
    exists = mod.last_committed_time(repo, group=group) is not None
    session = repo.writable_session("main")
    _to_store(_scans(times), session, group, append=exists)
    session.commit("test data")


def _scans(times) -> "xr.Dataset":
    import xarray as xr

    ds = xr.Dataset(
        {"image_pixel_values": (("t",), np.zeros(len(times), dtype="uint16"))},
        coords={"t": np.array(times, dtype="datetime64[ns]")},
    )
    # Fixed units, so an append is not encoded in the first write's coarser ones.
    ds["t"].encoding.update(units="nanoseconds since 1970-01-01", dtype="int64")
    return ds


def _to_store(ds, session, group: str = "", *, append: bool) -> None:
    """Write through ``session.store``, as the engine does, so a wrapped session works."""
    ds.to_zarr(
        session.store,
        group=group or None,
        mode="a" if append else "w",
        append_dim="t" if append else None,
        consolidated=False,
        zarr_format=3,
    )


@pytest.fixture
def t_of(monkeypatch):
    """Stand in for opening files: each URL's `t` is its slot plus 20 s.

    Set ``t_of[slot_datetime] = np.datetime64(...)`` to give a file a drifted `t`.
    """
    from planetary_datasets.providers.virtualized import goes_radf_common as common

    overrides: dict[dt.datetime, np.datetime64] = {}

    def t_for(url: str) -> np.datetime64:
        slot = mod.parse_slot_to_datetime(url)
        return overrides.get(slot.astype("datetime64[us]").item(), slot + np.timedelta64(20, "s"))

    monkeypatch.setattr(
        common, "open_virtual_batch", lambda urls, **kw: _scans([t_for(u) for u in urls])
    )
    return overrides


@pytest.fixture
def fake_ingest(monkeypatch, t_of):
    """Stand in for the engine: open the batch through its opener, commit its `t`.

    Records every call's arguments, and swallows a failed batch as the engine does.
    """
    calls: list[dict] = []

    def ingest_all_days(repo, band, *, all_days, group=None, **kwargs):
        calls.append({"band": band, "all_days": all_days, "group": group, **kwargs})
        urls = [u for _, day in all_days for u in day]
        try:
            vds = kwargs["open_batch_fn"](urls)
            _write(repo, list(vds["t"].values), group=group or "")
        except Exception as exc:  # noqa: BLE001 - the engine logs and moves on
            print(f"SKIPPING — {type(exc).__name__}: {exc}")

    monkeypatch.setattr(mod, "ingest_all_days", ingest_all_days)
    monkeypatch.setattr(mod, "_store", lambda: None)
    return calls


def _seed(local_config, *times, band: str = BAND):
    repo = mod.open_repo(band, config=local_config, create=True)
    if times:
        _write(repo, list(times))
    return repo


def _stored(local_config, band: str = BAND) -> list[np.datetime64]:
    from planetary_datasets.providers.virtualized import virtual_repo

    repo = mod.open_repo(band, config=local_config, create=False)
    return list(virtual_repo.committed_times(repo, group=""))


NOW = dt.datetime(2026, 10, 6, 12, 35)


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------
def test_only_scans_newer_than_the_store_are_appended(local_config, archive, fake_ingest):
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(*(dt.datetime(2026, 10, 6, 11, m) for m in (40, 50)))
    archive.add(*(dt.datetime(2026, 10, 6, 12, m) for m in (0, 10, 20, 30)))

    result = mod.append_latest(BAND, lookback_minutes=180, now=NOW, config=local_config)

    assert result == {"appended": 3, "last": "2026-10-06T12:30:20"}
    (call,) = fake_ingest
    urls = [u for _, day in call["all_days"] for u in day]
    assert [mod.parse_slot_to_datetime(u) for u in urls] == [
        np.datetime64(f"2026-10-06T12:{m}", "ns") for m in ("10", "20", "30")
    ]
    # The engine's day-granular resume would skip all of this; it must be off.
    assert call["resume"] is False
    assert call["group"] == ""
    # Only the hour directories from the store's last `t` onward are listed.
    assert archive.prefixes == [f"{mod.PRODUCT}202610/06/12/"]


def test_a_scan_stamped_before_its_slot_is_not_appended_twice(
    local_config, archive, fake_ingest, t_of
):
    # 12:10's `t` drifted to before its slot, and it is already stored.
    t_of[dt.datetime(2026, 10, 6, 12, 10)] = np.datetime64("2026-10-06T12:09:50", "ns")
    _seed(local_config, np.datetime64("2026-10-06T12:09:50", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10), dt.datetime(2026, 10, 6, 12, 20))

    result = mod.append_latest(BAND, now=NOW, config=local_config)

    # The slot filter lets 12:10 through; the opener drops it on its real `t`
    # rather than failing the scan queued behind it.
    assert result == {"appended": 1, "last": "2026-10-06T12:20:20"}
    stored = _stored(local_config)
    assert len(stored) == len(set(stored)) == 2


def test_a_drifted_scan_that_is_the_only_candidate_is_nothing_new(
    local_config, archive, fake_ingest, t_of
):
    t_of[dt.datetime(2026, 10, 6, 12, 10)] = np.datetime64("2026-10-06T12:09:50", "ns")
    _seed(local_config, np.datetime64("2026-10-06T12:09:50", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10))

    assert mod.append_latest(BAND, now=NOW, config=local_config) == {
        "appended": 0, "last": "2026-10-06T12:09:50",
    }


def test_a_scan_stamped_well_after_its_slot_does_not_hide_the_next(
    local_config, archive, fake_ingest
):
    # 12:00's `t` came out six minutes late, so 12:10 is only four minutes on.
    _seed(local_config, np.datetime64("2026-10-06T12:06:00", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 0), dt.datetime(2026, 10, 6, 12, 10))

    result = mod.append_latest(BAND, now=NOW, config=local_config)

    assert result == {"appended": 1, "last": "2026-10-06T12:10:20"}


def test_nothing_new_appends_nothing_and_does_not_write(local_config, archive, fake_ingest):
    _seed(local_config, np.datetime64("2026-10-06T12:30:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 20), dt.datetime(2026, 10, 6, 12, 30))

    result = mod.append_latest(BAND, now=NOW, config=local_config)

    assert result == {"appended": 0, "last": "2026-10-06T12:30:20"}
    assert fake_ingest == []


def test_the_lookback_bounds_how_far_back_an_empty_window_reaches(
    local_config, archive, fake_ingest
):
    # A store far behind only catches up on the lookback window.
    _seed(local_config, np.datetime64("2026-10-05T00:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 11, 50), dt.datetime(2026, 10, 6, 12, 30))

    result = mod.append_latest(BAND, lookback_minutes=30, now=NOW, config=local_config)

    assert result["appended"] == 1
    assert archive.prefixes == [f"{mod.PRODUCT}202610/06/12/"]
    # The scans skipped between the store and the window are reported.
    assert result["gap"] == {"from": "2026-10-05T00:00:20", "to": "2026-10-06T12:05:00"}


def test_a_window_across_midnight_lists_both_days_and_commits_once(
    local_config, archive, fake_ingest
):
    now = dt.datetime(2026, 10, 7, 0, 15)
    _seed(local_config, np.datetime64("2026-10-06T23:40:20", "ns"))
    archive.add(
        dt.datetime(2026, 10, 6, 23, 50),
        dt.datetime(2026, 10, 7, 0, 0),
        dt.datetime(2026, 10, 7, 0, 10),
    )

    result = mod.append_latest(BAND, lookback_minutes=60, now=now, config=local_config)

    assert result == {"appended": 3, "last": "2026-10-07T00:10:20"}
    assert archive.prefixes == [
        f"{mod.PRODUCT}202610/06/23/",
        f"{mod.PRODUCT}202610/07/00/",
    ]
    (call,) = fake_ingest
    assert [day for day, _ in call["all_days"]] == [(2026, 279), (2026, 280)]
    # Both days in one batch, so one commit.
    assert call["batch_size"] == 2
    assert len(_stored(local_config)) == 4


def test_hour_prefixes_cross_a_month_end():
    assert mod._hour_prefixes(dt.datetime(2026, 9, 30, 23, 55), dt.datetime(2026, 10, 1, 0, 5)) == [
        f"{mod.PRODUCT}202609/30/23/",
        f"{mod.PRODUCT}202610/01/00/",
    ]


def test_uncompressed_files_are_still_filtered_out(local_config, archive, fake_ingest):
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10))
    archive.add(dt.datetime(2026, 10, 6, 12, 20), size=mod.grid_size(BAND) ** 2 * 2)

    assert mod.append_latest(BAND, now=NOW, config=local_config)["appended"] == 1


# ---------------------------------------------------------------------------
# Store state and failures
# ---------------------------------------------------------------------------
def test_a_missing_store_is_an_error_unless_creation_is_asked_for(
    local_config, archive, fake_ingest
):
    archive.add(dt.datetime(2026, 10, 6, 12, 10))
    with pytest.raises(FileNotFoundError):
        mod.append_latest(BAND, now=NOW, config=local_config)
    assert not (local_config.icechunk_local_path / mod.store_prefix_for(BAND)).exists()

    result = mod.append_latest(BAND, now=NOW, config=local_config, create=True)
    assert result == {"appended": 1, "last": "2026-10-06T12:10:20"}


def test_a_batch_that_commits_nothing_raises_with_the_logged_reason(
    local_config, archive, monkeypatch, tmp_path
):
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10))
    monkeypatch.setattr(mod, "_store", lambda: None)

    def failing_ingest(repo, band, *, log_dir=None, **kwargs):
        from planetary_datasets.providers.virtualized import goes_radf_common as common

        common.log_event(log_dir, mod.SATELLITE, band, "2026-10-06", "ERROR", "CodecError: zstd")

    monkeypatch.setattr(mod, "ingest_all_days", failing_ingest)

    from planetary_datasets.providers.virtualized import virtual_repo

    with pytest.raises(virtual_repo.NothingCommitted, match="CodecError: zstd"):
        mod.append_latest(BAND, now=NOW, config=local_config)


def test_a_lost_commit_race_is_retried_from_a_fresh_session(local_config, archive, monkeypatch):
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10), dt.datetime(2026, 10, 6, 12, 20))
    monkeypatch.setattr(mod, "_store", lambda: None)
    seen: list[list[np.datetime64]] = []

    def racing_ingest(repo, band, *, all_days, group=None, **kwargs):
        urls = [u for _, day in all_days for u in day]
        seen.append([mod.parse_slot_to_datetime(u) for u in urls])
        times = [mod.parse_slot_to_datetime(u) + np.timedelta64(20, "s") for u in urls]
        session = repo.writable_session("main")
        _to_store(_scans(times), session, append=True)
        if len(seen) == 1:
            # Another writer lands the first new scan before this commit.
            _write(mod.open_repo(band, config=local_config, create=False), times[:1])
        try:
            session.commit("append")
        except Exception as exc:  # noqa: BLE001 - the engine logs and moves on
            print(f"SKIPPING — {type(exc).__name__}: {exc}")

    monkeypatch.setattr(mod, "ingest_all_days", racing_ingest)

    result = mod.append_latest(BAND, now=NOW, config=local_config)

    # The retry re-read the store and appended only what the other writer had not.
    assert seen == [
        [np.datetime64("2026-10-06T12:10", "ns"), np.datetime64("2026-10-06T12:20", "ns")],
        [np.datetime64("2026-10-06T12:20", "ns")],
    ]
    assert result == {"appended": 1, "last": "2026-10-06T12:20:20"}
    assert len(_stored(local_config)) == 3


def test_a_writer_landing_before_the_session_opens_is_not_duplicated(
    local_config, archive, monkeypatch
):
    """Without the snapshot check this would not conflict, and append 12:10 twice."""
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10), dt.datetime(2026, 10, 6, 12, 20))
    monkeypatch.setattr(mod, "_store", lambda: None)
    calls: list[int] = []

    def ingest(repo, band, *, all_days, group=None, **kwargs):
        urls = [u for _, day in all_days for u in day]
        times = [mod.parse_slot_to_datetime(u) + np.timedelta64(20, "s") for u in urls]
        calls.append(len(urls))
        if len(calls) == 1:
            _write(mod.open_repo(band, config=local_config, create=False), times[:1])
        try:
            session = repo.writable_session("main")
            _to_store(_scans(times), session, append=True)
            session.commit("append")
        except Exception as exc:  # noqa: BLE001 - the engine logs and moves on
            print(f"SKIPPING — {type(exc).__name__}: {exc}")

    monkeypatch.setattr(mod, "ingest_all_days", ingest)

    result = mod.append_latest(BAND, now=NOW, config=local_config)

    assert calls == [2, 1]
    assert result == {"appended": 1, "last": "2026-10-06T12:20:20"}
    stored = _stored(local_config)
    assert len(stored) == len(set(stored)) == 3


def test_a_store_with_coarse_time_units_is_refused_before_writing(
    local_config, archive, fake_ingest
):
    """A one-scan first write left to xarray gets "days since"; appending corrupts `t`."""
    repo = mod.open_repo(BAND, config=local_config, create=True)
    ds = _scans([np.datetime64("2026-10-06T12:00:20", "ns")])
    ds["t"].encoding = {}
    session = repo.writable_session("main")
    _to_store(ds, session, append=False)
    session.commit("one scan, default units")
    archive.add(dt.datetime(2026, 10, 6, 12, 10))

    with pytest.raises(ValueError, match="appending would corrupt it"):
        mod.append_latest(BAND, now=NOW, config=local_config)
    assert fake_ingest == []


def test_the_preprocess_pins_nanosecond_time_units():
    import xarray as xr

    raw = xr.Dataset(
        {"image_pixel_values": (("dim_image_y", "dim_image_x"), np.zeros((2, 2), "uint16"))},
        attrs={"observation_start_time": 8.12e8, "observation_end_time": 8.12e8 + 540},
    )
    out = mod.add_time_and_navigation(raw)
    for name in ("t", "t_end"):
        assert out[name].encoding["units"].startswith("nanoseconds since")
        assert out[name].encoding["dtype"] == "int64"


def test_the_opener_drops_only_files_that_do_not_advance_t(t_of):
    urls = [
        f"s3://noaa-gk2a-pds/{_path(BAND, dt.datetime(2026, 10, 6, 12, m))}" for m in (10, 20)
    ]
    t_of[dt.datetime(2026, 10, 6, 12, 10)] = np.datetime64("2026-10-06T12:00:10", "ns")
    last = np.datetime64("2026-10-06T12:00:20", "ns")

    opener = mod._NewerThanOpener(last)
    assert list(opener(urls)["t"].values) == [np.datetime64("2026-10-06T12:20:20", "ns")]

    with pytest.raises(mod.NothingNewer):
        opener(urls[:1])
    assert opener.nothing_newer

    # No store yet, or nothing at stake: every file is kept as is.
    assert mod._NewerThanOpener(None)(urls)["t"].size == 2


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def test_bands_accept_names_commas_and_all():
    assert cli.parse_bands(["ir105,IR087", "ir105"]) == ["ir105", "ir087"]
    assert cli.parse_bands(["all"]) == list(mod.BANDS)
    assert "vi006" in cli.parse_bands(["all"])
    assert cli.parse_bands(["vi006"]) == ["vi006"]
    with pytest.raises(ValueError, match="unknown band"):
        cli.parse_bands(["C13"])


def _summary(capsys) -> dict:
    out = capsys.readouterr().out.strip().splitlines()
    return json.loads(out[-1])


def test_cli_prints_the_summary_last_and_exits_zero_when_nothing_is_new(
    local_config, archive, fake_ingest, monkeypatch, capsys
):
    _seed(local_config, np.datetime64("2026-10-06T12:30:20", "ns"))
    monkeypatch.setattr(mod, "_utcnow", lambda: NOW)

    rc = cli.main(["--append-latest", "--bands", BAND, "--lookback-minutes", "60"])

    assert rc == 0
    assert _summary(capsys) == {
        "satellite": "gk2a",
        "channels": {BAND: {"appended": 0, "last": "2026-10-06T12:30:20"}},
        "errors": {},
    }


def test_cli_appends_then_reports_nothing_on_a_rerun(
    local_config, archive, fake_ingest, monkeypatch, capsys
):
    _seed(local_config, np.datetime64("2026-10-06T12:00:20", "ns"))
    archive.add(dt.datetime(2026, 10, 6, 12, 10), dt.datetime(2026, 10, 6, 12, 20))
    monkeypatch.setattr(mod, "_utcnow", lambda: NOW)
    argv = ["--append-latest", "--bands", BAND, "--lookback-minutes", "180"]

    assert cli.main(argv) == 0
    assert _summary(capsys)["channels"][BAND] == {"appended": 2, "last": "2026-10-06T12:20:20"}
    assert cli.main(argv) == 0
    assert _summary(capsys)["channels"][BAND] == {"appended": 0, "last": "2026-10-06T12:20:20"}


def test_cli_a_failed_band_does_not_stop_the_others(monkeypatch, capsys):
    attempted: list[str] = []

    def fake_append(band, **kwargs):
        attempted.append(band)
        if band == "ir087":
            raise RuntimeError("boom")
        return {"appended": 2, "last": "2026-10-06T12:20:20"}

    monkeypatch.setattr(mod, "append_latest", fake_append)

    rc = cli.main(["--append-latest", "--bands", "ir087,ir105"])

    assert rc == 0
    assert attempted == ["ir087", "ir105"]
    summary = _summary(capsys)
    assert summary["channels"]["ir105"] == {"appended": 2, "last": "2026-10-06T12:20:20"}
    assert summary["channels"]["ir087"]["error"] == "RuntimeError: boom"
    assert summary["errors"] == {"ir087": "RuntimeError: boom"}


def test_cli_exits_non_zero_only_when_every_band_failed(monkeypatch, capsys):
    def fake_append(band, **kwargs):
        raise RuntimeError(f"{band} down")

    monkeypatch.setattr(mod, "append_latest", fake_append)

    assert cli.main(["--append-latest", "--bands", "ir087,ir105"]) == 1
    assert set(_summary(capsys)["errors"]) == {"ir087", "ir105"}


def test_cli_passes_the_lookback_and_store_through(monkeypatch, capsys):
    seen: dict = {}
    monkeypatch.setattr(
        mod, "append_latest",
        lambda band, **kw: seen.update(band=band, **kw) or {"appended": 0, "last": None},
    )

    cli.main([
        "--append-latest", "--bands", "vi006", "--lookback-minutes", "45",
        "--store-base", "tmp/gk2a",
    ])

    assert seen["band"] == "vi006"
    assert seen["lookback_minutes"] == 45
    assert seen["base"] == "tmp/gk2a"
    assert seen["create"] is False
