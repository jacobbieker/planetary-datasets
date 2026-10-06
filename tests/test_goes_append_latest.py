"""Offline tests for the GOES live append (``--append-latest``).

The listing is a real obstore ``MemoryStore`` laid out like NOAA's bucket, and
the store a real local Icechunk repository. Only the HDF open is faked: each
"scan" becomes a tiny in-memory dataset whose ``t`` is the scan's mid-point, as
GOES files carry, so the ordering filter is tested against the same offset it
meets in production.
"""

from __future__ import annotations

import datetime
import json

import numpy as np
import pytest

pytest.importorskip("obstore", reason="virtualizarr 2.x stack not installed")
pytest.importorskip("obspec_utils", reason="virtualizarr 2.x stack not installed")
pytest.importorskip("icechunk", reason="icechunk not installed")

import obstore as obs  # noqa: E402
import xarray as xr  # noqa: E402

from planetary_datasets.providers.virtualized import goes_radf_common as common  # noqa: E402
from planetary_datasets.providers.virtualized import ingest_goes_radf  # noqa: E402

# The fake scans are in-memory arrays rather than virtual references, which
# virtualizarr warns about on every write.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Attempting to write an entirely non-virtual:UserWarning"
)

BUCKET = "s3://noaa-goes19"
PRODUCT = "ABI-L1b-RadF/"
#: A GOES file's `t` is the middle of its ten-minute scan.
MID_SCAN = datetime.timedelta(minutes=5)


def _key(start: datetime.datetime, channel: int = 13) -> str:
    doy = start.timetuple().tm_yday
    stamp = f"{start.year}{doy:03d}{start:%H%M%S}0"
    end = start + datetime.timedelta(minutes=9, seconds=40)
    end_stamp = f"{end.year}{end.timetuple().tm_yday:03d}{end:%H%M%S}0"
    return (
        f"{PRODUCT}{start.year}/{doy:03d}/{start:%H}/"
        f"OR_ABI-L1b-RadF-M6C{channel:02d}_G19_s{stamp}_e{end_stamp}_c{end_stamp}.nc"
    )


def _bucket(scans: list[datetime.datetime], channels=(13,)):
    """A MemoryStore holding one file per scan start and channel."""
    store = obs.store.MemoryStore()
    for start in scans:
        for channel in channels:
            obs.put(store, _key(start, channel), b"x" * 1000)
    return store


def _every_ten_minutes(first: datetime.datetime, last: datetime.datetime):
    out, t = [], first
    while t <= last:
        out.append(t)
        t += datetime.timedelta(minutes=10)
    return out


class _Opener:
    """open_batch_fn stand-in: one small dataset per URL, combined along `t`.

    URLs listed in ``new_era`` open with an extra variable, so they fail
    ``combine_failure_reason`` against an old-era scan, the way a codec change
    does against the real virtual datasets.
    """

    def __init__(self, new_era=(), broken=()):
        self.new_era = set(new_era)
        self.broken = set(broken)
        self.opened: list[str] = []

    def one(self, url: str) -> xr.Dataset:
        if url in self.broken:
            raise ValueError(f"unusable test file {url}")
        self.opened.append(url)
        t = common.parse_scan_start_to_datetime(url) + np.timedelta64(MID_SCAN)
        data = {"Rad": (("t", "y", "x"), np.full((1, 4, 4), 1.0, dtype="float32"))}
        if url in self.new_era:
            data["Rad_new_era"] = (("t", "y", "x"), np.zeros((1, 4, 4), dtype="float32"))
        ds = xr.Dataset(data, coords={"t": [t]})
        # GOES stores `t` as float seconds since J2000; without a fixed encoding
        # each append would pick its own units.
        ds["t"].encoding = {"units": "seconds since 2000-01-01 12:00:00", "dtype": "float64"}
        return ds

    def __call__(self, urls, **_kwargs) -> xr.Dataset:
        return xr.concat([self.one(u) for u in urls], dim="t")


def _repo_factory(cfg, channel: int = 13):
    base = common.default_store_prefix("goes19")

    def factory(suffix: str):
        prefix = common.suffixed_prefix(base, channel=channel, era=suffix or None)
        return common.open_virtual_repository(
            cfg.icechunk_storage(prefix), virtual_buckets=["noaa-goes19"]
        )

    return factory


def _append(cfg, store, now, *, lookback=180, opener=None, rename=None, **kwargs):
    archive = common.SATELLITES["goes19"]
    return common.append_latest_scans(
        13,
        repo_factory=_repo_factory(cfg),
        satellite_name="GOES-19",
        satellite="goes19",
        product_label="ABI-L1b-RadF",
        archive_start_date=archive.archive_start_date,
        store=store,
        bucket=BUCKET,
        product=PRODUCT,
        product_keys=["ABI-L1b-RadF"],
        loadable_variables=(),
        epoch_threshold=archive.epoch_threshold,
        lookback_minutes=lookback,
        now=now,
        log_dir=str(cfg.data_dir / "logs"),
        keep_data_vars=frozenset({"Rad"}),
        open_batch_fn=opener or _Opener(),
        rename_store_fn=rename,
        **kwargs,
    )


def _times(cfg, suffix: str = "") -> np.ndarray:
    repo = _repo_factory(cfg)(suffix)
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    return ds["t"].values


def _mid(start: datetime.datetime) -> np.datetime64:
    return np.datetime64(start + MID_SCAN, "ns")


def _commits(cfg) -> int:
    repo = _repo_factory(cfg)("")
    return len(list(repo.ancestry(branch="main")))


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------
def test_hour_prefixes_cross_midnight_and_new_year():
    prefixes = common.hour_prefixes(
        PRODUCT, datetime.datetime(2025, 12, 31, 23, 40), datetime.datetime(2026, 1, 1, 0, 20)
    )
    assert prefixes == ["ABI-L1b-RadF/2025/365/23/", "ABI-L1b-RadF/2026/001/00/"]


def test_hour_prefixes_accept_an_aware_now():
    utc = datetime.timezone.utc
    prefixes = common.hour_prefixes(
        PRODUCT,
        datetime.datetime(2026, 10, 6, 10, 5, tzinfo=utc),
        datetime.datetime(2026, 10, 6, 11, 0, tzinfo=utc),
    )
    assert prefixes == ["ABI-L1b-RadF/2026/279/10/", "ABI-L1b-RadF/2026/279/11/"]


def test_scans_after_excludes_the_committed_scan_by_its_mid_point():
    start = datetime.datetime(2026, 10, 6, 12, 0)
    urls = [f"{BUCKET}/{_key(t)}" for t in _every_ten_minutes(start, start.replace(minute=30))]
    # The store's last `t` is the 12:10 scan's mid-point, 12:15.
    kept = common.scans_after(urls, after=_mid(start.replace(minute=10)))
    assert [common.parse_scan_start_to_datetime(u) for u in kept] == [
        np.datetime64("2026-10-06T12:20", "ns"),
        np.datetime64("2026-10-06T12:30", "ns"),
    ]


@pytest.mark.parametrize(
    "token,expected",
    [
        ("13", [13]),
        ("C02", [2]),
        ("c7", [7]),
        ("C01,C13", [1, 13]),
        ("all", list(range(1, 17))),
    ],
)
def test_parse_channels_accepts_numbers_labels_and_all(token, expected):
    assert ingest_goes_radf.parse_channels(token) == expected


@pytest.mark.parametrize("token", ["C17", "0", "ir105", ","])
def test_parse_channels_rejects_anything_else(token):
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        ingest_goes_radf.parse_channels(token)


# ---------------------------------------------------------------------------
# Appending against a local store
# ---------------------------------------------------------------------------
def test_only_scans_newer_than_the_last_commit_are_appended(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 9, 0), datetime.datetime(2026, 10, 6, 11, 50)
    )
    store = _bucket(scans)
    now = datetime.datetime(2026, 10, 6, 12, 0)

    # Seed: the 10:00 scan alone.
    seeded = _append(local_config, store, datetime.datetime(2026, 10, 6, 10, 0), lookback=1)
    assert seeded["appended"] == 1
    assert list(_times(local_config)) == [_mid(datetime.datetime(2026, 10, 6, 10, 0))]

    result = _append(local_config, store, now)
    expected = [s for s in scans if s >= datetime.datetime(2026, 10, 6, 10, 0)]
    assert result["appended"] == len(expected) - 1
    assert list(_times(local_config)) == [_mid(s) for s in expected]
    assert result["last"] == "2026-10-06T11:55:00"


def test_nothing_new_appends_nothing_and_does_not_commit(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0), datetime.datetime(2026, 10, 6, 11, 50)
    )
    store = _bucket(scans)
    now = datetime.datetime(2026, 10, 6, 12, 0)
    assert _append(local_config, store, now)["appended"] == 6
    commits = _commits(local_config)

    again = _append(local_config, store, now)
    assert again == {"appended": 0, "last": "2026-10-06T11:55:00"}
    assert _commits(local_config) == commits


def test_the_lookback_bounds_how_far_back_an_empty_store_reaches(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 8, 0), datetime.datetime(2026, 10, 6, 11, 50)
    )
    result = _append(
        local_config, _bucket(scans), datetime.datetime(2026, 10, 6, 12, 0), lookback=30
    )
    assert result["appended"] == 3  # 11:30, 11:40, 11:50
    # Nothing past `now` either, though the hour directory is listed.
    late = _bucket(scans + [datetime.datetime(2026, 10, 6, 12, 10)])
    assert _append(local_config, late, datetime.datetime(2026, 10, 6, 12, 5))["appended"] == 0


def test_an_append_across_midnight_is_one_commit_spanning_both_days(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 5, 23, 0), datetime.datetime(2026, 10, 6, 0, 30)
    )
    store = _bucket(scans)
    _append(local_config, store, datetime.datetime(2026, 10, 5, 23, 30), lookback=1)
    commits = _commits(local_config)

    result = _append(local_config, store, datetime.datetime(2026, 10, 6, 0, 35))
    assert result["appended"] == 6  # 23:40 through 00:30
    assert _commits(local_config) == commits + 1
    times = _times(local_config)
    assert times[0] == _mid(datetime.datetime(2026, 10, 5, 23, 30))
    assert times[-1] == _mid(datetime.datetime(2026, 10, 6, 0, 30))
    assert np.all(np.diff(times) > np.timedelta64(0))


def test_a_commit_lost_to_another_writer_is_retried_from_a_fresh_session(
    local_config, monkeypatch
):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0), datetime.datetime(2026, 10, 6, 11, 50)
    )
    real = common.ingest_all_days
    calls = []

    def loses_the_first_race(repo, *args, **kwargs):
        calls.append(len(kwargs["all_days"][0][1]))
        if len(calls) == 1:
            return None  # ingest_all_days logs a failed commit and returns
        return real(repo, *args, **kwargs)

    monkeypatch.setattr(common, "ingest_all_days", loses_the_first_race)
    result = _append(local_config, _bucket(scans), datetime.datetime(2026, 10, 6, 12, 0))
    assert calls == [6, 6]
    assert result["appended"] == 6


def test_a_channel_that_never_commits_raises_with_the_logged_reason(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0), datetime.datetime(2026, 10, 6, 11, 20)
    )
    urls = {f"{BUCKET}/{_key(s)}" for s in scans}
    with pytest.raises(common.AppendIncomplete, match="unusable test file"):
        _append(
            local_config, _bucket(scans), datetime.datetime(2026, 10, 6, 12, 0),
            opener=_Opener(broken=urls), max_attempts=2,
        )


# ---------------------------------------------------------------------------
# Codec-era rollover
# ---------------------------------------------------------------------------
def _seeded_with_a_new_era(local_config, n_new: int):
    old = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 10, 0), datetime.datetime(2026, 10, 6, 10, 50)
    )
    new = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0),
        datetime.datetime(2026, 10, 6, 11, 0) + datetime.timedelta(minutes=10 * (n_new - 1)),
    )
    store = _bucket(old + new)
    _append(local_config, store, datetime.datetime(2026, 10, 6, 10, 55))
    opener = _Opener(new_era={f"{BUCKET}/{_key(s)}" for s in new})
    return store, opener, new


def test_a_new_codec_era_freezes_the_live_store_and_starts_a_new_one(local_config):
    store, opener, new = _seeded_with_a_new_era(local_config, n_new=3)
    renamer = common.make_config_store_renamer(
        lambda suffix: common.suffixed_prefix(
            common.default_store_prefix("goes19"), channel=13, era=suffix or None
        )
    )
    result = _append(
        local_config, store, datetime.datetime(2026, 10, 6, 11, 30),
        opener=opener, rename=renamer,
    )
    assert result["new_era"] == "2026-10-06"
    assert result["appended"] == 3
    # The old era is frozen under the last day it holds, intact...
    assert len(_times(local_config, "2026-10-06")) == 6
    # ...and the live store now holds only the new era.
    assert list(_times(local_config)) == [_mid(s) for s in new]


def test_a_new_codec_era_without_a_rename_hook_is_refused(local_config):
    store, opener, _ = _seeded_with_a_new_era(local_config, n_new=3)
    with pytest.raises(common.CodecEraChange):
        _append(local_config, store, datetime.datetime(2026, 10, 6, 11, 30), opener=opener)
    assert len(_times(local_config)) == 6


def test_too_few_odd_scans_are_deferred_rather_than_splitting_the_era(local_config):
    store, opener, _ = _seeded_with_a_new_era(local_config, n_new=2)
    result = _append(
        local_config, store, datetime.datetime(2026, 10, 6, 11, 30), opener=opener,
        rename=lambda *a: pytest.fail("must not freeze the store for two scans"),
    )
    assert result["appended"] == 0
    assert result["deferred"] == 2


def test_an_odd_scan_between_good_ones_is_dropped_not_an_era(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 10, 0), datetime.datetime(2026, 10, 6, 11, 30)
    )
    store = _bucket(scans)
    _append(local_config, store, datetime.datetime(2026, 10, 6, 10, 55))
    odd = f"{BUCKET}/{_key(datetime.datetime(2026, 10, 6, 11, 10))}"
    result = _append(
        local_config, store, datetime.datetime(2026, 10, 6, 11, 35),
        opener=_Opener(new_era={odd}),
    )
    assert result["appended"] == 3
    assert result["dropped"] == 1


def test_a_scan_that_will_not_open_is_held_back_not_written_past(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 10, 0), datetime.datetime(2026, 10, 6, 11, 30)
    )
    store = _bucket(scans)
    _append(local_config, store, datetime.datetime(2026, 10, 6, 10, 55))
    flaky = f"{BUCKET}/{_key(datetime.datetime(2026, 10, 6, 11, 10))}"
    result = _append(
        local_config, store, datetime.datetime(2026, 10, 6, 11, 35),
        opener=_Opener(broken={flaky}),
    )
    # 11:00 goes in; 11:10 and everything after it wait for the next run...
    assert result["appended"] == 1
    assert result["deferred"] == 3
    # ...which picks them all up once the file opens.
    again = _append(local_config, store, datetime.datetime(2026, 10, 6, 11, 40))
    assert again["appended"] == 3


def _renamer():
    return common.make_config_store_renamer(
        lambda suffix: common.suffixed_prefix(
            common.default_store_prefix("goes19"), channel=13, era=suffix or None
        )
    )


def test_a_failed_new_era_write_restores_the_live_store(local_config, monkeypatch):
    store, opener, new = _seeded_with_a_new_era(local_config, n_new=3)
    real = common.ingest_all_days

    def new_era_never_commits(repo, *args, **kwargs):
        first = kwargs["all_days"][0][1][0]
        if first in opener.new_era:
            return None
        return real(repo, *args, **kwargs)

    monkeypatch.setattr(common, "ingest_all_days", new_era_never_commits)
    with pytest.raises(common.AppendIncomplete):
        _append(
            local_config, store, datetime.datetime(2026, 10, 6, 11, 30),
            opener=opener, rename=_renamer(),
        )
    # The old era is back as the live store, and no frozen copy is left behind.
    assert len(_times(local_config)) == 6
    stores = local_config.icechunk_local_path / "bkr/geo/virtualized"
    assert [p.name for p in stores.iterdir()] == ["goes19_radf_C13.icechunk"]


def test_the_renamer_refuses_to_move_onto_a_store_that_holds_data(local_config):
    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0), datetime.datetime(2026, 10, 6, 11, 20)
    )
    _append(local_config, _bucket(scans), datetime.datetime(2026, 10, 6, 11, 30))
    rename = _renamer()
    rename("", "2026-10-06")
    _append(local_config, _bucket(scans), datetime.datetime(2026, 10, 6, 11, 30))
    with pytest.raises(FileExistsError):
        rename("", "2026-10-06")
    assert len(_times(local_config)) == 3
    assert len(_times(local_config, "2026-10-06")) == 3


def test_append_mode_rolls_over_only_on_the_config_storage():
    args = ingest_goes_radf.build_args("goes19")
    assert ingest_goes_radf._append_renamer(args, 13) is not None
    args.storage = "s3"
    assert ingest_goes_radf._append_renamer(args, 13) is None


# ---------------------------------------------------------------------------
# CLI: per-channel isolation and the JSON summary
# ---------------------------------------------------------------------------
def _args(**overrides):
    args = ingest_goes_radf.build_args("goes19", lookback_minutes=30)
    for name, value in overrides.items():
        setattr(args, name, value)
    args.parallel = False
    return args


def test_a_failed_channel_does_not_stop_the_others(local_config, capsys):
    def append_fn(args, channel):
        if channel == 2:
            raise RuntimeError("C02 broke")
        return {"appended": channel, "last": "2026-10-06T11:55:00"}

    code = ingest_goes_radf.run_append(_args(), [13, 2, 7], append_fn=append_fn)
    assert code == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["satellite"] == "goes19"
    assert list(summary["channels"]) == ["C13", "C02", "C07"]
    assert summary["channels"]["C13"] == {"appended": 13, "last": "2026-10-06T11:55:00"}
    assert summary["channels"]["C02"]["error"] == "RuntimeError: C02 broke"
    assert summary["channels"]["C07"]["appended"] == 7


def test_the_run_fails_only_when_every_channel_failed(local_config, capsys):
    def append_fn(args, channel):
        raise RuntimeError("down")

    assert ingest_goes_radf.run_append(_args(), [13, 2], append_fn=append_fn) == 1
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert all("error" in r for r in summary["channels"].values())


def test_the_cli_appends_through_the_config_store_and_prints_json_last(
    local_config, monkeypatch, capsys
):
    from planetary_datasets.providers.virtualized import goes_19_radf

    scans = _every_ten_minutes(
        datetime.datetime(2026, 10, 6, 11, 0), datetime.datetime(2026, 10, 6, 11, 50)
    )
    store = _bucket(scans, channels=(2, 13))
    monkeypatch.setattr(goes_19_radf, "_store", lambda: store)
    real_append = ingest_goes_radf.append_latest

    def append_latest(satellite, channel, **kwargs):
        return real_append(
            satellite, channel,
            now=datetime.datetime(2026, 10, 6, 12, 0),
            open_batch_fn=_Opener(),
            keep_data_vars=frozenset({"Rad"}),
            **{k: v for k, v in kwargs.items() if k != "now"},
        )

    monkeypatch.setattr(ingest_goes_radf, "append_latest", append_latest)
    monkeypatch.setattr(
        "sys.argv",
        ["ingest_goes_radf", "--satellite", "goes19", "--channels", "C13,C02",
         "--append-latest", "--lookback-minutes", "90"],
    )
    with pytest.raises(SystemExit) as exit_info:
        ingest_goes_radf.main()
    assert exit_info.value.code == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["channels"]["C13"]["appended"] == 6
    assert summary["channels"]["C02"]["appended"] == 6
    stores = local_config.icechunk_local_path / "bkr/geo/virtualized"
    assert sorted(p.name for p in stores.iterdir()) == [
        "goes19_radf_C02.icechunk", "goes19_radf_C13.icechunk",
    ]
