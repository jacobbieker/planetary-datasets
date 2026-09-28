"""Offline tests for the HURDAT2 provider. Nothing here touches the network."""

from __future__ import annotations

import io
import textwrap

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import hurdat2 as h2

# Two real storms trimmed to a few entries, plus the 1969 line whose latitude and
# longitude are not separated by a comma and a pre-2021 line with no radius of maximum
# wind column.
SAMPLE = textwrap.dedent(
    """\
    AL011851,            UNNAMED,      3,
    18510625, 0000,  , HU, 28.0N,  94.8W,  80, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
    18510625, 0600,  , HU, 28.0N,  95.4W,  80, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
    19690929, 0600,  , EX, 63.3N    7.5E,  70, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999, -999
    AL092011,              IRENE,      2,
    20110821, 0000,  , TS, 15.0N,  59.0W,  45,  1006,  105,   60,   60,   85,    0,    0,    0,    0,    0,    0,    0,    0,   60
    20110827, 1200, L, HU, 35.2N,  75.7W,  75,   952,  230,  230,  120,  150,  120,  120,   60,   75,   30,   30,    0,   25,   45
    """
)


@pytest.fixture
def storms():
    return h2.parse_hurdat2(SAMPLE.splitlines())


def test_parses_headers_and_tracks(storms):
    assert [s.storm_id for s in storms] == ["AL011851", "AL092011"]
    assert [s.name for s in storms] == ["UNNAMED", "IRENE"]
    assert [s.basin_code for s in storms] == ["AL", "AL"]
    assert [s.number for s in storms] == [1, 9]
    assert [s.year for s in storms] == [1851, 2011]
    assert [len(s.records) for s in storms] == [3, 2]


def test_signed_coordinates(storms):
    first = storms[0].records[0]
    assert first["latitude"] == pytest.approx(28.0)
    assert first["longitude"] == pytest.approx(-94.8)
    # Eastern hemisphere, from the line missing its separating comma.
    repaired = storms[0].records[2]
    assert repaired["latitude"] == pytest.approx(63.3)
    assert repaired["longitude"] == pytest.approx(7.5)


def test_missing_values_become_nan(storms):
    record = storms[0].records[0]
    assert np.isnan(record["min_pressure_mb"])
    assert np.isnan(record["wind_radii_34kt_ne"])
    # A pre-2021 line has no radius-of-maximum-wind column at all.
    assert np.isnan(record["radius_max_wind_nmi"])


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("28.0N", 28.0),
        ("28.0S", -28.0),
        ("94.8W", -94.8),
        ("7.5E", 7.5),
        # Two 1970s Atlantic records omit the hemisphere letter altogether.
        ("38.83", 38.83),
        ("", None),
        ("N", None),
    ],
)
def test_parse_coordinate(token, expected):
    got = h2._parse_coordinate(token)
    assert np.isnan(got) if expected is None else got == pytest.approx(expected)


@pytest.mark.parametrize(
    ("token", "expected"),
    [("45", 45.0), ("0", 0.0), ("-999", None), ("-99", None), ("", None), ("junk", None)],
)
def test_parse_number_sentinels(token, expected):
    """-99 is a second missing-wind sentinel in a few dozen 1970s records."""
    got = h2._parse_number(token)
    assert np.isnan(got) if expected is None else got == pytest.approx(expected)


def test_zero_radii_are_kept_not_treated_as_missing(storms):
    record = storms[1].records[0]
    assert record["wind_radii_34kt_ne"] == pytest.approx(105.0)
    assert record["wind_radii_50kt_ne"] == pytest.approx(0.0)
    assert record["radius_max_wind_nmi"] == pytest.approx(60.0)


def test_record_identifier_and_status(storms):
    assert storms[1].records[1]["record_identifier"] == "L"
    assert storms[1].records[1]["status"] == "HU"
    assert storms[1].records[0]["record_identifier"] == ""


def test_genesis_time(storms):
    assert storms[1].genesis_time == pd.Timestamp("2011-08-21T00:00")


def test_declared_count_mismatch_raises():
    bad = "AL011851,            UNNAMED,      5,\n" + SAMPLE.splitlines()[1] + "\n"
    with pytest.raises(h2.HURDAT2ParseError, match="declares 5"):
        h2.parse_hurdat2(bad.splitlines())


def test_track_line_before_header_raises():
    with pytest.raises(h2.HURDAT2ParseError, match="before any storm header"):
        h2.parse_hurdat2(SAMPLE.splitlines()[1:])


def test_unparseable_line_is_skipped_not_fatal():
    text = "AL011851,            UNNAMED,      1,\n18510625, 0000,  , HU\n"
    # The short line is dropped, which then fails the declared-count check.
    with pytest.raises(h2.HURDAT2ParseError):
        h2.parse_hurdat2(text.splitlines())


def test_dataset_layout_and_padding(storms):
    ds = h2.storms_to_dataset(storms[1:], pd.Timestamp("2011-01-01"), basin="atlantic")
    assert ds.sizes == {"time": 1, "storm": h2.MAX_STORMS, "record": h2.MAX_RECORDS}
    assert ds.storm_id.values[0, 0] == "AL092011"
    assert ds.storm_id.values[0, 1] == ""
    assert int(ds.record_count.values[0, 0]) == 2
    assert int(ds.record_count.values[0, 1]) == 0
    # Padding is NaN, not zero: zero is a real wind radius.
    assert np.isnan(ds.wind_radii_34kt_ne.values[0, 0, 2])
    assert np.isnan(ds.record_time.values[0, 0, 2])
    assert ds.max_sustained_wind_knots.dtype == np.float32


def test_record_time_is_cf_encoded_seconds(storms):
    ds = h2.storms_to_dataset(storms[1:], pd.Timestamp("2011-01-01"), basin="atlantic")
    assert ds.record_time.attrs["units"] == "seconds since 1970-01-01"
    expected = pd.Timestamp("2011-08-21T00:00").value / 1e9
    assert ds.record_time.values[0, 0, 0] == pytest.approx(expected)


def test_too_many_storms_raises(storms):
    with pytest.raises(ValueError, match="MAX_STORMS"):
        h2.storms_to_dataset(storms * 10, pd.Timestamp("2011-01-01"), "atlantic", max_storms=4)


def test_too_many_records_raises(storms):
    with pytest.raises(ValueError, match="MAX_RECORDS"):
        h2.storms_to_dataset(storms, pd.Timestamp("1851-01-01"), "atlantic", max_records=1)


def test_unknown_basin_raises():
    with pytest.raises(ValueError, match="unknown basin"):
        h2.HURDAT2Provider(basin="indian")


@pytest.mark.parametrize(
    ("token", "expected"),
    [("091226", "2026-09-12"), ("02272026", "2026-02-27"), ("notadate", None)],
)
def test_release_date_parsing(token, expected):
    got = h2._parse_release_date(token)
    assert got is None if expected is None else got == pd.Timestamp(expected)


def test_latest_release_url_picks_newest(monkeypatch):
    listing = """
    <a href="hurdat2-1851-2024-040425.txt">a</a>
    <a href="hurdat2-1851-2025-02272026.txt">b</a>
    <a href="hurdat2-1851-2025-091226.txt">c</a>
    <a href="hurdat2-nepac-1949-2025-091426.txt">d</a>
    """

    class _Ctx:
        def __enter__(self):
            return io.StringIO(listing)

        def __exit__(self, *exc):
            return False

    import fsspec

    monkeypatch.setattr(fsspec, "open", lambda *a, **k: _Ctx())
    assert h2.latest_release_url("atlantic").endswith("hurdat2-1851-2025-091226.txt")
    assert h2.latest_release_url("pacific").endswith("hurdat2-nepac-1949-2025-091426.txt")


def test_latest_release_url_prefers_a_same_day_revision(monkeypatch):
    """A re-release on the same date is published with a trailing letter."""
    listing = """
    <a href="hurdat2-nepac-1949-2020-043021.txt">a</a>
    <a href="hurdat2-nepac-1949-2020-043021a.txt">b</a>
    """

    class _Ctx:
        def __enter__(self):
            return io.StringIO(listing)

        def __exit__(self, *exc):
            return False

    import fsspec

    monkeypatch.setattr(fsspec, "open", lambda *a, **k: _Ctx())
    assert h2.latest_release_url("pacific").endswith("hurdat2-nepac-1949-2020-043021a.txt")


def test_latest_release_url_raises_when_naming_changes(monkeypatch):
    class _Ctx:
        def __enter__(self):
            return io.StringIO("<a href='hurdat3-whatever.txt'>x</a>")

        def __exit__(self, *exc):
            return False

    import fsspec

    monkeypatch.setattr(fsspec, "open", lambda *a, **k: _Ctx())
    with pytest.raises(h2.HURDAT2ParseError, match="naming scheme"):
        h2.latest_release_url("atlantic")


@pytest.fixture
def local_provider(tmp_path, local_config):
    """A provider reading a pinned local file and writing to a local store."""
    source = tmp_path / "hurdat2-1851-2011-010101.txt"
    source.write_text(SAMPLE)
    provider = h2.HURDAT2Provider(
        basin="atlantic",
        url=source.as_uri(),
        cache_dir=tmp_path / "cache",
        config=local_config,
    )
    return provider


def test_fetch_returns_empty_for_a_season_with_no_storms(local_provider):
    assert local_provider.fetch(pd.Timestamp("1900-01-01")) == []


def test_fetch_downloads_once_and_caches(local_provider):
    first = local_provider.fetch(pd.Timestamp("2011-01-01"))
    second = local_provider.fetch(pd.Timestamp("2011-01-01"))
    assert first == second and len(first) == 1


def test_run_partition_round_trip(local_provider):
    assert local_provider.run_partition(pd.Timestamp("1851-01-01")) is True
    assert local_provider.run_partition(pd.Timestamp("2011-01-01")) is True
    # Re-running is a no-op, not a duplicate append.
    assert local_provider.run_partition(pd.Timestamp("2011-01-01")) is False

    repo = local_provider.get_icechunk_repo()
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert list(pd.DatetimeIndex(stored.time.values)) == [
        pd.Timestamp("1851-01-01"),
        pd.Timestamp("2011-01-01"),
    ]
    irene = stored.sel(time="2011-01-01").isel(storm=0)
    assert str(irene.storm_name.values) == "IRENE"
    assert float(irene.max_sustained_wind_knots.values[1]) == pytest.approx(75.0)
    # record_time survives the round trip as a real datetime.
    assert pd.Timestamp(irene.record_time.values[0]) == pd.Timestamp("2011-08-21T00:00")
    assert pd.isna(pd.Timestamp(irene.record_time.values[2]))


def test_run_range_snaps_to_seasons_and_does_not_repeat_work(local_provider):
    mid_season = pd.DatetimeIndex(["2011-06-01", "2011-09-01", "1851-07-01"])
    assert local_provider.run_range(mid_season) == 2
    # The seasons are now stored under 1 January, so a second pass finds nothing to do
    # and must not re-parse and rebuild them.
    assert local_provider.run_range(mid_season) == 0

    repo = local_provider.get_icechunk_repo()
    stored = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    assert sorted(pd.DatetimeIndex(stored.time.values)) == [
        pd.Timestamp("1851-01-01"),
        pd.Timestamp("2011-01-01"),
    ]


def test_timezone_aware_partition_is_normalised(local_provider):
    aware = pd.Timestamp("2011-01-01", tz="America/New_York")
    assert local_provider.run_partition(aware) is True
    # 2011-01-01 05:00 UTC still belongs to the 2011 season and is stored at the epoch
    # the naive store uses.
    assert local_provider.run_partition(pd.Timestamp("2011-01-01")) is False
