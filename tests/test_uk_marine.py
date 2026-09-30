"""Offline tests for the Met Office UK marine observations provider.

Every fixture is synthesised here: the bucket is a rolling ten-day window, so a test
pinned to a real hour would start passing and then quietly stop.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import pandas as pd
import pytest

from helpers import read_store as stored
from planetary_datasets.providers.observations.base import NoStationDataError
from planetary_datasets.providers.observations.uk_marine import (
    BUCKET,
    CANONICAL_IDS,
    MEASUREMENT_VARIABLES,
    QC_FLAG_VALUES,
    SPECTRUM_BANDS,
    SPECTRUM_VARIABLES,
    STATIONS,
    UKMarineObservationsProvider,
    VARIABLES,
    qc_code,
    read_station_csv,
    spectrum_values,
    station_slug,
)

HOUR = pd.Timestamp("2026-09-22 07:00")

#: Two platforms that really are in the roster: a moored buoy that reports a wave spectrum
#: and a ferry that moves between hours.
BUOY = "brittany_buoy"
SHIP = "queen_mary_2_automatic_weather_station"


def spectrum_json(energy: float = 0.5) -> str:
    """A wave spectrum in the upstream's shape, with a ``-2`` sentinel in the last band."""
    bands = {
        f"frequency_band_{band}": {
            "bin_number_energy": 0.01 * band,
            "energy_field": energy * band,
            "a1": 0.1,
            "a2": 0.2,
            "b1": 0.3,
            "b2": 0.4,
        }
        for band in range(1, SPECTRUM_BANDS)
    }
    bands[f"frequency_band_{SPECTRUM_BANDS}"] = {
        "bin_number_energy": 0,
        "energy_field": 0,
        "a1": -2,
        "a2": -2,
        "b1": -2,
        "b2": -2,
    }
    return json.dumps(bands)


def write_station_csv(
    directory: pathlib.Path,
    slug: str,
    when: pd.Timestamp,
    *,
    uuid: str = "11d94ae4-fa21-42e0-9a38-72fe5595213a",
    longitude: float = -8.47,
    latitude: float = 47.5,
    pressure: float | None = 1027.79,
    flag: str = '{"good": []}',
    spectrum: str | None = None,
) -> pathlib.Path:
    """Write one platform-hour CSV in the bucket's pipe-delimited shape."""
    row = {
        "timestep": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": slug.replace("_", " ").title(),
        "longitude": longitude,
        "latitude": latitude,
        "wave_spectral_data_collection": spectrum if spectrum is not None else "",
        "wave_spectral_data_collection_qc": flag,
    }
    for name in MEASUREMENT_VARIABLES:
        row[name] = pressure if name == "pressure_marine" else ""
        row[f"{name}_qc"] = flag

    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{slug}_{uuid}.csv"
    pd.DataFrame([row]).to_csv(path, sep="|", index=False)
    return path


class FakeS3:
    """Enough of an ``s3fs.S3FileSystem`` for the fetch path, backed by a local tree.

    Batch directories live under ``root``; ``ls`` on the bucket lists them and ``ls`` on a
    batch lists its CSVs, both returning the key form s3fs uses (no scheme, bucket first).
    """

    def __init__(self, root: pathlib.Path):
        self.root = root
        self.listings = 0

    def _local(self, key: str) -> pathlib.Path:
        relative = str(key).removeprefix(BUCKET).lstrip("/")
        return self.root / relative if relative else self.root

    def ls(self, path: str, detail: bool = False) -> list[str]:
        self.listings += 1
        local = self._local(path)
        if not local.is_dir():
            return []
        prefix = str(path).rstrip("/")
        return sorted(f"{prefix}/{child.name}" for child in local.iterdir())

    def get(self, key: str, local: str) -> None:
        source = self._local(key)
        if not source.is_file():
            raise FileNotFoundError(key)
        pathlib.Path(local).write_bytes(source.read_bytes())


@pytest.fixture
def bucket(tmp_path):
    """A fake bucket root, plus a helper that creates a batch directory for an hour."""
    root = tmp_path / "bucket"

    def batch(when: pd.Timestamp, published: pd.Timestamp | None = None) -> pathlib.Path:
        published = published if published is not None else when + pd.Timedelta(75, "min")
        window_end = when + pd.Timedelta(59, "min")
        name = f"{published:%Y%m%d%H%M}_{when:%Y%m%d%H%M}_{window_end:%Y%m%d%H%M}"
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    root.mkdir(parents=True, exist_ok=True)
    batch.root = root  # type: ignore[attr-defined]
    return batch


@pytest.fixture
def provider(bucket, monkeypatch, local_config):
    """A provider whose filesystem is the fake bucket."""
    made = UKMarineObservationsProvider(config=local_config)
    fake = FakeS3(bucket.root)
    monkeypatch.setattr(made, "filesystem", lambda: fake)
    made.fake = fake  # type: ignore[attr-defined]
    return made


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


def test_station_slug_strips_the_per_file_uuid():
    """The UUID changes between batches, so it cannot be part of the station identity."""
    assert station_slug("brittany_buoy_11d94ae4-fa21-42e0-9a38-72fe5595213a.csv") == BUOY
    key = f"{BUCKET}/202609220815_x_y/{BUOY}_4d4f66e6-7fe6-400f-8f43-52732805938c.csv"
    assert station_slug(key) == BUOY


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('{"good": []}', 0),
        ('{"suspect": ["qc range check failed"]}', 1),
        ('{"bad": ["sensor offline"]}', 2),
        # An object naming several verdicts must not read cleaner than its worst check.
        ('{"good": [], "suspect": ["x"]}', 1),
        ('{"something_new": []}', 3),
    ],
    ids=["good", "suspect", "bad", "worst-wins", "unknown-verdict"],
)
def test_qc_code_reduces_a_verdict_object(value, expected):
    assert qc_code(value) == expected


@pytest.mark.parametrize(
    "value", ["", None, float("nan"), "{}"], ids=["empty", "none", "nan", "empty-object"]
)
def test_an_absent_quality_flag_is_not_good(value):
    """"We did not check" must not be stored as "we checked and it passed"."""
    assert np.isnan(qc_code(value))


def test_qc_code_survives_a_malformed_flag():
    assert qc_code("not json at all") == QC_FLAG_VALUES["unknown"]


def test_spectrum_values_shape_and_sentinel():
    values = spectrum_values(spectrum_json())

    assert values.shape == (len(SPECTRUM_VARIABLES), SPECTRUM_BANDS)
    # energy_field is the second field; band 1 carries 0.5 * 1.
    assert values[1, 0] == pytest.approx(0.5)
    # The -2 sentinel in the last band is "not measured", not a coefficient of -2.
    assert np.isnan(values[2, SPECTRUM_BANDS - 1])


@pytest.mark.parametrize(
    "value", ["", None, "not json", "[]"], ids=["empty", "none", "junk", "list"]
)
def test_a_platform_with_no_spectrum_reports_none(value):
    assert spectrum_values(value) is None


def test_read_station_csv_indexes_by_time_and_encodes_the_flags(tmp_path):
    path = write_station_csv(tmp_path, BUOY, HOUR, flag='{"suspect": ["x"]}')
    frame = read_station_csv(path)

    assert list(frame.index) == [HOUR]
    assert frame.index.tz is None, "the store's time axis is naive UTC"
    assert frame["pressure_marine"].iloc[0] == pytest.approx(1027.79)
    assert frame["pressure_marine_qc"].iloc[0] == QC_FLAG_VALUES["suspect"]


def test_read_station_csv_drops_rows_with_an_unparseable_time(tmp_path):
    path = write_station_csv(tmp_path, BUOY, HOUR)
    text = path.read_text().replace(HOUR.strftime("%Y-%m-%dT%H:%M:%SZ"), "not-a-time")
    path.write_text(text)

    assert read_station_csv(path).empty


# --------------------------------------------------------------------------------------
# The roster
# --------------------------------------------------------------------------------------


def test_the_station_axis_is_unique_and_sorted():
    """It keys the station dimension, so a duplicate would silently drop a platform."""
    assert len(set(CANONICAL_IDS)) == len(CANONICAL_IDS)
    assert list(CANONICAL_IDS) == sorted(CANONICAL_IDS)


def test_every_station_has_a_display_name():
    assert all(slug and name for slug, name in STATIONS)


def test_the_variable_list_pairs_every_measurement_with_a_flag():
    for name in MEASUREMENT_VARIABLES:
        assert name in VARIABLES
        assert f"{name}_qc" in VARIABLES


# --------------------------------------------------------------------------------------
# Finding and fetching a batch
# --------------------------------------------------------------------------------------


def test_batch_prefix_matches_on_the_window_not_the_publication_time(provider, bucket):
    """The publication timestamp drifts, so it cannot be used to construct the key."""
    bucket(HOUR, published=HOUR + pd.Timedelta(97, "min"))

    prefix = provider.batch_prefix(HOUR)

    assert prefix is not None
    assert prefix.endswith(f"{HOUR:%Y%m%d%H%M}_{HOUR + pd.Timedelta(59, 'min'):%Y%m%d%H%M}")


def test_batch_prefix_is_none_for_an_hour_that_aged_out(provider, bucket):
    bucket(HOUR)
    assert provider.batch_prefix(HOUR - pd.Timedelta(30, "D")) is None


def test_a_republished_hour_takes_the_latest_batch(provider, bucket):
    """A correction is published as a second batch over the same window."""
    first = bucket(HOUR, published=HOUR + pd.Timedelta(75, "min"))
    second = bucket(HOUR, published=HOUR + pd.Timedelta(6, "h"))
    write_station_csv(first, BUOY, HOUR, pressure=900.0)
    write_station_csv(second, BUOY, HOUR, pressure=1000.0)

    assert provider.batch_prefix(HOUR).endswith(second.name)


def test_fetch_downloads_the_batch(provider, bucket, tmp_path):
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR)
    write_station_csv(directory, SHIP, HOUR)

    found = provider.fetch(HOUR, temp_dir=tmp_path / "scratch")

    assert len(found) == 2
    assert {station_slug(p) for p in found} == {BUOY, SHIP}


def test_fetch_of_an_unpublished_hour_is_nothing_to_do(provider, tmp_path):
    assert provider.fetch(HOUR, temp_dir=tmp_path / "scratch") == []


def test_fetch_ignores_non_csv_objects(provider, bucket, tmp_path):
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR)
    (directory / "manifest.json").write_text("{}")

    assert len(provider.fetch(HOUR, temp_dir=tmp_path / "scratch")) == 1


# --------------------------------------------------------------------------------------
# Gridding
# --------------------------------------------------------------------------------------


def test_process_builds_the_full_station_axis(provider, bucket, tmp_path):
    """Two platforms reported; all fifty-eight must appear, the rest NaN."""
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR, pressure=1000.0)
    write_station_csv(directory, SHIP, HOUR, pressure=1010.0)

    ds = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)

    assert ds.sizes == {"time": 1, "station": len(CANONICAL_IDS), "frequency_band": SPECTRUM_BANDS}
    assert list(ds["station"].values) == list(CANONICAL_IDS)
    assert ds["pressure_marine"].sel(station=BUOY).item() == pytest.approx(1000.0)
    assert np.isnan(ds["pressure_marine"].sel(station=CANONICAL_IDS[-1]).item())


def test_position_is_a_data_variable_not_a_coordinate(provider, bucket, tmp_path):
    """Most of the network is under way, so a fixed station coordinate would be a lie."""
    directory = bucket(HOUR)
    write_station_csv(directory, SHIP, HOUR, longitude=-30.5, latitude=45.1)

    ds = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)

    assert "longitude" in ds.data_vars
    assert "latitude" in ds.data_vars
    assert ds["longitude"].dims == ("time", "station")
    assert ds["longitude"].sel(station=SHIP).item() == pytest.approx(-30.5)


def test_the_variable_set_does_not_depend_on_who_reported(provider, bucket, tmp_path):
    """A store's variables are pinned by its first write; they must not vary per hour."""
    quiet = bucket(HOUR)
    write_station_csv(quiet, SHIP, HOUR, spectrum=None)
    without = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "a"), HOUR)

    later = HOUR + pd.Timedelta(1, "h")
    write_station_csv(bucket(later), BUOY, later, spectrum=spectrum_json())
    with_spectrum = provider.process(provider.fetch(later, temp_dir=tmp_path / "b"), later)

    assert set(without.data_vars) == set(with_spectrum.data_vars)
    assert set(SPECTRUM_VARIABLES) <= set(without.data_vars)
    assert bool(without[SPECTRUM_VARIABLES[0]].isnull().all())


def test_the_wave_spectrum_lands_on_the_right_platform(provider, bucket, tmp_path):
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR, spectrum=spectrum_json(energy=2.0))
    write_station_csv(directory, SHIP, HOUR, spectrum=None)

    ds = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)
    energy = ds["wave_spectrum_energy_field"]

    assert energy.sel(station=BUOY).isel(time=0).values[0] == pytest.approx(2.0)
    assert bool(energy.sel(station=SHIP).isnull().all())
    assert list(ds["frequency_band"].values) == list(range(1, SPECTRUM_BANDS + 1))


def test_a_platform_outside_the_roster_is_dropped(provider, bucket, tmp_path):
    """Widening the station axis would be refused by every later append."""
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR)
    write_station_csv(directory, "brand_new_buoy", HOUR)

    ds = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)

    assert list(ds["station"].values) == list(CANONICAL_IDS)
    assert "brand_new_buoy" not in set(ds["station"].values)


def test_a_batch_with_no_usable_rows_is_retried_rather_than_recorded(provider, bucket, tmp_path):
    directory = bucket(HOUR)
    write_station_csv(directory, "brand_new_buoy", HOUR)

    with pytest.raises(NoStationDataError):
        provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)


def test_the_quality_flags_are_written_alongside_their_measurement(provider, bucket, tmp_path):
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR, flag='{"suspect": ["range check"]}')

    ds = provider.process(provider.fetch(HOUR, temp_dir=tmp_path / "scratch"), HOUR)

    assert ds["pressure_marine_qc"].sel(station=BUOY).item() == QC_FLAG_VALUES["suspect"]
    assert ds.attrs["qc_flag_meanings"] == "good suspect bad unknown"
    assert ds.attrs["qc_flag_values"] == "0 1 2 3"


# --------------------------------------------------------------------------------------
# The store
# --------------------------------------------------------------------------------------


def test_run_partition_writes_and_is_idempotent(provider, bucket):
    directory = bucket(HOUR)
    write_station_csv(directory, BUOY, HOUR, pressure=1002.0)

    assert provider.run_partition(HOUR) is True
    assert provider.run_partition(HOUR) is False

    ds = stored(provider)
    assert ds.sizes["time"] == 1
    assert pd.Timestamp(ds["time"].values[0]) == HOUR
    assert float(ds["pressure_marine"].sel(station=BUOY).values[0]) == pytest.approx(1002.0)


def test_run_partition_appends_the_next_hour(provider, bucket):
    """The station axis is identical between hours, which is what lets the append land."""
    later = HOUR + pd.Timedelta(1, "h")
    write_station_csv(bucket(HOUR), SHIP, HOUR, longitude=-40.0)
    write_station_csv(bucket(later), SHIP, later, longitude=-39.5)

    assert provider.run_partition(HOUR) is True
    assert provider.run_partition(later) is True

    ds = stored(provider)
    assert ds.sizes["time"] == 2
    assert pd.DatetimeIndex(ds["time"].values).is_monotonic_increasing
    # The ferry moved between the two hours, which is the point of storing position.
    assert list(ds["longitude"].sel(station=SHIP).values) == pytest.approx([-40.0, -39.5])


def test_an_hour_the_bucket_never_published_is_not_recorded(provider):
    assert provider.run_partition(HOUR) is False


def test_a_tz_aware_partition_start_matches_the_stored_hour(provider, bucket):
    """Dagster hands the partition start over tz-aware; the store's axis is naive UTC."""
    write_station_csv(bucket(HOUR), BUOY, HOUR)

    assert provider.run_partition(HOUR.tz_localize("UTC")) is True
    assert pd.Timestamp(stored(provider)["time"].values[0]) == HOUR
