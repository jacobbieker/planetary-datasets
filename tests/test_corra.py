"""CORRA providers, exercised offline with synthetic swaths.

The live download leg needs NASA PPS and Earthdata accounts, so it is not exercised here;
what is checked is that the credential requirement fails loudly, and that the swath-to-store
conversion and the day-level skip logic behave.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from helpers import read_store
from planetary_datasets.config import MissingCredential, load_config
from planetary_datasets.providers import corra


def make_swath(start="2026-01-01T00:00", n=6, cross_track=3, step_seconds=60):
    """A dataset shaped like a CORRA granule: values on (along_track, cross_track)."""
    times = pd.date_range(start, periods=n, freq=f"{step_seconds}s")
    return xr.Dataset(
        {
            "precipTotRate": (
                ("along_track", "cross_track"),
                np.arange(n * cross_track, dtype="float32").reshape(n, cross_track),
            )
        },
        coords={
            "along_track": np.arange(n),
            "cross_track": np.arange(cross_track),
            "time": ("along_track", times),
            "lat": (("along_track", "cross_track"), np.zeros((n, cross_track), dtype="float32")),
        },
    )


# --- credentials ----------------------------------------------------------------------


def test_configure_gpm_without_credentials_raises(local_config):
    with pytest.raises(MissingCredential) as excinfo:
        corra.configure_gpm(local_config)
    message = str(excinfo.value)
    assert "GPM_PPS_USERNAME" in message and "GPM_PPS_PASSWORD" in message


def test_configure_gpm_requires_earthdata_too(tmp_path, monkeypatch):
    monkeypatch.setenv("GPM_PPS_USERNAME", "pps-user")
    monkeypatch.setenv("GPM_PPS_PASSWORD", "pps-pass")
    cfg = load_config(env_file=tmp_path / "nonexistent.env")

    with pytest.raises(MissingCredential) as excinfo:
        corra.configure_gpm(cfg)
    assert "EARTHDATA_USERNAME" in str(excinfo.value)


def test_configure_gpm_applies_the_config_in_process(tmp_path, monkeypatch):
    """Credentials go into the live gpm config, never into ~/.config_gpm_api.yaml."""
    gpm = pytest.importorskip("gpm")
    for name, value in {
        "GPM_PPS_USERNAME": "pps-user",
        "GPM_PPS_PASSWORD": "pps-pass",
        "EARTHDATA_USERNAME": "ed-user",
        "EARTHDATA_PASSWORD": "ed-pass",
        "PLANETARY_DATASETS_DATA_DIR": str(tmp_path / "data"),
    }.items():
        monkeypatch.setenv(name, value)
    cfg = load_config(env_file=tmp_path / "nonexistent.env")

    with gpm.config.set({}):
        base_dir = corra.configure_gpm(cfg)
        assert base_dir == tmp_path / "data" / "GPM"
        assert base_dir.is_dir()
        assert gpm.config.get("username_pps") == "pps-user"
        assert gpm.config.get("password_earthdata") == "ed-pass"


def test_gpm_base_dir_ends_in_GPM(local_config):
    """The gpm package refuses a base directory that is not named GPM."""
    assert corra.gpm_base_dir(local_config).name == "GPM"


# --- product coverage -----------------------------------------------------------------


def test_trmm_and_gpm_cover_their_own_missions(local_config):
    trmm = corra.TRMMCorraProvider(config=local_config)
    gpm_corra = corra.GPMCorraProvider(config=local_config)

    assert trmm.covers(pd.Timestamp("2005-06-01"))
    assert not trmm.covers(pd.Timestamp("2016-06-01"))
    assert not gpm_corra.covers(pd.Timestamp("2005-06-01"))
    assert gpm_corra.covers(pd.Timestamp("2020-06-01"))


def test_a_day_outside_the_mission_fetches_nothing(local_config):
    trmm = corra.TRMMCorraProvider(config=local_config)
    assert trmm.fetch(pd.Timestamp("2020-01-01")) == []


def test_products_write_to_separate_stores(local_config):
    paths = {cls(config=local_config).store_path for cls in corra.PROVIDERS.values()}
    assert len(paths) == len(corra.PROVIDERS)


# --- download failures ----------------------------------------------------------------


@pytest.fixture
def stub_gpm(monkeypatch, tmp_path):
    """Replace the gpm download/search calls so fetch can be driven offline."""
    gpm = pytest.importorskip("gpm")
    state = {"download_error": None, "found": []}

    def fake_download(**kwargs):
        if state["download_error"] is not None:
            raise state["download_error"]

    monkeypatch.setattr(gpm, "download", fake_download)
    monkeypatch.setattr(gpm, "find_files", lambda **kwargs: list(state["found"]))
    monkeypatch.setattr(corra, "configure_gpm", lambda *a, **k: tmp_path / "GPM")
    return state


def test_a_failed_download_with_nothing_on_disk_raises(local_config, stub_gpm):
    """Otherwise a backfill reports success on every day while writing nothing."""
    stub_gpm["download_error"] = RuntimeError("PPS said no")
    with pytest.raises(RuntimeError, match="no granules are on disk"):
        corra.GPMCorraProvider(config=local_config).fetch(pd.Timestamp("2020-01-01"))


@pytest.mark.parametrize(
    ("download_error", "found"),
    [
        (RuntimeError("PPS said no"), ["/archive/a.HDF5"]),
        # No error and nothing found: an ordinary gap in the record.
        (None, []),
    ],
    ids=["failed-download-uses-disk", "no-granules-is-a-gap"],
)
def test_fetch_returns_whatever_granules_are_on_disk(local_config, stub_gpm, download_error, found):
    stub_gpm.update(download_error=download_error, found=found)
    assert corra.GPMCorraProvider(config=local_config).fetch(pd.Timestamp("2020-01-01")) == found


# --- swath conversion -----------------------------------------------------------------


def test_swath_becomes_indexed_by_time():
    out = corra.swath_to_time_series(make_swath())
    assert "time" in out.dims
    assert "along_track" not in out.dims
    assert out.sizes["time"] == 6


def test_swath_is_clipped_to_the_partition_day():
    """Granules straddle midnight; without clipping, neighbouring days would overlap."""
    swath = make_swath(start="2025-12-31T23:58", n=6, step_seconds=60)
    out = corra.swath_to_time_series(swath, day=pd.Timestamp("2026-01-01"))

    times = pd.DatetimeIndex(out.time.values)
    assert len(times) == 4
    assert times.min() >= pd.Timestamp("2026-01-01")


def test_repeated_scans_at_a_granule_boundary_are_dropped():
    first = make_swath(start="2026-01-01T00:00", n=3)
    second = make_swath(start="2026-01-01T00:02", n=3)
    joined = xr.concat([first, second], dim="along_track", coords="minimal", compat="override")

    out = corra.swath_to_time_series(joined, day=pd.Timestamp("2026-01-01"))
    times = pd.DatetimeIndex(out.time.values)
    assert times.is_unique
    assert times.is_monotonic_increasing
    assert len(times) == 5


def test_swath_out_of_order_is_sorted():
    swath = make_swath()
    reversed_swath = swath.isel(along_track=slice(None, None, -1))
    out = corra.swath_to_time_series(reversed_swath)
    assert pd.DatetimeIndex(out.time.values).is_monotonic_increasing


def test_swath_with_no_scans_in_the_day_raises():
    swath = make_swath(start="2026-01-05T00:00")
    with pytest.raises(ValueError):
        corra.swath_to_time_series(swath, day=pd.Timestamp("2026-01-01"))


def test_swath_without_a_time_coordinate_raises():
    ds = xr.Dataset({"x": ("along_track", np.zeros(3))}, coords={"along_track": np.arange(3)})
    with pytest.raises(ValueError, match="no time coordinate"):
        corra.swath_to_time_series(ds)


# --- store lifecycle ------------------------------------------------------------------


class OfflineCorraProvider(corra.GPMCorraProvider):
    """The GPM provider with the PPS download replaced by in-memory granules."""

    def __init__(self, granules, **kwargs):
        super().__init__(**kwargs)
        self.granules = granules
        self.fetch_calls: list[pd.Timestamp] = []

    def fetch(self, it, temp_dir=None, **kwargs):
        self.fetch_calls.append(pd.Timestamp(it))
        day = self.granules.get(pd.Timestamp(it).normalize(), [])
        return [f"granule-{i}" for i in range(len(day))]

    def open_granule(self, filepath):
        index = int(filepath.split("-")[-1])
        return self.granules[self._current_day][index]

    def process(self, input_files, it, temp_dir=None, **kwargs):
        self._current_day = pd.Timestamp(it).normalize()
        return super().process(input_files, it, temp_dir=temp_dir, **kwargs)


@pytest.fixture
def two_days_of_granules():
    return {
        pd.Timestamp("2020-01-01"): [
            make_swath("2020-01-01T01:00"),
            make_swath("2020-01-01T03:00"),
        ],
        pd.Timestamp("2020-01-02"): [make_swath("2020-01-02T01:00")],
    }


def test_run_partition_writes_then_appends_the_next_day(local_config, two_days_of_granules):
    provider = OfflineCorraProvider(two_days_of_granules, config=local_config)

    assert provider.run_partition(pd.Timestamp("2020-01-01")) is True
    assert provider.run_partition(pd.Timestamp("2020-01-02")) is True

    stored = read_store(provider)
    times = pd.DatetimeIndex(stored.time.values)
    assert len(times) == 18
    assert times.is_monotonic_increasing
    assert pd.Timestamp("2020-01-01T01:00") in times
    assert pd.Timestamp("2020-01-02T01:00") in times


def test_a_stored_day_is_skipped_without_downloading_again(local_config, two_days_of_granules):
    """Granule times never equal the partition timestamp, so the skip must work per day."""
    provider = OfflineCorraProvider(two_days_of_granules, config=local_config)
    day = pd.Timestamp("2020-01-01")

    provider.run_partition(day)
    assert provider.run_partition(day) is False
    assert provider.fetch_calls == [day]


def test_a_day_with_no_readable_granules_raises(local_config):
    provider = OfflineCorraProvider({}, config=local_config)
    with pytest.raises(ValueError, match="no readable granules"):
        provider.process([], pd.Timestamp("2020-01-01"))
