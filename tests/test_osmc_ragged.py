"""Offline tests for the ragged OSMC stores.

The yearly source store is synthesised in the local store directory with the layout of the
published ``bkr/aoml/aoml_<year>.icechunk`` (flat, time-sorted, platform fields as
coordinates, no ``country``), and ERDDAP is replaced by a fake response.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from icechunk.xarray import to_icechunk

from planetary_datasets.providers.observations import osmc_ragged
from planetary_datasets.providers.observations.osmc import OSMC_VARIABLES
from planetary_datasets.providers.observations.osmc_ragged import (
    LEDGER_ATTR,
    OSMCRaggedProvider,
    ledger,
    normalise,
    store_prefix_for,
    type_slug,
)

TYPES = ["DRIFTING BUOYS (GENERIC)", "DRIFTING BUOYS", "SHIPS (GENERIC)", "VOSCLIM"]


def yearly_rows(start: str, days: int, per_day: int = 12) -> xr.Dataset:
    """Rows shaped like a published yearly store, sorted by time."""
    times = pd.date_range(start, periods=days * per_day, freq=f"{24 * 60 // per_day}min")
    n = times.size
    rng = np.random.default_rng(0)
    return xr.Dataset(
        {
            "sst": ("time", rng.normal(290, 2, n).astype("float32")),
            "atmp": ("time", rng.normal(285, 2, n).astype("float32")),
            "observation_depth": ("time", np.zeros(n, dtype="float32")),
        },
        coords={
            "time": times,
            "platform_type": (
                "time",
                np.array([TYPES[i % len(TYPES)] for i in range(n)], dtype=object),
            ),
            "platform_code": ("time", np.array([f"P{i % 7}" for i in range(n)], dtype=object)),
            "platform_id": ("time", (np.arange(n) % 7 + 4_000_000_000).astype("float64")),
            "latitude": ("time", np.linspace(-60, 60, n)),
            "longitude": ("time", np.linspace(-170, 170, n)),
        },
    )


@pytest.fixture
def yearly_2012(local_config):
    """A two-day yearly store for 2012 at the published prefix."""
    repo = local_config.icechunk_repo("bkr/aoml/aoml_2012.icechunk")
    session = repo.writable_session("main")
    to_icechunk(yearly_rows("2012-01-01", days=2), session)
    session.commit("yearly fixture")
    return yearly_rows("2012-01-01", days=2)


@pytest.fixture
def provider(local_config):
    return OSMCRaggedProvider(config=local_config)


def stored(config, prefix: str) -> xr.Dataset:
    repo = config.icechunk_repo(prefix)
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


# =============================================================================
# Types and schema
# =============================================================================
@pytest.mark.parametrize(
    ("label", "slug"),
    [
        ("DRIFTING BUOYS (GENERIC)", "drifting_buoys"),
        ("DRIFTING BUOYS", "drifting_buoys"),
        ("C-MAN WEATHER STATIONS", "c_man_weather_stations"),
        ("PROFILING FLOATS AND GLIDERS (GENERIC)", "profiling_floats_and_gliders"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_type_slug_joins_the_generic_spelling(label, slug):
    assert type_slug(label) == slug


def test_type_stores_sit_beside_the_full_store():
    assert store_prefix_for("drifting_buoys") == "bkr/aoml/v2/osmc_drifting_buoys.icechunk"
    assert osmc_ragged.ALL_STORE == "bkr/aoml/v2/osmc_all.icechunk"


def test_normalise_gives_every_variable_and_fixed_dtypes():
    ds = normalise(yearly_rows("2012-01-01", days=1).isel(time=slice(None, None, -1)))
    assert set(ds.data_vars) == set(OSMC_VARIABLES) - {"time"}
    assert list(ds.coords) == ["time"]
    assert bool((np.diff(ds["time"].values) >= np.timedelta64(0)).all())
    assert ds["country"].values.tolist() == [""] * ds.sizes["time"]
    assert ds["sst"].dtype == np.float32 and ds["latitude"].dtype == np.float64
    assert ds["platform_id"].dtype == np.float64
    assert np.isnan(ds["wvht"].values).all()


def test_normalise_reads_erddap_rows_with_missing_strings():
    rows = xr.Dataset(
        {
            "time": ("row", pd.to_datetime(["2016-01-01T01:00", "2016-01-01T00:00"])),
            "platform_type": ("row", np.array(["SHIPS", np.nan], dtype=object)),
            "country": ("row", np.array(["UK", None], dtype=object)),
            "sst": ("row", np.array([280.0, 281.0])),
        }
    )
    ds = normalise(rows)
    assert ds["time"].values[0] == np.datetime64("2016-01-01T00:00")
    assert ds["platform_type"].values.tolist() == ["", "SHIPS"]
    assert ds["country"].values.tolist() == ["", "UK"]


# =============================================================================
# Writing
# =============================================================================
def test_a_day_lands_in_the_full_store_and_each_type_store(provider, yearly_2012, local_config):
    assert provider.run_partition(pd.Timestamp("2012-01-01"))

    full = stored(local_config, osmc_ragged.ALL_STORE)
    assert full.sizes["time"] == 12
    assert set(np.unique(full["platform_type"].values)) == set(TYPES)
    drifting = stored(local_config, store_prefix_for("drifting_buoys"))
    assert set(np.unique(drifting["platform_type"].values)) == {
        "DRIFTING BUOYS (GENERIC)",
        "DRIFTING BUOYS",
    }
    ships = stored(local_config, store_prefix_for("ships"))
    vosclim = stored(local_config, store_prefix_for("vosclim"))
    assert drifting.sizes["time"] + ships.sizes["time"] + vosclim.sizes["time"] == 12
    np.testing.assert_allclose(np.sort(full["sst"].values), np.sort(yearly_2012["sst"].values[:12]))
    for prefix in (osmc_ragged.ALL_STORE, store_prefix_for("ships")):
        assert ledger(local_config.icechunk_repo(prefix)) == {"2012-01-01"}


def test_days_append_and_a_stored_day_is_skipped(provider, yearly_2012, local_config):
    days = pd.date_range("2012-01-01", periods=2, freq="D")
    assert provider.run_range(days) == 2
    assert provider.missing_timesteps(days) == []
    assert not provider.run_partition(pd.Timestamp("2012-01-02"))

    full = stored(local_config, osmc_ragged.ALL_STORE)
    assert full.sizes["time"] == 24
    assert bool((np.diff(full["time"].values) >= np.timedelta64(0)).all())
    assert full["platform_code"].values[0] == "P0"


def test_a_day_interrupted_after_some_type_stores_is_not_duplicated(
    provider, yearly_2012, local_config
):
    day = pd.Timestamp("2012-01-01")
    processed = provider.process(provider.fetch(day), day)
    ships = processed.isel(
        time=np.flatnonzero(processed["platform_type"].values == "SHIPS (GENERIC)")
    )
    osmc_ragged._append_with_ledger(provider.type_repo("ships"), ships, "2012-01-01", "osmc_ships")

    assert provider.run_partition(day)
    assert stored(local_config, store_prefix_for("ships")).sizes["time"] == ships.sizes["time"]
    assert stored(local_config, osmc_ragged.ALL_STORE).sizes["time"] == 12


def test_the_ledger_is_kept_in_the_store_attributes(provider, yearly_2012, local_config):
    provider.run_partition(pd.Timestamp("2012-01-01"))
    assert stored(local_config, osmc_ragged.ALL_STORE).attrs[LEDGER_ATTR] == ["2012-01-01"]


# =============================================================================
# Sources
# =============================================================================
def test_days_before_2016_come_from_the_yearly_store(provider):
    assert provider.fetch(pd.Timestamp("2015-12-31")) == ["yearly:bkr/aoml/aoml_2015.icechunk"]


def test_the_erddap_request_covers_exactly_one_day():
    url = osmc_ragged.day_url(pd.Timestamp("2016-03-01"))
    assert url.startswith(osmc_ragged.ERDDAP_FLATTENED)
    assert "time%3E=2016-03-01T00%3A00%3A00Z" in url
    assert "time%3C=2016-03-01T23%3A59%3A59Z" in url


class _Response:
    def __init__(self, status: int, body: bytes):
        self.status_code = status
        self._body = body
        self.text = body.decode("utf-8", "replace")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size):
        stream = io.BytesIO(self._body)
        while chunk := stream.read(chunk_size):
            yield chunk

    def close(self):
        pass


def test_an_erddap_day_is_fetched_and_written(provider, local_config, tmp_path, monkeypatch):
    flat = yearly_rows("2016-01-01", days=1).reset_coords()
    # ERDDAP's layout: one "row" dimension with time as an ordinary variable.
    rows = xr.Dataset({name: ("row", flat[name].values) for name in flat.variables})
    body = bytes(rows.to_netcdf())
    monkeypatch.setattr(osmc_ragged.requests, "get", lambda url, **kw: _Response(200, body))

    assert provider.run_partition(pd.Timestamp("2016-01-01"))
    assert stored(local_config, osmc_ragged.ALL_STORE).sizes["time"] == 12


def test_an_erddap_day_with_no_reports_is_empty(provider, monkeypatch, tmp_path):
    body = b'Error {\n    message="Not Found: Your query produced no matching results.";\n}'
    monkeypatch.setattr(osmc_ragged.requests, "get", lambda url, **kw: _Response(404, body))
    assert provider.fetch(pd.Timestamp("2016-01-01"), temp_dir=tmp_path) == []


def test_an_erddap_failure_raises(provider, monkeypatch, tmp_path):
    monkeypatch.setattr(osmc_ragged.requests, "get", lambda url, **kw: _Response(503, b"busy"))
    with pytest.raises(RuntimeError, match="503"):
        provider.fetch(pd.Timestamp("2016-01-01"), temp_dir=tmp_path)


def test_appended_days_hold_exactly_the_source_rows(provider, yearly_2012, local_config):
    days = pd.date_range("2012-01-01", periods=2, freq="D")
    assert provider.run_range(days) == 2

    full = stored(local_config, osmc_ragged.ALL_STORE)
    source = yearly_2012.isel(time=slice(0, 24))
    for name in source.data_vars:
        np.testing.assert_array_equal(full[name].values, source[name].values, err_msg=name)
    np.testing.assert_array_equal(full["time"].values, source["time"].values)


def test_an_append_does_not_index_the_stored_rows(provider, yearly_2012, local_config, monkeypatch):
    """Opening the store through xarray reads every stored time: minutes at full size."""
    assert provider.run_partition(pd.Timestamp("2012-01-01"))

    def refuse(*args, **kwargs):
        raise AssertionError("an append opened the whole store through xarray")

    monkeypatch.setattr(xr, "open_zarr", refuse)
    monkeypatch.setattr(xr, "open_dataset", refuse)
    assert provider.run_partition(pd.Timestamp("2012-01-02"))
    monkeypatch.undo()
    assert stored(local_config, osmc_ragged.ALL_STORE).sizes["time"] == 24
