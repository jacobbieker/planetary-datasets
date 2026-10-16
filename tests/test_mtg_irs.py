"""Offline tests for the MTG IRS L1 providers.

Dwell files are synthesised with the quirks of the real ``CHK-BODY`` product: packed int16
spectra, enum and boolean flags whose ``_FillValue`` is their own zero, and a ``data/time``
whose units xarray mis-decodes.
"""

from __future__ import annotations

import contextlib
import io

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from helpers import read_store

from planetary_datasets.providers.eumetsat import mtg_irs
from planetary_datasets.providers.eumetsat.mtg_irs import (
    CollectionUnavailable,
    MtgIrsL1Lac4HourlyProvider,
    MtgIrsL1Provider,
    PackingChanged,
)
from planetary_datasets.providers.polar._eumdac import EumdacProvider

SIZE = 4
N_WAVENUMBERS = {"lwir": 5, "mwir": 6}
SECONDS_2000 = pd.Timestamp("2000-01-01")


def _seconds(stamp: str) -> float:
    return (pd.Timestamp(stamp) - SECONDS_2000).total_seconds()


def body_name(start: str, end: str, count: int = 1) -> str:
    """A ``CHK-BODY`` entry name sensed over ``[start, end]`` (``YYYYmmddHHMMSS``)."""
    return (
        "W_XX-EUMETSAT-Darmstadt,SND+SAT,MTS1+IRS-1B-SSS--Q4--CHK-BODY---NC4E_C_EUMT_"
        f"{end}_IDPFS_OPE_{start}_{end}_N__C_0004_{count:04d}.nc"
    )


def counts(band: str, seed: int) -> np.ndarray:
    """Packed radiance counts spanning the int16 range, with one fill value."""
    rng = np.random.default_rng(seed)
    values = rng.integers(-32766, 32767, (SIZE, SIZE, N_WAVENUMBERS[band]), dtype="int16")
    values[0, 0, 0] = mtg_irs.RADIANCE_FILL
    return values


def write_dwell(
    path,
    centre: str,
    dwell_number: int = 225,
    lac: int = 4,
    seed: int = 0,
    scale: dict[str, float] | None = None,
) -> str:
    """Write a dwell file shaped like the real product."""
    shape = ("dwell_row", "dwell_column")
    grid = np.linspace(30.0, 31.0, SIZE * SIZE, dtype="float32").reshape(SIZE, SIZE)
    flag = {"_FillValue": np.uint8(0), "coordinates": "latitude longitude"}
    data = xr.Dataset(
        {
            "time": ((), _seconds(centre), {"units": "seconds since 01:01:2000 00:00:00"}),
            "dwell_type": ((), np.uint8(0), {"_FillValue": np.uint8(0)}),
            "stroke_direction": ((), np.uint8(1), {"_FillValue": np.uint8(0)}),
            "dwell_number": ((), np.uint16(dwell_number)),
            "dust_warning": (shape, np.eye(SIZE, dtype="uint8"), flag),
            "latitude": (shape, grid),
            "longitude": (shape, grid - 20),
            **{
                name: (shape, np.full((SIZE, SIZE), 45.0, dtype="float32"))
                for name in mtg_irs.PIXEL_VARIABLES
                if name not in ("latitude", "longitude")
            },
        }
    )
    data.to_netcdf(path, group="data", engine="h5netcdf")
    instrument = xr.Dataset(
        {
            "repeat_cycle_lac_id": ((), np.int8(lac)),
            "repeat_cycle_start_time": ((), _seconds("2026-10-13T00:45:03")),
        }
    )
    instrument.to_netcdf(path, group="state/instrument", engine="h5netcdf", mode="a")
    for band in mtg_irs.BANDS:
        n = N_WAVENUMBERS[band]
        spectral = xr.Dataset({"wavenumber": ("wavenumber", np.arange(n, dtype="float32") + 1e5)})
        spectral.to_netcdf(path, group=f"data/{band}", engine="h5netcdf", mode="a")
        band_scale, offset = mtg_irs.RADIANCE_PACKING[band]
        if scale and band in scale:
            band_scale = scale[band]
        measured = xr.Dataset(
            {
                "effective_radiance": (
                    (*shape, "wavenumber"),
                    counts(band, seed),
                    {
                        "scale_factor": np.float32(band_scale),
                        "add_offset": np.float32(offset),
                        "_FillValue": np.int16(mtg_irs.RADIANCE_FILL),
                        "coordinates": "latitude longitude",
                    },
                ),
                "spatial_sample_quality": (shape, np.full((SIZE, SIZE), 4, dtype="int16")),
            }
        )
        # Packed already: write the counts and attributes as they are.
        measured.to_netcdf(
            path,
            group=f"data/{band}/measured",
            engine="h5netcdf",
            mode="a",
            encoding={"effective_radiance": {"dtype": "int16"}},
        )
        warnings = xr.Dataset(
            {
                name: (shape, np.zeros((SIZE, SIZE), dtype="uint8"), {"_FillValue": np.uint8(0)})
                for name in mtg_irs.BAND_WARNINGS
            }
        )
        warnings.to_netcdf(path, group=f"data/{band}/quality_band", engine="h5netcdf", mode="a")
    return str(path)


@pytest.fixture
def provider(local_config):
    return MtgIrsL1Provider(config=local_config)


@pytest.fixture
def dwells(tmp_path):
    """Two dwells in the 00Z hour and one just past it."""
    return [
        write_dwell(tmp_path / "a.nc", "2026-10-13T00:45:39", dwell_number=225, seed=1),
        write_dwell(tmp_path / "b.nc", "2026-10-13T00:52:22", dwell_number=263, seed=2),
        write_dwell(tmp_path / "c.nc", "2026-10-13T01:00:04", dwell_number=301, seed=3),
    ]


# =============================================================================
# Entries and times
# =============================================================================
def test_only_body_chunks_are_downloaded():
    assert mtg_irs.is_body_chunk(body_name("20261013005222", "20261013005231"))
    trailer = body_name("20261013004539", "20261013005925").replace("CHK-BODY", "CHK-TRAIL")
    assert not mtg_irs.is_body_chunk(trailer)
    quicklook = body_name("20261013004539", "20261013004549").replace(
        "CHK-BODY---NC4E", "QCK-IMAGE-LWIR--PNG"
    )
    assert not mtg_irs.is_body_chunk(quicklook.replace(".nc", ".png"))


def test_entry_sensing_is_read_from_the_filename():
    start, end = mtg_irs.entry_sensing(body_name("20261013005222", "20261013005231"))
    assert start == pd.Timestamp("2026-10-13T00:52:22")
    assert end == pd.Timestamp("2026-10-13T00:52:31")
    assert mtg_irs.entry_sensing("manifest.xml") is None


def test_a_dwell_across_the_hour_is_fetched_by_both_partitions():
    entry = body_name("20261013005958", "20261013010008")
    hour = pd.Timedelta(1, "h")
    zero, one = pd.Timestamp("2026-10-13T00:00"), pd.Timestamp("2026-10-13T01:00")
    assert mtg_irs.entry_overlaps(entry, zero, zero + hour)
    assert mtg_irs.entry_overlaps(entry, one, one + hour)
    assert not mtg_irs.entry_overlaps(entry, one + hour, one + 2 * hour)


def test_product_times_are_seconds_since_2000():
    decoded = mtg_irs.seconds_since_2000(845167539.0000001)
    assert decoded == np.datetime64("2026-10-13T00:45:39")


# =============================================================================
# Reading a dwell
# =============================================================================
def test_read_dwell_decodes_time_and_keeps_zero_flags(dwells):
    ds = mtg_irs.read_dwell(dwells[0])
    assert ds["time"].values[0] == np.datetime64("2026-10-13T00:45:39")
    assert ds["repeat_cycle_start_time"].values[0] == np.datetime64("2026-10-13T00:45:03")
    # Zero is EV / false, not missing.
    assert ds["dwell_type"].values.tolist() == [0]
    assert ds["dust_warning"].dtype == np.uint8
    np.testing.assert_array_equal(ds["dust_warning"].values[0], np.eye(SIZE))
    assert ds["lwir_instrument_quality_warning"].values.max() == 0
    assert ds["lac"].values.tolist() == [4]
    assert ds["dwell_number"].values.tolist() == [225]


def test_read_dwell_keeps_geolocation_as_data(dwells):
    ds = mtg_irs.read_dwell(dwells[0])
    assert set(ds.coords) == {"time", "wavenumber_lwir", "wavenumber_mwir"}
    assert ds["latitude"].dims == ("time", "y", "x")
    assert ds["latitude"].dtype == np.float32
    for name in ds.data_vars:
        assert "coordinates" not in ds[name].attrs, name


def test_read_dwell_leaves_the_spectra_lazy(dwells):
    ds = mtg_irs.read_dwell(dwells[0])
    radiance = ds["lwir_effective_radiance"]
    assert radiance.chunks is not None
    assert radiance.dims == ("time", "y", "x", "wavenumber_lwir")
    assert np.isnan(radiance.values[0, 0, 0, 0])


def test_a_differently_packed_file_is_refused(tmp_path):
    path = write_dwell(tmp_path / "d.nc", "2026-10-13T00:45:39", scale={"mwir": 1e-08})
    with pytest.raises(PackingChanged, match="mwir"):
        mtg_irs.read_dwell(path)


# =============================================================================
# Writing
# =============================================================================
def test_spectra_are_stored_as_the_source_counts(provider, dwells):
    it = pd.Timestamp("2026-10-13T00:00")
    assert provider.write_to_icechunk(provider.get_icechunk_repo(), provider.process(dwells, it))

    group = zarr.open_group(provider.get_icechunk_repo().readonly_session("main").store, mode="r")
    for band in mtg_irs.BANDS:
        stored = group[f"{band}_effective_radiance"]
        assert stored.dtype == np.int16
        np.testing.assert_array_equal(stored[0], counts(band, 1))
        np.testing.assert_array_equal(stored[1], counts(band, 2))


def test_the_partition_keeps_only_its_own_hour(provider, dwells):
    ds = provider.process(dwells, pd.Timestamp("2026-10-13T00:00"))
    assert ds.sizes["time"] == 2
    assert ds["dwell_number"].values.tolist() == [225, 263]


def test_a_dwell_fetched_twice_is_one_step(provider, dwells):
    ds = provider.process([dwells[0], dwells[0], dwells[1]], pd.Timestamp("2026-10-13T00:00"))
    assert ds.sizes["time"] == 2


def test_partitions_append_and_are_then_skipped(provider, dwells):
    repo = provider.get_icechunk_repo()
    zero, one = pd.Timestamp("2026-10-13T00:00"), pd.Timestamp("2026-10-13T01:00")
    assert provider.write_to_icechunk(repo, provider.process(dwells, zero))
    assert provider.write_to_icechunk(repo, provider.process(dwells, one))
    assert not provider.write_to_icechunk(repo, provider.process(dwells, one))

    stored = read_store(provider)
    assert stored.sizes["time"] == 3
    assert "latitude" in stored.data_vars
    np.testing.assert_allclose(
        stored["lwir_effective_radiance"].isel(time=2).values,
        mtg_irs.read_dwell(dwells[2])["lwir_effective_radiance"].values[0],
        rtol=1e-6,
    )
    assert provider.missing_timesteps(pd.DatetimeIndex([zero, one])) == []


def test_store_chunking(provider, dwells):
    ds = provider.process(dwells, pd.Timestamp("2026-10-13T00:00"))
    provider.write_to_icechunk(provider.get_icechunk_repo(), ds)
    stored = read_store(provider)
    assert stored["lwir_effective_radiance"].encoding["chunks"] == (1, SIZE, SIZE, 5)
    assert stored["latitude"].encoding["chunks"] == (1, SIZE, SIZE)


# =============================================================================
# Fetching
# =============================================================================
class _FakeProduct:
    def __init__(self, entries, fail=()):
        self.entries = list(entries)
        self.fail = set(fail)
        self.opened: list[str] = []

    @contextlib.contextmanager
    def open(self, entry):
        self.opened.append(entry)
        if entry in self.fail:
            raise ConnectionError("reset by peer")
        yield io.BytesIO(b"dwell")


def test_fetch_downloads_the_dwells_in_the_hour_only(provider, tmp_path, monkeypatch):
    inside = body_name("20261013005222", "20261013005231")
    across = body_name("20261013005958", "20261013010008")
    after = body_name("20261013010101", "20261013010111")
    trailer = inside.replace("CHK-BODY", "CHK-TRAIL")
    product = _FakeProduct([inside, across, after, trailer, "manifest.xml"])
    monkeypatch.setattr(provider, "search", lambda it: [product])

    paths = provider.fetch(pd.Timestamp("2026-10-13T00:00"), temp_dir=tmp_path)

    assert sorted(product.opened) == sorted([inside, across])
    assert len(paths) == 2
    assert not list(tmp_path.rglob("*.part"))


def test_a_dwell_that_never_downloads_fails_the_partition(provider, tmp_path, monkeypatch):
    good = body_name("20261013005222", "20261013005231")
    bad = body_name("20261013005232", "20261013005241")
    product = _FakeProduct([good, bad], fail=[bad])
    monkeypatch.setattr(provider, "search", lambda it: [product])
    monkeypatch.setattr(mtg_irs.time, "sleep", lambda seconds: None)

    with pytest.raises(ConnectionError):
        provider.fetch(pd.Timestamp("2026-10-13T00:00"), temp_dir=tmp_path)
    assert product.opened.count(bad) == provider.download_retries
    assert not list(tmp_path.rglob("*.part"))


def test_an_unsearchable_collection_fails_loudly(local_config, monkeypatch):
    from eumdac.collection import CollectionError

    def refuse(self, it):
        raise CollectionError("Could not get search options for EO:EUM:DAT:1199 - Not found")

    monkeypatch.setattr(EumdacProvider, "search", refuse)
    provider = MtgIrsL1Lac4HourlyProvider(config=local_config)
    with pytest.raises(CollectionUnavailable, match="EO:EUM:DAT:1199"):
        provider.fetch(pd.Timestamp("2026-10-13T00:00"))


# =============================================================================
# Stores and Dagster
# =============================================================================
def test_one_store_per_collection_beside_the_other_geostationary_stores():
    providers = list(mtg_irs.PROVIDERS.values())
    assert {p.collection_id for p in providers} == {"EO:EUM:DAT:0741", "EO:EUM:DAT:1199"}
    assert {p.store_prefix for p in providers} == {
        "bkr/geo/mtg_irs_l1.icechunk",
        "bkr/geo/mtg_irs_l1_lac4_hourly.icechunk",
    }


def test_dagster_partitions_match_the_provider_window():
    from dags.assets import geo_satellites

    partitions = geo_satellites.mtg_irs_partitions
    first = partitions.get_first_partition_key()
    window = partitions.time_window_for_partition_key(first)
    assert window.end - window.start == MtgIrsL1Provider.window.to_pytimedelta()
    assert window.start.replace(tzinfo=None) == mtg_irs.ARCHIVE_START
