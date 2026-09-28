"""GMGSI provider behaviour, exercised offline against synthetic source files."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers import gmgsi
from planetary_datasets.providers.gmgsi import (
    V1_CHANNELS,
    V3_CHANNELS,
    Channel,
    GMGSILegacyProvider,
    GMGSIProvider,
    rewrite_store_skipping_bad_chunks,
)

TIME = pd.Timestamp("2026-01-02T03:00")


def _source_file(tmp_path, channel: Channel, time=TIME, with_dqf=True, name=None):
    """Write a miniature stand-in for one GMGSI channel file."""
    data = np.arange(2 * 3, dtype="float32").reshape(1, 2, 3)
    variables = {"data": (("time", "yc", "xc"), data)}
    if with_dqf:
        dqf = np.zeros((1, 2, 3), dtype="float32")
        dqf[0, 0, 0] = np.nan
        variables["dqf"] = (("time", "yc", "xc"), dqf)
    # A scalar byte variable the real files carry and the provider must drop.
    variables["quality_information"] = ((), np.bytes_(b"x"))

    ds = xr.Dataset(
        variables,
        coords={
            "time": pd.DatetimeIndex([time]),
            "lat": (("yc", "xc"), np.full((2, 3), 10.0, dtype="float32")),
            "lon": (("yc", "xc"), np.full((2, 3), 20.0, dtype="float32")),
        },
    )
    filename = name or f"{channel.stem}_v3r0_blend_s{time.strftime('%Y%m%d%H')}0000_e0_c0.nc"
    path = tmp_path / filename
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture
def v3_files(tmp_path):
    return [_source_file(tmp_path, channel) for channel in V3_CHANNELS]


@pytest.fixture
def provider(local_config):
    return GMGSIProvider(config=local_config)


def test_store_path_resolves_through_config(provider, tmp_path):
    assert provider.store_path.startswith(str(tmp_path))
    assert "s3://" not in provider.store_path


def test_legacy_store_is_a_different_prefix(local_config):
    assert GMGSILegacyProvider(config=local_config).store_prefix != GMGSIProvider.store_prefix


def test_process_merges_channels_as_uint8(provider, v3_files):
    ds = provider.process(v3_files, TIME)

    assert set(ds.data_vars) == {
        "vis",
        "wv",
        "lwir",
        "swir",
        "vis_dqf",
        "wv_dqf",
        "lwir_dqf",
        "swir_dqf",
    }
    assert all(ds[var].dtype == np.uint8 for var in ds.data_vars)
    assert "quality_information" not in ds.variables
    assert ds["time"].values[0] == np.datetime64(TIME)


def test_process_renames_lat_lon_and_keeps_them_two_dimensional(provider, v3_files):
    ds = provider.process(v3_files, TIME)

    assert "latitude" in ds.coords and "longitude" in ds.coords
    assert "lat" not in ds.coords and "lon" not in ds.coords
    assert ds["latitude"].dims == ("yc", "xc")
    assert ds["latitude"].dtype == np.float32


def test_process_fills_missing_quality_flags_with_255(provider, v3_files):
    ds = provider.process(v3_files, TIME)
    assert ds["vis_dqf"].values[0, 0, 0] == 255


def test_process_rejects_an_empty_input_list(provider):
    with pytest.raises(ValueError):
        provider.process([], TIME)


def test_process_rejects_an_unrecognised_filename(provider, tmp_path):
    path = _source_file(tmp_path, V3_CHANNELS[0], name="NOT_A_MOSAIC.nc")
    with pytest.raises(ValueError, match="unrecognised"):
        provider.process([path], TIME)


def test_legacy_process_has_no_quality_flags(local_config, tmp_path):
    legacy = GMGSILegacyProvider(config=local_config)
    files = [
        _source_file(
            tmp_path,
            channel,
            with_dqf=False,
            name=f"{channel.stem}_nc.{TIME.strftime('%Y%m%d%H')}",
        )
        for channel in V1_CHANNELS
    ]

    ds = legacy.process(files, TIME)

    assert set(ds.data_vars) == {"vis", "ssr", "wv", "lwir", "swir"}
    assert not any(var.endswith("_dqf") for var in ds.data_vars)


def test_key_patterns_point_at_the_public_bucket():
    v3 = GMGSIProvider().key_pattern(V3_CHANNELS[2], TIME)
    v1 = GMGSILegacyProvider().key_pattern(V1_CHANNELS[0], TIME)

    assert v3 == "noaa-gmgsi-pds/GMGSI_LW/2026/01/02/03/GLOBCOMPLIR_v3r0_blend_s2026010203*"
    assert v1 == "noaa-gmgsi-pds/GMGSI_VIS/2026/01/02/03/GLOBCOMPVIS_nc.2026010203"


def test_fetch_before_the_archive_starts_returns_nothing(provider):
    assert provider.fetch(pd.Timestamp("2020-01-01T00:00")) == []


def test_fetch_returns_nothing_when_a_channel_is_missing(provider, monkeypatch, tmp_path):
    class OneChannelOnly:
        def glob(self, pattern):
            return ["noaa-gmgsi-pds/x/GLOBCOMPVIS_v3r0_blend_s1_e1_c1.nc"] if "VIS" in pattern else []

        def get(self, key, dest):
            (tmp_path / "downloaded").write_text(key)

    monkeypatch.setattr("planetary_datasets.providers.gmgsi._anon_s3", OneChannelOnly)
    assert provider.fetch(TIME, temp_dir=tmp_path) == []


def test_fetch_downloads_one_file_per_channel(provider, monkeypatch, tmp_path):
    class FakeS3:
        def __init__(self):
            self.fetched: list[str] = []

        def glob(self, pattern):
            stem = pattern.split("/")[-1].split("_")[0]
            # Two creation times for the same hour; the newest must win.
            return [f"bucket/{stem}_v3r0_blend_s1_e1_c1.nc", f"bucket/{stem}_v3r0_blend_s1_e1_c2.nc"]

        def get(self, key, dest):
            self.fetched.append(key)
            open(dest, "wb").write(b"payload")

    fake = FakeS3()
    monkeypatch.setattr("planetary_datasets.providers.gmgsi._anon_s3", lambda: fake)

    files = provider.fetch(TIME, temp_dir=tmp_path)

    assert len(files) == len(V3_CHANNELS)
    assert all(key.endswith("_c2.nc") for key in fake.fetched)
    assert all(path.endswith("_c2.nc") for path in files)


def test_fetch_skips_files_already_on_disk(provider, monkeypatch, tmp_path):
    calls: list[str] = []

    class FakeS3:
        def glob(self, pattern):
            stem = pattern.split("/")[-1].split("_")[0]
            return [f"bucket/{stem}_v3r0_blend_s1_e1_c1.nc"]

        def get(self, key, dest):
            calls.append(key)
            open(dest, "wb").write(b"payload")

    monkeypatch.setattr("planetary_datasets.providers.gmgsi._anon_s3", FakeS3)

    provider.fetch(TIME, temp_dir=tmp_path)
    provider.fetch(TIME, temp_dir=tmp_path)

    assert len(calls) == len(V3_CHANNELS)


def test_fetch_does_not_leave_partial_files_behind(provider, monkeypatch, tmp_path):
    class BrokenS3:
        def glob(self, pattern):
            return ["bucket/GLOBCOMPVIS_v3r0_blend_s1_e1_c1.nc"]

        def get(self, key, dest):
            open(dest, "wb").write(b"half")
            raise OSError("connection reset")

    monkeypatch.setattr("planetary_datasets.providers.gmgsi._anon_s3", BrokenS3)

    assert provider.fetch(TIME, temp_dir=tmp_path) == []
    assert list(tmp_path.rglob("*.part")) == []
    assert list(tmp_path.rglob("*.nc")) == []


def test_run_partition_writes_then_skips(provider, monkeypatch, v3_files):
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: v3_files)

    assert provider.run_partition(TIME) is True
    assert provider.run_partition(TIME) is False

    stored = xr.open_zarr(provider.get_icechunk_repo().readonly_session("main").store, consolidated=False)
    assert pd.Timestamp(stored["time"].values[0]) == TIME


def test_timestamps_are_clipped_to_the_archive_start(provider):
    times = provider.timestamps(start="2020-01-01", end="2025-03-10T20:00")
    assert times[0] == pd.Timestamp("2025-03-10T17:00")
    assert len(times) == 4


def test_rewrite_store_copies_every_readable_timestep(tmp_path):
    source = tmp_path / "source.zarr"
    ds = xr.Dataset(
        {"vis": (("time", "x"), np.arange(6, dtype="uint8").reshape(3, 2))},
        coords={"time": pd.date_range("2026-01-01", periods=3, freq="1h"), "x": [0, 1]},
    )
    ds.chunk({"time": 1}).to_zarr(source, consolidated=False)

    failed = rewrite_store_skipping_bad_chunks(source, tmp_path / "destination.zarr", max_workers=1)

    assert failed == []
    copied = xr.open_zarr(tmp_path / "destination.zarr", consolidated=False)
    assert np.array_equal(copied["vis"].values, ds["vis"].values)


def test_rewrite_store_drops_unreadable_timesteps(tmp_path, monkeypatch):
    source = tmp_path / "source.zarr"
    times = pd.date_range("2026-01-01", periods=3, freq="1h")
    ds = xr.Dataset(
        {"vis": (("time", "x"), np.arange(6, dtype="uint8").reshape(3, 2))},
        coords={"time": times, "x": [0, 1]},
    )
    ds.chunk({"time": 1}).to_zarr(source, consolidated=False)

    real_reads = gmgsi._timestep_reads
    monkeypatch.setattr(
        gmgsi, "_timestep_reads", lambda src, i: False if i == 1 else real_reads(src, i)
    )

    failed = rewrite_store_skipping_bad_chunks(source, tmp_path / "destination.zarr", max_workers=1)

    assert failed == [1]
    copied = xr.open_zarr(tmp_path / "destination.zarr", consolidated=False)
    # The corrupt step is absent rather than present as an all-zero image.
    assert list(pd.DatetimeIndex(copied["time"].values)) == [times[0], times[2]]
    assert np.array_equal(copied["vis"].values, ds["vis"].values[[0, 2]])


def test_rewrite_store_refuses_a_store_it_cannot_read_at_all(tmp_path, monkeypatch):
    source = tmp_path / "source.zarr"
    xr.Dataset(
        {"vis": (("time", "x"), np.zeros((2, 2), dtype="uint8"))},
        coords={"time": pd.date_range("2026-01-01", periods=2, freq="1h"), "x": [0, 1]},
    ).chunk({"time": 1}).to_zarr(source, consolidated=False)

    monkeypatch.setattr(gmgsi, "_timestep_reads", lambda src, i: False)

    with pytest.raises(ValueError, match="no readable timesteps"):
        rewrite_store_skipping_bad_chunks(source, tmp_path / "destination.zarr", max_workers=1)


def test_hierarchical_concat_rejects_too_few_tiles():
    from planetary_datasets.providers.himawari_h9_tiles import hierarchical_concat_h9

    with pytest.raises(ValueError, match="at least"):
        hierarchical_concat_h9([xr.Dataset()] * 10)


def test_hierarchical_concat_rejects_an_unknown_resolution():
    from planetary_datasets.providers.himawari_h9_tiles import hierarchical_concat_h9

    with pytest.raises(ValueError, match="resolution"):
        hierarchical_concat_h9([xr.Dataset()] * 100, resolution="020")


def test_asset_fails_rather_than_marking_an_empty_hour_materialised(local_config, monkeypatch):
    """An hour with no data must go red so Dagster retries it, not green and forgotten."""
    import logging
    from types import SimpleNamespace

    import dagster as dg

    from dags.assets import gmgsi as gmgsi_assets

    provider = GMGSIProvider(config=local_config)
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: [])
    context = SimpleNamespace(
        partition_time_window=SimpleNamespace(start=pd.Timestamp("2026-01-02T03:00", tz="UTC")),
        log=logging.getLogger("test"),
    )

    with pytest.raises(dg.Failure):
        gmgsi_assets._materialize(context, provider)


def test_asset_succeeds_when_the_hour_is_already_stored(local_config, monkeypatch, v3_files):
    import logging
    from types import SimpleNamespace

    from dags.assets import gmgsi as gmgsi_assets

    provider = GMGSIProvider(config=local_config)
    monkeypatch.setattr(provider, "fetch", lambda it, temp_dir=None, **kw: v3_files)
    context = SimpleNamespace(
        partition_time_window=SimpleNamespace(start=TIME.tz_localize("UTC")),
        log=logging.getLogger("test"),
    )

    assert gmgsi_assets._materialize(context, provider).metadata["written"].value is True
    assert gmgsi_assets._materialize(context, provider).metadata["written"].value is False
