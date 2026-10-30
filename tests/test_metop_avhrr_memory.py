"""AVHRR partitions are built from spilled swaths without changing what is written."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.polar import MetopAvhrrProvider
from planetary_datasets.providers.polar.metop_avhrr import spill_swath

IT = pd.Timestamp("2024-04-10T16:00")


def orbit(index: int, rows: int) -> xr.Dataset:
    """A small orbit shaped like ``process_granule``'s output, with NaNs and edge values."""
    rng = np.random.default_rng(index)
    channel = rng.normal(250, 30, (1, rows, 4)).astype(np.float16)
    channel[0, 0, 0] = np.nan
    channel[0, 1, 1] = np.float16(65504)
    lat = rng.uniform(-90, 90, (1, rows, 4)).astype(np.float32)
    start = IT + pd.Timedelta(minutes=30 * index)
    ds = xr.Dataset(
        {
            "4": (("time", "y", "x"), channel, {"units": "K", "wavelength": [10.3, 10.8, 11.3]}),
            "latitude": (("time", "y", "x"), lat),
            "start_time": (("time",), np.array([start], dtype="datetime64[ns]")),
            "end_time": (("time",), np.array([start + pd.Timedelta(minutes=5)], "datetime64[ns]")),
            "platform_name": (("time",), np.array([f"Metop-{'BC'[index % 2]}"], dtype=object)),
        },
        coords={"time": [start + pd.Timedelta(minutes=2)]},
    )
    ds["4"].encoding = {"dtype": np.dtype("float16")}
    return ds


@pytest.fixture
def avhrr(local_config, monkeypatch):
    provider = MetopAvhrrProvider(config=local_config)
    provider.swath_shape = {"y": 6, "x": 4}
    orbits = {f"o{i}": orbit(i, rows) for i, rows in enumerate([5, 6, 3])}
    monkeypatch.setattr(provider, "iter_natives", lambda files, temp_dir=None: iter(orbits))
    monkeypatch.setattr(provider, "process_granule", lambda name: orbits[name])
    provider.orbits = orbits
    return provider


def in_memory(provider) -> xr.Dataset:
    """The partition as the provider built it before spilling."""
    aligned = [provider.align(o) for o in provider.orbits.values()]
    ds = provider.restrict_to_window(provider.concat_granules(aligned), IT)
    return ds.chunk({"time": 1, "y": -1, "x": -1})


def test_spilled_partition_matches_the_in_memory_one(avhrr, tmp_path):
    spilled = avhrr.process(list(avhrr.orbits), IT, temp_dir=tmp_path)
    expected = in_memory(avhrr)
    assert spilled["4"].chunks is not None, "swath arrays stay lazy"
    for name in expected.variables:
        assert spilled[name].dtype == expected[name].dtype, name
        assert spilled[name].attrs == expected[name].attrs, name
        np.testing.assert_array_equal(spilled[name].values, expected[name].values)
        assert {k: v for k, v in spilled[name].encoding.items() if k != "preferred_chunks"} == {
            k: v for k, v in expected[name].encoding.items() if k != "preferred_chunks"
        }, name


def test_spilled_partition_writes_the_same_store(avhrr, tmp_path, local_config):
    spilled = avhrr.process(list(avhrr.orbits), IT, temp_dir=tmp_path / "spill")
    avhrr.write_to_icechunk(avhrr.get_icechunk_repo(), spilled)
    ours = xr.open_zarr(
        avhrr.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )

    reference = MetopAvhrrProvider(config=local_config)
    reference.store_prefix = "bkr/polar/metop_avhrr_reference.icechunk"
    reference.write_to_icechunk(reference.get_icechunk_repo(), in_memory(avhrr))
    theirs = xr.open_zarr(
        reference.get_icechunk_repo().readonly_session("main").store, consolidated=False
    )
    for name in theirs.variables:
        assert ours[name].dtype == theirs[name].dtype, name
        np.testing.assert_array_equal(ours[name].values, theirs[name].values)


def test_spill_keeps_small_variables_in_memory(tmp_path):
    ds = orbit(0, 4)
    out = spill_swath(ds, tmp_path / "s.zarr")
    assert out["latitude"].chunks is not None
    assert out["start_time"].chunks is None and out["platform_name"].chunks is None
    assert out["4"].attrs == ds["4"].attrs
