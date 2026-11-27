"""MetOp GOME-2: products read by harp, shaped to match the store."""

from __future__ import annotations

import pathlib
import sys
import types

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from planetary_datasets.providers.polar.metop_gome import MetopGomeProvider


def gome_product(start: str, rows: int = 3, orbit: int = 7) -> xr.Dataset:
    """What harp hands back for one GOME-2 product: ``orbit_index`` has no dimension."""
    return xr.Dataset(
        {
            "datetime": ("time", pd.date_range(start, periods=rows, freq="s").values),
            "latitude": ("time", np.linspace(-10, 10, rows, dtype="float32")),
            "measured_radiances": (("time", "spectral"), np.ones((rows, 4), "float32")),
            "orbit_index": ((), np.int32(orbit)),
        }
    )


@pytest.fixture
def gome(monkeypatch):
    """A provider whose harp reads two products, one per orbit."""
    products = {
        "a.nat": gome_product("2024-02-29T04:00:00", orbit=1),
        "b.nat": gome_product("2024-02-29T04:10:00", orbit=2),
    }
    monkeypatch.setattr(
        "planetary_datasets.providers.polar.metop_gome.harp_to_dataset",
        lambda name, **kwargs: products[name],
    )
    return MetopGomeProvider()


@pytest.mark.parametrize("natives", [["a.nat"], ["a.nat", "b.nat"]])
def test_per_product_fields_run_along_time_however_many_products_an_hour_has(
    gome, monkeypatch, natives
):
    """The store holds ``orbit_index`` per row, so a lone product must not leave it scalar."""
    monkeypatch.setattr(gome, "iter_natives", lambda *a, **k: iter(natives))
    ds = gome.process(natives, pd.Timestamp("2024-02-29T04:00"))
    assert ds["orbit_index"].dims == ("time",)
    assert ds.sizes["time"] == 3 * len(natives)
    expected = np.repeat(np.arange(1, len(natives) + 1), 3)
    np.testing.assert_array_equal(ds["orbit_index"].values, expected)


@pytest.fixture
def harp_on_disk(monkeypatch):
    """A stand-in ``harp`` whose products are datasets, exported as netCDF like harp's."""
    products = {
        "a.nat": gome_product("2024-02-29T03:50:00", rows=1200, orbit=1),
        "b.nat": gome_product("2024-02-29T04:10:00", rows=2500, orbit=2),
        "c.nat": gome_product("2024-02-29T04:05:00", rows=1500, orbit=3),
    }
    harp = types.SimpleNamespace(
        import_product=lambda name: products[pathlib.Path(name).name],
        export_product=lambda product, path: product.to_netcdf(path),
    )
    monkeypatch.setitem(sys.modules, "harp", harp)
    return products


def test_products_stay_on_disk_until_the_write(harp_on_disk, monkeypatch, tmp_path):
    """Read into memory, an hour of 2.9 GB peaked at 26.7 GB through concat and sortby."""
    provider = MetopGomeProvider()
    natives = [str(tmp_path / name) for name in harp_on_disk]
    monkeypatch.setattr(provider, "iter_natives", lambda *a, **k: iter(natives))
    ds = provider.process(natives, pd.Timestamp("2024-02-29T04:00"), temp_dir=tmp_path)

    assert ds["measured_radiances"].chunks is not None, "the radiances were loaded"
    assert max(ds["measured_radiances"].chunks[0]) <= provider.chunk_measurements
    # a.nat starts ten minutes before the hour: only its rows inside the window are kept.
    assert ds.sizes["time"] == 2500 + 1500 + 1200 - 600
    times = ds["time"].values
    assert (np.diff(times) >= np.timedelta64(0, "ns")).all()
    assert times[0] == np.datetime64("2024-02-29T04:00:00")
    expected = xr.concat(list(harp_on_disk.values()), "time", data_vars="all").sortby("datetime")
    expected = expected.isel(time=expected["datetime"].values >= times[0])
    np.testing.assert_array_equal(ds["latitude"].values, expected["latitude"].values)


def test_a_product_wholly_outside_the_hour_leaves_nothing_to_write(
    harp_on_disk, monkeypatch, tmp_path
):
    """A neighbour's product is dropped before concatenation rather than carried through it."""
    provider = MetopGomeProvider()
    natives = [str(tmp_path / "a.nat")]
    monkeypatch.setattr(provider, "iter_natives", lambda *a, **k: iter(natives))
    ds = provider.process(natives, pd.Timestamp("2024-02-29T05:00"), temp_dir=tmp_path)
    assert ds.sizes.get("time", 0) == 0
