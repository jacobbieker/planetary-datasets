"""MetOp GOME-2: products read by harp, shaped to match the store."""

from __future__ import annotations

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
        lambda name, operations="": products[name],
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


def test_each_product_is_cut_to_the_window_as_it_is_read(monkeypatch):
    """Only the hour's rows of each orbit are held: whole orbits peaked at 37 GB.

    harp is asked for the window (plus a margin) on import, and each product is cut to
    the exact window before the next is read, so concatenation never sees other rows.
    """
    import harp

    asked = []
    orbits = {
        # An orbit running into the hour, one inside it, one after it.
        "a.nat": gome_product("2024-02-29T03:59:58", rows=5, orbit=1),
        "b.nat": gome_product("2024-02-29T04:30:00", rows=3, orbit=2),
        "c.nat": None,
    }

    def fake_harp(name, operations=""):
        asked.append(operations)
        if orbits[name] is None:
            raise harp.NoDataError("no rows")
        return orbits[name]

    monkeypatch.setattr("planetary_datasets.providers.polar.metop_gome.harp_to_dataset", fake_harp)
    gome = MetopGomeProvider()
    monkeypatch.setattr(gome, "iter_natives", lambda *a, **k: iter(orbits))
    concatenated = []
    real = gome.concat_granules
    monkeypatch.setattr(gome, "concat_granules", lambda ds: concatenated.extend(ds) or real(ds))

    ds = gome.process(list(orbits), pd.Timestamp("2024-02-29T04:00"))

    epoch = pd.Timestamp("2000-01-01")
    low = (pd.Timestamp("2024-02-29T03:59") - epoch).total_seconds()
    high = (pd.Timestamp("2024-02-29T05:01") - epoch).total_seconds()
    unit = "[s since 2000-01-01]"
    window = f"datetime >= {low:.0f} {unit}; datetime < {high:.0f} {unit}"
    assert asked == [window] * 3
    assert [g.sizes["time"] for g in concatenated] == [3, 3]
    assert ds.sizes["time"] == 6
    assert ds["time"].values.min() == np.datetime64("2024-02-29T04:00:00")
