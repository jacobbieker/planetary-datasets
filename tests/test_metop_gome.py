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
    products = {
        "a.nat": gome_product("2024-02-29T04:00:00", orbit=1),
        "b.nat": gome_product("2024-02-29T04:10:00", orbit=2),
    }
    monkeypatch.setattr(
        "planetary_datasets.providers.polar.metop_gome.harp_to_dataset", products.__getitem__
    )
    return MetopGomeProvider()


@pytest.mark.parametrize("natives", [["a.nat"], ["a.nat", "b.nat"]])
def test_per_product_fields_run_along_time_however_many_products_an_hour_has(
    gome, monkeypatch, natives
):
    monkeypatch.setattr(gome, "iter_natives", lambda *a, **k: iter(natives))
    ds = gome.process(natives, pd.Timestamp("2024-02-29T04:00"))
    assert ds["orbit_index"].dims == ("time",)
    assert ds.sizes["time"] == 3 * len(natives)
    assert list(np.unique(ds["orbit_index"].values)) == list(range(1, len(natives) + 1))
