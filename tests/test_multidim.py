"""Appending partitions along several dimensions of one store."""

import icechunk
import numpy as np
import pytest
import xarray as xr

from planetary_datasets.common.lossless import LossyCastError
from planetary_datasets.common.multidim import append_partition, ledger


@pytest.fixture
def repo(tmp_path):
    return icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path / "s")))


def partition(n_wind: int, n_meas: int, offset: float = 0.0, product: str = "p0") -> xr.Dataset:
    return xr.Dataset(
        {
            "wind_time": ("wind", offset + np.arange(n_wind, dtype="float64") * 0.1),
            "wind_velocity": ("wind", np.arange(n_wind, dtype="int64")),
            "wind_product": ("wind", np.zeros(n_wind, dtype="int64")),
            "meas_map": (("meas", "bin"), np.ones((n_meas, 3), dtype="uint32")),
            "product_name": ("product", np.array([product])),
        },
        attrs={"Sources": np.array([["a", "b"]]), "mission": "test"},
    )


def read(repo) -> xr.Dataset:
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False).load()


def test_partitions_grow_every_dimension(repo):
    assert append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    assert append_partition(repo, partition(3, 5, 10.0, "p1"), "p1", fixed_dims=["bin"])
    ds = read(repo)
    assert dict(ds.sizes) == {"wind": 7, "meas": 7, "bin": 3, "product": 2}
    assert list(ds["product_name"].values) == ["p0", "p1"]
    np.testing.assert_array_equal(ds["wind_time"].values[4:], 10.0 + np.arange(3) * 0.1)
    assert ledger(repo) == {"p0", "p1"}


def test_dtypes_are_narrowed_losslessly(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    ds = read(repo)
    assert ds["wind_velocity"].dtype == np.int8
    assert ds["wind_time"].dtype == np.float64
    assert ds["meas_map"].dtype == np.uint8
    # Array attributes are kept as (nested) lists, which zarr stores as JSON.
    assert ds.attrs == {"Sources": [["a", "b"]], "mission": "test", "partitions_written": ["p0"]}


def test_a_written_partition_is_skipped(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    assert not append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    assert read(repo).sizes["wind"] == 4


def test_a_value_the_store_dtype_cannot_hold_is_refused(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    wide = partition(2, 1, product="p1")
    wide["wind_velocity"] = ("wind", np.array([1, 100000], dtype="int64"))
    with pytest.raises(LossyCastError):
        append_partition(repo, wide, "p1", fixed_dims=["bin"])
    assert ledger(repo) == {"p0"}


def test_a_changed_fixed_dimension_is_refused(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    other = partition(1, 1, product="p1")
    other["meas_map"] = (("meas", "bin"), np.ones((1, 4), dtype="uint32"))
    with pytest.raises(ValueError, match="fixed dimension"):
        append_partition(repo, other, "p1", fixed_dims=["bin"])
