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
    assert ds["wind_velocity"].dtype == np.int64
    assert ds["wind_time"].dtype == np.float64
    assert ds["meas_map"].dtype == np.uint32
    # Array attributes are kept as (nested) lists, which zarr stores as JSON.
    assert ds.attrs == {"Sources": [["a", "b"]], "mission": "test", "partitions_written": ["p0"]}


def test_a_written_partition_is_skipped(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    assert not append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    assert read(repo).sizes["wind"] == 4


def test_a_value_the_store_dtype_cannot_hold_widens_it(repo):
    halves = xr.Dataset({"v": ("x", np.array([0.5, 1.0, 1.5]))})
    halves["v"].attrs["_FillValue"] = -1.0
    append_partition(repo, halves, "p0")
    assert read(repo)["v"].dtype == np.float16
    append_partition(repo, xr.Dataset({"v": ("x", np.array([0.1]), {"_FillValue": -1.0})}), "p1")
    stored = read(repo)
    assert stored["v"].dtype == np.float64
    assert stored["v"].values.tolist() == [0.5, 1.0, 1.5, 0.1]
    raw = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert raw["v"].attrs["_FillValue"] == -1.0
    assert ledger(repo) == {"p0", "p1"}


def test_a_wider_fixed_dimension_widens_the_store(repo):
    append_partition(repo, partition(4, 2), "p0", fixed_dims=["bin"])
    wider = partition(1, 1, product="p1")
    wider["meas_map"] = (("meas", "bin"), np.full((1, 4), 7, dtype="uint32"))
    append_partition(repo, wider, "p1", fixed_dims=["bin"])
    stored = read(repo)["meas_map"].values
    assert stored.shape == (3, 4)
    assert stored[2].tolist() == [7, 7, 7, 7]
    assert stored[0, :3].tolist() == [1, 1, 1]


def test_a_narrower_fixed_dimension_is_padded(repo):
    append_partition(repo, xr.Dataset({"v": (("x", "lev"), np.ones((1, 3)))}), "p0", ["lev"])
    append_partition(repo, xr.Dataset({"v": (("x", "lev"), np.full((1, 2), 2.0))}), "p1", ["lev"])
    v = read(repo)["v"].values
    assert v[1, :2].tolist() == [2.0, 2.0] and np.isnan(v[1, 2])


def other_product(n: int) -> xr.Dataset:
    return xr.Dataset({"b_value": (("b_along", "b_height"), np.full((n, 2), 0.25))})


def test_a_second_product_adds_its_own_variables(repo):
    append_partition(repo, partition(4, 2), "a/p0", fixed_dims=["bin"])
    assert append_partition(repo, other_product(3), "b/d0", fixed_dims=["b_height"])
    assert append_partition(repo, other_product(2), "b/d1", fixed_dims=["b_height"])
    assert append_partition(repo, partition(1, 1, 5.0, "p1"), "a/p1", fixed_dims=["bin"])
    ds = read(repo)
    assert ds.sizes["b_along"] == 5 and ds.sizes["wind"] == 5
    assert ds["b_value"].dtype == np.float16
    assert ds.attrs["mission"] == "test"
    assert ledger(repo) == {"a/p0", "a/p1", "b/d0", "b/d1"}


def test_a_variable_new_to_the_store_is_added_with_fill_for_earlier_rows(repo):
    append_partition(repo, partition(4, 2), "a/p0", fixed_dims=["bin"])
    extra = partition(1, 1, product="p1").assign(extra=("wind", np.array([0.5])))
    append_partition(repo, extra, "a/p1", fixed_dims=["bin"])
    values = read(repo)["extra"].values
    assert np.isnan(values[:4]).all() and values[4] == 0.5


def test_a_variable_the_partition_lacks_reads_as_fill(repo):
    append_partition(repo, partition(4, 2), "a/p0", fixed_dims=["bin"])
    append_partition(
        repo, partition(1, 1, product="p1").drop_vars("wind_velocity"), "a/p1", ["bin"]
    )
    assert read(repo).sizes["wind"] == 5


def test_appended_rows_are_not_re_encoded_with_the_stores_attributes(repo):
    """Raw values with a scale factor, and raw NaNs beside a fill value, go in unchanged."""

    def raw(values, nan_too=False):
        ds = xr.Dataset(
            {
                "angle": ("x", np.array(values, dtype="int16")),
                "level": ("x", np.array([np.nan if nan_too else 1.5, -999.0])),
            }
        )
        ds["angle"].attrs = {"scale_factor": 0.01, "add_offset": 0.0}
        ds["level"].attrs = {"_FillValue": -999.0, "scale_factor": 2.0}
        return ds

    append_partition(repo, raw([100, 200]), "p0")
    append_partition(repo, raw([300, 400], nan_too=True), "p1")
    stored = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, mask_and_scale=False
    )
    assert stored["angle"].values.tolist() == [100, 200, 300, 400]
    level = stored["level"].values
    assert level[1] == -999.0 and np.isnan(level[2]) and level[3] == -999.0
    decoded = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    np.testing.assert_allclose(decoded["angle"].values, [1.0, 2.0, 3.0, 4.0])


def test_concat_frames_pads_to_the_largest_and_records_each_size():
    from planetary_datasets.common.multidim import concat_frames

    def frame(n_layers):
        return xr.Dataset(
            {
                "p_x": (("p_along_track", "p_nlayers"), np.ones((2, n_layers), "float32")),
                "p_name": (("p_frame",), np.array(["f"], dtype=object)),
            }
        )

    ds = concat_frames([frame(3), frame(5)], "p", 0)
    assert ds.sizes["p_nlayers"] == 5
    assert ds["p_size_nlayers"].values.tolist() == [3, 5]
    assert np.isnan(ds["p_x"].values[0, 3:]).all()


def test_pad_to_pads_every_variable_on_a_dimension_together():
    from planetary_datasets.common.multidim import pad_to

    ds = xr.Dataset(
        {"a": (("x", "lev"), np.ones((1, 2))), "b": (("lev",), np.array([1, 2], "int16"))}
    )
    out = pad_to(ds, {"lev": 4})
    assert out.sizes["lev"] == 4 and out["b"].values.tolist() == [1, 2, 0, 0]
