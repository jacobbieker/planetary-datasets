"""Lossless dtype narrowing and the exact-cast check."""

import numpy as np
import pytest
import xarray as xr

from planetary_datasets.common.lossless import (
    LossyCastError,
    cast_exactly,
    narrowest_dtype,
    narrowest_dtypes,
)


@pytest.mark.parametrize(
    "values, expected",
    [
        (np.array([0.5, 1.25, np.nan, -2.0]), "float16"),
        (np.array([0.1, 2.0], dtype="float32"), "float32"),
        (np.array([np.float32(0.1)], dtype="float64"), "float32"),
        (np.array([0.1, 2.0]), "float64"),
        (np.array([9999.0]), "float32"),
        (np.array([1e10]), "float32"),
        (np.array([0, 200], dtype="int64"), "int64"),
        (np.array([0, 200], dtype="uint32"), "uint32"),
        (np.array([], dtype="float64"), "float64"),
    ],
)
def test_narrowest_dtype_keeps_every_value(values, expected):
    dtype = narrowest_dtype(values)
    assert dtype == np.dtype(expected)
    np.testing.assert_array_equal(values.astype(dtype).astype(values.dtype), values)


def test_the_fill_value_must_survive_too():
    assert narrowest_dtype(np.array([0.5, 1.0]), fill_value=9.969209968386869e36) == np.float32
    assert narrowest_dtype(np.array([0.5, 1.0]), fill_value=-1.0) == np.float16


def test_times_and_strings_are_left_alone():
    ds = xr.Dataset(
        {
            "t": ("x", np.array(["2020-01-01", "2020-01-02"], dtype="datetime64[ns]")),
            "s": ("x", np.array(["a", "b"])),
            "v": ("x", np.array([1.0, 2.0])),
        }
    )
    assert narrowest_dtypes(ds) == {"v": np.dtype("float16")}


def test_cast_exactly_refuses_to_round():
    ds = xr.Dataset({"v": ("x", np.array([0.5, 0.1]))})
    with pytest.raises(LossyCastError):
        cast_exactly(ds, {"v": np.dtype("float16")})


def test_cast_exactly_casts_and_drops_the_source_encoding():
    ds = xr.Dataset({"v": ("x", np.array([0.5, 1.0]))})
    ds["v"].encoding = {"dtype": "float64", "_FillValue": -1.0}
    out = cast_exactly(ds, {"v": np.dtype("float16")})
    assert out["v"].dtype == np.float16
    assert out["v"].encoding == {}
