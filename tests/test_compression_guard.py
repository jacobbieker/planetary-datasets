"""Every array the shared writers create is compressed."""

from __future__ import annotations

import icechunk
import numpy as np
import xarray as xr
import zarr

from planetary_datasets.common import multidim
from planetary_datasets.common.store import build_encoding

COMPRESSORS = ("blosc", "zstd", "gzip", "lz4")


def _codecs(array) -> list[str]:
    names = []
    for codec in array.metadata.to_dict()["codecs"]:
        names.append(codec["name"])
        if codec["name"] == "sharding_indexed":
            names += [c["name"] for c in codec["configuration"]["codecs"]]
    return names


def _uncompressed(store) -> list[str]:
    def walk(group, prefix=""):
        for name, array in group.arrays():
            yield prefix + name, array
        for name, sub in group.groups():
            yield from walk(sub, prefix + name + "/")

    return [
        name
        for name, array in walk(zarr.open_group(store, mode="r"))
        if not any(any(c in n for c in COMPRESSORS) for n in _codecs(array))
    ]


def test_multidim_stores_compress_every_array_including_ones_added_later(tmp_path):
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(str(tmp_path)))
    first = xr.Dataset(
        {
            "value": ("obs", np.array([1.5, 2.5])),
            "count": ("obs", np.array([1, 2], dtype="int32")),
            "name": ("obs", np.array(["a", "b"], dtype=object)),
        }
    )
    multidim.append_partition(repo, first, "p1")
    later = first.assign(
        extra=("obs", np.array([0.25, 0.75])),
        label=("obs", np.array(["x", "y"], dtype=object)),
    )
    multidim.append_partition(repo, later, "p2", group=None)
    store = repo.readonly_session("main").store
    assert {"extra", "label"} <= set(zarr.open_group(store, mode="r").array_keys())
    assert _uncompressed(store) == []


def test_xarray_writes_get_a_compressor_for_every_variable():
    ds = xr.Dataset(
        {"t": (("time", "x"), np.zeros((2, 3), "float32")), "q": (("time",), np.ones(2))},
        coords={"time": np.array(["2026-01-01", "2026-01-02"], dtype="datetime64[ns]")},
    )
    encoding = build_encoding(ds)
    assert all("compressors" in encoding[v] for v in ds.data_vars)
