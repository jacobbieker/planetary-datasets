"""The fast v1 B-tree chunk reader must agree exactly with HDF5's own chunk iteration."""

from __future__ import annotations

import asyncio
import zlib

import h5py
import numpy as np
import pytest
from obstore.store import LocalStore

from planetary_datasets.common.hdf5_chunk_index import (
    ChunkIndex,
    UnsupportedHDF5Layout,
    h5py_chunk_index,
    read_chunk_index,
    read_chunk_index_async,
)


def _assert_same(fast: ChunkIndex, ref: ChunkIndex) -> None:
    assert fast.shape == ref.shape
    assert fast.chunk_shape == ref.chunk_shape
    assert fast.dtype == ref.dtype
    assert fast.dtype.byteorder in ("<", ">", "|", "=")
    assert np.dtype(fast.dtype).str == np.dtype(ref.dtype).str
    assert fast.offsets.dtype == np.uint64 and fast.lengths.dtype == np.uint64
    assert fast.offsets.shape == ref.offsets.shape
    np.testing.assert_array_equal(fast.offsets, ref.offsets)
    np.testing.assert_array_equal(fast.lengths, ref.lengths)
    assert fast.has_filters == ref.has_filters


def _compare(tmp_path, filename: str, dataset: str) -> ChunkIndex:
    fast = read_chunk_index(LocalStore(prefix=tmp_path), filename, dataset)
    with h5py.File(tmp_path / filename, "r") as f:
        ref = h5py_chunk_index(f, dataset)
    _assert_same(fast, ref)
    return fast


def _read_chunk(path, index: ChunkIndex, coord: tuple[int, ...]) -> np.ndarray:
    offset, length = int(index.offsets[coord]), int(index.lengths[coord])
    with open(path, "rb") as fh:
        fh.seek(offset)
        raw = fh.read(length)
    if index.has_filters:
        raw = zlib.decompress(raw)
    return np.frombuffer(raw, dtype=index.dtype).reshape(index.chunk_shape)


def _chunk_slice(index: ChunkIndex, coord: tuple[int, ...]) -> tuple[slice, ...]:
    return tuple(
        slice(i * c, min((i + 1) * c, s)) for i, c, s in zip(coord, index.chunk_shape, index.shape)
    )


@pytest.fixture
def mixed_file(tmp_path):
    """A superblock-v0 file with several dtypes, ragged edges and a multi-level B-tree."""
    rng = np.random.default_rng(0)
    path = tmp_path / "mixed.h5"
    with h5py.File(path, "w", libver="earliest") as f:
        for dtype in ("u2", "i2", "u1", "i1", "f2", "f4", "f8", "i4", "u8", ">i2", ">f4"):
            data = (rng.random((53, 37)) * 100).astype(dtype)
            f.create_dataset(f"ragged_{dtype.replace('>', 'be_')}", data=data, chunks=(20, 5))
        # 2500 chunks: far more than the 64 a node holds, so the tree has several levels.
        f.create_dataset("many", data=rng.integers(0, 1000, (100, 100), dtype="u2"), chunks=(2, 2))
        f.create_dataset("cube", data=rng.random((40, 30, 20), dtype="f4"), chunks=(3, 7, 4))
        # Only part written: the rest of the chunks are never allocated.
        sparse = f.create_dataset("sparse", shape=(60, 60), dtype="i2", chunks=(10, 10))
        sparse[0:10, 0:30] = 7
        sparse[45:60, 50:60] = -3
        f.create_dataset("empty", shape=(30, 30), dtype="f4", chunks=(10, 10))
        f.create_dataset(
            "gzip",
            data=rng.integers(0, 50, (64, 48), dtype="i2"),
            chunks=(16, 16),
            compression="gzip",
        )
        f.create_dataset("contiguous", data=np.arange(10, dtype="u2"))
        grp = f.create_group("grp").create_group("sub")
        grp.create_dataset("nested", data=np.arange(120, dtype="u2").reshape(12, 10), chunks=(5, 3))
    return path


@pytest.mark.parametrize(
    "dataset",
    [
        "ragged_u2",
        "ragged_i2",
        "ragged_u1",
        "ragged_i1",
        "ragged_f2",
        "ragged_f4",
        "ragged_u8",
        "ragged_f8",
        "ragged_i4",
        "ragged_be_i2",
        "ragged_be_f4",
        "many",
        "cube",
        "sparse",
        "empty",
        "gzip",
        "grp/sub/nested",
        "/grp/sub/nested",
    ],
)
def test_matches_h5py(mixed_file, dataset):
    _compare(mixed_file.parent, mixed_file.name, dataset)


def test_file_is_the_old_format(mixed_file):
    raw = mixed_file.read_bytes()
    assert raw[8] == 0, "libver='earliest' must give superblock v0"


def test_unallocated_chunks_have_zero_length(mixed_file):
    index = _compare(mixed_file.parent, mixed_file.name, "sparse")
    allocated = index.lengths > 0
    assert allocated[0, 0:3].all() and allocated[4:6, 5].all()
    assert allocated.sum() == 3 + 2
    assert (index.offsets[~allocated] == 0).all()

    empty = _compare(mixed_file.parent, mixed_file.name, "empty")
    assert not empty.lengths.any()


def test_byte_ranges_decode_to_the_data(mixed_file):
    with h5py.File(mixed_file, "r") as f:
        for name, coords in [
            ("ragged_f4", [(0, 0), (2, 7), (1, 3)]),
            ("ragged_be_i2", [(2, 7)]),
            ("many", [(0, 0), (49, 49), (17, 33)]),
            ("cube", [(13, 4, 4), (0, 0, 0)]),
            ("gzip", [(3, 2)]),
        ]:
            index = read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, name)
            for coord in coords:
                chunk = _read_chunk(mixed_file, index, coord)
                region = _chunk_slice(index, coord)
                expected = f[name][region]
                # Edge chunks are stored full size; only the in-bounds part holds data.
                got = chunk[tuple(slice(0, r.stop - r.start) for r in region)]
                np.testing.assert_array_equal(got, expected)
    assert index.has_filters


def test_many_root_members_span_several_symbol_nodes(tmp_path):
    """300 links need dozens of SNOD nodes and a multi-level group B-tree."""
    path = tmp_path / "wide.h5"
    with h5py.File(path, "w", libver="earliest") as f:
        for i in range(300):
            f.create_dataset(f"var_{i:03d}", data=np.full((6, 4), i, dtype="u2"), chunks=(4, 4))
    raw = path.read_bytes()
    assert raw.count(b"SNOD") > 32
    for name in ("var_000", "var_149", "var_299", "var_077"):
        index = _compare(tmp_path, path.name, name)
        chunk = _read_chunk(path, index, (1, 0))
        assert (chunk[:2] == int(name[-3:])).all()


def test_missing_dataset_raises_key_error(mixed_file):
    with pytest.raises(KeyError):
        read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "nope")
    with pytest.raises(KeyError):
        read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "grp/nope")


def test_contiguous_dataset_is_unsupported(mixed_file):
    with pytest.raises(UnsupportedHDF5Layout, match="contiguous"):
        read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "contiguous")
    with h5py.File(mixed_file, "r") as f, pytest.raises(UnsupportedHDF5Layout):
        h5py_chunk_index(f, "contiguous")


def test_soft_link_is_unsupported_not_missing(mixed_file):
    """A soft link must send callers to the h5py fallback, which can resolve it."""
    with h5py.File(mixed_file, "a", libver="earliest") as f:
        f["alias"] = h5py.SoftLink("/many")
    with pytest.raises(UnsupportedHDF5Layout, match="soft link"):
        read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "alias")


def test_a_dataset_used_as_a_group_is_missing(mixed_file):
    with pytest.raises(KeyError, match="not a group"):
        read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "many/extra")


def test_new_superblock_is_unsupported(tmp_path):
    path = tmp_path / "latest.h5"
    with h5py.File(path, "w", libver="latest") as f:
        f.create_dataset("x", data=np.zeros((10, 10), dtype="u2"), chunks=(5, 5))
    assert path.read_bytes()[8] >= 2
    with pytest.raises(UnsupportedHDF5Layout, match="superblock"):
        read_chunk_index(LocalStore(prefix=tmp_path), path.name, "x")
    with h5py.File(path, "r") as f:
        assert h5py_chunk_index(f, "x").lengths.all()


def test_not_hdf5_raises_value_error(tmp_path):
    (tmp_path / "junk.h5").write_bytes(b"not an hdf5 file" * 100)
    with pytest.raises(ValueError, match="not an HDF5 file"):
        read_chunk_index(LocalStore(prefix=tmp_path), "junk.h5", "x")


def test_works_inside_a_running_event_loop(mixed_file):
    async def inner() -> ChunkIndex:
        return read_chunk_index(LocalStore(prefix=mixed_file.parent), mixed_file.name, "many")

    index = asyncio.run(inner())
    assert index.lengths.all()

    store = LocalStore(prefix=mixed_file.parent)
    awaited = asyncio.run(read_chunk_index_async(store, mixed_file.name, "many"))
    _assert_same(awaited, index)


def test_low_concurrency_gives_the_same_answer(mixed_file):
    store = LocalStore(prefix=mixed_file.parent)
    a = read_chunk_index(store, mixed_file.name, "many", max_concurrency=1)
    b = read_chunk_index(store, mixed_file.name, "many", max_concurrency=64)
    _assert_same(a, b)
    with pytest.raises(ValueError):
        read_chunk_index(store, mixed_file.name, "many", max_concurrency=0)
