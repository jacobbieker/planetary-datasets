"""Fast chunk index for HDF5 files with v1 B-trees, read straight from object storage.

NSRDB files are 0.2-5 TB HDF5 files with superblock version 0, so every chunked dataset is
indexed by a version 1 B-tree. Walking that tree through h5py ``chunk_iter`` (or VirtualiZarr's
``HDFParser``) over S3 issues one tiny ranged read per node, one after another, which runs at
about 100 chunks/s. This module parses only the subset of the format needed to find a dataset
and walk its chunk B-tree, and fetches every node of a tree level concurrently, so a tree of
half a million chunks takes a handful of round trips per level instead of thousands.

Anything outside that subset raises :class:`UnsupportedHDF5Layout`; callers fall back to
:func:`h5py_chunk_index`, which produces the same :class:`ChunkIndex` through the HDF5 library.

Format reference: https://docs.hdfgroup.org/hdf5/develop/_f_m_t11.html (superblock v0/v1,
object header v1, symbol table groups, data layout message v3, v1 B-trees).
"""

from __future__ import annotations

import asyncio
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Coroutine, Sequence, TypeVar

import numpy as np
import obstore
from loguru import logger

if TYPE_CHECKING:
    import h5py
    from obstore.store import ObjectStore

T = TypeVar("T")

_SIGNATURE = b"\x89HDF\r\n\x1a\n"

# Header message types (object header v1).
_MSG_DATASPACE = 0x0001
_MSG_LINK_INFO = 0x0002
_MSG_DATATYPE = 0x0003
_MSG_LINK = 0x0006
_MSG_LAYOUT = 0x0008
_MSG_FILTERS = 0x000B
_MSG_CONTINUATION = 0x0010
_MSG_SYMBOL_TABLE = 0x0011

# Message flag bit 1: the message body is a reference to a shared (committed) message.
_MSG_FLAG_SHARED = 0x02

# Superblock v0 cannot store the indexed-storage K, so the library default applies.
_DEFAULT_ISTORE_K = 32

# Ranges per ``get_ranges_async`` call. obstore runs up to 10 requests of one call in
# parallel, so up to ``max_concurrency * 10`` requests are in flight. Measured on GOES
# full_disc (15.6k leaf nodes): 1 range per call is ~5x slower than 8-32.
_RANGES_PER_CALL = 10
# B-tree nodes are ~2.6 KB and scattered among multi-MB chunks; a large coalesce window
# would download chunk data between them for nothing.
_COALESCE = 64 * 1024

_UINT = {1: "<u1", 2: "<u2", 4: "<u4", 8: "<u8"}


class UnsupportedHDF5Layout(NotImplementedError):
    """The file uses a part of the HDF5 format this reader does not parse.

    Callers should fall back to :func:`h5py_chunk_index`.
    """


@dataclass(frozen=True)
class ChunkIndex:
    """Where every chunk of one HDF5 dataset lives in the file.

    Attributes:
        shape: Dataset shape.
        chunk_shape: Chunk shape, in elements.
        dtype: Element dtype, with explicit byte order for multi-byte types.
        offsets: Absolute file byte offset of each chunk, shaped like the chunk grid
            (``ceil(shape / chunk_shape)``). 0 where the chunk is not allocated.
        lengths: Stored size of each chunk in bytes, same shape. 0 means not allocated.
        has_filters: Whether the dataset has a filter pipeline (compression, shuffle...), in
            which case the stored bytes must be decoded before use.
    """

    shape: tuple[int, ...]
    chunk_shape: tuple[int, ...]
    dtype: np.dtype
    offsets: np.ndarray
    lengths: np.ndarray
    has_filters: bool = False


@dataclass(frozen=True)
class _Superblock:
    offset_size: int
    length_size: int
    group_leaf_k: int
    group_internal_k: int
    istore_k: int
    base: int
    root_header: int


@dataclass(frozen=True)
class _DatasetInfo:
    shape: tuple[int, ...]
    dtype: np.dtype
    chunk_shape: tuple[int, ...]
    btree: int | None
    has_filters: bool


def read_chunk_index(
    store: "ObjectStore",
    path: str,
    dataset: str,
    *,
    max_concurrency: int = 64,
) -> ChunkIndex:
    """Build the chunk index of ``dataset`` in an HDF5 file using batched ranged reads.

    Only reads metadata: the superblock, a few object headers, the group structures on the
    way to the dataset, and the dataset's chunk B-tree, one level at a time with all nodes
    of a level fetched concurrently.

    Args:
        store: obstore store holding the file (S3Store, LocalStore, ...).
        path: Path of the file within ``store``.
        dataset: Dataset name in the root group, or a ``/``-separated path through
            subgroups.
        max_concurrency: Maximum number of ``get_ranges`` calls in flight at once. Each
            call carries up to 10 node ranges that obstore fetches in parallel, so up to ten
            times as many HTTP requests can be in flight.

    Returns:
        The dataset's :class:`ChunkIndex`.

    Raises:
        UnsupportedHDF5Layout: The file or dataset uses a feature outside the supported
            subset (superblock other than v0/v1, object header v2, non-chunked or non-v3
            layout, unsupported datatype, links other than hard links, ...).
        KeyError: No object called ``dataset`` exists.
        ValueError: The file is not HDF5 or its metadata is corrupt.
    """
    return _run(read_chunk_index_async(store, path, dataset, max_concurrency=max_concurrency))


async def read_chunk_index_async(
    store: "ObjectStore",
    path: str,
    dataset: str,
    *,
    max_concurrency: int = 64,
) -> ChunkIndex:
    """Async form of :func:`read_chunk_index`, for callers already inside an event loop.

    Args:
        store: obstore store holding the file.
        path: Path of the file within ``store``.
        dataset: Dataset name or ``/``-separated path.
        max_concurrency: Maximum number of ``get_ranges`` calls in flight at once.

    Returns:
        The dataset's :class:`ChunkIndex`.

    Raises:
        UnsupportedHDF5Layout: See :func:`read_chunk_index`.
        KeyError: No object called ``dataset`` exists.
        ValueError: The file is not HDF5 or its metadata is corrupt.
    """
    if max_concurrency < 1:
        raise ValueError(f"max_concurrency must be >= 1, got {max_concurrency}")
    return await _read_chunk_index(_Reader(store, path, max_concurrency), dataset)


def h5py_chunk_index(h5file: "h5py.File", dataset: str) -> ChunkIndex:
    """Build the chunk index of ``dataset`` through the HDF5 library.

    Reference implementation and fallback for :func:`read_chunk_index`. Over remote storage
    this is slow, as HDF5 reads each B-tree node with a separate request.

    Args:
        h5file: Open h5py file (local or over an fsspec file object).
        dataset: Dataset path within the file.

    Returns:
        The dataset's :class:`ChunkIndex`.

    Raises:
        UnsupportedHDF5Layout: The dataset is not chunked.
        KeyError: No object called ``dataset`` exists.
    """
    dset = h5file[dataset]
    if dset.chunks is None:
        raise UnsupportedHDF5Layout(f"{dataset!r} is not chunked")
    shape = tuple(int(s) for s in dset.shape)
    chunk_shape = tuple(int(c) for c in dset.chunks)
    grid = _grid_shape(shape, chunk_shape)
    offsets = np.zeros(grid, dtype=np.uint64)
    lengths = np.zeros(grid, dtype=np.uint64)

    def _store(info: Any) -> None:
        coord = tuple(int(o) // c for o, c in zip(info.chunk_offset, chunk_shape))
        if all(i < g for i, g in zip(coord, grid)):
            offsets[coord] = info.byte_offset
            lengths[coord] = info.size

    dsid = dset.id
    dsid.chunk_iter(_store)

    return ChunkIndex(
        shape=shape,
        chunk_shape=chunk_shape,
        dtype=dset.dtype,
        offsets=offsets,
        lengths=lengths,
        has_filters=dsid.get_create_plist().get_nfilters() > 0,
    )


def _run(coro: Coroutine[Any, Any, T]) -> T:
    """Run ``coro`` to completion, also from inside an already running event loop.

    In the latter case the calling loop is blocked until the walk finishes; async callers
    should await :func:`read_chunk_index_async` instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _grid_shape(shape: Sequence[int], chunk_shape: Sequence[int]) -> tuple[int, ...]:
    return tuple(math.ceil(s / c) for s, c in zip(shape, chunk_shape))


def _uint(buf: bytes, pos: int, size: int) -> int:
    if pos + size > len(buf):
        raise ValueError("truncated HDF5 metadata")
    return int.from_bytes(buf[pos : pos + size], "little")


def _is_undefined(address: int, size: int) -> bool:
    return address == (1 << (8 * size)) - 1


class _Reader:
    """Ranged reads against one object, clamped to its size."""

    def __init__(self, store: "ObjectStore", path: str, max_concurrency: int) -> None:
        self.store = store
        self.path = path
        self.max_concurrency = max_concurrency
        self.size = 0
        self.requests = 0

    async def open(self) -> None:
        meta = await obstore.head_async(self.store, self.path)
        self.size = int(meta["size"])

    def _clamp(self, start: int, length: int) -> int:
        if start < 0 or start >= self.size:
            raise ValueError(f"address {start} is outside the file ({self.size} bytes)")
        return min(length, self.size - start)

    async def read(self, start: int, length: int) -> bytes:
        self.requests += 1
        data = await obstore.get_range_async(
            self.store, self.path, start=start, length=self._clamp(start, length)
        )
        return bytes(data)

    async def read_many(self, starts: Sequence[int], length: int) -> list[bytes]:
        """Read ``length`` bytes at each of ``starts``, concurrently; results keep order."""
        order = sorted(range(len(starts)), key=lambda i: starts[i])
        batches = [order[i : i + _RANGES_PER_CALL] for i in range(0, len(order), _RANGES_PER_CALL)]
        semaphore = asyncio.Semaphore(self.max_concurrency)
        results: list[bytes] = [b""] * len(starts)

        async def fetch(batch: list[int]) -> None:
            batch_starts = [starts[i] for i in batch]
            lengths = [self._clamp(s, length) for s in batch_starts]
            async with semaphore:
                self.requests += 1
                chunks = await obstore.get_ranges_async(
                    self.store,
                    self.path,
                    starts=batch_starts,
                    lengths=lengths,
                    coalesce=_COALESCE,
                )
            for i, chunk in zip(batch, chunks):
                results[i] = bytes(chunk)

        await asyncio.gather(*(fetch(batch) for batch in batches))
        return results


async def _read_chunk_index(reader: _Reader, dataset: str) -> ChunkIndex:
    await reader.open()
    sb = await _read_superblock(reader)
    header = await _find_object(reader, sb, dataset)
    info = _parse_dataset(await _read_object_header(reader, sb, header), sb, dataset)

    grid = _grid_shape(info.shape, info.chunk_shape)
    offsets = np.zeros(grid, dtype=np.uint64)
    lengths = np.zeros(grid, dtype=np.uint64)
    if info.btree is not None:
        leaves = await _walk_chunk_btree(reader, sb, info.btree, len(info.shape))
        _fill_grid(leaves, sb, info, offsets, lengths)
    logger.debug(
        f"{reader.path}:{dataset}: {int(np.count_nonzero(lengths))} chunks "
        f"from {reader.requests} requests"
    )
    return ChunkIndex(
        shape=info.shape,
        chunk_shape=info.chunk_shape,
        dtype=info.dtype,
        offsets=offsets,
        lengths=lengths,
        has_filters=info.has_filters,
    )


async def _read_superblock(reader: _Reader) -> _Superblock:
    # The superblock sits at 0 or, after a user block, at the next power of two from 512.
    head = await reader.read(0, 4096)
    location = 0
    while head[:8] != _SIGNATURE:
        location = 512 if location == 0 else location * 2
        if location >= reader.size:
            raise ValueError(f"{reader.path} is not an HDF5 file")
        head = await reader.read(location, 4096)

    version = head[8]
    if version not in (0, 1):
        raise UnsupportedHDF5Layout(f"superblock version {version}")
    offset_size, length_size = head[13], head[14]
    if offset_size not in _UINT or length_size not in _UINT:
        raise UnsupportedHDF5Layout(f"offset/length sizes {offset_size}/{length_size}")
    group_leaf_k = _uint(head, 16, 2)
    group_internal_k = _uint(head, 18, 2)
    pos = 24
    istore_k = _DEFAULT_ISTORE_K
    if version == 1:
        istore_k = _uint(head, 24, 2)
        pos = 28
    base = _uint(head, pos, offset_size)
    # Skip base, free-space, end-of-file and driver addresses, then the root group symbol
    # table entry's link-name offset, to reach its object header address.
    root_header = _uint(head, pos + 5 * offset_size, offset_size)
    return _Superblock(
        offset_size=offset_size,
        length_size=length_size,
        group_leaf_k=group_leaf_k,
        group_internal_k=group_internal_k,
        istore_k=istore_k,
        base=base,
        root_header=base + root_header,
    )


async def _read_object_header(
    reader: _Reader, sb: _Superblock, address: int
) -> list[tuple[int, int, bytes]]:
    """Return ``(type, flags, body)`` for every message of the v1 object header at ``address``."""
    first = await reader.read(address, 1024)
    if first[:4] == b"OHDR":
        raise UnsupportedHDF5Layout("object header version 2")
    if first[0] != 1:
        raise UnsupportedHDF5Layout(f"object header version {first[0]}")
    block_size = _uint(first, 8, 4)
    # The 12-byte prefix is padded to 16 so messages are 8-byte aligned.
    if 16 + block_size <= len(first):
        block = first[16 : 16 + block_size]
    else:
        block = await reader.read(address + 16, block_size)

    messages: list[tuple[int, int, bytes]] = []
    pending: list[tuple[int, int]] = []
    seen = {address}
    while True:
        pos = 0
        # Blocks are fully covered by messages (gaps are NIL messages), so walk them to
        # the end rather than trusting the header's message count.
        while pos + 8 <= len(block):
            msg_type = _uint(block, pos, 2)
            size = _uint(block, pos + 2, 2)
            flags = block[pos + 4]
            if pos + 8 + size > len(block):
                raise ValueError(f"header message overruns its block in object at {address}")
            body = block[pos + 8 : pos + 8 + size]
            pos += 8 + size
            messages.append((msg_type, flags, body))
            if msg_type == _MSG_CONTINUATION:
                pending.append(
                    (
                        sb.base + _uint(body, 0, sb.offset_size),
                        _uint(body, sb.offset_size, sb.length_size),
                    )
                )
        if not pending:
            return messages
        cont_address, cont_size = pending.pop(0)
        if cont_address in seen:
            raise ValueError(f"object header at {address} has a continuation cycle")
        seen.add(cont_address)
        block = await reader.read(cont_address, cont_size)


def _message(messages: list[tuple[int, int, bytes]], msg_type: int) -> bytes | None:
    for found_type, flags, body in messages:
        if found_type == msg_type:
            if flags & _MSG_FLAG_SHARED:
                raise UnsupportedHDF5Layout(f"shared header message 0x{msg_type:04x}")
            return body
    return None


async def _find_object(reader: _Reader, sb: _Superblock, name: str) -> int:
    """Return the object header address of ``name``, following groups from the root."""
    address = sb.root_header
    walked = ""
    for part in [p for p in name.split("/") if p]:
        messages = await _read_object_header(reader, sb, address)
        table = _message(messages, _MSG_SYMBOL_TABLE)
        if table is None:
            if any(t in (_MSG_LINK_INFO, _MSG_LINK) for t, _, _ in messages):
                raise UnsupportedHDF5Layout(f"group {walked or '/'!r} is a new-style group")
            raise KeyError(f"{walked or '/'!r} is not a group in {reader.path}")
        btree = sb.base + _uint(table, 0, sb.offset_size)
        heap = sb.base + _uint(table, sb.offset_size, sb.offset_size)
        entries = await _read_group(reader, sb, btree, heap)
        walked = f"{walked}/{part}"
        if part not in entries:
            raise KeyError(f"{walked!r} not found in {reader.path}")
        target = entries[part]
        if target is None:
            raise UnsupportedHDF5Layout(f"{walked!r} is a soft link")
        address = target
    return address


async def _read_group(
    reader: _Reader, sb: _Superblock, btree: int, heap: int
) -> dict[str, int | None]:
    """Map link name to object header address for every member of a symbol-table group.

    Soft links map to None: their target is a path, not an object header.
    """
    o, ln = sb.offset_size, sb.length_size
    heap_head = await reader.read(heap, 8 + 2 * ln + o)
    if heap_head[:4] != b"HEAP":
        raise ValueError(f"bad local heap signature at {heap}")
    heap_size = _uint(heap_head, 8, ln)
    heap_data_address = sb.base + _uint(heap_head, 8 + 2 * ln, o)

    node_size = 8 + 2 * o + 2 * sb.group_internal_k * (ln + o) + ln
    snod_size = 8 + 2 * sb.group_leaf_k * (2 * o + 24)

    heap_task = asyncio.ensure_future(reader.read(heap_data_address, heap_size))
    snods: list[int] = []
    level_addresses = [btree]
    expected_level: int | None = None
    while level_addresses:
        nodes = await reader.read_many(level_addresses, node_size)
        children: list[int] = []
        level = -1
        for address, node in zip(level_addresses, nodes):
            node_type, level, used = _tree_node_header(node, address)
            if node_type != 0:
                raise ValueError(f"expected a group B-tree node at {address}, got type {node_type}")
            if expected_level is not None and level != expected_level:
                raise ValueError(f"B-tree node at {address} is level {level}, not {expected_level}")
            start = 8 + 2 * o + ln
            kids = [sb.base + _uint(node, start + i * (ln + o), o) for i in range(used)]
            (children if level > 0 else snods).extend(kids)
        expected_level = level - 1
        level_addresses = children

    heap_data = await heap_task
    entries: dict[str, int | None] = {}
    for address, node in zip(snods, await reader.read_many(snods, snod_size)):
        if node[:4] != b"SNOD":
            raise ValueError(f"bad symbol table node signature at {address}")
        for i in range(_uint(node, 6, 2)):
            pos = 8 + i * (2 * o + 24)
            name_offset = _uint(node, pos, o)
            header = _uint(node, pos + o, o)
            cache_type = _uint(node, pos + 2 * o, 4)
            end = heap_data.index(b"\0", name_offset)
            name = heap_data[name_offset:end].decode("utf-8")
            entries[name] = None if cache_type == 2 else sb.base + header
    return entries


def _tree_node_header(node: bytes, address: int) -> tuple[int, int, int]:
    if node[:4] != b"TREE":
        raise ValueError(f"bad B-tree node signature at {address}")
    return node[4], node[5], _uint(node, 6, 2)


def _parse_datatype(body: bytes) -> np.dtype:
    cls, version = body[0] & 0x0F, body[0] >> 4
    bits = body[1] | (body[2] << 8) | (body[3] << 16)
    size = _uint(body, 4, 4)
    order = ">" if bits & 0x01 else "<"
    if cls == 0:
        bit_offset, precision = _uint(body, 8, 2), _uint(body, 10, 2)
        if size not in (1, 2, 4, 8) or bit_offset != 0 or precision != 8 * size:
            raise UnsupportedHDF5Layout(f"integer of {size} bytes, {precision}-bit precision")
        return np.dtype(f"{order}{'i' if bits & 0x08 else 'u'}{size}")
    if cls == 1:
        if bits & 0x40:
            raise UnsupportedHDF5Layout("VAX floating point byte order")
        # (sign bit, offset, precision, exponent loc/size, mantissa loc/size, bias) for
        # IEEE 754 with an implied leading mantissa bit (normalisation 2) and zero padding.
        ieee = {
            2: (15, 0, 16, 10, 5, 0, 10, 15),
            4: (31, 0, 32, 23, 8, 0, 23, 127),
            8: (63, 0, 64, 52, 11, 0, 52, 1023),
        }
        layout = (body[2], _uint(body, 8, 2), _uint(body, 10, 2), *body[12:16], _uint(body, 16, 4))
        if ieee.get(size) != layout or (bits & 0x3E) != 0x20:
            raise UnsupportedHDF5Layout(f"non-IEEE float of {size} bytes")
        return np.dtype(f"{order}f{size}")
    raise UnsupportedHDF5Layout(f"datatype class {cls} (version {version})")


def _parse_dataspace(body: bytes, length_size: int) -> tuple[int, ...]:
    version, rank = body[0], body[1]
    if version == 1:
        start = 8
    elif version == 2:
        start = 4
        if body[3] == 2:  # null dataspace
            rank = 0
    else:
        raise UnsupportedHDF5Layout(f"dataspace version {version}")
    return tuple(_uint(body, start + i * length_size, length_size) for i in range(rank))


def _parse_dataset(
    messages: list[tuple[int, int, bytes]], sb: _Superblock, name: str
) -> _DatasetInfo:
    dataspace = _message(messages, _MSG_DATASPACE)
    datatype = _message(messages, _MSG_DATATYPE)
    layout = _message(messages, _MSG_LAYOUT)
    if dataspace is None or datatype is None or layout is None:
        raise UnsupportedHDF5Layout(f"{name!r} is not a dataset")
    shape = _parse_dataspace(dataspace, sb.length_size)
    dtype = _parse_datatype(datatype)

    version, layout_class = layout[0], layout[1]
    if version != 3:
        raise UnsupportedHDF5Layout(f"data layout version {version}")
    if layout_class != 2:
        kind = {0: "compact", 1: "contiguous", 3: "virtual"}.get(layout_class, "unknown")
        raise UnsupportedHDF5Layout(f"{name!r} has {kind} layout, not chunked")
    n_dims = layout[2]
    btree = _uint(layout, 3, sb.offset_size)
    dims = [_uint(layout, 3 + sb.offset_size + 4 * i, 4) for i in range(n_dims)]
    # The last chunk dimension is the element size in bytes.
    if n_dims != len(shape) + 1 or dims[-1] != dtype.itemsize:
        raise ValueError(f"{name!r}: chunk dims {dims} do not match shape {shape}, {dtype}")

    filters = _message(messages, _MSG_FILTERS)
    return _DatasetInfo(
        shape=shape,
        dtype=dtype,
        chunk_shape=tuple(dims[:-1]),
        btree=None if _is_undefined(btree, sb.offset_size) else sb.base + btree,
        has_filters=filters is not None and filters[1] > 0,
    )


async def _walk_chunk_btree(reader: _Reader, sb: _Superblock, root: int, rank: int) -> np.ndarray:
    """Return every leaf entry of a chunk B-tree as a structured array.

    Fields: ``size`` (stored bytes), ``mask`` (filter mask), ``offset`` (rank + 1 element
    offsets, the last always 0) and ``child`` (chunk address relative to the base address).
    """
    o = sb.offset_size
    key_size = 8 + 8 * (rank + 1)
    entry = np.dtype(
        [("size", "<u4"), ("mask", "<u4"), ("offset", "<u8", (rank + 1,)), ("child", _UINT[o])]
    )
    header_size = 8 + 2 * o
    node_size = header_size + 2 * sb.istore_k * entry.itemsize + key_size

    leaves: list[np.ndarray] = []
    level_addresses = [root]
    expected_level: int | None = None
    while level_addresses:
        nodes = await reader.read_many(level_addresses, node_size)
        children: list[np.ndarray] = []
        level = -1
        for address, node in zip(level_addresses, nodes):
            node_type, level, used = _tree_node_header(node, address)
            if node_type != 1:
                raise ValueError(f"expected a chunk B-tree node at {address}, got type {node_type}")
            if expected_level is not None and level != expected_level:
                raise ValueError(f"B-tree node at {address} is level {level}, not {expected_level}")
            if header_size + used * entry.itemsize > len(node):
                raise UnsupportedHDF5Layout(f"B-tree node at {address} exceeds {node_size} bytes")
            entries = np.frombuffer(node, dtype=entry, count=used, offset=header_size)
            if level > 0:
                children.append(entries["child"].astype(np.uint64) + np.uint64(sb.base))
            else:
                leaves.append(entries)
        logger.debug(f"chunk B-tree level {level}: {len(level_addresses)} nodes")
        expected_level = level - 1
        level_addresses = [int(a) for a in np.concatenate(children)] if children else []

    return np.concatenate(leaves) if leaves else np.zeros(0, dtype=entry)


def _fill_grid(
    leaves: np.ndarray,
    sb: _Superblock,
    info: _DatasetInfo,
    offsets: np.ndarray,
    lengths: np.ndarray,
) -> None:
    rank = len(info.shape)
    if leaves["mask"].any():
        logger.warning(
            f"{int(np.count_nonzero(leaves['mask']))} chunks skipped part of the filter "
            "pipeline; their bytes are not encoded like the others"
        )
    coords = leaves["offset"][:, :rank] // np.asarray(info.chunk_shape, dtype=np.uint64)
    inside = np.all(coords < np.asarray(offsets.shape, dtype=np.uint64), axis=1)
    if not inside.all():
        logger.debug(f"ignoring {int((~inside).sum())} chunks beyond the dataset extent")
    coords = coords[inside]
    flat = np.ravel_multi_index(tuple(coords.T.astype(np.intp)), offsets.shape)
    offsets.reshape(-1)[flat] = leaves["child"][inside].astype(np.uint64) + np.uint64(sb.base)
    lengths.reshape(-1)[flat] = leaves["size"][inside]
