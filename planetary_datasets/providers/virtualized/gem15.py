r"""A virtual Icechunk store of Google's GEM-15 elevation models.

GEM-15 is a near-global 15 m surface (DSM) and terrain (DTM) elevation model derived from
sub-metre optical stereo imagery by Google (60 S to 84 N; Russia, China, Mongolia,
Greenland and Antarctica are masked, as NaN). It is published as Cloud-Optimized GeoTIFFs in
the public bucket ``gs://climate-and-sustainability/gem-15``, one cell (usually 8192 x 8192
pixels, 123 km; at most 32 x 32 tiles) per ``<block>/<cell>/{dsm,dtm}/zall/mosaic.tif``, in the
local UTM projection of the cell. 13,485 cells, 10.3 TB of DSM and 2.0 TB of DTM. Nothing is
copied: this store holds one virtual chunk reference per internal GeoTIFF tile, so reading it
fetches byte ranges from Google's bucket.

License CC-BY 4.0. Attribution: "This dataset is produced by Google". Citation: Batchu, V.
V., Neumann, M., Li, T., Allen, B., Zhang, J., & Van Arsdale, C. (2026). GEM-15: Global
surface and terrain elevation models (15 meters) from sub-meter optical stereo imagery.
EarthArXiv preprint. https://doi.org/10.31223/X55N75 . Earth Engine asset
``projects/climate-and-sustainability/assets/gem-15_v1`` (experimental). Heights are above
the EGM96 geoid (EPSG:5773).

Layout (``bkr/terrain/gem15.icechunk``): one group per UTM zone, ``epsg_32631`` for WGS 84 /
UTM 31N, ``epsg_32731`` for 31S, each on that zone's own native grid, never resampled:

* ``dsm_stack`` ``(y, x, dsm_band)`` float32, ``dsm_band`` being ``dsm``, ``dsm_percentile_5``,
  ``dsm_percentile_50``, ``dsm_percentile_95`` and ``dsm_std``, in that order;
* ``dtm`` ``(y, x)`` float32;
* ``spatial_ref``, a scalar carrying the CRS (``crs_wkt``, ``GeoTransform``) the data arrays name
  in ``grid_mapping``.

Chunks are 512 x 512, each exactly one internal tile of a GeoTIFF, so a usual cell is 16 x 16
chunks. Tiles of the GeoTIFFs hold the five DSM bands interleaved, which a Zarr chunk cannot
split: the five bands are one array, and reading one of them reads all five (the same bytes
Earth Engine reads). The arrays are plain Zarr v3 (bytes, then the numcodecs ``zlib`` codec):
no codec outside zarr-python and numcodecs is needed to read them. The grid of a zone starts at
a fixed origin (``GRID`` below, on the 7680 m lattice every cell's corner falls on) and is as
tall and wide as the zone can be, so cells never move it; a tile of the grid with no cell
has no chunk, and reads as NaN. There are no ``x``/``y`` coordinate arrays: at 15 m they would
outweigh the references; :func:`open_zone` computes them from ``GeoTransform``. A DSM and
the DTM of its cell need not cover the same area; each is placed by its own corner. The
corners of 6 of the 26,970 files are off the lattice by whole pixels that are not whole tiles
(UTM 26S and 23S): they go to groups on a shifted grid, ``epsg_32726_dx0_dy2880``, whose
name and ``GeoTransform`` say by how much.

Reading. The references point into Google's bucket, which a reader must authorise (anonymously)
or every read fails::

    import icechunk
    import planetary_datasets.providers.virtualized.gem15 as gem15

    repo = gem15.open_reader()      # Source Cooperative, anonymous
    # the same, spelled out:
    url = "gs://climate-and-sustainability/gem-15/"
    repo = icechunk.Repository.open(
        icechunk.s3_storage(
            bucket="us-west-2.opendata.source.coop", prefix="bkr/terrain/gem15.icechunk",
            region="us-west-2", anonymous=True,
        ),
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {url: icechunk.gcs_credentials(anonymous=True)}
        ),
    )

    ds = gem15.open_zone(repo, gem15.zone_group(lon=-0.12, lat=51.5))   # epsg_32630
    ds.dsm.sel(x=slice(699_000, 700_000), y=slice(5_710_000, 5_709_000)).compute()
    local = gem15.open_around(repo, lon=-0.12, lat=51.5, half_width_m=2_000)

``open_zone`` returns the six variables ``dsm``, ``dsm_std``, ``dsm_percentile_5``,
``dsm_percentile_50``, ``dsm_percentile_95`` and ``dtm`` on ``y`` (descending) and ``x``
metres. Without it, ``xr.open_zarr(repo.readonly_session("main").store, group="epsg_32630",
consolidated=False)`` gives the raw arrays.

Writing (one writer per store; re-run any time, it only writes what is missing or changed)::

    python -m planetary_datasets.providers.virtualized.gem15
    python -m planetary_datasets.providers.virtualized.gem15 --follow --pause 604800

Each pass lists the bucket, compares every object's etag, size and modification time with
the ledger kept in the zone group's ``cells`` attribute, reads the GeoTIFF headers of the
cells that are new or changed (the tile offsets and sizes: a few kilobytes a file), and writes
their references, a commit per ``--batch-cells`` cells. A cell is committed with its ledger entry
in one commit, so a killed run resumes where it stopped. Set ``ICECHUNK_LOCAL_PATH`` to write
to a local directory instead of Source Cooperative.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import re
import sys
import time
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import numpy as np
from loguru import logger

from planetary_datasets.config import get_config
from planetary_datasets.providers.virtualized.virtual_repo import open_virtual_repo

if TYPE_CHECKING:
    import icechunk
    import xarray as xr

STORE_PREFIX = "bkr/terrain/gem15.icechunk"
BUCKET = "climate-and-sustainability"
ROOT = "gem-15"
#: Where the references point, and so the virtual chunk container of the store.
SOURCE_URL = f"gs://{BUCKET}/{ROOT}/"

PIXEL = 15
TILE = 512
#: Cells are 8192 pixels, so their corners fall on a lattice of this many metres.
LATTICE = PIXEL * TILE
#: dsm_band of the DSM file, in file order (``tileset_band_index`` of each cell's manifest).
DSM_BANDS = ("dsm", "dsm_percentile_5", "dsm_percentile_50", "dsm_percentile_95", "dsm_std")
VARIABLES = (*DSM_BANDS, "dtm")
STACK = "dsm_stack"
LEDGER = "cells"

#: Per-hemisphere extent of every zone's grid in metres: ``(x_min, x_max, y_min, y_max)``,
#: all on the lattice. A cell belongs to the zone of its hemisphere though it may straddle the
#: equator, so the north grid reaches 32 lattice steps (two cells) below northing 0 and the
#: south one that far above the false northing of 10,000,000 m. The north ends at 84 N; the
#: south begins, from 60 S, well below the lowest northing that covers it.
GRID = {
    "N": (0, 132 * LATTICE, -32 * LATTICE, 1224 * LATTICE),
    "S": (0, 132 * LATTICE, 260 * LATTICE, 1335 * LATTICE),
}

LICENSE = "CC-BY-4.0"
ATTRIBUTION = "This dataset is produced by Google"
CITATION = (
    "Batchu, V. V., Neumann, M., Li, T., Allen, B., Zhang, J., & Van Arsdale, C. (2026). "
    "GEM-15: Global surface and terrain elevation models (15 meters) from sub-meter optical "
    "stereo imagery. EarthArXiv preprint. https://doi.org/10.31223/X55N75"
)
GLOBAL_ATTRS = {
    "title": "GEM-15: Global surface and terrain elevation models (15 m), virtual references",
    "license": LICENSE,
    "attribution": ATTRIBUTION,
    "citation": CITATION,
    "source": f"{SOURCE_URL} (Earth Engine: projects/climate-and-sustainability/assets/gem-15_v1)",
    "vertical_datum": "EGM96 geoid (EPSG:5773)",
    "comment": "Virtual store: chunks are byte ranges of Google's Cloud-Optimized GeoTIFFs.",
}

_OBJECT = re.compile(r"^(?P<block>[^/]+)/(?P<cell>[^/]+)/(?P<kind>dsm|dtm)/zall/mosaic\.tif$")


class UnsupportedTiff(ValueError):
    """A GeoTIFF this store cannot reference as it is (layout, codec or grid)."""


@dataclasses.dataclass(frozen=True)
class Source:
    """Where the GeoTIFFs are: an object store, and the URL its objects are referenced by.

    Attributes:
        store: An ``obstore`` store rooted at the dataset's ``gem-15`` directory.
        url: URL prefix of that directory as the store's virtual chunk container knows it
            (``gs://climate-and-sustainability/gem-15/``; a ``file://`` directory in tests).
    """

    store: Any
    url: str

    @classmethod
    def google(cls) -> "Source":
        """Google's public bucket, anonymously."""
        from obstore.store import GCSStore

        return cls(GCSStore(BUCKET, prefix=ROOT, skip_signature=True), SOURCE_URL)

    @classmethod
    def local(cls, directory: str) -> "Source":
        """A directory laid out like the bucket's ``gem-15`` directory."""
        import pathlib

        from obstore.store import LocalStore

        root = pathlib.Path(directory).resolve()
        return cls(LocalStore(str(root)), f"{root.as_uri()}/")


@dataclasses.dataclass(frozen=True)
class CellFiles:
    """The GeoTIFFs of one cell, each as a signature of its listing entry."""

    cell: str
    block: str
    dsm: str | None = None
    dtm: str | None = None

    def path(self, kind: str) -> str:
        """Path of one of the cell's files under the dataset root."""
        return f"{self.block}/{self.cell}/{kind}/zall/mosaic.tif"


def list_cells(source: Source) -> dict[str, CellFiles]:
    """List the bucket and group its GeoTIFFs by cell.

    The signature of a file is its etag, size and modification time, so a replaced object
    reads as changed. A cell without a DSM is dropped: the DTM alone has no band to
    anchor it.
    """
    import obstore

    found: dict[str, dict[str, Any]] = {}
    for batch in obstore.list(source.store, chunk_size=1000):
        for meta in batch:
            match = _OBJECT.match(meta["path"])
            if match is None:
                continue
            sig = f"{meta.get('e_tag')}|{meta['size']}|{meta['last_modified'].isoformat()}"
            entry = found.setdefault(match["cell"], {"block": match["block"]})
            entry[match["kind"]] = sig
    return {
        cell: CellFiles(cell, e["block"], e.get("dsm"), e.get("dtm"))
        for cell, e in sorted(found.items())
        if e.get("dsm")
    }


@dataclasses.dataclass(frozen=True)
class TileLayout:
    """What a GeoTIFF's header says about where its tiles are.

    Attributes:
        epsg: Projected CRS code.
        x0: Easting of the left edge, metres.
        y0: Northing of the top edge, metres.
        offsets: Byte offset of every tile, ``(tiles down, tiles across)``.
        counts: Byte length of every tile, same shape. Zero for a sparse tile.
        bands: Samples per pixel.
    """

    epsg: int
    x0: float
    y0: float
    offsets: np.ndarray
    counts: np.ndarray
    bands: int


async def _read_layout(source: Source, path: str) -> TileLayout:
    """Read one GeoTIFF's first IFD, and check it is the layout the arrays are declared for."""
    from async_tiff import TIFF
    from async_tiff.enums import Endianness

    tiff = await TIFF.open(path, store=source.store)
    ifd = tiff.ifds[0]
    problems = []
    if tiff.endianness != Endianness.LittleEndian:
        problems.append(f"byte order {tiff.endianness}")
    if int(ifd.compression) not in (8, 32946):
        problems.append(f"compression {ifd.compression}")
    if ifd.predictor not in (None, 1):
        problems.append(f"predictor {ifd.predictor}")
    if int(ifd.planar_configuration) != 1:
        problems.append("planar (not pixel-interleaved) samples")
    if [int(b) for b in ifd.bits_per_sample] != [32] * ifd.samples_per_pixel:
        problems.append(f"bits per sample {ifd.bits_per_sample}")
    if any(int(f) != 3 for f in ifd.sample_format):  # 3: IEEE floating point
        problems.append(f"sample format {ifd.sample_format}")
    if (ifd.tile_width, ifd.tile_height) != (TILE, TILE):
        problems.append(f"tile {ifd.tile_width}x{ifd.tile_height}")
    if ifd.image_width % TILE or ifd.image_height % TILE:
        problems.append(f"size {ifd.image_width}x{ifd.image_height}")
    scale = ifd.model_pixel_scale
    tie = ifd.model_tiepoint
    if not scale or not tie or (abs(scale[0]), abs(scale[1])) != (PIXEL, PIXEL):
        problems.append(f"pixel scale {scale}")
    geo = ifd.geo_key_directory
    epsg = getattr(geo, "projected_type", None) if geo is not None else None
    if not epsg:
        problems.append("no projected CRS")
    if problems:
        raise UnsupportedTiff(f"{path}: {'; '.join(problems)}")
    assert tie is not None and scale is not None
    x0, y0 = float(tie[3]) - tie[0] * scale[0], float(tie[4]) + tie[1] * scale[1]
    raster_type = getattr(geo, "raster_type", None)
    if raster_type == 2 or str(raster_type).endswith("PixelIsPoint"):
        x0, y0 = x0 - PIXEL / 2, y0 + PIXEL / 2
    shape = (ifd.image_height // TILE, ifd.image_width // TILE)
    return TileLayout(
        epsg=int(epsg),
        x0=x0,
        y0=y0,
        offsets=np.asarray(ifd.tile_offsets, dtype=np.uint64).reshape(shape),
        counts=np.asarray(ifd.tile_byte_counts, dtype=np.uint64).reshape(shape),
        bands=int(ifd.samples_per_pixel),
    )


async def _read_layouts(
    source: Source, jobs: Sequence[tuple[str, str]], concurrency: int
) -> dict[tuple[str, str], TileLayout | Exception]:
    """Read the headers of ``jobs`` (``(cell, path)``), a few at a time.

    A header that fails is returned as its exception, so one bad file does not stop the
    batch.
    """
    gate = asyncio.Semaphore(concurrency)

    async def one(path: str) -> TileLayout | Exception:
        async with gate:
            last: Exception | None = None
            for attempt in range(4):
                try:
                    return await _read_layout(source, path)
                except UnsupportedTiff as exc:
                    return exc
                except Exception as exc:  # noqa: BLE001 - network errors are retried
                    last = exc
                    await asyncio.sleep(2**attempt)
            assert last is not None
            return last

    results = await asyncio.gather(*(one(path) for _, path in jobs))
    return dict(zip(jobs, results))


def group_name(epsg: int, dx: int = 0, dy: int = 0) -> str:
    """Group of a projected UTM CRS: ``epsg_32631``, or ``epsg_32726_dx0_dy4800`` when shifted."""
    return f"epsg_{epsg}" if not (dx or dy) else f"epsg_{epsg}_dx{dx}_dy{dy}"


def hemisphere(epsg: int) -> str:
    """``N`` or ``S`` for a WGS 84 / UTM EPSG code.

    Raises:
        UnsupportedTiff: for any other CRS.
    """
    if 32601 <= epsg <= 32660:
        return "N"
    if 32701 <= epsg <= 32760:
        return "S"
    raise UnsupportedTiff(f"EPSG:{epsg} is not a WGS 84 / UTM zone")


def zone_group(lon: float, lat: float) -> str:
    """Group of the standard UTM zone containing a point.

    The Norway and Svalbard exceptions of the UTM grid are not applied, and neither are the
    shifted groups (:class:`Grid`) that hold the few cells off the standard lattice.
    """
    zone = min(60, max(1, int((lon + 180.0) // 6) + 1))
    return group_name((32600 if lat >= 0 else 32700) + zone)


@dataclasses.dataclass(frozen=True)
class Grid:
    """The native grid of one group: a UTM zone's, shifted when a cell is off its lattice.

    Nearly every cell's corner falls on the 7680 m lattice of its zone. A handful are
    offset by a whole number of pixels that is not a whole tile; their tiles could not line up
    with chunks of the standard grid, so they go to a group whose grid is shifted by that offset.

    Attributes:
        epsg: Projected CRS code, WGS 84 / UTM.
        dx: Metres the grid is shifted east of the standard one.
        dy: Metres the grid is shifted south of the standard one.
    """

    epsg: int
    dx: int = 0
    dy: int = 0

    @property
    def name(self) -> str:
        """The group's name."""
        return group_name(self.epsg, self.dx, self.dy)

    @property
    def bounds(self) -> tuple[int, int, int, int]:
        """``(x_min, x_max, y_min, y_max)`` in metres."""
        x_min, x_max, y_min, y_max = GRID[hemisphere(self.epsg)]
        return x_min + self.dx, x_max + self.dx, y_min - self.dy, y_max - self.dy

    @property
    def shape(self) -> tuple[int, int]:
        """``(rows, columns)`` in pixels."""
        x_min, x_max, y_min, y_max = self.bounds
        return (y_max - y_min) // PIXEL, (x_max - x_min) // PIXEL

    @property
    def geo_transform(self) -> tuple[float, ...]:
        """GDAL ``GeoTransform``: the top-left corner and the 15 m steps."""
        x_min, _, _, y_max = self.bounds
        return (float(x_min), float(PIXEL), 0.0, float(y_max), 0.0, -float(PIXEL))

    @classmethod
    def of(cls, layout: "TileLayout") -> "Grid":
        """The grid a cell's tiles line up with."""
        x_min, _, _, y_max = GRID[hemisphere(layout.epsg)]
        return cls(
            layout.epsg,
            int(round((layout.x0 - x_min) % LATTICE)),
            int(round((y_max - layout.y0) % LATTICE)),
        )

    def origin(self, layout: "TileLayout") -> tuple[int, int]:
        """``(row, column)`` in chunks of a cell's first tile.

        Raises:
            UnsupportedTiff: when the cell is outside the grid.
        """
        x_min, x_max, y_min, y_max = self.bounds
        cx, rx = divmod(layout.x0 - x_min, LATTICE)
        cy, ry = divmod(y_max - layout.y0, LATTICE)
        n_y, n_x = layout.offsets.shape
        if rx or ry:
            raise UnsupportedTiff(
                f"corner ({layout.x0}, {layout.y0}) is not on the grid of {self.name}"
            )
        if (
            cx < 0
            or cy < 0
            or (cx + n_x) * LATTICE > x_max - x_min
            or (cy + n_y) * LATTICE > y_max - y_min
        ):
            raise UnsupportedTiff(
                f"corner ({layout.x0}, {layout.y0}) is outside the grid of {self.name}"
            )
        return int(cy), int(cx)


def _crs_attrs(grid: Grid) -> dict[str, Any]:
    from rasterio.crs import CRS

    wkt = CRS.from_epsg(grid.epsg).to_wkt()
    return {
        "grid_mapping_name": "transverse_mercator",
        "epsg_code": f"EPSG:{grid.epsg}",
        "crs_wkt": wkt,
        "spatial_ref": wkt,
        "GeoTransform": " ".join(repr(v) for v in grid.geo_transform),
        "horizontal_datum": "WGS 84",
        "vertical_datum": "EGM96 geoid (EPSG:5773)",
    }


def ensure_zone(root: Any, grid: Grid) -> Any:
    """Create the group of a grid, if it is not there, and return it.

    Args:
        root: The store's root zarr group.
        grid: The group's grid.
    """
    from zarr.codecs import BytesCodec
    from zarr.codecs.numcodecs import Zlib

    if grid.name in root:
        return root[grid.name]
    if "license" not in root.attrs:
        root.attrs.update(GLOBAL_ATTRS)
    rows, cols = grid.shape
    title = f"GEM-15 on the grid of WGS 84 / UTM zone {grid.epsg % 100}{hemisphere(grid.epsg)}"
    if grid.dx or grid.dy:
        title += f", shifted {grid.dx} m east and {grid.dy} m south"
    group = root.create_group(
        grid.name, attributes={**GLOBAL_ATTRS, "title": f"{title} (EPSG:{grid.epsg})", LEDGER: {}}
    )
    group.create_array(
        "spatial_ref", shape=(), dtype="int32", fill_value=0, attributes=_crs_attrs(grid)
    )
    common = {
        "dtype": "float32",
        "fill_value": float("nan"),
        "serializer": BytesCodec(endian="little"),
        "compressors": [Zlib(level=6)],
    }
    meta = {
        "grid_mapping": "spatial_ref",
        "units": "m",
        "vertical_datum": "EGM96 geoid (EPSG:5773)",
        "license": LICENSE,
        "attribution": ATTRIBUTION,
    }
    group.create_array(
        STACK,
        shape=(rows, cols, len(DSM_BANDS)),
        chunks=(TILE, TILE, len(DSM_BANDS)),
        dimension_names=("y", "x", "dsm_band"),
        attributes={
            **meta,
            "long_name": "GEM-15 digital surface model and its statistics",
            "band_names": list(DSM_BANDS),
        },
        **common,
    )
    group.create_array(
        "dtm",
        shape=(rows, cols),
        chunks=(TILE, TILE),
        dimension_names=("y", "x"),
        attributes={**meta, "long_name": "GEM-15 digital terrain model"},
        **common,
    )
    return group


def _specs(source: Source, path: str, layout: TileLayout, row: int, col: int, third: bool) -> list:
    import icechunk

    location = f"{source.url}{path}"
    specs = []
    for (ty, tx), length in np.ndenumerate(layout.counts):
        if length == 0:
            continue
        index = [row + ty, col + tx] + ([0] if third else [])
        specs.append(
            icechunk.VirtualChunkSpec(
                index=index,
                location=location,
                offset=int(layout.offsets[ty, tx]),
                length=int(length),
            )
        )
    return specs


def _set_refs(session: Any, array_path: str, specs: list) -> None:
    failed = session.store.set_virtual_refs(array_path, specs, validate_containers=True)
    if failed:
        raise RuntimeError(
            f"{len(failed)} reference(s) of {array_path} are outside every virtual chunk "
            f"container, e.g. {failed[:3]}"
        )


def read_ledgers(repo: "icechunk.Repository", branch: str = "main") -> dict[str, dict[str, Any]]:
    """The ledger of every group: ``{cell: {"dsm": {"sig", "row", "col", "zone"}, "dtm": ...}}``.

    A file's entry names the group it was written to, and the signature (etag, size and
    modification time) of the object it was written from.
    """
    import zarr

    try:
        root = zarr.open_group(repo.readonly_session(branch).store, mode="r")
    except (FileNotFoundError, zarr.errors.GroupNotFoundError):
        return {}
    cells: dict[str, dict[str, Any]] = {}
    for name, group in root.groups():
        for cell, entry in dict(group.attrs.get(LEDGER, {})).items():
            for kind in ("dsm", "dtm"):
                if kind in entry:
                    cells.setdefault(cell, {})[kind] = {**entry[kind], "zone": name}
    return cells


@dataclasses.dataclass
class PassStats:
    """What one pass did."""

    listed: int = 0
    pending: int = 0
    written: int = 0
    failed: int = 0
    commits: int = 0


def _pending(
    cells: dict[str, CellFiles], ledger: dict[str, dict[str, Any]]
) -> list[tuple[CellFiles, tuple[str, ...]]]:
    """The cells with a file the ledger has not seen at this signature, and which files."""
    todo = []
    for cell in cells.values():
        entry = ledger.get(cell.cell, {})
        stale = tuple(
            kind
            for kind in ("dsm", "dtm")
            if getattr(cell, kind) and entry.get(kind, {}).get("sig") != getattr(cell, kind)
        )
        if stale:
            todo.append((cell, stale))
    return todo


def _batches(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def write_batch(
    repo: "icechunk.Repository",
    source: Source,
    batch: Sequence[tuple[CellFiles, tuple[str, ...]]],
    layouts: dict[tuple[str, str], TileLayout | Exception],
    branch: str = "main",
) -> tuple[int, int]:
    """Write the references of a batch of cells, and their ledger entries, in one commit.

    Returns ``(cells written, cells failed)``. A file whose header could not be read or whose
    grid does not fit is logged and left out of the ledger, so the next pass tries it again.
    A cell counts as written when any of its files was.
    """
    import icechunk
    import zarr

    session = repo.writable_session(branch)
    root = zarr.open_group(session.store, mode="a")
    entries: dict[str, dict[str, dict[str, Any]]] = {}
    written: set[str] = set()
    failed: set[str] = set()
    for cell, kinds in batch:
        for kind in kinds:
            path = cell.path(kind)
            try:
                layout = layouts[(cell.cell, path)]
                if isinstance(layout, Exception):
                    raise layout
                if layout.bands != (len(DSM_BANDS) if kind == "dsm" else 1):
                    raise UnsupportedTiff(f"{path}: {layout.bands} band(s) in the {kind}")
                grid = Grid.of(layout)
                row, col = grid.origin(layout)
            except Exception as exc:  # noqa: BLE001 - one bad file must not stop the batch
                logger.warning(f"gem15 {cell.cell} {kind}: skipped: {type(exc).__name__}: {exc}")
                failed.add(cell.cell)
                continue
            ensure_zone(root, grid)
            _set_refs(
                session,
                f"/{grid.name}/{STACK if kind == 'dsm' else 'dtm'}",
                _specs(source, path, layout, row, col, kind == "dsm"),
            )
            entry = entries.setdefault(grid.name, {}).setdefault(cell.cell, {"block": cell.block})
            entry[kind] = {"sig": getattr(cell, kind), "row": row, "col": col}
            written.add(cell.cell)
    for name, new in entries.items():
        group = root[name]
        ledger = dict(group.attrs.get(LEDGER, {}))
        for cell_id, entry in new.items():
            ledger[cell_id] = {**ledger.get(cell_id, {}), **entry}
        group.attrs[LEDGER] = ledger
    if written:
        session.commit(
            f"GEM-15: {len(written)} cell(s), {', '.join(sorted(entries))}",
            rebase_with=icechunk.ConflictDetector(),
            rebase_tries=5,
        )
    return len(written), len(failed)


def run_pass(
    repo: "icechunk.Repository",
    source: Source,
    *,
    batch_cells: int = 200,
    header_workers: int = 32,
    only: Sequence[str] | None = None,
    limit: int | None = None,
    branch: str = "main",
) -> PassStats:
    """Reference every cell that is new or changed since the last pass.

    Args:
        repo: The store, opened by :func:`open_writer`.
        source: Where the GeoTIFFs are.
        batch_cells: Cells per commit.
        header_workers: GeoTIFF headers read at once.
        only: Restrict the pass to these cell ids.
        limit: Stop after this many cells (for trials).
        branch: Branch to write.

    Returns:
        Counts of what the pass did.
    """
    cells = list_cells(source)
    if only:
        cells = {k: v for k, v in cells.items() if k in set(only)}
    todo = _pending(cells, read_ledgers(repo, branch))
    if limit is not None:
        todo = todo[:limit]
    stats = PassStats(listed=len(cells), pending=len(todo))
    logger.info(f"gem15: {len(cells):,} cell(s) listed, {len(todo):,} to write")
    for number, batch in enumerate(_batches(todo, batch_cells), 1):
        started = time.monotonic()
        jobs = [(cell.cell, cell.path(kind)) for cell, kinds in batch for kind in kinds]
        layouts = asyncio.run(_read_layouts(source, jobs, header_workers))
        layouts = {(cell, path): layout for (cell, path), layout in layouts.items()}
        written, failed = write_batch(repo, source, batch, layouts, branch)
        stats.written += written
        stats.failed += failed
        stats.commits += bool(written)
        logger.info(
            f"gem15: batch {number}: {written} cell(s) committed, {failed} skipped, "
            f"{time.monotonic() - started:.1f} s ({stats.written:,}/{stats.pending:,})"
        )
    return stats


def open_writer(prefix: str = STORE_PREFIX, source: Source | None = None) -> "icechunk.Repository":
    """Open (or create) the store for writing, with the source container authorised.

    Args:
        prefix: Store prefix. Resolved by the shared config, so ``ICECHUNK_LOCAL_PATH``
            redirects it to a local directory.
        source: Source whose URL is the virtual chunk container; Google's by default.
    """
    url = (source or Source.google()).url
    return open_virtual_repo(prefix, virtual_buckets=url, split_dim="y", split_size=16)


def open_reader(
    bucket: str = "us-west-2.opendata.source.coop",
    prefix: str = STORE_PREFIX,
    region: str = "us-west-2",
) -> "icechunk.Repository":
    """Open the published store read-only and anonymously, authorising Google's bucket."""
    import icechunk

    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region=region, anonymous=True)
    return icechunk.Repository.open(
        storage,
        authorize_virtual_chunk_access=icechunk.containers_credentials(
            {SOURCE_URL: icechunk.gcs_credentials(anonymous=True)}
        ),
    )


def open_zone(repo: "icechunk.Repository", group: str, branch: str = "main") -> "xr.Dataset":
    """One zone as a Dataset of the six variables on ``y`` and ``x`` coordinates in metres.

    Args:
        repo: A store opened with :func:`open_reader` (or any repository whose Google
            container is authorised).
        group: Zone group, e.g. from :func:`zone_group`.
        branch: Branch to read.

    Returns:
        ``dsm``, ``dsm_percentile_5``, ``dsm_percentile_50``, ``dsm_percentile_95``,
        ``dsm_std`` and ``dtm``, lazily; ``y`` descends. Pixel centres, so the first column
        is at the grid's left edge plus half a pixel.
    """
    import xarray as xr

    ds = xr.open_zarr(
        repo.readonly_session(branch).store, group=group, consolidated=False, chunks={}
    )
    stack = ds[STACK]
    names = list(stack.attrs["band_names"])
    x0, dx, _, y0, _, dy = (float(v) for v in ds["spatial_ref"].attrs["GeoTransform"].split())
    out = xr.Dataset(
        {name: stack.isel(dsm_band=i, drop=True) for i, name in enumerate(names)}
        | {"dtm": ds["dtm"]},
        attrs=ds.attrs,
    )
    out = out.assign_coords(
        x=("x", x0 + dx * (np.arange(out.sizes["x"]) + 0.5), {"units": "m", "axis": "X"}),
        y=("y", y0 + dy * (np.arange(out.sizes["y"]) + 0.5), {"units": "m", "axis": "Y"}),
        spatial_ref=ds["spatial_ref"],
    )
    return out


def open_around(
    repo: "icechunk.Repository",
    lon: float,
    lat: float,
    half_width_m: float = 1000.0,
    branch: str = "main",
) -> "xr.Dataset":
    """The window of a point's UTM zone within ``half_width_m`` of it, as :func:`open_zone`."""
    from rasterio.warp import transform

    group = zone_group(lon, lat)
    epsg = int(group.removeprefix("epsg_"))
    (x,), (y,) = transform("EPSG:4326", f"EPSG:{epsg}", [lon], [lat])
    ds = open_zone(repo, group, branch)
    return ds.sel(
        x=slice(x - half_width_m, x + half_width_m), y=slice(y + half_width_m, y - half_width_m)
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Command line: reference the new and changed cells once, or keep following the bucket."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default=STORE_PREFIX, help="store prefix")
    parser.add_argument(
        "--source-dir", help="a local directory laid out like gem-15 (tests, dry runs)"
    )
    parser.add_argument("--cells", help="comma-separated cell ids to restrict to, e.g. w5sm2")
    parser.add_argument("--limit", type=int, help="stop after this many cells")
    parser.add_argument("--batch-cells", type=int, default=200, help="cells per commit")
    parser.add_argument("--header-workers", type=int, default=32)
    parser.add_argument("--follow", action="store_true", help="keep passing")
    parser.add_argument(
        "--pause", type=int, default=604800, help="seconds between passes when following"
    )
    args = parser.parse_args(argv)
    source = Source.local(args.source_dir) if args.source_dir else Source.google()
    only = args.cells.split(",") if args.cells else None
    config = get_config()
    logger.info(f"gem15: store {config.store_path(args.prefix)}")
    while True:
        try:
            repo = open_writer(args.prefix, source)
            stats = run_pass(
                repo,
                source,
                batch_cells=args.batch_cells,
                header_workers=args.header_workers,
                only=only,
                limit=args.limit,
            )
            logger.info(
                f"gem15: pass done, {stats.written:,} cell(s) written, {stats.failed:,} skipped, "
                f"{stats.commits} commit(s)"
            )
        except Exception as exc:  # noqa: BLE001 - following: retry at the next pass
            if not args.follow:
                raise
            logger.exception(f"gem15: pass failed: {type(exc).__name__}: {exc}")
        if not args.follow:
            return 0
        waited = 0
        while waited < args.pause:  # saying so every ten minutes, for the box's stall guard
            time.sleep(min(600, args.pause - waited))
            waited += 600
            logger.info("gem15: waiting for the next pass")


if __name__ == "__main__":
    sys.exit(main())
