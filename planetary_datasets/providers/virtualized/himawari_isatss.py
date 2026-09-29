"""Virtual ingest of Himawari AHI full-disk into Icechunk, from ISatSS tiles.

Himawari-8/9 carry the AHI imager at 140.7E. The complete full-disk archive
(AHI-L1b-FLDK) is Himawari Standard Data -- `.DAT.bz2`, a custom binary format
under whole-file bz2 -- which cannot be referenced by chunk and so cannot be
virtualized at all. The netCDF product that *can* be is AHI-L2-FLDK-ISatSS:

    AHI-L2-FLDK-ISatSS/YYYY/MM/DD/HHMM/
        OR_HFD-<res>-B<bb>-M1C<cc>-T<ttt>_G<H8|H9>_s<YYYYDDDHHMMSSS>_c<...>.nc

Every scene is split into 88 tiles on a 10x10 grid, so one timestep is 88
files rather than one. That is ~88x the per-day file count of GOES ABI, and
it is the dominant cost of this ingest.

The tiles are, however, an unusually clean fit for a hand-built chunk
manifest. Each tile file holds *exactly one* Zarr chunk -- its chunk grid is
(1, 1) and its chunk shape equals its array shape -- and `tile_row_offset` /
`tile_column_offset` are exact multiples of the tile size. So a scene is
assembled by mapping each tile file to one entry of a 10x10 ChunkManifest,
with no pixel data read or copied. The 12 unoccupied corner cells (the
off-disk corners) are simply absent chunks, which Zarr serves as fill.

Resolutions, all on the same 10x10 grid of 88 tiles:
  - 0.5 km  C03            22000 x 22000, tile 2200
  - 1.0 km  C01, C02, C04  11000 x 11000, tile 1100
  - 2.0 km  C05 - C16       5500 x  5500, tile  550

Coverage: Himawari-8 2019-07 and 2020-2022; Himawari-9 2022-12 onwards.
"""

from __future__ import annotations

import datetime
import os
import re
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import numpy as np
import obstore as obs
import virtualizarr as vz
import xarray as xr
from loguru import logger
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.manifests import ChunkManifest, ManifestArray
from virtualizarr.manifests.utils import copy_and_replace_metadata

from planetary_datasets.config import Config
from planetary_datasets.providers.virtualized import virtual_repo

from . import goes_radf_common as common

if TYPE_CHECKING:
    import icechunk


# =============================================================================
# Constants
# =============================================================================
#: NOAA's public Open Data mirrors of the JMA archive. These are the identity
#: of the dataset rather than a deployment choice, so they are constants here;
#: the *output* store location is resolved through the shared config.
SATELLITE_BUCKET: dict[str, str] = {
    "himawari8": "s3://noaa-himawari8",
    "himawari9": "s3://noaa-himawari9",
}
SATELLITE_NAMES: dict[str, str] = {
    "himawari8": "Himawari-8",
    "himawari9": "Himawari-9",
}
#: Filename satellite token, used to reject stray files under a slot prefix.
SATELLITE_TOKEN: dict[str, str] = {"himawari8": "GH8", "himawari9": "GH9"}

PRODUCT = "AHI-L2-FLDK-ISatSS/"
PRODUCT_LABEL = "AHI-L2-FLDK-ISatSS"
PRODUCT_KEYS = ["AHI-L2-FLDK-ISatSS"]

ARCHIVE_START_DATE: dict[str, datetime.date] = {
    "himawari8": datetime.date(2019, 7, 1),
    "himawari9": datetime.date(2022, 12, 1),
}
#: Last day each satellite produced ISatSS, exclusive; None means "still going".
#: The two overlap: Himawari-9 started publishing on 2022-12-01 but Himawari-8
#: remained the operational satellite until the handover on 2022-12-13, so
#: cutting Himawari-8 off at the first Himawari-9 day would lose a fortnight.
ARCHIVE_END_DATE: dict[str, datetime.date | None] = {
    "himawari8": datetime.date(2022, 12, 14),
    "himawari9": None,
}

MIN_YEAR: dict[str, int] = {"himawari8": 2019, "himawari9": 2022}

EPOCH_THRESHOLD = np.datetime64("2015-01-01", "ns")

#: The payload variable in every tile.
IMAGE_VAR = "Sectorized_CMI"

BANDS: tuple[str, ...] = tuple(f"C{i:02d}" for i in range(1, 17))

BAND_RESOLUTION: dict[str, str] = {
    "C03": "005",
    "C01": "010", "C02": "010", "C04": "010",
    **{f"C{i:02d}": "020" for i in range(5, 17)},
}
_RESOLUTION_GRID: dict[str, int] = {"005": 22000, "010": 11000, "020": 5500}

#: Every resolution uses the same 10x10 tile grid.
TILE_GRID = 10
N_TILES = 88

#: The half-kilometre band, given its own pass as GOES C02 and GK-2A vi006 are.
HIGH_RES_BAND = "C03"

#: Root the stores sit under, alongside the other virtualized geostationary
#: stores rather than loose in ``bkr/geo`` beside the materialised ones.
#: Resolved against the configured bucket, or ``ICECHUNK_LOCAL_PATH``.
DEFAULT_STORE_ROOT = "bkr/geo/virtualized"

#: Logical store name, before the band and era discriminators are appended.
#: The satellite leads the name — ``himawari8_isatss_C01_2022-12-31`` — because
#: the two spacecraft's archives are separate datasets that happen to share a
#: product, and that is the layout already written to Source Cooperative.
#: ``None`` means "derive it from the satellite", which is what every caller
#: wants; pass a string only to redirect a run somewhere else entirely.
DEFAULT_STORE_BASE: str | None = None


def default_store_base(satellite: str) -> str:
    """Logical store name for one satellite, e.g. ``.../himawari9_isatss``."""
    return f"{DEFAULT_STORE_ROOT}/{satellite}_isatss"


#: AHI full disk runs a ten-minute cadence, so one day is ~144 scenes. Used as
#: the manifest split size so a split matches a day-sized commit batch.
SLOTS_PER_DAY = 144

#: Threads used to open one scene's tiles. The work is S3-latency bound,
#: not CPU bound, so this is the main throughput lever; override with the
#: HIMAWARI_TILE_THREADS environment variable.
TILE_THREADS = int(os.environ.get("HIMAWARI_TILE_THREADS", "16"))

#: Tiles carry no _FillValue attribute, so without this the absent corner
#: chunks decode through scale/offset to a large negative number instead of
#: masking to NaN.
FILL_VALUE = -32767

_KEEP_DATA_VARS = frozenset({IMAGE_VAR, "fixedgrid_projection"})

DEFAULT_LOADABLE_VARIABLES: tuple[str, ...] = ("x", "y", "fixedgrid_projection")

_FILENAME_RE = re.compile(
    r"OR_HFD-(?P<res>\d+)-B\d+-M\dC(?P<band>\d+)-T(?P<tile>\d+)_"
    r"G(?P<sat>H\d)_s(?P<start>\d{14})_"
)

RadFValidationError = common.RadFValidationError


class IncompleteBatch(RuntimeError):
    """Raised when a batch could not be stitched in full.

    Committing what did stitch would mark the day done with holes in it, and the store
    only appends along ``t``, so the missing timesteps could never be filled in.
    """


def grid_size(band: str) -> int:
    """Return the y=x pixel count of the full product for a band."""
    return _RESOLUTION_GRID[BAND_RESOLUTION[band.upper()]]


def tile_size(band: str) -> int:
    """Return the y=x pixel count of a single tile for a band."""
    return grid_size(band) // TILE_GRID


def _store(satellite: str):
    """Create an anonymous obstore handle for a Himawari bucket."""
    return common.make_store(SATELLITE_BUCKET[satellite])


def _filename(url: str) -> str:
    return url.rsplit("/", 1)[-1]


def parse_slot(url: str) -> np.datetime64:
    """Parse the sYYYYDDDHHMMSSS filename token into a datetime64[ns].

    Same token layout as GOES ABI, so the shared parser applies.
    """
    return common.parse_scan_start_to_datetime(url)


def _slot_key(url: str) -> str:
    m = _FILENAME_RE.match(_filename(url))
    if m is None:
        raise ValueError(f"Unexpected ISatSS filename: {_filename(url)!r}")
    return m.group("start")


def _tile_key(url: str) -> tuple[str, str]:
    """The ``(slot, tile)`` a URL belongs to: its identity on the scene grid."""
    m = _FILENAME_RE.match(_filename(url))
    if m is None:
        raise ValueError(f"Unexpected ISatSS filename: {_filename(url)!r}")
    return m.group("start"), m.group("tile")


def group_by_slot(urls: Iterable[str]) -> list[list[str]]:
    """Group a flat URL list into per-timestep tile sets, in time order.

    Deduplicated by ``(slot, tile)``, the way the GOES and GK-2A listers dedupe by scan
    slot. A re-delivered tile shows up in the listing twice; without this the slot reaches
    :func:`_mosaic_from_tiles` with 89 entries, two of them claiming the same grid cell,
    and the whole scene is discarded over a duplicate carrying the same data.
    """
    slots: dict[str, dict[str, str]] = {}
    for u in urls:
        slot, tile = _tile_key(u)
        # First wins, matching the sibling listers; the result is sorted either way.
        slots.setdefault(slot, {}).setdefault(tile, u)
    return [sorted(slots[k].values()) for k in sorted(slots)]


# =============================================================================
# Stitching
# =============================================================================
def _open_tile(
    url: str,
    registry: ObjectStoreRegistry,
    parser: Any,
    loadable: list[str],
) -> dict[str, Any]:
    """Open one tile and return its placement plus its single chunk entry."""
    vds = vz.open_virtual_dataset(
        url, registry=registry, parser=parser, loadable_variables=loadable
    )
    da = vds[IMAGE_VAR]
    attrs = vds.attrs
    th = int(attrs["product_tile_height"])
    tw = int(attrs["product_tile_width"])
    chunk_h, chunk_w = da.data.metadata.chunks[-2:]
    if th % chunk_h or tw % chunk_w:
        raise ValueError(
            f"Tile {th}x{tw} is not a whole number of {chunk_h}x{chunk_w} "
            f"chunks for {_filename(url)}"
        )

    # Most bands store a tile as a single chunk, but the half-kilometre band
    # (C03) splits its 2200px tile into 2x2 chunks of 1100. Place chunks, not
    # tiles, so every resolution works the same way.
    row_origin = int(attrs["tile_row_offset"]) // chunk_h
    col_origin = int(attrs["tile_column_offset"]) // chunk_w
    chunks: dict[tuple[int, int], Any] = {}
    for key, entry in da.data.manifest.dict().items():
        i, j = (int(v) for v in key.split("."))
        chunks[(row_origin + i, col_origin + j)] = entry

    return {
        "row": int(attrs["tile_row_offset"]) // th,
        "col": int(attrs["tile_column_offset"]) // tw,
        "rows": int(attrs["product_rows"]),
        "cols": int(attrs["product_columns"]),
        "th": th,
        "tw": tw,
        "chunk_h": chunk_h,
        "chunk_w": chunk_w,
        "chunks": chunks,
        "metadata": da.data.metadata,
        "var_attrs": dict(da.attrs),
        "ds": vds,
    }


def stitch_slot(
    urls: list[str],
    *,
    registry: ObjectStoreRegistry,
    parser: Any,
    loadable_variables: Iterable[str] = DEFAULT_LOADABLE_VARIABLES,
    max_workers: int = TILE_THREADS,
    require_full_scene: bool = True,
) -> xr.Dataset:
    """Assemble one timestep's tiles into a single virtual mosaic.

    Each tile contributes exactly one chunk, so the mosaic is built by
    placing chunk references on a 10x10 grid rather than by combining arrays.
    Absent tiles are left as absent chunks and read back as fill.
    """
    if not urls:
        raise ValueError("No tiles supplied for this slot.")
    loadable = list(loadable_variables)

    with ThreadPoolExecutor(max_workers) as pool:
        tiles = list(pool.map(
            lambda u: _open_tile(u, registry, parser, loadable), urls
        ))

    # Every tile handle is closed even when assembly raises. build_batch
    # swallows a bad scene and carries on, so without this a day of malformed
    # scenes leaks hundreds of handles in a module whose whole point is
    # bounding memory.
    try:
        return _mosaic_from_tiles(tiles, urls, require_full_scene=require_full_scene)
    finally:
        for tile in tiles:
            tile["ds"].close()


def _mosaic_from_tiles(
    tiles: list[dict[str, Any]],
    urls: list[str],
    *,
    require_full_scene: bool = True,
) -> xr.Dataset:
    """Place the opened tiles' chunks on the product grid.

    Does not close the tiles; :func:`stitch_slot` owns their lifetime.
    """
    if require_full_scene and len(tiles) != N_TILES:
        raise ValueError(
            f"Expected {N_TILES} tiles for a full scene, got {len(tiles)} "
            f"({_filename(urls[0])})"
        )

    first = tiles[0]
    rows, cols, th, tw = first["rows"], first["cols"], first["th"], first["tw"]
    chunk_h, chunk_w = first["chunk_h"], first["chunk_w"]

    shapes = {
        (t["rows"], t["cols"], t["th"], t["tw"], t["chunk_h"], t["chunk_w"])
        for t in tiles
    }
    if len(shapes) != 1:
        raise ValueError(f"Tiles disagree on product geometry: {shapes}")

    cells = {(t["row"], t["col"]) for t in tiles}
    if len(cells) != len(tiles):
        raise ValueError("Two tiles claim the same grid cell.")

    t = parse_slot(urls[0])

    # The chunk grid spans the whole product, on a (1, gy, gx) grid: the
    # leading 1 is the time axis, so the mosaic concatenates along `t`.
    grid = (rows // chunk_h, cols // chunk_w)
    entries: dict[str, Any] = {}
    for tile in tiles:
        for (r, c), entry in tile["chunks"].items():
            entries[f"0.{r}.{c}"] = entry
    manifest = ChunkManifest(entries, shape=(1, *grid))
    metadata = copy_and_replace_metadata(
        first["metadata"],
        new_shape=[1, rows, cols],
        new_chunks=[1, chunk_h, chunk_w],
    )
    mosaic = ManifestArray(chunkmanifest=manifest, metadata=metadata)

    var_attrs = dict(first["var_attrs"])
    # Tiles carry no _FillValue, so the absent corner chunks would otherwise
    # decode through scale_factor/add_offset instead of masking.
    var_attrs.setdefault("_FillValue", np.int16(FILL_VALUE))

    data_vars: dict[str, xr.DataArray] = {
        IMAGE_VAR: xr.DataArray(mosaic, dims=("t", "y", "x"), attrs=var_attrs),
    }

    # Assemble the full x/y coordinates from the tiles' own blocks. They are
    # tiny (one row of float64 per tile) and already loaded above.
    coords: dict[str, Any] = {"t": [t]}
    x_full = np.full(cols, np.nan, dtype="float64")
    y_full = np.full(rows, np.nan, dtype="float64")
    x_attrs = y_attrs = {}
    for tile in tiles:
        ds = tile["ds"]
        if "x" in ds.variables:
            x_full[tile["col"] * tw:(tile["col"] + 1) * tw] = ds["x"].values
            x_attrs = dict(ds["x"].attrs)
        if "y" in ds.variables:
            y_full[tile["row"] * th:(tile["row"] + 1) * th] = ds["y"].values
            y_attrs = dict(ds["y"].attrs)
    if not np.isnan(x_full).any():
        coords["x"] = xr.DataArray(x_full, dims=("x",), attrs=x_attrs)
    if not np.isnan(y_full).any():
        coords["y"] = xr.DataArray(y_full, dims=("y",), attrs=y_attrs)

    # CF grid mapping, carried per timestep like goes_imager_projection.
    proj_src = next(
        (t_["ds"]["fixedgrid_projection"] for t_ in tiles
         if "fixedgrid_projection" in t_["ds"].variables),
        None,
    )
    if proj_src is not None:
        proj = xr.DataArray(
            np.zeros(1, dtype="int8"), dims=("t",), attrs=dict(proj_src.attrs)
        )
        proj.encoding["_FillValue"] = None
        data_vars["fixedgrid_projection"] = proj

    out = xr.Dataset(data_vars, coords=coords)
    out["t"].attrs.update({"long_name": "scene start time", "standard_name": "time"})
    out.attrs.update({
        k: v for k, v in first["ds"].attrs.items()
        if k not in ("tile_row_offset", "tile_column_offset",
                     "tile_center_latitude", "tile_center_longitude")
    })
    return out


def build_batch(
    urls: list[str],
    *,
    registry: ObjectStoreRegistry,
    parser: Any = None,
    preprocess_fn: Any = None,
    loadable_variables: Iterable[str] = DEFAULT_LOADABLE_VARIABLES,
    max_workers: int = TILE_THREADS,
    allow_missing_scenes: bool = False,
) -> xr.Dataset:
    """Stitch every timestep in a batch and concatenate them along `t`.

    Signature matches the `open_batch_fn` hook in goes_radf_common, which
    calls this instead of open_virtual_mfdataset for instruments whose
    timestep spans many files.

    A scene that fails to stitch fails the batch. Dropping it and committing
    the rest looks like resilience and is not: the store is append-only along
    `t`, `guard_append_order` reports any non-zero count as "already covered"
    and `require_committed` only asserts `> 0`, so the re-run skips the day and
    the missing timesteps can never be filled in. The common cause is a slot
    that was still uploading when the day was listed, which is exactly the case
    a retry fixes.

    Pass `allow_missing_scenes=True` to commit what did stitch and log the rest.
    That is only for a day the archive genuinely never completed, where the
    alternative is no data at all.
    """
    parser = parser or vz.parsers.HDFParser()
    slots = group_by_slot(urls)
    if not slots:
        raise ValueError("No ISatSS tiles in this batch.")

    scenes: list[xr.Dataset] = []
    failures: list[str] = []
    for slot_urls in slots:
        try:
            scenes.append(stitch_slot(
                slot_urls, registry=registry, parser=parser,
                loadable_variables=loadable_variables, max_workers=max_workers,
            ))
        except Exception as e:
            failures.append(f"{_slot_key(slot_urls[0])}: {type(e).__name__}: {e}")

    if failures and not allow_missing_scenes:
        raise IncompleteBatch(
            f"{len(failures)} of {len(slots)} scene(s) failed to stitch: "
            f"{failures[:3]}{'...' if len(failures) > 3 else ''}. Refusing to commit a "
            "batch with holes in it; the store only appends along `t`, so these "
            "timesteps could not be added later. Retry, or pass allow_missing_scenes=True "
            "if the archive never published them."
        )
    if failures:
        logger.warning(
            f"skipped {len(failures)} of {len(slots)} scene(s): "
            f"{failures[:2]}{'...' if len(failures) > 2 else ''}"
        )
    if not scenes:
        raise ValueError(f"Every scene in this batch failed: {failures[:3]}")

    return xr.concat(scenes, dim="t", **common._CONCAT_KWARGS)


def probe_select(urls: list[str], n: int) -> list[str]:
    """Pick a single tile to probe the day with.

    Whether two days can share a store is decided by the tile array metadata
    -- dtype, codec pipeline, chunk shape -- and by the product geometry, all
    of which one tile carries. Probing a whole 88-tile scene (or two) would
    cost 88-176 file opens per day to learn what one open tells us, which on a
    year-long walk is the difference between minutes and hours.

    The first tile of the day's first scene is used: the newest slots are
    often still being uploaded and are the ones that come back incomplete.
    """
    slots = group_by_slot(urls)
    if not slots:
        return []
    return slots[0][:1]


def probe_open(urls: list[str], **kwargs: Any) -> xr.Dataset:
    """Open a probe sample as a mosaic with the real scene's geometry.

    Stitching a single tile yields the same 5500x5500 shape, chunk grid and
    codecs a full scene would, just with 87 absent chunks instead of 12, so
    combine_failure_reason compares exactly what it would at ingest time.
    """
    kwargs.setdefault("parser", vz.parsers.HDFParser())
    kwargs.pop("preprocess_fn", None)
    return stitch_slot(urls, require_full_scene=False, **kwargs)


# =============================================================================
# Archive listing
# =============================================================================
def _kids(store, prefix: str) -> list[str]:
    return common.common_prefix_children(store, prefix)


def iter_days(
    store=None,
    product: str = PRODUCT,
    min_year: int = 0,
    product_keys: list[str] | None = None,
) -> Iterator[tuple[int, int]]:
    """Yield (year, doy) for every day present, chronologically.

    Signature matches goes_radf_common.iter_days for injection as
    `list_days_fn`.
    """
    for year in sorted(y for y in _kids(store, product) if y.isdigit()):
        if int(year) < min_year:
            continue
        for month in sorted(m for m in _kids(store, f"{product}{year}/") if m.isdigit()):
            for day in sorted(
                d for d in _kids(store, f"{product}{year}/{month}/") if d.isdigit()
            ):
                try:
                    date = datetime.date(int(year), int(month), int(day))
                except ValueError:
                    continue
                yield (date.year, date.timetuple().tm_yday)


def list_day_files(
    store=None,
    bucket: str = "",
    product: str = PRODUCT,
    year: int | None = None,
    doy: int | None = None,
    channel: str = "",
    *,
    product_reproc: str | None = None,
    reproc: bool = False,
    satellite: str | None = None,
) -> list[str]:
    """Every tile URL for one band on one day, across all slots.

    One paginated listing of the day prefix covers all 144 slot directories.
    """
    band = channel.upper()
    res = BAND_RESOLUTION[band]
    date = common._date_from_doy(year, doy)
    day_prefix = f"{product}{date.year}/{date.month:02d}/{date.day:02d}/"
    want_res = f"HFD-{res}-"
    want_band = f"C{int(band[1:]):02d}-T"
    token = SATELLITE_TOKEN.get(satellite or "", "")

    urls: list[str] = []
    for page in obs.list(store, prefix=day_prefix):
        for o in page:
            path = o["path"]
            if not path.endswith(".nc"):
                continue
            fname = _filename(path)
            if want_res not in fname or want_band not in fname:
                continue
            if token and f"_{token}_" not in fname:
                continue
            urls.append(f"{bucket}/{path}")
    return sorted(urls)


def probe_list_day_files(
    store=None,
    bucket: str = "",
    product: str = PRODUCT,
    year: int | None = None,
    doy: int | None = None,
    channel: str = "",
    *,
    satellite: str | None = None,
    **kwargs: Any,
) -> list[str]:
    """List only the day's first slot, for probing.

    A day prefix holds every band and every tile for all 142 slots -- roughly
    200,000 objects -- so listing it costs ~200 paginated calls. The probe
    needs one representative tile, so this lists the slot directories (a
    single delimiter call) and then only the first of them.

    ``satellite`` applies the same GH8/GH9 filename filter as
    :func:`list_day_files`. Without it the probe can anchor an era on a stray
    tile that the ingest listing will then exclude, so the combinability
    decision is made against a file that is never ingested.
    """
    band = channel.upper()
    res = BAND_RESOLUTION[band]
    date = common._date_from_doy(year, doy)
    day_prefix = f"{product}{date.year}/{date.month:02d}/{date.day:02d}/"
    slots = common.common_prefix_children(store, day_prefix)
    if not slots:
        return []

    token = SATELLITE_TOKEN.get(satellite, "") if satellite else ""
    want_res, want_band = f"HFD-{res}-", f"C{int(band[1:]):02d}-T"
    for slot in sorted(slots):
        urls: list[str] = []
        for page in obs.list(store, prefix=f"{day_prefix}{slot}/"):
            for o in page:
                fname = _filename(o["path"])
                if not (o["path"].endswith(".nc")
                        and want_res in fname and want_band in fname):
                    continue
                if token and f"_{token}_" not in fname:
                    continue
                urls.append(f"{bucket}/{o['path']}")
        if urls:
            return sorted(urls)
    return []


def make_probe_list_day_files(satellite: str):
    """Bind a satellite into the probe listing signature."""
    def _list(store, bucket, product, year, doy, channel, **kwargs):
        return probe_list_day_files(
            store, bucket, product, year, doy, channel, satellite=satellite
        )
    return _list


def make_day_urls_fn(satellite: str, band: str):
    """Return a callable resolving one (year, doy) to that band's full day."""
    store = _store(satellite)
    bucket = SATELLITE_BUCKET[satellite]

    def _urls(day: tuple[int, int]) -> list[str]:
        return list_day_files(
            store, bucket, PRODUCT, day[0], day[1], band, satellite=satellite
        )
    return _urls


def make_list_day_files(satellite: str):
    """Bind a satellite into the list_day_files signature the core expects."""
    def _list(store, bucket, product, year, doy, channel, **kwargs):
        kwargs.pop("product_reproc", None)
        kwargs.pop("reproc", None)
        return list_day_files(
            store, bucket, product, year, doy, channel, satellite=satellite
        )
    return _list


def days_to_ingest(
    satellite: str,
    band: str,
    *,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> list[tuple[tuple[int, int], list[str]]]:
    """Eagerly enumerate (day, tile urls) for a band over a date range."""
    store = _store(satellite)
    bucket = SATELLITE_BUCKET[satellite]
    out: list[tuple[tuple[int, int], list[str]]] = []
    for year, doy in iter_days(store, PRODUCT, MIN_YEAR[satellite], PRODUCT_KEYS):
        date = common._date_from_doy(year, doy)
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            break
        out.append((
            (year, doy),
            list_day_files(store, bucket, PRODUCT, year, doy, band,
                           satellite=satellite),
        ))
    return out


# =============================================================================
# Preprocess
# =============================================================================
def preprocess(ds: xr.Dataset) -> xr.Dataset:
    """No-op: stitch_slot already emits the final schema.

    Kept so the shared ingest loop can call a preprocess uniformly.
    """
    return ds


# =============================================================================
# Smoke test
# =============================================================================
def smoke_test(
    satellite: str = "himawari9",
    band: str = "C13",
    date: datetime.date | None = None,
) -> xr.Dataset:
    """Stitch a single scene and return it."""
    store = _store(satellite)
    bucket = SATELLITE_BUCKET[satellite]
    for year, doy in iter_days(store, PRODUCT, MIN_YEAR[satellite], PRODUCT_KEYS):
        d = common._date_from_doy(year, doy)
        if date is not None and d != date:
            continue
        urls = list_day_files(store, bucket, PRODUCT, year, doy, band,
                              satellite=satellite)
        if urls:
            slot = group_by_slot(urls)[0]
            logger.info(f"stitching {len(slot)} tiles for {d} {band}")
            return stitch_slot(
                slot,
                registry=ObjectStoreRegistry({bucket: store}),
                parser=vz.parsers.HDFParser(),
            )
    raise ValueError(f"No ISatSS files found for {satellite} {band}.")


# =============================================================================
# Store location
# =============================================================================
def store_prefix_for(
    satellite: str,
    band: str,
    era: str | None = None,
    base: str | None = DEFAULT_STORE_BASE,
) -> str:
    """Store prefix for one satellite and band, optionally for one era.

    With no ``base`` the name is derived from the satellite, giving
    ``bkr/geo/virtualized/himawari9_isatss_C13_2025-12-31.icechunk``. An
    explicit ``base`` is used as given, so a run can be redirected without the
    satellite being spliced into the middle of the name.
    """
    return virtual_repo.store_prefix(
        base or default_store_base(satellite), band.upper(), era
    )


def open_repo(
    satellite: str,
    band: str,
    era: str | None = None,
    *,
    base: str | None = DEFAULT_STORE_BASE,
    config: Config | None = None,
) -> "icechunk.Repository":
    """Open or create the store for one satellite and band, via the config.

    Both Himawari buckets are registered as virtual chunk containers, so a
    store still reads if references from the other satellite are ever mixed in
    — which happens across the Himawari-8 to Himawari-9 handover.
    """
    return virtual_repo.open_virtual_repo(
        store_prefix_for(satellite, band, era, base),
        virtual_buckets=SATELLITE_BUCKET.values(),
        split_size=SLOTS_PER_DAY,
        config=config,
    )


# =============================================================================
# Ingest
# =============================================================================
def ingest_day(
    satellite: str,
    date: datetime.date,
    band: str,
    *,
    repo: icechunk.Repository | None = None,
    base: str | None = DEFAULT_STORE_BASE,
    config: Config | None = None,
    branch: str = "main",
    group: str | None = "",
    **kwargs: Any,
) -> int:
    """Ingest a single day for one band. Returns the scenes now stored for it.

    This is the entry point the Dagster daily partition calls. Enumerating the
    day directly avoids walking the whole archive listing to reach one day.

    The result is read back from the store rather than counted from the listing,
    because the shared engine logs and swallows a failed batch: without the read
    a partition that committed nothing would still report success.

    Raises:
        OutOfOrderPartition: when the store already holds a newer day.
        NothingCommitted: when the ingest ran but committed nothing.
    """
    band = band.upper()
    if repo is None:
        repo = open_repo(satellite, band, base=base, config=config)

    what = f"{SATELLITE_NAMES[satellite]} {band}"
    # Same `group` for the guards as for the write below; reading a different group
    # would report a written day as absent and an absent day as writable.
    already = virtual_repo.guard_append_order(repo, date, what, branch=branch, group=group)
    if already:
        logger.info(f"{what}: {date.isoformat()} already holds {already} scene(s), skipping")
        return already

    store = _store(satellite)
    bucket = SATELLITE_BUCKET[satellite]
    doy = date.timetuple().tm_yday
    urls = list_day_files(
        store, bucket, PRODUCT, date.year, doy, band, satellite=satellite
    )
    if not urls:
        logger.warning(f"{what}: no tiles for {date.isoformat()}")
        return 0

    common.ingest_all_days(
        repo,
        band,
        satellite_name=SATELLITE_NAMES[satellite],
        satellite=satellite,
        product_label=PRODUCT_LABEL,
        archive_start_date=ARCHIVE_START_DATE[satellite],
        all_days=[((date.year, doy), urls)],
        preprocess_fn=preprocess,
        branch=branch,
        group=group,
        bucket=bucket,
        loadable_variables=DEFAULT_LOADABLE_VARIABLES,
        keep_data_vars=_KEEP_DATA_VARS,
        epoch_threshold=EPOCH_THRESHOLD,
        channel_label=band,
        grid_size=grid_size(band),
        open_batch_fn=build_batch,
        day_urls_fn=make_day_urls_fn(satellite, band),
        scan_start_fn=parse_slot,
        **kwargs,
    )
    return virtual_repo.require_committed(repo, date, what, branch=branch, group=group)


def ingest_backwards(
    satellite: str,
    band: str,
    *,
    repo_factory: Any,
    end_date: datetime.date | None = None,
    start_date: datetime.date | None = None,
    first_store_suffix: str | None = None,
    branch: str = "main",
    group: str | None = "",
    batch_size: int = 1,
    zarr_async_concurrency: int | None = None,
    log_dir: str | None = None,
    max_eras: int | None = None,
    registry: ObjectStoreRegistry | None = None,
) -> list[str]:
    """Walk one band backwards from end_date, one store per combinable era."""
    band = band.upper()
    store = _store(satellite)
    bucket = SATELLITE_BUCKET[satellite]
    return common.ingest_backwards(
        band,
        repo_factory=repo_factory,
        satellite_name=SATELLITE_NAMES[satellite],
        satellite=satellite,
        product_label=PRODUCT_LABEL,
        archive_start_date=ARCHIVE_START_DATE[satellite],
        store=store,
        bucket=bucket,
        product=PRODUCT,
        min_year=MIN_YEAR[satellite],
        product_keys=PRODUCT_KEYS,
        loadable_variables=DEFAULT_LOADABLE_VARIABLES,
        epoch_threshold=EPOCH_THRESHOLD,
        end_date=end_date,
        start_date=start_date or ARCHIVE_START_DATE[satellite],
        first_store_suffix=first_store_suffix,
        branch=branch,
        group=group,
        batch_size=batch_size,
        zarr_async_concurrency=zarr_async_concurrency,
        log_dir=log_dir,
        keep_data_vars=_KEEP_DATA_VARS,
        registry=registry,
        max_eras=max_eras,
        channel_label=band,
        grid_size=grid_size(band),
        list_days_fn=iter_days,
        list_day_files_fn=make_list_day_files(satellite),
        era_preprocess_fn=preprocess,
        open_batch_fn=build_batch,
        probe_select_fn=probe_select,
        probe_open_fn=probe_open,
        probe_list_fn=make_probe_list_day_files(satellite),
        day_urls_fn=make_day_urls_fn(satellite, band),
    )
