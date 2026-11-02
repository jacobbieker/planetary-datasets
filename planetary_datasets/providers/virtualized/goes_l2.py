r"""Virtual Icechunk stores of the GOES-R ABI Level 2 products on NOAA Open Data.

Every gridded ``ABI-L2-*`` product of GOES-16/17/18/19 is referenced, not copied: each
store holds virtual chunk references into ``s3://noaa-goes{NN}`` (us-east-1, anonymous)
plus the small per-file metadata, appended along ``t``. Google Cloud's
``gs://gcp-public-data-goes-{NN}`` holds byte-identical files (checked across all 320
product x satellite combinations), so it is a mirror and nothing points at it.

One store per satellite, product and scan sector::

    bkr/geo/virtualized/goes19_l2/acm_f.icechunk           ABI-L2-ACMF, full disk
    bkr/geo/virtualized/goes19_l2/cmip_c_c13.icechunk      ABI-L2-CMIPC, channel 13
    bkr/geo/virtualized/goes19_l2/mcmip_f.icechunk         all 16 channels, as published

CMIP is split by channel like the L1b radiance stores; MCMIP keeps its 16 channels in
one file and so in one store. A product whose layout changes (variables, grid, chunking
or codecs, as algorithm updates do) continues in a generation named for the first day of
the new layout (``acm_f_15062024.icechunk``), found again by the layout's signature.

Variables keep NOAA's names (``CMI``, ``ACM``, ``DQF``) with their ``long_name`` and
units: virtual stores are written without the renaming the materialised stores get.

The mesoscale sectors (``--sectors M1,M2``) are one-minute scans, 1440 files a day per
sector, so each is split by year::

    bkr/geo/virtualized/goes19_l2_meso/acm_m1_2026.icechunk
    bkr/geo/virtualized/goes19_l2_meso/cmip_m2_c13_2026.icechunk

A mesoscale sector's box moves (about every other day in a month of GOES-19 samples, never
within a day), so its stores have no ``x``/``y`` index: each time carries its own
``x_scan_angle``/``y_scan_angle`` along ``t``, with the scalar ``x_image``/``y_image``.

Not covered here: the point products (derived motion winds and the GLM lightning
events), which cannot be appended as references because each file's length differs;
``planetary_datasets.providers.goes_l2_points`` materialises them.

Each day is written in one commit and recorded in the store's ledger, so a run resumes
where it stopped. The last few days go first, newest first, then the backlog oldest
first; times are therefore not sorted in the store.

    python -m planetary_datasets.providers.virtualized.goes_l2 --satellite 19 --follow
    python -m planetary_datasets.providers.virtualized.goes_l2 --satellite 19 --follow \
        --sectors M1,M2
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Sequence

import numpy as np
from loguru import logger

from planetary_datasets.config import Config, get_config

SATELLITES = {16: "noaa-goes16", 17: "noaa-goes17", 18: "noaa-goes18", 19: "noaa-goes19"}
SOURCE_REGION = "us-east-1"
STORE_ROOT = "bkr/geo/virtualized"
#: Files that would not open while others of the same day did, one ledger per product
#: (``<url>\t<error>`` lines). A damaged granule (GOES-18 ACHA2KMM1 2025-100 16:09 has a
#: bad HDF5 metadata checksum) left the HDF5 library unstable: the lane segfaulted minutes
#: after every attempt at that day, so later passes do not open these files again.
#: ``GOES_L2_UNREADABLE_DIR`` overrides the default, ``<data_dir>/goes_l2_unreadable``.
UNREADABLE_DIR: pathlib.Path | None = (
    pathlib.Path(os.environ["GOES_L2_UNREADABLE_DIR"])
    if os.environ.get("GOES_L2_UNREADABLE_DIR")
    else None
)
_UNREADABLE_LOCK = threading.Lock()
#: Sectors archived by default: full disk and CONUS (or PACUS for the west satellites).
SECTORS = ("F", "C")
#: The two mesoscale sectors. Both are published under one prefix (``ABI-L2-ACMM``) and
#: told apart by the file name (``OR_ABI-L2-ACMM1-M6_G19_...``).
MESO_SECTORS = ("M1", "M2")
#: Where the mesoscale stores live; they are split by year (see :class:`Product`).
MESO_ROOT = f"{STORE_ROOT}/goes{{satellite}}_l2_meso"
#: One-minute scans: 1440 files a day per mesoscale sector.
MESO_FILES_PER_DAY = 1440
#: Products whose files are not on a fixed grid: winds and lightning are lists of
#: observations of a different length in every file.
POINT_PRODUCTS = ("DMW", "DMWV")
#: Covered elsewhere: ``planetary_datasets.providers.goes_l2_points`` materialises them.
DEFERRED = (
    "ABI-L2-DMW*, ABI-L2-DMWV* (derived motion winds) and GLM-L2-LCFA (lightning): "
    "per-file lengths differ, so they are materialised by goes_l2_points",
)
#: Root attributes.
LEDGER_ATTR = "days_written"
SIGNATURE_ATTR = "layout_signature"
#: Grid coordinates loaded when probing a file, so files on different grids are told apart.
GRID_AXES = ("x", "y")
#: Days written newest first before the backlog.
RECENT_DAYS = 7
#: A day is written once it has ended this long ago, so it is complete upstream.
SETTLE = dt.timedelta(hours=3)
#: Files opened at once when reading a day's metadata.
OPEN_WORKERS = 16
#: The image grids: variables on them are referenced, everything else is loaded inline.
GRID_DIMS = (("y", "x"), ("lat", "lon"))
_COMBINE = {
    "combine": "nested",
    "concat_dim": "t",
    "combine_attrs": "drop_conflicts",
    "coords": "minimal",
    "compat": "override",
}


@dataclasses.dataclass(frozen=True)
class Product:
    """One store's source: a satellite, an ABI L2 product, a sector, perhaps a channel."""

    satellite: int
    code: str  # e.g. "ACM", "CMIP", "ACHA2KM"
    sector: str  # "F", "C", or a mesoscale sector "M1" / "M2"
    channel: int | None = None
    #: Mesoscale stores only: the year this store holds. A sector writes 1440 files a
    #: day, so one store per year keeps each store's manifests a manageable size.
    year: int | None = None

    @property
    def meso(self) -> bool:
        return self.sector in MESO_SECTORS

    @property
    def bucket(self) -> str:
        return SATELLITES[self.satellite]

    @property
    def source_prefix(self) -> str:
        return f"ABI-L2-{self.code}{self.sector[0]}"

    @property
    def _stem(self) -> str:
        tail = f"_c{self.channel:02d}" if self.channel else ""
        year = f"_{self.year}" if self.year else ""
        return f"{self.code.lower()}_{self.sector.lower()}{tail}{year}"

    @property
    def name(self) -> str:
        return f"goes{self.satellite}_l2_{self._stem}"

    @property
    def store_prefix(self) -> str:
        if self.meso:
            return f"{MESO_ROOT.format(satellite=self.satellite)}/{self._stem}.icechunk"
        return f"{STORE_ROOT}/goes{self.satellite}_l2/{self._stem}.icechunk"

    @property
    def files_per_day(self) -> int:
        return MESO_FILES_PER_DAY if self.meso else 288 if self.sector == "C" else 144

    def wants(self, key: str) -> bool:
        """Whether a file in the product's prefix belongs to this store."""
        name = key.rsplit("/", 1)[-1]
        if not name.endswith(".nc"):
            return False
        # OR_ABI-L2-ACMM1-M6_G19_s...: both mesoscale sectors share one prefix.
        if self.meso and f"L2-{self.code}{self.sector}-" not in name:
            return False
        if self.channel is None:
            return True
        # OR_ABI-L2-CMIPF-M6C13_G19_s...: mode then channel after the product.
        return f"C{self.channel:02d}_G" in name


def products_from_listing(
    satellite: int, prefixes: Iterable[str], sectors: Sequence[str] = SECTORS
) -> list[Product]:
    """The stores to build from a bucket's top-level product prefixes."""
    out = []
    for prefix in sorted(set(prefixes)):
        prefix = prefix.rstrip("/")
        if not prefix.startswith("ABI-L2-"):
            continue
        body = prefix[len("ABI-L2-") :]
        code, letter = body[:-1], body[-1]
        if code in POINT_PRODUCTS:
            continue
        wanted = [s for s in sectors if s[0] == letter]
        for sector in wanted:
            if code == "CMIP":
                out += [Product(satellite, code, sector, ch) for ch in range(1, 17)]
            else:
                out.append(Product(satellite, code, sector))
    return out


def by_year(store, product: Product) -> list[Product]:
    """A mesoscale product as one store per year it has data for."""
    years = [y for y in _children(store, f"{product.source_prefix}/") if y.isdigit()]
    return [dataclasses.replace(product, year=int(y)) for y in years if int(y) >= 2017]


def scan_start(key: str) -> dt.datetime:
    """The scan start in a GOES file name (``..._s20263041200203_...``)."""
    part = next(p for p in key.rsplit("/", 1)[-1].split("_") if p.startswith("s"))
    return dt.datetime.strptime(part[1:14], "%Y%j%H%M%S")


# ----------------------------------------------------------------------------- source


def source_store(bucket: str):
    import obstore

    return obstore.store.from_url(f"s3://{bucket}", region=SOURCE_REGION, skip_signature=True)


def source(product: Product) -> tuple[str, object]:
    """The URL prefix references point at, and an object store reading it."""
    return f"s3://{product.bucket}", source_store(product.bucket)


def _children(store, prefix: str) -> list[str]:
    import obstore

    listed = obstore.list_with_delimiter(store, prefix=prefix)
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in listed["common_prefixes"])


def available_days(store, product: Product) -> list[dt.date]:
    """Every day the product has a folder for, skipping mis-stamped year folders."""
    days = []
    years = [str(product.year)] if product.year else _children(store, f"{product.source_prefix}/")
    for year in years:
        if not year.isdigit() or int(year) < 2017:
            continue  # a few products carry a stray 2000/ folder of mis-stamped files
        for doy in _children(store, f"{product.source_prefix}/{year}/"):
            if doy.isdigit():
                days.append(dt.date(int(year), 1, 1) + dt.timedelta(int(doy) - 1))
    return days


def day_files(store, product: Product, day: dt.date) -> list[str]:
    """The keys of the product's files for one day, in scan order."""
    import obstore

    prefix = f"{product.source_prefix}/{day:%Y}/{day.timetuple().tm_yday:03d}/"
    keys = []
    for batch in obstore.list(store, prefix=prefix):
        keys += [o["path"] for o in batch if product.wants(o["path"])]
    return sorted(keys, key=scan_start)


def order_days(days: Sequence[dt.date], today: dt.date, recent: int = RECENT_DAYS) -> list[dt.date]:
    """The last ``recent`` days newest first, then the rest oldest first."""
    cutoff = today - dt.timedelta(days=recent)
    newer = sorted((d for d in days if d >= cutoff), reverse=True)
    return newer + sorted(d for d in days if d < cutoff)


# ----------------------------------------------------------------------------- virtual


def _virtual_names(ds) -> list[str]:
    from virtualizarr.manifests import ManifestArray

    return [
        name
        for name, var in ds.variables.items()
        if isinstance(var.data, ManifestArray) and tuple(var.dims[-2:]) in GRID_DIMS
    ]


def _add_time(ds):
    """Every data variable gains a leading ``t`` from the file's scalar ``t``."""
    import xarray as xr

    if "t" not in ds.coords and "t" not in ds.variables:
        raise ValueError("granule has no 't'")
    t = ds["t"]
    out = ds.drop_vars("t")
    variables = {}
    for name, da in out.data_vars.items():
        expanded = da.expand_dims({"t": [t.values]}, axis=0)
        expanded.encoding = dict(da.encoding)
        variables[name] = expanded
    new = xr.Dataset(variables, coords=out.coords, attrs=ds.attrs)
    new["t"].attrs.update(t.attrs)
    new["t"].encoding.update(t.encoding)
    for var in new.variables.values():
        if (
            "_FillValue" not in var.encoding
            and np.dtype(var.encoding.get("dtype", var.dtype)).kind == "f"
        ):
            var.encoding["_FillValue"] = None
    return new


def _add_time_meso(ds):
    """A mesoscale granule along ``t``, keeping the granule's own position.

    The sector box moves (about every other day; never within a day in the samples
    taken), so ``x``/``y`` are not one grid for the store: they become the per-time
    ``x_scan_angle``/``y_scan_angle`` (radians), as do the scalar coordinates that
    describe the box (``x_image``, ``y_image``), and ``x``/``y`` are left as bare image
    dimensions without an index.
    """
    renames = {n: f"{n}_scan_angle" for n in ("x", "y") if n in ds.variables}
    ds = ds.drop_indexes([n for n in renames if n in ds.indexes]).rename_vars(renames)
    per_file = [n for n in ds.coords if n != "t" and n not in ds.indexes]
    return _add_time(ds.reset_coords(per_file))


def _unreadable_ledger(product: Product) -> pathlib.Path:
    """The ledger file of ``product``, under the configured data directory by default."""
    root = UNREADABLE_DIR or pathlib.Path(get_config().data_dir) / "goes_l2_unreadable"
    return root / f"{product.name}.txt"


def known_unreadable(product: Product) -> set[str]:
    """URLs of ``product`` recorded as unreadable by an earlier pass."""
    path = _unreadable_ledger(product)
    if not path.is_file():
        return set()
    return {line.split("\t", 1)[0] for line in path.read_text().splitlines() if line.strip()}


def _damage(url: str) -> str | None:
    """Download one file and walk every object's attributes and chunk index with h5py.

    Args:
        url: The file's full URL (``s3://`` on the box, ``file://`` in tests).

    Returns:
        The error if the file's own bytes are damaged; None if it reads cleanly.

    Raises:
        RuntimeError: The file could not be downloaded. It is not known to be damaged, so
            its day fails and is retried by a later pass.
    """
    import tempfile

    import h5py

    from planetary_datasets.common.download import download_one

    options = {"anon": True} if url.startswith("s3://") else None
    with tempfile.TemporaryDirectory(prefix="goes-l2-check-") as tmp:
        path = download_one(url, pathlib.Path(tmp) / "granule.nc", storage_options=options)
        if path is None:
            raise RuntimeError(f"could not download {url} to check it; retrying the day later")

        def check(_name, obj):
            list(obj.attrs.items())
            # HDFParser also reads every chunk's offset from the chunk index.
            if isinstance(obj, h5py.Dataset) and obj.chunks is not None:
                for index in range(obj.id.get_num_chunks()):
                    obj.id.get_chunk_info(index)
            return None  # visititems stops at the first callback that returns non-None

        try:
            with h5py.File(path, "r") as f:
                list(f.attrs.items())
                f.visititems(check)
        except Exception as exc:  # noqa: BLE001 - any HDF5 error means the bytes are bad
            return f"{type(exc).__name__}: {exc}"
    return None


def _record_unreadable(product: Product, url: str, error: str) -> None:
    """Append ``url`` to the product's ledger, so later passes never open it."""
    path = _unreadable_ledger(product)
    with _UNREADABLE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as ledger:
            ledger.write(f"{url}\t{error}\n")
            ledger.flush()
            os.fsync(ledger.fileno())
    logger.warning(f"{product.name}: {url} recorded as unreadable; later passes skip it")


def open_day(keys: Sequence[str], product: Product):
    """The day's files as virtual datasets along ``t``, one per layout (usually one).

    The grid variables stay references; everything small (times, bounds, projection,
    per-file statistics) is loaded and stored inline. Files whose grid variables differ
    or chunking from the others are combined separately; unreadable files are left out.
    """
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry

    url_base, store = source(product)
    registry = ObjectStoreRegistry({url_base: store})
    parser = vz.parsers.HDFParser()
    listed = [f"{url_base}/{key}" for key in keys]
    skip = known_unreadable(product)
    urls = [url for url in listed if url not in skip]
    skipped = len(listed) - len(urls)
    errors: dict[str, str] = {}
    damaged: set[str] = set()
    unchecked: dict[str, str] = {}

    def probe(url):
        try:
            ds = vz.open_virtual_dataset(
                url, registry=registry, parser=parser, loadable_variables=list(GRID_AXES)
            )
        except Exception as exc:  # noqa: BLE001 - an unreadable file is left out
            errors[url] = f"{type(exc).__name__}: {exc}"
            logger.warning(f"{product.name}: {url}: {errors[url]}")
            # Checked and recorded at once: the process can crash minutes after HDF5 has
            # read a damaged file, long before the rest of a 1440-file day is probed.
            try:
                damage = _damage(url)
            except Exception as check:  # noqa: BLE001 - not known to be damaged
                unchecked[url] = f"{type(check).__name__}: {check}"
            else:
                if damage is not None:
                    _record_unreadable(product, url, damage)
                    damaged.add(url)
            return url, None, None, None
        try:
            if "t" not in ds.variables or ds["t"].ndim != 0:
                # _add_time needs one scalar time, and a file without it failed its whole
                # day (GOES-18 ACHA2KMM1 2025-04-10): leave it out like any unusable file.
                logger.warning(f"{product.name}: {url}: no scalar 't'; left out")
                return url, None, None, None
            names = list(ds.variables)
            # The layout is the grid variables with their chunking, codecs and dtype:
            # files differing in any (GOES-18 changed chunk shapes mid-day while
            # commissioning; GOES-16 files mix shuffle on and off) cannot be concatenated,
            # though their variables match.
            layout = tuple(
                sorted(
                    (
                        n,
                        tuple(ds[n].data.metadata.chunks),
                        str(ds[n].data.metadata.codecs),
                        str(ds[n].data.metadata.data_type),
                    )
                    for n in _virtual_names(ds)
                )
            )
            # The grid itself: a sector can move part way through a day (GOES-16 CONUS
            # on 2023-06-01 shifted x at 15:26), and files on different grids only
            # combine by aligning, which a virtual array cannot do.
            # Not mesoscale: its box moves on purpose, and each file keeps its position.
            grid = tuple(
                (axis, hashlib.sha256(np.asarray(ds[axis].values).tobytes()).hexdigest())
                for axis in GRID_AXES
                if axis in ds.variables and not product.meso
            )
            return url, layout, names, grid
        finally:
            ds.close()

    def screened_probe(url):
        """Check a file's bytes with h5py before the parser opens it.

        Opening a damaged file with HDFParser can crash the process outright (GOES-18
        ACHA2KMM1 2025-100 20:44 segfaulted the lane with no Python error), while h5py on
        a downloaded copy raises cleanly. Used where every file of a day is opened.
        """
        try:
            damage = _damage(url)
        except Exception as check:  # noqa: BLE001 - not known to be damaged
            unchecked[url] = f"{type(check).__name__}: {check}"
            return url, None, None, None
        if damage is not None:
            _record_unreadable(product, url, damage)
            damaged.add(url)
            return url, None, None, None
        return probe(url)

    def combine(keep: list[str], loadable: list[str]):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return vz.open_virtual_mfdataset(
                keep,
                registry=registry,
                parser=parser,
                loadable_variables=loadable,
                preprocess=_add_time_meso if product.meso else _add_time,
                parallel=ThreadPoolExecutor,
                **_COMBINE,
            )

    # Usually every file of a day has the same layout: learn it from the first readable
    # file and combine them all in one pass. Only if that fails is each file checked.
    for url in urls:
        _, layout, names, _ = probe(url)
        if layout is not None:
            break
    else:
        return [], skipped
    grid = {n for n, *_ in layout}
    try:
        vds = combine(urls, [n for n in names if n not in grid])
        if set(_virtual_names(vds)) == grid:
            return [vds], skipped
    except Exception as exc:  # noqa: BLE001 - fall back to checking each file
        logger.info(
            f"{product.name}: day does not combine as is ({type(exc).__name__}); checking each file"
        )
    with ThreadPoolExecutor(OPEN_WORKERS) as pool:
        probed = list(pool.map(screened_probe, urls))
    # A file whose grid variables differ from the day's majority (an occasional file
    # missing a field) is left out. The rest are split by layout and grid: each the day
    # holds is combined on its own and goes to its own generation, losing no file.
    # probe has checked every file that would not open: damaged ones are in the ledger.
    # One that could not be fetched to check fails the day, for a later pass to retry; one
    # whose bytes read cleanly failed for some other reason, so it is opened once more.
    for i, (url, layout, *_) in enumerate(probed):
        if layout is not None or url in damaged:
            continue
        if url in unchecked:
            raise RuntimeError(f"{product.name}: could not check {url}: {unchecked.pop(url)}")
        probed[i] = probe(url)
        if url in unchecked:
            raise RuntimeError(f"{product.name}: could not check {url}: {unchecked.pop(url)}")
    readable = [entry for entry in probed if entry[1] is not None]
    variables = [frozenset(n for n, *_ in layout) for _, layout, _, _ in readable]
    majority = max(set(variables), key=variables.count)
    groups: dict[tuple, list[str]] = {}
    names_of: dict[tuple, list[str]] = {}
    for (url, layout, names, grid), found in zip(readable, variables):
        if found != majority:
            continue
        groups.setdefault((layout, grid), []).append(url)
        names_of.setdefault((layout, grid), names)
    datasets = [
        combine(keep, [n for n in names_of[key] if n not in majority])
        for key, keep in sorted(groups.items(), key=lambda item: -len(item[1]))
    ]
    return datasets, len(listed) - sum(len(keep) for keep in groups.values())


def layout_signature(vds) -> str:
    """What must match for a day to append to a store: grids, chunks, codecs, dtypes."""
    from virtualizarr.manifests import ManifestArray

    parts = []
    for name in sorted(vds.variables):
        var = vds[name].variable
        if isinstance(var.data, ManifestArray):
            meta = var.data.metadata
            codecs = [c.to_dict() if hasattr(c, "to_dict") else str(c) for c in meta.codecs]
            parts.append(
                [
                    name,
                    list(var.dims),
                    list(var.shape[1:]),
                    list(meta.chunks[1:]),
                    str(meta.data_type),
                    codecs,
                ]
            )
        else:
            parts.append([name, list(var.dims), list(var.shape[1:]), str(var.dtype)])
    for axis in ("x", "y", "lat", "lon"):
        if axis in vds.coords:
            values = np.asarray(vds[axis].values)
            parts.append([axis, hashlib.sha1(values.tobytes()).hexdigest()])
    return hashlib.sha1(json.dumps(parts, default=str).encode()).hexdigest()


# ----------------------------------------------------------------------------- stores


def open_store(prefix: str, product: Product, config: Config, create: bool = True):
    from planetary_datasets.providers.virtualized.virtual_repo import open_virtual_repo

    return open_virtual_repo(
        prefix,
        virtual_buckets=[f"s3://{product.bucket}"],
        split_size=product.files_per_day,
        split_dim="t",
        source_region=SOURCE_REGION,
        config=config,
        create=create,
    )


def _root_attrs(repo) -> dict:
    import zarr

    try:
        return dict(zarr.open_group(repo.readonly_session("main").store, mode="r").attrs)
    except Exception:  # noqa: BLE001 - an empty store has no root group yet
        return {}


def stored_attrs(prefix: str, product: Product, config: Config) -> dict:
    """The root attributes of the store at ``prefix``; empty if there is none (never creates)."""
    try:
        repo = open_store(prefix, product, config, create=False)
    except Exception:  # noqa: BLE001 - no repository there yet
        return {}
    return _root_attrs(repo)


def generations(config: Config, base: str) -> list[str]:
    """The base store and its dated generations that exist, base first.

    Listed, never probed: opening a name to see whether it exists would create it.
    """
    import re

    import fsspec

    parent, _, name = base.rpartition("/")
    stem = name.removesuffix(".icechunk")
    pattern = re.compile(rf"^{re.escape(stem)}_(?P<date>\d{{8}})(?:_\d+)?\.icechunk$")
    root = config.store_path(parent)
    try:
        if config.use_local_store:
            filesystem, path = fsspec.core.url_to_fs(root)
        else:
            filesystem, path = fsspec.core.url_to_fs(root, **config.fsspec_storage_options())
        filesystem.invalidate_cache(path)
        entries = [str(e).rstrip("/").rpartition("/")[2] for e in filesystem.ls(path, detail=False)]
    except FileNotFoundError:
        entries = []
    dated = sorted(
        (dt.datetime.strptime(m["date"], "%d%m%Y"), e) for e in entries if (m := pattern.match(e))
    )
    return [base] + [f"{parent}/{e}" for _, e in dated]


def store_for(product: Product, signature: str, day: dt.date, config: Config) -> str:
    """The generation of the product's store whose layout is ``signature``.

    The base store when it is empty or matches; another generation whose signature
    matches; otherwise a new generation named for ``day``.
    """
    from planetary_datasets.common.generations import prefix_for_date

    base = product.store_prefix
    for prefix in generations(config, base):
        attrs = stored_attrs(prefix, product, config)
        if not attrs.get(SIGNATURE_ATTR) or attrs.get(SIGNATURE_ATTR) == signature:
            return prefix
    for disambiguator in range(10):
        prefix = prefix_for_date(base, day, disambiguator)
        attrs = stored_attrs(prefix, product, config)
        if not attrs.get(SIGNATURE_ATTR) or attrs.get(SIGNATURE_ATTR) == signature:
            logger.info(f"{product.name}: layout changed on {day}; new generation {prefix}")
            return prefix
    raise RuntimeError(f"{product.name}: no generation free for {day}")


def written_days(product: Product, config: Config) -> set[str]:
    """Days recorded in any generation's ledger."""
    done: set[str] = set()
    for prefix in generations(config, product.store_prefix):
        done |= set(stored_attrs(prefix, product, config).get(LEDGER_ATTR, []))
    return done


def write_day(product: Product, day: dt.date, keys: Sequence[str], config: Config) -> int:
    """Reference one day's files in the right generation; one commit. Returns files written."""
    import icechunk
    import zarr

    datasets, dropped = open_day(keys, product)
    if not datasets:
        logger.warning(f"{product.name} {day}: no readable files")
        return 0
    if len(datasets) > 1:
        logger.info(f"{product.name} {day}: {len(datasets)} layouts in one day; one store each")
    return sum(_write_layout(product, day, vds, dropped, config) for vds in datasets)


def _write_layout(product: Product, day: dt.date, vds, dropped: int, config: Config) -> int:
    """Reference one layout's files of a day in the generation matching it; one commit."""
    import icechunk
    import zarr

    signature = layout_signature(vds)
    prefix = store_for(product, signature, day, config)
    attrs = stored_attrs(prefix, product, config)
    repo = open_store(prefix, product, config)
    session = repo.writable_session("main")
    if attrs.get(SIGNATURE_ATTR):
        vds.vz.to_icechunk(session.store, append_dim="t")
    else:
        vds.vz.to_icechunk(session.store)
    root = zarr.open_group(session.store, mode="a")
    days = set(root.attrs.get(LEDGER_ATTR, [])) | {day.isoformat()}
    root.attrs.update(
        {
            SIGNATURE_ATTR: signature,
            LEDGER_ATTR: sorted(days),
            "source": f"s3://{product.bucket}/{product.source_prefix}/ (mirror: gs://gcp-public-data-goes-{product.satellite}/)",
            "product": product.source_prefix
            + (f" sector {product.sector}" if product.meso else "")
            + (f" channel {product.channel}" if product.channel else ""),
        }
    )
    session.commit(
        f"{product.name}: {day} ({vds.sizes['t']} file(s), {dropped} left out)",
        rebase_with=icechunk.ConflictDetector(),
    )
    logger.info(f"{product.name} {day}: {vds.sizes['t']} file(s) -> {prefix} ({dropped} left out)")
    return int(vds.sizes["t"])


def run_product(
    product: Product,
    config: Config,
    start: dt.date | None = None,
    end: dt.date | None = None,
    now: dt.datetime | None = None,
) -> int:
    """Write every settled, unwritten day of one product. Returns files written."""
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    last = (now - SETTLE).date() - dt.timedelta(days=1)
    _, store = source(product)
    days = [d for d in available_days(store, product) if d <= last]
    days = [d for d in days if (start is None or d >= start) and (end is None or d <= end)]
    done = written_days(product, config)
    todo = [d for d in order_days(days, last) if d.isoformat() not in done]
    total = 0
    for day in todo:
        keys = day_files(store, product, day)
        if not keys:
            continue
        try:
            total += write_day(product, day, keys, config)
        except Exception as exc:  # noqa: BLE001 - one bad day must not stop the product
            logger.exception(f"{product.name} {day}: {type(exc).__name__}: {exc}")
    return total


def list_products(
    satellite: int, sectors: Sequence[str], codes: Sequence[str] | None
) -> list[Product]:
    _, store = source(Product(satellite, "", ""))
    found = products_from_listing(satellite, _children(store, ""), sectors)
    if codes:
        found = [p for p in found if p.code in codes]
    # Newest year first, so a long backlog never holds back the live edge.
    out = []
    for product in found:
        out += sorted(by_year(store, product), key=lambda p: -p.year) if product.meso else [product]
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--satellite", type=int, required=True, choices=sorted(SATELLITES))
    parser.add_argument("--sectors", default=",".join(SECTORS))
    parser.add_argument("--products", help="comma-separated product codes (default: all)")
    parser.add_argument("--start", type=dt.date.fromisoformat)
    parser.add_argument("--end", type=dt.date.fromisoformat)
    parser.add_argument("--workers", type=int, default=4, help="products written at once")
    parser.add_argument("--follow", action="store_true", help="keep passing, hourly")
    args = parser.parse_args(argv)
    config = get_config()
    sectors = args.sectors.split(",")
    codes = args.products.split(",") if args.products else None
    while True:
        products = list_products(args.satellite, sectors, codes)
        logger.info(f"goes{args.satellite} L2: {len(products)} store(s) to keep")
        with ThreadPoolExecutor(args.workers) as pool:
            written = sum(
                pool.map(lambda p: run_product(p, config, args.start, args.end), products)
            )
        logger.info(f"goes{args.satellite} L2: pass done, {written:,} file(s) referenced")
        if not args.follow:
            return 0
        for _ in range(6):  # an hour, saying so every ten minutes for the box's stall guard
            time.sleep(600)
            logger.info(f"goes{args.satellite} L2: waiting for the next pass")


if __name__ == "__main__":
    sys.exit(main())
