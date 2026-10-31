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

Not covered here (see ``DEFERRED``): the point products (derived motion winds and the
GLM lightning events), which cannot be appended as references because each file's
length differs, and the mesoscale sectors, ~870 million one-minute files whose position
moves with each sector.

Each day is written in one commit and recorded in the store's ledger, so a run resumes
where it stopped. The last few days go first, newest first, then the backlog oldest
first; times are therefore not sorted in the store.

    python -m planetary_datasets.providers.virtualized.goes_l2 --satellite 19 --follow
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import sys
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
#: Sectors archived by default: full disk and CONUS (or PACUS for the west satellites).
SECTORS = ("F", "C")
#: Products whose files are not on a fixed grid: winds and lightning are lists of
#: observations of a different length in every file.
POINT_PRODUCTS = ("DMW", "DMWV")
DEFERRED = (
    "ABI-L2-DMW*, ABI-L2-DMWV* (derived motion winds) and GLM-L2-LCFA (lightning): "
    "per-file lengths differ, so they need materialising rather than references",
    "mesoscale sectors (ABI-L2-*M): ~870M one-minute files, sector position moves",
)
#: Root attributes.
LEDGER_ATTR = "days_written"
SIGNATURE_ATTR = "layout_signature"
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
    sector: str  # "F" or "C" (or "M")
    channel: int | None = None

    @property
    def bucket(self) -> str:
        return SATELLITES[self.satellite]

    @property
    def source_prefix(self) -> str:
        return f"ABI-L2-{self.code}{self.sector}"

    @property
    def name(self) -> str:
        tail = f"_c{self.channel:02d}" if self.channel else ""
        return f"goes{self.satellite}_l2_{self.code.lower()}_{self.sector.lower()}{tail}"

    @property
    def store_prefix(self) -> str:
        tail = f"_c{self.channel:02d}" if self.channel else ""
        return (
            f"{STORE_ROOT}/goes{self.satellite}_l2/"
            f"{self.code.lower()}_{self.sector.lower()}{tail}.icechunk"
        )

    def wants(self, key: str) -> bool:
        """Whether a file in the product's prefix belongs to this store."""
        name = key.rsplit("/", 1)[-1]
        if not name.endswith(".nc"):
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
        code, sector = body[:-1], body[-1]
        if sector not in sectors or code in POINT_PRODUCTS:
            continue
        if code == "CMIP":
            out += [Product(satellite, code, sector, ch) for ch in range(1, 17)]
        else:
            out.append(Product(satellite, code, sector))
    return out


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
    for year in _children(store, f"{product.source_prefix}/"):
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


def open_day(keys: Sequence[str], product: Product):
    """The day's files as one virtual dataset along ``t``.

    The grid variables stay references; everything small (times, bounds, projection,
    per-file statistics) is loaded and stored inline. Files whose grid variables differ
    from the day's majority (an occasional file missing a field) are left out.
    """
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry

    url_base, store = source(product)
    registry = ObjectStoreRegistry({url_base: store})
    parser = vz.parsers.HDFParser()
    urls = [f"{url_base}/{key}" for key in keys]

    def probe(url):
        try:
            ds = vz.open_virtual_dataset(
                url, registry=registry, parser=parser, loadable_variables=[]
            )
        except Exception as exc:  # noqa: BLE001 - an unreadable file is left out
            logger.warning(f"{product.name}: {url}: {type(exc).__name__}: {exc}")
            return url, None, None
        try:
            names = list(ds.variables)
            return url, tuple(sorted(_virtual_names(ds))), names
        finally:
            ds.close()

    def combine(keep: list[str], loadable: list[str]):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return vz.open_virtual_mfdataset(
                keep,
                registry=registry,
                parser=parser,
                loadable_variables=loadable,
                preprocess=_add_time,
                parallel=ThreadPoolExecutor,
                **_COMBINE,
            )

    # Usually every file of a day has the same layout: learn it from the first readable
    # file and combine them all in one pass. Only if that fails is each file checked.
    for url in urls:
        _, grid, names = probe(url)
        if grid is not None:
            break
    else:
        return None, 0
    try:
        vds = combine(urls, [n for n in names if n not in grid])
        if set(_virtual_names(vds)) == set(grid):
            return vds, 0
    except Exception as exc:  # noqa: BLE001 - fall back to checking each file
        logger.info(
            f"{product.name}: day does not combine as is ({type(exc).__name__}); checking each file"
        )
    with ThreadPoolExecutor(OPEN_WORKERS) as pool:
        probed = list(pool.map(probe, urls))
    layouts = [grid for _, grid, _ in probed if grid is not None]
    majority = max(set(layouts), key=layouts.count)
    keep = [url for url, grid, _ in probed if grid == majority]
    names = next(names for _, grid, names in probed if grid == majority)
    vds = combine(keep, [n for n in names if n not in majority])
    return vds, len(urls) - len(keep)


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

    files_per_day = 288 if product.sector == "C" else 144
    return open_virtual_repo(
        prefix,
        virtual_buckets=[f"s3://{product.bucket}"],
        split_size=files_per_day,
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

    vds, dropped = open_day(keys, product)
    if vds is None:
        logger.warning(f"{product.name} {day}: no readable files")
        return 0
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
    return found


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
