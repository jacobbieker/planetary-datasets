"""Copernicus Sentinel-4 (MTG-S1 UVN) Level 2 products, one Icechunk group per product.

Sentinel-4 is the UV/visible/near-infrared imaging spectrometer on MTG-S1, scanning Europe
and North Africa every hour in daylight. EUMETSAT took over operations in August 2026 and
publishes the guides for its L1 and L2 products (NO2, O3, ...) from October 2026; the
products themselves come from the EUMETSAT Data Store, need credentials, and so are
copied, not referenced. Every L2 product goes into one store,
``bkr/geo/sentinel4_l2.icechunk``, a zarr group per product::

    xr.open_datatree(store, engine="zarr", consolidated=False)
    xr.open_zarr(store, group="no2", consolidated=False)

The products follow the MTG UVN L2 format specification (EUM/MTG/SPE/10/0454): one
netCDF-4 "body" file per 5-minute chunk of the hourly repeat cycle (12 a cycle), with
the retrieval laid out like TROPOMI's on ``scanline`` x ``ground_pixel``; the daily
surface-reflectance maps come once a day. Rather than hard-code a layout no file has yet
confirmed, each file is read generically: every netCDF group is flattened (a name used by
two groups takes its group as a prefix), the dimension the 2-D ``latitude`` runs along is
the one chunks are appended on, and the rest are fixed (padded to the widest seen). A
variable with no track dimension, and the file's global attributes, become one row per
file on ``granule``; ``granule_index`` ties each scanline to its file. Times are decoded
to datetime64, since their ``... since`` epochs differ file to file.

Collections are found by title, so the archive starts by itself once EUMETSAT publishes
them; ``SENTINEL4_COLLECTIONS`` (JSON, product key to collection id) overrides. Until any
is published :meth:`Sentinel4Provider.available` is False and the lane exits 3, which the
box script takes to mean "try again tomorrow".
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Iterable, List

import h5py
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import multidim
from planetary_datasets.common.store import STORE_READ_ERRORS
from planetary_datasets.providers.polar._eumdac import EumdacProvider

STORE_PREFIX = "bkr/geo/sentinel4_l2.icechunk"
#: First light was 2025-10-08; nothing earlier will be published.
FIRST_DAY = "2025-10-08"
COLLECTIONS_ENV = "SENTINEL4_COLLECTIONS"
#: A day is recorded complete once this old and every listed product is stored.
SETTLE_DAYS = 3
#: Bytes of netCDF (times a margin for decoding) written per append.
BATCH_BYTES = 2 * 2**30
#: Attribute on a group's ``granule_name`` listing the days recorded complete.
DAYS_ATTR = "days_complete"


@dataclass(frozen=True)
class Product:
    """One Sentinel-4 L2 product.

    Attributes:
        key: Its group in the store.
        product_id: EUMETSAT product id (format specification, table 1-1/1-2).
        title: Regular expression a Data Store collection title is matched against.
        daily: Made once a day, not per 5-minute chunk.
    """

    key: str
    product_id: str
    title: str
    daily: bool = False


#: In matching order: the more specific titles first.
PRODUCTS: dict[str, Product] = {
    p.key: p
    for p in (
        Product(
            "o3_tropospheric",
            "UVN-2-O3-TSC",
            r"tropospheric\W+(sub-?)?column\W+ozone"
            r"|tropospheric\s+ozone|ozone\W+tropospheric|O3[- ]TSC",
        ),
        Product("o3", "UVN-2-O3", r"\bozone\b|\bO3\b"),
        Product("no2", "UVN-2-NO2", r"nitrogen\s+dioxide|\bNO2\b"),
        Product("so2", "UVN-2-SO2", r"sul(ph|f)ur\s+dioxide|\bSO2\b"),
        Product("hcho", "UVN-2-FDY", r"formaldehyde|\bHCHO\b"),
        Product("chocho", "UVN-2-GLY", r"glyoxal|CHOCHO"),
        Product("aerosol_layer_height", "UVN-2-ALH", r"aerosol\s+layer\s+height|\bALH\b"),
        Product("aerosol_index", "UVN-2-AUI", r"aerosol\s+index|\bUVAI\b|\bAAI\b|\bAUI\b"),
        Product("gapless_surface_reflectance", "UVN-2-GSR", r"gapless", daily=True),
        Product("surface_reflectance", "UVN-2-SR", r"surface\s+reflectance", daily=True),
        Product("cloud", "UVN-2-CLD", r"\bclouds?\b"),
    )
}
#: The daily "background" variants of these trace-gas products get their own groups.
BACKGROUND = ("no2", "so2", "hcho", "chocho")


def classify(title: str) -> str | None:
    """The product key a Data Store collection title names; None if not a S4 L2 product."""
    if not re.search(r"sentinel[- ]?4|\bUVN\b", title, re.I):
        return None
    if re.search(r"level[- ]?1|\bL1[BC]?\b|irradiance|radiance", title, re.I):
        return None
    if re.search(r"\bFCI\b|sentinel[- ]?5|TROPOMI", title, re.I):
        return None
    for product in PRODUCTS.values():
        if re.search(product.title, title, re.I):
            if product.key in BACKGROUND and re.search(r"background", title, re.I):
                return f"{product.key}_background"
            return product.key
    return None


def product_for(key: str) -> Product:
    """The :class:`Product` of a group key (a background variant shares its gas's)."""
    if key.endswith("_background"):
        base = PRODUCTS[key.removesuffix("_background")]
        return Product(key, f"{base.product_id}-BG", base.title, daily=True)
    return PRODUCTS[key]


ALL_KEYS = [*PRODUCTS, *(f"{k}_background" for k in BACKGROUND)]


# ---------------------------------------------------------------------------- reading

TIME_UNITS = re.compile(r"^\s*\w+\s+since\s+", re.I)


def _decode_strings(values: np.ndarray) -> np.ndarray:
    if values.dtype.kind == "S":
        return np.char.decode(values, "utf-8", errors="replace").astype(object)
    return values


def _groups(path: pathlib.Path) -> list[str]:
    """The root and every group, depth first."""
    found = ["/"]
    with h5py.File(path, "r") as f:
        f.visititems(lambda name, obj: found.append(name) if isinstance(obj, h5py.Group) else None)
    return found


def _decode_time(variable: xr.Variable) -> xr.Variable:
    """A numeric ``... since ...`` variable as datetime64; anything else unchanged."""
    units = str(variable.attrs.get("units", ""))
    if variable.dtype.kind not in "iuf" or not TIME_UNITS.match(units):
        return variable
    decoded = xr.decode_cf(xr.Dataset({"t": variable}), decode_times=True, mask_and_scale=True)[
        "t"
    ].variable
    if decoded.dtype.kind != "M":
        return variable
    attrs = {
        k: v for k, v in variable.attrs.items() if k not in ("units", "calendar", "_FillValue")
    }
    return xr.Variable(variable.dims, decoded.values.astype("datetime64[ns]"), attrs)


def _attr(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray):
        return [_attr(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    return value


def read_granule(path: pathlib.Path) -> tuple[xr.Dataset, str | None]:
    """One product file as a flat dataset, and the dimension chunks append along.

    Variables on the track dimension (the first dimension of the 2-D ``latitude``) come
    first on it; the rest get a leading ``granule`` dimension of length one. A singleton
    ``time`` dimension (TROPOMI's layout) is dropped. Returns ``(dataset, None)`` when the
    file has no 2-D latitude, e.g. a gridded daily map: every variable is then per file.
    """
    parts: dict[str, xr.Dataset] = {}
    for group in _groups(path):
        with xr.open_dataset(
            path,
            group=None if group == "/" else group,
            engine="h5netcdf",
            phony_dims="access",
            mask_and_scale=False,
            decode_times=False,
            decode_timedelta=False,
        ) as opened:
            if opened.data_vars:
                ds = opened.load()
                if ds.sizes.get("time") == 1:
                    ds = ds.isel(time=0, drop=True)
                parts[group] = ds

    track = ground = None
    for ds in parts.values():
        lat = ds.variables.get("latitude")
        if lat is not None and lat.ndim >= 2:
            track, ground = lat.dims[0], lat.dims[1]
            break

    seen: dict[str, int] = {}
    for ds in parts.values():
        for name in ds.data_vars:
            seen[name] = seen.get(name, 0) + 1
    sizes: dict[str, int] = {}
    variables: dict[str, xr.Variable] = {}
    for group, ds in parts.items():
        leaf = "" if group == "/" else group.rsplit("/", 1)[-1].lower()
        # A dimension name two groups use at different sizes is told apart by group.
        rename = {}
        for dim, size in ds.sizes.items():
            if dim in (track, ground):
                continue
            if sizes.setdefault(dim, size) != size:
                rename[dim] = f"{leaf}_{dim}"
                sizes.setdefault(rename[dim], size)
        for name, variable in ds.data_vars.items():
            if variable.dims == (name,):
                continue  # an index coordinate; the dimension says the same
            label = name if seen[name] == 1 or not leaf else f"{leaf}_{name}"
            if label in (track, ground, "granule") or label in sizes:
                label = f"{label}_value"
            values = _decode_strings(variable.values)
            v = _decode_time(xr.Variable(variable.dims, values, dict(variable.attrs)))
            v = v.rename({d: rename.get(d, d) for d in v.dims}) if rename else v
            if track is not None and track in v.dims:
                v = v.transpose(track, *[d for d in v.dims if d != track])
            else:
                v = xr.Variable(("granule", *v.dims), v.values[np.newaxis], v.attrs)
            variables[label] = v

    with h5py.File(path, "r") as f:
        attrs = {k: _attr(v) for k, v in f.attrs.items()}
    for key in (
        "time_coverage_start",
        "time_coverage_end",
        "product_version",
        "processing_baseline",
        "processor_version",
        "product_name",
    ):
        if key in attrs:
            variables[key] = xr.Variable(("granule",), np.array([str(attrs[key])], dtype=object))
    variables["granule_attributes"] = xr.Variable(
        ("granule",), np.array([json.dumps(attrs, default=str, sort_keys=True)], dtype=object)
    )
    variables["granule_name"] = xr.Variable(("granule",), np.array([path.name], dtype=object))
    return xr.Dataset(variables), track


def concat_granules(
    frames: list[tuple[xr.Dataset, str | None]], first_index: int
) -> tuple[xr.Dataset, str]:
    """Files end to end on their track dimension, per-file rows on ``granule``.

    Returns the combined dataset and the track dimension (``granule`` for gridded files,
    which then each contribute one row of every variable).
    """
    tracks = {t for _, t in frames}
    if len(tracks) != 1:
        raise ValueError(f"files disagree on their track dimension: {sorted(map(str, tracks))}")
    track = tracks.pop() or "granule"
    if track == "granule":
        ds = xr.concat([f for f, _ in frames], dim="granule", data_vars="all")
        return ds, track
    # concat_frames works on prefixed names; prefix, combine, strip.
    prefix = "s4"

    def wrap(ds: xr.Dataset) -> xr.Dataset:
        dims = {d: f"{prefix}_along_track" if d == track else f"{prefix}_{d}" for d in ds.dims}
        return ds.rename({n: f"{prefix}_{n}" for n in ds.variables}).rename_dims(dims)

    combined = multidim.concat_frames([wrap(f) for f, _ in frames], prefix, first_index, "granule")
    dims = {
        d: track if d == f"{prefix}_along_track" else d.removeprefix(f"{prefix}_")
        for d in combined.dims
    }
    combined = combined.rename_dims(dims)
    return combined.rename({n: n.removeprefix(f"{prefix}_") for n in combined.variables}), track


def _body_files(path: pathlib.Path, into: pathlib.Path) -> list[pathlib.Path]:
    """The netCDF body chunks of a downloaded Data Store product (zipped or not)."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            names = [n for n in z.namelist() if n.endswith(".nc")]
            body = [n for n in names if "BODY" in n.upper()] or [
                n for n in names if "TRAIL" not in n.upper()
            ]
            out = []
            for name in sorted(body):
                target = into / pathlib.Path(name).name
                with z.open(name) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                out.append(target)
        path.unlink(missing_ok=True)
        return out
    return [path] if path.suffix == ".nc" else []


# ---------------------------------------------------------------------------- provider


class Sentinel4Provider(BaseProvider):
    """Sentinel-4 L2 products from the EUMETSAT Data Store, a day per partition."""

    name = "sentinel4_l2"
    append_dim = "time"
    store_prefix = STORE_PREFIX
    download_retries = 3

    def __init__(
        self, config=None, products: Iterable[str] | None = None, download_workers: int = 4
    ):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            products: Group keys to ingest (see :data:`ALL_KEYS`); all by default.
            download_workers: Concurrent Data Store downloads. The account's throughput
                is shared with the MetOp and MTG lanes, so keep this small.
        """
        super().__init__(config=config)
        keys = list(products) if products is not None else list(ALL_KEYS)
        unknown = set(keys) - set(ALL_KEYS)
        if unknown:
            raise ValueError(f"unknown Sentinel-4 products: {sorted(unknown)}")
        self.keys = keys
        self.download_workers = download_workers
        self._collections: dict[str, str] | None = None

    # The Data Store client and the stall-proof download are EumdacProvider's.
    datastore = EumdacProvider.datastore
    _download_product = EumdacProvider._download_product

    # ------------------------------------------------------------------ collections

    def collections(self) -> dict[str, str]:
        """Group key to Data Store collection id, for the products published so far."""
        if self._collections is None:
            found: dict[str, str] = {}
            try:
                for collection in self.datastore().collections:
                    key = classify(str(collection.title))
                    if key and key not in found:
                        found[key] = str(collection)
            except Exception as exc:  # noqa: BLE001 - an outage is "not available yet"
                logger.warning(f"{self.name}: could not list Data Store collections: {exc}")
            override = os.environ.get(COLLECTIONS_ENV)
            if override:
                found.update(json.loads(override))
            self._collections = {k: v for k, v in found.items() if k in self.keys}
            logger.info(f"{self.name}: collections {self._collections or 'none published yet'}")
        return self._collections

    def available(self) -> bool:
        """Whether any wanted product is published yet."""
        return bool(self.collections())

    # ------------------------------------------------------------------ state

    def _root(self) -> zarr.Group | None:
        try:
            return zarr.open_group(
                self.get_icechunk_repo().readonly_session("main").store, mode="r"
            )
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    def days_complete(self, key: str, root: zarr.Group | None = None) -> set[str]:
        root = root if root is not None else self._root()
        if root is None or key not in root or "granule_name" not in root[key]:
            return set()
        return set(root[key]["granule_name"].attrs.get(DAYS_ATTR, []))

    def stored(self, key: str) -> tuple[set[str], int]:
        """Data Store products already stored for ``key``, and the files stored."""
        root = self._root()
        if root is None or key not in root or "granule_name" not in root[key]:
            return set(), 0
        group = root[key]
        files = group["granule_name"].shape[0]
        products = (
            {str(v) for v in group["data_store_product"][:]}
            if "data_store_product" in group
            else set()
        )
        return products, files

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Days some published product has not been recorded complete for."""
        keys = list(self.collections())
        root = self._root()
        done = {k: self.days_complete(k, root) for k in keys}
        first = pd.Timestamp(FIRST_DAY)
        return [
            day
            for day in desired
            if day >= first and any(f"{day:%Y-%m-%d}" not in done[k] for k in keys)
        ]

    # ------------------------------------------------------------------ writing

    def _write(self, key: str, files: list[tuple[pathlib.Path, str]], first_index: int) -> int:
        frames = []
        for path, product_id in files:
            try:
                ds, track = read_granule(path)
            except Exception as exc:  # noqa: BLE001 - one bad file must not lose the rest
                logger.warning(f"{self.name} {key}: {path.name} unreadable, skipped: {exc}")
                continue
            ds["data_store_product"] = xr.Variable(
                ("granule",), np.array([product_id], dtype=object)
            )
            frames.append((ds, track))
        if not frames:
            return 0
        ds, track = concat_granules(frames, first_index)
        multidim.append_partition(
            self.get_icechunk_repo(),
            ds,
            None,
            fixed_dims=[d for d in ds.dims if d not in (track, "granule")],
            label=f"{self.name} {key}",
            group=key,
        )
        return len(frames)

    def run_product_day(self, key: str, day: pd.Timestamp, pool: ThreadPoolExecutor) -> int:
        """Store the day's products of one group not yet stored. Returns files written."""
        collection = self.datastore().get_collection(self.collections()[key])
        listed = {
            str(p): p
            for p in collection.search(
                dtstart=day.to_pydatetime(), dtend=(day + pd.Timedelta(1, "D")).to_pydatetime()
            )
        }
        stored, count = self.stored(key)
        todo = [listed[n] for n in sorted(listed) if n not in stored]
        written = 0
        if todo:
            scratch = pathlib.Path(
                tempfile.mkdtemp(prefix=f"sentinel4-{key}-", dir=self.config.scratch_dir)
            )
            # The box guard removes scratch whose owner has died.
            (scratch / ".pid").write_text(str(os.getpid()))
            try:
                futures = [pool.submit(self._download_product, p, scratch) for p in todo]
                batch: list[tuple[pathlib.Path, str]] = []
                size = 0
                for product, future in zip(todo, futures):
                    path = future.result()
                    if path is None:
                        logger.warning(f"{self.name} {key}: {product} failed to download")
                        continue
                    # A product's chunks go in together, so a product is stored whole or not.
                    for body in _body_files(path, scratch):
                        batch.append((body, str(product)))
                        size += body.stat().st_size * 4
                    if size >= BATCH_BYTES:
                        written += self._write(key, batch, count + written)
                        for done, _ in batch:
                            done.unlink(missing_ok=True)
                        batch, size = [], 0
                if batch:
                    written += self._write(key, batch, count + written)
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
            stored, _ = self.stored(key)
        self._settle(key, day, set(listed), stored)
        return written

    def _settle(self, key: str, day: pd.Timestamp, listed: set[str], stored: set[str]) -> None:
        """Record the day complete once it is old enough and every listed product is in."""
        now = pd.Timestamp.now("UTC").tz_localize(None)
        if now - day <= pd.Timedelta(SETTLE_DAYS + 1, "D") or not listed or not listed <= stored:
            return
        import icechunk

        for attempt in range(1, 6):
            session = self.get_icechunk_repo().writable_session("main")
            array = zarr.open_group(session.store, path=key, mode="a")["granule_name"]
            days = set(array.attrs.get(DAYS_ATTR, []))
            if f"{day:%Y-%m-%d}" in days:
                return
            array.attrs[DAYS_ATTR] = sorted(days | {f"{day:%Y-%m-%d}"})
            try:
                session.commit(
                    f"{self.name} {key}: {day:%Y-%m-%d} complete",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return
            except (icechunk.ConflictError, icechunk.RebaseFailedError):
                if attempt == 5:
                    raise

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Every published product's files for the day starting at ``it``."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        written = 0
        with ThreadPoolExecutor(max_workers=self.download_workers) as pool:
            for key in self.collections():
                if check_present and f"{day:%Y-%m-%d}" in self.days_complete(key):
                    continue
                n = self.run_product_day(key, day, pool)
                if n:
                    logger.info(f"{self.name}: {key} {day:%Y-%m-%d}: {n} file(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` downloads and writes product by product."""
        raise NotImplementedError("Sentinel4Provider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("Sentinel4Provider writes through run_partition")


__all__ = [
    "ALL_KEYS",
    "PRODUCTS",
    "STORE_PREFIX",
    "Sentinel4Provider",
    "classify",
    "concat_granules",
    "read_granule",
]
