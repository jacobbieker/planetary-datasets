"""NASA TEMPO gridded (Level 3) cloud and aerosol products, copied into one Icechunk store.

TEMPO (Tropospheric Emissions: Monitoring of Pollution) is a geostationary UV-visible
spectrometer over North America. Its Level 3 products put each hourly-ish East-West scan
on one fixed 0.02 degree grid (2950 latitudes x 7750 longitudes, 14.01..72.99 N,
167.99..13.01 W, unchanged since first light on 2023-08-02). They are in LaRC ASDC's
protected bucket (``asdc-prod-protected``, us-west-2), readable only with Earthdata S3
credentials, so they are materialized here rather than referenced::

    bkr/geo/tempo.icechunk
        /cloud_l3     <- TEMPO_CLDO4_L3 V04: O2-O2 cloud fraction and pressure
        /aerosol_l3   <- TEMPO_O3TOT_L3 V04: UV aerosol index (and the SO2 index,
                         total ozone and its cloud fields, from the same retrieval)

TEMPO has no aerosol product of its own: its aerosol output is the UV aerosol index of the
total ozone retrieval, hence ``aerosol_l3``. Each group appends one row per scan along
``time`` on the fixed ``latitude``/``longitude`` grid; values are the source's, in its
dtypes and units, with the source names kept as ``source_name``. Open the store with
``xarray.open_datatree(store, engine="zarr", consolidated=False)`` or one product with
``xarray.open_zarr(store, group="cloud_l3", consolidated=False)``.

Left out: the L3 viewing geometry (solar and viewing zenith, relative azimuth), which
follows from each scan's time and the grid; everything else in the files is kept.

Scans are appended in the order they are fetched (the lanes write the last week first),
so ``time`` is not sorted; see the store's reorder step.
"""

from __future__ import annotations

import os
import pathlib
import re
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, List

import h5py
import icechunk
import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.earthdata import EarthdataS3, granules, s3_url
from planetary_datasets.common.store import STORE_READ_ERRORS

STORE_PREFIX = "bkr/geo/tempo.icechunk"
#: TEMPO's time epoch (GPS time zero), as its files declare it.
EPOCH = "1980-01-06T00:00:00"
FIRST_DAY = "2023-08-02"


@dataclass(frozen=True)
class Product:
    """One TEMPO L3 collection and the group it goes to."""

    group: str
    short_name: str
    version: str
    title: str


PRODUCTS = {
    p.group: p
    for p in (
        Product("cloud_l3", "TEMPO_CLDO4_L3", "V04", "TEMPO gridded cloud fraction and pressure"),
        Product(
            "aerosol_l3",
            "TEMPO_O3TOT_L3",
            "V04",
            "TEMPO gridded UV aerosol index (total ozone retrieval)",
        ),
    )
}

#: HDF5 groups whose fields are left out (derivable from time and grid).
SKIPPED_GROUPS = ("geolocation",)
#: Readable names for the source's fields; anything else is put into snake case.
NAMES = {
    "CloudRadianceFraction440": "cloud_radiance_fraction_440nm",
    "CloudRadianceFraction466": "cloud_radiance_fraction_466nm",
    "cloud_fraction": "effective_cloud_fraction",
    "cloud_pressure": "cloud_pressure",
    "GLER440": "surface_lambertian_equivalent_reflectivity_440nm",
    "GLER466": "surface_lambertian_equivalent_reflectivity_466nm",
    "surface_pressure": "surface_pressure",
    "column_amount_o3": "total_column_ozone",
    "fc": "effective_cloud_fraction",
    "o3_below_cloud": "ozone_below_cloud",
    "radiative_cloud_frac": "radiative_cloud_fraction",
    "so2_index": "so2_index",
    "uv_aerosol_index": "uv_aerosol_index",
    "max_column_sample": "maximum_total_column_ozone_sample",
    "min_column_sample": "minimum_total_column_ozone_sample",
    "num_column_samples": "total_column_ozone_sample_count",
    "terrain_height": "terrain_height",
    "terrain_pressure": "terrain_pressure",
    "weight": "area_weight",
}
#: Attributes of a field worth keeping; HDF5's dimension bookkeeping is not.
KEPT_ATTRS = (
    "units",
    "long_name",
    "standard_name",
    "comment",
    "_FillValue",
    "valid_min",
    "valid_max",
    "flag_values",
    "flag_masks",
    "flag_meanings",
)
#: Spatial chunk of every gridded field: 5 x 5 chunks per scan, ~3.6 MB each raw.
CHUNK = (1, 590, 1550)
#: A day is recorded complete this long after it ends, once every listed scan is in.
SETTLE_DAYS = 3
#: Group attribute listing the days recorded complete.
DAYS_ATTR = "days_complete"
ROOT_ATTRS = {
    "title": "NASA TEMPO Level 3 cloud and aerosol products",
    "source": "NASA LaRC ASDC, TEMPO V04 (TEMPO_CLDO4_L3, TEMPO_O3TOT_L3), via Earthdata",
    "layout": (
        "one group per product (cloud_l3, aerosol_l3); each appends scans along time on "
        "the fixed 0.02 degree latitude/longitude grid. Open with "
        "xr.open_datatree(store, engine='zarr', consolidated=False) or one product with "
        "xr.open_zarr(store, group='cloud_l3', consolidated=False)."
    ),
    "references": "https://asdc.larc.nasa.gov/project/TEMPO",
}


class GridChanged(ValueError):
    """A scan's grid differs from the one its group already holds."""


def snake(name: str) -> str:
    """``CloudRadianceFraction440`` -> ``cloud_radiance_fraction440``."""
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def readable(leaf: str) -> str:
    """The store's name for a source field."""
    return NAMES.get(leaf) or snake(leaf)


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, np.ndarray):
        return ", ".join(_text(v) for v in value.ravel())
    return str(value)


def _attrs(dataset: h5py.Dataset) -> dict:
    attrs = {}
    for key in KEPT_ATTRS:
        if key not in dataset.attrs:
            continue
        value = dataset.attrs[key]
        if isinstance(value, (bytes, str)):
            value = _text(value)
        elif isinstance(value, np.ndarray) and value.size == 1:
            value = value.ravel()[0].item()
        elif isinstance(value, np.ndarray):
            value = value.tolist()
        attrs[key] = value
    return attrs


def scan_time(seconds: float) -> np.datetime64:
    """Seconds since :data:`EPOCH` as a datetime64[ns]."""
    return np.datetime64(EPOCH, "ns") + np.timedelta64(int(round(seconds * 1e9)), "ns")


def read_scan(path: pathlib.Path) -> xr.Dataset:
    """One L3 scan as ``(time, latitude, longitude)`` fields plus per-scan metadata.

    Raises:
        ValueError: The file has no ``latitude``/``longitude``/``time``.
    """
    variables: dict[str, xr.Variable] = {}
    with h5py.File(path, "r") as f:
        for required in ("latitude", "longitude", "time"):
            if required not in f:
                raise ValueError(f"{path.name}: no {required}")
        lat = np.asarray(f["latitude"][:])
        lon = np.asarray(f["longitude"][:])
        grid = (lat.size, lon.size)
        start = scan_time(float(f["time"][0]))

        def visit(name: str, obj) -> None:
            if not isinstance(obj, h5py.Dataset) or name in ("latitude", "longitude", "time"):
                return
            group, _, leaf = name.rpartition("/")
            if group.split("/")[0] in SKIPPED_GROUPS:
                return
            values = np.asarray(obj[()])
            if values.shape == grid:
                values = values[np.newaxis]
            if values.shape != (1, *grid):
                logger.debug(f"{path.name}: {name} {values.shape} is not on the grid; skipped")
                return
            target = readable(leaf)
            if target in variables:
                target = f"{snake(group)}_{target}"
            attrs = {**_attrs(obj), "source_name": name}
            variables[target] = xr.Variable(("time", "latitude", "longitude"), values, attrs)

        f.visititems(visit)
        meta = {k: _text(v) for k, v in f.attrs.items() if k != "coremetadata"}
    inputs = [s for s in re.split(r"[,\s]+", meta.get("input_files", "")) if s]
    per_scan = {
        "scan_name": np.array([path.name], dtype=object),
        "scan_number": np.array([int(float(meta.get("scan_num", -1)))], dtype="int32"),
        "time_coverage_end": np.array(
            [pd.Timestamp(meta["time_coverage_end"]).tz_convert(None).to_datetime64()]
        )
        if meta.get("time_coverage_end")
        else np.array([np.datetime64("NaT", "ns")]),
        "processing_version": np.array([int(float(meta.get("processing_version", -1)))], "int16"),
        "production_date_time": np.array([meta.get("production_date_time", "")], dtype=object),
        "l2_granule_count": np.array([len(inputs)], dtype="int16"),
    }
    for name, values in per_scan.items():
        variables[name] = xr.Variable(("time",), values)
    ds = xr.Dataset(
        variables,
        coords={
            "time": ("time", np.array([start]), {"long_name": "scan start time"}),
            "latitude": ("latitude", lat, {"units": "degrees_north", "long_name": "latitude"}),
            "longitude": ("longitude", lon, {"units": "degrees_east", "long_name": "longitude"}),
        },
    )
    return ds


def _encoding(ds: xr.Dataset) -> dict:
    """Chunks, compression and the source fill values, for a group's first scan."""
    compressor = zarr.codecs.BloscCodec(cname="zstd", clevel=5, shuffle="bitshuffle")
    encoding: dict[str, dict] = {}
    for name, variable in ds.variables.items():
        entry: dict = {}
        if variable.dims == ("time", "latitude", "longitude"):
            entry["chunks"] = tuple(min(c, n) for c, n in zip(CHUNK, (1, *variable.shape[1:])))
            entry["compressors"] = (compressor,)
        elif variable.dims == ("time",) and variable.dtype.kind not in "OUS":
            entry["chunks"] = (4096,)
        if "_FillValue" in variable.attrs:
            entry["_FillValue"] = np.asarray(variable.attrs["_FillValue"]).astype(variable.dtype)[
                ()
            ]
        elif variable.dtype.kind in "fiu" and name not in ("latitude", "longitude"):
            # Otherwise xarray declares a fill (NaN, or 0) the source never had.
            entry["_FillValue"] = None
        if name in ("time", "time_coverage_end"):
            entry.update(units=f"seconds since {EPOCH}", dtype="float64")
        encoding[name] = entry
    return encoding


def ensure_root(repo: icechunk.Repository) -> None:
    """Write the root group's attributes once, before any product group."""
    for attempt in range(1, 6):
        try:
            root = zarr.open_group(repo.readonly_session("main").store, mode="r")
            if all(root.attrs.get(k) == v for k, v in ROOT_ATTRS.items()):
                return
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            pass
        session = repo.writable_session("main")
        zarr.open_group(session.store, mode="a").attrs.update(ROOT_ATTRS)
        try:
            session.commit("tempo: root group", rebase_with=icechunk.ConflictDetector())
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == 5:
                raise
            time.sleep(attempt)


def append_scan(repo: icechunk.Repository, product: Product, ds: xr.Dataset) -> bool:
    """Append one scan to ``product``'s group. False if the scan is stored already.

    Raises:
        GridChanged: The scan's grid is not the group's.
    """
    name = str(ds["scan_name"].values[0])
    for attempt in range(1, 6):
        session = repo.writable_session("main")
        try:
            group = zarr.open_group(session.store, path=product.group, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            group = None
        exists = group is not None and "scan_name" in group
        if exists:
            if name in {str(v) for v in group["scan_name"][:]}:
                return False
            for axis in ("latitude", "longitude"):
                if not np.array_equal(group[axis][:], ds[axis].values):
                    raise GridChanged(f"{product.group}: {name} is on a different {axis} grid")
            attrs = dict(group.attrs)
            part = ds.drop_vars(["latitude", "longitude"])
            for variable in part.variables.values():
                variable.attrs.pop("_FillValue", None)
            to_icechunk(part, session, group=product.group, mode="a", append_dim="time")
        else:
            attrs = {"title": product.title, "source_collection": product.short_name}
            attrs["source_version"] = product.version
            created = ds.copy()
            encoding = _encoding(created)
            for variable in created.variables.values():
                variable.attrs.pop("_FillValue", None)
            created.attrs = attrs
            to_icechunk(created, session, group=product.group, mode="w-", encoding=encoding)
        node = zarr.open_group(session.store, path=product.group, mode="a")
        if dict(node.attrs) != attrs:
            node.attrs.update(attrs)
        try:
            session.commit(
                f"tempo {product.group}: {name}", rebase_with=icechunk.ConflictDetector()
            )
            logger.info(f"tempo {product.group}: wrote {name}")
            return True
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == 5:
                raise
            time.sleep(attempt)
    raise AssertionError("unreachable")


class TempoL3Provider(BaseProvider):
    """TEMPO L3 cloud and aerosol scans, one day per partition, a group per product."""

    name = "tempo_l3"
    append_dim = "time"
    store_prefix = STORE_PREFIX

    def __init__(
        self, config=None, products: Iterable[str] | None = None, download_workers: int = 2
    ):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            products: Groups to write (keys of :data:`PRODUCTS`); all by default.
            download_workers: Scans downloaded at once (each is ~140-220 MB, ~1 GB in memory
                while it is written).
        """
        super().__init__(config=config)
        names = list(products) if products is not None else list(PRODUCTS)
        unknown = set(names) - set(PRODUCTS)
        if unknown:
            raise ValueError(f"unknown TEMPO products: {sorted(unknown)}")
        self.products = [PRODUCTS[n] for n in names]
        self.download_workers = download_workers
        self._s3: EarthdataS3 | None = None
        self._http = requests.Session()

    @property
    def s3(self) -> EarthdataS3:
        """Earthdata S3 credentials for LaRC ASDC, built on first use."""
        if self._s3 is None:
            self._s3 = EarthdataS3("asdc", self.config)
        return self._s3

    # ------------------------------------------------------------------ state

    def _node(self, product: Product) -> zarr.Group | None:
        try:
            store = self.get_icechunk_repo().readonly_session("main").store
            return zarr.open_group(store, path=product.group, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    def days_complete(self, product: Product) -> set[str]:
        """Days (``YYYY-MM-DD``) recorded complete for ``product``."""
        node = self._node(product)
        return set(node.attrs.get(DAYS_ATTR, [])) if node is not None else set()

    def stored_scans(self, product: Product) -> set[str]:
        """File names of the scans ``product``'s group holds."""
        node = self._node(product)
        if node is None or "scan_name" not in node:
            return set()
        return {str(v) for v in node["scan_name"][:]}

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Days some product has not been recorded complete for."""
        done = {p.group: self.days_complete(p) for p in self.products}
        return [
            day
            for day in desired
            if day >= pd.Timestamp(FIRST_DAY)
            and any(f"{day:%Y-%m-%d}" not in done[p.group] for p in self.products)
        ]

    # ------------------------------------------------------------------ writing

    def _download(self, url: str, directory: pathlib.Path) -> pathlib.Path | None:
        dest = directory / url.rsplit("/", 1)[1]
        for attempt in range(1, 4):
            try:
                self.s3.filesystem().get(url, str(dest))
                return dest
            except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
                logger.warning(f"{self.name}: {url} attempt {attempt} failed: {exc}")
        return None

    def run_product_day(self, product: Product, day: pd.Timestamp, pool: ThreadPoolExecutor) -> int:
        """Write the day's scans of one product not yet stored. Returns scans written."""
        listed = {
            s3_url(entry).rsplit("/", 1)[1]: s3_url(entry)
            for entry in granules(
                product.short_name, product.version, day, day + pd.Timedelta(1, "D"), self._http
            )
        }
        stored = self.stored_scans(product)
        todo = [listed[n] for n in sorted(listed) if n not in stored]
        written = 0
        if todo:
            repo = self.get_icechunk_repo()
            ensure_root(repo)
            scratch = pathlib.Path(
                tempfile.mkdtemp(prefix=f"tempo-{product.group}-", dir=self.config.scratch_dir)
            )
            # The box guard removes scratch whose owner has died.
            (scratch / ".pid").write_text(str(os.getpid()))
            try:
                futures = [pool.submit(self._download, url, scratch) for url in todo]
                for url, future in zip(todo, futures):
                    path = future.result()
                    if path is None:
                        logger.warning(f"{self.name}: {url} failed to download; next pass")
                        continue
                    try:
                        written += append_scan(repo, product, read_scan(path))
                    except (OSError, ValueError) as exc:
                        if isinstance(exc, GridChanged):
                            raise
                        logger.warning(f"{self.name}: {path.name} unreadable ({exc}); skipped")
                    finally:
                        path.unlink(missing_ok=True)
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
            stored = self.stored_scans(product)
        self._settle(product, day, set(listed), stored)
        return written

    def _settle(
        self, product: Product, day: pd.Timestamp, listed: set[str], stored: set[str]
    ) -> None:
        """Record the day complete once it is old enough and every listed scan is in."""
        now = pd.Timestamp.now("UTC").tz_localize(None)
        if now - day <= pd.Timedelta(SETTLE_DAYS + 1, "D") or not listed <= stored:
            return
        repo = self.get_icechunk_repo()
        for attempt in range(1, 6):
            session = repo.writable_session("main")
            try:
                node = zarr.open_group(session.store, path=product.group, mode="a")
            except zarr.errors.GroupNotFoundError:
                if listed:
                    return
                ensure_root(repo)
                session = repo.writable_session("main")
                node = zarr.open_group(session.store, path=product.group, mode="a")
            days = set(node.attrs.get(DAYS_ATTR, []))
            if f"{day:%Y-%m-%d}" in days:
                return
            node.attrs[DAYS_ATTR] = sorted(days | {f"{day:%Y-%m-%d}"})
            try:
                session.commit(
                    f"tempo {product.group}: {day:%Y-%m-%d} complete",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return
            except (icechunk.ConflictError, icechunk.RebaseFailedError):
                if attempt == 5:
                    raise
                time.sleep(attempt)

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Every product's scans for the day starting at ``it``. True if any were written."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        written = 0
        with ThreadPoolExecutor(max_workers=self.download_workers) as pool:
            for product in self.products:
                if day < pd.Timestamp(FIRST_DAY):
                    continue
                if check_present and f"{day:%Y-%m-%d}" in self.days_complete(product):
                    continue
                n = self.run_product_day(product, day, pool)
                if n:
                    logger.info(f"{self.name}: {product.group} {day:%Y-%m-%d}: {n} scan(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` downloads and writes product by product."""
        raise NotImplementedError("TempoL3Provider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("TempoL3Provider writes through run_partition")


__all__ = ["PRODUCTS", "GridChanged", "Product", "TempoL3Provider", "append_scan", "read_scan"]
