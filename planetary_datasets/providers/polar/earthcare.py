"""EarthCARE: every validated ESA L1 and L2 product, in one icechunk store.

ESA distributes EarthCARE through MAAP (see :mod:`planetary_datasets.common.maap`), one
HDF5 file per product per *frame*: an eighth of an orbit, about 12 minutes, labelled by
orbit number and frame letter (``13601E``). The validated collections start on
2024-08-11 (L1) and 2025-03-11 (L2), and grow by roughly 120 frames a day per product.

Store layout (``bkr/polar/earthcare.icechunk``). Every product shares the store, so every
variable and dimension carries its product as a prefix, lower-cased with runs of
underscores collapsed: ``CPR_CLD_2A`` becomes ``cpr_cld_2a``, and its ``along_track``
dimension ``cpr_cld_2a_along_track``. Per product:

``<p>_along_track``
    The growing axis: every variable of the product's ``ScienceData`` group that runs
    along the track, frame after frame, transposed so the track comes first. The frames'
    14 s margins overlap their neighbours and are kept, so nothing is dropped;
    ``<p>_frame_index`` gives each row's frame, and the frame's ``<p>_header_frameStartTime``
    and ``..._frameStopTime`` mark where its nominal extent ends.
``<p>_frame``
    One row per frame: the product's ``HeaderData`` (orbit, frame letter, baseline,
    processor version, validity, ...) and any ``ScienceData`` variable that does not run
    along the track (a class table, per-frame scalars).
Fixed dimensions (height bins, across-track pixels, bands) are as in the files.

Values are copied exactly. Files are read without CF decoding: fill values become the
zarr fill values and scale factors stay as attributes, so ``xarray.open_zarr`` decodes them
just as it decodes the source file. Each variable is stored in the narrowest dtype that
holds every value exactly (:mod:`planetary_datasets.common.lossless`) and compressed with
zstd. Times are seconds since 2000-01-01, decoded by xarray from their ``units``.

A frame is identified by its orbit and frame letter, so it is written once whichever
baseline is processed later; when MAAP holds two baselines of a frame the newest is
taken. A day is recorded as complete once all of its frames are written and it is
:data:`SETTLE_DAYS` old, after which it is not listed again.

Environment: ``MAAP_OFFLINE_TOKEN`` (90 days; see :mod:`planetary_datasets.common.maap`).

Source: https://earth.esa.int/eogateway/missions/earthcare/data
"""

from __future__ import annotations

import collections
import pathlib
import re
import shutil
import tempfile
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
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common import multidim
from planetary_datasets.common.download import download_one
from planetary_datasets.common.maap import MaapAuth, search
from planetary_datasets.common.multidim import append_partition
from planetary_datasets.common.store import STORE_READ_ERRORS

L1_COLLECTION = "EarthCAREL1Validated_MAAP"
L2_COLLECTION = "EarthCAREL2Validated_MAAP"


@dataclass(frozen=True)
class Product:
    """One EarthCARE product type."""

    code: str
    collection: str
    start: str

    @property
    def prefix(self) -> str:
        """Lower-case store prefix, e.g. ``cpr_cld_2a``."""
        return re.sub(r"_+", "_", self.code.lower())


#: L1 and L1-like auxiliary products in the validated collection.
L1_CODES = ("ATL_NOM_1B", "CPR_NOM_1B", "MSI_NOM_1B", "MSI_RGR_1C", "AUX_JSG_1D")
#: ESA L2 products in the validated collection.
L2_CODES = (
    "ACM_CAP_2B",
    "ACM_COM_2B",
    "ACM_RT__2B",
    "AC__TC__2B",
    "ALL_3D__2B",
    "ALL_DF__2B",
    "AM__ACD_2B",
    "AM__CTH_2B",
    "ATL_AER_2A",
    "ATL_ALD_2A",
    "ATL_CTH_2A",
    "ATL_EBD_2A",
    "ATL_FM__2A",
    "ATL_ICE_2A",
    "ATL_TC__2A",
    "CPR_CD__2A",
    "CPR_CLD_2A",
    "CPR_FMR_2A",
    "CPR_TC__2A",
    "MSI_AOT_2A",
    "MSI_CM__2A",
    "MSI_COP_2A",
)
PRODUCTS = {
    **{code: Product(code, L1_COLLECTION, "2024-08-11") for code in L1_CODES},
    **{code: Product(code, L2_COLLECTION, "2025-03-11") for code in L2_CODES},
}

#: Days after a day ends before it is recorded complete and no longer listed.
SETTLE_DAYS = 3
#: Attribute of ``<p>_frame_key`` listing the product's complete days. Kept on the
#: product's own array, not the store's root, so writers of different products running
#: side by side never change the same metadata.
DAYS_ATTR = "days_complete"
#: Uncompressed bytes of frames read into memory before they are written.
BATCH_BYTES = 3 * 2**30
#: Names the track dimension goes by across the products.
ALONG_TRACK_NAMES = ("along_track", "nray")
#: Header groups copied into the per-frame variables.
HEADER_GROUPS = (
    "HeaderData/FixedProductHeader",
    "HeaderData/VariableProductHeader/MainProductHeader",
)
#: Header values too long to repeat per frame without being data.
HEADER_SKIP = ("ConfigurationParameters",)
FRAME_KEY = re.compile(r"_(\d{5}[A-H])$")
FRAME_START = re.compile(r"_(\d{8})T\d{6}Z_")


def frame_key(product_name: str) -> str:
    """Orbit and frame letter of a product name, e.g. ``13601E``."""
    match = FRAME_KEY.search(product_name)
    if match is None:
        raise ValueError(f"not an EarthCARE frame product: {product_name}")
    return match.group(1)


def baseline(product_name: str) -> str:
    """The two-letter processing baseline, the last two characters of the file class."""
    return product_name.split("_")[1][2:4]


def _decode_strings(values: np.ndarray) -> np.ndarray:
    if values.dtype.kind == "S":
        return np.char.decode(values, "utf-8", errors="replace").astype(object)
    return values


def _science_groups(path: pathlib.Path) -> list[str]:
    """``ScienceData``, or its subgroups when the variables live one level down."""
    with h5py.File(path, "r") as f:
        science = f["ScienceData"]
        if any(isinstance(v, h5py.Dataset) for v in science.values()):
            return ["ScienceData"]
        return [f"ScienceData/{name}" for name, v in science.items() if isinstance(v, h5py.Group)]


def _name_phony_dims(ds: xr.Dataset) -> xr.Dataset:
    """Name ``phony_dim_*`` from the ``DimensionNames`` attributes JAXA products carry.

    The attribute is not reliable on its own (``covarianceCoeff`` lists its dimensions out
    of order; a length-1 header field claims ``nray``), so each phony dimension takes the
    name most variables give it, a name goes to only one size, and a dimension left
    without one is called ``size_<n>``.
    """
    votes: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for variable in ds.variables.values():
        declared = variable.attrs.get("DimensionNames")
        if not isinstance(declared, str):
            continue
        labels = [label.strip() for label in declared.split(",")]
        if len(labels) != len(variable.dims):
            continue
        for dim, label in zip(variable.dims, labels):
            if dim.startswith("phony_dim_") and label and not label.isdigit():
                votes[dim][label] += 1
    names: dict[str, str] = {}
    taken: dict[str, int] = {}
    ranked = sorted(votes.items(), key=lambda kv: -kv[1].most_common(1)[0][1])
    for dim, counter in ranked:
        label = counter.most_common(1)[0][0]
        if taken.get(label, ds.sizes[dim]) == ds.sizes[dim]:
            names[dim] = label
            taken[label] = ds.sizes[dim]
    for dim in ds.dims:
        if dim.startswith("phony_dim_") and dim not in names:
            names[dim] = f"size_{ds.sizes[dim]}"
    return ds.rename_dims(names) if names else ds


def _header(path: pathlib.Path) -> dict[str, np.ndarray]:
    """Scalar header values as one-element arrays, keyed by their leaf name."""
    out: dict[str, np.ndarray] = {}
    with h5py.File(path, "r") as f:
        for root in HEADER_GROUPS:
            if root not in f:
                continue

            def keep(name: str, obj) -> None:
                leaf = name.rsplit("/", 1)[-1]
                if not isinstance(obj, h5py.Dataset) or obj.shape not in ((), (1,)):
                    return
                if leaf in HEADER_SKIP:
                    return
                key = name.replace("/", "_")
                if key in out:
                    key = f"{root.rsplit('/', 1)[-1]}_{key}"
                out[key] = _decode_strings(np.asarray(obj[()]).reshape(1))

            f[root].visititems(keep)
    return out


def read_frame(path: pathlib.Path, product: Product) -> xr.Dataset:
    """One frame file as prefixed ``<p>_along_track`` and ``<p>_frame`` variables.

    Raises:
        ValueError: The file has no along-track dimension this module recognises.
    """
    parts = []
    for group in _science_groups(path):
        with xr.open_dataset(
            path,
            group=group,
            engine="h5netcdf",
            phony_dims="access",
            mask_and_scale=False,
            decode_times=False,
            decode_timedelta=False,
        ) as opened:
            parts.append(_name_phony_dims(opened.load()))
    ds = xr.merge(parts, compat="no_conflicts", combine_attrs="drop_conflicts")
    track = next((d for d in ALONG_TRACK_NAMES if d in ds.dims), None)
    if track is None:
        raise ValueError(f"{product.code}: no along-track dimension in {sorted(ds.dims)}")
    ds = ds.rename_dims({track: "along_track"}) if track != "along_track" else ds

    p = product.prefix
    variables: dict[str, xr.Variable] = {}
    for name, variable in ds.variables.items():
        values = _decode_strings(variable.values)
        attrs = {k: v for k, v in variable.attrs.items() if k != "DimensionNames"}
        if "along_track" in variable.dims:
            order = ["along_track", *[d for d in variable.dims if d != "along_track"]]
            variable = xr.Variable(variable.dims, values, attrs).transpose(*order)
            dims = tuple(f"{p}_{d}" for d in variable.dims)
            variables[f"{p}_{name}"] = xr.Variable(dims, variable.values, attrs)
        else:
            dims = (f"{p}_frame", *(f"{p}_{d}" for d in variable.dims))
            variables[f"{p}_{name}"] = xr.Variable(dims, values[np.newaxis], attrs)
    for key, values in _header(path).items():
        variables[f"{p}_header_{key}"] = xr.Variable((f"{p}_frame",), values)
    variables[f"{p}_frame_key"] = xr.Variable(
        (f"{p}_frame",), np.array([frame_key(path.stem)], dtype=object)
    )
    variables[f"{p}_product_name"] = xr.Variable(
        (f"{p}_frame",), np.array([path.stem], dtype=object)
    )
    return xr.Dataset(variables, attrs={})


def concat_frames(frames: list[xr.Dataset], product: Product, first_index: int) -> xr.Dataset:
    """Frames of one product, end to end, with each track row's frame index."""
    return multidim.concat_frames(frames, product.prefix, first_index)


class EarthCAREProvider(BaseProvider):
    """Every validated EarthCARE product, one day of frames per partition."""

    name = "earthcare"
    append_dim = "time"
    store_prefix = "bkr/polar/earthcare.icechunk"

    def __init__(
        self,
        config=None,
        products: Iterable[str] | None = None,
        download_workers: int = 48,
    ):
        """Build the provider.

        Args:
            config: Override the process-wide configuration.
            products: Product codes to ingest; all of :data:`PRODUCTS` by default.
            download_workers: Concurrent downloads from MAAP. Each stream is slow (about
                0.5 MB/s), so throughput comes from running many.
        """
        super().__init__(config=config)
        codes = list(products) if products is not None else list(PRODUCTS)
        unknown = set(codes) - set(PRODUCTS)
        if unknown:
            raise ValueError(f"unknown EarthCARE products: {sorted(unknown)}")
        self.products = [PRODUCTS[c] for c in codes]
        self.download_workers = download_workers
        self._auth: MaapAuth | None = None
        self._http = requests.Session()

    @property
    def auth(self) -> MaapAuth:
        """The MAAP token source, built on first use."""
        if self._auth is None:
            self._auth = MaapAuth(self.config)
        return self._auth

    # ------------------------------------------------------------------ listing

    def _group(self) -> zarr.Group | None:
        try:
            store = self.get_icechunk_repo().readonly_session("main").store
            return zarr.open_group(store, mode="r")
        except (*STORE_READ_ERRORS, zarr.errors.GroupNotFoundError):
            return None

    @staticmethod
    def _days(group: zarr.Group | None, product: Product) -> set[str]:
        name = f"{product.prefix}_frame_key"
        if group is None or name not in group:
            return set()
        return set(group[name].attrs.get(DAYS_ATTR, []))

    def days_complete(self, product: Product) -> set[str]:
        """Days (``YYYY-MM-DD``) recorded complete for ``product``."""
        return self._days(self._group(), product)

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Days some product has not been recorded complete for."""
        group = self._group()
        done = {p.code: self._days(group, p) for p in self.products}
        return [
            day
            for day in desired
            if any(
                f"{day:%Y-%m-%d}" not in done[p.code]
                for p in self.products
                if day >= pd.Timestamp(p.start)
            )
        ]

    def frames(self, product: Product, day: pd.Timestamp) -> dict[str, dict]:
        """The day's frames of ``product``, newest baseline of each, keyed by frame."""
        start = f"{day:%Y-%m-%d}T00:00:00Z"
        end = f"{day + pd.Timedelta(1, 'D') - pd.Timedelta(1, 's'):%Y-%m-%dT%H:%M:%SZ}"
        chosen: dict[str, dict] = {}
        for item in search(
            product.collection, start, end, {"productType": product.code}, session=self._http
        ):
            if "enclosure_h5" not in item.get("assets", {}):
                continue
            key = frame_key(item["id"])
            # The frame belongs to the day it starts on; the search also returns the one
            # that started the evening before.
            started = FRAME_START.search(item["id"])
            if started is None or started.group(1) != f"{day:%Y%m%d}":
                continue
            if key not in chosen or baseline(item["id"]) > baseline(chosen[key]["id"]):
                chosen[key] = item
        return chosen

    def stored_frames(self, product: Product) -> tuple[set[str], int]:
        """Frame keys already stored for ``product``, and how many frames that is."""
        group = self._group()
        name = f"{product.prefix}_frame_key"
        if group is None or name not in group:
            return set(), 0
        keys = group[name][:]
        return {str(k) for k in keys}, len(keys)

    # ------------------------------------------------------------------ writing

    def _download(self, item: dict, directory: pathlib.Path) -> pathlib.Path | None:
        href = item["assets"]["enclosure_h5"]["href"]
        return download_one(
            href,
            directory / f"{item['id']}.h5",
            retries=5,
            backoff=10.0,
            timeout=1800,
            storage_options={"headers": self.auth.headers()},
        )

    def _write(self, product: Product, paths: list[pathlib.Path], first_index: int) -> int:
        frames = [read_frame(path, product) for path in paths]
        ds = concat_frames(frames, product, first_index)
        append_partition(
            self.get_icechunk_repo(),
            ds,
            None,
            fixed_dims=[
                d
                for d in ds.dims
                if d not in (f"{product.prefix}_along_track", f"{product.prefix}_frame")
            ],
            label=f"{self.name} {product.code}",
        )
        return len(frames)

    def run_product_day(self, product: Product, day: pd.Timestamp, pool: ThreadPoolExecutor) -> int:
        """Write the day's frames of one product not yet stored. Returns frames written."""
        listed = self.frames(product, day)
        stored, count = self.stored_frames(product)
        todo = [listed[k] for k in sorted(listed) if k not in stored]
        if not todo:
            self._settle(product, day, listed, stored)
            return 0
        scratch = pathlib.Path(
            tempfile.mkdtemp(prefix=f"earthcare-{product.prefix}-", dir=self.config.scratch_dir)
        )
        written = 0
        try:
            futures = [pool.submit(self._download, item, scratch) for item in todo]
            batch: list[pathlib.Path] = []
            size = 0
            for item, future in zip(todo, futures):
                path = future.result()
                if path is None:
                    logger.warning(f"{self.name}: {item['id']} failed to download; next pass")
                    continue
                batch.append(path)
                size += path.stat().st_size * 4
                if size >= BATCH_BYTES:
                    written += self._write(product, batch, count + written)
                    for done in batch:
                        done.unlink(missing_ok=True)
                    batch, size = [], 0
            if batch:
                written += self._write(product, batch, count + written)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        stored, _ = self.stored_frames(product)
        self._settle(product, day, listed, stored)
        return written

    def _settle(self, product: Product, day: pd.Timestamp, listed: dict, stored: set[str]) -> None:
        """Record the day complete once it is old enough and every listed frame is in."""
        now = pd.Timestamp.now("UTC").tz_localize(None)
        if (
            now - day <= pd.Timedelta(SETTLE_DAYS + 1, "D")
            or not stored
            or not set(listed) <= stored
        ):
            return
        name = f"{product.prefix}_frame_key"
        for attempt in range(1, 6):
            session = self.get_icechunk_repo().writable_session("main")
            array = zarr.open_group(session.store, mode="a")[name]
            days = set(array.attrs.get(DAYS_ATTR, []))
            if f"{day:%Y-%m-%d}" in days:
                return
            array.attrs[DAYS_ATTR] = sorted(days | {f"{day:%Y-%m-%d}"})
            try:
                session.commit(
                    f"{self.name} {product.code}: {day:%Y-%m-%d} complete",
                    rebase_with=icechunk.ConflictDetector(),
                )
                return
            except (icechunk.ConflictError, icechunk.RebaseFailedError):
                if attempt == 5:
                    raise

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Every product's frames for the day starting at ``it``. True if any were written."""
        day = pd.Timestamp(it).normalize()
        if day.tzinfo is not None:
            day = day.tz_convert("UTC").tz_localize(None)
        group = self._group() if check_present else None
        written = 0
        with ThreadPoolExecutor(max_workers=self.download_workers) as pool:
            for product in self.products:
                if day < pd.Timestamp(product.start) or f"{day:%Y-%m-%d}" in self._days(
                    group, product
                ):
                    continue
                n = self.run_product_day(product, day, pool)
                if n:
                    logger.info(f"{self.name}: {product.code} {day:%Y-%m-%d}: {n} frame(s)")
                written += n
        return written > 0

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Not used: :meth:`run_partition` downloads and writes product by product."""
        raise NotImplementedError("EarthCAREProvider writes through run_partition")

    def process(self, input_files, it, temp_dir=None, **kwargs) -> xr.Dataset:
        """Not used: see :meth:`fetch`."""
        raise NotImplementedError("EarthCAREProvider writes through run_partition")


__all__ = [
    "EarthCAREProvider",
    "PRODUCTS",
    "Product",
    "baseline",
    "concat_frames",
    "frame_key",
    "read_frame",
]
