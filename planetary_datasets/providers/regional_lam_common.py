"""Shared engine for the regional limited-area model (LAM) providers.

DMI HARMONIE, the HRRR Alaska nest, the NAM Hawaii nest and MeteoSwiss KENDA-CH1 are all
the same pipeline wearing different hats: open a handful of GRIB files per init time with
``cfgrib``, discard the level types that are not wanted, rename what is left after its
GRIB ``long_name``, merge the pieces, reduce precision and append to an icechunk store.

The scripts this replaces each carried their own ~90-line copy of the level-type filter,
differing only in a couple of thresholds. That filter lives here once, parameterised by
:class:`GribMergeSpec`; the per-model modules are left holding only their file naming, the
rules that genuinely differ and the final chunking.

This mirrors the split already used by
``planetary_datasets/providers/virtualized/goes_radf_common.py``.
"""

from __future__ import annotations

import os
import pathlib
import time
from dataclasses import dataclass
from typing import Callable, List, Sequence

import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_many

#: Level types that carry fields nobody asked for, or that collide with the levels that are
#: kept. A cfgrib sub-dataset exposing any of these as a coordinate is dropped whole.
SKIP_LEVEL_COORDS: tuple[str, ...] = (
    "sigma",
    "sigmaLayer",
    "tropopause",
    "pressureFromGroundLayer",
    "potentialVorticity",
    "planetaryBoundaryLayer",
    "nominalTop",
    "middleCloudTop",
    "lowCloudTop",
    "highCloudTop",
    "middleCloudLayer",
    "middleCloudBottom",
    "maxWind",
    "isothermZero",
    "lowCloudLayer",
    "lowCloudBottom",
    "highestTroposphericFreezing",
    "highCloudLayer",
    "highCloudBottom",
    "heightAboveSea",
    "heightAboveGroundLayer",
    "convectiveCloudTop",
    "convectiveCloudLayer",
    "convectiveCloudBottom",
    "boundaryLayerCloudLayer",
    "boundaryLayerCloudBottom",
    "boundaryLayerCloudTop",
)

#: ``heightAboveGround`` as a *dimension* means a stack of near-surface levels, which is
#: handled by suffixing single-level fields instead; as a scalar coordinate it is fine.
SKIP_LEVEL_DIMS: tuple[str, ...] = ("heightAboveGround",)

#: Coordinates worth carrying into the merged dataset. Everything else cfgrib attaches
#: (GRIB edition, step type, surface markers and so on) is dropped so that sub-datasets
#: from different files can be merged without conflicts.
COORDS_TO_KEEP: tuple[str, ...] = (
    "time",
    "values",
    "step",
    "latitude",
    "longitude",
    "valid_time",
    "y",
    "x",
    "isobaricInhPa",
    "heightAboveGroundLayer",
    "heightAboveGround",
    "depthBelowLandLayer",
)

#: Variables dropped once renamed: diagnostics that duplicate fields already kept, or that
#: are undefined over most of the domain.
DROP_AFTER_RENAME: tuple[str, ...] = (
    "pressure_of_level_from_which_parcel_was_lifted",
    "geometric_vertical_velocity",
    "best_(4-layer)_lifted_index",
    "mslp_(maps_system_reduction)",
    "surface_lifted_index",
)

#: Short names that cfgrib cannot resolve, or that are simulated-imagery/radar products
#: stored at a resolution the archive does not need.
DROP_SHORT_NAME_HINTS: tuple[str, ...] = ("SBT", "refc", "refd")


def long_name_slug(name: str, strip_parens: bool = False) -> str:
    """Slugify a GRIB ``long_name`` into a variable name.

    Args:
        name: The ``long_name`` attribute.
        strip_parens: Remove parentheses as well. The NOAA nests keep them, because the
            drop lists built against those archives spell names such as
            ``best_(4-layer)_lifted_index``; HARMONIE strips them.
    """
    slug = name.replace(" ", "_").lower()
    if strip_parens:
        slug = slug.replace("(", "").replace(")", "")
    return slug


def soil_level_count(coord: xr.DataArray) -> int:
    """Number of soil levels on a ``depthBelowLandLayer`` coordinate.

    A scalar coordinate counts as zero levels: it is a single layer masquerading as the
    whole soil column, and merging it with the real profile produces conflicts.
    """
    return 0 if coord.shape == () else int(coord.size)


@dataclass(frozen=True)
class GribMergeSpec:
    """Per-model configuration for :func:`merge_grib_files`.

    Attributes:
        coords_to_keep: Coordinates that survive the merge.
        skip_coords: Level-type coordinates whose sub-datasets are dropped whole.
        skip_dims: Dimensions whose presence drops a sub-dataset whole.
        min_soil_levels: Drop soil sub-datasets with fewer than this many levels. HRRR
            Alaska publishes both a 9-layer profile and thinner duplicates; the NAM Hawaii
            nest only needs the scalar single-layer case excluded.
        drop_vars: Renamed variables to drop before merging.
        extra_skips: Model-specific predicates applied after renaming. Returning True
            drops the sub-dataset.
        drop_coords_after_merge: Coordinates dropped from the merged result. The Hawaii
            nest concatenates on ``valid_time`` so drops ``time``/``step``; Alaska
            concatenates on ``step`` so drops ``valid_time``.
    """

    coords_to_keep: tuple[str, ...] = COORDS_TO_KEEP
    skip_coords: tuple[str, ...] = SKIP_LEVEL_COORDS
    skip_dims: tuple[str, ...] = SKIP_LEVEL_DIMS
    min_soil_levels: int = 1
    drop_vars: tuple[str, ...] = DROP_AFTER_RENAME
    extra_skips: tuple[Callable[[xr.Dataset], bool], ...] = ()
    drop_coords_after_merge: tuple[str, ...] = ()


def resolve_renames(ds: xr.Dataset, targets: dict) -> dict:
    """Reduce ``targets`` to the renames :meth:`xarray.Dataset.rename` will actually accept.

    ``rename`` raises on a source name that is not in the dataset, and on a target name
    that is already taken. Both happen routinely here: GRIB reuses a ``long_name`` across
    level types, and the per-variable feeds do not always publish every variable. Raising
    would lose a whole timestep over one duplicated field, so the offending renames are
    dropped and the earlier claim on a name wins.

    Dropping one rename can create a new conflict — the variable that stays put still
    occupies its own name, which a later rename may have been counting on being vacated —
    so this drops one at a time and rechecks until the mapping is clean. Simultaneous
    swaps, where two variables exchange names, are left intact because ``rename`` applies
    the mapping in one go.
    """
    present = {str(name) for name in ds.variables}

    resolved: dict = {}
    for source, target in targets.items():
        if str(source) in present:
            resolved[source] = target
        else:
            logger.debug(f"cannot rename {source!r} to {target!r}: not in the dataset")

    while True:
        staying = present - {str(source) for source in resolved}
        claimed: set = set()
        conflicting = None
        for source, target in resolved.items():
            if target in staying or target in claimed:
                conflicting = source
                break
            claimed.add(target)
        if conflicting is None:
            return resolved
        logger.debug(
            f"cannot rename {conflicting!r} to {resolved[conflicting]!r}: name already taken"
        )
        del resolved[conflicting]


def _rename_by_grib_long_name(ds: xr.Dataset) -> xr.Dataset:
    """Rename variables after their ``long_name``, suffixed with the level they sit on.

    GRIB short names repeat across level types, so ``t`` from the surface file and ``t``
    from the pressure-level file would collide on merge. The level suffix keeps them
    apart. Variables cfgrib could not decode are dropped.
    """
    to_drop: list = []
    renames: dict = {}

    suffix = ""
    if "heightAboveGround" in ds.coords:
        suffix = f"_at_{ds.coords['heightAboveGround'].values}m"
    if "surface" in ds.coords:
        suffix = "_at_surface"
    if "atmosphereSingleLayer" in ds.coords:
        suffix = "_atmosphere_single_layer"

    for var in ds.data_vars:
        name = str(var)
        if name == "unknown" or any(hint in name for hint in DROP_SHORT_NAME_HINTS):
            to_drop.append(var)
            continue
        long_name = str(ds[var].attrs.get("long_name", name))
        if "deprecated" in long_name:
            to_drop.append(var)
            continue
        if "long_name" in ds[var].attrs:
            renames[var] = long_name_slug(long_name) + suffix

    ds = ds.drop_vars(to_drop)
    resolved = resolve_renames(ds, renames)
    # A refused rename means two messages on this level type share a long_name, so the
    # loser is genuinely ambiguous. Drop it rather than storing it under its raw GRIB
    # short name, which would change the variable set and make every later append skip.
    refused = [var for var in renames if var not in resolved]
    if refused:
        logger.debug(f"dropping {refused}: their long_name is already taken")
        ds = ds.drop_vars(refused)
    return ds.rename(resolved).drop_vars("heightAboveGround", errors="ignore")


def _is_incomplete_wind_or_height(ds: xr.Dataset) -> bool:
    """True for sub-datasets that carry nothing usable on their own.

    A wind component without its partner, or a bare geopotential height field, is dropped:
    both arrive again on the level types that are kept.
    """
    names = {str(v) for v in ds.data_vars}
    if "geopotential_height" in names and "isobaricInhPa" not in ds.coords:
        return True
    if names <= {"u_component_of_wind", "v_component_of_wind"}:
        return True
    return names == {"geopotential_height"}


def clean_grib_subset(ds: xr.Dataset, spec: GribMergeSpec) -> xr.Dataset | None:
    """Filter, rename and tidy one cfgrib sub-dataset. Returns None if it is dropped."""
    if any(coord in ds.coords for coord in spec.skip_coords):
        return None
    if any(dim in ds.dims for dim in spec.skip_dims):
        return None
    if "depthBelowLandLayer" in ds.coords:
        if soil_level_count(ds.coords["depthBelowLandLayer"]) < spec.min_soil_levels:
            return None

    ds = _rename_by_grib_long_name(ds)
    ds = ds.drop_vars([coord for coord in ds.coords if coord not in spec.coords_to_keep])

    if _is_incomplete_wind_or_height(ds):
        return None
    if any(skip(ds) for skip in spec.extra_skips):
        return None

    ds = ds.drop_vars(list(spec.drop_vars), errors="ignore")
    if not ds.data_vars:
        return None
    return ds


def merge_grib_files(paths: Sequence[str | os.PathLike], spec: GribMergeSpec) -> xr.Dataset:
    """Open GRIB files with cfgrib and merge their usable sub-datasets into one.

    Every path is opened with :func:`cfgrib.open_datasets`, which splits a GRIB file into
    one dataset per level type / step type combination. The sub-datasets from all the
    paths are pooled before filtering, so the surface, native and pressure-level files of
    a single forecast step merge as if they had arrived together.

    Raises:
        ValueError: If nothing survives the filter, which means the inputs were not the
            files this provider expects.
    """
    import cfgrib  # imported lazily: eccodes is a heavy, optional native dependency

    subsets: list[xr.Dataset] = []
    for path in paths:
        subsets.extend(cfgrib.open_datasets(str(path)))

    kept = [cleaned for ds in subsets if (cleaned := clean_grib_subset(ds, spec)) is not None]
    if not kept:
        raise ValueError(f"no usable GRIB messages in {[str(p) for p in paths]}")

    merged = xr.merge(kept, join="outer", compat="no_conflicts")
    return merged.drop_vars(list(spec.drop_coords_after_merge), errors="ignore")


def rename_present(ds: xr.Dataset, mapping: dict[str, str]) -> xr.Dataset:
    """Apply the subset of ``mapping`` whose keys actually exist on ``ds``.

    Archives gain and lose level types over the years, so a hard rename turns a partial
    day into a crash. Renaming only what is there keeps a backfill moving.
    """
    present = {old: new for old, new in mapping.items() if old in ds.variables or old in ds.dims}
    return ds.rename(present) if present else ds


def chunk_present(ds: xr.Dataset, chunks: dict[str, int]) -> xr.Dataset:
    """Chunk along the dimensions in ``chunks`` that exist on ``ds``."""
    return ds.chunk({dim: size for dim, size in chunks.items() if dim in ds.dims})


def init_time_download_dir(
    scratch_root: str | os.PathLike,
    name: str,
    it: pd.Timestamp,
    temp_dir: str | os.PathLike | None = None,
) -> pathlib.Path:
    """Directory to download one init time's files into.

    When no temporary directory is supplied the files land under the scratch directory,
    in a sub-directory named after the init time. That sub-directory matters: these
    archives name their files after the run hour but not the date
    (``hrrr.t06z.wrfsfcf00.ak.grib2``), so a flat directory would let the
    skip-if-present check hand yesterday's file back for today's run.
    """
    if temp_dir is not None:
        return pathlib.Path(temp_dir)
    return pathlib.Path(scratch_root) / name / it.strftime("%Y%m%dT%H%M%S")


def download_with_filesystem(
    fs,
    remote: str,
    dest: str | os.PathLike,
    retries: int = 3,
    backoff: float = 1.0,
) -> pathlib.Path | None:
    """Copy ``remote`` to ``dest`` through an fsspec filesystem, atomically.

    :func:`planetary_datasets.common.download.download_one` covers anything
    ``fsspec.open`` can reach with default options; this is its sibling for buckets that
    need a pre-configured filesystem, such as anonymous access to ``dmi-opendata``.

    The download lands on a ``.part`` file and is renamed only once complete, so an
    interrupted run cannot leave a truncated file that the skip-if-present check below
    would later mistake for good data.
    """
    dest = pathlib.Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file() and dest.stat().st_size > 0:
        logger.debug(f"skipping {dest.name}, already downloaded")
        return dest

    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, retries + 1):
        try:
            fs.get(remote, str(part))
            os.replace(part, dest)
            return dest
        except Exception as exc:  # noqa: BLE001 - any transport error is worth retrying
            part.unlink(missing_ok=True)
            if attempt == retries:
                logger.warning(f"failed to download {remote} after {retries} attempts: {exc}")
                return None
            time.sleep(backoff * (2 ** (attempt - 1)))
    return None


class GribNestProvider(BaseProvider):
    """Base for the regional GRIB nests published on public HTTPS-backed buckets.

    Subclasses declare which files make up one init time via :attr:`forecast_steps` and
    :attr:`products` and implement :meth:`file_url` and :meth:`combine`. Files are fetched
    step-major then product-minor, and :meth:`process` hands :meth:`combine` one merged
    dataset per forecast step in that same order.
    """

    #: Forecast hours retrieved for each init time.
    forecast_steps: tuple[int, ...] = (0,)
    #: File flavours retrieved for each forecast step, e.g. surface / native / pressure.
    products: tuple[str, ...] = ("",)
    #: Level-type filtering rules for this model. Frozen, so sharing one instance across
    #: every provider of a model is safe.
    merge_spec: GribMergeSpec = GribMergeSpec()
    #: Parallel downloads. These are large files; a handful at a time is plenty.
    download_workers: int = 3

    def file_url(self, it: pd.Timestamp, step: int, product: str) -> str:
        """URL of one GRIB file for init time ``it``, forecast hour ``step``."""
        raise NotImplementedError

    def combine(self, per_step: List[xr.Dataset], it: pd.Timestamp) -> xr.Dataset:
        """Assemble the per-step datasets into the dataset that gets written."""
        raise NotImplementedError

    def expected_urls(self, it: pd.Timestamp) -> List[str]:
        """Every file needed for init time ``it``, step-major then product-minor."""
        return [
            self.file_url(it, step, product)
            for step in self.forecast_steps
            for product in self.products
        ]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every file for ``it``, or nothing at all.

        A partial init time would be written with holes and then skipped forever by the
        already-present check, so an incomplete set is reported as "nothing to do".
        """
        target = init_time_download_dir(self.config.scratch_dir, self.name, it, temp_dir)
        urls = self.expected_urls(it)
        paths = download_many(urls, target, workers=self.download_workers)
        if len(paths) != len(urls):
            logger.warning(
                f"{self.name}: only {len(paths)}/{len(urls)} files available for {it}, skipping"
            )
            return []
        return [str(p) for p in paths]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge each forecast step's files, then combine the steps."""
        per_product = len(self.products)
        expected = per_product * len(self.forecast_steps)
        if len(input_files) != expected:
            raise ValueError(
                f"{self.name}: expected {expected} files for {it}, got {len(input_files)}"
            )

        per_step = [
            merge_grib_files(input_files[i * per_product : (i + 1) * per_product], self.merge_spec)
            for i in range(len(self.forecast_steps))
        ]
        return self.combine(per_step, it)
