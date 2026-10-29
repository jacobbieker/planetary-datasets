"""DWD ICON-Global, ICON-ART and ICON-ART-EU from the ``v1`` GRIB trees, into Icechunk.

DWD publishes each ICON run as one GRIB2 file per parameter, level and lead time: about
151,000 files (~450 GB) for an ICON-Global 00Z run once its model levels are included.
:mod:`~planetary_datasets.providers.icon_v1_catalog` knows where those files live — on
DWD's live server for about a day, and in the Source Cooperative mirror after that — and
which fields a complete run has. This provider turns one run into one ``init_time`` of an
Icechunk store, streaming it: nothing is downloaded to disk, and at most a few zarr
chunks are in memory at once, so a run that is hundreds of GB decoded is written under a
4 GB ceiling.

How a run becomes a dataset
---------------------------

Every variable is a lazy array from
:func:`~planetary_datasets.common.lazy_fields.lazy_field_dataset`,
in which dask blocks are exactly the store's zarr chunks: one init time, a few lead times,
one level and every cell. Computing a block GETs its fields concurrently with
:func:`~planetary_datasets.common.download.read_bytes` and decodes each straight into its
row of the block with :func:`~planetary_datasets.common.grib.decode_grib_field`. A field
that is absent from the source — the archive mirror often lacks whole parameters of a
run — reads back as NaN and is counted, rather than failing the run.

The layout comes from the frozen schema
(:func:`~planetary_datasets.providers.icon_v1_catalog.load_schema`),
never from what one run happens to hold, so every run of a model has the same variables,
level axes and step axis and appends to the same store:

* ``step`` is the schema's step axis (every lead time any variable has in any cycle), the
  same for every variable. A variable, or a cycle, without a lead time is NaN there: the
  06/18Z runs stop at 120 h, so their steps beyond are NaN.
* Variable names are the DWD parameter in lower case, as in the original
  ``icon_global.icechunk`` (``t_2m``, ``tot_prec``, ``t`` on pressure levels).
  Model-level fields get an ``_ml`` suffix (``t_ml``), since ``T`` is published on both.
* Pressure levels are ``isobaricInhPa`` in hPa, soil levels ``depth`` in metres (the top
  of the layer, as DWD's paths give it) and model levels ``model_level`` (full levels) or
  ``model_level_half`` (half levels, those reaching the model's lowest half level, such
  as ``HHL`` and ``W``).
* Where variables of one level type are published on different sets of levels, the most
  common set takes the plain dimension name and each other set gets its own dimension,
  so no variable is padded with levels it never has: ``model_level_39_120`` for ICON's
  cloud cover, ``model_level_77_120`` for ICON-ART's dust column, ``depth_t_so`` for
  ``T_SO``'s nine levels beside ``W_SO``'s eight. Model-level subsets are named by their
  first and last level, other subsets by the first parameter on them.
* The ICON-ART backscatter fields (``CEIL_BSC_DUST``, ``SAT_BSC_DUST``) are published per
  wavelength on levels 77–120 (31–74 for ART-EU); they are
  ``(init_time, step, wavelength, model_level_77_120, values)``, ``wavelength`` in nm.
* Parameters published only at step 0 (``HSURF``, ``HHL``, ...) have no ``step``.
* ``latitude``/``longitude`` (degrees, float32, along ``values``) come from the run's
  ``CLAT``/``CLON``, from an earlier run, or from the store, in that order of preference;
  a run is refused if none of them has the grid.

Precision
---------

Each variable is stored at the dtype its schema entry gives (``VariableSpec.dtype``):
float16 where sampled real fields fit comfortably inside float16's range and do not live
below its normal range, float32 otherwise and always for moisture, heights, pressure and
the ART dust and aerosol fields (see
:func:`~planetary_datasets.providers.icon_v1_catalog.classify_precision`). Float16
variables decode straight into float16 block buffers, so they cost half the memory and
half the store. A float16 field that overflows raises :class:`PrecisionOverflow` rather
than storing inf. The generation fingerprint does not look at dtypes, so an append whose
dtypes differ from the store's is refused; changing a variable's precision later means a
new store or a rewrite.

Memory and throughput
---------------------

:meth:`ICONV1Provider.write_to_icechunk` sizes the write with
:func:`~planetary_datasets.common.lazy_fields.estimate_peak_bytes`, picks how many blocks
may be in flight so the estimate fits under the ceiling (``min(4 GB, MEMORY_CEILING_GB)``
by default), and runs the dask compute and the commit inside
:func:`~planetary_datasets.common.lazy_fields.bounded_compute` and
:func:`~planetary_datasets.memory.memory_guard`. With four blocks in flight and eight
reads per block, 32 GETs are in flight against the archive; reads from DWD's live server
use fewer (:attr:`ICONV1Provider.live_fetch_workers`).

A store this provider creates is compressed with zstd at level
:data:`CREATE_CLEVEL` rather than the project's default of 9. On these fields level 9
compresses at about 17 MB/s per thread against about 220 MB/s at level 5, for a store
about 2% larger; at level 9 a single ICON-Global run would spend most of a day in the
compressor.

Stores
------

Each variant writes a family of generations
(:class:`~planetary_datasets.common.generations.GenerationalStoreMixin`). ICON-Global
shares its base prefix with the original ``bkr/icon/icon_global.icechunk``, whose schema
is different, so its first run here starts a dated generation
``icon_global_<DDMMYYYY>.icechunk``. ICON-ART and ICON-ART-EU start at their base prefix,
which no store occupies yet; one of another layout there would push them to a dated
generation in the same way.

ICON-ART and ICON-ART-EU changed grids with the 2026-10-06T00 run
(:data:`~planetary_datasets.providers.icon_v1_catalog.ERAS`). Each run is laid out by its
own era's schema and decoded against that era's grid; the two grids' cell coordinates
differ, so the fingerprint sends them to different generations. Backfilled oldest first,
the old-grid runs fill ``icon_global_art.icechunk`` / ``icon_eu_art.icechunk`` and the
2026-10-06T00 run starts ``..._06102026.icechunk``.

Incomplete runs
---------------

:meth:`ICONV1Provider.fetch` lists each of a run's ``.../r/<run>/s/`` directories before
anything is read (about 1,500 for an ICON-Global run, 16 at a time; about 30 s). An
archive run holding less than :attr:`ICONV1Provider.min_completeness` of its expected
fields (95% by default) is skipped, so it can be written once the archive mirror fills
in; the Dagster asset marks such a partition failed rather than green. A live run below
the threshold is still being published and raises :class:`IncompleteRun` to be retried. A
run above the threshold is written with NaN for its gaps, and only listed fields are
requested, so a gap costs no 404s.

Only generations with this provider's variables on its dimensions count when deciding
whether a run is already stored (:meth:`ICONV1Provider.generations`). A run held only by
the original ``icon_global.icechunk``, or by a generation a ``params`` subset wrote, is
still written to the full store.

Example::

    ICONV1Provider("eu_art").run_partition(pd.Timestamp("2026-10-11T00:00"))
"""

from __future__ import annotations

import dataclasses
import multiprocessing
import os
import pathlib
import pickle
import re
import threading
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, List, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from icechunk.xarray import to_icechunk
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import coords_match
from planetary_datasets.common.download import read_bytes
from planetary_datasets.common.generations import (
    GRID_COORDS,
    GenerationalStoreMixin,
    snap_to_stored_coords,
)
from planetary_datasets.common.grib import decode_grib_field, grib_field_info
from planetary_datasets.common.lazy_fields import (
    FieldVariable,
    Reader,
    bounded_compute,
    choose_step_chunk,
    estimate_peak_bytes,
    lazy_field_dataset,
    missing_report,
    read_failures,
)
from planetary_datasets.common.store import (
    COMMIT_ATTEMPTS,
    RACE_ERRORS,
    STORE_READ_ERRORS,
    StoreReadError,
    build_encoding,
    has_committed_data,
    quiet_timedelta_decoding,
)
from planetary_datasets.config import Config
from planetary_datasets.memory import (
    BYTES_PER_GB,
    MemoryLimitExceeded,
    memory_budget_gb,
    memory_guard,
    process_tree_rss_gb,
)
from planetary_datasets.providers._timestamps import to_naive_utc
from planetary_datasets.providers.icon import IncompleteRun
from planetary_datasets.providers.icon_v1_catalog import (
    ARCHIVE_ROOT,
    ERAS,
    LIVE_ROOT,
    MODELS,
    FieldKey,
    ICONEra,
    ICONSchema,
    Source,
    VariableSpec,
    choose_source,
    clean_names,
    current_era,
    era_for,
    field_path,
    field_uri,
    get_lister,
    load_era_schema,
    storage_options,
)

__all__ = [
    "ARCHIVE_STARTS",
    "CREATE_CLEVEL",
    "DEFAULT_CEILING_GB",
    "STORE_PREFIXES",
    "VARIANT_MODELS",
    "ICONV1Provider",
    "IncompleteRun",
    "MissingGridError",
    "ChildDied",
    "MULTIPART_PART_BYTES",
    "MULTIPART_UPLOAD_MIN_BYTES",
    "PartitionResult",
    "RunCompleteness",
    "run_in_child",
    "run_partition_child",
    "PrecisionOverflow",
    "plan_variables",
]

#: Provider variant -> ICON model directory in the catalog.
VARIANT_MODELS: dict[str, str] = {
    "global": "icon",
    "global_art": "icon-art",
    "eu_art": "icon-art-eu",
}

#: Provider variant -> base store prefix of its generations.
STORE_PREFIXES: dict[str, str] = {
    "global": "bkr/icon/icon_global.icechunk",
    "global_art": "bkr/icon/icon_global_art.icechunk",
    "eu_art": "bkr/icon/icon_eu_art.icechunk",
}

#: Provider variant -> first run in the Source Cooperative archive.
ARCHIVE_STARTS: dict[str, pd.Timestamp] = {
    variant: MODELS[model].archive_start for variant, model in VARIANT_MODELS.items()
}

#: Memory ceiling a write is held to unless the constructor says otherwise, in GB.
DEFAULT_CEILING_GB = 4.0

#: Resident memory kept free on top of the write estimate, for allocator slack. Decoding
#: fields in a loop grows RSS by about 140 MiB before it levels off, and a 10-parameter
#: ICON-ART-EU write peaked about 0.6 GB above estimate_peak_bytes plus the resident set.
HEADROOM_GB = 0.5

#: Bytes one in-flight read holds per grid cell: the compressed GRIB (up to ~3 bytes a
#: cell), eccodes' copy of it and its unpacking scratch, and the decoded float32 field
#: before it is copied into the block. The decoder measured ~20 MiB for a 2.9M-cell
#: global field, so 10 bytes a cell is generous.
READER_BYTES_PER_CELL = 10

#: zstd level of stores this provider creates; see the module docstring.
CREATE_CLEVEL = 5

#: Chunk objects larger than this are uploaded as a multipart upload, its parts in parallel.
#:
#: icechunk's default is 100 MB, and an ICON chunk (13 fields, 2-80 MB compressed) is
#: smaller, so each went up as one PUT on one TCP stream - one stream per block in flight.
#: A stream's throughput is its send buffer over the round trip: aero's 4 MiB buffer
#: across the 143 ms to us-west-2 allows at most ~29 MB/s, and the ICON-Global backfill
#: sent 24 MB/s while the host's uplink carried 119 MB/s over 8 streams. Writing a real
#: ICON-Global subset through this provider to a fake S3 capped at 25 MB/s a stream:
#: 9.4 fields/s with single PUTs, 10.7 with 24 MiB / 12 MiB parts, 13.9 with these.
MULTIPART_UPLOAD_MIN_BYTES = 16 * 2**20

#: Size of each part of a multipart upload. Half the threshold, so every multipart upload
#: has at least two parts: one part would only add two round trips to a single PUT.
MULTIPART_PART_BYTES = 8 * 2**20


def repository_config() -> icechunk.RepositoryConfig:
    """Repository settings for writing these stores: chunks uploaded in parallel parts.

    Only the storage settings are given, so everything else comes from the repository's
    own saved configuration. They apply to the handle :meth:`ICONV1Provider.get_icechunk_repo`
    opens, which is the one every write goes through; a store is usually created first,
    by the generation lookup, without them, so they are not saved with it.
    """
    config = icechunk.RepositoryConfig()
    config.storage = icechunk.StorageSettings(
        minimum_size_for_multipart_upload=MULTIPART_UPLOAD_MIN_BYTES,
        concurrency=icechunk.StorageConcurrencySettings(
            ideal_concurrent_request_size=MULTIPART_PART_BYTES
        ),
    )
    return config


#: Target decoded size of one chunk, in MiB.
CHUNK_TARGET_MB = 150.0

#: How many variables to name in the missing-field summary of a run.
REPORT_TOP_N = 10

#: Fraction of a run's expected fields that must be listed at the source for it to be
#: written. Below it an archive run is skipped, not failed: the archive mirror fills in
#: over time, and a run written with large holes cannot be rewritten without a rebuild. A
#: live run below it is retried instead (:class:`IncompleteRun`): DWD is still publishing.
DEFAULT_MIN_COMPLETENESS = 0.95

#: Concurrent directory listings when checking what a run holds.
LISTING_WORKERS = 16


#: Dimension name for each level type with a base of its own. Level types not listed get
#: ``level_<type>``.
_PRESSURE, _SOIL, _MODEL = 100, 106, 150
_LEVEL_DIMS = {_PRESSURE: "isobaricInhPa", _SOIL: "depth"}
_MODEL_FULL, _MODEL_HALF = "model_level", "model_level_half"
_WAVELENGTH = "wavelength"

#: File name of one lead time, the last component of every field path.
_STEP_FILE = "PT{step:03d}H00M.grib2"


class MissingGridError(RuntimeError):
    """No cell coordinates for a run: its CLAT/CLON are missing and none are stored."""


class PrecisionOverflow(ValueError):
    """A field of a float16 variable does not fit in float16.

    Never stored as inf: the schema has the variable at the wrong precision and must be
    reclassified (``python -m planetary_datasets.providers.icon_v1_catalog precision``).
    """


# ---------------------------------------------------------------------------------------
# Layout: which variables a schema becomes, on which dimensions
# ---------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class VariablePlan:
    """How one dataset variable is built from the schema.

    Attributes:
        name: Variable name in the store.
        param: DWD parameter.
        level_type: ``typeOfFirstFixedSurface`` or None for single-level fields.
        levels: Levels as they appear in paths; empty if single-level.
        level_dim: Dimension the levels lie along, or None.
        wavelengths: Wavelengths in nm for the ``wvl1/`` fields; empty otherwise.
        invariant: Published at step 0 only, so stored without a ``step`` dimension.
        specs: The schema variables behind this one, one per wavelength.
        dtype: Storage dtype, ``float16`` or ``float32``, from the schema.
    """

    name: str
    param: str
    level_type: int | None
    levels: tuple[str, ...]
    level_dim: str | None
    wavelengths: tuple[int, ...]
    invariant: bool
    specs: tuple[VariableSpec, ...]
    dtype: str = "float32"

    @property
    def dims(self) -> tuple[str, ...]:
        """Dimensions, ending in ``values``."""
        dims = ["init_time"]
        if not self.invariant:
            dims.append("step")
        if self.wavelengths:
            dims.append(_WAVELENGTH)
        if self.level_dim is not None:
            dims.append(self.level_dim)
        return (*dims, "values")


@dataclasses.dataclass(frozen=True)
class Layout:
    """Every variable a schema becomes, and the coordinates of their level dimensions."""

    variables: tuple[VariablePlan, ...]
    level_coords: dict[str, np.ndarray]

    def select(self, params: Iterable[str] | None) -> "Layout":
        """The variables of ``params`` only, and only the dimensions they use."""
        if params is None:
            return self
        wanted = set(params)
        variables = tuple(v for v in self.variables if v.param in wanted)
        used = {d for v in variables for d in v.dims}
        coords = {d: c for d, c in self.level_coords.items() if d in used}
        return Layout(variables, coords)


def _level_value(level_type: int, level: str) -> float | int:
    """A level's coordinate value: hPa for pressure, metres for soil, numbers for models."""
    if level_type == _PRESSURE:
        return float(level) / 100.0
    if level_type == _MODEL:
        return int(float(level))
    return float(level)


def _subset_suffix(base: str, levels: tuple[str, ...], params: Sequence[str]) -> str:
    """Dimension name for a level set that is not the most common of its kind."""
    if base in (_MODEL_FULL, _MODEL_HALF):
        return f"{base}_{int(float(levels[0]))}_{int(float(levels[-1]))}"
    return f"{base}_{sorted(params)[0].lower()}"


def _name_dims(families: dict[str, dict[tuple, list[str]]]) -> dict[tuple[str, tuple], str]:
    """Name each (dimension kind, value set): the commonest set takes the plain name.

    Args:
        families: Dimension base name -> value set -> parameters on it.

    Returns:
        ``(base, value set) -> dimension name``.
    """
    names: dict[tuple[str, tuple], str] = {}
    taken: set[str] = set()
    for base, sets in sorted(families.items()):
        # Most parameters first, then most values; ties broken on the values themselves.
        ranked = sorted(sets.items(), key=lambda item: (-len(set(item[1])), -len(item[0]), item[0]))
        for rank, (values, params) in enumerate(ranked):
            name = base if rank == 0 else _subset_suffix(base, values, params)
            candidate, n = name, 2
            while candidate in taken:
                candidate, n = f"{name}_{n}", n + 1
            taken.add(candidate)
            names[(base, values)] = candidate
    return names


def plan_variables(schema: ICONSchema) -> Layout:
    """Lay a schema out as dataset variables. See the module docstring for the rules.

    The layout depends on the whole schema and nothing else, so it is the same for every
    run of a model and for every subset of its parameters.

    Args:
        schema: A model's frozen schema.

    Returns:
        The variables and the coordinate values of every level dimension they use.

    Raises:
        ValueError: If two variables would get the same name, or the wavelengths of one
            parameter disagree on their levels.
    """
    model_levels = [
        int(float(lv)) for v in schema.variables if v.level_type == _MODEL for lv in v.levels
    ]
    bottom_half_level = max(model_levels) if model_levels else None
    invariant = set(schema.invariant_params)

    # Group the wavelengths of one parameter into one variable.
    grouped: dict[tuple[str, int | None], list[VariableSpec]] = {}
    for spec in schema.variables:
        grouped.setdefault((spec.param, spec.level_type), []).append(spec)

    def level_base(level_type: int, levels: tuple[str, ...]) -> str:
        if level_type == _MODEL:
            last = int(float(levels[-1]))
            return _MODEL_HALF if last == bottom_half_level else _MODEL_FULL
        return _LEVEL_DIMS.get(level_type, f"level_{level_type}")

    families: dict[str, dict[tuple, list[str]]] = {}
    base_level_type: dict[str, int] = {}
    for (param, level_type), specs in grouped.items():
        levels = specs[0].levels
        if any(s.levels != levels for s in specs):
            raise ValueError(f"{param}@{level_type}: wavelengths are on different levels")
        if level_type is not None:
            base = level_base(level_type, levels)
            base_level_type[base] = level_type
            families.setdefault(base, {}).setdefault(levels, []).append(param)
        wavelengths = tuple(sorted(s.wavelength for s in specs if s.wavelength is not None))
        if wavelengths:
            families.setdefault(_WAVELENGTH, {}).setdefault(wavelengths, []).append(param)
    dim_names = _name_dims(families)

    level_coords: dict[str, np.ndarray] = {}
    for (base, values), name in dim_names.items():
        if base == _WAVELENGTH:
            level_coords[name] = np.asarray(values, dtype="int64")
        else:
            level_type = base_level_type[base]
            level_coords[name] = np.asarray([_level_value(level_type, lv) for lv in values])

    plans: list[VariablePlan] = []
    names: dict[str, tuple[str, int | None]] = {}
    for (param, level_type), specs in sorted(
        grouped.items(), key=lambda i: (i[0][0], i[0][1] or -1)
    ):
        specs = sorted(specs, key=lambda s: s.wavelength or -1)
        levels = specs[0].levels
        wavelengths = tuple(s.wavelength for s in specs if s.wavelength is not None)
        if wavelengths and len(wavelengths) != len(specs):
            raise ValueError(f"{param}@{level_type}: mixes wavelength and plain fields")
        dtypes = {s.dtype for s in specs}
        if len(dtypes) != 1:
            raise ValueError(f"{param}@{level_type}: wavelengths disagree on dtype {dtypes}")
        name = param.lower() + ("_ml" if level_type == _MODEL else "")
        if name in names:
            raise ValueError(f"{param}@{level_type} and {names[name]} would both be named {name!r}")
        names[name] = (param, level_type)
        level_dim = None
        if level_type is not None:
            level_dim = dim_names[(level_base(level_type, levels), levels)]
        if wavelengths:
            wavelength_dim = dim_names[(_WAVELENGTH, wavelengths)]
            if wavelength_dim != _WAVELENGTH:
                raise ValueError(f"{param}: a second wavelength set is not supported")
        plans.append(
            VariablePlan(
                name=name,
                param=param,
                level_type=level_type,
                levels=levels,
                level_dim=level_dim,
                wavelengths=wavelengths,
                invariant=param in invariant,
                specs=tuple(specs),
                dtype=dtypes.pop(),
            )
        )
    return Layout(tuple(plans), level_coords)


# ---------------------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------------------


class _Progress:
    """Counts the fields of a run as they are read, and logs the rate now and then."""

    def __init__(self, what: str, expected: int, interval: float = 60.0) -> None:
        self.what = what
        self.expected = expected
        self.interval = interval
        self.read = 0
        self.missing = 0
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self._last = self._started

    def record(self, found: bool) -> None:
        """Count one field; log progress if :attr:`interval` has passed since the last log."""
        with self._lock:
            if found:
                self.read += 1
            else:
                self.missing += 1
            now = time.monotonic()
            if now - self._last < self.interval:
                return
            self._last = now
            done = self.read + self.missing
            elapsed = now - self._started
        rate = done / elapsed if elapsed > 0 else 0.0
        remaining = (self.expected - done) / rate / 60 if rate > 0 else float("nan")
        logger.info(
            f"{self.what}: {done:,}/{self.expected:,} fields "
            f"({100 * done / max(1, self.expected):.1f}%), {self.missing:,} missing, "
            f"{rate:.0f} fields/s, ~{remaining:.0f} min to go"
        )


def _param_of(uri: str) -> str:
    """The DWD parameter a field URI belongs to."""
    return uri.split("/p/", 1)[1].split("/", 1)[0]


def _source_of(uris: Sequence[str]) -> Source:
    """Whether ``uris`` point into the archive or at the live server."""
    first = uris[0]
    if first.startswith(ARCHIVE_ROOT):
        return "archive"
    if first.startswith(LIVE_ROOT):
        return "live"
    raise ValueError(f"{first} is in neither {ARCHIVE_ROOT} nor {LIVE_ROOT}")


@dataclasses.dataclass(frozen=True)
class RunCompleteness:
    """How much of a run a source holds, from listing its directories.

    Attributes:
        init_time: The run.
        source: Where it was listed.
        expected: Data fields the schema expects of the run (of the selected parameters).
        present: Of those, how many the source lists.
        missing_by_param: Expected but unlisted fields, per DWD parameter.
        seconds: How long the listing took.
    """

    init_time: pd.Timestamp
    source: Source
    expected: int
    present: int
    missing_by_param: dict[str, int]
    seconds: float

    @property
    def fraction(self) -> float:
        """Present over expected; 1.0 for a run that expects nothing."""
        return self.present / self.expected if self.expected else 1.0

    def most_missing(self, n: int = REPORT_TOP_N) -> str:
        """The parameters missing the most fields, for a log line."""
        top = Counter(self.missing_by_param).most_common(n)
        return ", ".join(f"{param} {count:,}" for param, count in top) or "none"


@dataclasses.dataclass
class _RunState:
    """What :meth:`ICONV1Provider.process` learnt about a run, for the write."""

    source: Source
    expected: int
    progress: _Progress
    listed: RunCompleteness | None = None


@dataclasses.dataclass(frozen=True)
class _EraPlan:
    """One grid era of a model: its schema, and the layout of the selected parameters."""

    era: ICONEra
    schema: ICONSchema
    layout: Layout

    @property
    def n_cells(self) -> int:
        return self.schema.n_cells

    @property
    def step_chunk(self) -> int:
        """Lead times per chunk. Fixed per era: every append must match the store."""
        return choose_step_chunk(self.n_cells, 4, len(self.schema.step_axis), CHUNK_TARGET_MB)


# ---------------------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------------------


class ICONV1Provider(GenerationalStoreMixin, BaseProvider):
    """One ICON model from DWD's ``v1`` GRIB trees, streamed into an Icechunk store.

    Args:
        variant: ``"global"`` (ICON-Global), ``"global_art"`` (ICON-ART) or ``"eu_art"``
            (ICON-ART-EU).
        config: Process configuration; defaults to the process-wide one.
        params: Only these DWD parameters, e.g. ``["T_2M", "TOT_PREC"]``. A subset has a
            different schema from the full model, so it is written to its own generation;
            meant for tests and one-off extracts, not for the production stores.
        ceiling_gb: Memory ceiling for the write, in GB. Defaults to
            :data:`DEFAULT_CEILING_GB`; never more than the configured budget
            (``MEMORY_CEILING_GB``, or a fraction of available memory).
        fetch_workers: Concurrent reads per block from the archive.
        live_fetch_workers: Concurrent reads per block from DWD's live server.
        max_workers: Upper bound on blocks in flight; fewer if the ceiling requires it.
        min_completeness: Fraction of a run's expected fields the source must list for the
            run to be written; below it the run is skipped. See
            :data:`DEFAULT_MIN_COMPLETENESS`.

    Each run is laid out by the schema of its grid era
    (:func:`~planetary_datasets.providers.icon_v1_catalog.era_for`). Runs of different
    grids have different ``latitude``/``longitude`` axes, so the generation fingerprint
    sends them to different stores.
    """

    append_dim = "init_time"

    #: Variables probed for a live run's last step: the first, middle and last selected.
    probe_count: int = 3

    def __init__(
        self,
        variant: str = "global",
        config: Config | None = None,
        *,
        params: Sequence[str] | None = None,
        ceiling_gb: float | None = None,
        fetch_workers: int = 8,
        live_fetch_workers: int = 4,
        max_workers: int = 4,
        min_completeness: float = DEFAULT_MIN_COMPLETENESS,
    ) -> None:
        """Build a provider for one variant; see the class docstring for the arguments."""
        super().__init__(config)
        if variant not in VARIANT_MODELS:
            raise ValueError(
                f"unknown ICON v1 variant {variant!r}; expected one of {sorted(VARIANT_MODELS)}"
            )
        if min(fetch_workers, live_fetch_workers, max_workers) < 1:
            raise ValueError("fetch_workers, live_fetch_workers and max_workers must be >= 1")
        if not 0.0 <= min_completeness <= 1.0:
            raise ValueError(f"min_completeness must be in [0, 1], got {min_completeness}")
        self.variant = variant
        self.model = VARIANT_MODELS[variant]
        self.name = f"icon_v1_{variant}"
        self.base_store_prefix = STORE_PREFIXES[variant]
        schemas = {era: load_era_schema(self.model, era) for era in ERAS[self.model]}
        self.params: tuple[str, ...] | None = None
        if params is not None:
            known = set()
            for schema in schemas.values():
                known |= {v.param for v in schema.variables} | set(schema.coordinate_params)
            unknown = sorted(set(params) - known)
            if unknown:
                raise ValueError(f"{self.model} has no parameters {unknown}")
            coordinates = {p for schema in schemas.values() for p in schema.coordinate_params}
            self.params = tuple(sorted(set(params) - coordinates))
            if not self.params:
                raise ValueError("params selects no data variables")
        self._plans: dict[ICONEra, _EraPlan] = {
            era: _EraPlan(era, schema, plan_variables(schema).select(self.params))
            for era, schema in schemas.items()
        }
        for era, plan in self._plans.items():
            if not plan.layout.variables:
                raise ValueError(
                    f"params {list(self.params or ())} select no variables of the {self.model} "
                    f"era on grid {era.grid_number}"
                )
        self.min_completeness = min_completeness
        self._ceiling_gb = ceiling_gb
        self.fetch_workers = fetch_workers
        self.live_fetch_workers = live_fetch_workers
        self.max_workers = max_workers
        #: Cell coordinates per grid number, as decoded or adopted from a store.
        self._grids: dict[int, tuple[np.ndarray, np.ndarray]] = {}
        self._runs: dict[pd.Timestamp, _RunState] = {}
        #: What :meth:`fetch` found each run to hold, for reports such as the backfill's.
        self.completeness: dict[pd.Timestamp, RunCompleteness] = {}

    # -- configuration ------------------------------------------------------------------

    def plan_for(self, it: pd.Timestamp | None = None) -> _EraPlan:
        """The era plan a run follows; the current era's for None."""
        era = current_era(self.model) if it is None else era_for(self.model, it)
        return self._plans[era]

    @property
    def schema(self) -> ICONSchema:
        """The current era's schema."""
        return self.plan_for().schema

    @property
    def layout(self) -> Layout:
        """The current era's layout of the selected parameters."""
        return self.plan_for().layout

    @property
    def n_cells(self) -> int:
        """Cells in one field of the current era."""
        return self.plan_for().n_cells

    @property
    def step_chunk(self) -> int:
        """Lead times per chunk in the current era."""
        return self.plan_for().step_chunk

    @property
    def alignment_coords(self) -> tuple[str, ...]:  # type: ignore[override]
        """Coordinates an append must match exactly: the grid, steps and every level axis."""
        dims = sorted({d for plan in self._plans.values() for d in plan.layout.level_coords})
        return ("latitude", "longitude", "step", *dims)

    @property
    def ceiling_gb(self) -> float:
        """Absolute RSS the write is held to, in GB."""
        configured = self.config.memory_ceiling_gb
        budget = configured if configured is not None else memory_budget_gb()
        wanted = self._ceiling_gb if self._ceiling_gb is not None else DEFAULT_CEILING_GB
        return min(wanted, budget)

    # -- fetch --------------------------------------------------------------------------

    def _selected(self, keys: Iterable[FieldKey]) -> list[FieldKey]:
        if self.params is None:
            return list(keys)
        wanted = set(self.params)
        return [k for k in keys if k.param in wanted]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return the URIs of the fields the source holds for the run at ``it``.

        The source is chosen by :func:`~planetary_datasets.providers.icon_v1_catalog.choose_source`,
        and each of the run's directories is listed (:meth:`list_run`) rather than each file
        requested, so absent fields are known before anything is read and cost no GETs. An
        archive run holding less than :attr:`min_completeness` of its expected fields is
        skipped: an empty list, logged as a warning, and recorded in :attr:`completeness`.

        Raises:
            IncompleteRun: The run is read from the live server and is not fully there:
                the last files of the probed variables are absent, or it is below the
                threshold. Whatever its age - a scheduled partition is always six hours
                old - since DWD only ever adds to a run, and the live server keeps it for a
                day, by when the archive takes over.
            ListingError: A source could not be listed.
        """
        it = to_naive_utc(it)
        source = self.source_for(it)
        if source is None:
            logger.info(f"{self.name}: run {it} is in neither the archive nor the live server")
            return []
        present, listed = self.list_run(it, source)
        if source == "live":
            self._require_published(it, present)
        if listed.fraction < self.min_completeness:
            if source == "live":
                raise IncompleteRun(
                    f"{self.name}: live run {it} lists {listed.fraction:.1%} of its fields; "
                    f"still being published, retry later. Most missing: {listed.most_missing()}"
                )
            logger.warning(
                f"{self.name}: skipping run {it}: the {source} lists {listed.present:,} of "
                f"{listed.expected:,} expected fields ({listed.fraction:.1%}), below "
                f"{self.min_completeness:.0%}. Most missing: {listed.most_missing()}"
            )
            return []
        logger.info(
            f"{self.name}: run {it} from {source}, {listed.present:,} of {listed.expected:,} "
            f"fields listed ({listed.fraction:.1%}) in {listed.seconds:.1f}s"
        )
        return self._uris(present, it, source)

    def _uris(self, keys: Sequence[FieldKey], it: pd.Timestamp, source: Source) -> list[str]:
        """``field_uri`` of each key, building each directory's URI once."""
        directories: dict[tuple, str] = {}
        step_zero = _STEP_FILE.format(step=0)
        uris = []
        for key in keys:
            where = (key.param, key.level_type, key.level, key.wavelength)
            if where not in directories:
                zero = dataclasses.replace(key, step=0)
                directories[where] = field_uri(self.model, zero, it, source).removesuffix(step_zero)
            uris.append(directories[where] + _STEP_FILE.format(step=key.step))
        return uris

    def _paths(self, keys: Sequence[FieldKey], it: pd.Timestamp) -> dict[FieldKey, str]:
        """``field_path`` of each key, building each directory's path once."""
        directories: dict[tuple, str] = {}
        step_zero = _STEP_FILE.format(step=0)
        paths = {}
        for key in keys:
            where = (key.param, key.level_type, key.level, key.wavelength)
            if where not in directories:
                zero = dataclasses.replace(key, step=0)
                directories[where] = field_path(self.model, zero, it).removesuffix(step_zero)
            paths[key] = directories[where] + _STEP_FILE.format(step=key.step)
        return paths

    def source_for(self, it: pd.Timestamp) -> Source | None:
        """Where the run at ``it`` would be read from; see ``choose_source``."""
        return choose_source(self.model, to_naive_utc(it))

    def list_run(self, it: pd.Timestamp, source: Source) -> tuple[list[FieldKey], RunCompleteness]:
        """List which of a run's expected fields, and of its CLAT/CLON, ``source`` holds.

        One listing per ``.../r/<run>/s/`` directory, up to :data:`LISTING_WORKERS` at
        once. The result is also kept in :attr:`completeness`.

        Returns:
            The present fields, data and coordinates, in schema order; and the run's
            completeness over its data fields.

        Raises:
            ListingError: A directory could not be listed.
        """
        it = to_naive_utc(it)
        plan = self.plan_for(it)
        data_keys = self._selected(plan.schema.expected_fields(it))
        keys = data_keys + plan.schema.coordinate_fields()
        paths = self._paths(keys, it)
        directories = sorted({path.rpartition("/")[0] for path in paths.values()})
        lister = get_lister(self.model, source)

        def names(directory: str) -> set[str]:
            try:
                return {f"{directory}/{name}" for name in clean_names(lister(directory))}
            except FileNotFoundError:
                return set()

        started = time.monotonic()
        listed: set[str] = set()
        with ThreadPoolExecutor(max_workers=min(LISTING_WORKERS, len(directories))) as pool:
            for found in pool.map(names, directories):
                listed |= found
        seconds = time.monotonic() - started
        present = [key for key in keys if paths[key] in listed]
        missing = Counter(key.param for key in data_keys if paths[key] not in listed)
        completeness = RunCompleteness(
            init_time=it,
            source=source,
            expected=len(data_keys),
            present=len(data_keys) - sum(missing.values()),
            missing_by_param=dict(missing),
            seconds=seconds,
        )
        self.completeness[it] = completeness
        logger.debug(
            f"{self.name}: listed {len(directories)} directories of {it} in {seconds:.1f}s"
        )
        return present, completeness

    def _probe_fields(self, it: pd.Timestamp) -> list[FieldKey]:
        """The last expected file of the first, middle and last selected variables."""
        hour = it.hour
        plan = self.plan_for(it)
        candidates = []
        for variable in plan.layout.variables:
            for spec in variable.specs:
                steps = plan.schema.steps_for_cycle(
                    spec.param, spec.level_type, hour, spec.wavelength
                )
                if steps:
                    level = spec.levels[-1] if spec.levels else None
                    candidates.append(
                        FieldKey(spec.param, spec.level_type, level, max(steps), spec.wavelength)
                    )
        if len(candidates) <= self.probe_count:
            return candidates
        picks = np.linspace(0, len(candidates) - 1, self.probe_count).round().astype(int)
        return [candidates[i] for i in sorted(set(picks.tolist()))]

    def _require_published(self, it: pd.Timestamp, present: Iterable[FieldKey]) -> None:
        """Raise :class:`IncompleteRun` if a live run's last files are not there yet.

        Asked of every live run, whatever its age: a scheduled partition runs once its
        six-hour window has closed, by when DWD has normally finished, so the age alone
        would never trigger the check in exactly the case it is for, a late run. Every
        field still missing when the run is written would be stored as NaN for good.

        Args:
            it: The run.
            present: The fields :meth:`list_run` found.
        """
        age = pd.Timestamp.now("UTC").tz_localize(None) - it
        listed = set(present)
        absent = [
            field_path(self.model, key, it) for key in self._probe_fields(it) if key not in listed
        ]
        if absent:
            raise IncompleteRun(
                f"{self.name}: live run {it} is {age} old and not fully published; "
                f"{len(absent)} probe file(s) absent, e.g. {absent[0]}. Retry later."
            )

    # -- process ------------------------------------------------------------------------

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Build the run's lazy dataset. Only the cell coordinates are read here.

        Every schema variable of the selected parameters is present whatever
        ``input_files`` holds: a field not in ``input_files`` is NaN, as is one that turns
        out to be missing when the write reads it.

        Raises:
            MissingGridError: The run has no CLAT/CLON and no earlier ones are known.
        """
        it = to_naive_utc(it)
        source = _source_of(input_files)
        available = frozenset(input_files)
        era_plan = self.plan_for(it)
        latitude, longitude = self._grid_coords(it, source, available)

        hour = it.hour
        step_axis = era_plan.schema.step_axis
        n_steps = len(step_axis)
        n_cells = era_plan.n_cells
        variables = []
        for plan in era_plan.layout.variables:
            shape: list[int] = [1]
            chunks: list[int] = [1]
            if not plan.invariant:
                shape.append(n_steps)
                chunks.append(era_plan.step_chunk)
            if plan.wavelengths:
                shape.append(len(plan.wavelengths))
                chunks.append(1)
            if plan.level_dim is not None:
                shape.append(len(plan.levels))
                chunks.append(1)
            shape.append(n_cells)
            chunks.append(n_cells)
            attrs = {"dwd_param": plan.param}
            if plan.level_type is not None:
                attrs["dwd_level_type"] = plan.level_type
            variables.append(
                FieldVariable(
                    name=plan.name,
                    dims=plan.dims,
                    shape=tuple(shape),
                    chunks=tuple(chunks),
                    uri_for=self._uri_for(plan, it, hour, source, available),
                    dtype=np.dtype(plan.dtype),
                    attrs=attrs,
                )
            )

        coords: dict = {
            "init_time": pd.DatetimeIndex([it], name="init_time"),
            "step": ("step", pd.to_timedelta(np.asarray(step_axis), unit="h")),
            "latitude": ("values", latitude, {"units": "degrees_north"}),
            "longitude": ("values", longitude, {"units": "degrees_east"}),
        }
        for dim, values in era_plan.layout.level_coords.items():
            coords[dim] = (dim, values, _level_attrs(dim))

        expected = sum(
            1 for uri in input_files if _param_of(uri) not in era_plan.schema.coordinate_params
        )
        progress = _Progress(f"{self.name} {it:%Y-%m-%dT%H}", expected)
        workers = self.live_fetch_workers if source == "live" else self.fetch_workers
        ds = lazy_field_dataset(
            variables,
            coords,
            self._reader(source, progress, era_plan.era.grid_number),
            fetch_workers=workers,
            attrs={
                "title": f"DWD {self.model} forecasts",
                "source": f"{ARCHIVE_ROOT}/{self.model} and {LIVE_ROOT}/{self.model}",
                "icon_v1_model": self.model,
                "icon_v1_schema_generated_at": era_plan.schema.generated_at or "unknown",
                "icon_v1_grid_number": era_plan.era.grid_number,
            },
            token=f"{self.model}:{it.isoformat()}:{source}",
        )
        listed = self.completeness.get(it)
        self._runs[it] = _RunState(source, expected, progress, listed)
        return ds

    def _uri_for(
        self,
        plan: VariablePlan,
        it: pd.Timestamp,
        hour: int,
        source: Source,
        available: frozenset[str],
    ) -> Callable[[tuple[int, ...]], str | None]:
        """Map a block index of ``plan`` to its field's URI, or None if it has none."""
        schema = self.plan_for(it).schema
        step_axis = schema.step_axis
        by_wavelength = {
            spec.wavelength: frozenset(
                schema.steps_for_cycle(spec.param, spec.level_type, hour, spec.wavelength)
            )
            for spec in plan.specs
        }
        model, param, level_type = self.model, plan.param, plan.level_type
        levels, wavelengths, invariant = plan.levels, plan.wavelengths, plan.invariant
        # field_uri costs ~35 us, mostly re-validating the init time, and is called for
        # every field of a block. Only the step changes within a directory, so each
        # directory's URI is built once and the file name appended.
        step_zero = _STEP_FILE.format(step=0)
        directories: dict[tuple[str | None, int | None], str] = {}

        def directory(level: str | None, wavelength: int | None) -> str:
            found = directories.get((level, wavelength))
            if found is None:
                key = FieldKey(param, level_type, level, 0, wavelength)
                found = field_uri(model, key, it, source).removesuffix(step_zero)
                directories[(level, wavelength)] = found
            return found

        def uri_for(index: tuple[int, ...]) -> str | None:
            position = 1
            step = 0
            if not invariant:
                step = step_axis[index[position]]
                position += 1
            wavelength = None
            if wavelengths:
                wavelength = wavelengths[index[position]]
                position += 1
            level = levels[index[position]] if levels else None
            if step not in by_wavelength[wavelength]:
                return None
            uri = directory(level, wavelength) + _STEP_FILE.format(step=step)
            return uri if uri in available else None

        return uri_for

    def _reader(self, source: Source, progress: _Progress, grid_number: int) -> Reader:
        """A reader that GETs a field and decodes it into its row.

        The first field of each parameter also has its header checked against the run's
        era's grid; every field's length is checked by the decoder.
        """
        options = storage_options(source)
        checked: set[str] = set()
        lock = threading.Lock()

        def read(uri: str, row: np.ndarray) -> bool:
            data = read_bytes(uri, storage_options=options)
            if data is None:
                progress.record(False)
                return False
            param = _param_of(uri)
            with lock:
                first = param not in checked
                checked.add(param)
            if first:
                info = grib_field_info(data)
                if info.grid_number is not None and info.grid_number != grid_number:
                    raise ValueError(
                        f"{uri} is on grid {info.grid_number}, not the era's {grid_number}"
                    )
            # Decoding casts to the row's dtype; in a float16 row a value beyond float16's
            # range becomes inf, which no GRIB field holds.
            with np.errstate(over="ignore"):
                decode_grib_field(data, out=row)
            if row.dtype == np.float16 and np.isinf(row).any():
                peak = float(np.nanmax(np.abs(decode_grib_field(data, dtype=np.float32))))
                raise PrecisionOverflow(
                    f"{param} is stored as float16 but {uri} reaches {peak:g}, beyond "
                    "float16's 65504; reclassify it in the schema (icon_v1_catalog precision) "
                    "and start a new store for it"
                )
            progress.record(True)
            return True

        return read

    # -- the cell grid ------------------------------------------------------------------

    def _grid_coords(
        self, it: pd.Timestamp, source: Source, available: frozenset[str]
    ) -> tuple[np.ndarray, np.ndarray]:
        """Latitude and longitude of the cells: cached, decoded from the run, or stored.

        Cached per grid number, so a backfill crossing a grid change keeps both.
        """
        era = self.plan_for(it).era
        if era.grid_number in self._grids:
            return self._grids[era.grid_number]
        decoded = self._decode_grid(it, source, available)
        if decoded is None:
            decoded = self._stored_grid(self.plan_for(it).n_cells)
        if decoded is None:
            raise MissingGridError(
                f"{self.name}: run {it} has no readable CLAT/CLON in the {source} and no "
                f"store of {self.base_store_prefix} holds grid {era.grid_number}; process a "
                "run of that grid that has them first"
            )
        self._grids[era.grid_number] = decoded
        return decoded

    def _decode_grid(
        self, it: pd.Timestamp, source: Source, available: frozenset[str]
    ) -> tuple[np.ndarray, np.ndarray] | None:
        options = storage_options(source)
        plan = self.plan_for(it)
        arrays = []
        for key in plan.schema.coordinate_fields():
            uri = field_uri(self.model, key, it, source)
            data = read_bytes(uri, storage_options=options) if uri in available else None
            if data is None:
                logger.warning(f"{self.name}: no {key.param} in run {it} ({source})")
                return None
            info = grib_field_info(data)
            if info.number_of_values != plan.n_cells or (
                info.grid_number is not None and info.grid_number != plan.era.grid_number
            ):
                raise ValueError(
                    f"{uri}: {info.number_of_values} cells on grid {info.grid_number}, but "
                    f"{self.model} runs from {plan.era.start or 'the start'} have "
                    f"{plan.n_cells} on grid {plan.era.grid_number}"
                )
            arrays.append(decode_grib_field(data, dtype=np.float32))
        return arrays[0], arrays[1]

    def _stored_grid(self, n_cells: int) -> tuple[np.ndarray, np.ndarray] | None:
        """The grid of the newest generation that has one of ``n_cells`` cells."""
        for prefix in reversed(self.generations()):
            try:
                stored = xr.open_zarr(
                    self.config.icechunk_repo(prefix).readonly_session("main").store,
                    consolidated=False,
                )
            except STORE_READ_ERRORS as exc:
                logger.debug(f"{prefix}: no grid ({type(exc).__name__})")
                continue
            if "latitude" in stored.coords and stored.sizes.get("values") == n_cells:
                logger.info(f"{self.name}: using the grid stored in {prefix}")
                return (
                    np.asarray(stored["latitude"].values, dtype=np.float32),
                    np.asarray(stored["longitude"].values, dtype=np.float32),
                )
        return None

    # -- stores -------------------------------------------------------------------------

    def generations(self) -> list[str]:
        """The generations holding this provider's variables on its dimensions, oldest first.

        Narrower than the mixin's "every generation with data", which is what the presence
        check and the grid fallback consult. ICON-Global's base prefix is also the original
        ``icon_global.icechunk``, holding a few hand-picked fields, and a ``params`` subset
        is written to a generation of its own; a run in either is not a run in this store,
        and counting it as one would leave a permanent hole in the full store. A store of
        any of the model's grid eras counts.
        """
        layouts = [{v.name: v.dims for v in plan.layout.variables} for plan in self._plans.values()]
        own = []
        for prefix in super().generations():
            try:
                stored = xr.open_zarr(
                    self.config.icechunk_repo(prefix).readonly_session("main").store,
                    consolidated=False,
                )
            except STORE_READ_ERRORS as exc:
                logger.debug(f"{prefix}: unreadable ({type(exc).__name__}), not ours")
                continue
            found = {str(n): tuple(map(str, stored[n].dims)) for n in stored.data_vars}
            if found in layouts:
                own.append(prefix)
            else:
                logger.debug(f"{prefix}: a different layout, not consulted for {self.name}")
        return own

    def get_icechunk_repo(self) -> icechunk.Repository:
        """Open or create the generation being written, with parallel chunk uploads.

        See :data:`MULTIPART_UPLOAD_MIN_BYTES`. The handle is cached until the generation
        changes, as in :meth:`BaseProvider.get_icechunk_repo`.
        """
        if self._repo is None:
            self._repo = icechunk.Repository.open_or_create(
                self.config.icechunk_storage(self.store_prefix), config=repository_config()
            )
        return self._repo

    def empty_generations(self) -> list[str]:
        """Dated generations that exist but hold no committed data.

        An interrupted first write leaves one behind: a repository named for the day of the
        run it was writing, with nothing on ``main``. ``resolve_generation`` skips an empty
        dated store unless the schema change it is placing happens on that very day, in
        which case it claims it - so a later run of that day would be written into a
        repository still holding the dead write's orphaned chunks. Nothing here deletes
        one; an operator should, once sure nothing is writing to it.

        Only dated names are reported. An empty base store is claimed by whichever run
        comes first, which is what it is for, and the numbered ``_2``.. names are never
        chosen for a new generation (``existing_generations`` even creates them, empty,
        when it probes a store root it cannot list).
        """
        from planetary_datasets.common.generations import existing_generations

        stem = self.base_store_prefix.removesuffix(".icechunk")
        dated = re.compile(rf"^{re.escape(stem)}_\d{{8}}(?:_\d+)?\.icechunk$")
        empty = []
        for prefix in existing_generations(self.config, self.base_store_prefix):
            if not dated.match(prefix):
                continue
            try:
                committed = has_committed_data(self.config.icechunk_repo(prefix))
            except Exception as exc:  # noqa: BLE001 - an unopenable store is reported too
                logger.warning(f"{prefix}: could not be opened ({type(exc).__name__}: {exc})")
                committed = False
            if not committed:
                empty.append(prefix)
        return empty

    # -- write --------------------------------------------------------------------------

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Fetch, process and write one run, holding the write to :attr:`ceiling_gb`.

        Replaces the base implementation, whose memory guard covers only ``process`` and
        whose fit check ignores the write's overhead; here ``process`` is cheap and the
        write is where the memory goes. See :meth:`write_to_icechunk`.

        Returns:
            True if the run was written; False if it was already stored or no source has it.
        """
        it = to_naive_utc(it)
        if check_present and not self.missing_timesteps(pd.DatetimeIndex([it])):
            logger.debug(f"{self.name}: {it} already in {self.describe_store()}, skipping")
            return False
        started = time.monotonic()
        input_files = self.fetch(it)
        if not input_files:
            return False
        processed = self.process(input_files, it)
        try:
            target = self.store_for(processed, None)
            written = self.write_to_icechunk(target, self.prepare_names(processed, target))
        finally:
            self._runs.pop(it, None)
        logger.info(
            f"{self.name}: run {it} {'written to' if written else 'not written to'} "
            f"{self.store_prefix} in {(time.monotonic() - started) / 60:.1f} min"
        )
        return written

    def compute_workers(self, processed: xr.Dataset) -> int:
        """How many blocks may be in flight for the write of ``processed`` to fit.

        Raises:
            MemoryLimitExceeded: If not even one block fits under :attr:`ceiling_gb`.
        """
        resident = process_tree_rss_gb()
        budget = self.ceiling_gb - resident - HEADROOM_GB
        reader_bytes = int(processed.sizes["values"]) * READER_BYTES_PER_CELL
        for workers in range(self.max_workers, 0, -1):
            peak = estimate_peak_bytes(processed, workers, reader_bytes=reader_bytes) / BYTES_PER_GB
            if peak <= budget:
                logger.info(
                    f"{self.name}: {workers} block(s) in flight, ~{peak:.2f} GB estimated on top "
                    f"of {resident:.2f} GB resident, ceiling {self.ceiling_gb:.2f} GB"
                )
                return workers
        raise MemoryLimitExceeded(
            f"{self.name}: one block needs ~{peak:.2f} GB but only {budget:.2f} GB is left "
            f"under the {self.ceiling_gb:.2f} GB ceiling ({resident:.2f} GB resident)"
        )

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Stream one run into ``repo`` under a bounded scheduler and a memory guard.

        The number of blocks in flight comes from :meth:`compute_workers`. The dask compute
        and the commit both run inside :func:`bounded_compute` and :func:`memory_guard`:
        they are one call, so the guard cannot stop between them. A breach is raised if the
        write failed, and logged as an error if the run was committed anyway.

        Written here rather than through :func:`~planetary_datasets.common.store.write_to_icechunk`
        for two reasons: a new store is created at :data:`CREATE_CLEVEL`, and the commit
        message records how many fields were missing at the source, which is only known
        once the data has been read.

        Returns:
            True if committed, False if the run was already stored.

        Raises:
            ValueError: If the store's variables or coordinates do not match the run.
        """
        it = pd.Timestamp(np.atleast_1d(processed[self.append_dim].values)[0])
        state = self._runs.get(it)
        workers = self.compute_workers(processed)
        what = f"{self.name} {it:%Y-%m-%dT%H}"
        written = False
        try:
            with memory_guard(ceiling_gb=self.ceiling_gb, what=what):
                with bounded_compute(workers):
                    written = self._write_run(repo, processed, it, state)
        except MemoryLimitExceeded as exc:
            if not written:
                raise
            # The run is committed and correct; failing the partition now would only make
            # a retry find it stored and report it as not written.
            logger.error(f"{self.name}: run {it} was committed, but {exc}")
        return written

    def _open_stored(self, repo: icechunk.Repository, session) -> xr.Dataset | None:
        """The store's contents, or None if nothing has been written to it yet."""
        try:
            stored = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
        except STORE_READ_ERRORS as exc:
            if has_committed_data(repo):
                raise StoreReadError(
                    f"{self.name}: store holds committed data but could not be read "
                    f"({type(exc).__name__}: {exc}); refusing to overwrite it"
                ) from exc
            return None
        return stored if self.append_dim in stored.coords else None

    def _align_to_store(self, ds: xr.Dataset, stored: xr.Dataset) -> xr.Dataset:
        """Adopt the store's spelling of the grid and axes, refusing a different grid."""
        if set(ds.data_vars) != set(stored.data_vars):
            only_new = sorted(set(ds.data_vars) - set(stored.data_vars))
            only_old = sorted(set(stored.data_vars) - set(ds.data_vars))
            raise ValueError(
                f"{self.name}: variables differ from the store: new {only_new[:5]}, "
                f"stored only {only_old[:5]}"
            )
        # The generation fingerprint ignores dtypes; appending float32 data to a float16
        # array would silently round it, so a change of precision is refused here.
        changed = sorted(n for n in ds.data_vars if ds[n].dtype != stored[n].dtype)
        if changed:
            raise ValueError(
                f"{self.name}: dtype differs from the store for {changed[:5]} (e.g. "
                f"{ds[changed[0]].dtype} vs {stored[changed[0]].dtype}); a change of precision "
                "needs a new store"
            )
        for name in ("latitude", "longitude"):
            if name not in stored.coords:
                continue
            incoming = np.asarray(ds[name].values)
            existing = np.asarray(stored[name].values)
            if incoming.shape != existing.shape or not np.allclose(
                incoming, existing, rtol=0, atol=1e-4, equal_nan=True
            ):
                raise ValueError(f"{self.name}: {name} differs from the store's grid")
            if not np.array_equal(incoming, existing):
                ds = ds.assign_coords({name: ("values", existing, dict(ds[name].attrs))})
        ds = snap_to_stored_coords(ds, stored, tuple(set(GRID_COORDS) | set(self.alignment_coords)))
        ok, bad = coords_match(stored, ds, self.alignment_coords)
        if not ok:
            raise ValueError(f"{self.name}: coordinate {bad!r} does not match the store")
        # Later runs of this grid that lack CLAT/CLON reuse the store's spelling of it.
        grid_number = ds.attrs.get("icon_v1_grid_number")
        if grid_number is not None:
            self._grids[int(grid_number)] = (
                np.asarray(ds["latitude"].values, dtype=np.float32),
                np.asarray(ds["longitude"].values, dtype=np.float32),
            )
        return ds

    def _write_run(
        self,
        repo: icechunk.Repository,
        ds: xr.Dataset,
        it: pd.Timestamp,
        state: _RunState | None,
    ) -> bool:
        """Create or append, then commit with the run's missing-field count."""
        before = read_failures(ds)
        for attempt in range(COMMIT_ATTEMPTS):
            session = repo.writable_session("main")
            stored = self._open_stored(repo, session)
            commit_kwargs: dict = {}
            if stored is None:
                encoding = build_encoding(ds, append_dim=self.append_dim, clevel=CREATE_CLEVEL)
                with quiet_timedelta_decoding():
                    to_icechunk(ds, session, encoding=encoding)
            else:
                present = stored[self.append_dim].values
                if np.isin(np.datetime64(it, "ns"), present).any():
                    logger.info(f"{self.name}: {it} is already stored, not writing it again")
                    return False
                ds = self._align_to_store(ds, stored)
                if present.size and np.datetime64(it, "ns") < present.max():
                    logger.info(
                        f"{self.name}: {it} sorts before the stored {pd.Timestamp(present.max())}; "
                        "appending out of order (sort with the reorder_store asset)"
                    )
                with quiet_timedelta_decoding():
                    to_icechunk(ds, session, append_dim=self.append_dim, mode="a-")
                commit_kwargs["rebase_with"] = icechunk.ConflictDetector()

            after = read_failures(ds)
            missing = {name: after[name] - before.get(name, 0) for name in after}
            total = sum(missing.values())
            listed = state.listed if state is not None else None
            read = state.expected if state is not None else None
            expected = listed.expected if listed is not None else read
            absent = listed.expected - listed.present if listed is not None else 0
            source = state.source if state is not None else "unknown"
            message = (
                f"{self.name}: {it:%Y-%m-%dT%H:%M} from {source}; {absent + total} of "
                f"{expected if expected is not None else '?'} fields missing at the source "
                f"({absent} unlisted, {total} unreadable)"
            )
            metadata = {
                "init_time": it.isoformat(),
                "source": source,
                "fields_expected": expected,
                "fields_listed": listed.present if listed is not None else None,
                "fields_unreadable": total,
                "fields_missing": absent + total,
            }
            try:
                session.commit(message, metadata=metadata, **commit_kwargs)
            except RACE_ERRORS as exc:
                if attempt == COMMIT_ATTEMPTS - 1:
                    raise
                logger.warning(
                    f"{self.name}: commit raced another writer ({type(exc).__name__}), retrying"
                )
                before = after
                time.sleep(min(2**attempt, 30))
                continue
            self._log_missing(it, missing, read, ds, listed)
            return True
        raise RuntimeError("unreachable")

    def _log_missing(
        self,
        it: pd.Timestamp,
        missing: dict[str, int],
        read: int | None,
        ds: xr.Dataset,
        listed: RunCompleteness | None,
    ) -> None:
        """Summarise the fields of a run that were missing at the source."""
        total = sum(missing.values())
        absent = listed.expected - listed.present if listed is not None else 0
        unpublished = sum(missing_report(ds).values()) - absent
        logger.info(
            f"{self.name}: run {it} committed; {absent:,} expected fields not listed at the "
            f"source, {total:,} of {read or 0:,} listed fields unreadable, {unpublished:,} "
            "slots with no field in this cycle (NaN by design)"
        )
        if absent and listed is not None:
            logger.warning(f"{self.name}: run {it} unlisted fields: {listed.most_missing()}")
        if total:
            top = Counter(missing).most_common(REPORT_TOP_N)
            summary = ", ".join(f"{name} {count:,}" for name, count in top if count)
            logger.warning(f"{self.name}: run {it} unreadable fields by variable: {summary}")


@dataclasses.dataclass(frozen=True)
class PartitionResult:
    """What one :func:`run_partition_child` call did, small enough to send between processes.

    Attributes:
        init_time: The run.
        written: Whether it was committed.
        store_prefix: The generation it went to, or would have.
        completeness: What the source listed for it, if it was listed.
        seconds: Wall time of :func:`run_partition_child` itself: building the provider and
            running the partition, not starting the process or importing the library.
    """

    init_time: pd.Timestamp
    written: bool
    store_prefix: str
    completeness: RunCompleteness | None
    seconds: float


def run_partition_child(
    variant: str, it: pd.Timestamp, provider_kwargs: dict | None = None
) -> PartitionResult:
    """Build a provider and run one partition; the entry point of a backfill's worker.

    Module-level so a ``spawn`` child process can import it. A long backfill runs each
    partition in a fresh process (:func:`run_in_child`), so nothing a run leaves behind -
    caches in libraries, allocator fragmentation - can slow the next one down. The cost is
    a fresh provider each run: the source's run listing and the run's CLAT/CLON are read
    again, about a second against minutes of writing.

    Args:
        variant: Provider variant.
        it: Run to write. Already known not to be stored; the presence check is skipped.
        provider_kwargs: Keyword arguments for :class:`ICONV1Provider`.
    """
    started = time.monotonic()
    provider = ICONV1Provider(variant, **(provider_kwargs or {}))
    it = to_naive_utc(it)
    written = provider.run_partition(it, check_present=False)
    return PartitionResult(
        init_time=it,
        written=written,
        store_prefix=provider.store_prefix,
        completeness=provider.completeness.get(it),
        seconds=time.monotonic() - started,
    )


def _level_attrs(dim: str) -> dict[str, str]:
    """Units and a description for a level coordinate."""
    if dim.startswith("isobaricInhPa"):
        return {"units": "hPa", "long_name": "pressure level"}
    if dim.startswith("depth"):
        return {"units": "m", "long_name": "depth below land surface (top of layer)"}
    if dim.startswith(_MODEL_HALF):
        return {"long_name": "ICON model half level, 1 at the model top"}
    if dim.startswith(_MODEL_FULL):
        return {"long_name": "ICON model full level, 1 at the model top"}
    if dim == _WAVELENGTH:
        return {"units": "nm", "long_name": "wavelength"}
    return {}


# ---------------------------------------------------------------------------------------
# Running one partition in a child process
# ---------------------------------------------------------------------------------------


class ChildDied(RuntimeError):
    """A child process ended without reporting a result: killed, or crashed natively."""


#: Seconds between a child's checks that the process that started it is still there.
PARENT_CHECK_INTERVAL = 5.0


def _watch_parent(parent_pid: int) -> None:
    """Exit the process as soon as its parent is gone.

    A backfill must stay the only writer to its store. If the parent is killed and a new
    one started, an orphaned child still writing its run would be a second writer, so the
    child ends itself rather than finish. It never commits half a run; an interrupted
    first write can leave an empty generation, which the backfill refuses to start beside.
    """
    while True:
        time.sleep(PARENT_CHECK_INTERVAL)
        if os.getppid() != parent_pid:
            os._exit(3)


def _sendable(exc: BaseException) -> BaseException:
    """``exc`` if it survives pickling, else a RuntimeError carrying its type and message."""
    try:
        pickle.loads(pickle.dumps(exc))
    except Exception:  # noqa: BLE001 - any failure to round-trip means "send a stand-in"
        return RuntimeError(f"{type(exc).__module__}.{type(exc).__qualname__}: {exc}")
    return exc


def _child_entry(conn, parent_pid: int, target: Callable, args: tuple) -> None:
    """Body of a :func:`run_in_child` process: run ``target`` and send back the outcome."""
    threading.Thread(target=_watch_parent, args=(parent_pid,), daemon=True).start()
    try:
        result = target(*args)
    except BaseException as exc:  # noqa: BLE001 - every failure goes back to the parent
        conn.send(("error", _sendable(exc), traceback.format_exc()))
    else:
        conn.send(("ok", result, None))
    finally:
        conn.close()


def run_in_child(target: Callable, *args, poll: float = 1.0):
    """Call ``target(*args)`` in a freshly spawned process and return its result.

    The child imports everything afresh and exits when done, so nothing the call leaves
    behind outlives it. It lives no longer than the caller: it ends itself if the caller
    disappears (:func:`_watch_parent`), and an interrupt in the caller terminates it.

    Args:
        target: A module-level callable, so the child can import it.
        *args: Its arguments; they and its result must pickle.
        poll: Seconds between checks that the child is still alive.

    Returns:
        What ``target`` returned.

    Raises:
        ChildDied: If the child ended without sending a result.
        BaseException: Whatever ``target`` raised, with the child's traceback as a note;
            a RuntimeError naming it if it could not be pickled.
    """
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_child_entry, args=(sender, os.getpid(), target, args), daemon=True
    )
    process.start()
    sender.close()
    outcome = None
    try:
        while outcome is None:
            if receiver.poll(poll):
                try:
                    outcome = receiver.recv()
                except EOFError:
                    break
            elif not process.is_alive():
                break
    except BaseException:
        process.terminate()
        process.join(30)
        if process.is_alive():
            process.kill()
        raise
    finally:
        receiver.close()
    process.join()
    if outcome is None:
        raise ChildDied(f"child process {process.pid} ended with exit code {process.exitcode}")
    status, payload, child_traceback = outcome
    if status == "error":
        payload.add_note(f"in child process {process.pid}:\n{child_traceback}")
        raise payload
    return payload
