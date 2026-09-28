"""Combine the GRIB output of `mars.py` into a single Icechunk store.

`mars.py` retrieves five families of GRIB files, all of which land on ECMWF's
native octahedral reduced Gaussian grid (O1280, 6,599,680 points):

* ``output_an_*``          -- model-level analyses at 00/06/12/18Z
* ``output_fc_{hh}z_*``    -- model-level forecast steps 1-5, filling the
                              hours between analyses
* ``output_wave_an_*``     -- 3-hourly ocean wave analyses
* ``output_spectra_an_*``  -- 3-hourly 2D wave spectra analyses
* ``output_spectra_fc_*``  -- 2D wave spectra forecast steps 1-5

The model-level files mix two GRIB representations: ``crwc``, ``cswc``, ``q``,
``clwc``, ``ciwc``, ``cc`` and ``cat`` are archived as reduced Gaussian
gridpoint fields, while ``etadot``, ``z``, ``t``, ``w``, ``vo``, ``lnsp``,
``d``, ``u`` and ``v`` are archived as T1279 spherical harmonics. The spectral
fields are transformed to the same O1280 grid with MIR (via Metview) so that
every variable shares one spatial index; gridpoint fields pass through MIR
bit-identically. The wave and spectra files are already on O1280, bitmapped to
sea points, so they need no transform and can live in the same store.

Because the grid is a *reduced* Gaussian grid, the horizontal dimension is the
flat ``values`` dimension, with ``latitude`` and ``longitude`` as 1-D
coordinates along it (plus ``pl``, the number of points per latitude row, so
the grid can be reconstructed). The store is chunked one timestep and one model
level per chunk, at full spatial resolution.

Storage is lossy: every variable is bit-rounded to a per-variable number of
retained mantissa bits (see `KEEPBITS`) before Blosc/zstd. On a real
temperature field this takes compression from 2.2x to 12.5x.

Resulting schema::

    time                        (T,)                    valid time of the analysis/forecast step
    level                       (137,)                  model level
    direction                   (36,)                   wave spectra direction bins, degrees
    frequency                   (29,)                   wave spectra frequency bins, Hz
    latitude, longitude         (values,)               O1280 coordinates
    pl                          (row,)                  points per latitude row
    hybrid_a, hybrid_b          (halflevel,)            model level coefficients

    <model level var>           (time, level, values)   chunks (1, 1, values)
    z, lnsp                     (time, values)          chunks (1, values)
    <wave var>                  (time, values)          chunks (1, values)
    d2fd                        (time, direction, frequency, values)
                                                        chunks (1, 1, 1, values)

The time axis is a regular hourly axis. Hours the retrievals do not (yet)
cover are left unwritten, which costs nothing in storage and reads back as NaN
with the ``ingested_*`` flags false. A store created from an earlier, narrower
retrieval is grown in place by `extend_time_axis`: new hours can land before,
between or after the existing ones, and existing chunks are moved to their new
positions by rewriting the manifest, not by copying data.

Usage (from the repository root, inside the pixi environment)::

    # Create or extend both schemas once, then fan out over disjoint time ranges.
    pixi run python -m planetary_datasets.providers.mars_icechunk --init-only --axis-end 2026-09-14T23
    pixi run python -m planetary_datasets.providers.mars_icechunk --regrid 0.25 1 --init-only
    # Once, for a regrid store made before model levels were chunked as full columns.
    pixi run python -m planetary_datasets.providers.mars_icechunk --regrid 0.25 --rechunk-levels
    pixi run python -m planetary_datasets.providers.mars_icechunk --start 2026-01-01 --end 2026-01-10
    pixi run python -m planetary_datasets.providers.mars_icechunk --regrid 0.25 1 --follow

Every timestep is an independent set of region writes, so several processes can
run concurrently over different ``--start``/``--end`` slices. Extending the
time axis is not safe alongside them: it moves chunks, so a worker that looked
up a timestep's position before the move would write to the wrong place. Run
``--init-only`` with nothing else writing, then start the workers.

Configuration
-------------

Every path and credential comes from
:func:`planetary_datasets.config.get_config`:

* the store is ``cfg.icechunk_repo(STORE_PREFIX)``, so ``ICECHUNK_BUCKET`` /
  ``ICECHUNK_PREFIX`` move it and ``ICECHUNK_LOCAL_PATH`` redirects it to a
  local directory for tests;
* S3 credentials come from ``AWS_PROFILE`` or ``AWS_ACCESS_KEY_ID`` /
  ``AWS_SECRET_ACCESS_KEY``, resolved by ``Config.icechunk_storage``;
* the GRIB lives under ``<PLANETARY_DATASETS_DATA_DIR>/mars`` and is staged
  under ``<PLANETARY_DATASETS_SCRATCH_DIR>/mars_staging``.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import functools
import os
import pathlib
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable, List, Sequence

import dask
import dask.array as da
import eccodes as ec
import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import zarr.codecs
from icechunk.xarray import to_icechunk
from loguru import logger
from zarr.codecs.numcodecs import BitRound

# numcodecs only lets Blosc use its own threads when called from the main
# thread, and zarr v3 encodes chunks from worker threads, so without this every
# chunk compresses single-threaded. A 0.25 degree model level chunk is 569MB;
# measured, this takes its encode from 12.9s to 9.4s, output byte-identical.
import numcodecs.blosc  # noqa: E402

numcodecs.blosc.use_threads = True

from planetary_datasets.base import BaseProvider
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import (
    BYTES_PER_GB,
    available_memory_gb,
    estimate_dataset_gb,
    estimate_peak_gb,
    memory_guard,
    require_memory,
)

# Native grid of the archive. Every family of files in the retrieval uses it,
# and MIR is asked to transform the spectral fields onto exactly this grid.
NATIVE_GRID = "O1280"
N_VALUES = 6_599_680
N_ROWS = 2560
N_MODEL_LEVELS = 137
N_HALF_LEVELS = N_MODEL_LEVELS + 1

#: This module's own import path. Spelled out rather than read from
#: ``__name__``, which is ``"__main__"`` when the CLI below is run with
#: ``python -m``: a MIR child told to import from ``__main__`` would import
#: its own ``-c`` script and fail.
_MODULE = "planetary_datasets.providers.mars_icechunk"

#: Published location of the combined store, relative to the configured bucket.
#: The regridded stores sit beside it, named by `regrid_store_prefix`
#: (0.25 degrees -> ``ifs_native_025.icechunk``).
STORE_PREFIX = "bkr/ifs/ifs_native.icechunk"


def default_source_dir(config: Config | None = None) -> pathlib.Path:
    """Directory holding the ``output_*.grib`` retrievals: ``<data_dir>/mars``.

    Set ``PLANETARY_DATASETS_DATA_DIR`` to move it; on the Linux box that is
    the big array, on a laptop wherever there is room.
    """
    return (config or get_config()).data_dir / "mars"


def budget_gb(config: Config | None = None) -> float:
    """:func:`~planetary_datasets.memory.memory_budget_gb`, but against `config`.

    The shared helper reads the process-wide configuration, so a provider or
    pipeline handed an explicit ``Config`` would resolve every path from it and
    its memory budget from somewhere else. These jobs are the ones where that
    matters, so the same two settings are read here from the config in hand.
    """
    config = config or get_config()
    if config.memory_ceiling_gb is not None:
        return config.memory_ceiling_gb
    return available_memory_gb() * config.memory_fraction


def default_staging_dir(config: Config | None = None) -> pathlib.Path:
    """Where a timestep's GRIB messages are staged for decoding.

    ``<scratch_dir>/mars_staging``. This wants to be fast local disk rather
    than the RAM-backed ``/tmp`` -- about 10GB per timestep in flight -- so set
    ``PLANETARY_DATASETS_SCRATCH_DIR`` to an NVMe mount on a machine that has
    one. See `stage_timestep`.
    """
    return (config or get_config()).scratch_dir / "mars_staging"


#: MIR processes to transform one variable's spherical harmonics with, each on
#: a run of its levels. Overridden by $MARS_MIR_PROCESSES. See `_to_gridpoint_parallel`.
DEFAULT_MIR_PROCESSES = int(os.environ.get("MARS_MIR_PROCESSES", "4"))

SPECTRA_PARAM_ID = 140251
SPECTRA_VAR = "d2fd"
N_DIRECTIONS = 36
N_FREQUENCIES = 29

#: ``vo`` and ``d`` are the IFS prognostic variables; MARS derives ``u`` and
#: ``v`` from them on retrieval. This is exact, not approximate: transforming
#: the archived vorticity and divergence with MIR's uvwind reproduces the
#: archived u and v *bit for bit*, spectral coefficient for spectral
#: coefficient. The two pairs are one field in two coordinates -- the Helmholtz
#: decomposition is a bijection on a sphere -- so storing both would duplicate
#: about 14% of the model-level volume for no information.
#:
#: Dropped in favour of u/v, which is what downstream consumers actually want.
#: Note the asymmetry if you reverse this: recovering vo/d from gridded,
#: bit-rounded u/v needs a full spectral transform back to T1279, and the
#: forward map multiplies by n(n+1), amplifying exactly the small-scale noise
#: bit rounding introduces. Pass ``exclude=EXCLUDED_SHORT_NAMES -
#: REDUNDANT_WITH_UV`` to keep them.
REDUNDANT_WITH_UV = frozenset({"vo", "d"})

#: ``o3`` is archived in the analyses only, so it would be missing for three
#: quarters of the hourly time axis. ``cat`` is the mirror case -- forecast
#: steps only -- but is kept deliberately.
#:
#: ``w`` and ``etadot`` are both kept. They are diagnostic in the IFS, computed
#: from the divergence field and surface pressure, but recovering them takes a
#: vertical integral over all 137 levels against the model's own discretisation
#: -- nothing like the one-line exact inversion that recovers u/v from vo/d.
#: They are not substitutes for each other either: omega follows pressure
#: surfaces while eta-dot crosses the terrain-following hybrid surfaces, and
#: they correlate only about 0.7 in the mid-troposphere.
EXCLUDED_SHORT_NAMES = frozenset({"o3"}) | REDUNDANT_WITH_UV

#: Archived on model level 1 but really surface fields, so they are stored
#: without a ``level`` dimension.
ML_SURFACE_SHORT_NAMES = frozenset({"z", "lnsp"})

#: float32 carries 23 explicit mantissa bits, so keeping 23 or more is a no-op.
#: Variables set to this get no BitRound filter at all and are stored exactly.
LOSSLESS_KEEPBITS = 23

#: Mantissa bits kept by the BitRound filter, per variable. Bit rounding bounds
#: the *relative* error at ``2 ** -(keepbits + 1)``, so the right value differs
#: by variable and is chosen one of two ways:
#:
#: * Fields with a bounded range and a natural absolute tolerance get
#:   ``keepbits = ceil(log2(max_magnitude / tolerance)) - 1``.
#: * Fields spanning orders of magnitude (condensate, humidity, vorticity)
#:   have no meaningful absolute tolerance, so relative precision is the
#:   criterion and a flat value is used.
#:
#: ``lnsp`` is the trap: it is a logarithm, so relative precision on the stored
#: value becomes a much larger error in the pressure it encodes. Measured on a
#: real field, keepbits=12 gives a 101 Pa surface pressure error and keepbits=20
#: gives 0.4 Pa.
KEEPBITS: dict[str, int] = {
    # Bounded range, absolute tolerance.
    "t": 14,  # 330 K / 0.01 K
    # Stored as full float32: no bit rounding at all, so the winds carry
    # exactly what MIR produced. The rounding was never far from mattering
    # here -- the archive's own 16-bit spectral encoding of u has an RMS error
    # of 9.6e-4 m s-1, and bit rounding at keepbits=13 was adding 9.1e-4 on
    # top of it, roughly doubling the error.
    "u": LOSSLESS_KEEPBITS,
    "v": LOSSLESS_KEEPBITS,
    "z": 15,  # 6e4 m2 s-2 / 1 m2 s-2; one static field, so precision is free
    "lnsp": 20,  # 1e-5 in ln(Pa), i.e. 1 Pa of surface pressure
    "cc": 10,  # 0-1 fraction / 5e-4
    # Orders of magnitude of range, relative precision.
    "q": 10,
    "crwc": 10,
    "cswc": 10,
    "clwc": 10,
    "ciwc": 10,
    "cat": 10,
    "w": 12,
    "vo": 12,
    "d": 12,
    "etadot": 12,
    # log10 of spectral density: 1e-3 in log10 is 0.2% in the spectrum itself.
    SPECTRA_VAR: 12,
}

#: The 68 wave fields are one field per timestep each, against 137 levels for
#: every model level variable, so they are a rounding error in the total volume
#: and are kept near-lossless (3e-5 relative) rather than tuned individually.
WAVE_KEEPBITS = 14

DEFAULT_KEEPBITS = 12

GROUP_ML = "ml"
GROUP_WAVE = "wave"
GROUP_SPECTRA = "spectra"
GROUPS = (GROUP_ML, GROUP_WAVE, GROUP_SPECTRA)

_INGESTED = {group: f"ingested_{group}" for group in GROUPS}

_INDEX_COLUMNS = [
    "path",
    "offset",
    "length",
    "short_name",
    "param_id",
    "level",
    "valid_time",
    "data_type",
    "grid_type",
    "direction",
    "frequency",
    "group",
]

_metview_module = None

#: The environment as it was before any Metview session started. Starting one
#: adds its session to os.environ -- EVENT_PORT, METVIEW_TMPDIR, MARS_CONFIG,
#: even TMPDIR -- and a child process that inherits those tries to join this
#: process's session instead of starting its own, and never gets an answer.
_BASE_ENV = dict(os.environ)


def _metview():
    """Import Metview, putting the environment's ``bin`` on PATH if needed.

    The Metview Python bindings spawn the ``metview`` binary, which lives in
    the conda/pixi environment's ``bin`` directory. That directory is not on
    PATH unless the environment has been activated, so a bare
    ``import metview`` fails with FileNotFoundError under `pixi run`.
    """
    global _metview_module
    if _metview_module is None:
        # Metview gives its server 8s to start by default. Several starting at
        # once (see `_to_gridpoint_parallel`) on a loaded machine take longer.
        os.environ.setdefault("METVIEW_PYTHON_START_TIMEOUT", "300")
        if shutil.which("metview") is None:
            env_bin = str(pathlib.Path(sys.prefix) / "bin")
            os.environ["PATH"] = os.pathsep.join([env_bin, os.environ.get("PATH", "")])
        import metview  # noqa: PLC0415 -- deferred so importing this module is cheap

        _metview_module = metview
    return _metview_module


@functools.lru_cache(maxsize=None)
def _physical(path: str | pathlib.Path) -> str:
    """The path to read `path` through, bypassing mergerfs when it is behind one.

    The GRIB array on the ingest box is a mergerfs FUSE mount with
    ``cache.files=off``, so every read is a round trip through the FUSE daemon,
    and eccodes reads GRIB
    headers in small pieces. mergerfs publishes each file's real location in
    the ``user.mergerfs.fullpath`` xattr; reading from there instead measured
    1.4x faster for a header scan. Paths keep their mergerfs form everywhere
    else -- in the index, notably -- since that is the file's stable identity.

    ``os.getxattr`` is Linux-only, so on macOS the lookup raises AttributeError
    rather than OSError; there is no mergerfs there either way, and the path is
    returned unchanged.
    """
    try:
        return os.getxattr(str(path), "user.mergerfs.fullpath").decode()
    except (AttributeError, OSError):
        return str(path)


def _date_order(path: pathlib.Path) -> tuple:
    """Sort key putting files in order of the dates they cover, whatever the family."""
    match = re.search(r"_(\d{8})_(\d{8})\.grib$", path.name)
    return (match.groups() if match else ("", ""), path.name)


def _classify(param_id: int, type_of_level: str) -> str:
    """Return which family a GRIB message belongs to.

    ``cat`` (260290) has a paramId above the wave table, so model levels have
    to be recognised by their level type rather than by paramId range.
    """
    if param_id == SPECTRA_PARAM_ID:
        return GROUP_SPECTRA
    if type_of_level == "hybrid":
        return GROUP_ML
    return GROUP_WAVE


def scan_grib(path: str | pathlib.Path) -> pd.DataFrame:
    """Index every GRIB message in `path` by byte range and identity.

    Only the message headers are decoded, so this is a seek-bound scan: a 67GB
    model-level file with 23,040 messages takes roughly 25 seconds.
    """
    path = pathlib.Path(path)
    rows: List[tuple] = []
    truncated = False
    with open(_physical(path), "rb") as f:
        while True:
            try:
                handle = ec.codes_grib_new_from_file(f, headers_only=True)
            except ec.PrematureEndOfFileError:
                # An interrupted download leaves a partial trailing message.
                # Everything before it is intact and usable, so keep it and
                # let the caller decide; `_ml_is_complete` stops a timestep
                # missing levels from being marked ingested.
                truncated = True
                break
            if handle is None:
                break
            try:
                param_id = ec.codes_get_long(handle, "paramId")
                type_of_level = ec.codes_get_string(handle, "typeOfLevel")
                group = _classify(param_id, type_of_level)
                if group == GROUP_SPECTRA:
                    direction = ec.codes_get_long(handle, "directionNumber")
                    frequency = ec.codes_get_long(handle, "frequencyNumber")
                else:
                    direction = frequency = 0
                data_date = ec.codes_get_long(handle, "dataDate")
                data_time = ec.codes_get_long(handle, "dataTime")
                step = ec.codes_get_long(handle, "step")
                valid_time = (
                    pd.Timestamp(str(data_date))
                    # Explicit units: the keyword form builds a generic-unit
                    # numpy timedelta, which is deprecated and due to become
                    # an error.
                    + pd.Timedelta(data_time // 100, unit="h")
                    + pd.Timedelta(data_time % 100, unit="m")
                    + pd.Timedelta(step, unit="h")
                )
                rows.append(
                    (
                        str(path),
                        ec.codes_get_long(handle, "offset"),
                        ec.codes_get_long(handle, "totalLength"),
                        ec.codes_get_string(handle, "shortName"),
                        param_id,
                        ec.codes_get_long(handle, "level"),
                        valid_time,
                        ec.codes_get_string(handle, "dataType"),
                        ec.codes_get_string(handle, "gridType"),
                        direction,
                        frequency,
                        group,
                    )
                )
            finally:
                ec.codes_release(handle)
    if truncated:
        logger.warning(
            f"{path.name} is truncated: indexed {len(rows)} complete messages, then hit a "
            "partial one. Re-download it to get the timesteps it cuts short."
        )
    else:
        logger.info(f"Indexed {len(rows)} messages in {path.name}")
    return pd.DataFrame(rows, columns=_INDEX_COLUMNS)


def read_grid(path: str | pathlib.Path) -> dict:
    """Read the O1280 grid description.

    Every family of files shares the same grid, so any message will do.
    """
    with open(_physical(path), "rb") as f:
        handle = ec.codes_grib_new_from_file(f)
        if handle is None:
            raise ValueError(f"{path} contains no GRIB messages")
        try:
            return {
                "latitude": ec.codes_get_array(handle, "latitudes"),
                "longitude": ec.codes_get_array(handle, "longitudes"),
                "pl": ec.codes_get_array(handle, "pl").astype(np.int32),
            }
        finally:
            ec.codes_release(handle)


def read_hybrid_coefficients(path: str | pathlib.Path) -> tuple[np.ndarray, np.ndarray]:
    """Read the model level A/B coefficients from a model-level message.

    Only hybrid-level messages carry ``pv``; the wave and spectra files do not,
    so this must be pointed at one of the ``output_an_*`` / ``output_fc_*``
    files rather than at whichever file happens to be first in the index.
    """
    with open(_physical(path), "rb") as f:
        handle = ec.codes_grib_new_from_file(f)
        if handle is None:
            raise ValueError(f"{path} contains no GRIB messages")
        try:
            pv = ec.codes_get_array(handle, "pv")
        finally:
            ec.codes_release(handle)
    return pv[:N_HALF_LEVELS].astype(np.float64), pv[N_HALF_LEVELS:].astype(np.float64)


def read_spectra_bins(path: str | pathlib.Path) -> tuple[np.ndarray, np.ndarray]:
    """Read the physical direction (degrees) and frequency (Hz) bins."""
    with open(_physical(path), "rb") as f:
        handle = ec.codes_grib_new_from_file(f)
        if handle is None:
            raise ValueError(f"{path} contains no GRIB messages")
        try:
            directions = ec.codes_get_array(handle, "scaledDirections") / ec.codes_get_long(
                handle, "directionScalingFactor"
            )
            frequencies = ec.codes_get_array(handle, "scaledFrequencies") / ec.codes_get_long(
                handle, "frequencyScalingFactor"
            )
            return directions.astype(np.float32), frequencies.astype(np.float32)
        finally:
            ec.codes_release(handle)


def _copy_messages(refs: Sequence[tuple[str, int, int]], target: pathlib.Path) -> None:
    """Copy the referenced GRIB messages, by byte range, into one file."""
    with open(target, "wb") as out:
        for path, group in pd.DataFrame(refs, columns=["path", "offset", "length"]).groupby(
            "path", sort=False
        ):
            with open(_physical(path), "rb") as src:
                for offset, length in zip(group["offset"], group["length"]):
                    src.seek(offset)
                    out.write(src.read(length))


def _to_gridpoint(source: pathlib.Path, target: pathlib.Path, accuracy: int) -> pathlib.Path:
    """Transform spherical harmonic fields onto the native O1280 grid.

    MIR amortises the Legendre coefficient setup over a fieldset, so this is
    about fifteen times cheaper per field when called on all 137 levels of a
    variable at once than when called field by field.

    MIR would otherwise inherit the 16 bits per value of the spectral packing;
    `accuracy` raises that so the transform is not quantised a second time
    before it reaches float32 in the store.
    """
    mv = _metview()
    fieldset = mv.read(str(source))
    mv.write(str(target), mv.read(data=fieldset, grid=NATIVE_GRID, accuracy=accuracy))
    return target


def _message_spans(path: str | pathlib.Path) -> List[tuple[int, int]]:
    """(offset, length) of every GRIB message in `path`, from section 0 alone.

    Reads 16 bytes per message and seeks over the rest, so it costs nothing
    even for a 137-level variable.
    """
    spans = []
    with open(path, "rb") as f:
        offset = 0
        while header := f.read(16):
            if len(header) < 16 or header[:4] != b"GRIB":
                raise ValueError(f"{path}: no GRIB message at byte {offset}")
            edition = header[7]
            length = (
                int.from_bytes(header[8:16], "big")
                if edition == 2
                else int.from_bytes(header[4:7], "big")
            )
            spans.append((offset, length))
            offset += length
            f.seek(offset)
    return spans


def _mir_child(source: str, target: str, accuracy: int) -> None:
    """Body of one `_to_gridpoint_parallel` process."""
    _to_gridpoint(pathlib.Path(source), pathlib.Path(target), accuracy)


def _to_gridpoint_parallel(
    source: pathlib.Path, target: pathlib.Path, accuracy: int, processes: int
) -> pathlib.Path:
    """`_to_gridpoint`, with the messages split across `processes` MIR processes.

    MIR's cost is linear in the number of fields -- measured 1.2s a level plus
    about 3s of setup -- so a 137-level variable took 165s through one
    process, and a forecast hour has five such variables. Transforms of
    different fields are independent, so the messages are split into
    consecutive runs, one MIR (each in its own Python and Metview session;
    Metview serialises calls within one) per run, and the outputs are joined
    in order. Memory is about the same as one process doing it all, since each
    holds only its share of the fields.

    Children are started with `subprocess` rather than `multiprocessing`
    because this runs inside process-pool workers. Each gets its own session,
    and the whole session is killed once the child exits: a child's Metview
    server outlives it, and would otherwise pile up and -- had the child's
    stderr been a pipe -- hold that pipe open forever.
    """
    spans = _message_spans(source)
    runs = min(processes, len(spans) // 8)  # not worth splitting small variables
    if runs <= 1:
        return _to_gridpoint(source, target, accuracy)

    bounds = np.linspace(0, len(spans), runs + 1).round().astype(int)
    parts, children = [], []
    # The directory `planetary_datasets` itself lives in, so that the child's
    # `import planetary_datasets.providers.mars_icechunk` resolves even when
    # the package is not installed into the environment. `parents[0]` is
    # `providers`, `parents[1]` the package, `parents[2]` its parent.
    package_root = str(pathlib.Path(__file__).resolve().parents[2])
    env = {
        **_BASE_ENV,
        "PYTHONPATH": os.pathsep.join([package_root, _BASE_ENV.get("PYTHONPATH", "")]),
        "METVIEW_PYTHON_START_TIMEOUT": _BASE_ENV.get("METVIEW_PYTHON_START_TIMEOUT", "300"),
    }
    try:
        with open(source, "rb") as src:
            for k, (lo, hi) in enumerate(zip(bounds[:-1], bounds[1:])):
                part_in = target.with_name(f"{target.stem}.part{k}.in.grib")
                part_out = target.with_name(f"{target.stem}.part{k}.grib")
                log = target.with_name(f"{target.stem}.part{k}.log")
                src.seek(spans[lo][0])
                part_in.write_bytes(src.read(spans[hi - 1][0] + spans[hi - 1][1] - spans[lo][0]))
                parts.append((part_in, part_out))
                code = (
                    f"from {_MODULE} import _mir_child; "
                    f"_mir_child({str(part_in)!r}, {str(part_out)!r}, {accuracy})"
                )
                with open(log, "wb") as err:
                    child = subprocess.Popen(
                        [sys.executable, "-c", code],
                        env=env,
                        stdout=subprocess.DEVNULL,
                        stderr=err,
                        start_new_session=True,
                    )
                children.append((child, log))
        failures = []
        for child, log in children:
            child.wait()
            if child.returncode:
                failures.append(log.read_text(errors="replace")[-2000:])
    finally:
        for child, log in children:
            if child.poll() is None:
                child.kill()
                child.wait()
            try:
                os.killpg(child.pid, signal.SIGKILL)  # the child's leftover Metview server
            except ProcessLookupError:
                pass
            log.unlink(missing_ok=True)
    if failures:
        raise RuntimeError(f"{len(failures)} of {runs} MIR processes failed:\n{failures[0]}")
    with open(target, "wb") as out:
        for part_in, part_out in parts:
            with open(part_out, "rb") as f:
                shutil.copyfileobj(f, out, 64 << 20)
            part_in.unlink()
            part_out.unlink()
    return target


def _whole_file(refs: Sequence[tuple[str, int, int]]) -> str | None:
    """The file `refs` cover exactly, if they are every message of one file."""
    paths = {path for path, _, _ in refs}
    if len(paths) != 1:
        return None
    (path,) = paths
    end = 0
    for _, offset, length in sorted(refs, key=lambda ref: ref[1]):
        if offset != end:
            return None
        end += length
    return path if end == os.path.getsize(path) else None


def _load_fields(
    refs: Sequence[tuple[str, int, int]],
    shape: tuple[int, ...],
    mode: str,
    spectral: bool,
    accuracy: int,
    workdir: str | None = None,
    mir_processes: int = 1,
) -> np.ndarray:
    """Extract, transform if needed, and decode a block of GRIB messages.

    Fields are placed by the keys read back from the GRIB rather than by input
    order, so a reordering inside MIR cannot silently transpose the block.
    Points masked out by a bitmap (land, for the wave fields) decode to NaN.

    When `refs` are exactly one whole file -- as they are after
    `stage_timestep` -- that file is decoded in place instead of being copied.
    Scratch files (the copy, MIR's output) go in `workdir`. Spherical
    harmonics are transformed by `mir_processes` MIR processes at once.

    A block's size is known exactly from `shape`, so the memory budget is
    checked before the allocation rather than after the OOM killer has
    intervened: the caller sees ``MemoryLimitExceeded`` naming the block, and
    the timestep is retried later rather than taking the host down.

    The block itself is nearly all of it -- 3.6GB for a model level variable,
    766MB for one spectra direction. Only one message is decoded at a time on
    top of it (26MB on this grid) and MIR's output goes to `workdir`, not to
    memory, so a quarter over the block is enough headroom.
    """
    block_gb = float(np.prod(shape, dtype=np.int64)) * 4 / BYTES_PER_GB
    require_memory(block_gb * 1.25, what=f"GRIB block {shape} ({mode})")

    out = np.full(shape, np.nan, dtype=np.float32)
    seen = 0
    with tempfile.TemporaryDirectory(prefix="mars_grib_", dir=workdir) as tmp:
        raw = _whole_file(refs)
        if raw is None:
            raw = pathlib.Path(tmp) / "raw.grib"
            _copy_messages(refs, raw)
        source = (
            _to_gridpoint_parallel(
                pathlib.Path(raw), pathlib.Path(tmp) / "gg.grib", accuracy, mir_processes
            )
            if spectral
            else raw
        )
        with open(source, "rb") as f:
            while True:
                handle = ec.codes_grib_new_from_file(f)
                if handle is None:
                    break
                try:
                    ec.codes_set_double(handle, "missingValue", np.nan)
                    values = ec.codes_get_array(handle, "values").astype(np.float32)
                    if values.size != N_VALUES:
                        raise ValueError(
                            f"expected {N_VALUES} values on the {NATIVE_GRID} grid, "
                            f"got {values.size}"
                        )
                    if mode == "level":
                        out[ec.codes_get_long(handle, "level") - 1] = values
                    elif mode == "spectra":
                        out[ec.codes_get_long(handle, "frequencyNumber") - 1] = values
                    else:
                        out[:] = values
                    seen += 1
                finally:
                    ec.codes_release(handle)
    if seen != len(refs):
        raise ValueError(f"expected {len(refs)} messages back from eccodes, decoded {seen}")
    return out


#: Raised when a commit races another worker. `RebaseFailedError` is the one
#: that actually surfaces when two workers touched the same chunk; catching
#: only `ConflictError` silently leaves that case unhandled.
_RACE_ERRORS = (icechunk.ConflictError, icechunk.RebaseFailedError)


def _commit(session, message: str, attempts: int = 8) -> str:
    """Commit an already-written session, rebasing past concurrent writes.

    Only safe where the session touched chunks no other worker writes, which
    holds for every writer here: workers take disjoint timesteps, and every
    array, the ingest flags included, is chunked one timestep per chunk. A
    same-chunk conflict cannot be rebased away and ends in `RebaseFailedError`.
    """
    for attempt in range(attempts):
        try:
            return session.commit(message, rebase_with=icechunk.ConflictDetector())
        except _RACE_ERRORS:
            if attempt == attempts - 1:
                raise
            time.sleep(min(2**attempt, 30) * (0.5 + random.random()))
    raise RuntimeError("unreachable")


def hourly_axis(*times: Iterable[pd.Timestamp]) -> pd.DatetimeIndex:
    """Regular hourly axis spanning every timestamp in `times`."""
    stamps = [pd.Timestamp(t) for group in times for t in group if t is not None]
    if not stamps:
        raise ValueError("no timestamps to build a time axis from")
    return pd.date_range(min(stamps).floor("h"), max(stamps).ceil("h"), freq="h")


def extend_time_axis(repo, new_times: pd.DatetimeIndex, attempts: int = 5) -> bool:
    """Grow a store's time axis to `new_times`, moving existing chunks to match.

    `new_times` must contain every time already in the store. Arrays are resized
    along ``time`` and their chunks remapped with ``Session.reindex_array``, so
    this is a manifest rewrite: no chunk is read or copied, however much data
    the store holds. Positions that no existing chunk maps onto are cleared, so
    newly added timesteps read back as fill values (NaN, flags false).

    Every array with a ``time`` dimension must be chunked one timestep per
    chunk, except the ``time`` coordinate itself, which is rewritten whole.

    Committed without rebasing: a concurrent chunk write rebased over the move
    would land at its old position, which now belongs to a different time. On a
    race the whole extension is redone against the new snapshot instead.

    Returns whether anything changed.
    """
    new_times = pd.DatetimeIndex(new_times)
    if not (new_times.is_monotonic_increasing and new_times.is_unique):
        raise ValueError("the new time axis must be sorted and unique")

    for attempt in range(attempts):
        session = repo.writable_session("main")
        old_times = pd.DatetimeIndex(
            xr.open_zarr(session.store, consolidated=False)["time"].values
        )
        if old_times.equals(new_times):
            return False
        dropped = old_times.difference(new_times)
        if len(dropped):
            raise ValueError(
                f"the new time axis drops {len(dropped)} existing time(s), e.g. {dropped[0]}"
            )

        forward_map = new_times.get_indexer(old_times)
        backward_map = {int(new): old for old, new in enumerate(forward_map)}
        # A pure append leaves every existing chunk where it is.
        moved = not np.array_equal(forward_map, np.arange(len(old_times)))

        group = zarr.open_group(session.store, mode="r+", zarr_format=3)
        time_array = group["time"]
        encoded, _, _ = xr.coding.times.encode_cf_datetime(
            new_times.values,
            units=time_array.attrs["units"],
            calendar=time_array.attrs.get("calendar", "standard"),
        )

        for path, array in group.arrays():
            dims = tuple(array.metadata.dimension_names or ())
            if "time" not in dims:
                continue
            axis = dims.index("time")
            shape = list(array.shape)
            shape[axis] = len(new_times)
            if path == "time":
                array.resize(tuple(shape))
                array[:] = encoded.astype(array.dtype)
                continue
            if array.chunks[axis] != 1:
                raise ValueError(
                    f"{path} is chunked {array.chunks[axis]} timesteps per chunk; "
                    "extending the axis needs one per chunk"
                )
            array.resize(tuple(shape))
            if not moved:
                continue

            def forward(coord, axis=axis):
                coord = list(coord)
                coord[axis] = int(forward_map[coord[axis]])
                return coord

            def backward(coord, axis=axis):
                coord = list(coord)
                source = backward_map.get(coord[axis])
                if source is None:
                    return None
                coord[axis] = source
                return coord

            session.reindex_array(f"/{path}", forward, backward)

        try:
            session.commit(
                f"extend time axis from {len(old_times)} to {len(new_times)} timesteps "
                f"({new_times[0]} to {new_times[-1]})"
            )
        except _RACE_ERRORS:
            if attempt == attempts - 1:
                raise
            time.sleep(min(2**attempt, 30) * (0.5 + random.random()))
            continue
        logger.info(
            f"Extended time axis from {len(old_times)} to {len(new_times)} timesteps"
            f"{', moving existing chunks' if moved else ''}"
        )
        return True
    raise RuntimeError("unreachable")


def stage_timestep(rows: pd.DataFrame, directory: str | pathlib.Path) -> pd.DataFrame:
    """Copy a timestep's messages to `directory` in one sequential pass per file.

    A timestep's messages sit in one contiguous block of each source file
    (measured: 100% dense), but the files interleave variables level by level,
    so pulling out one variable at a time crosses the block once per variable
    with a seek per message. On the busy spinning disks that measured 0.35-1s a
    message -- about 30 of the 60 minutes a timestep took. Here each block is
    read once, front to back, and every message written to a per-block file:
    one per model level or wave variable, one per spectra direction. Messages
    in the block that are not wanted (excluded variables) are read through
    rather than seeked past, so the read stays sequential.

    Returns `rows` pointing at the staged files, each of which holds exactly
    one block, so `_load_fields` decodes it in place.
    """
    directory = pathlib.Path(directory)
    rows = rows.sort_values(["path", "offset"]).reset_index(drop=True)
    keys = [
        f"{SPECTRA_VAR}_{direction}" if group == GROUP_SPECTRA else name
        for group, name, direction in zip(rows["group"], rows["short_name"], rows["direction"])
    ]
    offsets = np.zeros(len(rows), dtype=np.int64)
    written: dict[str, int] = {}
    handles: dict[str, Any] = {}
    skip_limit = 256 << 20  # beyond this a gap is seeked over rather than read
    try:
        for path, block in rows.groupby("path", sort=False):
            with open(_physical(path), "rb", buffering=0) as src:
                position = int(block["offset"].iloc[0])
                src.seek(position)
                for i, offset, length in zip(block.index, block["offset"], block["length"]):
                    gap = int(offset) - position
                    if gap < 0:
                        raise ValueError(f"overlapping GRIB messages in {path} at {offset}")
                    if gap > skip_limit:
                        src.seek(offset)
                    else:
                        while gap:
                            gap -= len(src.read(min(gap, 64 << 20)))
                    data = src.read(int(length))
                    if len(data) != length:
                        raise ValueError(f"{path} ends inside the message at {offset}")
                    position = int(offset) + int(length)
                    key = keys[i]
                    if key not in handles:
                        handles[key] = open(directory / f"{key}.grib", "wb")
                        written[key] = 0
                    handles[key].write(data)
                    offsets[i] = written[key]
                    written[key] += int(length)
    finally:
        for handle in handles.values():
            handle.close()
    staged = rows.copy()
    staged["path"] = [str(directory / f"{key}.grib") for key in keys]
    staged["offset"] = offsets
    return staged


def _delayed_block(
    rows: pd.DataFrame,
    shape: tuple[int, ...],
    mode: str,
    accuracy: int,
    workdir: str | None = None,
    mir_processes: int = 1,
) -> da.Array:
    """Wrap `_load_fields` as a single-chunk dask array.

    One dask chunk per variable block keeps the MIR batch large; the store's
    chunks are smaller and divide it evenly.
    """
    refs = list(zip(rows["path"], rows["offset"], rows["length"]))
    spectral = bool((rows["grid_type"] == "sh").any())
    task = dask.delayed(_load_fields, pure=True)(
        refs, shape, mode, spectral, accuracy, workdir, mir_processes
    )
    return da.from_delayed(task, shape=shape, dtype=np.float32)


class MARSIcechunkProvider(BaseProvider):
    """Combine the `mars.py` GRIB output into one Icechunk store on O1280.

    The store is written region by region against a pre-allocated time axis
    rather than appended to, because a single timestep spans three families of
    source files that are retrieved independently. Completeness is tracked per
    timestep and family by the ``ingested_ml`` / ``ingested_wave`` /
    ``ingested_spectra`` flags, so a run can be resumed or re-run safely.
    """

    name = "ecmwf_mars"
    append_dim = "time"
    store_prefix = STORE_PREFIX

    def __init__(
        self,
        source_dir: str | pathlib.Path | None = None,
        store_prefix: str | None = None,
        index_path: str | pathlib.Path | None = None,
        pattern: str = "output_*.grib",
        accuracy: int = 24,
        scan_workers: int | None = None,
        keepbits: dict[str, int] | None = None,
        clevel: int = 5,
        exclude: Iterable[str] = EXCLUDED_SHORT_NAMES,
        aws_profile: str | None = None,
        staging_dir: str | pathlib.Path | None = None,
        mir_processes: int | None = None,
        config: Config | None = None,
    ):
        """Configure the combine.

        Args:
            source_dir: Directory holding the ``output_*.grib`` files. Defaults
                to `default_source_dir`.
            store_prefix: Location of the store relative to the configured
                bucket, e.g. ``bkr/ifs/ifs_native.icechunk``. Resolved to S3 or
                to a local directory by the configuration; defaults to
                `STORE_PREFIX`.
            index_path: Where to cache the GRIB message index. Defaults to
                ``<source_dir>/mars_grib_index.parquet``.
            pattern: Glob for the source files. Partial downloads written by
                `mars.py` end in ``.tmp`` and are excluded by this default.
            accuracy: Bits per value MIR encodes transformed spectral fields
                with, before they are decoded to float32.
            scan_workers: Processes used to build the message index.
            keepbits: Per-variable overrides for the BitRound filter, merged
                over `KEEPBITS`. Pass ``{}`` to keep the defaults, or set a
                variable to 23 to store it losslessly.
            clevel: Blosc/zstd compression level. 5 is the measured sweet spot:
                level 9 is roughly 14 times slower for 15% less on disk, which
                does not pay at this volume.
            exclude: Short names to drop from the store. Add `REDUNDANT_WITH_UV`
                to drop vorticity and divergence, which carry no information
                beyond u and v.
            aws_profile: Named profile to take S3 credentials from, overriding
                ``AWS_PROFILE``. ``Config.icechunk_storage`` exports it and
                lets the AWS SDK resolve it, which is how writing to
                source.coop is authorised.
            staging_dir: Fast local disk to stage each timestep's messages on
                (see `stage_timestep`); about 10GB per timestep being
                processed. Defaults to `default_staging_dir`.
            mir_processes: MIR processes per spectral variable; see
                `_to_gridpoint_parallel`. Defaults to `DEFAULT_MIR_PROCESSES`.
            config: Configuration to resolve paths and credentials against.
                Defaults to the process-wide `get_config`.
        """
        config = config or get_config()
        if aws_profile is not None and aws_profile != config.credentials.aws_profile:
            config = dataclasses.replace(
                config,
                credentials=dataclasses.replace(config.credentials, aws_profile=aws_profile),
            )
        super().__init__(config)
        self.store_prefix = store_prefix or STORE_PREFIX
        self.source_dir = pathlib.Path(source_dir or default_source_dir(config))
        self.index_path = pathlib.Path(index_path or self.source_dir / "mars_grib_index.parquet")
        self.pattern = pattern
        self.accuracy = accuracy
        self.staging_dir = pathlib.Path(staging_dir or default_staging_dir(config))
        self.mir_processes = mir_processes or DEFAULT_MIR_PROCESSES
        self.scan_workers = scan_workers or min(8, os.cpu_count() or 1)
        self.keepbits = {**KEEPBITS, **(keepbits or {})}
        self.exclude = frozenset(exclude)
        # Byte shuffle, not bit shuffle: bit rounding zeroes whole low mantissa
        # bytes, which byte shuffle gathers into long runs. Measured on a real
        # temperature field, byte shuffle compresses 12.5x against bit
        # shuffle's 7.0x at the same level.
        self.compressor = zarr.codecs.BloscCodec(cname="zstd", clevel=clevel, shuffle="shuffle")
        self._index: pd.DataFrame | None = None
        #: Cache for `expected_names`; invalidated wherever `_index` is.
        self._expected_names: dict[str, set[str]] | None = None

    @property
    def aws_profile(self) -> str | None:
        """The profile S3 credentials are taken from, for logging."""
        return self.config.credentials.aws_profile

    @property
    def region(self) -> str:
        return self.config.region

    # `icechunk_path` is the resolved location of the store, for logs, error
    # messages and the ``source`` attribute. Opening it is `BaseProvider`'s job:
    # `Config.icechunk_storage` handles the profile, the static keys and the
    # local-filesystem redirect, so this class no longer carries its own copy.
    @property
    def icechunk_path(self) -> str:
        return self.config.store_path(self.store_prefix)

    def keepbits_for(self, name: str, group: str) -> int:
        """Mantissa bits retained for `name`, falling back on its family."""
        if name in self.keepbits:
            return self.keepbits[name]
        return WAVE_KEEPBITS if group == GROUP_WAVE else DEFAULT_KEEPBITS

    # ------------------------------------------------------------------
    # Source index
    # ------------------------------------------------------------------

    def source_files(self) -> List[pathlib.Path]:
        """Source GRIB files matching the configured pattern.

        Resolved to absolute paths, because the parquet cache keys on the path
        string: a relative source_dir in one run and an absolute one in the
        next would otherwise look like different files and index everything a
        second time.
        """
        return sorted(p.resolve() for p in self.source_dir.glob(self.pattern))

    def build_index(self, force: bool = False, scan: bool = True) -> pd.DataFrame:
        """Index every message of every source file, caching to parquet.

        Files already present in the cache are not rescanned, so the index can
        be extended cheaply as further MARS retrievals land. With `scan`
        false, only the cache is read: that is what ingest workers want while
        a separate `build_index` is still scanning, rather than each of them
        setting off to scan every remaining file itself.
        """
        cached = pd.DataFrame(columns=_INDEX_COLUMNS)
        if self.index_path.exists() and not force:
            cached = pd.read_parquet(self.index_path)

        known = set(cached["path"].unique())
        if not scan:
            logger.info(f"Using the cached index only: {len(known)} file(s), {len(cached):,} messages")
        # In date order across families, so that complete timesteps are
        # indexed -- and can be ingested -- long before the scan finishes.
        todo = sorted(
            (p for p in self.source_files() if scan and str(p) not in known), key=_date_order
        )
        if todo:
            # The scan is seek-bound: on spinning disks it runs at a couple of
            # hundred messages a second, so a year of retrievals takes hours.
            # The cache is saved as each file completes, so an interrupted scan
            # resumes where it stopped instead of starting over.
            logger.info(f"Scanning {len(todo)} GRIB file(s) with {self.scan_workers} worker(s)")
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.scan_workers) as pool:
                futures = [pool.submit(scan_grib, str(p)) for p in todo]
                for done, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    scanned = future.result()
                    if len(scanned):
                        cached = pd.concat(
                            [f for f in (cached, scanned) if len(f)], ignore_index=True
                        )
                    # Write-then-rename, so a crash mid-write cannot corrupt the cache.
                    partial = self.index_path.with_name(self.index_path.name + ".tmp")
                    cached.to_parquet(partial, index=False)
                    partial.replace(self.index_path)
                    logger.info(
                        f"Index: {done}/{len(todo)} files scanned, {len(cached):,} messages"
                    )

        cached = cached[~cached["short_name"].isin(self.exclude)].reset_index(drop=True)
        self._index = cached
        # `expected_names` is derived from the index; a rescan can widen it.
        self._expected_names = None
        return cached

    @property
    def index(self) -> pd.DataFrame:
        """The GRIB message index, built on first access."""
        if self._index is None:
            self.build_index()
        return self._index

    def times(self) -> pd.DatetimeIndex:
        """Valid times present in the source files, sorted and deduplicated.

        These are the times there is something to ingest. The store's own axis
        is `time_axis`, which is regular and can run wider.
        """
        return pd.DatetimeIndex(sorted(self.index["valid_time"].unique()))

    def time_axis(
        self,
        existing: pd.DatetimeIndex | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> pd.DatetimeIndex:
        """The hourly store axis: the source times, any `existing` axis, and `start`/`end`.

        Padding with `end` lets the axis cover data still being retrieved, so
        it only has to be extended once rather than every time a file lands.
        """
        existing = existing if existing is not None else pd.DatetimeIndex([])
        return hourly_axis(self.times(), existing, [start, end])

    def ensure_time_axis(self, repo=None, start=None, end=None) -> pd.DatetimeIndex:
        """Create the store, or extend its axis to cover the source files.

        Must not run alongside ingest workers; see `extend_time_axis`.
        """
        repo = repo or self.get_icechunk_repo()
        try:
            existing = self.store_times(repo)
        except Exception:
            self.initialize_store(repo, times=self.time_axis(start=start, end=end))
            return self.store_times(repo)
        extend_time_axis(repo, self.time_axis(existing, start, end))
        return self.store_times(repo)

    def select(self, group: str, it: pd.Timestamp) -> pd.DataFrame:
        """Messages for one family at one valid time, preferring analyses.

        The 2D spectra forecast steps 1-5 from the 00Z run overlap the 03Z
        spectra analysis, so one valid time can be covered twice. Analyses win.
        """
        rows = self.index[(self.index["group"] == group) & (self.index["valid_time"] == it)].copy()
        if rows.empty:
            return rows
        rows["_pref"] = (rows["data_type"] != "an").astype(int)
        keys = {
            GROUP_ML: ["short_name", "level"],
            GROUP_WAVE: ["short_name"],
            GROUP_SPECTRA: ["direction", "frequency"],
        }[group]
        rows = rows.sort_values(["_pref", *keys]).drop_duplicates(keys, keep="first")
        return rows.drop(columns="_pref")

    def ml_is_complete(self, it: pd.Timestamp) -> bool:
        """Whether every model-level variable at `it` has all 137 levels.

        A truncated source file yields a timestep where some variables stop
        part way up the column. The data that is there is valid and still gets
        written, but the timestep must not be flagged ingested, or a re-run
        after re-downloading would skip it and leave the gap as NaN forever.

        The variables *expected* are checked, not only the ones that turned up.
        A truncation that loses a variable outright leaves it out of the index
        entirely, so counting what is present called such a timestep complete,
        deleted its source file, and then refused to fetch it again.
        """
        rows = self.select(GROUP_ML, it)
        if rows.empty:
            return False
        counts = rows.groupby("short_name")["level"].nunique()
        expected_names = self.expected_names(GROUP_ML)
        absent = expected_names - set(counts.index)
        if absent:
            logger.warning(f"{it}: model-level variable(s) missing entirely: {sorted(absent)[:5]}")
            return False
        return all(
            counts[name] == (1 if name in ML_SURFACE_SHORT_NAMES else N_MODEL_LEVELS)
            for name in expected_names
        )

    def expected_names(self, group: str) -> set[str]:
        """The variable names a complete timestep of ``group`` carries.

        Taken from the store's schema, which :meth:`initialize_store` builds from
        :meth:`variables` over the whole index, so it is the same set for every
        timestep and does not shrink because one source file came up short.
        Cached: it is consulted once per family per timestep and the index runs
        to millions of rows.
        """
        if getattr(self, "_expected_names", None) is None:
            names = self.variables()
            self._expected_names = {
                GROUP_ML: set(names["ml_level"]) | set(names["ml_surface"]),
                GROUP_WAVE: set(names["wave"]),
                GROUP_SPECTRA: set(names["spectra"]),
            }
        return self._expected_names[group]

    def variables(self) -> dict[str, List[str]]:
        """Variable names per family, discovered from the index.

        Names must not repeat across families, since each becomes one array in
        the store and `write_to_icechunk` attributes a written variable back to
        its family to set the ingest flags.
        """
        ml = set(self.index[self.index["group"] == GROUP_ML]["short_name"].unique())
        wave = set(self.index[self.index["group"] == GROUP_WAVE]["short_name"].unique())
        clash = (ml & wave) | ({SPECTRA_VAR} & (ml | wave))
        if clash:
            raise ValueError(f"variable name(s) used by more than one family: {sorted(clash)}")
        return {
            "ml_level": sorted(ml - ML_SURFACE_SHORT_NAMES),
            "ml_surface": sorted(ml & ML_SURFACE_SHORT_NAMES),
            "wave": sorted(wave),
            "spectra": [SPECTRA_VAR] if (self.index["group"] == GROUP_SPECTRA).any() else [],
        }

    # ------------------------------------------------------------------
    # Store
    # ------------------------------------------------------------------

    def initialize_store(self, repo=None, times: pd.DatetimeIndex | None = None) -> None:
        """Create the store schema, pre-allocating the full time axis.

        Only metadata is written; every chunk is left unwritten and therefore
        reads back as NaN until a region write fills it.
        """
        repo = repo or self.get_icechunk_repo()
        session = repo.writable_session("main")
        times = times if times is not None else self.time_axis()
        variables = self.variables()
        grid = read_grid(self.index.iloc[0]["path"])

        coords = {
            "time": times,
            "level": np.arange(1, N_MODEL_LEVELS + 1, dtype=np.int32),
            "latitude": ("values", grid["latitude"]),
            "longitude": ("values", grid["longitude"]),
        }
        data_vars: dict = {"pl": ("row", grid["pl"])}
        model_level = self.index[self.index["group"] == GROUP_ML]
        if not model_level.empty:
            hybrid_a, hybrid_b = read_hybrid_coefficients(model_level.iloc[0]["path"])
            data_vars["hybrid_a"] = ("halflevel", hybrid_a)
            data_vars["hybrid_b"] = ("halflevel", hybrid_b)
        encoding: dict = {
            "time": {"units": "seconds since 1970-01-01", "calendar": "standard", "dtype": "int64"}
        }
        for group in GROUPS:
            data_vars[_INGESTED[group]] = ("time", np.zeros(len(times), dtype=bool))
            # One timestep per chunk. Left as a single chunk these become a
            # hotspot every parallel worker writes to, and a same-chunk
            # conflict is the one case icechunk cannot rebase away.
            encoding[_INGESTED[group]] = {"chunks": (1,)}

        var_attrs: dict[str, dict] = {}

        def _placeholder(
            name: str, group: str, dims: tuple[str, ...], shape: tuple[int, ...], chunks
        ):
            keepbits = self.keepbits_for(name, group)
            lossless = keepbits >= LOSSLESS_KEEPBITS
            data_vars[name] = (dims, da.zeros(shape, chunks=chunks, dtype=np.float32))
            encoding[name] = {
                # BitRound is an array-to-array codec, so it is a filter and
                # runs before the compressor sees the buffer. Skipped entirely
                # when the variable is stored at full float32 precision, rather
                # than attaching a codec that would do nothing.
                "filters": [] if lossless else [BitRound(keepbits=keepbits)],
                "compressors": [self.compressor],
                "chunks": chunks,
                "_FillValue": np.float32(np.nan),
            }
            var_attrs[name] = (
                {"bitround_keepbits": "none", "precision": "full float32"}
                if lossless
                else {
                    "bitround_keepbits": keepbits,
                    "bitround_max_relative_error": float(2.0 ** -(keepbits + 1)),
                }
            )

        n_time = len(times)
        for name in variables["ml_level"]:
            _placeholder(
                name,
                GROUP_ML,
                ("time", "level", "values"),
                (n_time, N_MODEL_LEVELS, N_VALUES),
                (1, 1, N_VALUES),
            )
        for name in variables["ml_surface"]:
            _placeholder(name, GROUP_ML, ("time", "values"), (n_time, N_VALUES), (1, N_VALUES))
        for name in variables["wave"]:
            _placeholder(name, GROUP_WAVE, ("time", "values"), (n_time, N_VALUES), (1, N_VALUES))
        if variables["spectra"]:
            directions, frequencies = read_spectra_bins(
                self.index[self.index["group"] == GROUP_SPECTRA].iloc[0]["path"]
            )
            coords["direction"] = directions
            coords["frequency"] = frequencies
            _placeholder(
                SPECTRA_VAR,
                GROUP_SPECTRA,
                ("time", "direction", "frequency", "values"),
                (n_time, N_DIRECTIONS, N_FREQUENCIES, N_VALUES),
                (1, 1, 1, N_VALUES),
            )

        template = xr.Dataset(data_vars, coords=coords, attrs=self._attrs())
        for name, attrs in var_attrs.items():
            template[name].attrs.update(attrs)
        template["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
        template["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
        template["level"].attrs = {"long_name": "model level number"}
        template["pl"].attrs = {"long_name": "number of grid points per latitude row"}
        if "hybrid_a" in template:
            template["hybrid_a"].attrs = {"long_name": "hybrid level A coefficient", "units": "Pa"}
            template["hybrid_b"].attrs = {"long_name": "hybrid level B coefficient", "units": "1"}
        if variables["spectra"]:
            template["direction"].attrs = {"long_name": "wave direction bin", "units": "degrees"}
            template["frequency"].attrs = {"long_name": "wave frequency bin", "units": "s-1"}
            template[SPECTRA_VAR].attrs.update(
                {
                    "long_name": "2D wave spectra, log10 of spectral density",
                    "units": "log10(m2 s radian-1)",
                }
            )

        template.to_zarr(
            session.store, compute=False, encoding=encoding, zarr_format=3, consolidated=False
        )
        session.commit(f"initialise {self.name} store with {n_time} timesteps")
        logger.info(
            f"Initialised {self.icechunk_path}: {n_time} timesteps, "
            f"{len(variables['ml_level'])} model-level vars, "
            f"{len(variables['wave'])} wave vars, spectra={bool(variables['spectra'])}"
        )

    def _attrs(self) -> dict:
        return {
            "title": "ECMWF operational analyses and short-range forecasts on model levels",
            "source": "ECMWF MARS (class=od, stream=oper/wave, expver=1)",
            "grid": NATIVE_GRID,
            "grid_description": (
                "Octahedral reduced Gaussian grid, N=1280, 2560 latitude rows, "
                "6599680 points. The horizontal dimension is the flat 'values' "
                "dimension; use latitude/longitude or pl to reconstruct the grid."
            ),
            "spectral_fields": (
                "etadot, z, t, w, vo, lnsp, d, u and v are archived as T1279 "
                f"spherical harmonics and were transformed to {NATIVE_GRID} with "
                f"MIR at {self.accuracy} bits per value. Gridpoint fields are "
                "unmodified."
            ),
            "compression": (
                "Lossy. Each variable is bit-rounded to a per-variable number of "
                "retained mantissa bits before Blosc/zstd, bounding its relative "
                "error at 2 ** -(keepbits + 1). See the bitround_keepbits and "
                "bitround_max_relative_error attributes on each variable."
            ),
            "Conventions": "CF-1.8",
        }

    def store_times(self, repo=None) -> pd.DatetimeIndex:
        """The time axis the store was initialised with."""
        repo = repo or self.get_icechunk_repo()
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        return pd.DatetimeIndex(ds["time"].values)

    def pending_groups(self, desired_timestamps: Iterable[pd.Timestamp]) -> dict:
        """Families still to ingest at each timestep, omitting finished timesteps.

        Completeness is judged against what the source files actually offer,
        not against all three families: the wave analyses are 3-hourly, so
        01Z, 02Z, 04Z and so on will never have a wave field. Demanding all
        three would leave those timesteps permanently "missing" and re-ingest
        them on every run.

        Tracking families separately matters because they arrive separately:
        `mars.py` fetches the analyses first and the wave files later, and
        redoing a timestep's model levels because its wave field has since
        landed would repeat by far the most expensive part of the ingest.
        """
        desired = [pd.Timestamp(it) for it in desired_timestamps]
        pairs = self.index[["group", "valid_time"]].drop_duplicates()
        available = {
            (group, pd.Timestamp(t)) for group, t in zip(pairs["group"], pairs["valid_time"])
        }
        try:
            repo = self.get_icechunk_repo()
            ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
            flags = {
                group: pd.Series(
                    ds[_INGESTED[group]].values.astype(bool),
                    index=pd.DatetimeIndex(ds["time"].values),
                )
                for group in GROUPS
                if _INGESTED[group] in ds
            }
        except Exception:
            flags = {}

        pending = {}
        for it in desired:
            todo = [
                g
                for g in GROUPS
                if (g, it) in available and not (g in flags and bool(flags[g].get(it, False)))
            ]
            if todo:
                pending[it] = todo
        return pending

    def missing_timesteps(self, desired_timestamps: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Timesteps with at least one family not yet ingested; see `pending_groups`."""
        return list(self.pending_groups(desired_timestamps))

    # ------------------------------------------------------------------
    # Provider interface
    # ------------------------------------------------------------------

    def fetch(self, it: pd.Timestamp, temp_dir=None, **kwargs) -> List[str]:
        """Source files contributing to `it`. The GRIBs are already local."""
        rows = self.index[self.index["valid_time"] == it]
        return sorted(rows["path"].unique())

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        groups: Iterable[str] = GROUPS,
        **kwargs,
    ) -> xr.Dataset:
        """Build one timestep as a lazy Dataset, one dask task per block.

        Each task decodes a whole variable at once -- all 137 levels of a model
        level variable, or all 29 frequencies of one spectra direction -- so
        that MIR gets a large fieldset and the peak working set stays at a
        single block (about 3.6GB for a model level variable).

        Only the families in `groups` are built, so a timestep can be finished
        off without redoing the families already ingested.
        """
        data_vars: dict = {}
        groups = set(groups)
        empty = self.index.iloc[0:0]
        wanted = [self.select(g, it) for g in GROUPS if g in groups]
        wanted = pd.concat([w for w in wanted if len(w)] or [empty], ignore_index=True)

        # Stage the timestep's messages with one sequential read per file; the
        # directory is removed by write_to_icechunk once the timestep is written.
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        stage = tempfile.mkdtemp(prefix=f"mars_stage_{it:%Y%m%dT%H}_", dir=self.staging_dir)
        try:
            staged = stage_timestep(wanted, stage) if len(wanted) else wanted
        except BaseException:
            shutil.rmtree(stage, ignore_errors=True)
            raise

        ml = staged[staged["group"] == GROUP_ML]
        for name, rows in ml.groupby("short_name", sort=True):
            if name in ML_SURFACE_SHORT_NAMES:
                data_vars[name] = (
                    ("time", "values"),
                    _delayed_block(rows, (N_VALUES,), "single", self.accuracy, stage, self.mir_processes)[None, :],
                )
            else:
                data_vars[name] = (
                    ("time", "level", "values"),
                    _delayed_block(rows, (N_MODEL_LEVELS, N_VALUES), "level", self.accuracy, stage, self.mir_processes)[
                        None, :, :
                    ],
                )

        wave = staged[staged["group"] == GROUP_WAVE]
        for name, rows in wave.groupby("short_name", sort=True):
            data_vars[name] = (
                ("time", "values"),
                _delayed_block(rows, (N_VALUES,), "single", self.accuracy, stage, self.mir_processes)[None, :],
            )

        spectra = staged[staged["group"] == GROUP_SPECTRA]
        if not spectra.empty:
            groups = list(spectra.groupby("direction", sort=True))
            # Directions are written by *position*: `write_to_icechunk` puts
            # block k at ``direction=k``. Frequencies and model levels are
            # placed by the key read back from the GRIB (`_load_fields`), but
            # there is no such key here, so a direction missing from a
            # truncated file would silently shift every later direction into
            # the wrong bin -- and, unlike incomplete model levels, nothing
            # downstream would notice before the timestep was flagged
            # ingested and its GRIB deleted.
            if len(groups) != N_DIRECTIONS:
                present = sorted(int(d) for d, _ in groups)
                missing = sorted(set(range(1, N_DIRECTIONS + 1)) - set(present))
                raise ValueError(
                    f"{it}: expected {N_DIRECTIONS} wave spectra directions, got "
                    f"{len(groups)} (missing {missing}). A source file is truncated; "
                    "re-retrieve it rather than writing the directions out of place."
                )
            blocks = [
                _delayed_block(
                    rows.sort_values("frequency"),
                    (N_FREQUENCIES, N_VALUES),
                    "spectra",
                    self.accuracy,
                    stage,
                    self.mir_processes,
                )[None, :, :]
                for _, rows in groups
            ]
            data_vars[SPECTRA_VAR] = (
                ("time", "direction", "frequency", "values"),
                da.concatenate(blocks, axis=0)[None, ...],
            )

        return xr.Dataset(data_vars, coords={"time": [it]}, attrs={"_staging_dir": stage})

    def write_to_icechunk(
        self, repo, processed: xr.Dataset, regrids: Sequence = ()
    ) -> dict[str, bool]:
        """Region-write one timestep, a variable at a time, in a single commit.

        Returns which families were flagged as ingested.

        `regrids` holds ``(MARSRegridProvider, repo)`` pairs whose stores are
        written from the same decoded blocks, so a regridded store never has to
        read the native store back. Reading and decoding a model level variable
        from S3 is 30-45s -- well over half of what a separate regrid pass
        costs -- against about 4s for the remapping itself. The native store is
        committed first and each target after it, with the same flags; if a
        target's commit fails the native timestep is still flagged, so the
        follow-mode regrid picks it up.

        The spectra are decoded, written and regridded one direction at a
        time (0.8GB) rather than as the whole 27.6GB variable.

        Variables are computed and written one by one rather than as a single
        graph so that peak memory is one block, not one timestep -- a whole
        timestep of model levels is about 50GB. Chunks are uploaded as each
        variable is written, so holding the session open costs no memory.

        Everything, the ingest flags included, goes into one commit. Commits
        cost seconds each, far more than writing a small variable, so a commit
        per variable spent most of a timestep committing; and a single commit
        makes the timestep atomic -- its data and its "done" flag land
        together, and an interrupted timestep leaves nothing behind. Workers
        take disjoint timesteps, and the flags are chunked one per timestep,
        so concurrent commits touch disjoint chunks and rebase cleanly.
        """
        try:
            return self._write_timestep(repo, processed, regrids)
        finally:
            stage = processed.attrs.get("_staging_dir")
            if stage:
                shutil.rmtree(stage, ignore_errors=True)

    def _write_timestep(self, repo, processed: xr.Dataset, regrids: Sequence) -> dict[str, bool]:
        """The body of `write_to_icechunk`."""
        it = pd.Timestamp(processed["time"].values[0])
        time_index = self.store_times(repo)
        position = int(time_index.get_loc(it))

        # Names actually written, per family, rather than a bare "something was written".
        # The ingest flag means "this timestep is finished", and a source file that is
        # short of a whole variable produces a timestep that writes some of its family and
        # none of the rest; flagging that would delete the file and refuse the re-download.
        written_names: dict[str, set[str]] = {group: set() for group in GROUPS}
        ml_names = set(self.select(GROUP_ML, it)["short_name"])
        wave_names = set(self.select(GROUP_WAVE, it)["short_name"])

        session = repo.writable_session("main")
        targets = []
        if regrids:
            grid = xr.open_zarr(session.store, consolidated=False)
            for provider, target_repo in regrids:
                target_session = target_repo.writable_session("main")
                target = xr.open_zarr(target_session.store, consolidated=False)
                targets.append(
                    {
                        "provider": provider,
                        "session": target_session,
                        "weights": provider.weights(grid),
                        "position": int(pd.DatetimeIndex(target["time"].values).get_loc(it)),
                        "names": set(target.data_vars),
                    }
                )

        for name, array in processed.data_vars.items():
            # The spectra go one direction at a time: slicing the lazy array
            # computes only that direction's block.
            if name == SPECTRA_VAR:
                pieces = [
                    ({"direction": slice(k, k + 1)}, array.isel(direction=slice(k, k + 1)))
                    for k in range(array.sizes["direction"])
                ]
            else:
                pieces = [({}, array)]
            for sel, piece in pieces:
                values = piece.compute(scheduler="synchronous").values
                region = {"time": slice(position, position + 1)}
                for dim, size in zip(piece.dims[1:], piece.shape[1:]):
                    region[dim] = sel.get(dim, slice(0, size))
                to_icechunk(xr.Dataset({name: (piece.dims, values)}), session, region=region)

                for t in targets:
                    if name not in t["names"]:
                        continue
                    provider = t["provider"]
                    out = regrid_block(t["weights"], values, provider.lats.size, provider.lons.size)
                    target_region = dict(region)
                    target_region["time"] = slice(t["position"], t["position"] + 1)
                    del target_region["values"]
                    target_region["latitude"] = slice(0, provider.lats.size)
                    target_region["longitude"] = slice(0, provider.lons.size)
                    dims = (*piece.dims[:-1], "latitude", "longitude")
                    to_icechunk(xr.Dataset({name: (dims, out)}), t["session"], region=target_region)
                    del out
                del values
            logger.debug(f"Wrote {name} for {it}")

            if name == SPECTRA_VAR:
                written_names[GROUP_SPECTRA].add(str(name))
            elif name in wave_names and name not in ml_names:
                written_names[GROUP_WAVE].add(str(name))
            elif name in ml_names:
                written_names[GROUP_ML].add(str(name))

        written = {group: False for group in GROUPS}
        for group, names in written_names.items():
            if not names:
                continue
            absent = self.expected_names(group) - names
            if absent:
                logger.warning(
                    f"{it}: {group} is short of {len(absent)} variable(s) "
                    f"({sorted(absent)[:5]}); a source file is truncated. Data written, "
                    "but left unflagged so a re-run picks it up."
                )
                continue
            written[group] = True

        if written[GROUP_ML] and not self.ml_is_complete(it):
            logger.warning(
                f"{it}: model levels are incomplete (a source file is truncated). "
                "Data written, but left unflagged so a re-run picks it up."
            )
            written[GROUP_ML] = False

        flags = xr.Dataset(
            {
                _INGESTED[group]: (("time",), np.array([True]))
                for group, done in written.items()
                if done
            }
        )
        if flags.data_vars:
            to_icechunk(flags, session, region={"time": slice(position, position + 1)})
        done = sorted(g for g, d in written.items() if d) or "incomplete"
        if session.has_uncommitted_changes:
            _commit(session, f"ingest {it}: {done}")

        for t in targets:
            if flags.data_vars:
                to_icechunk(
                    flags, t["session"], region={"time": slice(t["position"], t["position"] + 1)}
                )
            if not t["session"].has_uncommitted_changes:
                continue
            try:
                _commit(t["session"], f"regrid {it} at ingest: {done}")
            except _RACE_ERRORS:
                # A follow-mode regrid got there first; its result is the same.
                logger.warning(
                    f"{t['provider'].icechunk_path}: commit for {it} lost a race; "
                    "leaving it to the follow-mode regrid"
                )
        return written

    def run(
        self,
        times: Iterable[pd.Timestamp] | None = None,
        skip_complete: bool = True,
        regrids: Sequence[MARSRegridProvider] = (),
        scan: bool = True,
    ) -> None:
        """Ingest every timestep, initialising the store on first use.

        An existing store whose axis does not cover `times` is an error rather
        than being extended here, because extending moves chunks and is unsafe
        while other workers write; run `ensure_time_axis` (``--init-only``)
        first. The same holds for each of `regrids`, the regridded stores to
        write alongside; see `write_to_icechunk`. `scan` is passed to
        `build_index`.
        """
        self.build_index(scan=scan)
        repo = self.get_icechunk_repo()
        try:
            axis = self.store_times(repo)
        except Exception:
            self.initialize_store(repo)
            axis = self.store_times(repo)

        wanted = pd.DatetimeIndex(times) if times is not None else self.times()
        outside = wanted.difference(axis)
        if len(outside):
            raise ValueError(
                f"{len(outside)} timestep(s) fall outside the store's time axis "
                f"({axis[0]} to {axis[-1]}), e.g. {outside[0]}. Extend it first with --init-only."
            )
        target_repos = [(t, _open_regrid_target(t, wanted)) for t in regrids]
        if skip_complete:
            pending = self.pending_groups(wanted)
        else:
            pending = {
                it: [g for g in GROUPS if not self.select(g, it).empty] for it in wanted
            }
        logger.info(f"{len(pending)} of {len(wanted)} timesteps to ingest")
        for it, groups in pending.items():
            if skip_complete:
                # Re-read the flags: another worker may have done this since
                # the list was drawn up, and a timestep costs many minutes.
                groups = self.pending_groups([it]).get(it, [])
                if not groups:
                    continue
            input_files = self.fetch(it)
            if not input_files:
                logger.warning(f"No source messages for {it}, skipping")
                continue
            logger.info(f"Ingesting {it} ({', '.join(groups)}) from {len(input_files)} file(s)")
            # `process` stages the timestep's messages on disk and
            # `write_to_icechunk` removes them again, so it must be called
            # exactly once per timestep or every extra call leaks a staging
            # directory.
            processed = self.process(input_files, it, groups=groups)
            # A whole timestep of model levels is about 50GB, but it is never
            # resident: `write_to_icechunk` computes and writes one dask chunk
            # -- one variable, or one spectra direction -- at a time, so the
            # working set is the largest chunk. That is what the peak estimate
            # measures, with concurrency 1 because the compute is synchronous.
            require_memory(
                estimate_peak_gb(processed, concurrency=1), what=f"{self.name} {it}"
            )
            logger.debug(
                f"{it}: {estimate_dataset_gb(processed):.1f} GB of fields, "
                "written a chunk at a time"
            )
            # The guard watches the process tree while the timestep runs and
            # aborts the run if the peak stays over budget, rather than
            # letting the ingest grow until the kernel picks a victim.
            with memory_guard(what=f"{self.name} {it}") as usage:
                self.write_to_icechunk(repo, processed, regrids=target_repos)
            logger.debug(f"{it}: peak {usage.peak_gb:.1f} GB")


# ----------------------------------------------------------------------
# Regridding the native store onto a regular latitude/longitude grid
# ----------------------------------------------------------------------


def regrid_store_prefix(native_prefix: str, resolution: float) -> str:
    """Sibling prefix for the regridded store, e.g. ``..._025.icechunk``."""
    tag = f"_{round(resolution * 100):03d}"
    head, sep, tail = native_prefix.rpartition(".icechunk")
    if not sep:
        raise ValueError(f"{native_prefix!r} does not end in .icechunk")
    return f"{head}{tag}.icechunk{tail}"


#: The slab size the regrid was tuned for; see `MARSRegridProvider`.
SLAB_BYTES = 4 << 30


def default_slab_bytes(config: Config | None = None) -> int:
    """How much of one variable a regrid may hold at once.

    `SLAB_BYTES`, unless the memory budget (``MEMORY_CEILING_GB``, or
    ``MEMORY_FRACTION`` of what is actually free) does not stretch to it. A
    slab is copied twice while it is regridded -- the native block and its
    output -- and several resolutions share a process, so a quarter of the
    budget is the most one slab may claim. The floor of 256MiB keeps a slab
    larger than a single target chunk on a small machine, below which the
    streaming loop in `regrid_timestep` would rewrite chunks.
    """
    budget = budget_gb(config) * BYTES_PER_GB
    return int(min(SLAB_BYTES, max(256 << 20, budget // 4)))


def target_grid(resolution: float) -> tuple[np.ndarray, np.ndarray]:
    """Global regular grid, north to south, starting at the Greenwich meridian."""
    n_lat = int(round(180.0 / resolution)) + 1
    n_lon = int(round(360.0 / resolution))
    return (
        np.linspace(90.0, -90.0, n_lat),
        np.arange(n_lon, dtype=np.float64) * resolution,
    )


def build_regrid_weights(
    row_lat: np.ndarray, row_start: np.ndarray, pl: np.ndarray, resolution: float
) -> Any:
    """Bilinear weights from the reduced Gaussian grid onto a regular grid.

    Returns a ``scipy.sparse.csr_matrix``.

    The two axes are handled separately because the source grid is only
    irregular in one of them: latitude rows are unevenly spaced and must be
    searched, but within a row the longitudes are evenly spaced, so bracketing
    one is arithmetic.

    Returns a sparse matrix with four entries per target point, so regridding a
    field is one matrix-vector product. Building it takes well under a second
    and the result depends only on the grids, so it is computed once and reused
    for every field.
    """
    import scipy.sparse as sp  # noqa: PLC0415 -- only needed when regridding

    lats, lons = target_grid(resolution)
    n_target = lats.size * lons.size

    # Bracketing source rows for each target latitude. row_lat runs north to
    # south, so searchsorted needs the negated, ascending form.
    j1 = np.searchsorted(-row_lat, -lats).clip(1, row_lat.size - 1)
    j0 = j1 - 1
    span = row_lat[j0] - row_lat[j1]
    w_lat = np.where(span == 0, 0.0, (row_lat[j0] - lats) / np.where(span == 0, 1.0, span))
    # The target grid reaches the poles exactly but the Gaussian rows stop just
    # short of them (89.946 for O1280), so without clamping the end rows would
    # extrapolate: negative weights, and an overshoot at 90N and 90S. Clamping
    # holds the nearest row instead.
    w_lat = w_lat.clip(0.0, 1.0)

    target_ids = np.arange(n_target).reshape(lats.size, lons.size)
    rows, cols, vals = [], [], []
    for j, w_j in ((j0, 1.0 - w_lat), (j1, w_lat)):
        n = pl[j].astype(np.float64)  # points in each bracketing row
        pos = lons[None, :] / (360.0 / n[:, None])
        k0 = np.floor(pos)
        w_lon = pos - k0
        k0 = np.mod(k0, n[:, None]).astype(np.int64)
        k1 = np.mod(k0 + 1, n[:, None].astype(np.int64))
        for k, w_k in ((k0, 1.0 - w_lon), (k1, w_lon)):
            rows.append(target_ids.ravel())
            cols.append((row_start[j][:, None] + k).ravel())
            vals.append((w_j[:, None] * w_k).ravel())

    return sp.csr_matrix(
        (
            np.concatenate(vals).astype(np.float32),
            (np.concatenate(rows), np.concatenate(cols)),
        ),
        shape=(n_target, int(pl.sum())),
    )


def apply_weights(weights, field: np.ndarray) -> np.ndarray:
    """Regrid one field, renormalising around missing values.

    A plain product would spread NaN into every target cell that touches a
    masked source point, eating one cell into every coastline: measured on a
    real wave field, 45,228 cells of the 0.1 degree grid, 0.7% of the globe.
    Instead the missing points are dropped from the stencil and the remaining
    weights renormalised, which costs a second product. Fields with no missing
    values -- every model level variable -- skip that entirely.
    """
    missing = np.isnan(field)
    if not missing.any():
        return (weights @ field).astype(np.float32)
    covered = weights @ (~missing).astype(np.float32)
    total = weights @ np.where(missing, np.float32(0), field)
    # Require at least half the stencil; below that the target point is
    # genuinely outside the data and stays missing.
    return np.where(covered > 0.5, total / np.maximum(covered, 1e-12), np.nan).astype(np.float32)


def gaussian_row_edges(row_lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """sin(latitude) of the northern and southern edge of each Gaussian row.

    A Gaussian grid's rows are the Gauss-Legendre nodes, and the quadrature
    weight of a row is exactly the area of its latitude band (in units of
    sin(latitude)), so cumulating the weights from the north pole gives band
    edges that partition the sphere and that give every source cell the area
    the grid itself assigns it. The nodes are checked against the stored row
    latitudes; if they disagree (not a Gaussian grid), the edges fall back to
    midpoints between rows.
    """
    nodes, quad = np.polynomial.legendre.leggauss(row_lat.size)
    # leggauss orders south to north; the grid runs north to south.
    if np.allclose(np.rad2deg(np.arcsin(nodes[::-1])), row_lat, rtol=0, atol=1e-6):
        edges = 1.0 - np.concatenate([[0.0], np.cumsum(quad[::-1])])
        edges[-1] = -1.0  # absorb rounding so the bands tile exactly
        return edges[:-1], edges[1:]
    logger.warning("row latitudes are not Gauss-Legendre nodes; using midpoint band edges")
    mid = np.sin(np.deg2rad((row_lat[:-1] + row_lat[1:]) / 2.0))
    return np.concatenate([[1.0], mid]), np.concatenate([mid, [-1.0]])


def build_conservative_weights(
    row_lat: np.ndarray, row_start: np.ndarray, pl: np.ndarray, resolution: float
) -> Any:
    """First-order conservative weights from the reduced Gaussian grid.

    Returns a ``scipy.sparse.csr_matrix`` of the same shape as
    `build_regrid_weights`, so `apply_weights` uses it unchanged.

    Each target value is the area-weighted mean of every source cell its cell
    overlaps, so a coarse cell averages the roughly 200 O1280 points inside it
    instead of sampling the four nearest -- bilinear interpolation to 1 degree
    from a 9km grid is close to point sampling, and aliases small-scale
    structure into the result. The area integral of every field is preserved.

    Source cells are the grid's own: in latitude the Gaussian bands of
    `gaussian_row_edges`, in longitude ``360 / pl`` degrees centred on each
    point. Target cells are ``resolution`` degrees centred on each target
    point, with the pole rows half cells. Both are bounded by meridians and
    parallels, so an overlap's area factorises into a latitude part (difference
    of sin(latitude)) and a longitude part (length of the overlap), and each
    target row sums to one exactly.
    """
    import scipy.sparse as sp  # noqa: PLC0415 -- only needed when regridding

    lats, lons = target_grid(resolution)
    n_lon = lons.size
    src_top, src_bot = gaussian_row_edges(row_lat)
    half = resolution / 2.0
    tgt_top = np.sin(np.deg2rad(np.minimum(lats + half, 90.0)))
    tgt_bot = np.sin(np.deg2rad(np.maximum(lats - half, -90.0)))
    west = lons - half  # target cell k spans [west[k], west[k] + resolution]

    rows, cols, vals = [], [], []
    for i in range(lats.size):
        band = tgt_top[i] - tgt_bot[i]
        for j in np.nonzero((src_bot < tgt_top[i]) & (src_top > tgt_bot[i]))[0]:
            f_lat = (min(src_top[j], tgt_top[i]) - max(src_bot[j], tgt_bot[i])) / band
            if f_lat <= 0.0:
                continue
            n = int(pl[j])
            d = 360.0 / n  # source cell k spans [k*d - d/2, k*d + d/2]
            first = np.floor((west + d / 2.0) / d).astype(np.int64)
            span = int(np.ceil(resolution / d)) + 1
            k = first[:, None] + np.arange(span)[None, :]
            lo = k * d - d / 2.0
            f_lon = (
                np.minimum(west[:, None] + resolution, lo + d) - np.maximum(west[:, None], lo)
            ).clip(min=0.0) / resolution
            keep = f_lon > 0.0
            rows.append(np.broadcast_to((i * n_lon + np.arange(n_lon))[:, None], k.shape)[keep])
            cols.append(row_start[j] + np.mod(k, n)[keep])
            vals.append(f_lat * f_lon[keep])

    # csr_matrix sums duplicate entries, which covers a source row too coarse
    # for its cells to be told apart (never the case for O1280 at 1 degree).
    return sp.csr_matrix(
        (
            np.concatenate(vals).astype(np.float32),
            (np.concatenate(rows), np.concatenate(cols)),
        ),
        shape=(lats.size * n_lon, int(pl.sum())),
    )


#: How each published regridded store is resampled. Both are coarser than
#: O1280's 9km -- a 0.25 degree cell holds about 12 source points and a 1 degree
#: cell about 200 -- so both are conservative: bilinear interpolation samples
#: the nearest four points and aliases the rest of the cell's small-scale
#: structure into the result. Measured on real temperature at 1 degree, it also
#: biases the area mean where conservative remapping preserves it exactly.
REGRID_METHODS: dict[float, str] = {0.25: "conservative", 1.0: "conservative"}

_REGRID_BUILDERS = {"bilinear": build_regrid_weights, "conservative": build_conservative_weights}


class MARSRegridProvider(BaseProvider):
    """Resample the native O1280 store onto a regular latitude/longitude grid.

    Reads the native store rather than the GRIB files, so the expensive
    spherical-harmonic transforms are not repeated, and gates on the native
    store's ``ingested_*`` flags so it can run while the native ingest is still
    going. Unlike the native store, the output has real ``latitude`` and
    ``longitude`` dimensions.
    """

    name = "ecmwf_mars_regrid"
    append_dim = "time"

    def __init__(
        self,
        native: MARSIcechunkProvider,
        resolution: float = 0.25,
        slab_bytes: int | None = None,
        chunk_bytes: int = 64 << 20,
        method: str | None = None,
    ):
        """Target `resolution` degrees, reading from the `native` O1280 store.

        `method` is ``"bilinear"`` or ``"conservative"``, defaulting to the
        published choice for `resolution` in `REGRID_METHODS` (bilinear for
        anything not listed). It is recorded in the store's
        ``regridding_method`` attribute, and `run_regrids` refuses to write to
        a store built with a different method.

        `chunk_bytes` sizes the store's chunks when it is created; see
        `chunks_for`. It has no effect on an existing store.

        `slab_bytes` caps how much of a variable is held at once. Every slab
        costs an icechunk commit, and commits are far more expensive than the
        regrid itself -- slicing model level variables into four slabs measured
        6x slower end to end. The 4GiB default is chosen so a model level
        variable (3.6GB) stays a single slab while the 2D spectra (27.6GB, the
        only variable that would actually exhaust memory) still splits into
        eight. It defaults to `default_slab_bytes`, which is that 4GiB except
        on a host whose memory budget does not stretch to it.
        """
        super().__init__(native.config)
        self.native = native
        self.resolution = resolution
        self.slab_bytes = (
            slab_bytes if slab_bytes is not None else default_slab_bytes(native.config)
        )
        self.chunk_bytes = chunk_bytes
        self.store_prefix = regrid_store_prefix(native.store_prefix, resolution)
        self.keepbits = native.keepbits
        self.compressor = native.compressor
        self.lats, self.lons = target_grid(resolution)
        self.method = method or REGRID_METHODS.get(resolution, "bilinear")
        if self.method not in _REGRID_BUILDERS:
            raise ValueError(f"unknown regridding method {self.method!r}")
        self._weights = None

    aws_profile = MARSIcechunkProvider.aws_profile
    region = MARSIcechunkProvider.region
    icechunk_path = MARSIcechunkProvider.icechunk_path
    keepbits_for = MARSIcechunkProvider.keepbits_for
    store_times = MARSIcechunkProvider.store_times

    def native_dataset(self) -> xr.Dataset:
        """The native O1280 store this regrid reads from."""
        return xr.open_zarr(
            self.native.get_icechunk_repo().readonly_session("main").store, consolidated=False
        )

    def weights(self, native: xr.Dataset):
        """Regridding weights for `method`, derived from the native store's own grid.

        Row latitudes come from the stored coordinate rather than from a GRIB
        key, so the mapping between a row, its latitude and its offset into the
        flat ``values`` dimension is guaranteed self-consistent.
        """
        if self._weights is None:
            pl = native["pl"].values.astype(np.int64)
            row_start = np.concatenate([[0], np.cumsum(pl)[:-1]])
            row_lat = native["latitude"].values[row_start]
            if not np.all(np.diff(row_lat) < 0):
                raise ValueError("expected reduced Gaussian rows ordered north to south")
            self._weights = _REGRID_BUILDERS[self.method](
                row_lat, row_start, pl, self.resolution
            )
            logger.info(
                f"Built {self.resolution} degree {self.method} weights: "
                f"{self._weights.shape[0]} target points, {self._weights.nnz:,} nonzeros"
            )
        return self._weights

    def chunks_for(self, dims: Sequence[str], shape: Sequence[int]) -> tuple[int, ...]:
        """Chunk shape for a regridded variable: one timestep and one full field.

        Model level variables keep the whole 137-level column in each chunk, at
        every resolution, so a profile or a full 3D state is one read: 569MB
        uncompressed at 0.25 degrees, 36MB at 1 degree.

        Other middle dimensions (the spectra's direction and frequency) are
        taken whole from the innermost outwards while the chunk stays within
        `chunk_bytes`, and one at a time from the first that does not fit. At
        0.25 degrees that is one spectral bin per chunk (a field is 4.2MB); at
        1 degree, where a field is only 260KB, all 29 frequencies of a
        direction (7.6MB), which avoids tens of millions of tiny objects.

        Chunks line up with the slabs `regrid_timestep` writes (whole columns,
        or runs of whole directions), so no chunk is written piecemeal. Time
        stays one step per chunk, which the concurrent writers and
        `extend_time_axis` both rely on.
        """
        n_lat, n_lon = self.lats.size, self.lons.size
        middle_dims = list(dims[1:-2])
        middle = list(shape[1:-2])
        if "level" in middle_dims:
            return (1, *middle, n_lat, n_lon)
        size = n_lat * n_lon * np.dtype(np.float32).itemsize
        chunked = [1] * len(middle)
        for k in range(len(middle) - 1, -1, -1):
            if size * middle[k] > self.chunk_bytes:
                break
            chunked[k] = middle[k]
            size *= middle[k]
        return (1, *chunked, n_lat, n_lon)

    def rechunk_model_levels(self, repo=None) -> List[str]:
        """Recreate model level arrays whose chunks differ from `chunks_for`.

        For a store created before full-column chunking. Zarr cannot change an
        existing array's chunk shape, so each such array is deleted and created
        again with identical metadata -- shape, dtype, codecs, fill value,
        attributes -- apart from its chunks. Its data is dropped and
        ``ingested_ml`` is cleared at every timestep, so the next regrid redoes
        the model levels and nothing else: wave and spectra arrays and their
        flags are untouched.

        Destructive for the dropped data (though icechunk history keeps it), and
        like `extend_time_axis` must not run alongside regrid workers.

        Returns the names of the arrays recreated.
        """
        repo = repo or self.get_icechunk_repo()
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="r+", zarr_format=3)
        redo = []
        for name, array in sorted(group.arrays()):
            dims = tuple(array.metadata.dimension_names or ())
            if "level" not in dims or dims[0] != "time":
                continue
            if tuple(array.chunks) != self.chunks_for(dims, array.shape):
                redo.append((name, array, dims))
        if not redo:
            logger.info(f"{self.icechunk_path}: model levels already chunked as full columns")
            return []

        for name, array, dims in redo:
            chunks = self.chunks_for(dims, array.shape)
            spec = {
                "shape": array.shape,
                "dtype": array.dtype,
                "chunks": chunks,
                "filters": array.filters,
                "compressors": array.compressors,
                "serializer": array.serializer,
                "fill_value": array.fill_value,
                "dimension_names": dims,
                "attributes": dict(array.attrs),
            }
            logger.info(f"Rechunking {name} from {array.chunks} to {chunks}")
            del group[name]
            group.create_array(name, **spec)

        flag = group[_INGESTED[GROUP_ML]]
        flag[:] = np.zeros(flag.shape, dtype=flag.dtype)
        session.commit(
            f"rechunk {len(redo)} model level arrays to full columns; clear ingested_ml "
            "so the model levels are regridded again"
        )
        return [name for name, _, _ in redo]

    def initialize_store(self, repo=None) -> None:
        """Create the regridded schema, mirroring the native variable set."""
        repo = repo or self.get_icechunk_repo()
        session = repo.writable_session("main")
        native = self.native_dataset()
        n_lat, n_lon = self.lats.size, self.lons.size

        coords = {
            "time": pd.DatetimeIndex(native["time"].values),
            "level": native["level"].values,
            "latitude": self.lats,
            "longitude": self.lons,
        }
        for name in ("direction", "frequency"):
            if name in native.coords:
                coords[name] = native[name].values

        data_vars: dict = {}
        for name in ("hybrid_a", "hybrid_b"):
            if name in native:
                data_vars[name] = (native[name].dims, native[name].values)
        encoding: dict = {
            "time": {"units": "seconds since 1970-01-01", "calendar": "standard", "dtype": "int64"}
        }
        for group in GROUPS:
            data_vars[_INGESTED[group]] = ("time", np.zeros(len(coords["time"]), dtype=bool))
            encoding[_INGESTED[group]] = {"chunks": (1,)}
        var_attrs: dict[str, dict] = {}
        n_time = len(coords["time"])

        for name, var in native.data_vars.items():
            if "values" not in var.dims:
                continue
            dims = tuple(d for d in var.dims if d != "values") + ("latitude", "longitude")
            shape = tuple(
                {
                    "time": n_time,
                    "level": N_MODEL_LEVELS,
                    "direction": N_DIRECTIONS,
                    "frequency": N_FREQUENCIES,
                }[d]
                for d in dims[:-2]
            ) + (n_lat, n_lon)
            chunks = self.chunks_for(dims, shape)
            keepbits = self.keepbits_for(name, GROUP_ML if "level" in dims else GROUP_WAVE)
            lossless = keepbits >= LOSSLESS_KEEPBITS
            data_vars[name] = (dims, da.zeros(shape, chunks=chunks, dtype=np.float32))
            encoding[name] = {
                "filters": [] if lossless else [BitRound(keepbits=keepbits)],
                "compressors": [self.compressor],
                "chunks": chunks,
                "_FillValue": np.float32(np.nan),
            }
            var_attrs[name] = dict(native[name].attrs)

        template = xr.Dataset(data_vars, coords=coords, attrs=self._attrs(native))
        for name, attrs in var_attrs.items():
            template[name].attrs.update(attrs)
        template["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
        template["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}

        template.to_zarr(
            session.store, compute=False, encoding=encoding, zarr_format=3, consolidated=False
        )
        _commit(session, f"initialise {self.resolution} degree store with {n_time} timesteps")
        logger.info(f"Initialised {self.icechunk_path}: {n_lat}x{n_lon}, {n_time} timesteps")

    def _attrs(self, native: xr.Dataset) -> dict:
        attrs = dict(native.attrs)
        attrs["grid"] = f"regular latitude/longitude, {self.resolution} degrees"
        attrs["grid_description"] = (
            f"{self.lats.size} x {self.lons.size} global grid, 90N to 90S and 0E "
            f"eastward, {self._method_phrase()} from the native O1280 store."
        )
        attrs["source"] = f"{native.attrs.get('source', '')} via {self.native.icechunk_path}".strip()
        attrs.update(self._method_attrs())
        return attrs

    def _method_phrase(self) -> str:
        return {
            "bilinear": "bilinearly interpolated",
            "conservative": "conservatively remapped",
        }[self.method]

    def _method_attrs(self) -> dict:
        missing = (
            " Around missing values the {} is restricted to valid source points "
            "and the weights renormalised, so coastlines are not eroded; a target "
            "cell stays missing when less than half of it is covered."
        )
        if self.method == "conservative":
            text = (
                "First-order conservative remapping from the octahedral reduced "
                "Gaussian grid: each value is the area-weighted mean of the source "
                "cells its cell overlaps, with source cells bounded by the Gaussian "
                "latitude bands and 360/pl degrees of longitude. Area integrals are "
                "preserved." + missing.format("average")
            )
        else:
            text = "Bilinear from the octahedral reduced Gaussian grid." + missing.format(
                "stencil"
            )
        return {"regridding_method": self.method, "regridding": text}

    def stored_method(self, repo=None) -> str:
        """The method the store was built with. Stores predating the attribute are bilinear."""
        repo = repo or self.get_icechunk_repo()
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
        return group.attrs.get("regridding_method", "bilinear")

    def reset(self, repo=None) -> None:
        """Mark every timestep un-regridded and record this provider's method.

        For changing a store's regridding method: the next run regrids every
        timestep the native store has, overwriting each variable in full, so
        no data from the old method survives where it matters. Until then the
        old values stay readable, flagged as not ingested. Must not run
        alongside regrid workers.
        """
        repo = repo or self.get_icechunk_repo()
        session = repo.writable_session("main")
        group = zarr.open_group(session.store, mode="r+", zarr_format=3)
        for g in GROUPS:
            flag = group[_INGESTED[g]]
            flag[:] = np.zeros(flag.shape, dtype=flag.dtype)
        description = group.attrs.get("grid_description", "")
        for phrase in ("bilinearly interpolated", "conservatively remapped"):
            description = description.replace(phrase, self._method_phrase())
        group.attrs.update({**self._method_attrs(), "grid_description": description})
        session.commit(f"reset for {self.method} regridding: clear all ingest flags")
        logger.info(f"Reset {self.icechunk_path} for {self.method} regridding")

    def pending_groups(
        self, desired_timestamps: Iterable[pd.Timestamp], native: xr.Dataset | None = None
    ) -> dict:
        """Families ingested in the native store but not yet regridded, per timestep.

        `native` lets a caller pass the snapshot it is about to read from, so
        the readiness check and the read cannot disagree. Both axes are looked
        up by time rather than position, so they need not be identical.
        """
        native = native if native is not None else self.native_dataset()
        native_flags = {
            g: pd.Series(
                native[_INGESTED[g]].values.astype(bool),
                index=pd.DatetimeIndex(native["time"].values),
            )
            for g in GROUPS
        }
        try:
            target = xr.open_zarr(
                self.get_icechunk_repo().readonly_session("main").store, consolidated=False
            )
            target_flags = {
                g: pd.Series(
                    target[_INGESTED[g]].values.astype(bool),
                    index=pd.DatetimeIndex(target["time"].values),
                )
                for g in GROUPS
            }
        except Exception:
            target_flags = {g: pd.Series(dtype=bool) for g in GROUPS}

        pending = {}
        for it in desired_timestamps:
            it = pd.Timestamp(it)
            todo = [
                g
                for g in GROUPS
                if bool(native_flags[g].get(it, False)) and not bool(target_flags[g].get(it, False))
            ]
            if todo:
                pending[it] = todo
        return pending

    def missing_timesteps(
        self, desired_timestamps: pd.DatetimeIndex, native: xr.Dataset | None = None
    ) -> List[pd.Timestamp]:
        """Timesteps with a family ready in the native store but not regridded."""
        return list(self.pending_groups(desired_timestamps, native))

    def ensure_time_axis(self, repo=None) -> pd.DatetimeIndex:
        """Create the store, or extend its axis to match the native store's.

        Must not run alongside regrid workers; see `extend_time_axis`.
        """
        repo = repo or self.get_icechunk_repo()
        try:
            self.store_times(repo)
        except Exception:
            self.initialize_store(repo)
            return self.store_times(repo)
        extend_time_axis(repo, pd.DatetimeIndex(self.native_dataset()["time"].values))
        return self.store_times(repo)

    def fetch(self, it: pd.Timestamp, **kwargs) -> List[str]:
        """The native store is the only input."""
        return [self.native.icechunk_path]

    def process(self, input_files, it: pd.Timestamp, **kwargs) -> xr.Dataset:
        """Not used: regridding streams variable by variable in `run`."""
        raise NotImplementedError("use run(), which streams to bound memory")

    def regrid_timestep(
        self, repo, native: xr.Dataset, it: pd.Timestamp, groups: Iterable[str] | None = None
    ) -> None:
        """Regrid and write the variables of `groups` for one timestep.

        `groups` defaults to every family the native store has ingested at
        `it`; a family it has not ingested is skipped even if requested.
        """
        regrid_timestep([(self, repo, groups)], native, it)

    def run(
        self,
        times: Iterable[pd.Timestamp] | None = None,
        skip_complete: bool = True,
        follow: bool = False,
        poll_seconds: int = 300,
        max_idle_polls: int = 12,
    ) -> None:
        """Regrid every timestep the native store has finished; see `run_regrids`."""
        run_regrids([self], times, skip_complete, follow, poll_seconds, max_idle_polls)


def regrid_block(weights, values: np.ndarray, n_lat: int, n_lon: int) -> np.ndarray:
    """Regrid a block whose last axis is the native ``values`` dimension.

    Field by field rather than as one sparse-dense product: measured on 137
    levels at 0.25 degrees, the per-field matrix-vector products take 3.6s and
    a single product with the whole block takes 18s.
    """
    flat = values.reshape(-1, values.shape[-1])
    out = np.stack([apply_weights(weights, row) for row in flat])
    return out.reshape(values.shape[:-1] + (n_lat, n_lon))


def _open_regrid_target(provider: MARSRegridProvider, wanted: pd.DatetimeIndex):
    """Open, or create, a regridded store and check it can take `wanted`."""
    repo = provider.get_icechunk_repo()
    try:
        axis = provider.store_times(repo)
    except Exception:
        provider.initialize_store(repo)
        axis = provider.store_times(repo)
    stored = provider.stored_method(repo)
    if stored != provider.method:
        raise ValueError(
            f"{provider.icechunk_path} was built with {stored} regridding, not "
            f"{provider.method}. Mixing the two would leave the store inconsistent; "
            "run --regrid ... --reset first to redo it all."
        )
    outside = pd.DatetimeIndex(wanted).difference(axis)
    if len(outside):
        raise ValueError(
            f"{len(outside)} timestep(s) fall outside {provider.icechunk_path}'s time axis "
            f"({axis[0]} to {axis[-1]}), e.g. {outside[0]}. "
            "Extend it first with --regrid ... --init-only."
        )
    return repo


def _family(name: str, dims: Sequence[str]) -> str:
    """Which family a native store variable belongs to."""
    if name == SPECTRA_VAR:
        return GROUP_SPECTRA
    if "level" in dims or name in ML_SURFACE_SHORT_NAMES:
        return GROUP_ML
    return GROUP_WAVE


def regrid_timestep(
    targets: Sequence[tuple[MARSRegridProvider, Any, Iterable[str] | None]],
    native: xr.Dataset,
    it: pd.Timestamp,
) -> None:
    """Regrid one timestep onto every target grid, reading each native slab once.

    `targets` holds ``(provider, repo, groups)`` triples; ``groups=None`` means
    every family the native store has ingested at `it`. All targets must read
    the same native store. Reading the native slab dominates the cost -- a
    model level variable is 3.6GB per timestep from S3, against a sparse
    matrix product that takes a fraction of a second -- so sharing the read
    makes each extra resolution nearly free.
    """
    times = pd.DatetimeIndex(native["time"].values)
    i = int(times.get_loc(it))
    # isel before .values: the flags are one chunk per timestep, so reading
    # the whole array would touch every chunk of a 6,000-step axis.
    ingested = {g: bool(native[_INGESTED[g]].isel(time=i).values) for g in GROUPS}

    # One session per target for the whole timestep, committed once at the
    # end with its flags; see MARSIcechunkProvider.write_to_icechunk for why.
    plans = []
    for provider, repo, groups in targets:
        wanted = set(GROUPS if groups is None else groups)
        target = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        plans.append(
            {
                "provider": provider,
                "repo": repo,
                "session": repo.writable_session("main"),
                "weights": provider.weights(native),
                "position": int(pd.DatetimeIndex(target["time"].values).get_loc(it)),
                "ready": {g: g in wanted and ingested[g] for g in GROUPS},
                "names": set(target.data_vars),
                "chunks": {
                    k: tuple(v.encoding.get("chunks") or (1,) * v.ndim)
                    for k, v in target.data_vars.items()
                },
            }
        )
    slab_bytes = min(plan["provider"].slab_bytes for plan in plans)

    for name, var in native.data_vars.items():
        if "values" not in var.dims:
            continue
        group = _family(name, var.dims)
        consumers = [p for p in plans if p["ready"][group] and name in p["names"]]
        if not consumers:
            continue

        middle = tuple(d for d in var.dims if d not in ("time", "values"))
        dims = ("time", *middle, "latitude", "longitude")
        # Stream along the outermost non-time dimension rather than pulling
        # the whole variable-timestep in. Loading it whole costs 3.6GB for a
        # model level variable and 27.6GB for the spectra, which is fine for
        # one worker on a big machine and fatal for several.
        n_outer = var.sizes[middle[0]] if middle else 1
        per_slab = int(np.prod([var.sizes[d] for d in middle[1:]], dtype=int)) if middle else 1
        step = max(1, int(slab_bytes // (per_slab * N_VALUES * 4)))
        if middle:
            # Never split a target chunk across slabs: a full-column chunk
            # written in pieces would be rewritten once per piece.
            align = max(p["chunks"][name][1] for p in consumers)
            step = max(align, step // align * align)

        # Chunk alignment can push a slab back over `slab_bytes`, so the
        # budget is checked against the size actually used: one native slab,
        # plus one regridded copy at each consumer's own resolution.
        fields = min(step, n_outer) * per_slab
        points = N_VALUES + sum(
            p["provider"].lats.size * p["provider"].lons.size for p in consumers
        )
        require_memory(fields * points * 4 / BYTES_PER_GB, what=f"regrid {name} at {it}")

        for begin in range(0, n_outer, step):
            stop = min(begin + step, n_outer)
            sel = {"time": i}
            if middle:
                sel[middle[0]] = slice(begin, stop)
            source = var.isel(**sel).values

            for plan in consumers:
                provider = plan["provider"]
                out = regrid_block(plan["weights"], source, provider.lats.size, provider.lons.size)
                region = {"time": slice(plan["position"], plan["position"] + 1)}
                if middle:
                    region[middle[0]] = slice(begin, stop)
                    for dim in middle[1:]:
                        region[dim] = slice(0, var.sizes[dim])
                region["latitude"] = slice(0, provider.lats.size)
                region["longitude"] = slice(0, provider.lons.size)
                to_icechunk(
                    xr.Dataset({name: (dims, out[None, ...])}), plan["session"], region=region
                )
                del out
            del source
        logger.debug(f"Regridded {name} for {it} onto {[p['provider'].resolution for p in consumers]}")

    for plan in plans:
        flags = xr.Dataset(
            {_INGESTED[g]: (("time",), np.array([True])) for g, ok in plan["ready"].items() if ok}
        )
        if flags.data_vars:
            to_icechunk(
                flags,
                plan["session"],
                region={"time": slice(plan["position"], plan["position"] + 1)},
            )
        if plan["session"].has_uncommitted_changes:
            done = sorted(g for g, ok in plan["ready"].items() if ok)
            _commit(plan["session"], f"regrid {it}: {done}")


def run_regrids(
    providers: Sequence[MARSRegridProvider],
    times: Iterable[pd.Timestamp] | None = None,
    skip_complete: bool = True,
    follow: bool = False,
    poll_seconds: int = 300,
    max_idle_polls: int = 12,
) -> None:
    """Regrid every timestep the native store has finished, onto every target.

    The targets must share one native store. A timestep is visited once, and
    each target is given only the families it still lacks there, so a target
    that is behind the others (a resolution added later, say) catches up
    without the others redoing work.

    With `follow`, keep re-checking for timesteps the native ingest has
    finished since, instead of exiting on the snapshot taken at startup.
    This is what lets the regrid run alongside the ingest rather than
    needing to be relaunched each time the ingest gets ahead. It gives up
    after `max_idle_polls` quiet polls, which is how it terminates once the
    ingest has finished the whole slice.
    """
    if not providers:
        raise ValueError("no regrid targets")
    native_paths = {p.native.icechunk_path for p in providers}
    if len(native_paths) != 1:
        raise ValueError(f"regrid targets read different native stores: {sorted(native_paths)}")

    lead = providers[0]
    wanted = pd.DatetimeIndex(
        times if times is not None else lead.native_dataset()["time"].values
    )
    repos = [(_open_regrid_target(provider, wanted), None) for provider in providers]

    idle = 0
    while True:
        # A fresh snapshot each pass, so newly ingested timesteps appear.
        native = lead.native_dataset()
        if skip_complete:
            per_target = [p.pending_groups(wanted, native) for p in providers]
        else:
            per_target = [{it: list(GROUPS) for it in wanted} for _ in providers]
        todo = sorted(set().union(*per_target))
        logger.info(f"{len(todo)} of {len(wanted)} timesteps ready to regrid")
        for it in todo:
            if skip_complete:
                # Re-read the target flags, so a timestep another worker has
                # finished since the pass began is not done twice.
                per_target = [
                    {**pending, it: p.pending_groups([it], native).get(it)}
                    for p, pending in zip(providers, per_target)
                ]
            targets = [
                (provider, repo, pending[it])
                for provider, (repo, _), pending in zip(providers, repos, per_target)
                if pending.get(it)
            ]
            if not targets:
                continue
            logger.info(
                f"Regridding {it}: "
                + "; ".join(f"{p.resolution} deg ({', '.join(g)})" for p, _, g in targets)
            )
            try:
                # Slabs are sized to the budget (`default_slab_bytes`), but a
                # leak or an unexpectedly large variable would still creep up
                # over a long --follow run; the guard bounds it.
                with memory_guard(what=f"regrid {it}"):
                    regrid_timestep(targets, native, it)
            except _RACE_ERRORS:
                # Another writer (an ingest regridding inline, say) committed
                # the same chunks first. Its result is identical, so move on;
                # the next pass re-checks the flags.
                logger.warning(f"Lost a commit race regridding {it}; skipping it")

        if not follow:
            return
        idle = 0 if todo else idle + 1
        if idle >= max_idle_polls:
            logger.info(f"Nothing new for {idle} polls, stopping")
            return
        if not todo:
            time.sleep(poll_seconds)


def build_provider(
    source_dir: str | pathlib.Path | None = None,
    store_prefix: str | None = None,
    aws_profile: str | None = None,
    staging_dir: str | pathlib.Path | None = None,
    mir_processes: int | None = None,
    config: Config | None = None,
) -> MARSIcechunkProvider:
    """The provider configured for the published store.

    Everything defaults to the configuration: the store is `STORE_PREFIX`
    under ``ICECHUNK_BUCKET``, the GRIB comes from `default_source_dir` and is
    staged in `default_staging_dir`. The GRIB index is cached beside the source
    files, so every run over the same directory shares it.
    """
    config = config or get_config()
    source_dir = pathlib.Path(source_dir or default_source_dir(config))
    return MARSIcechunkProvider(
        source_dir=source_dir,
        index_path=source_dir / "mars_grib_index.parquet",
        store_prefix=store_prefix or STORE_PREFIX,
        aws_profile=aws_profile,
        staging_dir=staging_dir,
        mir_processes=mir_processes,
        config=config,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--source-dir",
        default=None,
        help=(
            "Directory holding the output_*.grib files from mars.py "
            "(default: <PLANETARY_DATASETS_DATA_DIR>/mars)"
        ),
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help=(
            "Fast local disk to stage each timestep's messages on, about 10GB per "
            "timestep in flight (default: <PLANETARY_DATASETS_SCRATCH_DIR>/mars_staging)"
        ),
    )
    parser.add_argument(
        "--store-prefix",
        default=STORE_PREFIX,
        help="Store location relative to ICECHUNK_BUCKET (default: %(default)s)",
    )
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="Profile with write access to the bucket (default: $AWS_PROFILE)",
    )
    parser.add_argument(
        "--start", default=None, help="First valid time to ingest, e.g. 2026-08-01T00"
    )
    parser.add_argument("--end", default=None, help="Last valid time to ingest (inclusive)")
    parser.add_argument(
        "--shard",
        default="0/1",
        metavar="K/N",
        help=(
            "Take only every Nth timestep, starting from the Kth, so N workers "
            "launched with 0/N .. N-1/N split the work without overlapping and "
            "all move through the time range together (default 0/1: everything)"
        ),
    )
    parser.add_argument(
        "--init-only",
        action="store_true",
        help=(
            "Create the store schema, or extend an existing store's time axis to "
            "cover the source files, and stop. Run with nothing else writing."
        ),
    )
    parser.add_argument(
        "--axis-start",
        default=None,
        help="With --init-only, start the hourly time axis no later than this",
    )
    parser.add_argument(
        "--axis-end",
        default=None,
        help=(
            "With --init-only, run the hourly time axis to at least this, so it "
            "already covers data mars.py has yet to retrieve"
        ),
    )
    parser.add_argument(
        "--regrid",
        type=float,
        nargs="+",
        default=None,
        metavar="DEGREES",
        help=(
            "Instead of ingesting GRIB, resample the native store onto regular "
            "lat/lon grids of these spacings, each written to a sibling store "
            "tagged with its resolution (0.25 -> ..._025.icechunk, 1 -> "
            "..._100.icechunk). Each native slab is read once for all of them. "
            "With --init-only, creates them or extends their time axes to match "
            "the native store."
        ),
    )
    parser.add_argument(
        "--no-scan",
        action="store_true",
        help=(
            "Ingest from the files already in the index cache only, without "
            "scanning new ones; for ingest workers started while build_index is "
            "still scanning"
        ),
    )
    parser.add_argument(
        "--with-regrid",
        type=float,
        nargs="+",
        default=[],
        metavar="DEGREES",
        help=(
            "When ingesting GRIB, also write these regridded stores (e.g. 0.25 1) "
            "from the decoded fields, instead of leaving them to a separate --regrid "
            "pass that reads the native store back from S3"
        ),
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "With --regrid, clear the target stores' ingest flags and record their "
            "regridding method (see REGRID_METHODS), so the next run redoes every "
            "timestep. For changing a store's method. Run with nothing else writing."
        ),
    )
    parser.add_argument(
        "--rechunk-levels",
        action="store_true",
        help=(
            "With --regrid, recreate model level arrays that are not chunked as "
            "full 137-level columns, dropping their data and clearing ingested_ml "
            "so the next regrid redoes them. Run with nothing else writing."
        ),
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help=(
            "With --regrid, keep polling for timesteps the native ingest "
            "finishes rather than exiting on the startup snapshot."
        ),
    )
    parser.add_argument(
        "--idle-polls",
        type=int,
        default=12,
        help="With --follow, stop after this many 5-minute polls find nothing new",
    )
    args = parser.parse_args()
    shard, n_shards = (int(x) for x in args.shard.split("/"))
    if not 0 <= shard < n_shards:
        parser.error(f"--shard {args.shard}: need 0 <= K < N")

    def _slice(times: pd.DatetimeIndex) -> pd.DatetimeIndex:
        if args.start:
            times = times[times >= pd.Timestamp(args.start)]
        if args.end:
            times = times[times <= pd.Timestamp(args.end)]
        # Shard on the hour, not on the position in `times`, so every worker
        # agrees on the split whichever list of times it happens to start from.
        hours = ((times - pd.Timestamp("1970-01-01")) // pd.Timedelta(1, unit="h")).to_numpy()
        return times[hours % n_shards == shard]

    provider = build_provider(
        source_dir=args.source_dir,
        store_prefix=args.store_prefix,
        aws_profile=args.aws_profile,
        staging_dir=args.staging_dir,
    )
    logger.info(f"Store {provider.icechunk_path}, GRIB in {provider.source_dir}")

    if args.regrid:
        # The regrid reads the native store, so it needs no GRIB index.
        targets = [MARSRegridProvider(provider, resolution=r) for r in args.regrid]
        if args.init_only or args.rechunk_levels or args.reset:
            for target in targets:
                target.ensure_time_axis()
                if args.rechunk_levels:
                    target.rechunk_model_levels()
                if args.reset:
                    target.reset()
        else:
            times = _slice(pd.DatetimeIndex(targets[0].native_dataset()["time"].values))
            run_regrids(targets, times=times, follow=args.follow, max_idle_polls=args.idle_polls)
    else:
        provider.build_index(scan=not args.no_scan)
        if args.init_only:
            provider.ensure_time_axis(
                start=pd.Timestamp(args.axis_start) if args.axis_start else None,
                end=pd.Timestamp(args.axis_end) if args.axis_end else None,
            )
        else:
            # Slicing the time axis lets several processes cover disjoint ranges;
            # each timestep is an independent set of region writes.
            provider.run(
                times=_slice(provider.times()),
                regrids=[MARSRegridProvider(provider, resolution=r) for r in args.with_regrid],
                scan=False,  # already built above
            )
