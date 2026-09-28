"""Météo-France AROME NWP models, published as GRIB2 paquets on data.gouv.fr.

Three variants share one download engine, one cleanup policy and one write path. They
differ only in the grid, the paquet naming and how forecast steps are laid out in the
files, so each is a thin configuration plus a ``process`` that merges its own paquets:

* :class:`AromeOverseasProvider` — AROME Overseas 0.025°, one GRIB per hourly step, for
  the five overseas domains (New Caledonia, Indian Ocean, French Guiana, Caribbean and
  Polynesia). Surface (SP1-SP2), pressure (IP1-IP5) and height (HP1-HP2) paquets.
* :class:`AromeFranceProvider` — AROME France 0.025°, one ``00H06H`` GRIB per paquet
  holding every step. Surface (SP1-SP3), pressure (IP1, IP3) and height (HP1-HP2).
* :class:`AromeFranceHDProvider` — AROME France 0.01°, one GRIB per hourly step, surface
  (SP1-SP3) and height (HP1) only.

Downloads land in a persistent directory under
:attr:`~planetary_datasets.config.Config.data_dir` rather than a scratch one. A GRIB file
is deleted only once the init time it belongs to has been committed to the store, so a
failed run leaves its files behind and the next attempt skips what it already has.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import cfgrib
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.dataset import (
    reduce_precision,
    rename_vars_by_long_name,
    sort_vertical_coords,
)
from planetary_datasets.common.download import cleanup_files, download_one
from planetary_datasets.memory import memory_guard, require_dataset_fits

URL_BASE = "https://files.data.gouv.fr/meteofrance-pnt/pnt"

#: Overseas domain code as used in the URLs, mapped to the name used in the store prefix.
OVERSEAS_REGIONS = {
    "NCALED": "new_caledonia",
    "INDIEN": "indian_ocean",
    "GUYANE": "french_guiana",
    "ANTIL": "caribbean",
    "POLYN": "polynesia",
}

#: GRIB coordinates that cfgrib attaches to surface fields and that would otherwise
#: collide when the sub-datasets of one paquet are merged back together.
_SURFACE_DROP = ("heightAboveGround", "level", "surface", "meanSea", "unknown")

#: Dimensions written as a single chunk. ``time`` is always chunked to one step.
_WHOLE_CHUNK_DIMS = ("latitude", "longitude", "level", "height")


@dataclass(frozen=True)
class AromeLayout:
    """Where one AROME variant lives and how its files are named.

    Attributes:
        model_path: URL segment between the init time and the paquet, e.g. ``arome/0025``.
        file_stem: Filename prefix, e.g. ``arome`` or ``arome-om-NCALED``.
        resolution: Grid token in the filename, ``0025`` or ``001``.
        paquets: Paquets to download. Only paquets that ``process`` actually reads are
            listed, so nothing is fetched to be thrown away.
        step_tokens: Step part of the filename, one entry per file per paquet. Either one
            file per hourly step (``000H``…) or a single file covering a range (``00H06H``).
        download_subdir: Directory under ``data_dir`` that GRIB files are kept in.
        optional_steps: ``(paquet, step_token)`` pairs that are allowed to be absent. Every
            other missing file is fatal for the init time. This is deliberately a list of
            named exceptions rather than a "tolerate anything missing" flag: the only
            genuine gap is a paquet the archive does not publish an analysis step for (IP4
            starts at 001H overseas), which the merge fills. Accepting *any* subset instead
            meant an init time caught mid-publication was written with two valid times out
            of seven, and ``run_partition``'s "is this init time stored?" check then called
            the whole thing done, leaving a permanent silent hole at the later steps.
    """

    model_path: str
    file_stem: str
    resolution: str
    paquets: tuple[str, ...]
    step_tokens: tuple[str, ...]
    download_subdir: str
    optional_steps: tuple[tuple[str, str], ...] = ()

    def step_is_optional(self, paquet: str, step_token: str) -> bool:
        """Whether this paquet is known not to publish this step."""
        return (paquet, step_token) in self.optional_steps

    def filename(self, paquet: str, step_token: str, init_time: pd.Timestamp) -> str:
        """Local filename for one paquet at one step."""
        stamp = init_time.strftime("%Y-%m-%dT%H%M%SZ")
        return f"{self.file_stem}__{self.resolution}__{paquet}__{step_token}__{stamp}.grib2"

    def url(self, paquet: str, step_token: str, init_time: pd.Timestamp) -> str:
        """Source URL for one paquet at one step."""
        stamp = init_time.strftime("%Y-%m-%dT%H:00:00Z")
        name = f"{self.file_stem}__{self.resolution}__{paquet}__{step_token}__{stamp}.grib2"
        return f"{URL_BASE}/{stamp}/{self.model_path}/{paquet}/{name}"


class AromeProvider(BaseProvider):
    """Shared download, cleanup and write behaviour for every AROME variant.

    Subclasses supply a :attr:`layout` and implement :meth:`process`.
    """

    #: Attempts per file. A paquet with no analysis step 404s every time, so keep it low.
    download_retries: int = 3

    @property
    def layout(self) -> AromeLayout:
        """Configuration describing this variant's files."""
        raise NotImplementedError

    @property
    def download_dir(self) -> pathlib.Path:
        """Persistent directory GRIB files are downloaded into, created if absent."""
        path = self.config.data_dir / self.layout.download_subdir
        path.mkdir(parents=True, exist_ok=True)
        return path

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every paquet for one init time.

        ``temp_dir`` is ignored: AROME keeps its GRIB files until the init time has been
        written, so they go to :attr:`download_dir` instead. Returns an empty list when a
        paquet is missing entirely, since a partial init time would write a dataset whose
        variables do not match the store.
        """
        layout = self.layout
        dest_dir = self.download_dir
        downloaded: List[str] = []

        for paquet in layout.paquets:
            found: List[str] = []
            for step_token in layout.step_tokens:
                dest = dest_dir / layout.filename(paquet, step_token, it)
                path = download_one(
                    layout.url(paquet, step_token, it),
                    dest,
                    retries=self.download_retries,
                )
                if path is None:
                    if not layout.step_is_optional(paquet, step_token):
                        logger.warning(f"{self.name}: {dest.name} unavailable, skipping {it}")
                        return []
                    logger.debug(f"{self.name}: {paquet} has no {step_token}, as expected")
                    continue
                found.append(str(path))
            if not found:
                logger.warning(f"{self.name}: no {paquet} files for {it}, skipping")
                return []
            downloaded.extend(found)

        return downloaded

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Fetch, process and write one init time, deleting the GRIB files on success.

        Overrides :meth:`BaseProvider.run_partition` only to add that deletion, and to
        keep the downloads out of the base class's temporary directory. Files are removed
        after the commit, never before, so a failure leaves them for the next attempt.
        """
        repo = self.get_icechunk_repo()

        if check_present and not self.missing_timesteps(pd.DatetimeIndex([it])):
            logger.debug(f"{self.name}: {it} already in {self.store_path}, skipping")
            return False

        input_files = self.fetch(it)
        if not input_files:
            logger.debug(f"{self.name}: no input files for {it}, skipping")
            return False

        logger.info(f"{self.name}: processing {len(input_files)} file(s) for {it}")
        if self.guard_memory:
            # As in the base class, only the processing is guarded: memory_guard raises
            # when its block exits, so a breach must prevent the write rather than leave
            # a committed store behind a failed run.
            with memory_guard(what=f"{self.name} {it}"):
                processed = self.process(input_files, it)
                require_dataset_fits(processed, what=f"{self.name} {it}")
        else:
            processed = self.process(input_files, it)

        written = self.write_to_icechunk(repo, processed)
        if written:
            removed = cleanup_files(*input_files)
            logger.debug(f"{self.name}: removed {removed} local file(s) for {it}")
        return written


class AromeOverseasProvider(AromeProvider):
    """AROME Overseas 0.025°, one hourly GRIB per paquet per step.

    Init times are every 6 hours. Seven steps (0-6h) are downloaded and the last is
    dropped, because the next init time supplies it as its own analysis.
    """

    append_dim = "time"

    def __init__(self, region: str = "INDIEN", config=None):
        if region not in OVERSEAS_REGIONS:
            raise ValueError(
                f"unknown AROME overseas region {region!r}, expected one of "
                f"{sorted(OVERSEAS_REGIONS)}"
            )
        self.region = region
        super().__init__(config)

    @property
    def name(self) -> str:
        return f"arome_{OVERSEAS_REGIONS[self.region]}"

    @property
    def store_prefix(self) -> str:
        return f"bkr/dmi/arome_{OVERSEAS_REGIONS[self.region]}.icechunk"

    @property
    def layout(self) -> AromeLayout:
        return AromeLayout(
            model_path=f"arome-om/{self.region}/0025",
            file_stem=f"arome-om-{self.region}",
            resolution="0025",
            paquets=("HP1", "HP2", "IP1", "IP2", "IP3", "IP4", "IP5", "SP1", "SP2"),
            step_tokens=tuple(f"{step:03d}H" for step in range(7)),
            download_subdir="meteofrance",
            # IP4 is published from 001H onwards overseas; there is no analysis step to
            # download. Nothing else may be missing.
            optional_steps=(("IP4", "000H"),),
        )

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        height = xr.merge(
            [_open_steps(_require_all(input_files, paquet, it)) for paquet in ("HP1", "HP2")]
        )

        pressure_parts = []
        for paquet in ("IP1", "IP2", "IP3", "IP4", "IP5"):
            files = _files_for(input_files, paquet)
            if not files:
                continue
            part = _open_steps(files)
            if paquet == "IP5":
                # IP5 repeats u/v/z on potential-vorticity surfaces, which collide with
                # the isobaric fields of the other pressure paquets.
                part = part.drop_vars(["potentialVorticity", "u", "v", "z"], errors="ignore")
            pressure_parts.append(part)
        pressure = xr.merge(pressure_parts).drop_vars(["time", "step"], errors="ignore")

        surface = xr.merge(
            [
                xr.concat(
                    [
                        _open_surface(f, per_step_file=True)
                        for f in _require_all(input_files, paquet, it)
                    ],
                    dim="valid_time",
                )
                for paquet in ("SP1", "SP2")
            ]
        )

        surface = rename_vars_by_long_name(surface, suffix="_at_surface")
        height = rename_vars_by_long_name(height, suffix="_at_height")
        pressure = rename_vars_by_long_name(pressure)

        ds = (
            xr.merge([surface, pressure, height])
            .drop_vars(["time", "step"], errors="ignore")
            .rename({"valid_time": "time", "heightAboveGround": "height", "isobaricInhPa": "level"})
        )
        # The 6h step is the next init time's analysis, so it is left to that partition.
        return _keep_hours(_finalise(ds), it, hours=6)


class AromeFranceProvider(AromeProvider):
    """AROME France 0.025°, one ``00H06H`` GRIB per paquet covering every step.

    Init times are every 3 hours and each file carries steps 0-6h, so only the first three
    hours are kept: the rest arrive as the analysis of the following init times.
    """

    name = "arome_france_0025"
    append_dim = "time"
    store_prefix = "bkr/dmi/arome_france_0025.icechunk"

    @property
    def layout(self) -> AromeLayout:
        return AromeLayout(
            model_path="arome/0025",
            file_stem="arome",
            resolution="0025",
            paquets=("HP1", "HP2", "IP1", "IP3", "SP1", "SP2", "SP3"),
            step_tokens=("00H06H",),
            download_subdir="meteofrance_france",
        )

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        height = xr.merge(
            [xr.merge(cfgrib.open_datasets(f)) for f in _require(input_files, ("HP1", "HP2"), it)]
        )
        pressure = xr.merge(
            [xr.merge(cfgrib.open_datasets(f)) for f in _require(input_files, ("IP1", "IP3"), it)]
        )
        surface = xr.merge(
            [_open_surface(f) for f in _require(input_files, ("SP1", "SP2", "SP3"), it)]
        )

        # Keep the three hours up to the next init time; the rest of the 0-6h file is
        # superseded by the runs that follow.
        height = _keep_steps(height, hours=3).drop_vars("valid_time", errors="ignore")
        pressure = _keep_steps(pressure, hours=3).drop_vars("valid_time", errors="ignore")
        surface = _keep_steps(surface, hours=3).drop_vars("valid_time", errors="ignore")

        surface = rename_vars_by_long_name(surface, suffix="_at_surface")
        height = rename_vars_by_long_name(height, suffix="_at_height")
        pressure = rename_vars_by_long_name(pressure, suffix="_at_pressure")

        ds = xr.merge([surface, height, pressure]).rename(
            {"heightAboveGround": "height", "isobaricInhPa": "level"}
        )
        return _finalise(_step_to_time(ds))


class AromeFranceHDProvider(AromeProvider):
    """AROME France 0.01°, one GRIB per paquet per hourly step.

    Init times are every 3 hours, with three steps (0-2h) per init time. Only surface and
    height-level paquets are published at this resolution.
    """

    name = "arome_france_hd"
    append_dim = "time"
    store_prefix = "bkr/dmi/arome_france.icechunk"

    @property
    def layout(self) -> AromeLayout:
        return AromeLayout(
            model_path="arome/001",
            file_stem="arome",
            resolution="001",
            paquets=("HP1", "SP1", "SP2", "SP3"),
            step_tokens=tuple(f"{step:02d}H" for step in range(3)),
            download_subdir="meteofrance_france",
        )

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        layout = self.layout
        heights = []
        surfaces = []
        for step_token in layout.step_tokens:
            per_step = [f for f in input_files if f"__{step_token}__" in os.path.basename(f)]
            heights.append(xr.merge(cfgrib.open_datasets(_require(per_step, ("HP1",), it)[0])))
            surfaces.append(
                xr.merge([_open_surface(f) for f in _require(per_step, ("SP1", "SP2", "SP3"), it)])
            )

        height = xr.concat(heights, dim="step")
        surface = xr.concat(surfaces, dim="step")

        surface = rename_vars_by_long_name(surface, suffix="_at_surface")
        height = rename_vars_by_long_name(height, suffix="_at_height")

        ds = xr.merge([surface, height]).rename({"heightAboveGround": "height"})
        return _finalise(_step_to_time(ds))


def _files_for(files: Iterable[str], paquet: str) -> List[str]:
    """Return the files belonging to one paquet, in step order."""
    return sorted(f for f in files if f"__{paquet}__" in os.path.basename(f))


def _require_all(files: Iterable[str], paquet: str, it: pd.Timestamp) -> List[str]:
    """Return every file of one paquet, raising when it is absent entirely."""
    matches = _files_for(files, paquet)
    if not matches:
        raise FileNotFoundError(f"no {paquet} files among the inputs for {it}")
    return matches


def _require(files: Sequence[str], paquets: Sequence[str], it: pd.Timestamp) -> List[str]:
    """Return exactly one file per paquet, raising when any is absent."""
    return [_require_all(files, paquet, it)[0] for paquet in paquets]


def _open_steps(files: Sequence[str]) -> xr.Dataset:
    """Open one file per forecast step and stack them along ``valid_time``."""
    return xr.open_mfdataset(list(files), concat_dim="valid_time", combine="nested")


def _open_surface(path: str, per_step_file: bool = False) -> xr.Dataset:
    """Open a surface paquet, whose fields sit on several incompatible level types.

    cfgrib returns one sub-dataset per level type; the level coordinates are dropped so
    they can be merged into a single flat surface dataset.

    Args:
        path: GRIB file to open.
        per_step_file: True when the file holds a single forecast step that the caller
            will concatenate along ``valid_time``. That drops the scalar ``time`` and
            ``step``; otherwise the file's own ``step`` dimension is kept and the
            redundant ``valid_time`` is dropped instead.
    """
    drop = list(_SURFACE_DROP)
    drop += ["step", "time"] if per_step_file else ["valid_time"]
    return xr.merge([ds.drop_vars(drop, errors="ignore") for ds in cfgrib.open_datasets(path)])


def _keep_steps(ds: xr.Dataset, hours: int) -> xr.Dataset:
    """Keep the first ``hours`` forecast steps of a file that holds a step dimension.

    Selected by value rather than position: a surface paquet whose accumulated fields
    only start at 1h would otherwise shift the window.
    """
    return ds.sel(step=slice(pd.Timedelta(0), pd.Timedelta(hours=hours - 1)))


def _keep_hours(ds: xr.Dataset, it: pd.Timestamp, hours: int) -> xr.Dataset:
    """Keep the ``hours`` valid times starting at the init time.

    Selected by value rather than position: when a paquet is missing a step the dataset
    is shorter than expected, and a positional slice would let the step belonging to the
    next init time through and write that timestamp twice.
    """
    return ds.sel(time=slice(None, it + pd.Timedelta(hours=hours - 1)))


def _step_to_time(ds: xr.Dataset) -> xr.Dataset:
    """Replace the (init time, step) pair with a single valid ``time`` coordinate."""
    ds = ds.assign_coords(step=ds["time"] + ds["step"])
    return ds.drop_vars(["time", "valid_time"], errors="ignore").rename({"step": "time"})


def _finalise(ds: xr.Dataset) -> xr.Dataset:
    """Reduce precision, order the coordinates and chunk one step at a time."""
    ds = reduce_precision(ds)
    ds = sort_vertical_coords(ds).sortby("time")
    chunks = {"time": 1}
    chunks.update({dim: -1 for dim in _WHOLE_CHUNK_DIMS if dim in ds.dims})
    return ds.chunk(chunks)
