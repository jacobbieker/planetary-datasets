r"""Fetch one partition of an earth2studio "direct observation" source and stage it.

This is the download half of the earth2studio observation pipeline;
:mod:`planetary_datasets.providers.earth2studio_obs` is the processing half, and publishes
what this stages. It runs inside the ``docker/earth2studio`` image, alongside the OPERA
downloader, because earth2studio needs ``torch`` and ``netcdf4<1.7.3``. Like that module
it imports nothing from ``planetary_datasets`` and loads earth2studio lazily, so the
project can import :data:`DATASETS` without it.

Every source is described by a :class:`Dataset` in :data:`DATASETS`, and fetched by one
of three generic routines according to its :attr:`Dataset.kind`:

``table``
    DataFrame sources (station reports, satellite footprints, lightning). One call per
    partition, cast to the source's own ``SCHEMA`` and staged as a single Parquet file,
    which is published as-is to a partitioned Parquet dataset.
``grid``
    Fixed-grid DataArray sources. The partition's frames are fetched a few at a time and
    each batch is staged as its own netCDF file, so a full-disk hour never has to fit in
    memory at once. They are appended to an icechunk store.
``granules``
    Swath sources whose geography moves every granule (VIIRS, Sentinel-3 SYNERGY). The
    granules in the partition are listed first and requested exactly, then stacked along
    ``time`` with their latitude and longitude kept as per-granule variables.

A partition is only complete once its manifest, ``<name>_<stamp>.json``, is written; it
is the last thing written and lists every staged file.

Windows. earth2studio filters to ``[t + lower, t + upper]`` inclusive, with sub-second
timestamps, so a partition is requested with ``upper`` equal to its full length and then
cut to ``[start, end)`` here. Several sources also need the window widened below the
start (:attr:`Dataset.lead`) because a file that began before the partition carries its
first observations.

Run it as::

    python -m planetary_datasets.providers.earth2studio_download \
        --dataset iem_asos --time 2026-09-28T00:00 --target /data/earth2studio
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import pathlib
import re
import sys
from typing import Any, Callable, Iterable, Sequence

from loguru import logger

DEFAULT_TARGET = "/data/earth2studio"

#: ``async_timeout`` bounds download *and* decode together, so the 600 s default fails
#: whole partitions of the heavier sources. Dagster's own run timeout is the real backstop.
TABLE_TIMEOUT_S = 6 * 3600
GRID_TIMEOUT_S = 2 * 3600

EUMETSAT = ("EUMETSAT_CONSUMER_KEY", "EUMETSAT_CONSUMER_SECRET")

#: MODIS 14A1 land tiles, as listed by Planetary Computer's STAC for 2001, 2010 and 2023
#: (identical in all three). Generated from that listing, not typed.
MODIS_TILES: tuple[str, ...] = (
    "h00v08", "h00v09", "h00v10", "h01v07", "h01v08", "h01v09", "h01v10", "h01v11",
    "h02v06", "h02v08", "h02v09", "h02v10", "h02v11", "h03v06", "h03v07", "h03v09",
    "h03v10", "h03v11", "h04v09", "h04v10", "h04v11", "h05v10", "h05v11", "h05v13",
    "h06v03", "h06v11", "h07v03", "h07v05", "h07v06", "h07v07", "h08v03", "h08v04",
    "h08v05", "h08v06", "h08v07", "h08v08", "h08v09", "h08v11", "h09v02", "h09v03",
    "h09v04", "h09v05", "h09v06", "h09v07", "h09v08", "h09v09", "h10v02", "h10v03",
    "h10v04", "h10v05", "h10v06", "h10v07", "h10v08", "h10v09", "h10v10", "h10v11",
    "h11v02", "h11v03", "h11v04", "h11v05", "h11v06", "h11v07", "h11v08", "h11v09",
    "h11v10", "h11v11", "h11v12", "h12v01", "h12v02", "h12v03", "h12v04", "h12v05",
    "h12v07", "h12v08", "h12v09", "h12v10", "h12v11", "h12v12", "h12v13", "h13v01",
    "h13v02", "h13v03", "h13v04", "h13v08", "h13v09", "h13v10", "h13v11", "h13v12",
    "h13v13", "h13v14", "h14v00", "h14v01", "h14v02", "h14v03", "h14v04", "h14v09",
    "h14v10", "h14v11", "h14v14", "h15v00", "h15v01", "h15v02", "h15v03", "h15v05",
    "h15v07", "h15v11", "h15v14", "h16v00", "h16v01", "h16v02", "h16v05", "h16v06",
    "h16v07", "h16v08", "h16v09", "h16v12", "h16v14", "h17v00", "h17v01", "h17v02",
    "h17v03", "h17v04", "h17v05", "h17v06", "h17v07", "h17v08", "h17v10", "h17v12",
    "h17v13", "h18v00", "h18v01", "h18v02", "h18v03", "h18v04", "h18v05", "h18v06",
    "h18v07", "h18v08", "h18v09", "h18v14", "h19v00", "h19v01", "h19v02", "h19v03",
    "h19v04", "h19v05", "h19v06", "h19v07", "h19v08", "h19v09", "h19v10", "h19v11",
    "h19v12", "h20v00", "h20v01", "h20v02", "h20v03", "h20v04", "h20v05", "h20v06",
    "h20v07", "h20v08", "h20v09", "h20v10", "h20v11", "h20v12", "h20v13", "h21v00",
    "h21v01", "h21v02", "h21v03", "h21v04", "h21v05", "h21v06", "h21v07", "h21v08",
    "h21v09", "h21v10", "h21v11", "h21v13", "h22v01", "h22v02", "h22v03", "h22v04",
    "h22v05", "h22v06", "h22v07", "h22v08", "h22v09", "h22v10", "h22v11", "h22v13",
    "h22v14", "h23v01", "h23v02", "h23v03", "h23v04", "h23v05", "h23v06", "h23v07",
    "h23v08", "h23v09", "h23v10", "h23v11", "h24v02", "h24v03", "h24v04", "h24v05",
    "h24v06", "h24v07", "h24v12", "h25v02", "h25v03", "h25v04", "h25v05", "h25v06",
    "h25v07", "h25v08", "h25v09", "h26v02", "h26v03", "h26v04", "h26v05", "h26v06",
    "h26v07", "h26v08", "h27v03", "h27v04", "h27v05", "h27v06", "h27v07", "h27v08",
    "h27v09", "h27v10", "h27v11", "h27v12", "h27v14", "h28v03", "h28v04", "h28v05",
    "h28v06", "h28v07", "h28v08", "h28v09", "h28v10", "h28v11", "h28v12", "h28v13",
    "h28v14", "h29v03", "h29v05", "h29v06", "h29v07", "h29v08", "h29v09", "h29v10",
    "h29v11", "h29v12", "h29v13", "h30v05", "h30v06", "h30v07", "h30v08", "h30v09",
    "h30v10", "h30v11", "h30v12", "h30v13", "h31v06", "h31v07", "h31v08", "h31v09",
    "h31v10", "h31v11", "h31v12", "h31v13", "h32v07", "h32v08", "h32v09", "h32v10",
    "h32v11", "h32v12", "h33v07", "h33v08", "h33v09", "h33v10", "h33v11", "h34v07",
    "h34v08", "h34v09", "h34v10", "h35v08", "h35v09", "h35v10",
)  # fmt: skip

ABI_BANDS = tuple(f"abi{b:02d}c" for b in range(1, 17))
AHI_BANDS = tuple(f"ahi{b:02d}" for b in range(1, 17))
FCI_2KM = (
    "fci38ir",
    "fci63wv",
    "fci73wv",
    "fci87ir",
    "fci97ir",
    "fci105ir",
    "fci123ir",
    "fci133ir",
)
#: earth2studio appends ``_lat``/``_lon`` along ``variable`` itself; asking for them fails.
VIIRS_I = tuple(f"viirs{b:02d}i" for b in range(1, 6))
VIIRS_M = tuple(f"viirs{b:02d}m" for b in range(1, 17))
S3_AOD = (
    tuple(f"s3sy{b:02d}aod" for b in range(1, 7))
    + tuple(f"s3sy{b:02d}ssa" for b in range(1, 6))
    + tuple(f"s3sy{b:02d}sr" for b in range(1, 6))
    + ("s3sysunzen", "s3sysatzen", "s3syrelaz", "s3sycloudfrac", "s3sy_lat", "s3sy_lon")
)
NCEP_CONV = ("u", "v", "q", "t", "pres", "gps", "gps_refractivity")
GLM_LEVELS = {
    "event": ("lightning_event_energy",),
    "group": ("lightning_group_energy", "lightning_group_area"),
    "flash": ("lightning_flash_energy", "lightning_flash_area"),
}
LI_LEVELS = {
    "flash": (
        "lightning_flash_radiance",
        "lightning_flash_duration",
        "lightning_flash_footprint_pixels",
    ),
    "group": ("lightning_group_radiance", "lightning_group_footprint_pixels"),
    "event": ("lightning_event_radiance",),
}


@dataclasses.dataclass(frozen=True)
class Dataset:
    """One stored product of an earth2studio source.

    Attributes:
        name: Store and asset name.
        source: Class name in ``earth2studio.data``.
        kind: ``table``, ``grid`` or ``granules``; see the module docstring.
        freq: Partition length, as a pandas offset (``10min``, ``1h``, ``6h``, ``1D``,
            ``1MS``, ``1YS``).
        start: First partition, ISO 8601 UTC.
        variables: earth2studio variable names, all requested together.
        description: One line for the Dagster asset.
        kwargs: Constructor arguments.
        by_date: ``(from, kwargs)`` overrides applied from a date, for a satellite
            changeover within one store.
        end: First partition that no longer exists upstream, if any.
        lead: How far before the partition start to request, for files that begin before
            it. Rows before the start are cut again afterwards.
        step: ``grid``: spacing of the frames within a partition.
        frames_per_call: ``grid``/``granules``: frames or granules per request.
        stations: ``table``: enumerate this network's stations (``ghcnd``, ``ghcnh``,
            ``isd``) and pass them in.
        station_chunk: Stations per request, when a source holds them all in memory.
        granules: ``granules``: how to list them (``viirs`` or ``s3aod``).
        tiles: ``grid``: fetch each tile separately and stack them along ``tile``.
        credentials: Environment variables the source needs.
        dtype: ``grid``/``granules``: dtype data variables are stored as.
        valid_min: ``grid``: values below this are sentinels and become NaN.
        end_offset: Partitions held back from the newest, for upstream publication lag.
        scheduled: Whether Dagster schedules it; heavy ones are backfilled by hand.
        memory_gb: Memory one partition needs.
        publish: Where :mod:`~planetary_datasets.providers.earth2studio_obs` writes it.
    """

    name: str
    source: str
    kind: str
    freq: str
    start: str
    variables: tuple[str, ...]
    description: str
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
    by_date: tuple[tuple[str, dict[str, Any]], ...] = ()
    end: str | None = None
    lead: str = "0s"
    step: str | None = None
    frames_per_call: int = 0
    stations: str | None = None
    station_chunk: int = 0
    granules: str | None = None
    tiles: tuple[str, ...] = ()
    credentials: tuple[str, ...] = ()
    dtype: str = "float32"
    valid_min: float | None = None
    end_offset: int = -1
    scheduled: bool = True
    memory_gb: float = 8.0

    @property
    def publish(self) -> str:
        """``parquet`` for tables, ``icechunk`` for everything gridded."""
        return "parquet" if self.kind == "table" else "icechunk"

    def kwargs_for(self, when: dt.datetime) -> dict[str, Any]:
        """Constructor arguments in force at ``when``."""
        kwargs = dict(self.kwargs)
        for since, overrides in self.by_date:
            if when >= dt.datetime.fromisoformat(since):
                kwargs.update(overrides)
        return kwargs


def _glm(slot: str, level: str) -> Dataset:
    return Dataset(
        name=f"goes_glm_{slot}_{level}",
        source="GOESGLM",
        kind="table",
        freq="1h",
        start="2018-02-13" if slot == "east" else "2018-12-10",
        variables=GLM_LEVELS[level],
        kwargs={"satellite": slot},
        # A file is kept when it starts within 20 s before the window.
        lead="20s",
        description=f"GOES-{slot.title()} GLM L2 LCFA lightning {level}s, full disk.",
        memory_gb=16.0 if level == "event" else 8.0,
    )


def _li(level: str) -> Dataset:
    return Dataset(
        name=f"meteosat_li_{level}",
        source="MeteosatLI",
        kind="table",
        freq="1h",
        start="2024-07-04T14:00",
        variables=LI_LEVELS[level],
        credentials=EUMETSAT,
        description=f"MTG-I Lightning Imager L2 {level}s, full disk.",
        memory_gb=16.0 if level == "event" else 8.0,
    )


def _goes(backend: str, slot: str, mode: str) -> Dataset:
    east = slot == "east"
    prefix = "goes" if backend == "GOES" else "pc_goes"
    sector = {"F": "fd", "C": "conus" if east else "pacus"}[mode]
    return Dataset(
        name=f"{prefix}_{slot}_{sector}",
        source=backend,
        kind="grid",
        freq="1h",
        # GOES-16 full disk ran every 15 minutes until 2019-04-02, which a 10-minute
        # store would fill with duplicated frames.
        start=("2019-04-02" if mode == "F" else "2017-12-18") if east else "2019-02-12",
        variables=ABI_BANDS,
        kwargs={"satellite": "goes16" if east else "goes17", "scan_mode": mode},
        by_date=(("2025-04-07", {"satellite": "goes19"}),)
        if east
        else (("2023-01-04", {"satellite": "goes18"}),),
        step="10min" if mode == "F" else "5min",
        frames_per_call=1 if mode == "F" else 6,
        description=(
            f"GOES-{slot.title()} ABI L2 MCMIP {sector.upper()}, 16 bands"
            + (" (Planetary Computer mirror)." if backend != "GOES" else ".")
        ),
        # Full disk is ~3.8 GB a frame as earth2studio's float64, plus the grid.
        memory_gb=24.0 if mode == "F" else 8.0,
        scheduled=mode == "C" and backend == "GOES",
    )


def _nnja_sat(sensor: str, since: str, until: str | None = None) -> Dataset:
    infrared = sensor in {"airs", "iasi", "cris"}
    return Dataset(
        name=f"nnja_sat_{sensor}",
        source="NNJAObsSat",
        kind="table",
        freq="1h",
        start=since,
        end=until,
        variables=(sensor,),
        description=f"NNJA NCEP {sensor.upper()} radiances (6-hourly GDAS dumps).",
        memory_gb=48.0 if infrared else 16.0,
        scheduled=False,
    )


def _metop(source: str, name: str, satellite: str | None, freq: str, memory: float) -> Dataset:
    suffix = f"_{satellite.split('-')[1]}" if satellite else ""
    return Dataset(
        name=f"{name}{suffix}",
        source=source,
        kind="table",
        freq=freq,
        start="2007-06-01"
        if satellite in (None, "metop-a")
        else "2013-01-01"
        if satellite == "metop-b"
        else "2019-01-01",
        variables=(name.removeprefix("metop_"),),
        kwargs={"satellite": satellite},
        credentials=EUMETSAT,
        description=f"EUMETSAT {name.replace('_', ' ').upper()} L1 observations"
        + (f", {satellite}." if satellite else ", all MetOp satellites."),
        memory_gb=memory,
        scheduled=memory <= 16,
    )


_DATASETS: list[Dataset] = [
    # --- In situ, NCEP ----------------------------------------------------------------
    Dataset(
        name="nomads_gdas_conv",
        source="NomadsGDASObsConv",
        kind="table",
        # Aligned to the cycles, which each cover [T-3h, T+3h).
        freq="6h",
        start="2026-09-28",
        variables=NCEP_CONV,
        end_offset=-2,
        description="Real-time GDAS conventional observations from NOMADS (2-day retention).",
    ),
    Dataset(
        name="nnja_conv",
        source="NNJAObsConv",
        kind="table",
        freq="1D",
        start="1979-01-01",
        variables=NCEP_CONV,
        # Satellite winds are their own dataset below.
        kwargs={"exclude_message_types": ["SATWND"]},
        description="NNJA PrepBUFR conventional observations and GPS-RO, 1979 onwards.",
        memory_gb=16.0,
    ),
    Dataset(
        name="nnja_satwnd",
        source="NNJAObsSatwnd",
        kind="table",
        freq="6h",
        start="1979-01-01",
        variables=("u", "v"),
        description="NNJA satellite atmospheric motion vectors, 1979 onwards.",
        memory_gb=32.0,
        scheduled=False,
    ),
    Dataset(
        name="ufs_conv",
        source="UFSObsConv",
        kind="table",
        freq="1D",
        start="1980-01-01",
        variables=("u", "v", "q", "t", "pres", "gps", "gps_t", "gps_q"),
        description="UFS GEFSv13 replay GSI conventional diagnostics, 1980 onwards.",
    ),
    Dataset(
        name="ufs_sat",
        source="UFSObsSat",
        kind="table",
        freq="1D",
        start="1980-01-01",
        variables=("airs", "atms", "mhs", "amsua", "amsub", "iasi", "crisfsr"),
        description="UFS GEFSv13 replay GSI satellite radiance diagnostics, 1980 onwards.",
        memory_gb=32.0,
        scheduled=False,
    ),
    # --- In situ, station networks -----------------------------------------------------
    Dataset(
        name="ghcn_daily",
        source="GHCNDaily",
        kind="table",
        # Above 500 stations the source reads NOAA's by-year bulk files, so a month
        # re-reads twelve year files, which is still far cheaper than per station.
        freq="1MS",
        start="1750-01-01",
        variables=(
            "t2m_max",
            "t2m_min",
            "t2m",
            "d2m",
            "r2m",
            "tp",
            "sf",
            "sd",
            "sde",
            "ws10m",
            "fg10m",
            "tcc",
        ),  # fmt: skip
        stations="ghcnd",
        description="GHCN-Daily, every station, all twelve elements.",
        memory_gb=16.0,
    ),
    Dataset(
        name="ghcn_hourly",
        source="GHCNHourly",
        kind="table",
        # One file per station-year: anything shorter re-reads every one of them.
        freq="1YS",
        start="1901-01-01",
        variables=("t2m", "d2m", "ws10m", "fg10m", "tp", "u10m", "v10m", "tcc"),
        stations="ghcnh",
        station_chunk=2000,
        kwargs={"async_workers": 48},
        end_offset=-1,
        description="GHCN-hourly, every station, one partition per year.",
        memory_gb=32.0,
        scheduled=False,
    ),
    Dataset(
        name="isd",
        source="ISD",
        kind="table",
        freq="1YS",
        start="1901-01-01",
        variables=("ws10m", "u10m", "v10m", "tp", "t2m", "fg10m", "d2m", "tcc"),
        stations="isd",
        # The source opens every station-year at once, with no concurrency limit.
        station_chunk=400,
        description="NOAA Integrated Surface Database, every station active in the year.",
        memory_gb=32.0,
        scheduled=False,
    ),
    Dataset(
        name="iem_asos",
        source="IEM_ASOS",
        kind="table",
        freq="1D",
        start="1928-01-01",
        variables=("t2m", "d2m", "r2m", "ws10m", "u10m", "v10m", "fg10m", "tp01", "msl", "tcc"),
        kwargs={"stations": None, "networks": None},
        description="Iowa Environmental Mesonet ASOS/AWOS/METAR, whole network.",
    ),
    Dataset(
        name="ibtracs",
        source="IBTrACS",
        kind="table",
        # One archive file, rescanned whole on every call: a year per call.
        freq="1YS",
        start="1842-01-01",
        variables=(
            "tcwnd",
            "mslp",
            "tcustm",
            "tcvstm",
            "tcr34",
            "tcr50",
            "tcr64",
            "tcsshs",
            "tcd2l",
        ),
        kwargs={"region": "ALL"},
        end_offset=0,
        description="IBTrACS tropical cyclone best tracks, all basins.",
    ),
    # --- Lightning ------------------------------------------------------------------------
    *[_glm(slot, level) for slot in ("east", "west") for level in GLM_LEVELS],
    *[_li(level) for level in LI_LEVELS],
    # --- Polar sounders and imagers, DataFrame ---------------------------------------
    Dataset(
        name="jpss_atms",
        source="JPSS_ATMS",
        kind="table",
        freq="1h",
        start="2023-09-06",
        variables=("atms",),
        # Files are selected by start time, so one beginning just before the hour
        # carries its first scan lines.
        lead="5min",
        description="JPSS ATMS L1 BUFR brightness temperatures, NOAA-20/21 and S-NPP.",
    ),
    *[
        Dataset(
            name=f"jpss_cris_{sat}",
            source="JPSS_CRIS",
            kind="table",
            # ~270M rows per satellite-hour at full spectral resolution.
            freq="10min",
            start="2023-09-06",
            variables=("crisfsr",),
            kwargs={"satellites": [sat]},
            lead="1min",
            description=f"JPSS CrIS FSR L1 brightness temperatures, {sat.upper()}.",
            memory_gb=16.0,
            scheduled=False,
        )
        for sat in ("n20", "n21", "npp")
    ],
    _metop("MetOpAMSUA", "metop_amsua", None, "1D", 8.0),
    _metop("MetOpMHS", "metop_mhs", None, "1D", 8.0),
    *[
        _metop("MetOpAVHRR", "metop_avhrr", s, "1h", 32.0)
        for s in ("metop-a", "metop-b", "metop-c")
    ],
    *[_metop("MetOpIASI", "metop_iasi", s, "1h", 32.0) for s in ("metop-a", "metop-b", "metop-c")],
    _nnja_sat("amsua", "1998-01-01"),
    _nnja_sat("amsub", "1998-01-01"),
    _nnja_sat("mhs", "2005-01-01"),
    _nnja_sat("atms", "2012-01-01"),
    _nnja_sat("airs", "2002-01-01", "2024-01-01"),
    _nnja_sat("iasi", "2008-01-01"),
    _nnja_sat("cris", "2018-01-01"),
    # --- Geostationary imagers, gridded ------------------------------------------------
    *[
        _goes(b, s, m)
        for b in ("GOES", "PlanetaryComputerGOES")
        for s in ("east", "west")
        for m in ("F", "C")
    ],
    *[
        Dataset(
            name=f"goes_glm_grid_{slot}",
            source="GOESGLMGrid",
            kind="grid",
            freq="1h",
            start="2018-02-13" if slot == "east" else "2018-12-10",
            variables=("glm_density", "glm_energy_density"),
            kwargs={"satellite": slot},
            step="5min",
            description=f"GOES-{slot.title()} GLM 0.1 degree CONUS lightning density, 5 min bins.",
        )
        for slot in ("east", "west")
    ],
    Dataset(
        name="himawari_ahi_fd",
        source="HimawariAHI",
        kind="grid",
        freq="1h",
        start="2015-07-07",
        variables=AHI_BANDS,
        kwargs={"satellite": "himawari8"},
        by_date=(("2022-12-13", {"satellite": "himawari9"}),),
        step="10min",
        frames_per_call=1,
        description="Himawari-8/9 AHI full disk, 16 bands at 2 km.",
        memory_gb=16.0,
        scheduled=False,
    ),
    Dataset(
        name="meteosat_fci_2km",
        source="MeteosatFCI",
        kind="grid",
        freq="1h",
        start="2024-01-16",
        variables=FCI_2KM,
        kwargs={"resolution": "2km", "async_timeout": 3600},
        step="10min",
        frames_per_call=1,
        credentials=EUMETSAT,
        description="MTG-I FCI L1C full disk, the eight 2 km infrared channels.",
        memory_gb=16.0,
        scheduled=False,
    ),
    Dataset(
        name="mrms_conus",
        source="MRMS",
        kind="grid",
        freq="1h",
        start="2020-10-14",
        variables=("refc", "refc_base"),
        # A missing frame would otherwise be filled silently from a neighbour.
        kwargs={"max_offset_minutes": 1},
        # MRMS encodes missing (-999) and no coverage (-99) in-band; earth2studio passes
        # them through.
        valid_min=-90.0,
        step="2min",
        frames_per_call=10,
        description="MRMS CONUS composite and base reflectivity, 2-minute frames.",
        memory_gb=16.0,
    ),
    Dataset(
        name="nclimgrid_daily",
        source="NClimGridDaily",
        kind="grid",
        freq="1MS",
        start="1951-01-01",
        variables=("t2m_max", "t2m_min", "t2m", "tp"),
        step="1D",
        # Scaled monthly files are posted one to three months after the month.
        end_offset=-3,
        description="NOAA NClimGrid daily CONUS temperature and precipitation.",
    ),
    Dataset(
        name="pc_oisst",
        source="PlanetaryComputerOISST",
        kind="grid",
        freq="1MS",
        start="1981-09-01",
        variables=("sst", "ssta", "sstu", "sic"),
        # Requests must be at 00:00: from 12:00 the next day's item matches first.
        step="1D",
        end_offset=-1,
        description="NOAA OISST v2.1 daily 0.25 degree SST and sea ice.",
    ),
    *[
        Dataset(
            name=f"pc_modis_fire_{platform.lower()}",
            source="PlanetaryComputerMODISFire",
            kind="grid",
            freq="1D",
            start="2000-02-24" if platform == "MOD" else "2002-07-04",
            # Only FireMask is valid: earth2studio returns FireMask for every variable.
            variables=("fmask",),
            # Terra and Aqua share the collection and earth2studio takes whichever item the
            # server lists first; the platform is pinned when choosing among them.
            kwargs={"platform": f"{platform}14A1"},
            tiles=MODIS_TILES,
            step="1D",
            dtype="uint8",
            end_offset=-16,
            description=f"MODIS {platform}14A1 daily FireMask, all 294 land tiles.",
            memory_gb=4.0,
        )
        for platform in ("MOD", "MYD")
    ],
    # --- Swaths, stacked by granule ----------------------------------------------------
    *[
        Dataset(
            name=f"jpss_viirs_{sat.replace('-', '')}_{band.lower()}",
            source="JPSS",
            kind="granules",
            freq="1h",
            start={"noaa-20": "2018-01-05", "noaa-21": "2023-02-10", "snpp": "2012-01-19"}[sat],
            variables=VIIRS_I if band == "I" else VIIRS_M,
            kwargs={"satellite": sat, "product_type": band},
            granules="viirs",
            frames_per_call=4,
            description=f"JPSS VIIRS {band}-band SDR radiances, {sat.upper()}, granule stack.",
            memory_gb=16.0,
            scheduled=False,
        )
        for sat in ("noaa-20", "noaa-21", "snpp")
        for band in ("I", "M")
    ],
    Dataset(
        name="pc_sentinel3_aod",
        source="PlanetaryComputerSentinel3AOD",
        kind="granules",
        freq="1D",
        start="2020-04-16",
        variables=S3_AOD,
        granules="s3aod",
        frames_per_call=8,
        end_offset=-3,
        description="Sentinel-3 A/B SYNERGY L2 aerosol optical depth, granule stack.",
        memory_gb=8.0,
    ),
]

DATASETS: dict[str, Dataset] = {d.name: d for d in _DATASETS}

#: Every earth2studio source these datasets cover.
SOURCES = sorted({d.source for d in _DATASETS})


class IncompletePartition(RuntimeError):
    """A partition could not be fetched in full."""


def to_naive_utc(value: str | dt.datetime) -> dt.datetime:
    """Parse a time, returning naive UTC as earth2studio expects."""
    when = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(value)
    if when.tzinfo is not None:
        when = when.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return when


def partition_window(dataset: Dataset, start: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    """``[start, end)`` of the partition beginning at ``start``."""
    import pandas as pd

    offset = pd.tseries.frequencies.to_offset(dataset.freq)
    begin = pd.Timestamp(start)
    # Every timestamp is "on" a Day or Hour offset as pandas sees it, so fixed-length
    # partitions are checked by flooring instead.
    if isinstance(offset, pd.offsets.Tick):
        aligned = begin.floor(offset) == begin
    else:
        aligned = offset.is_on_offset(begin) and begin == begin.normalize()
    if not aligned:
        raise ValueError(f"{start} is not the start of a {dataset.freq} partition")
    return begin.to_pydatetime(), (begin + offset).to_pydatetime()


def staged_dir(target: str | os.PathLike, dataset: Dataset, start: dt.datetime) -> pathlib.Path:
    """Directory an ``<name>`` partition is staged in."""
    return pathlib.Path(target) / dataset.name / start.strftime("%Y/%m/%d")


def stamp(when: dt.datetime) -> str:
    """The ``YYYYMMDDhhmm`` stamp staged files carry."""
    return when.strftime("%Y%m%d%H%M")


def manifest_path(target, dataset: Dataset, start: dt.datetime) -> pathlib.Path:
    """The manifest marking a staged partition complete."""
    return staged_dir(target, dataset, start) / f"{dataset.name}_{stamp(start)}.json"


def _e2s_class(name: str):
    import earth2studio.data as e2s_data

    return getattr(e2s_data, name)


# --- stations -------------------------------------------------------------------------


def list_stations(network: str, start: dt.datetime, end: dt.datetime) -> list[str]:
    """Every station of ``network``, narrowed to the window where the metadata allows.

    ``get_stations_bbox`` is avoided: a (-90, -180, 90, 180) box shifts longitudes to
    [0, 360) before filtering and so returns only the eastern hemisphere.
    """
    import pandas as pd

    if network in ("ghcnd", "ghcnh"):
        cls = _e2s_class("GHCNDaily" if network == "ghcnd" else "GHCNHourly")
        return sorted(cls.get_station_metadata()["ID"].astype(str).unique())
    if network == "isd":
        history = _e2s_class("ISD").get_station_history()
        cols = {c.lower(): c for c in history.columns}
        begin = pd.to_datetime(history[cols["begin"]].astype(str), format="%Y%m%d", errors="coerce")
        finish = pd.to_datetime(history[cols["end"]].astype(str), format="%Y%m%d", errors="coerce")
        active = history[(begin < end) & (finish >= start)].copy()
        wban = pd.to_numeric(active[cols["wban"]], errors="coerce").fillna(0).astype(int)
        usaf = active[cols["usaf"]].astype(str).str.zfill(6)
        return sorted(set(usaf + wban.map(lambda x: f"{x:05d}")))
    raise ValueError(f"unknown station network {network!r}")


def chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    """``items`` in slices of ``size`` (all at once when ``size`` is 0)."""
    if not size:
        yield items
        return
    for i in range(0, len(items), size):
        yield items[i : i + size]


# --- tables -----------------------------------------------------------------------------


def to_arrow(df, schema):
    """Cast a source's DataFrame to its own ``SCHEMA``.

    Empty results arrive with object columns, and some sources return categoricals or
    millisecond times, so without this the files of one dataset would disagree on types.
    Columns the source did not return are left out rather than invented.
    """
    import pandas as pd
    import pyarrow as pa

    fields = [f for f in schema if f.name in df.columns]
    missing = [f.name for f in schema if f.name not in df.columns]
    if missing:
        logger.warning(f"columns absent from the result: {missing}")
    target = pa.schema(fields, metadata=schema.metadata)
    if df.empty:
        return target.empty_table()
    frame = df[[f.name for f in fields]].copy()
    for name in frame.columns:
        if isinstance(frame[name].dtype, pd.CategoricalDtype):
            frame[name] = frame[name].astype(object)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    return table.cast(target, safe=False)


def fetch_table(dataset: Dataset, start: dt.datetime, end: dt.datetime, factory=None):
    """Fetch one partition of a DataFrame source as an Arrow table."""
    import pandas as pd
    import pyarrow as pa

    cls = factory or _e2s_class(dataset.source)
    lead = pd.Timedelta(dataset.lead).to_pytimedelta()
    kwargs = {
        "time_tolerance": (-lead, end - start),
        "cache": True,
        "verbose": False,
        "async_timeout": TABLE_TIMEOUT_S,
        **dataset.kwargs_for(start),
    }
    station_groups: list = [None]
    if dataset.stations:
        stations = list_stations(dataset.stations, start, end)
        logger.info(f"{dataset.name}: {len(stations)} stations")
        station_groups = list(chunks(stations, dataset.station_chunk))

    tables = []
    for group in station_groups:
        if group is not None:
            kwargs["stations"] = list(group)
        source = cls(**kwargs)
        df = source([start], list(dataset.variables))
        if len(df):
            df = df[(df["time"] >= pd.Timestamp(start)) & (df["time"] < pd.Timestamp(end))]
        tables.append(to_arrow(df, cls.SCHEMA))
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


# --- grids and granules -----------------------------------------------------------------


def to_dataset(array, dtype: str = "float32", valid_min: float | None = None):
    """Turn an earth2studio ``[time, variable, ...]`` array into a dataset to stage.

    One data variable per earth2studio variable. Geolocation is renamed to
    ``latitude``/``longitude`` whether it arrives as 2-D ``_lat``/``_lon`` coordinates,
    1-D ``lat``/``lon`` dimensions, or as entries along ``variable`` (VIIRS and
    Sentinel-3), where it stays a per-granule variable. Attributes that netCDF cannot hold
    are dropped.
    """
    import numpy as np

    ds = array.to_dataset(dim="variable")
    renames = {
        old: new
        for old, new in {
            "_lat": "latitude",
            "_lon": "longitude",
            "lat": "latitude",
            "lon": "longitude",
            "s3sy_lat": "latitude",
            "s3sy_lon": "longitude",
        }.items()
        if old in ds.variables
    }
    ds = ds.rename(renames)
    for name in list(ds.data_vars):
        if valid_min is not None and name not in ("latitude", "longitude"):
            ds[name] = ds[name].where(ds[name] >= valid_min)
        if name in ("latitude", "longitude"):
            ds[name] = ds[name].astype("float32")
        elif np.dtype(dtype).kind in "ui":
            ds[name] = ds[name].fillna(0).astype(dtype)
        elif ds[name].dtype.kind == "f":
            ds[name] = ds[name].astype(dtype)
    for name in ("latitude", "longitude"):
        if name in ds.coords and ds[name].dtype == np.float64 and ds[name].ndim > 1:
            ds = ds.assign_coords({name: ds[name].astype("float32")})
    plain = (str, int, float, np.integer, np.floating)
    ds.attrs = {k: v for k, v in array.attrs.items() if isinstance(v, plain)}
    for name in ds.variables:
        ds[name].attrs = {k: v for k, v in ds[name].attrs.items() if isinstance(v, plain)}
    return ds


def write_netcdf(ds, path: pathlib.Path) -> pathlib.Path:
    """Write ``ds`` to ``path`` atomically, compressed, one chunk per frame."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    encoding = {
        name: {"zlib": True, "complevel": 4, "chunksizes": (1, *ds[name].shape[1:])}
        for name in ds.data_vars
        if ds[name].dims and ds[name].dims[0] == "time"
    }
    try:
        ds.to_netcdf(partial, engine="netcdf4", format="NETCDF4", encoding=encoding)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    return path


def _platform_source(cls, platform: str):
    """``cls`` restricted to items of one platform, and to the item covering the date.

    Planetary Computer's ``iLike`` is effectively case-sensitive and earth2studio
    lowercases the tile filter, so the platform (``MOD14A1``/``MYD14A1``) cannot go in
    the search; it is applied when choosing among the results instead. Among those, the
    8-day composite whose interval contains the requested day is taken, rather than
    relying on the server's ordering.
    """

    class PlatformItems(cls):
        def _select_item(self, items, when):
            target = to_naive_utc(when)
            mine = [item for item in items if item.id.startswith(platform)]
            if not mine:
                raise FileNotFoundError(f"no {platform} item for {target:%Y-%m-%d}")

            def covers(item):
                props = item.properties
                begin = to_naive_utc(props.get("start_datetime", "1900-01-01T00:00:00+00:00"))
                end = to_naive_utc(props.get("end_datetime", "2999-01-01T00:00:00+00:00"))
                return begin <= target <= end

            return next((item for item in mine if covers(item)), mine[0])

    return PlatformItems


class _TiledSource:
    """Fetch every tile of a tiled source and stack them along ``tile``.

    One source instance serves every tile, with its tile filter swapped in between: the
    instance holds the Planetary Computer stores and their SAS credentials, and building
    294 of them a day to fetch 294 tiles got downloads refused for want of valid
    authentication.

    A tile with no published item (the Antarctic tiles in polar night, for example) is
    zero-filled, FireMask's "not processed" class, and listed in :attr:`absent`. Any other
    failure is retried a few times and then listed in :attr:`missing`, which fails the
    partition unless partial partitions are allowed.
    """

    #: Attempts per tile before it is reported missing.
    attempts: int = 3

    def __init__(self, dataset: Dataset, cls, kwargs: dict):
        self.dataset = dataset
        platform = kwargs.pop("platform", None)
        self.cls = _platform_source(cls, platform) if platform else cls
        self.kwargs = kwargs
        self.missing: list[str] = []
        self.absent: list[str] = []
        self._source = None

    def _for_tile(self, tile: str):
        if self._source is None or not hasattr(self._source, "_search_kwargs"):
            self._source = self.cls(tile=tile, **self.kwargs)
        else:
            # Same filter the constructor builds (earth2studio planetary_computer.py).
            self._source._search_kwargs = {
                "filter": {"op": "iLike", "args": [{"property": "id"}, f"%{tile.lower()}%"]}
            }
        return self._source

    def _fetch_tile(self, tile: str, times, variables):
        import time

        for attempt in range(1, self.attempts + 1):
            try:
                return self._for_tile(tile)(times, variables)
            except FileNotFoundError as exc:
                # The STAC search found no item: nothing is published for this tile.
                logger.info(f"{self.dataset.name} {tile}: {exc}")
                self.absent.append(tile)
                return None
            except Exception as exc:  # noqa: BLE001 - transport and auth errors vary in type
                logger.warning(f"{self.dataset.name} {tile} attempt {attempt}: {exc}")
                if attempt < self.attempts:
                    time.sleep(5 * attempt)
        self.missing.append(tile)
        return None

    def __call__(self, times, variables):
        import numpy as np
        import xarray as xr

        arrays = [self._fetch_tile(tile, times, variables) for tile in self.dataset.tiles]
        template = next((a for a in arrays if a is not None), None)
        if template is None:
            raise IncompletePartition(f"{self.dataset.name}: no tile could be fetched")
        filled = [a if a is not None else xr.zeros_like(template) for a in arrays]
        stacked = xr.concat(filled, dim="tile").assign_coords(tile=list(self.dataset.tiles))
        return stacked.transpose("time", "variable", "tile", ...).astype(np.float32)


_VIIRS_STAMP = re.compile(r"_d(\d{8})_t(\d{6})(\d)_")


def viirs_granule_time(path: str) -> dt.datetime | None:
    """Start time encoded in a VIIRS SDR filename, e.g. ``..._d20260920_t0001033_...``.

    The last digit of the ``t`` field is tenths of a second.
    """
    match = _VIIRS_STAMP.search(path)
    if not match:
        return None
    when = dt.datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H%M%S")
    return when + dt.timedelta(milliseconds=100 * int(match.group(3)))


def list_viirs_granules(
    dataset: Dataset, start: dt.datetime, end: dt.datetime
) -> list[dt.datetime]:
    """Start times of the VIIRS SDR granules beginning in ``[start, end)``."""
    import obstore
    from earth2studio.lexicon import JPSSLexicon
    from obstore.store import S3Store

    cls = _e2s_class("JPSS")
    bucket = cls.SATELLITE_BUCKETS[dataset.kwargs_for(start)["satellite"]]
    folder = JPSSLexicon.get_item(dataset.variables[0])[1]
    store = S3Store(bucket, region="us-east-1", skip_signature=True)
    found = set()
    day = dt.datetime(start.year, start.month, start.day)
    while day < end:
        for batch in obstore.list(store, prefix=f"{folder}/{day:%Y/%m/%d}/"):
            for meta in batch:
                when = viirs_granule_time(meta["path"])
                if when is not None and start <= when < end:
                    found.add(when)
        day += dt.timedelta(days=1)
    return sorted(found)


def list_s3aod_granules(
    dataset: Dataset, start: dt.datetime, end: dt.datetime
) -> list[dt.datetime]:
    """Nominal (mid-granule) times of the SYNERGY AOD granules in ``[start, end)``."""
    from pystac_client import Client

    cls = _e2s_class(dataset.source)
    client = Client.open(cls.STAC_API_URL)
    search = client.search(
        collections=[cls.COLLECTION_ID],
        datetime=f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}",
    )
    times = set()
    for item in search.items():
        when = item.datetime.astimezone(dt.timezone.utc).replace(tzinfo=None)
        if start <= when < end:
            times.add(when)
    return sorted(times)


def _granule_source(dataset: Dataset, cls):
    """The earth2studio class, adjusted to return exactly the granule asked for.

    The Sentinel-3 source searches +/-12 h and takes the first item the server lists, so
    it is narrowed to a few seconds and made to pick the item nearest the request. VIIRS
    already picks the nearest granule, so exact start times are enough.
    """
    if dataset.granules != "s3aod":
        return cls

    class ExactGranule(cls):
        SEARCH_TOLERANCE = dt.timedelta(seconds=5)

        def _select_item(self, items, when):
            # earth2studio hands over a tz-aware time here; compare everything as naive UTC.
            target = to_naive_utc(when)

            def offset(item):
                nominal = item.datetime.astimezone(dt.timezone.utc).replace(tzinfo=None)
                return abs(nominal - target)

            return min(items, key=offset)

    return ExactGranule


def frame_times(dataset: Dataset, start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """The frames a ``grid`` partition should contain."""
    import pandas as pd

    return [
        t.to_pydatetime() for t in pd.date_range(start, end, freq=dataset.step, inclusive="left")
    ]


def fetch_frames(
    dataset: Dataset,
    start: dt.datetime,
    end: dt.datetime,
    target: str | os.PathLike,
    allow_partial: bool = False,
    factory=None,
) -> dict[str, Any]:
    """Fetch a ``grid`` or ``granules`` partition, staging one netCDF file per batch."""
    cls = factory or _e2s_class(dataset.source)
    kwargs = {
        "cache": True,
        "verbose": False,
        "async_timeout": GRID_TIMEOUT_S,
        **dataset.kwargs_for(start),
    }
    if dataset.kind == "granules":
        lister = {"viirs": list_viirs_granules, "s3aod": list_s3aod_granules}[dataset.granules]
        wanted = lister(dataset, start, end)
        source = _granule_source(dataset, cls)(**kwargs)
    else:
        wanted = frame_times(dataset, start, end)
        source = _TiledSource(dataset, cls, kwargs) if dataset.tiles else cls(**kwargs)

    directory = staged_dir(target, dataset, start)
    files, missing = [], []
    for i, batch in enumerate(chunks(wanted, dataset.frames_per_call)):
        try:
            array = source(list(batch), list(dataset.variables))
        except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
            logger.warning(f"{dataset.name} {batch[0]:%Y-%m-%dT%H:%M}: {exc}")
            missing.extend(batch)
            continue
        path = directory / f"{dataset.name}_{stamp(start)}_{i:03d}.nc"
        write_netcdf(to_dataset(array, dataset.dtype, dataset.valid_min), path)
        files.append(path.name)

    summary = {
        "dataset": dataset.name,
        "start": start.isoformat(),
        "frames": [t.isoformat() for t in wanted if t not in missing],
        "missing_frames": [t.isoformat() for t in missing],
        "missing_tiles": list(getattr(source, "missing", [])),
        "absent_tiles": list(getattr(source, "absent", [])),
        "files": files,
    }
    missing_tiles = list(getattr(source, "missing", []))
    if missing_tiles and not allow_partial:
        missing = missing or wanted
    if missing and not allow_partial:
        for name in files:
            (directory / name).unlink(missing_ok=True)
        raise IncompletePartition(
            f"{dataset.name} {start:%Y-%m-%dT%H:%M}: {len(missing)} of {len(wanted)} "
            f"frames failed: {[t.strftime('%H:%M') for t in missing][:12]}"
            + (f"; tiles missing: {missing_tiles[:12]}" if missing_tiles else "")
        )
    return summary


# --- entry point -------------------------------------------------------------------------


def write_manifest(target, dataset: Dataset, start: dt.datetime, summary: dict) -> pathlib.Path:
    """Write the manifest last, atomically: its presence means the partition is staged."""
    path = manifest_path(target, dataset, start)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    partial.write_text(json.dumps(summary, indent=1))
    os.replace(partial, path)
    return path


def download(
    name: str,
    when: str | dt.datetime,
    target: str | os.PathLike,
    allow_partial: bool = False,
    factory: Callable | None = None,
) -> dict[str, Any]:
    """Fetch the partition of dataset ``name`` starting at ``when`` and stage it.

    Args:
        name: A key of :data:`DATASETS`.
        when: Partition start, UTC; a naive value is taken as UTC.
        target: Staging root.
        allow_partial: Stage a gridded partition with some frames missing.
        factory: Stand-in for the earth2studio class, for tests.

    Returns:
        A JSON-serialisable summary, also written as the partition's manifest.
    """
    import pyarrow.parquet as pq

    dataset = DATASETS[name]
    missing_env = [v for v in dataset.credentials if not os.environ.get(v)]
    if missing_env:
        raise RuntimeError(f"{name} needs {', '.join(missing_env)}")
    start, end = partition_window(dataset, to_naive_utc(when))
    if start < dt.datetime.fromisoformat(dataset.start):
        raise ValueError(f"{name} starts at {dataset.start}")
    if dataset.end and start >= dt.datetime.fromisoformat(dataset.end):
        raise ValueError(f"{name} ends before {dataset.end}")

    if dataset.kind == "table":
        table = fetch_table(dataset, start, end, factory)
        path = staged_dir(target, dataset, start) / f"{name}_{stamp(start)}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".part")
        pq.write_table(table, partial, compression="zstd")
        os.replace(partial, path)
        summary = {
            "dataset": name,
            "start": start.isoformat(),
            "rows": table.num_rows,
            "files": [path.name],
        }
    else:
        summary = fetch_frames(dataset, start, end, target, allow_partial, factory)

    write_manifest(target, dataset, start, summary)
    logger.info(f"{name} {start:%Y-%m-%dT%H:%M}: staged {summary}")
    return summary


def pipes_metadata(summary: dict[str, Any]) -> dict[str, Any]:
    """Tag container values as JSON for Dagster Pipes, which rejects untagged dicts."""
    return {
        key: {"raw_value": value, "type": "json"} if isinstance(value, (dict, list)) else value
        for key, value in summary.items()
    }


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dataset", required=True, choices=sorted(DATASETS))
    parser.add_argument("--time", required=True, help="Partition start, ISO 8601, UTC.")
    parser.add_argument(
        "--target",
        default=os.environ.get("E2S_OBS_ARCHIVE_DIR", DEFAULT_TARGET),
        help=f"Staging root (default: $E2S_OBS_ARCHIVE_DIR or {DEFAULT_TARGET}).",
    )
    parser.add_argument("--allow-partial", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; reports through Dagster Pipes when launched by it."""
    args = _parse_args(argv)

    def run() -> dict[str, Any]:
        return download(args.dataset, args.time, args.target, allow_partial=args.allow_partial)

    if not os.environ.get("DAGSTER_PIPES_CONTEXT"):
        run()
        return 0

    from dagster_pipes import open_dagster_pipes

    with open_dagster_pipes() as pipes:
        pipes.report_asset_materialization(metadata=pipes_metadata(run()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
