"""DWD ICON providers: the global/EU forecast models and the ICON-D2 rapid update cycle.

Deutscher Wetterdienst publishes ICON on its open data server at
https://opendata.dwd.de/weather/nwp. Two directory layouts are in use and both are
supported here:

``filename`` layout
    ``<base>/<model>/grib/<run>/<var>/<var_url>_<YYYYMMDDHH>_<step>_[<level>_]<VAR>.grib2.bz2``.
    This is the long-standing layout for ICON global, ICON-EU and their ensembles.

``path`` layout
    ``<base>/v1/m/<model>/p/<VAR>[/wvl1/<w>][/lvt1/<t>/lv1/<l>]/r/<run>/s/PT<step>H00M.grib2``.
    Uncompressed, used by the newer ICON-ART products and by ICON-D2-RUC.

Two providers live here:

:class:`ICONProvider`
    Downloads one model run, merges single-level, pressure-level and model-level GRIB into
    a single dataset with an ``init_time`` axis, and writes it to icechunk. It can also
    publish the result to the Hugging Face Hub (``openclimatefix/dwd-icon-global`` /
    ``dwd-icon-eu`` by default, overridable with ``HF_REPO_ID``).

:class:`ICOND2RUCProvider`
    Reads an existing local mirror of the ICON-D2 rapid update cycle and splits it into the
    hourly, 15-minute and 5-minute stores, since DWD publishes those three cadences
    interleaved in one tree.

:func:`write_model_level_half_heights` writes the static half-level height field (``HHL``)
that the model-level datasets are indexed against.
"""

from __future__ import annotations

import bz2
import dataclasses
import datetime as dt
import os
import pathlib
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Sequence

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.download import download_one
from planetary_datasets.common.store import STORE_READ_ERRORS, has_committed_data
from planetary_datasets.config import Config

DWD_OPENDATA = "https://opendata.dwd.de/weather/nwp"


class IncompleteRun(Exception):
    """A run was only partly published when it was fetched.

    Raised rather than returned so the partition fails and Dagster retries it. Returning
    False would mark the partition green, and the run would never be revisited.
    """

# ---------------------------------------------------------------------------------------
# Variable lists
# ---------------------------------------------------------------------------------------

VAR_2D_GLOBAL = [
    "alb_rad", "alhfl_s", "ashfl_s", "asob_s", "asob_t", "aswdifd_s", "aswdifu_s",
    "aswdir_s", "athb_s", "athb_t", "aumfl_s", "avmfl_s", "cape_con", "cape_ml", "clch",
    "clcl", "clcm", "clct", "clct_mod", "cldepth", "c_t_lk", "freshsnw", "fr_ice",
    "h_snow", "h_ice", "h_ml_lk", "hbas_con", "htop_con", "htop_dc", "hzerocl", "pmsl",
    "ps", "qv_s", "rain_con", "rain_gsp", "relhum_2m", "rho_snow", "runoff_g", "runoff_s",
    "snow_con", "snow_gsp", "t_2m", "t_g", "t_snow", "t_ice", "tch", "tcm", "td_2m",
    "tmax_2m", "tmin_2m", "tot_prec", "tqc", "tqi", "tqr", "tqs", "tqv", "u_10m", "v_10m",
    "vmax_10m", "w_snow", "ww", "z0",
]

VAR_2D_EUROPE = [
    "alb_rad", "alhfl_s", "ashfl_s", "asob_s", "asob_t", "aswdifd_s", "aswdifu_s",
    "aswdir_s", "athb_s", "athb_t", "aumfl_s", "avmfl_s", "cape_con", "cape_ml", "clch",
    "clcl", "clcm", "clct", "clct_mod", "cldepth", "evap_pl", "h_snow", "hbas_con",
    "htop_con", "htop_dc", "hsnow_max", "hzerocl", "pmsl", "ps", "qv_2m", "qv_s",
    "rain_con", "rain_gsp", "relhum_2m", "rho_snow", "runoff_g", "runoff_s", "snow_con",
    "snow_gsp", "snowlmt", "t_2m", "t_g", "t_snow", "tch", "tcm", "td_2m", "tmax_2m",
    "tmin_2m", "tot_prec", "tqc", "tqi", "u_10m", "v_10m", "vmax_10m", "w_snow", "ww",
    "z0",
]

VAR_2D_ENSEMBLE_GLOBAL = [
    "asob_s", "aswdir_s", "athb_s", "relhum_2m", "sobs_rad", "t_2m", "td_2m", "thbs_rad",
    "tot_prec", "u_10m", "v_10m", "vmax_10m", "clct",
]

VAR_2D_ENSEMBLE_EUROPE = [
    "aswdifd_s", "aswdir_s", "athb_s", "t_2m", "snow_con", "snow_gsp", "sobs_rad",
    "thbs_rad", "tot_prec", "u_10m", "v_10m", "vmax_10m", "tqv", "ps", "cape_ml",
]

# Pressure-level variables. "p", "omega", "clc", "qv", "tke" and "w" are model-level only
# in the global model, so they live in VAR_MODEL_GLOBAL instead.
VAR_3D_GLOBAL = ["fi", "relhum", "t", "u", "v"]
VAR_3D_EUROPE = ["clc", "fi", "omega", "relhum", "t", "u", "v"]
VAR_3D_ENSEMBLE_EUROPE = ["fi", "u", "v", "t"]

# Model-level (generalised vertical height coordinate) variables.
VAR_MODEL_GLOBAL = ["t", "u", "v", "w", "p", "qv", "qc", "qi", "clc", "tke"]
VAR_MODEL_EUROPE = ["u", "v", "w"]

INVARIANT_VARS = ["clat", "clon"]

#: Variables that carry no meaningful precision beyond float16 and dominate store size.
REDUCE_PRECISION_VARS_GLOBAL = [
    "u", "v", "w", "t", "clc", "clch", "clcl", "clcm", "clct", "t_2m", "t_g", "t_snow",
    "t_ice", "u_10m", "v_10m", "vmax_10m", "tmax_2m", "tmin_2m",
]

PRESSURE_LEVELS_GLOBAL = [
    1000, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30,
]

PRESSURE_LEVELS_EUROPE = [
    1000, 950, 925, 900, 875, 850, 825, 800, 775, 700, 600, 500, 400, 300, 250, 200, 150,
    100, 70, 50,
]

#: ICON global has 120 full levels and 121 half levels.
MODEL_LEVELS_GLOBAL = list(range(1, 122))
MODEL_LEVELS_EUROPE = list(range(1, 76))

# ICON-ART: aerosol and dust, published only under the newer "path" layout.
ART_3D_VARS = ["FI", "RELHUM"]

ART_2D_VARS = [
    "ACCDRYDEPO_DUSTA", "ACCDRYDEPO_DUSTB", "ACCDRYDEPO_DUSTC", "ACCEMISS_DUSTA",
    "ACCEMISS_DUSTB", "ACCEMISS_DUSTC", "ACCSEDIM_DUSTA", "ACCSEDIM_DUSTB",
    "ACCSEDIM_DUSTC", "ACCWETDEPO_CON_DUSTA", "ACCWETDEPO_CON_DUSTB",
    "ACCWETDEPO_CON_DUSTC", "ACCWETDEPO_GSP_DUSTA", "ACCWETDEPO_GSP_DUSTB",
    "ACCWETDEPO_GSP_DUSTC", "ALB_RAD", "ALHFL_S", "ASHFL_S", "ASOB_S", "ASOB_T",
    "ASWDIFD_S", "ASWDIFU_S", "ASWDIR_S", "ATHB_S", "ATHB_T", "AUMFL_S", "AVMFL_S",
    "CAPE_CON", "CAPE_ML", "CLCH", "CLCL", "CLCM", "CLCT", "CLCT_MOD", "CLDEPTH", "C_T_LK",
    "DUST_TOTAL_MC_VI", "FRESHSNW", "FR_ICE", "HBAS_CON", "HTOP_CON", "HTOP_DC", "HZEROCL",
    "H_ICE", "H_ML_LK", "H_SNOW", "LPI_CON_MAX", "PMSL", "PS", "QV_S", "RAIN_CON",
    "RAIN_GSP", "RELHUM_2M", "RHO_SNOW", "ROOTDP", "RUNOFF_G", "RUNOFF_S", "SNOW_CON",
    "SNOW_GSP", "TAOD_DUST", "TCH", "TCM", "TD_2M", "T_2M", "TOT_PREC", "TQC", "TQI",
    "TQR", "TQS", "TQV", "U_10M", "V_10M", "VMAX_10M", "T_SNOW", "W_SNOW",
]

ART_ENSEMBLE_2D_VARS = [
    v for v in ART_2D_VARS
    if v not in {
        "ALB_RAD", "AUMFL_S", "AVMFL_S", "CAPE_CON", "C_T_LK", "HTOP_DC", "HZEROCL",
        "H_ML_LK", "QV_S", "RHO_SNOW", "ROOTDP", "RUNOFF_G", "RUNOFF_S", "TQR", "TQS",
    }
]

ART_SOIL_VARS = ["T_SO", "W_SO", "W_SO_ICE", "SMI"]

ART_MODEL_VARS = [
    "T", "U", "V", "W", "P", "DUSTA", "DUSTB", "DUSTC", "DUSTA0", "DUSTB0", "DUSTC0",
    "DUST_MAX_TOTAL_MC_LAYER", "DUST_TOTAL_MC", "CEIL_BSC_DUST", "SAT_BSC_DUST", "AER_DUST",
]

ART_ENSEMBLE_MODEL_VARS = [v for v in ART_MODEL_VARS if v != "DUST_MAX_TOTAL_MC_LAYER"]

ART_PRESSURE_LEVELS = [
    3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000, 40000, 50000, 60000, 70000, 80000,
    85000, 90000, 92500, 95000, 100000,
]

#: Widest range published; most ART fields only cover 118-120.
ART_MODEL_LEVELS = list(range(77, 121))

ART_WAVELENGTHS = [532, 1064]

ART_SOIL_LEVELS = [
    0.0, 0.005, 0.01, 0.02, 0.03, 0.06, 0.09, 0.18, 0.27, 0.54, 0.81, 1.62, 2.43, 4.86,
    7.29, 14.58,
]

#: GRIB level types used by the "path" layout, from ``typeOfFirstFixedSurface``.
LEVEL_TYPE_PRESSURE = 100
LEVEL_TYPE_SOIL = 106
LEVEL_TYPE_MODEL = 150


# ---------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ICONConfig:
    """Everything that differs between the ICON variants.

    Attributes:
        store_prefix: Icechunk store location relative to the configured bucket.
        base_url: Root of the DWD open data NWP tree.
        model_url: Model-specific path segment, e.g. ``icon/grib`` or ``icon-art/p``.
        var_url: Filename stem used by the ``filename`` layout, e.g.
            ``icon_global_icosahedral``.
        f_steps: Forecast steps, in hours, to retrieve.
        chunking: Chunk sizes applied before writing. Keys absent from the dataset are
            ignored, so one dict can serve several variants.
        vars_2d: Single-level variable shortnames.
        vars_3d: Pressure-level variables as ``name@level``.
        vars_model: Model-level variables as ``name@level``.
        vars_soil: Soil-level variables as ``name@depth``.
        vars_wavelength: Wavelength-resolved variables as ``name@wavelength@level``.
        vars_invariant: Time-invariant variables, e.g. the icosahedral grid coordinates.
        reduced_precision_vars: Variables downcast to float16 before writing.
        hf_repo_id: Default Hugging Face dataset repository for :meth:`ICONProvider.publish`.
        url_style: ``filename`` for the bz2 layout, ``path`` for the ``/v1/m/`` layout.
        uppercase_names: Whether variable shortnames are upper-cased in filenames. The
            ensemble products use lower case.
        ignore_grib_errors: Pass ``errors="ignore"`` to cfgrib for level-type variables.
            Needed for the global icosahedral files, which mix level types.
    """

    store_prefix: str
    base_url: str = DWD_OPENDATA
    model_url: str = "icon/grib"
    var_url: str = "icon_global_icosahedral"
    f_steps: Sequence[int] = dataclasses.field(default_factory=lambda: list(range(0, 79)))
    chunking: dict[str, int] = dataclasses.field(default_factory=dict)
    vars_2d: Sequence[str] = ()
    vars_3d: Sequence[str] = ()
    vars_model: Sequence[str] = ()
    vars_soil: Sequence[str] = ()
    vars_wavelength: Sequence[str] = ()
    vars_invariant: Sequence[str] = ()
    reduced_precision_vars: Sequence[str] = ()
    hf_repo_id: str | None = None
    url_style: str = "filename"
    uppercase_names: bool = True
    ignore_grib_errors: bool = False

    def __post_init__(self) -> None:
        """Validate the variant before anything tries to download with it."""
        if self.url_style not in ("filename", "path"):
            raise ValueError(f"url_style must be 'filename' or 'path', got {self.url_style!r}")
        if not any((self.vars_2d, self.vars_3d, self.vars_model, self.vars_soil,
                    self.vars_wavelength)):
            raise ValueError("an ICON variant needs at least one 2D, 3D, model or soil variable")

    def case(self, name: str) -> str:
        """Return ``name`` in the case the variant uses in its file paths."""
        return name.upper() if self.uppercase_names else name.lower()


def _leveled(names: Iterable[str], levels: Iterable) -> list[str]:
    """Expand ``["t", "u"]`` and ``[500, 850]`` into ``["t@500", "t@850", ...]``."""
    return [f"{name}@{level}" for name in names for level in levels]


GLOBAL_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global.icechunk",
    model_url="icon/grib",
    var_url="icon_global_icosahedral",
    vars_2d=VAR_2D_GLOBAL,
    vars_3d=_leveled(VAR_3D_GLOBAL, PRESSURE_LEVELS_GLOBAL),
    vars_invariant=INVARIANT_VARS,
    f_steps=list(range(0, 79)),
    reduced_precision_vars=REDUCE_PRECISION_VARS_GLOBAL,
    hf_repo_id="openclimatefix/dwd-icon-global",
    ignore_grib_errors=True,
    chunking={"init_time": 1, "step": 37, "values": 122500, "isobaricInhPa": -1},
)

GLOBAL_MODEL_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global_model_level.icechunk",
    model_url="icon/grib",
    var_url="icon_global_icosahedral",
    vars_model=_leveled(VAR_MODEL_GLOBAL, MODEL_LEVELS_GLOBAL),
    vars_invariant=INVARIANT_VARS,
    f_steps=list(range(0, 79)),
    reduced_precision_vars=REDUCE_PRECISION_VARS_GLOBAL,
    hf_repo_id="openclimatefix/dwd-icon-global",
    ignore_grib_errors=True,
    chunking={
        "init_time": 1, "step": 37, "values": 122500, "model_level": 40,
        "model_level_half": 41,
    },
)

GLOBAL_ENSEMBLE_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global_ensemble.icechunk",
    model_url="icon-eps/grib",
    var_url="icon-eps_global_icosahedral",
    vars_2d=VAR_2D_ENSEMBLE_GLOBAL,
    vars_invariant=INVARIANT_VARS,
    f_steps=list(range(0, 49)),
    hf_repo_id="openclimatefix/dwd-icon-global",
    uppercase_names=False,
    chunking={"init_time": 1, "step": 24, "values": 122500, "number": -1},
)

EUROPE_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_eu.icechunk",
    model_url="icon-eu/grib",
    var_url="icon-eu_europe_regular-lat-lon",
    vars_2d=VAR_2D_EUROPE,
    vars_3d=_leveled(VAR_3D_EUROPE, PRESSURE_LEVELS_EUROPE),
    f_steps=list(range(0, 79)),
    hf_repo_id="openclimatefix/dwd-icon-eu",
    chunking={
        "init_time": 1, "step": 37, "latitude": 326, "longitude": 350, "isobaricInhPa": -1,
    },
)

EUROPE_MODEL_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_eu_model_level.icechunk",
    model_url="icon-eu/grib",
    var_url="icon-eu_europe_regular-lat-lon",
    vars_model=_leveled(VAR_MODEL_EUROPE, MODEL_LEVELS_EUROPE),
    f_steps=list(range(0, 79)),
    hf_repo_id="openclimatefix/dwd-icon-eu",
    chunking={
        "init_time": 1, "step": 37, "latitude": 326, "longitude": 350, "model_level": -1,
        "model_level_half": -1,
    },
)

EUROPE_ENSEMBLE_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_eu_ensemble.icechunk",
    model_url="icon-eu-eps/grib",
    var_url="icon-eu-eps_europe_icosahedral",
    vars_2d=VAR_2D_ENSEMBLE_EUROPE,
    vars_3d=_leveled(VAR_3D_ENSEMBLE_EUROPE, PRESSURE_LEVELS_EUROPE),
    vars_invariant=INVARIANT_VARS,
    f_steps=list(range(0, 49)),
    hf_repo_id="openclimatefix/dwd-icon-eu",
    uppercase_names=False,
    chunking={"init_time": 1, "step": 24, "values": 122500, "isobaricInhPa": -1, "number": -1},
)

GLOBAL_ART_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global_art.icechunk",
    base_url=f"{DWD_OPENDATA}/v1/m",
    model_url="icon-art/p",
    var_url="icon_global_icosahedral",
    url_style="path",
    vars_2d=ART_2D_VARS,
    vars_3d=_leveled(ART_3D_VARS, ART_PRESSURE_LEVELS),
    vars_model=_leveled(ART_MODEL_VARS, ART_MODEL_LEVELS),
    vars_invariant=INVARIANT_VARS,
    f_steps=list(range(0, 52)),
    hf_repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "init_time": 1, "step": 26, "values": 122500, "isobaricInhPa": -1,
        "model_level": -1, "depth": -1, "wavelength": -1,
    },
)

GLOBAL_ART_ANALYSIS_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global_art_analysis.icechunk",
    base_url=f"{DWD_OPENDATA}/v1/m",
    model_url="icon-art/p",
    var_url="icon_global_icosahedral",
    url_style="path",
    vars_2d=ART_2D_VARS,
    vars_3d=_leveled(ART_3D_VARS, ART_PRESSURE_LEVELS),
    vars_model=_leveled(ART_MODEL_VARS, ART_MODEL_LEVELS),
    vars_soil=_leveled(ART_SOIL_VARS, ART_SOIL_LEVELS),
    vars_wavelength=[
        f"{v}@{w}@{lv}"
        for v in ("CEIL_BSC_DUST", "SAT_BSC_DUST")
        for w in ART_WAVELENGTHS
        for lv in ART_MODEL_LEVELS
    ],
    vars_invariant=INVARIANT_VARS,
    f_steps=[0],
    hf_repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "init_time": 1, "step": 1, "values": 122500, "isobaricInhPa": -1,
        "model_level": -1, "depth": -1, "wavelength": -1,
    },
)

GLOBAL_ART_ENSEMBLE_ANALYSIS_CONFIG = ICONConfig(
    store_prefix="bkr/icon/icon_global_art_ensemble_analysis.icechunk",
    base_url=f"{DWD_OPENDATA}/v1/m",
    model_url="icon-art-eps/p",
    var_url="icon_global_icosahedral",
    url_style="path",
    vars_2d=ART_ENSEMBLE_2D_VARS,
    vars_3d=_leveled(ART_3D_VARS, ART_PRESSURE_LEVELS),
    vars_model=_leveled(ART_ENSEMBLE_MODEL_VARS, ART_MODEL_LEVELS),
    vars_invariant=INVARIANT_VARS,
    f_steps=[0],
    hf_repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "init_time": 1, "step": 1, "values": 122500, "isobaricInhPa": -1,
        "model_level": -1, "depth": -1, "wavelength": -1,
    },
)

#: Selectable ICON variants, keyed by the name Dagster and the CLI use.
VARIANTS: dict[str, ICONConfig] = {
    "global": GLOBAL_CONFIG,
    "global_model": GLOBAL_MODEL_CONFIG,
    "global_ensemble": GLOBAL_ENSEMBLE_CONFIG,
    "eu": EUROPE_CONFIG,
    "eu_model": EUROPE_MODEL_CONFIG,
    "eu_ensemble": EUROPE_ENSEMBLE_CONFIG,
    "global_art": GLOBAL_ART_CONFIG,
    "global_art_analysis": GLOBAL_ART_ANALYSIS_CONFIG,
    "global_art_ensemble_analysis": GLOBAL_ART_ENSEMBLE_ANALYSIS_CONFIG,
}

#: Runs DWD publishes for the deterministic global and EU models.
MODEL_RUNS = ["00", "06", "12", "18"]


# ---------------------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------------------

#: What a downloaded file contains, parsed back out of its name by :func:`index_files`.
KIND_SINGLE = "single"
KIND_PRESSURE = "pressure"
KIND_MODEL = "model"
KIND_SOIL = "soil"
KIND_WAVELENGTH = "wavelength"
KIND_INVARIANT = "invariant"

# Local filename used for the "path" layout, where every remote file is called
# PT000H00M.grib2 and the metadata lives in the directory names.
_PATH_NAME = re.compile(
    r"^(?P<var>.+?)__(?P<kind>single|pressure|model|soil|wavelength|invariant)"
    r"(?:-(?P<level>[^_]+))?__step-(?P<step>\d+)\.grib2$"
)

# The "filename" layout puts the step, the level and the variable in the name, but variable
# shortnames contain underscores (``t_2m``), so the fields are matched positionally.
_FILENAME_PATTERNS = (
    (KIND_SINGLE, re.compile(r"^single-level_\d{10}_(?P<step>\d{3})_(?P<var>.+)$")),
    (
        KIND_PRESSURE,
        re.compile(r"^pressure-level_\d{10}_(?P<step>\d{3})_(?P<level>\d+)_(?P<var>.+)$"),
    ),
    (
        KIND_MODEL,
        re.compile(r"^model-level_\d{10}_(?P<step>\d{3})_(?P<level>\d+)_(?P<var>.+)$"),
    ),
    (
        KIND_INVARIANT,
        re.compile(r"^time-invariant_\d{10}_(?:(?P<level>\d+)_)?(?P<var>.+)$"),
    ),
)

_PATH_STEP = re.compile(r"__step-(\d+)\.grib2$")
_FILENAME_STEP = re.compile(r"-level_\d{10}_(\d{3})_")


def _path_local_name(var: str, kind: str, step: int, level: str | None = None) -> str:
    """Name a file from the ``path`` layout so its metadata survives the download."""
    level_part = f"-{level}" if level is not None else ""
    return f"{var}__{kind}{level_part}__step-{step:03d}.grib2"


def build_urls(config: ICONConfig, run: str, date: dt.date) -> list[tuple[str, str]]:
    """Return ``(url, local filename)`` pairs for one model run.

    Existence on the server is deliberately not checked: with tens of thousands of
    candidate files a HEAD request per file costs more than letting the download fail.
    Missing files are simply skipped by :func:`download_run`.
    """
    if config.url_style == "path":
        return _build_path_urls(config, run, date)
    return _build_filename_urls(config, run, date)


def _build_filename_urls(config: ICONConfig, run: str, date: dt.date) -> list[tuple[str, str]]:
    date_string = date.strftime("%Y%m%d") + run
    urls: list[tuple[str, str]] = []

    def add(stem: str) -> None:
        url = f"{config.base_url}/{config.model_url}/{run}/{stem}"
        urls.append((url, os.path.basename(url).removesuffix(".bz2")))

    for step in config.f_steps:
        for var in config.vars_2d:
            add(
                f"{var}/{config.var_url}_single-level_{date_string}_{step:03d}"
                f"_{config.case(var)}.grib2.bz2"
            )
        for var in config.vars_3d:
            name, level = var.split("@")
            add(
                f"{name}/{config.var_url}_pressure-level_{date_string}_{step:03d}_{level}"
                f"_{config.case(name)}.grib2.bz2"
            )
        for var in config.vars_model:
            name, level = var.split("@")
            add(
                f"{name}/{config.var_url}_model-level_{date_string}_{step:03d}_{level}"
                f"_{config.case(name)}.grib2.bz2"
            )
    for var in config.vars_invariant:
        add(f"{var}/{config.var_url}_time-invariant_{date_string}_{config.case(var)}.grib2.bz2")
    return urls


def _build_path_urls(config: ICONConfig, run: str, date: dt.date) -> list[tuple[str, str]]:
    run_string = date.strftime("%Y-%m-%d") + f"T{run}:00"
    root = f"{config.base_url}/{config.model_url}"
    urls: list[tuple[str, str]] = []

    def add(var: str, kind: str, step: int, middle: str, level: str | None = None) -> None:
        url = f"{root}/{var}{middle}/r/{run_string}/s/PT{step:03d}H00M.grib2"
        urls.append((url, _path_local_name(var, kind, step, level)))

    for step in config.f_steps:
        for var in config.vars_2d:
            add(var, KIND_SINGLE, step, "")
        for var in config.vars_3d:
            name, level = var.split("@")
            add(name, KIND_PRESSURE, step, f"/lvt1/{LEVEL_TYPE_PRESSURE}/lv1/{level}", level)
        for var in config.vars_model:
            name, level = var.split("@")
            add(name, KIND_MODEL, step, f"/lvt1/{LEVEL_TYPE_MODEL}/lv1/{level}", level)
        for var in config.vars_soil:
            name, level = var.split("@")
            add(name, KIND_SOIL, step, f"/lvt1/{LEVEL_TYPE_SOIL}/lv1/{level}", level)
        for var in config.vars_wavelength:
            name, wavelength, level = var.split("@")
            add(
                name,
                KIND_WAVELENGTH,
                step,
                f"/wvl1/{wavelength}/lvt1/{LEVEL_TYPE_MODEL}/lv1/{level}",
                f"{wavelength}-{level}",
            )
    for var in config.vars_invariant:
        add(config.case(var), KIND_INVARIANT, 0, "")
    return urls


# ---------------------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------------------


def _decompress_bz2(source: pathlib.Path, dest: pathlib.Path) -> pathlib.Path | None:
    """Decompress ``source`` to ``dest`` atomically, removing the archive afterwards."""
    part = dest.with_name(dest.name + ".part")
    try:
        with open(source, "rb") as src, open(part, "wb") as out:
            out.write(bz2.decompress(src.read()))
    except (OSError, EOFError, ValueError) as exc:
        logger.warning(f"failed to decompress {source.name}: {exc}")
        part.unlink(missing_ok=True)
        source.unlink(missing_ok=True)
        return None
    os.replace(part, dest)
    source.unlink(missing_ok=True)
    return dest


def download_grib(url: str, dest: str | os.PathLike, **kwargs) -> pathlib.Path | None:
    """Download one GRIB file, decompressing it when the URL is bz2-compressed.

    Returns the path to the decompressed GRIB, or None when the file is not on the server
    (which is normal: :func:`build_urls` does not check availability).
    """
    dest = pathlib.Path(dest)
    if dest.is_file() and dest.stat().st_size > 0:
        return dest
    if not url.endswith(".bz2"):
        return download_one(url, dest, **kwargs)

    archive = dest.with_name(dest.name + ".bz2")
    if not archive.is_file() and download_one(url, archive, **kwargs) is None:
        return None
    return _decompress_bz2(archive, dest)


def download_run(
    urls: Sequence[tuple[str, str]],
    dest_dir: str | os.PathLike,
    workers: int = 8,
    passes: int = 2,
    pause: float = 5.0,
    **kwargs,
) -> list[str]:
    """Download every ``(url, filename)`` pair into ``dest_dir``, skipping what is missing.

    Most of a run's candidate URLs do not exist — :func:`build_urls` enumerates every
    variable at every level and step, and DWD publishes only a subset. Retrying each of
    those 404s with backoff would cost hours of sleeping, so a URL is tried once per pass
    and the whole set of failures is retried in a second pass instead. A file that is
    genuinely absent costs one request per pass; a transient failure still gets a retry.

    Args:
        urls: ``(url, local filename)`` pairs.
        dest_dir: Directory to download into; created if needed.
        workers: Concurrent downloads.
        passes: How many times to sweep the outstanding URLs.
        pause: Seconds to wait between passes.
        **kwargs: Passed through to :func:`download_grib`.
    """
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    kwargs.setdefault("retries", 1)

    def one(pair: tuple[str, str]) -> pathlib.Path | None:
        url, name = pair
        return download_grib(url, dest_dir / name, **kwargs)

    paths: list[str] = []
    outstanding = list(urls)
    for attempt in range(1, max(passes, 1) + 1):
        if not outstanding:
            break
        if attempt > 1:
            logger.info(f"retrying {len(outstanding)} ICON file(s), pass {attempt}/{passes}")
            time.sleep(pause)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(one, outstanding))
        paths.extend(str(p) for p in results if p is not None)
        outstanding = [pair for pair, got in zip(outstanding, results) if got is None]

    logger.info(f"downloaded {len(paths)}/{len(urls)} ICON files into {dest_dir}")
    return sorted(paths)


# ---------------------------------------------------------------------------------------
# Indexing and merging
# ---------------------------------------------------------------------------------------


def index_files(config: ICONConfig, paths: Iterable[str]) -> dict[tuple[str, str], list[str]]:
    """Group downloaded files by ``(kind, variable)``.

    Both layouts encode the variable, level type and step in the filename, so the files can
    be grouped without opening them. Wavelength-resolved variables key on
    ``<variable>@<wavelength>``, because each wavelength is a separate stack of levels.
    """
    grouped: dict[tuple[str, str], list[str]] = {}
    for path in paths:
        parsed = _parse_name(config, os.path.basename(path))
        if parsed is None:
            logger.debug(f"ignoring unrecognised ICON file {os.path.basename(path)}")
            continue
        grouped.setdefault(parsed, []).append(path)
    return {key: sorted(value) for key, value in grouped.items()}


def _parse_name(config: ICONConfig, name: str) -> tuple[str, str] | None:
    """Return ``(kind, variable)`` for a downloaded file, or None if it does not match."""
    if config.url_style == "path":
        match = _PATH_NAME.match(name)
        if match is None:
            return None
        kind = match.group("kind")
        variable = match.group("var").lower()
        if kind == KIND_WAVELENGTH:
            # "532-120" is wavelength then level; the level belongs to the stack, the
            # wavelength to the group.
            variable = f"{variable}@{match.group('level').split('-')[0]}"
        return kind, variable

    stem = name.removesuffix(".grib2")
    if not stem.startswith(config.var_url + "_"):
        return None
    rest = stem[len(config.var_url) + 1:]
    for kind, pattern in _FILENAME_PATTERNS:
        match = pattern.match(rest)
        if match is not None:
            return kind, match.group("var").lower()
    return None


def _step_of(path: str) -> int:
    """Forecast step, in hours, encoded in a downloaded file's name.

    Time-invariant files carry no step and sort to zero, which is where they belong.
    """
    name = os.path.basename(path)
    for pattern in (_PATH_STEP, _FILENAME_STEP):
        match = pattern.search(name)
        if match is not None:
            return int(match.group(1))
    return 0


#: Vertical coordinates cfgrib may attach, and the dimension name we standardise on.
_VERTICAL_COORDS = {
    "isobaricInhPa": ("isobaricInhPa", True),
    "generalVerticalLayer": ("model_level", False),
    "generalVertical": ("model_level_half", False),
    "depthBelowLandLayer": ("depth", True),
    "depthBelowLand": ("depth", True),
}


def _sorted_by_step(paths: Sequence[str]) -> list[str]:
    """Sort paths by forecast step, breaking ties lexicographically."""
    return sorted(paths, key=lambda p: (_step_of(p), p))


def _sort_along(ds: xr.Dataset, name: str, ascending: bool = True) -> xr.Dataset:
    """Sort along ``name`` when it is a real dimension coordinate, otherwise pass through."""
    if name in ds.dims and name in ds.coords:
        return ds.sortby(name, ascending=ascending)
    return ds


def _promote_to_dim(ds: xr.Dataset, coord: str, dim: str, rename_to: str | None = None):
    """Make ``coord`` the index of ``dim``, whether or not the concat promoted it.

    ``open_mfdataset`` only promotes a scalar coordinate to the concat dimension when its
    value differs between files, so a single-level or single-step group comes back with the
    dimension present but the coordinate still scalar. Both shapes end up the same here.
    """
    if dim not in ds.dims or coord not in ds.coords:
        return ds
    values = np.atleast_1d(ds.coords[coord].values)
    if values.size != ds.sizes[dim]:
        logger.warning(
            f"{coord} has {values.size} value(s) for a {dim} of {ds.sizes[dim]}, leaving as-is"
        )
        return ds
    target = rename_to or coord
    ds = ds.drop_vars(coord)
    if dim != target:
        ds = ds.rename({dim: target})
    return ds.assign_coords({target: values})


def _open_leveled(
    paths: Sequence[str],
    variable: str,
    ignore_grib_errors: bool,
) -> xr.Dataset | None:
    """Open one level-resolved variable, concatenating over level and then step.

    Each remote file holds a single variable at a single level and step, so the level and
    step axes have to be rebuilt here. The vertical coordinate cfgrib attaches depends on
    the level type, which is why it is detected rather than assumed.
    """
    by_step: dict[int, list[str]] = {}
    for path in paths:
        by_step.setdefault(_step_of(path), []).append(path)

    backend_kwargs = {"errors": "ignore"} if ignore_grib_errors else {}
    per_step = []
    for step in sorted(by_step):
        try:
            ds = xr.open_mfdataset(
                sorted(by_step[step]),
                engine="cfgrib",
                backend_kwargs=backend_kwargs,
                combine="nested",
                concat_dim="level",
                coords="different",
                compat="no_conflicts",
                decode_timedelta=True,
            )
        except Exception as exc:  # noqa: BLE001 - a bad step must not lose the whole run
            logger.error(f"failed to open {variable} at step {step}: {exc}")
            continue
        per_step.append(ds)

    if not per_step:
        return None
    try:
        ds = xr.concat(per_step, dim="step", coords="different", compat="no_conflicts")
    except Exception as exc:  # noqa: BLE001 - mismatched steps are logged, not fatal
        logger.error(f"failed to concatenate {variable} over step: {exc}")
        return None

    ds = ds.rename({v: variable for v in ds.data_vars})
    ds = _sort_along(_promote_to_dim(ds, "step", "step"), "step")

    for source, (target, ascending) in _VERTICAL_COORDS.items():
        if source in ds.coords:
            ds = _promote_to_dim(ds, source, "level", rename_to=target)
            ds = _sort_along(ds, target, ascending=ascending)
            break
    else:
        logger.debug(f"{variable}: no known vertical coordinate, leaving 'level' as-is")

    return _drop_scalar_coords(ds)


def _open_single_level(
    paths: Sequence[str],
    variable: str,
) -> xr.Dataset | None:
    """Open one single-level variable, concatenating its forecast steps."""
    try:
        ds = xr.open_mfdataset(
            _sorted_by_step(paths),
            engine="cfgrib",
            backend_kwargs={"errors": "ignore"},
            combine="nested",
            concat_dim="step",
            coords="different",
            compat="no_conflicts",
            decode_timedelta=True,
        ).drop_vars("valid_time", errors="ignore")
    except Exception as exc:  # noqa: BLE001 - one missing variable must not fail the run
        logger.error(f"failed to open 2D variable {variable}: {exc}")
        return None
    ds = ds.rename({v: variable for v in ds.data_vars})
    ds = _sort_along(_promote_to_dim(ds, "step", "step"), "step")
    return _drop_scalar_coords(ds)


def _open_wavelength_groups(
    grouped: dict[tuple[str, str], list[str]],
    ignore_grib_errors: bool,
) -> list[xr.Dataset]:
    """Build one dataset per wavelength-resolved variable, with a ``wavelength`` axis.

    ICON-ART publishes the backscatter fields once per wavelength and level, so each
    wavelength is a separate stack of levels that then has to be concatenated.
    """
    by_variable: dict[str, dict[float, xr.Dataset]] = {}
    for (kind, key), paths in grouped.items():
        if kind != KIND_WAVELENGTH:
            continue
        variable, _, wavelength = key.partition("@")
        ds = _open_leveled(paths, variable, ignore_grib_errors)
        if ds is None:
            continue
        by_variable.setdefault(variable, {})[float(wavelength)] = ds

    datasets = []
    for variable, per_wavelength in by_variable.items():
        wavelengths = sorted(per_wavelength)
        try:
            ds = xr.concat(
                [per_wavelength[w] for w in wavelengths],
                dim=pd.Index(wavelengths, name="wavelength"),
                coords="different",
                compat="no_conflicts",
            )
        except Exception as exc:  # noqa: BLE001 - one bad variable must not fail the run
            logger.error(f"failed to concatenate {variable} over wavelength: {exc}")
            continue
        datasets.append(ds)
    return datasets


def _drop_scalar_coords(ds: xr.Dataset) -> xr.Dataset:
    """Drop coordinates that are neither dimensions nor the run's ``time``.

    cfgrib attaches several scalar coordinates per file (``surface``, ``heightAboveGround``,
    …) that differ between variables and would otherwise block the merge.
    """
    extra = [c for c in ds.coords if c not in ds.dims and c != "time"]
    return ds.drop_vars(extra) if extra else ds


# ---------------------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------------------


class ICONProvider(BaseProvider):
    """DWD ICON global, ICON-EU, their ensembles and ICON-ART.

    One instance covers one variant from :data:`VARIANTS`. The forecast is stored with
    ``init_time`` as the append dimension and ``step`` as the lead-time axis::

        ICONProvider("eu").run_partition(pd.Timestamp("2026-09-27T00:00"))

    Only the four synoptic runs in :data:`MODEL_RUNS` are published; the provider does not
    check the run hour, so a caller asking for 03:00 will simply find nothing to download.
    """

    append_dim = "init_time"

    #: How long after its initialisation time a run is assumed to be fully published. Used
    #: only when the store does not exist yet, where there is no schema to check against.
    #: DWD publishes a global run over roughly two hours; six is generous but the cost of
    #: waiting is one retry, and the cost of not waiting is a permanently poisoned store.
    settle_after: pd.Timedelta = pd.Timedelta(hours=6)

    def __init__(
        self,
        variant: str = "global",
        config: Config | None = None,
        icon_config: ICONConfig | None = None,
        store_prefix: str | None = None,
        workers: int = 8,
    ):
        """Build a provider for one ICON variant.

        Args:
            variant: Key into :data:`VARIANTS`. Ignored when ``icon_config`` is given.
            config: Process configuration; defaults to the process-wide one.
            icon_config: Explicit variant configuration, for ad hoc subsets.
            store_prefix: Override the store location from the variant.
            workers: Concurrent downloads.
        """
        super().__init__(config)
        if icon_config is None:
            if variant not in VARIANTS:
                raise ValueError(
                    f"unknown ICON variant {variant!r}; expected one of {sorted(VARIANTS)}"
                )
            icon_config = VARIANTS[variant]
        self.variant = variant
        self.icon_config = icon_config
        self.name = f"icon_{variant}"
        self.store_prefix = store_prefix or icon_config.store_prefix
        self.workers = workers

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download every GRIB file for the run at ``it``."""
        cfg = self.icon_config
        run = it.strftime("%H")
        urls = build_urls(cfg, run, it.date())
        logger.info(f"{self.name}: {len(urls)} candidate files for {it.date()} run {run}")
        return download_run(urls, self._download_dir(it, temp_dir), workers=self.workers)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the downloaded GRIB files into one dataset for the run at ``it``."""
        cfg = self.icon_config
        grouped = index_files(cfg, input_files)

        parts: list[xr.Dataset] = []
        for kind in (KIND_SINGLE, KIND_PRESSURE, KIND_MODEL, KIND_SOIL):
            for (group_kind, variable), paths in grouped.items():
                if group_kind != kind:
                    continue
                if kind == KIND_SINGLE:
                    ds = _open_single_level(paths, variable)
                else:
                    ds = _open_leveled(paths, variable, cfg.ignore_grib_errors)
                if ds is not None:
                    parts.append(ds)
        parts.extend(_open_wavelength_groups(grouped, cfg.ignore_grib_errors))

        if not parts:
            raise ValueError(f"{self.name}: no usable GRIB files for {it}")

        ds = xr.merge(parts, compat="no_conflicts")
        ds = self._assign_icosahedral_coords(ds, grouped)

        # GRIB's scalar "time" is the run's initialisation time. Promote it to the append
        # dimension under an unambiguous name, warning if it disagrees with the partition.
        if "time" in ds.coords and ds.coords["time"].size == 1:
            grib_init = pd.Timestamp(ds.coords["time"].values.reshape(-1)[0])
            if grib_init != it:
                logger.warning(f"{self.name}: GRIB init {grib_init} differs from partition {it}")
        ds = ds.drop_vars("time", errors="ignore")
        ds = ds.expand_dims(init_time=pd.DatetimeIndex([it]))

        for var in cfg.reduced_precision_vars:
            if var in ds.data_vars:
                ds[var] = ds[var].astype("float16")

        chunks = {dim: size for dim, size in cfg.chunking.items() if dim in ds.dims}
        return ds.chunk(chunks) if chunks else ds

    def _assign_icosahedral_coords(
        self,
        ds: xr.Dataset,
        grouped: dict[tuple[str, str], list[str]],
    ) -> xr.Dataset:
        """Attach latitude/longitude to the icosahedral grid, which GRIB does not carry."""
        if "values" not in ds.dims:
            return ds
        lat_paths = grouped.get((KIND_INVARIANT, "clat"))
        lon_paths = grouped.get((KIND_INVARIANT, "clon"))
        if not lat_paths or not lon_paths:
            logger.warning(f"{self.name}: no CLAT/CLON files, leaving the grid unlabelled")
            return ds
        # An unlabelled grid is recoverable; losing a run that has already been downloaded
        # because one small invariant file is truncated is not.
        try:
            with (
                xr.open_dataset(lat_paths[0], engine="cfgrib", decode_timedelta=True) as lat_ds,
                xr.open_dataset(lon_paths[0], engine="cfgrib", decode_timedelta=True) as lon_ds,
            ):
                lats = lat_ds["tlat"].values
                lons = lon_ds["tlon"].values
        except Exception as exc:  # noqa: BLE001 - any read failure leaves the grid unlabelled
            logger.warning(f"{self.name}: could not read CLAT/CLON ({exc}), grid left unlabelled")
            return ds

        if lats.shape != (ds.sizes["values"],) or lons.shape != (ds.sizes["values"],):
            logger.warning(
                f"{self.name}: CLAT/CLON have {lats.shape}/{lons.shape} for "
                f"{ds.sizes['values']} values, grid left unlabelled"
            )
            return ds

        return ds.assign_coords({"latitude": ("values", lats), "longitude": ("values", lons)})

    def _download_dir(self, it: pd.Timestamp, temp_dir: pathlib.Path | None) -> pathlib.Path:
        base = pathlib.Path(temp_dir) if temp_dir is not None else self.config.data_dir / "icon"
        return base / self.variant / it.strftime("%Y%m%d") / it.strftime("%H")

    # -- completeness ---------------------------------------------------------------

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write one run, refusing one that is still being published.

        Nothing upstream of here notices an incomplete run. :func:`download_run` treats a
        404 as "this file does not exist", which is normally true — :func:`build_urls`
        enumerates far more candidates than DWD publishes — but is also exactly what a run
        that is only half uploaded looks like. ``_open_leveled`` and ``_open_single_level``
        then swallow their own read errors and return ``None``. The result is a dataset
        that is merely *smaller*, with no signal that anything is wrong.

        That matters because the first run written fixes the store's schema. A truncated
        first run bakes in a short variable set and a short ``step`` axis; every later
        complete run then fails the writer's variable check, is logged and skipped, and the
        asset still materialises green. Raising instead lets Dagster retry the partition
        once DWD has caught up.
        """
        self._require_complete(repo, processed)
        return super().write_to_icechunk(repo, processed)

    def _require_complete(self, repo: icechunk.Repository, processed: xr.Dataset) -> None:
        """Raise :class:`IncompleteRun` when ``processed`` is short of the store's schema.

        With a store to compare against, "complete" means "holds at least the variables and
        the lead times the store already does". With no store yet there is nothing to
        compare against, so completeness is inferred from age instead: a run older than
        :attr:`settle_after` has certainly finished publishing.
        """
        init = pd.Timestamp(np.atleast_1d(processed[self.append_dim].values)[0])

        if not has_committed_data(repo):
            age = pd.Timestamp.utcnow().tz_localize(None) - init
            if age < self.settle_after:
                raise IncompleteRun(
                    f"{self.name}: refusing to create {self.store_path} from run {init}, "
                    f"only {age} old and possibly still uploading. The first run written "
                    "fixes the store's variable set and step axis, so a truncated one would "
                    f"lock every later run out. Retry once the run is {self.settle_after} old."
                )
            return

        try:
            existing = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        except STORE_READ_ERRORS as exc:
            raise IncompleteRun(
                f"{self.name}: store holds data but could not be read to check run {init} "
                f"for completeness ({type(exc).__name__}: {exc})"
            ) from exc

        missing_vars = sorted(set(existing.data_vars) - set(processed.data_vars))
        stored_steps = existing.sizes.get("step")
        run_steps = processed.sizes.get("step")
        short_steps = (
            stored_steps is not None and run_steps is not None and run_steps < stored_steps
        )
        if missing_vars or short_steps:
            raise IncompleteRun(
                f"{self.name}: run {init} is incomplete against {self.store_path} — "
                f"{len(missing_vars)} variable(s) missing ({missing_vars[:5]}), "
                f"{run_steps} of {stored_steps} step(s). DWD is most likely still "
                "publishing it; retry rather than writing a short run."
            )

    # -- Hugging Face ---------------------------------------------------------------

    @property
    def hf_repo_id(self) -> str:
        """Target Hugging Face dataset repository.

        ``HF_REPO_ID`` overrides the variant's default so the same code can publish to a
        fork or a scratch repository.
        """
        repo_id = self.config.hf_repo_id or self.icon_config.hf_repo_id
        if repo_id is None:
            raise ValueError(f"{self.name}: no Hugging Face repo configured; set HF_REPO_ID")
        return repo_id

    def publish(
        self,
        path: str | os.PathLike,
        path_in_repo: str | None = None,
        dry_run: bool = False,
    ) -> str:
        """Upload ``path`` to the Hugging Face Hub and return the target repository.

        Args:
            path: Local directory to upload, typically the store for one run. Object store
                URIs cannot be uploaded from; the Hub client needs files on disk.
            path_in_repo: Destination inside the repository. Defaults to the directory name.
            dry_run: Log what would be uploaded and return without touching the network.
        """
        repo_id = self.hf_repo_id
        if "://" in str(path):
            raise ValueError(
                f"{self.name}: publish needs a local directory but got {path!r}. Set "
                "ICECHUNK_LOCAL_PATH, or pass the path of a local copy of the store."
            )
        folder = pathlib.Path(path)
        if not folder.is_dir():
            raise FileNotFoundError(f"{folder} is not a directory")
        target = path_in_repo or folder.name

        if dry_run:
            logger.info(f"{self.name}: would upload {folder} to {repo_id}:{target}")
            return repo_id

        (token,) = self.config.credentials.require("hf_token")
        from huggingface_hub import HfApi

        api = HfApi(token=token)
        api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        api.upload_folder(
            folder_path=str(folder),
            path_in_repo=target,
            repo_id=repo_id,
            repo_type="dataset",
        )
        logger.info(f"{self.name}: uploaded {folder} to {repo_id}:{target}")
        return repo_id


class ICOND2RUCProvider(BaseProvider):
    """DWD ICON-D2 rapid update cycle, read from a local mirror of the open data tree.

    DWD interleaves three output cadences in one directory tree: hourly fields, 15-minute
    fields and 5-minute fields. They are told apart by the number and spacing of the valid
    times a variable's analysis files carry, and each cadence gets its own store. Create
    one provider per cadence::

        ICOND2RUCProvider("15min").run_partition(pd.Timestamp("2026-09-27T06:00"))

    The mirror location defaults to ``<data_dir>/dwd-ruc`` and can be overridden with
    ``grib_dir`` or the ``ICON_D2_RUC_GRIB_DIR`` environment variable.
    """

    append_dim = "time"

    #: Number of valid times a variable carries in each cadence.
    TIMESTEPS = {"60min": 1, "15min": 4, "5min": 12}

    #: Spacing, in minutes, between those valid times. The hourly fields have a single
    #: time, so there is nothing to check; the other two are only distinguishable by their
    #: spacing when a variable is partially mirrored.
    SPACING_MINUTES = {"60min": None, "15min": 15, "5min": 5}

    MIRROR_SUBPATH = "opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc/p"

    VARIABLES = [
        "CLAT", "CLON", "ALB_RAD", "ASOB_S", "ASWDIFD_S", "ASWDIR_S", "CAPE_ML", "CAPE_MU",
        "CEILING", "CIN_ML", "CIN_MU", "CLC", "CLCH", "CLCL", "CLCM", "CLCT", "DEPTH_LK",
        "ECHOTOP", "ECHOTOPinM", "FI", "FR_ICE", "FR_LAKE", "FR_LAND", "GRAU_GSP",
        "HAIL_GSP", "HHL", "HSURF", "H_SNOW", "LAI", "LAPSE_RATE", "LPI", "LPI_MAX", "P",
        "PLCOV", "PMSL", "PREC_GSP", "PRG_GSP", "PRH_GSP", "PRR_GSP", "PRS_GSP", "PR_GSP",
        "PS", "QC", "QC_DIA", "QG", "QH", "QI", "QI_DIA", "QR", "QS", "QV", "QV_2M", "QV_S",
        "RAIN_GSP", "RELHUM", "RELHUM_2M", "ROOTDP", "SDI_2", "SNOWLMT", "SNOW_GSP", "SRH",
        "T", "TCH", "TCM", "TCOND10_MX", "TCOND_MAX", "TD_2M", "TKE", "TOT_PR", "TOT_PREC",
        "TOT_PREC_D", "TOT_PR_MAX", "TQC", "TQC_DIA", "TQG", "TQH", "TQI", "TQI_DIA", "TQR",
        "TQS", "TQV", "TQV_DIA", "T_2M", "T_G", "U", "UH_MAX", "UH_MAX_LOW", "UH_MAX_MED",
        "U_10M", "V", "VIS", "VMAX_10M", "VORW_CTMAX", "V_10M", "W", "WSHEAR_U", "WSHEAR_V",
        "W_CTMAX", "Z0",
    ]

    def __init__(
        self,
        timescale: str = "60min",
        config: Config | None = None,
        grib_dir: str | os.PathLike | None = None,
        store_suffix: str = "",
    ):
        """Build a provider for one RUC cadence.

        Args:
            timescale: One of ``60min``, ``15min`` or ``5min``.
            config: Process configuration; defaults to the process-wide one.
            grib_dir: Root of the local mirror. Defaults to ``<data_dir>/dwd-ruc/...``.
            store_suffix: Appended to the store name, for writing a parallel archive.
        """
        super().__init__(config)
        if timescale not in self.TIMESTEPS:
            raise ValueError(
                f"unknown timescale {timescale!r}; expected one of {sorted(self.TIMESTEPS)}"
            )
        self.timescale = timescale
        self.name = f"icon_d2_ruc_{timescale}"
        self.store_prefix = f"bkr/icon/icon_d2_ruc_{timescale}{store_suffix}.icechunk"
        self._grib_dir = pathlib.Path(grib_dir) if grib_dir is not None else None

    @property
    def grib_dir(self) -> pathlib.Path:
        """Root of the local ICON-D2-RUC mirror."""
        if self._grib_dir is not None:
            return self._grib_dir
        override = os.environ.get("ICON_D2_RUC_GRIB_DIR")
        if override:
            return pathlib.Path(override).expanduser()
        return self.config.data_dir / "dwd-ruc" / self.MIRROR_SUBPATH

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """List the analysis GRIB files for ``it`` in the local mirror."""
        base = self.grib_dir
        if not base.is_dir():
            logger.warning(f"{self.name}: mirror {base} does not exist")
            return []

        init_time_str = it.strftime("%Y-%m-%dT%H:%M")
        files: List[str] = []
        for var in self.VARIABLES:
            var_path = base / var
            if not var_path.is_dir():
                continue
            files.extend(str(f) for f in var_path.rglob(f"*/{init_time_str}/*/PT000H*.grib2"))

        if not files:
            logger.warning(f"{self.name}: no files for {init_time_str} under {base}")
        return sorted(files)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the variables belonging to this provider's cadence."""
        selected: list[xr.Dataset] = []

        for var in self.VARIABLES:
            var_files = [f for f in input_files if f"/{var}/" in f]
            if not var_files:
                continue
            for ds, suffix in self._level_groups(var_files):
                if ds is None:
                    continue
                ds = _clean_ruc_dataset(ds)
                if not self._matches_cadence(ds, var):
                    continue
                # cfgrib often names the variable "unknown" for DWD's local GRIB tables, so
                # the directory name is authoritative.
                target = f"{var}{suffix}".lower()
                data_vars = list(ds.data_vars)
                if len(data_vars) == 1:
                    ds = ds.rename({data_vars[0]: target})
                else:
                    ds = ds.rename({dv: f"{target}_{dv}".lower() for dv in data_vars})
                selected.append(ds)

        if not selected:
            raise ValueError(f"{self.name}: no {self.timescale} variables found for {it}")

        merged = xr.merge(selected)
        merged = merged.drop_vars(
            ["entireLake", "meanSea", "entireAtmosphere"], errors="ignore"
        )

        renames = {
            "isobaricInhPa": "level",
            "generalVerticalLayer": "model_level",
            "generalVertical": "model_level_half",
            "heightAboveGroundLayer": "height",
        }
        merged = merged.rename({k: v for k, v in renames.items() if k in merged.dims})

        chunks: dict[str, int] = {"time": 1}
        if "values" in merged.dims:
            chunks["values"] = -1
        for dim in ("level", "model_level", "model_level_half", "height"):
            if dim in merged.dims:
                merged = merged.sortby(dim, ascending=dim != "level")
                chunks[dim] = -1
        return merged.sortby("time").chunk(chunks)

    def _matches_cadence(self, ds: xr.Dataset, variable: str) -> bool:
        """True when ``ds`` has the number and spacing of valid times of this cadence.

        Counting alone is not enough: a 5-minute variable that is only partly mirrored can
        have exactly four valid times and would otherwise be written into the 15-minute
        store at the wrong spacing.
        """
        wanted = self.TIMESTEPS[self.timescale]
        if ds.sizes.get("time", 1) != wanted:
            return False

        minutes = self.SPACING_MINUTES[self.timescale]
        if minutes is None or wanted < 2:
            return True

        deltas = np.diff(np.asarray(ds["time"].values))
        if not np.all(deltas == np.timedelta64(minutes, "m")):
            logger.warning(
                f"{self.name}: {variable} has {wanted} valid times but not {minutes} minutes "
                "apart, skipping it rather than writing it at the wrong cadence"
            )
            return False
        return True

    def _level_groups(self, var_files: List[str]):
        """Yield ``(dataset, suffix)`` for a variable, splitting mixed level types.

        A variable published on both pressure and model levels arrives in one directory
        tree; merging the two would collide on the variable name, so they are suffixed.
        """
        pressure = [f for f in var_files if f"lvt1/{LEVEL_TYPE_PRESSURE}" in f]
        model = [f for f in var_files if f"lvt1/{LEVEL_TYPE_MODEL}" in f]
        if pressure and model:
            yield _open_and_merge_by_valid_time(model), "_model_level"
            yield _open_and_merge_by_valid_time(pressure), "_pressure_level"
        else:
            yield _open_and_merge_by_valid_time(var_files), ""


def _clean_ruc_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Turn the GRIB ``valid_time`` axis into ``time`` and drop per-file scalar coords."""
    ds = ds.drop_vars(
        ["step", "time", "level", "surface", "isobaricLayer", "heightAboveSeaLayer"],
        errors="ignore",
    ).rename({"valid_time": "time"})
    if "heightAboveGround" in ds.coords and "heightAboveGround" not in ds.dims:
        ds = ds.drop_vars("heightAboveGround")
    return ds


def _open_and_merge_by_valid_time(files: Sequence[str]) -> xr.Dataset | None:
    """Open ICON-D2-RUC GRIB files and rebuild the level and valid-time axes.

    Each file holds one variable at one level and one valid time. Files are first grouped
    by valid time and stacked over their level coordinate, then concatenated over valid
    time. Valid times with fewer levels than the rest are dropped: they are a partially
    mirrored timestep and would otherwise produce a ragged array.
    """
    per_valid_time: dict = {}
    for path in files:
        try:
            ds = xr.open_dataset(path, engine="cfgrib")
        except Exception as exc:  # noqa: BLE001 - a corrupt file must not fail the run
            logger.warning(f"error opening {path}: {exc}")
            continue
        if "valid_time" not in ds.coords:
            logger.warning(f"{path} has no valid_time coordinate, skipping")
            continue
        key = pd.Timestamp(np.asarray(ds.coords["valid_time"].values).reshape(-1)[0])
        per_valid_time.setdefault(key, []).append(ds)

    if not per_valid_time:
        return None

    max_length = max(len(v) for v in per_valid_time.values())
    complete = {k: v for k, v in per_valid_time.items() if len(v) == max_length}

    merged = []
    for valid_time in sorted(complete):
        ds_list = complete[valid_time]
        if len(ds_list) == 1:
            merged.append(ds_list[0])
            continue
        level_coords = [
            c for c in ds_list[0].coords if c not in ("time", "step", "valid_time")
        ]
        if not level_coords:
            logger.warning(f"no level coordinate to stack {len(ds_list)} files on, skipping")
            continue
        merged.append(xr.concat(ds_list, dim=level_coords[0]))

    if not merged:
        return None
    return xr.concat(merged, dim="valid_time")


# ---------------------------------------------------------------------------------------
# Static model-level half heights
# ---------------------------------------------------------------------------------------

STATIC_HEIGHTS_PREFIX = "bkr/icon/icon_global_model_level_half_heights_static.icechunk"


def model_level_half_height_urls(
    date: dt.date,
    run: str = "00",
    config: ICONConfig = GLOBAL_CONFIG,
    levels: Sequence[int] = MODEL_LEVELS_GLOBAL,
) -> list[tuple[str, str]]:
    """Return ``(url, filename)`` pairs for the static half-level height (``HHL``) field."""
    date_string = date.strftime("%Y%m%d") + run
    urls = []
    for level in levels:
        name = f"{config.var_url}_time-invariant_{date_string}_{level}_HHL.grib2"
        urls.append((f"{config.base_url}/{config.model_url}/{run}/hhl/{name}.bz2", name))
    return urls


def open_model_level_half_heights(paths: Sequence[str]) -> xr.Dataset:
    """Concatenate the per-level ``HHL`` GRIB files into one half-level height field."""
    import cfgrib

    def level_of(path: str) -> int:
        match = re.search(r"_(\d+)_HHL\.grib2$", os.path.basename(path))
        return int(match.group(1)) if match else 0

    dses = [cfgrib.open_dataset(p) for p in sorted(paths, key=level_of)]
    if not dses:
        raise ValueError("no HHL files to combine")
    ds = xr.concat(dses, dim="generalVertical")
    ds = ds.rename({"generalVertical": "model_level_half"})
    return ds.drop_vars(["time", "step", "valid_time"], errors="ignore")


def write_model_level_half_heights(
    date: dt.date | None = None,
    run: str = "00",
    config: Config | None = None,
    dest_dir: str | os.PathLike | None = None,
    levels: Sequence[int] = MODEL_LEVELS_GLOBAL,
) -> bool:
    """Download and store the static ICON global half-level heights.

    The field is time-invariant, so this only needs running once; it returns False when the
    store already holds it. Model-level datasets are indexed by half level and are hard to
    interpret without it.

    Args:
        date: Run date to take the field from. Defaults to today; any run will do.
        run: Run hour to take the field from.
        config: Process configuration; defaults to the process-wide one.
        dest_dir: Where to stage the downloads. Defaults to a directory under the scratch
            directory, which is kept so a rerun does not download the field again.
        levels: Half levels to retrieve.
    """
    from planetary_datasets.config import get_config

    cfg = config if config is not None else get_config()
    date = date or dt.datetime.now(tz=dt.timezone.utc).date()
    repo = cfg.icechunk_repo(STATIC_HEIGHTS_PREFIX)

    try:
        existing = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except Exception:  # noqa: BLE001 - an unreadable store is an empty store here
        existing = None
    if existing is not None and "model_level_half" in existing.dims:
        logger.info("ICON half-level heights already stored, skipping")
        return False

    target = pathlib.Path(dest_dir) if dest_dir else cfg.scratch_dir / "icon-hhl"
    paths = download_run(model_level_half_height_urls(date, run, levels=levels), target)
    if not paths:
        logger.warning(f"no HHL files available for {date} run {run}")
        return False
    if len(paths) != len(levels):
        # A partial field would be cached forever by the check above, so refuse it and let
        # a later run pick up the rest.
        logger.warning(
            f"only {len(paths)}/{len(levels)} HHL levels available for {date} run {run}, "
            "not writing a partial field"
        )
        return False

    ds = open_model_level_half_heights(paths).chunk({"model_level_half": -1})
    from icechunk.xarray import to_icechunk

    session = repo.writable_session("main")
    to_icechunk(ds, session)
    session.commit("Initial write of ICON global model level half heights")
    logger.info(f"wrote ICON half-level heights from {date} run {run}")
    return True
