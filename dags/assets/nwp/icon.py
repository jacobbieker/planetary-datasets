"""Jacob's ICON global processing script, modified a little bit.

New paths, including for ICON-ART, are:

The following new variants exist for the deterministic models:

/v1/m/<model-name>/p/<parameter-shortname>/r/<run>/s/<step>.grib2
/v1/m/<model-name>/p/<parameter-shortname>/lvt1/<level-type>/lv1/<level>/r/<run>/s/<step>.grib2
/v1/m/<model-name>/p/<parameter-shortname>/wvl1/<wavelength>/lvt1/<level-type>/lv1/<level>/r/<run>/s/<step>.grib2
There are the following new variants for the ensemble models:

/v1/m/<model-name>/p/<parameter-shortname>/lvt1/<level-type>/lv1/<level>/r/<run>/e/<ensemble-member>/s/<step>.grib2
/v1/m/<model-name>/p/<parameter-shortname>/r/<run>/e/<ensemble-member>/s/<step>.grib2
/v1/m/<model-name>/p/<parameter-shortname>/wvl1/<wavelength>/lvt1/<level-type>/lv1/<level>/r/<run>/e/<ensemble-member>/s/<step>.grib2
Here is <level-type> the numeric GRIB-header typeOfFirstFixedSurface. . .. Examples of <level-type>:

100: Isobaric Surface / "Pressure"
106: Depth Below Land Surface
150: Generalized Vertical Height Coordinate

Everything seems to be all caps


Usage
=====

For usage see:

    $ python download_combine_upload_icon.py --help

For ease the script is also packaged as a docker container:

    $ docker run \
        -e HF_TOKEN=<SOME_TOKEN> \
        -v /some/path:/tmp/nwp \
        ghcr.io/openclimatefix/icon-etl:main --help

Datasets
========

Example ICON-EU dataset (~20Gb):

     <xarray.Dataset> Dimensions: (step: 93, latitude: 657, longitude: 1377, isobaricInhPa: 20)

     Coordinates:
         * isobaricInhPa (isobaricInhPa) float64 50.0 70.0 100.0 ... 950.0 1e+03
         * latitude (latitude) float64 29.5 29.56 29.62 ... 70.44 70.5
         * longitude (longitude) float64 -23.5 -23.44 -23.38 ... 62.44 62.5
         * step (step) timedelta64[ns] 00:00:00 ... 5 days 00:00:00 time datetime64[ns] ...
         valid_time (step) datetime64[ns] dask.array<chunksize=(93,), meta=np.ndarray>

     Data variables: (3/60)
         alb_rad (step, latitude, longitude)
            float32 dask.array<chunksize=(37, 326, 350), meta=np.ndarray>
         ... ...
         v (step, isobaricInhPa, latitude, longitude)
            float32 dask.array<chunksize=(37, 20, 326, 350), meta=np.ndarray>
         z0 (step, latitude, longitude)
            float32 dask.array<chunksize=(37, 326, 350), meta=np.ndarray>
"""

import argparse
import bz2
import dataclasses
import datetime as dt
import logging
import os
import pathlib
import shutil
import sys
from multiprocessing import Pool, cpu_count

import requests
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
import icechunk
from pyasn1.codec.ber.decoder import decode

# Set up logging
handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="".join((
        "{",
        '"message": "%(message)s", ',
        '"severity": "%(levelname)s", "timestamp": "%(asctime)s.%(msecs)03dZ", ',
        '"logging.googleapis.com/labels": {"python_logger": "%(name)s"}, ',
        '"logging.googleapis.com/sourceLocation": ',
        '{"file": "%(filename)s", "line": %(lineno)d, "function": "%(funcName)s"}',
        "}",
    )),
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("cfgrib.dataset").setLevel(logging.WARNING)
log = logging.getLogger("icon-etl")

"""
# CDO grid description file for global regular grid of ICON.
gridtype  = lonlat
xsize     = 2399
ysize     = 1199
xfirst    = -180
xinc      = 0.15
yfirst    = -90
yinc      = 0.15
"""

var_2d_list_europe = [
    "alb_rad",
    "alhfl_s",
    "ashfl_s",
    "asob_s",
    "asob_t",
    "aswdifd_s",
    "aswdifu_s",
    "aswdir_s",
    "athb_s",
    "athb_t",
    "aumfl_s",
    "avmfl_s",
    "cape_con",
    "cape_ml",
    "clch",
    "clcl",
    "clcm",
    "clct",
    "clct_mod",
    "cldepth",
    "evap_pl",
    "h_snow",
    "hbas_con",
    "htop_con",
    "htop_dc",
    "hsnow_max",
    "hzerocl",
    "pmsl",
    "ps",
    "qv_2m",
    "qv_s",
    "rain_con",
    "rain_gsp",
    "relhum_2m",
    "rho_snow",
    "runoff_g",
    "runoff_s",
    "snow_con",
    "snow_gsp",
    "snowlmt",
    "t_2m",
    "t_g",
    "t_snow",
    "tch",
    "tcm",
    "td_2m",
    "tmax_2m",
    "tmin_2m",
    "tot_prec",
    "tqc",
    "tqi",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "w_snow",
    "ww",
    "z0",
]

var_2d_list_global = [
    "alb_rad",
    "alhfl_s",
    "ashfl_s",
    "asob_s",
    "asob_t",
    "aswdifd_s",
    "aswdifu_s",
    "aswdir_s",
    "athb_s",
    "athb_t",
    "aumfl_s",
    "avmfl_s",
    "cape_con",
    "cape_ml",
    "clch",
    "clcl",
    "clcm",
    "clct",
    "clct_mod",
    "cldepth",
    "c_t_lk",
    "freshsnw",
    "fr_ice",
    "h_snow",
    "h_ice",
    "h_ml_lk",
    "hbas_con",
    "htop_con",
    "htop_dc",
    "hzerocl",
    "pmsl",
    "ps",
    "qv_s",
    "rain_con",
    "rain_gsp",
    "relhum_2m",
    "rho_snow",
    "runoff_g",
    "runoff_s",
    "snow_con",
    "snow_gsp",
    "t_2m",
    "t_g",
    "t_snow",
    "t_ice",
    "tch",
    "tcm",
    "td_2m",
    "tmax_2m",
    "tmin_2m",
    "tot_prec",
    "tqc",
    "tqi",
    "tqr",
    "tqs",
    "tqv",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "w_snow",
    "ww",
    "z0",
]

var_ensemble_list_global = [
    "asob_s",
    "aswdir_s",
    "athb_s",
    "relhum_2m",
    "sobs_rad",
    "t_2m",
    "td_2m",
    "thbs_rad",
    "tot_prec",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "clct"
]

var_ensemble_list_2d_eu = [
    "aswdifd_s",
    "aswdir_s",
    "athb_s",
    "t_2m",
    "snow_con",
    "snow_gsp",
    "sobs_rad",
    "thbs_rad",
    "tot_prec",
    "u_10m",
    "v_10m",
    "vmax_10m",
    "tqv",
    "ps",
    "cape_ml",
]

var_ensemble_list_3d_eu = [
    "fi",
    "u",
    "v",
    "t"
]

# "p", "omega", "clc", "qv", "tke", "w" are model-level only in global so have been removed
var_3d_list_global = ["fi", "relhum", "t", "u", "v"]

var_model_list_global = ["u", "v", "w", "qv"]

# "p", "omega", "qv", "tke", "w" are model-level only in europe so have been removed
var_3d_list_europe = ["clc", "fi", "omega", "relhum", "t", "u", "v"]

var_model_list_europe = ["u", "v", "w"]

invarient_list = ["clat", "clon"]

pressure_levels_global = [
    1000,
    950,
    925,
    900,
    850,
    800,
    700,
    600,
    500,
    400,
    300,
    250,
    200,
    150,
    100,
    70,
    50,
    30,
]

model_levels_global = list(range(121))
model_levels_europe = list(range(76))

pressure_levels_europe = [
    1000,
    950,
    925,
    900,
    875,
    850,
    825,
    800,
    775,
    700,
    600,
    500,
    400,
    300,
    250,
    200,
    150,
    100,
    70,
    50,
]

"""
Analysis from all models all variables

Full forecast just winds and solar-related



"""

# ICON ART variables, which are decently different to normal ICON
global_art_3d_vars = [
    "FI",
    "RELHUM",
]

global_art_2d_vars = [
    "ACCDRYDEPO_DUSTA",
    "ACCDRYDEPO_DUSTB",
    "ACCDRYDEPO_DUSTC",
    "ACCEMISS_DUSTA",
    "ACCEMISS_DUSTB",
    "ACCEMISS_DUSTC",
    "ACCSEDIM_DUSTA",
    "ACCSEDIM_DUSTB",
    "ACCSEDIM_DUSTC",
    "ACCWETDEPO_CON_DUSTA",
    "ACCWETDEPO_CON_DUSTB",
    "ACCWETDEPO_CON_DUSTC",
    "ACCWETDEPO_GSP_DUSTA",
    "ACCWETDEPO_GSP_DUSTB",
    "ACCWETDEPO_GSP_DUSTC",
    "ALB_RAD",
    "ALHFL_S",
    "ASHFL_S",
    "ASOB_S",
    "ASOB_T",
    "ASWDIFD_S",
    "ASWDIFU_S",
    "ASWDIR_S",
    "ATHB_S",
    "ATHB_T",
    "AUMFL_S",
    "AVMFL_S",
    "CAPE_CON",
    "CAPE_ML",
    "CLCH",
    "CLCL",
    "CLCM",
    "CLCT",
    "CLCT_MOD",
    "CLDEPTH",
    "C_T_LK",
    "DUST_TOTAL_MC_VI",
    "FRESHSNW",
    "FR_ICE",
    "HBAS_CON",
    "HTOP_CON",
    "HTOP_DC",
    "HZEROCL",
    "H_ICE",
    "H_ML_LK",
    "H_SNOW",
    "LPI_CON_MAX",
    "PMSL",
    "PS",
    "QV_S",
    "RAIN_CON",
    "RAIN_GSP",
    "RELHUM_2M",
    "RHO_SNOW",
    "ROOTDP",
    "RUNOFF_G",
    "RUNOFF_S",
    "SMI",
    "SNOW_CON",
    "SNOW_GSP",
    "TAOD_DUST",
    "TCH",
    "TCM",
    "TD_2M",
    "T_2M",
    "TOT_PREC",
    "TQC",
    "TQI",
    "TQR",
    "TQS",
    "TQV",
    "U_10M",
    "V_10M",
    "VMAX_10M",
    "T_SNOW",
    "W_SNOW",
]

global_art_soil_vars = [
    "T_SO",
    "W_SO",
    "W_SO_ICE"
]

global_art_model_vars = [ # Most are potential layers, CEIL and SAT are wavelengths, MC_LAYER has its own numbers
    "T",
    "U",
    "V",
    "W",
    "P",
    "DUSTA",
    "DUSTB",
    "DUSTC",
    "DUSTA0",
    "DUSTB0",
    "DUSTC0",
    "DUST_MAX_TOTAL_MC_LAYER",
    "DUST_TOTAL_MC",
    "CEIL_BSC_DUST",
    "SAT_BSC_DUST",
    "AER_DUST",
]

global_art_ensemble_model_vars = [
    "T",
    "U",
    "V",
    "W",
    "P",
    "DUSTA",
    "DUSTB",
    "DUSTC",
    "DUSTA0",
    "DUSTB0",
    "DUSTC0",
    "DUST_TOTAL_MC",
    "CEIL_BSC_DUST",
    "SAT_BSC_DUST",
    "AER_DUST",
]

global_art_ensemble_3d_vars = global_art_3d_vars

global_art_ensemble_2d_vars = [
    "ACCDRYDEPO_DUSTA",
    "ACCDRYDEPO_DUSTB",
    "ACCDRYDEPO_DUSTC",
    "ACCEMISS_DUSTA",
    "ACCEMISS_DUSTB",
    "ACCEMISS_DUSTC",
    "ACCSEDIM_DUSTA",
    "ACCSEDIM_DUSTB",
    "ACCSEDIM_DUSTC",
    "ACCWETDEPO_CON_DUSTA",
    "ACCWETDEPO_CON_DUSTB",
    "ACCWETDEPO_CON_DUSTC",
    "ACCWETDEPO_GSP_DUSTA",
    "ACCWETDEPO_GSP_DUSTB",
    "ACCWETDEPO_GSP_DUSTC",
    "ALHFL_S",
    "ASHFL_S",
    "ASOB_S",
    "ASOB_T",
    "ASWDIFD_S",
    "ASWDIFU_S",
    "ASWDIR_S",
    "ATHB_S",
    "ATHB_T",
    "CAPE_ML",
    "CLCH",
    "CLCL",
    "CLCM",
    "CLCT",
    "CLCT_MOD",
    "DUST_TOTAL_MC_VI",
    "FRESHSNW",
    "FR_ICE",
    "HBAS_CON",
    "HTOP_CON",
    "H_ICE",
    "H_SNOW",
    "LPI_CON_MAX",
    "PMSL",
    "PS",
    "RAIN_CON",
    "RAIN_GSP",
    "RELHUM_2M",
    "SNOW_CON",
    "SNOW_GSP",
    "TAOD_DUST",
    "TCH",
    "TCM",
    "TD_2M",
    "T_2M",
    "TOT_PREC",
    "TQC",
    "TQI",
    "TQV",
    "U_10M",
    "V_10M",
    "VMAX_10M",
    "T_SNOW",
    "W_SNOW",
]


@dataclasses.dataclass
class Config:
    """Details differing elements for each icon instance."""

    vars_2d: list[str]
    vars_3d: list[str]
    vars_invarient: list[str]
    vars_model: list[str]
    base_url: str
    model_url: str
    var_url: str
    chunking: dict[str, int]
    f_steps: list[int]
    repo_id: str
    per_init_time: bool = False

GLOBAL_CONFIG = Config(
    vars_2d=var_2d_list_global,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_global
        for p in pressure_levels_global
    ],
    vars_model=[],
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon/grib",
    var_url="icon_global_icosahedral",
    f_steps=list(range(0, 79)), # + list(range(81, 183, 3)),
    repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "step": 37,
        "values": 122500,
        "isobaricInhPa": -1,
    },
)

GLOBAL_ART_CONFIG = Config(
    vars_2d=global_art_2d_vars,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_global
        for p in pressure_levels_global
    ],
    vars_model=global_art_model_vars,
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp/v1/m",
    model_url="icon-art/p",
    var_url="icon_global_icosahedral",
    f_steps=list(range(0, 52)), # + list(range(81, 183, 3)),
    repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "step": 26,
        "values": 122500,
        "isobaricInhPa": -1,
    },
)

GLOBAL_ENSEMBLE_CONFIG = Config(
    vars_2d=var_2d_list_global,
    vars_3d=[],
    vars_model=[],
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon-eps/grib",
    var_url="icon-eps_global_icosahedral",
    f_steps=list(range(0, 49)), # + list(range(81, 183, 3)),
    repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "step": 24,
        "values": 122500,
        "number": -1,
    },
)

GLOBAL_MODEL_CONFIG = Config(
    vars_2d=[],
    vars_model=[
        v + "@" + str(p)
        for v in var_model_list_global
        for p in model_levels_global
    ],
    vars_3d=[],
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon/grib",
    var_url="icon_global_icosahedral",
    f_steps=list(range(0, 79)), # + list(range(81, 183, 3)),
    repo_id="openclimatefix/dwd-icon-global",
    chunking={
        "step": 37,
        "values": 122500,
        "generalVertical": -1,
    },
)

EUROPE_CONFIG = Config(
    vars_2d=var_2d_list_europe,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_europe
        for p in pressure_levels_europe
    ],
    vars_model=[],
    vars_invarient=[],
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon-eu/grib",
    var_url="icon-eu_europe_regular-lat-lon",
    f_steps=list(range(0, 79)), # + list(range(81, 123, 3)),
    repo_id="openclimatefix/dwd-icon-eu",
    chunking={
        "step": 37,
        "latitude": 326,
        "longitude": 350,
        "isobaricInhPa": -1,
    },
)

EUROPE_ENSEMBLE_CONFIG = Config(
    vars_2d=var_ensemble_list_2d_eu,
    vars_3d=[
        v + "@" + str(p)
        for v in var_3d_list_europe
        for p in pressure_levels_europe
    ],
    vars_model=[],
    vars_invarient=invarient_list,
    base_url="https://opendata.dwd.de/weather/nwp",
    model_url="icon-eu-eps/grib",
    var_url="icon-eu-eps_europe_icosahedral",
    f_steps=list(range(0, 49)), # + list(range(81, 123, 3)),
    repo_id="openclimatefix/dwd-icon-eu",
    chunking={
        "step": 24,
        "values": 122500,
        "isobaricInhPa": -1,
        "number": -1,
    },
)

def find_file_name(
    config: Config,
    run_string: str,
    date: dt.date,
) -> list[str]:
    """Find file names to be downloaded.

    - vars_2d, a list of 2d variables to download, e.g. ['t_2m']
    - vars_3d, a list of 3d variables to download with pressure
      level, e.g. ['t@850','fi@500']
    - f_times, forecast steps, e.g. 0 or list(np.arange(1, 79))
    Note that this function WILL NOT check if the files exist on
    the server to avoid wasting time. When they're passed
    to the download_extract_files function if the file does not
    exist it will simply not be downloaded.
    """
    # New data comes in 3 ish hours after the run time,
    # ensure the script is running with a decent buffer
    date_string = date.strftime("%Y%m%d") + run_string
    if (len(config.vars_2d) == 0) and (len(config.vars_3d) == 0) and (len(config.vars_model) == 0):
        raise ValueError("You need to specify at least one 2D, 3D, or model level variable")

    if config in [GLOBAL_ENSEMBLE_CONFIG, EUROPE_ENSEMBLE_CONFIG]:
        up_var_name = False
    else:
        up_var_name = True

    urls = []
    for f_time in config.f_steps:
        for var in config.vars_2d:
            var_url = config.var_url + "_single-level"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var}/"
                f"{var_url}_{date_string}_{f_time:03d}_{var.upper() if up_var_name else var.lower()}.grib2.bz2",
            )
        for var in config.vars_3d:
            var_t, plev = var.split("@")
            var_url = config.var_url + "_pressure-level"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var_t}/"
                f"{var_url}_{date_string}_{f_time:03d}_{plev}_{var_t.upper() if up_var_name else var_t.lower()}.grib2.bz2",
            )
        for var in config.vars_invarient:
            var_url = config.var_url + "_time-invariant"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var}/"
                f"{var_url}_{date_string}_{var.upper() if up_var_name else var.lower()}.grib2.bz2",
            )
        for var in config.vars_model:
            var_t, plev = var.split("@")
            var_url = config.var_url + "_model-level"
            urls.append(
                f"{config.base_url}/{config.model_url}/{run_string}/{var_t}/"
                f"{var_url}_{date_string}_{f_time:03d}_{plev}_{var_t.upper() if up_var_name else var_t.lower()}.grib2.bz2",
            )
    return urls


def download_extract_url(url: str, folder: str) -> str | None:
    """Download and extract a file from a given url."""
    filename = folder + os.path.basename(url).replace(".bz2", "")

    # If the file already exists, do nothing
    if os.path.exists(filename):
        return filename
    # If the file exists as a .bz2, convert it
    elif os.path.exists(filename + ".bz2"):
        with open(filename + ".bz2", "rb") as source, open(filename, "wb") as dest:
            try:
                dest.write(bz2.decompress(source.read()))
            except Exception as e:
                log.error(f"Failed to decompress {filename}.bz2: {e}")
                return None
            os.remove(filename + ".bz2")
        return filename
    # If the file does not exist, attempt to download and extract it
    else:
        r = requests.get(url, stream=True, timeout=60*60)
        if r.status_code == requests.codes.ok:
            with r.raw as source, open(filename, "wb") as dest:
                dest.write(bz2.decompress(source.read()))
            return filename
        else:
            log.debug(f"Failed to download {url}")
            return None


def run(path: str, config: Config, run: str, date: dt.date) -> None:
    """Download ICON data, combine and upload to Hugging Face Hub."""
    # Download files first for run
    if not pathlib.Path(f"{path}/{run}/").exists():
        pathlib.Path(f"{path}/{run}/").mkdir(parents=True, exist_ok=True)

    results: list[str | None] = []
    not_done = True
    while not_done:
        try:
            urls = find_file_name(
                config=config,
                run_string=run,
                date=date,
            )
            log.info(f"Downloading {len(urls)} files for {date} run {run}")

            # We only parallelize if we have a number of files
            # larger than the cpu count
            if len(urls) > cpu_count():
                pool = Pool(cpu_count())
                results = pool.starmap(
                    download_extract_url,
                    [(url, f"{path}/{run}/") for url in urls],
                )
                pool.close()
                pool.join()
            else:
                results = []
                for url in urls:
                    result = download_extract_url(url, f"{path}/{run}/")
                    if result is not None:
                        results.append(result)

            not_done = False
        except Exception as e:
            log.error(f"Error downloading files for run {run}: {e}")

    filepaths: list[str] = list(filter(None, results))
    if len(filepaths) == 0:
        log.info(f"No files downloaded for run {run}: Data not yet available")
        return
    nbytes: int = sum([os.path.getsize(f) for f in filepaths])
    log.info(
        f"Downloaded {len(filepaths)} files "
        f"with {len(results) - len(filepaths)} failed downloads "
        f"for run {run}: {nbytes} bytes",
    )

    # Write files to zarr
    log.info(f"Converting {len(filepaths)} files for run {run}")
    if config in [GLOBAL_ENSEMBLE_CONFIG, EUROPE_ENSEMBLE_CONFIG]:
        up_var_name = False
    else:
        up_var_name = True
    if config in [GLOBAL_CONFIG, GLOBAL_ENSEMBLE_CONFIG, EUROPE_ENSEMBLE_CONFIG, GLOBAL_MODEL_CONFIG]:
        if config in [GLOBAL_CONFIG, GLOBAL_MODEL_CONFIG]:
            name = "icon_global"
            lat = "CLAT"
            lon = "CLON"
        elif config == GLOBAL_ENSEMBLE_CONFIG:
            name = "icon-eps_global"
            lat = "clat"
            lon = "clon"
        elif config == EUROPE_ENSEMBLE_CONFIG:
            name = "icon-eu-eps_europe"
            lat = "clat"
            lon = "clon"
        lon_ds = xr.open_mfdataset(
            f"{path}/{run}/{name}_icosahedral_time-invariant_*_{lon}.grib2", engine="cfgrib", decode_timedelta=True,
        )
        lat_ds = xr.open_mfdataset(
            f"{path}/{run}/{name}_icosahedral_time-invariant_*_{lat}.grib2", engine="cfgrib", decode_timedelta=True,
        )
        lons = lon_ds.tlon.values
        lats = lat_ds.tlat.values

    datasets = []
    for var_3d in [v.split("@")[0] for v in config.vars_3d]:
        var_paths: list[list[pathlib.Path]] = []
        for step in config.f_steps:
            step_paths: list[pathlib.Path] = list(pathlib.Path(f"{path}/{run}/").glob(
                f"{config.var_url}_pressure-level_*_{str(step).zfill(3)}_*_{var_3d.upper() if up_var_name else var_3d.lower()}.grib2",
            ))
            if len(step_paths) == 0:
                log.debug(f"No files found for 3D var {var_3d} for run {run} and step {step}")
                log.debug(list(pathlib.Path(f"{path}/{run}/").glob(
                    f"{config.var_url}_pressure-level_*_{var_3d.upper() if up_var_name else var_3d.lower()}.grib2",
                )))
                continue
            else:
                var_paths.append(step_paths)
        if len(var_paths) == 0:
            log.warning(f"No files found for 3D var {var_3d} for run {run}")
            continue
        try:
            ds = xr.concat(
                [
                    xr.open_mfdataset(
                        p,
                        engine="cfgrib",
                        backend_kwargs={"errors": "ignore"} if config == GLOBAL_CONFIG else {},
                        combine="nested",
                        concat_dim="isobaricInhPa",
                        decode_timedelta=True,
                    ).sortby("isobaricInhPa")
                    for p in var_paths
                ],
                dim="step",
            ).sortby("step")
        except Exception as e:
            log.error(e)
            continue
        ds = ds.rename({v: var_3d for v in ds.data_vars})
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        datasets.append(ds)
    if len(datasets) != 0:
        ds_atmos = xr.merge(datasets)
        log.debug(f"Merged 3D datasets: {ds_atmos}")

    model_datasets = []
    for var_model in [v.split("@")[0] for v in config.vars_model]:
        var_paths: list[list[pathlib.Path]] = []
        for step in config.f_steps:
            step_paths: list[pathlib.Path] = list(pathlib.Path(f"{path}/{run}/").glob(
                f"{config.var_url}_model-level_*_{str(step).zfill(3)}_*_{var_model.upper() if up_var_name else var_model.lower()}.grib2",
            ))
            if len(step_paths) == 0:
                log.debug(f"No files found for 3D var {var_model} for run {run} and step {step}")
                log.debug(list(pathlib.Path(f"{path}/{run}/").glob(
                    f"{config.var_url}_model-level_*_{var_model.upper() if up_var_name else var_model.lower()}.grib2",
                )))
                continue
            else:
                var_paths.append(step_paths)
        if len(var_paths) == 0:
            log.warning(f"No files found for Model var {var_model} for run {run}")
            continue
        try:
            ds = xr.concat(
                [
                    xr.open_mfdataset(
                        p,
                        engine="cfgrib",
                        backend_kwargs={"errors": "ignore"} if config == GLOBAL_CONFIG else {},
                        combine="nested",
                        concat_dim="generalVertical",
                        decode_timedelta=True,
                    ).sortby("generalVertical")
                    for p in var_paths
                ],
                dim="step",
            ).sortby("step")
        except Exception as e:
            log.error(e)
            continue
        ds = ds.rename({v: var_model for v in ds.data_vars})
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        model_datasets.append(ds)
    if len(model_datasets) != 0:
        ds_model = xr.merge(model_datasets)
        log.debug(f"Merged Model datasets: {ds_model}")

    total_dataset = []
    for var_2d in config.vars_2d:
        paths = list(
            pathlib.Path(f"{path}/{run}").glob(
                f"{config.var_url}_single-level_*_*_{var_2d.upper() if up_var_name else var_2d.lower()}.grib2",
            ),
        )
        if len(paths) == 0:
            log.warning(f"No files found for 2D var {var_2d} at {run}")
            continue
        try:
            ds = (
                xr.open_mfdataset(
                    paths,
                    engine="cfgrib",
                    backend_kwargs={"errors": "ignore"},
                    combine="nested",
                    concat_dim="step",
                    decode_timedelta=True,
                )
                .sortby("step")
                .drop_vars("valid_time")
            )
        except Exception as e:
            log.error(e)
            continue
        # Rename data variable to name in list, so no conflicts
        ds = ds.rename({v: var_2d for v in ds.data_vars})
        # Remove extra coordinates that are not dimensions or time
        coords_to_remove = []
        for coord in ds.coords:
            if coord not in ds.dims and coord != "time":
                coords_to_remove.append(coord)
        if len(coords_to_remove) > 0:
            ds = ds.drop_vars(coords_to_remove)
        total_dataset.append(ds)
    if len(total_dataset) != 0:
        ds_surface = xr.merge(total_dataset)
        log.debug(f"Merged 2D datasets: {ds_surface}")
    # Merge both
    ds = None
    if len(total_dataset) != 0:
        ds = ds_surface
    if len(datasets) != 0:
        if ds is None:
            ds = ds_atmos
        else:
            ds = xr.merge([ds, ds_atmos])
    if len(model_datasets) != 0:
        if ds is None:
            ds = ds_model
        else:
            ds = xr.merge([ds, ds_model])
    # Add lats and lons manually for icon global
    if config in [GLOBAL_CONFIG, GLOBAL_MODEL_CONFIG]:
        ds = ds.assign_coords({"latitude": lats, "longitude": lons})
    log.debug(f"Created final dataset for run {run}: {ds}")
    encoding = {var: {"compressors": zarr.codecs.BloscCodec(
                                        cname="zstd",
                                        clevel=9,
                                        shuffle=zarr.codecs.BloscShuffle.bitshuffle,
                                    ),} for var in ds.data_vars}
    encoding["time"] = {"units": "nanoseconds since 1970-01-01"}
    if config.per_init_time:
        icechunk_path = f"{path}_{date.strftime('%Y%m%d')}_{run}.icechunk"
    else:
        icechunk_path = f"{path}.icechunk"
    if os.path.exists(icechunk_path):
        appending = True
    else:
        appending = False
    storage = icechunk.local_filesystem_storage(icechunk_path)
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")
    # First check which variables are already present, only append if all are present and time is not a duplicate
    if appending:
        existing_ds = xr.open_zarr(session.store, consolidated=False)
        if not all([v in existing_ds.data_vars for v in ds.data_vars]):
            log.warning(
                f"Not all variables present in existing dataset, cannot append. "
                f"Existing: {list(existing_ds.data_vars)}, New: {list(ds.data_vars)}",
            )
            return
        if ds.time.isin(existing_ds.time).any():
            log.info(f"Data for {date} run {run} already present, skipping")
            return
        try:
            to_icechunk(ds.chunk(config.chunking), session, append_dim="time")
        except Exception as e:
            log.error(f"Failed to append data for {date} run {run}: {e}")
            return
    else:
        to_icechunk(ds.chunk(config.chunking), session, encoding=encoding)
    session.commit(f"Added ICON data for {date} run {run}")
    log.info(f"Added {run} to {path}.icechunk")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("area", choices=["eu", "global", "global_model", "global_ensemble", "eu_ensemble"], help="Area to download data for")
    parser.add_argument("--path", default="./", help="Folder in which to save files") # noqa: S108
    parser.add_argument(
        "--run",
        default="all",
        choices=["00", "06", "12", "18", "all"],
        help="Run time to download",
    )
    parser.add_argument("--rm", action="store_true", help="Remove files on completion")
    parser.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        default=dt.datetime.now(tz=dt.UTC).date(),
        help="Date to download data for (YYY-MM-DD)",
    )

    # Check HF_TOKEN env var is present
    # _ = os.environ["HF_TOKEN"]
    log.info("Starting ICON download script")
    args = parser.parse_args()

    if args.date < dt.datetime.now(tz=dt.UTC).date() and args.rm:
        log.warning(
            "The script is set to remove downloaded files. "
            "If all your files are in the same 'run' folder, "
            "you will lose data before it has a chance to be processed. "
            "Consider running the script without the --rm flag.",
        )

    path: str = f"{args.path}/{args.area}"
    if args.run == "all":
        runs: list[str] = ["00", "06", "12", "18"]
    else:
        runs = [args.run]
    # Cleanup any leftover files in path
    for hour in runs:
        log.info(f"Cleaning up leftover files in {path}/{hour}")
        if args.rm:
            shutil.rmtree(f"{path}/{hour}", ignore_errors=True)
        if args.area == "eu":
            run(path=path, config=EUROPE_CONFIG, run=hour, date=args.date)
        elif args.area == "global":
            run(path=path, config=GLOBAL_CONFIG, run=hour, date=args.date)
        elif args.area == "global_model":
            run(path=path, config=GLOBAL_MODEL_CONFIG, run=hour, date=args.date)
        elif args.area == "global_ensemble":
            run(path=path, config=GLOBAL_ENSEMBLE_CONFIG, run=hour, date=args.date)
        elif args.area == "eu_ensemble":
            run(path=path, config=EUROPE_ENSEMBLE_CONFIG, run=hour, date=args.date)
        # Remove files
        if args.rm:
            log.info(f"Removing downloaded files in {path}/{hour}")
            shutil.rmtree(f"{path}/{hour}", ignore_errors=True)