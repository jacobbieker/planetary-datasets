"""NASA AERONET sun photometer observations as ragged Icechunk stores.

AERONET publishes every Version 3 product two ways: weekly "all sites" tarballs of one text
file per site (``/data_push/V3/``), and a web service answering a time window for every
site at once (``/cgi-bin/print_web_data_v3`` and ``print_web_data_inv_v3``). Each product,
quality level and averaging gets a store of its own under ``bkr/obs/aeronet/``, for
example ``aod_level20_all_points.icechunk``; see :data:`PRODUCTS`.

Layout. One row per observation along ``observation``; the sites along ``site``, with their
AERONET name, identifier, location, land use and principal investigators. ``site_index``
ties a row to its site. A store opens with ``xarray.open_zarr``. Columns keep their values
exactly as published: every number is stored in the narrowest type that reprints the
published text (float32 where it does, else float64, ``-999`` declared as the fill value),
counts as integers, dates as datetimes, and the few text columns with a fixed vocabulary
(quality level, phase function mode, scan type) as CF flag codes. Names are readable
snake case (``aerosol_optical_depth_500nm``, ``number_of_points_...``), each variable
keeping the original column header as ``long_name``. The raw polarized principal plane
files have no header row and are not archived.

Sources and revisions. The tarballs, about 130 GB compressed and slow to fetch, are read
once for the history and again every :data:`BULK_REFRESH_DAYS`. The web service keeps the
stores current: the last :data:`RECENT_DAYS` days of every web product every few hours,
and the Level 2.0 products over the past :data:`REVISIT_DAYS` days weekly, since Level 2.0
is only released once an instrument's post-deployment calibration is in, months later.
The history lane fills everything else up to the recent window: Level 1.0 and the total
optical depth products (web only) from 1993, and the days since a product's last tarball
read, a month at a time (:func:`history_days`), so no day of a web product is left out.

:mod:`planetary_datasets.providers.observations.aeronet` is a different thing: Level 1.5
AOD averaged onto an hourly ``(time, station)`` grid for the Dagster assets. This module
archives everything AERONET publishes, unaveraged.

Lanes (``python -m planetary_datasets.providers.observations.aeronet_archive MODE``):
``bulk``, ``recent``, ``revisit`` and ``history``, as above. Each makes one request at a
time; a per-store lock lets them share stores, a lane re-reading what a store holds when
another has written it since.

A row is identified by its site, time and, where present, phase function mode, scan type,
measurement type and instrument; a row already stored is never written again, so sources
overlap freely. A value AERONET revises after it is stored (Level 1.5 can change until
Level 2.0 is out) is not rewritten: the stores keep the first published value, and Level
2.0 is the final one.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import io
import pathlib
import re
import sys
import tarfile
import time
from typing import Iterable, Iterator, Sequence

import icechunk
import numpy as np
import pandas as pd
import requests
import xarray as xr
import zarr
from loguru import logger

from planetary_datasets.common import multidim
from planetary_datasets.common.download import download_one
from planetary_datasets.config import Config, get_config

BASE_URL = "https://aeronet.gsfc.nasa.gov"
BULK_URL = f"{BASE_URL}/data_push/V3/"
WEB_URL = f"{BASE_URL}/cgi-bin/print_web_data_v3"
WEB_INV_URL = f"{BASE_URL}/cgi-bin/print_web_data_inv_v3"
SITES_URL = f"{BASE_URL}/aeronet_locations_extended_v3.txt"
STORE_BASE = "bkr/obs/aeronet"

#: Days of every web product re-read each pass.
RECENT_DAYS = 7
#: Days of the Level 2.0 web products re-read each revisit pass.
REVISIT_DAYS = 365
#: A tarball read longer ago than this is read again.
BULK_REFRESH_DAYS = 90
#: Rows gathered before an append; a few hundred megabytes of values at most.
BATCH_ROWS = 2_000_000
#: First day of the AERONET record.
ARCHIVE_START = pd.Timestamp("1993-01-01")
#: Published fill value for a missing number.
FILL = -999
#: Seconds to wait for the web service; a day of every site can take a minute.
TIMEOUT = 300

HEADERS = {"User-Agent": "planetary-datasets AERONET archiver (bkr/obs on Source Cooperative)"}


# ---------------------------------------------------------------------------- products


@dataclasses.dataclass(frozen=True)
class Product:
    """One store: a product at one quality level and averaging.

    Attributes:
        key: Store name, e.g. ``aod_level20_all_points``.
        bulk: Tarball path under :data:`BULK_URL`, if AERONET publishes one.
        web: Web service queries (one per request) for a day of every site, if served.
        inversion: Whether the queries go to the inversion web service.
    """

    key: str
    bulk: str | None = None
    web: tuple[str, ...] = ()
    inversion: bool = False

    @property
    def store_prefix(self) -> str:
        """Where the store lives in the bucket."""
        return f"{STORE_BASE}/{self.key}.icechunk"

    @property
    def level(self) -> str:
        """The quality level: ``10``, ``15`` or ``20``."""
        return re.search(r"level(\d+)", self.key).group(1)


AVERAGING = {
    "all_points": ("All_Points", "10"),
    "daily": ("Daily", "20"),
    "monthly": ("Monthly", None),
}


def _products() -> dict[str, Product]:
    products = []
    # Direct sun: AOD and SDA as tarballs at Levels 1.5 and 2.0; Level 1.0 and the total
    # optical depth only through the web service, which has no monthly averages.
    for family, web_name in (("aod", "AOD"), ("sda", "SDA")):
        for level in ("10", "15", "20"):
            for averaging, (bulk_name, avg) in AVERAGING.items():
                bulk = None
                if level != "10":
                    bulk = f"{web_name}/{web_name}_Level{level}_{bulk_name}_V3.tar.gz"
                web = (f"{web_name}{level}=1&AVG={avg}",) if avg else ()
                if bulk or web:
                    products.append(Product(f"{family}_level{level}_{averaging}", bulk, web))
    for level in ("10", "15", "20"):
        for averaging, (_, avg) in AVERAGING.items():
            if avg:
                products.append(
                    Product(f"tot_level{level}_{averaging}", None, (f"TOT{level}=1&AVG={avg}",))
                )
    # Inversions: almucantar and hybrid scans, each with phase functions and polarized
    # variants. The web service serves the non-polarized ones (phase functions all points
    # only, one request per mode).
    for scan, directory, web_name in (("almucantar", "INV", "ALM"), ("hybrid", "INV_HYB", "HYB")):
        stem = "INV" if scan == "almucantar" else "INV_HYB"
        for pfn in (False, True):
            for polar in (False, True):
                for level, bulk_level in (("15", "15"), ("20", "2")):
                    for averaging, (bulk_name, avg) in AVERAGING.items():
                        parts = [stem] + (["PFN"] if pfn else []) + (["Polar"] if polar else [])
                        bulk = (
                            f"{directory}/{'_'.join(parts)}_Level{bulk_level}_{bulk_name}_V3.tar.gz"
                        )
                        key = (
                            f"inversion_{scan}"
                            + ("_phase_functions" if pfn else "")
                            + ("_polarized" if polar else "")
                            + f"_level{level}_{averaging}"
                        )
                        web: tuple[str, ...] = ()
                        if not polar and avg:
                            if pfn and avg == "10":
                                web = tuple(
                                    f"product=PFN&AVG=10&{web_name}{level}=1&pfn_type={t}"
                                    for t in (0, 1, 2)
                                )
                            elif not pfn:
                                web = (f"product=ALL&AVG={avg}&{web_name}{level}=1",)
                        products.append(Product(key, bulk, web, inversion=True))
    # Ocean colour (AERONET-OC) water-leaving radiances.
    for level in ("15", "20"):
        for averaging, (bulk_name, _) in AVERAGING.items():
            products.append(
                Product(
                    f"ocean_colour_level{level}_{averaging}",
                    f"LWN/LWN_Level{level}_{bulk_name}_V3.tar.gz",
                )
            )
    return {p.key: p for p in products}


#: Every store, by key.
PRODUCTS: dict[str, Product] = _products()


# ---------------------------------------------------------------------------- names

#: Units AERONET writes in parentheses, as CF units.
UNITS = {
    "cm": "cm",
    "um": "um",
    "degrees": "degrees",
    "decimal_degrees": "degrees",
    "m": "m",
    "meters": "m",
    "hpa": "hPa",
    "dobson": "DU",
    "du": "DU",
    "%": "%",
    "km": "km",
    "m/s": "m s-1",
    "degrees_c": "degC",
}
#: Abbreviations spelled out, as whole snake-case tokens.
WORDS = {
    "aod": "aerosol_optical_depth",
    "ae": "angstrom_exponent",
    "pw": "precipitable_water",
    "reff": "effective_radius",
    "volc": "volume_concentration",
    "vmr": "volume_median_radius",
    "std": "standard_deviation",
    "eff": "efficiency",
    "rad": "radiative",
    "boa": "bottom_of_atmosphere",
    "toa": "top_of_atmosphere",
    "lw": "water_leaving_radiance",
    "lwn": "normalized_water_leaving_radiance",
    "li": "sky_radiance",
    "lt": "total_radiance",
    "rho": "sea_surface_reflectance_factor",
    "no2": "nitrogen_dioxide",
    "o3": "ozone",
    "co2": "carbon_dioxide",
    "ch4": "methane",
    "watervapor": "water_vapour",
    "asea": "sea_azimuth_angle",
    "zsea": "sea_zenith_angle",
    "reg": "regression",
}
#: Whole columns whose name is set rather than derived.
FIXED = {
    "day_of_year": "day_of_year",
    "day_of_year(fraction)": "day_of_year_fraction",
    "latitude(degrees)": "latitude",
    "longitude(degrees)": "longitude",
    "elevation(m)": "elevation",
    "elevation(meters)": "elevation",
    "site_latitude(degrees)": "latitude",
    "site_longitude(degrees)": "longitude",
    "site_elevation(m)": "elevation",
    "aeronet_instrument_number": "instrument_number",
    "instrument_number": "instrument_number",
    "precipitable_water(cm)": "total_column_water_vapour",
    "total_precipitable_water(cm)": "total_column_water_vapour",
    "pressure(hpa)": "surface_pressure",
    "sensor_temperature(degrees_c)": "sensor_temperature",
}
#: Unit conversions to ERA5's units, declared as CF attributes (values stay as published).
CONVERSIONS = {
    "total_column_water_vapour": {"scale_factor": 10.0, "add_offset": 0.0, "units": "kg m-2"},
    "surface_pressure": {"scale_factor": 100.0, "add_offset": 0.0, "units": "Pa"},
    "sensor_temperature": {"scale_factor": 1.0, "add_offset": 273.15, "units": "K"},
}
#: Columns that name the site.
SITE_COLUMNS = {"site", "aeronet_site", "aeronet_site_name", "aeronet_aeronet_site"}
#: Text columns with a fixed vocabulary, stored as CF flag codes.
VOCABULARIES = {
    "data_quality_level": {"lev10": 10, "lev15": 15, "lev20": 20},
    "phase_function_mode": {"Total": 0, "Fine": 1, "Coarse": 2},
    "retrieval_scan_type": {"Almucantar": 0, "Hybrid": 1},
    "measurement_type": {"solar": 0, "lunar": 1},
}
CATEGORY_COLUMNS = {
    "data_quality_level": "data_quality_level",
    "inversion_data_quality_level": "data_quality_level",
    "level": "data_quality_level",
    "phase_function_mode": "phase_function_mode",
    "retrieval_measurement_scan_type": "retrieval_scan_type",
    "measurement_type(solar or lunar)": "measurement_type",
}
#: Columns read as integers (counts and identifiers).
INTEGER = re.compile(
    r"^(n|num_points|num_days)\[|^day_of_year$|instrument_number$|^number_of_wavelengths$|^if_"
)
#: Variables that, with the site and time, identify a row.
KEY_VARIABLES = (
    "phase_function_mode",
    "retrieval_scan_type",
    "measurement_type",
    "instrument_number",
)


def _snake(text: str) -> str:
    """Snake case, abbreviations spelled out and CamelCase words split."""
    text = text.replace("f/Q", "_f_over_Q_")
    text = text.replace("Forcing_Eff", "Radiative_Forcing_Efficiency")
    # A trailing -C, -F or -T is the coarse, fine or total mode (VolC-C, Std-T).
    text = re.sub(
        r"-([CFT])(?=$|_)", lambda m: "_" + {"C": "coarse", "F": "fine", "T": "total"}[m[1]], text
    )
    out = []
    for token in re.split(r"[^0-9A-Za-z]+", text):
        if not token:
            continue
        if token.lower() in WORDS:
            out.append(WORDS[token.lower()])
            continue
        for part in re.findall(r"[A-Z]+\d*(?![a-z])|[A-Z]?[a-z]+\d*|\d+[a-z]*", token):
            out.append(WORDS.get(part.lower(), part.lower()))
    return "_".join(out)


def _number(text: str) -> str:
    """``0.050000`` as ``0_05``, ``180.000000`` as ``180``."""
    value = f"{float(text):g}"
    return value.replace(".", "_").replace("-", "minus_")


def column_name(column: str) -> tuple[str, str | None]:
    """A readable variable name for an AERONET column header, and its units."""
    raw = column.strip()
    lowered = raw.lower()
    if lowered in FIXED:
        units = re.search(r"\(([^)]*)\)$", raw)
        return FIXED[lowered], UNITS.get(units.group(1).lower()) if units else None
    count = re.match(r"^(N|NUM_POINTS|NUM_DAYS)\[(.*)\]$", raw)
    if count:
        inner, _ = column_name(count.group(2))
        what = "number_of_days" if count.group(1) == "NUM_DAYS" else "number_of_points"
        return f"{what}_{inner}", None
    phase = re.match(r"^(\d+(?:\.\d+)?)\[(\d+)nm\]-(\w+)$", raw)
    if phase:
        angle, wavelength, mode = phase.groups()
        return f"phase_function_{mode.lower()}_{wavelength}nm_at_{_number(angle)}_degrees", None
    if re.fullmatch(r"\d+\.\d+", raw):
        return f"volume_size_distribution_at_{_number(raw)}um", "um3 um-2"
    angstrom = re.match(r"^(\d+)-(\d+)_Angstrom_Exponent(\[Polar\])?$", raw)
    if angstrom:
        low, high, polar = angstrom.groups()
        return f"angstrom_exponent_{low}_{high}nm" + ("_polar" if polar else ""), None
    if raw.startswith("dAE/dln(wavelength)"):
        rest = raw.removeprefix("dAE/dln(wavelength)")
        return "angstrom_exponent_spectral_derivative" + (
            "_" + _bare(rest) if _bare(rest) else ""
        ), None

    units = None
    for found in re.findall(r"\(([^)]*)\)", raw):
        if found.lower() in UNITS:
            units = UNITS[found.lower()]
    body = re.sub(
        r"\(([^)]*)\)",
        lambda m: ""
        if m.group(1).lower() in UNITS or m.group(1) in ("AE", "solar or lunar")
        else f"_{m.group(1)}",
        raw,
    )
    return _bare(body), units


def _bare(body: str) -> str:
    """Snake case of a header with its units removed: wavelengths kept, symbols dropped."""

    def bracket(match: re.Match) -> str:
        inside = match.group(1)
        if re.fullmatch(r"\d+(\.\d+)?n?m|Empty|Polar", inside):
            return f"_{inside}"
        return ""  # a symbol such as [tau_a] or [alpha]

    body = re.sub(r"\[([^\[\]]*)\]", bracket, body)
    return _snake(body)


def variable_names(columns: Sequence[str]) -> list[tuple[str, str | None]]:
    """:func:`column_name` for each column, a repeated name numbered from the second."""
    names, seen = [], {}
    for column in columns:
        name, units = column_name(column)
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        names.append((name, units))
    return names


# ---------------------------------------------------------------------------- parsing


@dataclasses.dataclass
class Table:
    """One parsed file or response: its rows and what its header says about the site."""

    rows: pd.DataFrame
    attrs: dict[str, dict]
    site: str | None = None
    principal_investigators: str = ""


def _header_index(lines: Sequence[str]) -> int:
    for i, line in enumerate(lines[:20]):
        first = line.split(",", 1)[0].strip()
        if line.count(",") >= 1 and (
            first.startswith("Date") or first in ("Month",) or first.lower() in SITE_COLUMNS
        ):
            return i
    raise ValueError("no column header in the first 20 lines")


def _float_column(values: np.ndarray, places: int) -> np.ndarray:
    """Numbers as float32 when that reprints every published value, else float64.

    ``places`` is the most decimals any number in the file was published with; a value
    that rounds to the same number at that precision prints the same text.
    """
    values = np.asarray(values, dtype="float64")
    narrow = values.astype("float32")
    with np.errstate(invalid="ignore", over="ignore"):
        exact = np.array_equal(
            np.round(narrow.astype("float64"), places), np.round(values, places), equal_nan=True
        )
    return narrow if exact else values


#: Decimals of a published number (dates use colons, so only numbers match).
DECIMALS = re.compile(r"(?<![\w.])-?\d+\.(\d*)")


def _times(date: pd.Series, clock: pd.Series | None) -> np.ndarray:
    """``dd:mm:yyyy`` (or ``dd-mm-yyyy``) and ``hh:mm:ss`` as datetime64[s]."""
    text = date.str.strip().str.replace("-", ":", regex=False)
    if clock is not None:
        text = text + " " + clock.str.strip()
        parsed = pd.to_datetime(text, format="%d:%m:%Y %H:%M:%S")
    else:
        parsed = pd.to_datetime(text, format="%d:%m:%Y")
    return parsed.to_numpy("datetime64[s]")


def parse_table(text: str) -> Table:
    """One AERONET text file or web response as typed columns.

    Raises:
        ValueError: The text has no column header, or a text column holds a value outside
            its vocabulary.
    """
    lines = text.splitlines()
    index = _header_index(lines)
    head = lines[:index]
    columns = [c.strip() for c in lines[index].split(",")]
    # A header ending in a separator (SDA daily) names an empty last column no row has.
    while columns and not columns[-1]:
        columns.pop()
    lowered = [c.lower() for c in columns]
    # Some rows end with a separator; anything past the named columns is dropped.
    body = "\n".join(line for line in lines[index + 1 :] if line.strip() and "<" not in line)
    textual = {
        i
        for i, c in enumerate(lowered)
        if c in SITE_COLUMNS
        or c in CATEGORY_COLUMNS
        or c == "month"
        or c.startswith(("date", "time", "last_"))
    }
    # Rows may carry fewer fields than the header (read as missing) or a trailing
    # separator (dropped), so read as wide as the widest row and keep the named columns.
    width = max([len(columns)] + [line.count(",") + 1 for line in body.splitlines()])
    frame = pd.read_csv(
        io.StringIO(body),
        header=None,
        names=range(width),
        dtype={i: str for i in textual},
        keep_default_na=False,
        na_values={i: [""] for i in range(len(columns)) if i not in textual},
        skipinitialspace=True,
    )[list(range(len(columns)))]
    places = max((len(d) for d in DECIMALS.findall(body)), default=0)
    out: dict[str, np.ndarray] = {}
    attrs: dict[str, dict] = {}
    names = variable_names(columns)

    def find(*options: str) -> int | None:
        for option in options:
            if option in lowered:
                return lowered.index(option)
        return None

    used: set[int] = set()
    date = find("date(dd:mm:yyyy)", "date_(dd:mm:yyyy)", "date(dd-mm-yyyy)")
    clock = find("time(hh:mm:ss)", "time_(hh:mm:ss)")
    month = find("month")
    if date is not None:
        out["time"] = _times(frame[date], frame[clock] if clock is not None else None)
        used |= {date} | ({clock} if clock is not None else set())
    elif month is not None:
        out["time"] = pd.to_datetime(frame[month].str.strip(), format="%Y-%b").to_numpy(
            "datetime64[s]"
        )
        used.add(month)
    else:
        raise ValueError("no date column")
    attrs["time"] = {"long_name": "observation time (UTC)"}
    processed = find(
        "last_processing_date(dd:mm:yyyy)", "last_date_processed", "last_processing_date"
    )
    if processed is not None:
        processed_clock = find("last_processing_time(hh:mm:ss)")
        out["last_processing_time"] = _times(
            frame[processed], frame[processed_clock] if processed_clock is not None else None
        )
        attrs["last_processing_time"] = {"long_name": "when AERONET last processed the row"}
        used |= {processed} | ({processed_clock} if processed_clock is not None else set())

    site_column = next((i for i, c in enumerate(lowered) if c in SITE_COLUMNS), None)
    sites = None
    if site_column is not None:
        sites = frame[site_column].str.strip().to_numpy(object)
        used.add(site_column)
        # Some files carry a second site column; it must agree with the first.
        for i, c in enumerate(lowered):
            if c in SITE_COLUMNS and i != site_column:
                if not (frame[i].str.strip().to_numpy(object) == sites).all():
                    raise ValueError(f"site columns {columns[site_column]} and {columns[i]} differ")
                used.add(i)

    for i, (column, (name, units)) in enumerate(zip(columns, names)):
        if i in used or not column:
            continue
        values = frame[i]
        category = CATEGORY_COLUMNS.get(column.lower())
        if category:
            vocabulary = VOCABULARIES[category]
            unknown = set(values.str.strip()) - set(vocabulary)
            if unknown:
                raise ValueError(f"{column}: values outside the known vocabulary: {unknown}")
            out[category] = values.str.strip().map(vocabulary).to_numpy("uint8")
            attrs[category] = {
                "long_name": column,
                "flag_values": list(vocabulary.values()),
                "flag_meanings": " ".join(vocabulary),
            }
            continue
        attr = {"long_name": column}
        if units:
            attr["units"] = units
        if i in textual:
            raise ValueError(f"{column}: an unexpected text column")
        numbers = values
        if numbers.dtype.kind not in "iuf":
            numbers = pd.to_numeric(numbers, errors="coerce")
        numbers = numbers.to_numpy("float64")
        if np.isnan(numbers).any():
            # A blank is published as -999 elsewhere; keep it the fill value here too.
            numbers = np.where(np.isnan(numbers), FILL, numbers)
        if INTEGER.search(column.lower()):
            if not np.array_equal(numbers, np.round(numbers)):
                raise ValueError(f"{column}: not integers")
            out[name] = numbers.astype("int32")
            attr["_FillValue"] = FILL
        else:
            out[name] = _float_column(numbers, places)
            attr["_FillValue"] = float(FILL)
            if name in CONVERSIONS:
                attr = {**attr, **CONVERSIONS[name], "source_units": units}
        attrs[name] = attr

    rows = pd.DataFrame(out)
    if sites is not None:
        rows["site_name"] = sites
    pi = next((line for line in head if "PI=" in line), "")
    return Table(
        rows=rows,
        attrs=attrs,
        site=head[1].strip() if len(head) > 1 and sites is None else None,
        principal_investigators=pi[pi.index("PI=") :].strip() if pi else "",
    )


# ---------------------------------------------------------------------------- sites


def site_list(session: requests.Session | None = None) -> pd.DataFrame:
    """AERONET's extended site list: one row per site, its latest location."""
    response = (session or requests).get(SITES_URL, headers=HEADERS, timeout=120)
    response.raise_for_status()
    lines = response.text.splitlines()
    frame = pd.read_csv(io.StringIO("\n".join(lines[1:])), dtype=str)
    frame.columns = [c.strip() for c in frame.columns]
    return frame.drop_duplicates("Name", keep="last").set_index("Name")


SITE_VARIABLES = {
    "site_name": "AERONET site name",
    "site_id": "AERONET site identifier",
    "site_latitude": "site latitude",
    "site_longitude": "site longitude",
    "site_elevation": "site elevation",
    "land_use_type": "land use type",
    "principal_investigators": "principal investigators, as the data files list them",
}


# ---------------------------------------------------------------------------- stores


class AeronetStore:
    """One product's store: what it holds, and appending rows it does not.

    The sites and the identity of every stored row are read once, on first use, and kept
    up to date as rows are appended, so a run reads the store's arrays once.
    """

    def __init__(self, product: Product, config: Config, sites: pd.DataFrame | None = None):
        """Open ``product``'s store; ``sites`` (:func:`site_list`) fills in new sites."""
        self.product = product
        self.config = config
        self.repo = config.icechunk_repo(product.store_prefix)
        self.site_table = sites
        self._sites: dict[str, int] | None = None
        self._keys: np.ndarray | None = None
        self._seen: str | None = None

    # -- reading

    def _group(self) -> zarr.Group | None:
        try:
            group = zarr.open_group(self.repo.readonly_session("main").store, mode="r")
        except (zarr.errors.GroupNotFoundError, FileNotFoundError):
            return None
        return group if list(group.array_keys()) else None

    def _tip(self) -> str | None:
        try:
            return self.repo.lookup_branch("main")
        except icechunk.IcechunkError:
            return None

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """Hold the store's lock: the lanes of one machine write it one at a time."""
        path = pathlib.Path(self.config.data_dir) / "aeronet" / "locks" / f"{self.product.key}.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _load(self) -> None:
        tip = self._tip()
        if self._sites is not None and tip == self._seen:
            return
        # First use, or another lane wrote since: what the store holds is read afresh.
        self._seen = tip
        group = self._group()
        if group is None:
            self._sites, self._keys = {}, np.empty(0, "uint64")
            return
        names = [str(v) for v in group["site_name"][:]]
        self._sites = {name: i for i, name in enumerate(names)}
        # Raw values: the hash must see the same numbers the incoming rows have.
        frame = pd.DataFrame(
            {
                name: group[name][:]
                for name in ("site_index", "time", *KEY_VARIABLES)
                if name in group
            }
        )
        self._keys = np.unique(row_keys(frame))

    def ledger(self) -> set[str]:
        """Tarballs (``bulk:<day>``) and history months (``web:<month>``) read whole."""
        return multidim.ledger(self.repo)

    # -- writing

    def _site_rows(self, names: Iterable[str], principal_investigators: dict[str, str]):
        """New sites' station-table rows, in the order they get their indices."""
        table = self.site_table if self.site_table is not None else pd.DataFrame()
        rows = {k: [] for k in SITE_VARIABLES}
        for name in names:
            info = table.loc[name] if name in table.index else None

            def get(column, default=""):
                if info is None or column not in info:
                    return default
                return info[column]

            rows["site_name"].append(name)
            rows["site_id"].append(get("New_Site_ID"))
            rows["site_latitude"].append(float(get("Latitude(decimal_degrees)", "nan")))
            rows["site_longitude"].append(float(get("Longitude(decimal_degrees)", "nan")))
            rows["site_elevation"].append(float(get("Altitude(Meters)", "nan")))
            rows["land_use_type"].append(get("Land_Use_type"))
            rows["principal_investigators"].append(principal_investigators.get(name, ""))
        return rows

    def append(self, tables: Sequence[Table]) -> int:
        """Append the rows of ``tables`` not already stored. Returns how many."""
        with self._locked():
            self._load()
            written = self._append(tables)
            self._seen = self._tip()
        return written

    def _append(self, tables: Sequence[Table]) -> int:
        frames, attrs, pis = [], {}, {}
        for table in tables:
            rows = table.rows
            if "site_name" not in rows:
                if not table.site:
                    raise ValueError("rows without a site")
                rows = rows.assign(site_name=table.site)
            if table.principal_investigators:
                named = table.site or (rows["site_name"].iat[0] if len(rows) else None)
                if named and (table.site or rows["site_name"].nunique() == 1):
                    pis[named] = table.principal_investigators
            frames.append(rows)
            for name, attr in table.attrs.items():
                attrs.setdefault(name, attr)
        if not frames:
            return 0
        rows = pd.concat(frames, ignore_index=True)
        # A column one file has and another lacks is fill there.
        for name, attr in attrs.items():
            if name in rows and rows[name].isna().any() and "_FillValue" in attr:
                filled = rows[name].fillna(attr["_FillValue"])
                kind = next(f[name].dtype for f in frames if name in f)
                rows[name] = filled.astype(kind)

        new_sites = [s for s in dict.fromkeys(rows["site_name"]) if s not in self._sites]
        first = len(self._sites)
        site_index = {**self._sites, **{s: first + i for i, s in enumerate(new_sites)}}
        rows["site_index"] = rows["site_name"].map(site_index).astype("uint16")
        rows = rows.drop(columns="site_name")
        keys = row_keys(rows)
        fresh = ~np.isin(keys, self._keys)
        # Within a batch too: the first of two identical rows is kept.
        _, first_seen = np.unique(keys, return_index=True)
        once = np.zeros(len(rows), bool)
        once[first_seen] = True
        keep = fresh & once
        rows = rows[keep]
        new_sites = [s for s in new_sites if site_index[s] in set(rows["site_index"])]
        if rows.empty:
            return 0
        # Indices must stay contiguous when a new site's rows were all duplicates.
        if len(new_sites) != len(site_index) - first:
            renumber = {site_index[s]: first + i for i, s in enumerate(new_sites)}
            rows["site_index"] = (
                rows["site_index"].map(lambda i: renumber.get(i, i)).astype("uint16")
            )
        ds = self._dataset(rows, attrs, self._site_rows(new_sites, pis))
        multidim.append_partition(self.repo, ds, None, label=self.product.key)
        self._keys = np.union1d(self._keys, keys[keep])
        self._sites.update({s: first + i for i, s in enumerate(new_sites)})
        return len(rows)

    def _dataset(self, rows: pd.DataFrame, attrs: dict, sites: dict) -> xr.Dataset:
        variables = {}
        for name in rows.columns:
            attr = dict(attrs.get(name, {}))
            if name == "site_index":
                attr = {"long_name": "index of the row's site along the site dimension"}
            values = rows[name].to_numpy()
            if values.dtype.kind == "M":
                values = values.astype("datetime64[s]").astype("int64")
                attr = {**attr, **TIME_UNITS}
            variables[name] = xr.Variable(("observation",), values, attr)
        for name, values in sites.items():
            array = (
                np.asarray(values, dtype=object if isinstance(values[0], str) else "float64")
                if values
                else np.empty(0, object)
            )
            attr = {"long_name": SITE_VARIABLES[name]}
            if name in ("site_latitude", "site_longitude"):
                attr["units"] = "degrees"
            if name == "site_elevation":
                attr["units"] = "m"
            variables[name] = xr.Variable(("site",), array, attr)
        product = self.product
        ds = xr.Dataset(variables)
        ds.attrs = {
            "title": f"AERONET Version 3 {product.key.replace('_', ' ')}",
            "source": "NASA AERONET, https://aeronet.gsfc.nasa.gov",
            "conventions": "CF-1.10",
            "comment": (
                "One row per observation along `observation`; `site_index` gives its row "
                "along `site`. Values as published, -999 the fill value."
            ),
            "citation": (
                "Cite AERONET and the sites' principal investigators: "
                "https://aeronet.gsfc.nasa.gov/new_web/data_usage.html"
            ),
        }
        return ds


def row_keys(frame: pd.DataFrame) -> np.ndarray:
    """A 64-bit hash per row of its identity: site, time and :data:`KEY_VARIABLES`."""
    columns = ["site_index", "time", *[k for k in KEY_VARIABLES if k in frame]]
    key = pd.DataFrame(
        {
            name: (
                frame[name].to_numpy("datetime64[s]").astype("int64")
                if frame[name].dtype.kind == "M"
                else frame[name].to_numpy().astype("int64")
            )
            for name in columns
        }
    )
    return pd.util.hash_pandas_object(key, index=False).to_numpy("uint64")


#: How times are stored: whole seconds, so every source's times encode exactly and a
#: store's units never depend on the first rows written.
TIME_UNITS = {"units": "seconds since 1970-01-01 00:00:00", "calendar": "proleptic_gregorian"}


# ---------------------------------------------------------------------------- bulk


def tar_tables(path: pathlib.Path) -> Iterator[Table]:
    """Every site file of a downloaded tarball, parsed."""
    with tarfile.open(path, mode="r:gz") as archive:
        for member in archive:
            if not member.isfile() or "USAGE" in member.name:
                continue
            text = archive.extractfile(member).read().decode("latin-1")
            try:
                yield parse_table(text)
            except ValueError as exc:
                logger.warning(f"aeronet: {member.name} skipped: {exc}")


def published(url: str, session: requests.Session | None = None) -> tuple[str, int]:
    """The tarball's Last-Modified day (``YYYY-MM-DD``) and its size in bytes."""
    response = (session or requests).head(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    day = pd.Timestamp(response.headers["Last-Modified"]).strftime("%Y-%m-%d")
    return day, int(response.headers.get("Content-Length", 0))


def due(ledger: set[str], published: str, now: pd.Timestamp) -> bool:
    """Whether a tarball published on ``published`` should be read into a store."""
    reads = sorted(p.removeprefix("bulk:") for p in ledger if p.startswith("bulk:"))
    if not reads:
        return True
    if published in reads:
        return False
    return now - pd.Timestamp(reads[-1]) >= pd.Timedelta(BULK_REFRESH_DAYS, "D")


def run_bulk(
    products: Iterable[Product],
    config: Config | None = None,
    sites: pd.DataFrame | None = None,
    now: pd.Timestamp | None = None,
) -> int:
    """Read every product's tarball that is due. Returns the rows written."""
    config = config or get_config()
    now = now or pd.Timestamp.now()
    session = requests.Session()
    sites = sites if sites is not None else site_list(session)
    scratch = pathlib.Path(config.scratch_dir) / "aeronet"
    written = 0
    queue = []
    for product in products:
        if not product.bulk:
            continue
        url = BULK_URL + product.bulk
        store = AeronetStore(product, config, sites)
        day, size = published(url, session)
        if due(store.ledger(), day, now):
            queue.append((size, product.key, product, store, url, day))
    # Smallest first: most stores are online long before the largest tarball is in.
    for size, _, product, store, url, day in sorted(queue):
        dest = scratch / pathlib.Path(product.bulk).name
        logger.info(f"aeronet {product.key}: reading {url} ({size / 1e9:.1f} GB, {day})")
        if download_one(url, dest, retries=5, timeout=TIMEOUT, overwrite=True) is None:
            logger.warning(f"aeronet {product.key}: download failed; next pass")
            continue
        batch, rows = [], 0
        for table in tar_tables(dest):
            batch.append(table)
            rows += len(table.rows)
            if rows >= BATCH_ROWS:
                written += store.append(batch)
                batch, rows = [], 0
        written += store.append(batch)
        if store._group() is not None:
            multidim.mark_written(store.repo, f"bulk:{day}", label=product.key)
        dest.unlink(missing_ok=True)
    return written


# ---------------------------------------------------------------------------- web


def web_query(product: Product, query: str, day: pd.Timestamp) -> str:
    """The web service URL for one query and one UTC day of every site."""
    base = WEB_INV_URL if product.inversion else WEB_URL
    return (
        f"{base}?year={day.year}&month={day.month}&day={day.day}"
        f"&year2={day.year}&month2={day.month}&day2={day.day}&{query}&if_no_html=1"
    )


def fetch_web(
    product: Product, day: pd.Timestamp, session: requests.Session | None = None
) -> list[Table]:
    """Every site's rows of one product for one UTC day, from the web service."""
    tables = []
    for query in product.web:
        url = web_query(product, query, day)
        for attempt in range(1, 4):
            try:
                response = (session or requests).get(url, headers=HEADERS, timeout=TIMEOUT)
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == 3:
                    raise
                logger.warning(f"aeronet {product.key} {day:%Y-%m-%d}: {exc}; retrying")
                time.sleep(30 * attempt)
        text = response.text
        try:
            table = parse_table(text)
        except ValueError:
            if "No data" in text or text.count("\n") < 3:
                continue  # nothing that day
            raise
        if not table.rows.empty:
            tables.append(table)
    return tables


def run_web(
    products: Iterable[Product],
    days: Sequence[pd.Timestamp],
    config: Config | None = None,
    sites: pd.DataFrame | None = None,
    ledger_months: bool = False,
) -> int:
    """Read ``days`` of every product from the web service. Returns the rows written.

    With ``ledger_months``, a calendar month read through to its last day is recorded in
    the store's ledger (``web:YYYY-MM``) and skipped on later runs; the history walk uses
    it. A month still running is read again each pass and only ledgered once it is over.
    """
    config = config or get_config()
    session = requests.Session()
    sites = sites if sites is not None else site_list(session)
    written = 0
    months = pd.Series(pd.DatetimeIndex(days)).groupby(lambda i: f"{days[i]:%Y-%m}")
    for product in products:
        if not product.web:
            continue
        store = AeronetStore(product, config, sites)
        done = store.ledger() if ledger_months else set()
        for month, group in months:
            if f"web:{month}" in done:
                continue
            tables = []
            for day in group:
                tables += fetch_web(product, pd.Timestamp(day), session)
            written += store.append(tables)
            whole = ledger_months and _whole_month(month, group)
            if whole and store._group() is not None:
                multidim.mark_written(store.repo, f"web:{month}", label=product.key)
    return written


def _whole_month(month: str, days: pd.Series) -> bool:
    """Whether ``days`` reach the month's last day (the days before are someone's)."""
    start = pd.Timestamp(f"{month}-01")
    return start + pd.offsets.MonthEnd(0) in set(pd.DatetimeIndex(days))


def recent_window(today: pd.Timestamp) -> pd.DatetimeIndex:
    """Days the ``recent`` lane reads each pass."""
    return pd.date_range(today - pd.Timedelta(RECENT_DAYS, "D"), today)


def history_days(product: Product, ledger: set[str], today: pd.Timestamp) -> pd.DatetimeIndex:
    """Days the ``history`` lane reads for ``product``, up to :func:`recent_window`.

    A web-only product is walked from :data:`ARCHIVE_START`. A product with a tarball is
    walked from :data:`RECENT_DAYS` before the newest tarball read into its store (the
    tarball's last days may still have been filling in), so the gap between that tarball
    and the recent window is never left to the next re-read; until a tarball is read the
    bulk lane is still coming, and nothing is walked.
    """
    if not product.web:
        return pd.DatetimeIndex([])
    end = recent_window(today)[0] - pd.Timedelta(1, "D")
    if product.bulk:
        reads = sorted(e.removeprefix("bulk:") for e in ledger if e.startswith("bulk:"))
        if not reads:
            return pd.DatetimeIndex([])
        start = pd.Timestamp(reads[-1]) - pd.Timedelta(RECENT_DAYS, "D")
    else:
        start = ARCHIVE_START
    return pd.date_range(start, end)


def run_history(
    products: Iterable[Product],
    today: pd.Timestamp,
    config: Config | None = None,
    sites: pd.DataFrame | None = None,
) -> int:
    """Walk every web product's :func:`history_days`, a month at a time. Returns rows."""
    config = config or get_config()
    sites = sites if sites is not None else site_list()
    written = 0
    for product in products:
        ledger = AeronetStore(product, config, sites).ledger()
        days = history_days(product, ledger, today)
        if len(days):
            written += run_web([product], list(days), config, sites, ledger_months=True)
    return written


# ---------------------------------------------------------------------------- lanes


def select(names: Sequence[str]) -> list[Product]:
    """Products whose key contains any of ``names``; all when empty."""
    return [p for k, p in PRODUCTS.items() if not names or any(n in k for n in names)]


def main(argv: Sequence[str] | None = None) -> int:
    """Run one of the archiver's lanes until interrupted, or once with ``--once``."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("mode", choices=("bulk", "recent", "revisit", "history"))
    parser.add_argument("--products", nargs="*", default=[], help="key substrings")
    parser.add_argument("--pause", type=int, default=21600, help="seconds between passes")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    products = select(args.products)

    while True:
        began = time.time()
        today = pd.Timestamp.now().normalize()
        try:
            if args.mode == "bulk":
                written = run_bulk(products)
            elif args.mode == "recent":
                written = run_web(products, list(recent_window(today)))
            elif args.mode == "revisit":
                level2 = [p for p in products if p.level == "20"]
                days = pd.date_range(today - pd.Timedelta(REVISIT_DAYS, "D"), today)
                written = run_web(level2, list(days))
            else:
                written = run_history(products, today)
        except requests.RequestException as exc:
            logger.warning(f"aeronet {args.mode}: {exc}; next pass")
            written = 0
        logger.info(
            f"aeronet {args.mode}: pass done in {time.time() - began:.0f}s, {written:,} row(s)"
        )
        if args.once:
            return 0
        time.sleep(args.pause)


if __name__ == "__main__":
    sys.exit(main())
