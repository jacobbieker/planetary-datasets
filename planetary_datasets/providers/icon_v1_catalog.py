"""Catalog of the DWD ICON ``v1`` GRIB archives and their frozen store schemas.

DWD publishes ICON-Global (``icon``), ICON-ART (``icon-art``) and ICON-ART-EU
(``icon-art-eu``) on its open data server under the ``v1`` "path" layout::

    <model>/p/<PARAM>/r/<YYYY-MM-DDTHH:MM>/s/PT<HHH>H<MM>M.grib2
    <model>/p/<PARAM>/lvt1/<typeOfFirstFixedSurface>/lv1/<level>/r/<run>/s/PT<HHH>H00M.grib2
    <model>/p/<PARAM>/wvl1/<nm>/lvt1/<type>/lv1/<level>/r/<run>/s/PT<HHH>H00M.grib2

The live server (:data:`LIVE_ROOT`) keeps roughly the last day of runs. Source Cooperative
(:data:`ARCHIVE_ROOT`) holds a byte-for-byte mirror of it going back further, with gaps. The
mirror includes DWD's ``index.html`` listing pages and, in some trees, macOS ``._*`` files;
both are ignored.

This module answers two questions for the ICON v1 provider, without decoding any GRIB:

* For one init time, where do its files live and which source should be read
  (:func:`choose_source`, :func:`field_uri`)?
* Which fields make up a run, so every append to a store has the same variables, level axes
  and step axis (:func:`load_schema`, :meth:`ICONSchema.expected_fields`)?

The schemas are discovered from real reference runs by :func:`discover_schema` and frozen
as JSON in ``icon_v1_schemas/<model>.json``. Regenerate one with::

    python -m planetary_datasets.providers.icon_v1_catalog schema icon --source live

Each variable also records the precision it is stored at, ``float16`` or ``float32``, chosen
by sampling real fields (:func:`measure_precision`, :func:`classify_precision`) and kept
across rediscovery. Record or refresh it with::

    python -m planetary_datasets.providers.icon_v1_catalog precision icon --source live

The committed schemas come from the four runs on the live server, one per cycle hour. The
archive mirror often lacks whole parameters or levels of a run (ICON-Global especially),
which the discovery refuses as reference runs; the live server's finished runs are whole.
The cycle hours differ beyond the 00/12Z vs 06/18Z horizon: ICON-ART publishes ``RSTOM``
and ``ALHFL_BS`` at 00Z only. The step axis is the union over all variables, so it is longer
than ``T_2M``'s schedule where a field such as ``VMAX_10M`` stays hourly to the end.

Fields excluded from a schema:

* ``CLAT``/``CLON``: the cell-centre coordinates, kept apart as
  :attr:`ICONSchema.coordinate_params`.
* Fields not on the model's cell grid. ``ELAT``/``ELON`` (edge midpoints) and
  ``VLAT``/``VLON`` (vertices) are excluded by name; any other field is excluded when the
  GRIB2 grid section of its first file reports a ``numberOfDataPoints`` other than the
  model's cell count, or an unstructured-grid number other than the model's.
* Fields published under a layout :class:`FieldKey` cannot address: any directory beside
  ``r/``, ``lvt1/`` and ``wvl1/``.

The wavelength-resolved ICON-ART backscatter fields (``CEIL_BSC_DUST``, ``SAT_BSC_DUST``)
are published once per wavelength in nanometres under ``wvl1/<nm>/`` and are cataloged as
one variable per (param, level type, wavelength); see :attr:`FieldKey.wavelength`.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import importlib.resources
import json
import pathlib
import re
import sys
import threading
import time
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from html.parser import HTMLParser
from typing import Any, Callable, Collection, Iterable, Literal, Mapping, Sequence

import fsspec
import numpy as np
import pandas as pd
import requests
from loguru import logger

ARCHIVE_ROOT = "s3://us-west-2.opendata.source.coop/bkr/icon"
LIVE_ROOT = "https://opendata.dwd.de/weather/nwp/v1/m"
LIVE_RETENTION = pd.Timedelta(24, "h")

Source = Literal["archive", "live"]

#: Runs DWD publishes for these models.
CYCLE_HOURS: tuple[int, ...] = (0, 6, 12, 18)

#: 00Z and 12Z share a forecast horizon, as do 06Z and 18Z. A schema discovered without a
#: reference run for some hour copies that hour from its partner.
CYCLE_PARTNER: dict[int, int] = {0: 12, 12: 0, 6: 18, 18: 6}

#: Cell-centre coordinates. Every run carries them at step 0; they are not data variables.
COORDINATE_PARAMS: tuple[str, str] = ("CLAT", "CLON")

#: Coordinates of the edge and vertex grids, which never match the cell grid.
NON_CELL_GRID_PARAMS: frozenset[str] = frozenset({"ELAT", "ELON", "VLAT", "VLON"})

#: Version of the JSON layout written by :func:`discover_schema`.
SCHEMA_FORMAT_VERSION = 1

#: Upper bound on concurrent listing requests against either source.
MAX_LISTING_WORKERS = 8

#: How long :func:`cached_runs` reuses a listing before asking the source again.
LISTING_TTL: dict[str, pd.Timedelta] = {
    "archive": pd.Timedelta(15, "min"),
    "live": pd.Timedelta(5, "min"),
}

#: Slack on top of :data:`LIVE_RETENTION` before a run is assumed gone from the live server
#: without asking it. Files expire by age from publication, which trails the init time.
LIVE_SLACK = pd.Timedelta(6, "h")

_RUN_FORMAT = "%Y-%m-%dT%H:%M"
_RUN_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
_STEP_NAME = re.compile(r"^PT(\d{3})H(\d{2})M\.grib2$")
_GRIB_HEADER_BYTES = 1024
_HTTP_TIMEOUT = 30.0
_HTTP_RETRIES = 3
_SCHEMA_DIR = "icon_v1_schemas"

#: Lists the children of a model-relative directory (``p/T_2M/r``). Returns raw names as
#: the source gives them; raises :class:`FileNotFoundError` if the directory does not exist.
Lister = Callable[[str], list[str]]

#: Returns the first bytes of a model-relative file.
HeaderReader = Callable[[str], bytes]


class ListingError(RuntimeError):
    """A source could not be listed, so a run's absence from it proves nothing."""


class IncompleteReferenceRun(ValueError):
    """A reference run given to :func:`discover_schema` is missing files."""


# ---------------------------------------------------------------------------------------
# Models and field addressing
# ---------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ICONModelSpec:
    """Static facts about one ICON model's v1 tree.

    Attributes:
        model: Directory name under :data:`ARCHIVE_ROOT` and :data:`LIVE_ROOT`.
        n_cells: Number of cells on the model's icosahedral grid.
        grid_number: GRIB2 ``numberOfGridUsed`` of the cell grid.
        archive_start: First run in the Source Cooperative archive.
    """

    model: str
    n_cells: int
    grid_number: int
    archive_start: pd.Timestamp


MODELS: dict[str, ICONModelSpec] = {
    "icon": ICONModelSpec("icon", 2_949_120, 26, pd.Timestamp("2026-08-25T12:00")),
    "icon-art": ICONModelSpec("icon-art", 1_310_720, 39, pd.Timestamp("2025-09-30T18:00")),
    "icon-art-eu": ICONModelSpec("icon-art-eu", 1_013_404, 61, pd.Timestamp("2025-09-30T18:00")),
}


@dataclasses.dataclass(frozen=True)
class FieldKey:
    """One GRIB file of a run: a parameter at one level and one forecast step.

    Attributes:
        param: DWD parameter name as in the path, e.g. ``T_2M``.
        level_type: ``typeOfFirstFixedSurface`` (100 pressure, 106 soil, 150 model level),
            or None for single-level fields.
        level: Level exactly as it appears in the path (``"85000"``, ``"0.01"``), or None.
        step: Forecast step in hours.
        wavelength: Wavelength in nanometres for the ``wvl1/<nm>/`` trees of the ICON-ART
            backscatter fields, or None for every other field.
    """

    param: str
    level_type: int | None
    level: str | None
    step: int
    wavelength: int | None = None

    def __post_init__(self) -> None:
        """Reject keys that cannot name a file."""
        if (self.level_type is None) != (self.level is None):
            raise ValueError(f"level_type and level must both be set or both be None: {self}")
        if self.step < 0:
            raise ValueError(f"step must be non-negative: {self}")
        if self.wavelength is not None and self.wavelength <= 0:
            raise ValueError(f"wavelength must be positive: {self}")


def _spec(model: str) -> ICONModelSpec:
    try:
        return MODELS[model]
    except KeyError:
        raise ValueError(
            f"unknown ICON model {model!r}, expected one of {sorted(MODELS)}"
        ) from None


def _normalize_init_time(init_time: pd.Timestamp | str | dt.datetime) -> pd.Timestamp:
    """Return ``init_time`` as a naive UTC timestamp, checking it is a model cycle."""
    ts = pd.Timestamp(init_time)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    if ts.hour not in CYCLE_HOURS or ts != ts.floor("h"):
        raise ValueError(f"{init_time} is not an ICON cycle (00/06/12/18Z on the hour)")
    return ts


def _run_name(init_time: pd.Timestamp) -> str:
    return _normalize_init_time(init_time).strftime(_RUN_FORMAT)


def _field_dir(key: FieldKey) -> str:
    base = f"p/{key.param}"
    if key.wavelength is not None:
        base = f"{base}/wvl1/{key.wavelength}"
    if key.level_type is None:
        return base
    return f"{base}/lvt1/{key.level_type}/lv1/{key.level}"


def field_path(model: str, key: FieldKey, init_time: pd.Timestamp) -> str:
    """Return the path of one field relative to the model directory.

    Args:
        model: Model name, a key of :data:`MODELS`.
        key: Field to address.
        init_time: Model run.

    Returns:
        ``p/<PARAM>[/wvl1/<nm>][/lvt1/<t>/lv1/<l>]/r/<YYYY-MM-DDTHH:MM>/s/PT<HHH>H00M.grib2``.

    Raises:
        ValueError: If the model is unknown or ``init_time`` is not a model cycle.
    """
    _spec(model)
    return f"{_field_dir(key)}/r/{_run_name(init_time)}/s/PT{key.step:03d}H00M.grib2"


def _model_uri(model: str, relative: str, source: Source) -> str:
    if source == "archive":
        return f"{ARCHIVE_ROOT}/{model}/{relative}"
    if source == "live":
        return f"{LIVE_ROOT}/{model}/{urllib.parse.quote(relative, safe='/')}"
    raise ValueError(f"source must be 'archive' or 'live', got {source!r}")


def field_uri(model: str, key: FieldKey, init_time: pd.Timestamp, source: Source) -> str:
    """Return the full URI of one field in ``source``.

    The archive is an S3 URI with the run's ``:`` literal; the live server needs it
    percent-encoded as ``%3A``.

    Args:
        model: Model name, a key of :data:`MODELS`.
        key: Field to address.
        init_time: Model run.
        source: ``"archive"`` or ``"live"``.

    Returns:
        A URI fsspec can open with :func:`storage_options`.
    """
    return _model_uri(model, field_path(model, key, init_time), source)


def storage_options(source: Source) -> dict:
    """Return the fsspec storage options for reading ``source``.

    Args:
        source: ``"archive"`` (anonymous S3) or ``"live"`` (plain HTTPS).

    Returns:
        Keyword arguments for ``fsspec.open``/``fsspec.filesystem``.
    """
    if source == "archive":
        return {"anon": True}
    if source == "live":
        return {}
    raise ValueError(f"source must be 'archive' or 'live', got {source!r}")


# ---------------------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------------------


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value)


def parse_index_html(html: str) -> list[str]:
    """Return the entry names of a DWD (nginx autoindex) directory listing.

    Args:
        html: The listing page.

    Returns:
        Raw hrefs that point at direct children; parent, absolute and sorting links dropped.
    """
    parser = _AnchorParser()
    parser.feed(html)
    return [
        href
        for href in parser.hrefs
        if not href.startswith(("../", "/", "?", "#")) and "://" not in href
    ]


def clean_names(names: Iterable[str]) -> list[str]:
    """Reduce raw listing entries to the names of real children.

    Strips path prefixes and trailing slashes, decodes ``%3A`` and drops DWD's
    ``index.html`` pages, macOS ``._*`` resource forks and other dotfiles.

    Args:
        names: Entries as returned by a :data:`Lister`.

    Returns:
        Sorted, deduplicated child names.
    """
    out: set[str] = set()
    for raw in names:
        name = urllib.parse.unquote(raw.rstrip("/").rsplit("/", 1)[-1])
        if not name or name.startswith(".") or name.lower() in {"index.html", "index.htm"}:
            continue
        out.add(name)
    return sorted(out)


def _archive_lister(model: str) -> Lister:
    fs = fsspec.filesystem("s3", **storage_options("archive"))
    root = ARCHIVE_ROOT.removeprefix("s3://") + f"/{model}"

    def ls(relative: str) -> list[str]:
        path = f"{root}/{relative}".rstrip("/")
        try:
            return list(fs.ls(path, detail=False, refresh=True))
        except FileNotFoundError:
            raise
        except (OSError, ValueError) as exc:
            raise ListingError(f"could not list {path}: {exc}") from exc

    return ls


_RETRY_STATUS = frozenset({408, 425, 429})
_http_local = threading.local()


def _session() -> requests.Session:
    """Return this thread's session, so listings reuse connections to the DWD server."""
    session = getattr(_http_local, "session", None)
    if session is None:
        session = requests.Session()
        _http_local.session = session
    return session


def _http_get(url: str, headers: Mapping[str, str] | None = None) -> requests.Response:
    """GET ``url``, retrying transport errors, throttling and 5xx responses.

    Raises:
        FileNotFoundError: On HTTP 404.
        ListingError: On any other failure, after the retries for transient ones.
    """
    last: Exception | None = None
    for attempt in range(1, _HTTP_RETRIES + 1):
        try:
            response = _session().get(url, headers=dict(headers or {}), timeout=_HTTP_TIMEOUT)
        except requests.RequestException as exc:
            last = exc
        else:
            status = response.status_code
            if status == 404:
                raise FileNotFoundError(url)
            if status < 400:
                return response
            if status < 500 and status not in _RETRY_STATUS:
                raise ListingError(f"GET {url} returned HTTP {status}")
            last = requests.HTTPError(f"HTTP {status} for {url}")
        if attempt < _HTTP_RETRIES:
            time.sleep(2 ** (attempt - 1))
    raise ListingError(f"GET {url} failed after {_HTTP_RETRIES} attempts: {last}")


def _live_lister(model: str) -> Lister:
    def ls(relative: str) -> list[str]:
        url = _model_uri(model, relative.strip("/"), "live").rstrip("/") + "/"
        return parse_index_html(_http_get(url).text)

    return ls


def get_lister(model: str, source: Source) -> Lister:
    """Return a :data:`Lister` for one model in one source.

    Args:
        model: Model name, a key of :data:`MODELS`.
        source: ``"archive"`` or ``"live"``.
    """
    _spec(model)
    if source == "archive":
        return _archive_lister(model)
    if source == "live":
        return _live_lister(model)
    raise ValueError(f"source must be 'archive' or 'live', got {source!r}")


def _children(lister: Lister, relative: str) -> list[str]:
    """List ``relative`` with noise removed; a missing directory has no children."""
    try:
        return clean_names(lister(relative))
    except FileNotFoundError:
        return []


def _parse_run(name: str) -> pd.Timestamp | None:
    if not _RUN_NAME.match(name):
        return None
    try:
        return _normalize_init_time(pd.Timestamp(name))
    except ValueError:
        return None


def list_runs(
    model: str,
    source: Source,
    *,
    probe_param: str = "T_2M",
    lister: Lister | None = None,
) -> list[pd.Timestamp]:
    """List the runs one source holds for a model.

    A run is taken to exist when the probe parameter has a directory for it. That does not
    prove every file of the run is present; per-parameter run counts differ slightly.

    Args:
        model: Model name, a key of :data:`MODELS`.
        source: ``"archive"`` or ``"live"``.
        probe_param: Single-level parameter whose ``r/`` directory is listed.
        lister: Listing function to use instead of the network, for tests.

    Returns:
        Sorted, deduplicated init times.

    Raises:
        ListingError: If the source could not be listed.
    """
    lister = lister or get_lister(model, source)
    runs: set[pd.Timestamp] = set()
    for name in _children(lister, f"p/{probe_param}/r"):
        run = _parse_run(name)
        if run is None:
            logger.debug(f"ignoring unexpected run directory {name!r} in {model} {source}")
            continue
        runs.add(run)
    return sorted(runs)


_listing_cache: dict[tuple[str, str, str], tuple[float, tuple[pd.Timestamp, ...]]] = {}
_listing_lock = threading.Lock()


def _clock() -> float:
    """Monotonic seconds for the listing cache; a seam for tests."""
    return time.monotonic()


def cached_runs(
    model: str, source: Source, *, probe_param: str = "T_2M"
) -> tuple[pd.Timestamp, ...]:
    """Return :func:`list_runs`, reusing a listing younger than :data:`LISTING_TTL`.

    Args:
        model: Model name, a key of :data:`MODELS`.
        source: ``"archive"`` or ``"live"``.
        probe_param: Passed to :func:`list_runs`.

    Returns:
        Sorted init times.
    """
    key = (model, source, probe_param)
    ttl = LISTING_TTL[source].total_seconds()
    with _listing_lock:
        hit = _listing_cache.get(key)
        if hit is not None and _clock() - hit[0] < ttl:
            return hit[1]
    runs = tuple(list_runs(model, source, probe_param=probe_param))
    with _listing_lock:
        _listing_cache[key] = (_clock(), runs)
    return runs


def clear_listing_cache() -> None:
    """Forget every cached run listing."""
    with _listing_lock:
        _listing_cache.clear()


def choose_source(
    model: str,
    init_time: pd.Timestamp,
    *,
    now: pd.Timestamp | None = None,
    listings: Mapping[str, Iterable[pd.Timestamp]] | None = None,
) -> Source | None:
    """Decide where to read one run from.

    The live server is preferred for runs up to :data:`LIVE_RETENTION` old that it lists.
    Older runs come from the archive when it has them: DWD deletes files by age, so a run
    past the retention may still be listed live after its first files are gone. Such a run
    is read live only if the archive lacks it, and the live server is not asked at all about
    runs older than :data:`LIVE_RETENTION` plus :data:`LIVE_SLACK`. Listings come from
    :func:`cached_runs`, so a backfill over hundreds of runs lists each source once per
    :data:`LISTING_TTL`.

    Being listed means the run has started to appear, not that it is complete: DWD
    publishes a run over a few hours, and the archive mirror has gaps. Check the files of
    :meth:`ICONSchema.expected_fields` before treating a run as whole.

    Args:
        model: Model name, a key of :data:`MODELS`.
        init_time: Model run.
        now: Current time (naive UTC), for the live retention check. Defaults to now.
        listings: Run listings per source to use instead of querying, for tests. A source
            missing from the mapping is treated as holding no runs.

    Returns:
        ``"live"``, ``"archive"``, or None if neither source has the run.

    Raises:
        ListingError: If a source that could hold the run could not be listed and the
            other source does not have it.
    """
    _spec(model)
    init_time = _normalize_init_time(init_time)
    now = pd.Timestamp.now("UTC").tz_localize(None) if now is None else pd.Timestamp(now)
    if now.tzinfo is not None:
        now = now.tz_convert("UTC").tz_localize(None)
    if init_time > now:
        return None

    def runs(source: Source) -> Iterable[pd.Timestamp]:
        if listings is not None:
            return listings.get(source, ())
        return cached_runs(model, source)

    age = now - init_time
    failure: ListingError | None = None
    in_live = False
    if age <= LIVE_RETENTION + LIVE_SLACK:
        try:
            in_live = init_time in set(runs("live"))
        except ListingError as exc:
            logger.warning(f"live listing for {model} failed, trying the archive: {exc}")
            failure = exc
    if in_live and age <= LIVE_RETENTION:
        return "live"
    if init_time in set(runs("archive")):
        return "archive"
    if in_live:
        return "live"
    if failure is not None:
        raise failure
    return None


# ---------------------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------------------


def parse_grib2_grid(header: bytes) -> tuple[int, int | None]:
    """Read the cell count and grid number from the start of a GRIB2 message.

    Args:
        header: At least the bytes up to the end of section 3.

    Returns:
        ``(numberOfDataPoints, numberOfGridUsed)``; the grid number is None unless the
        grid definition template is 3.101 (unstructured grid).

    Raises:
        ValueError: If ``header`` is not GRIB2 or ends before section 3 does.
    """
    if header[:4] != b"GRIB" or len(header) < 16 or header[7] != 2:
        raise ValueError("not a GRIB2 message")
    offset = 16
    while offset + 5 <= len(header):
        length = int.from_bytes(header[offset : offset + 4], "big")
        number = header[offset + 4]
        if length < 5:
            break
        if number == 3:
            section = header[offset : offset + length]
            if len(section) < length or length < 14:
                break
            points = int.from_bytes(section[6:10], "big")
            template = int.from_bytes(section[12:14], "big")
            grid = None
            if template == 101:
                if length < 18:
                    break
                grid = int.from_bytes(section[15:18], "big")
            return points, grid
        if number > 3:
            break
        offset += length
    raise ValueError("GRIB2 grid definition section not found in header")


def _header_reader(model: str, source: Source) -> HeaderReader:
    if source == "archive":
        fs = fsspec.filesystem("s3", **storage_options("archive"))

        def read(relative: str) -> bytes:
            uri = _model_uri(model, relative, "archive").removeprefix("s3://")
            return fs.cat_file(uri, start=0, end=_GRIB_HEADER_BYTES)

        return read

    def read_live(relative: str) -> bytes:
        headers = {"Range": f"bytes=0-{_GRIB_HEADER_BYTES - 1}"}
        return _http_get(_model_uri(model, relative, "live"), headers).content

    return read_live


def _level_sort_key(level: str) -> tuple[float, str]:
    try:
        return float(level), level
    except ValueError:
        return float("inf"), level


@dataclasses.dataclass
class _Group:
    """Files found for one (param, level_type, wavelength) across the reference runs."""

    param: str
    level_type: int | None
    levels: list[str | None]
    # run -> level -> steps
    found: dict[pd.Timestamp, dict[str | None, set[int]]] = dataclasses.field(default_factory=dict)
    wavelength: int | None = None

    @property
    def key(self) -> tuple[str, int | None, int | None]:
        """What identifies the variable this group becomes."""
        return self.param, self.level_type, self.wavelength

    def field_key(self, level: str | None, step: int) -> FieldKey:
        """The file of this group at one level and step."""
        return FieldKey(self.param, self.level_type, level, step, self.wavelength)


def _walk_param(lister: Lister, param: str) -> tuple[list[_Group], list[dict[str, Any]]]:
    """Find a parameter's wavelengths, level types and levels."""
    return _walk_tree(lister, param, f"p/{param}", None)


def _walk_tree(
    lister: Lister, param: str, base: str, wavelength: int | None
) -> tuple[list[_Group], list[dict[str, Any]]]:
    """Find the level types and levels under ``p/<PARAM>`` or ``p/<PARAM>/wvl1/<nm>``."""
    groups: list[_Group] = []
    excluded: list[dict[str, Any]] = []
    for child in _children(lister, base):
        if child == "r":
            groups.append(_Group(param, None, [None], wavelength=wavelength))
        elif child == "lvt1":
            for lt in _children(lister, f"{base}/lvt1"):
                if not lt.isdigit():
                    excluded.append(
                        _with_wavelength(
                            {"param": param, "level_type": lt, "reason": "non-numeric level type"},
                            wavelength,
                        )
                    )
                    continue
                levels = _children(lister, f"{base}/lvt1/{lt}/lv1")
                levels.sort(key=_level_sort_key)
                if levels:
                    groups.append(_Group(param, int(lt), list(levels), wavelength=wavelength))
        elif child == "wvl1" and wavelength is None:
            for nm in _children(lister, f"{base}/wvl1"):
                if not nm.isdigit() or int(nm) <= 0:
                    excluded.append(
                        {"param": param, "level_type": None, "reason": f"wavelength {nm!r}"}
                    )
                    continue
                found, skipped = _walk_tree(lister, param, f"{base}/wvl1/{nm}", int(nm))
                groups.extend(found)
                excluded.extend(skipped)
        else:
            excluded.append(
                _with_wavelength(
                    {
                        "param": param,
                        "level_type": None,
                        "reason": f"published under {child}/, which FieldKey cannot address",
                    },
                    wavelength,
                )
            )
    return groups, excluded


def _list_steps(lister: Lister, relative_dir: str) -> set[int]:
    steps: set[int] = set()
    for name in _children(lister, relative_dir):
        match = _STEP_NAME.match(name)
        if match is None:
            continue
        if match.group(2) != "00":
            logger.warning(f"ignoring sub-hourly step {relative_dir}/{name}")
            continue
        steps.add(int(match.group(1)))
    return steps


def _step_set_ids(sets: Iterable[tuple[int, ...]]) -> dict[tuple[int, ...], str]:
    ordered = sorted(set(sets), key=lambda s: (-len(s), s))
    return {steps: f"s{i}" for i, steps in enumerate(ordered)}


def discover_schema(
    model: str,
    init_times: Sequence[pd.Timestamp],
    source: Source,
    *,
    lister: Lister | None = None,
    header_reader: HeaderReader | None = None,
    check_grid: bool = True,
    strict: bool = False,
    max_workers: int = MAX_LISTING_WORKERS,
) -> dict:
    """Discover the fields of a model from reference runs.

    Lists ``p/`` and walks every parameter's ``lvt1``/``lv1`` directories with delimiter
    listings, then lists ``r/<run>/s/`` per (param, level, run). Steps are recorded per
    cycle hour. Give one run per hour: the hours differ beyond the 00/12 vs 06/18 horizon
    (ICON-ART publishes ``RSTOM`` and ``ALHFL_BS`` at 00Z only). An hour without a reference
    run copies its partner (:data:`CYCLE_PARTNER`), minus the variables the source has never
    published at that hour, so at least one 00Z or 12Z and one 06Z or 18Z run is needed.

    Args:
        model: Model name, a key of :data:`MODELS`.
        init_times: Reference runs, ideally one per cycle hour.
        source: ``"archive"`` or ``"live"``.
        lister: Listing function to use instead of the network, for tests.
        header_reader: Header reader to use instead of the network, for tests.
        check_grid: Read the GRIB2 header of one file per variable and exclude fields not
            on the model's cell grid.
        strict: Raise :class:`IncompleteReferenceRun` instead of logging when a reference
            run lacks a variable, levels or steps that the other files suggest it should
            have. A run still being published or mirrored would otherwise freeze a
            truncated schema.
        max_workers: Concurrent listing requests, capped at :data:`MAX_LISTING_WORKERS`.

    Returns:
        A JSON-serialisable schema, the input of :class:`ICONSchema`.

    Raises:
        ValueError: If no run covers one of the two cycle classes, or nothing was found.
        IncompleteReferenceRun: If ``strict`` and a reference run has gaps.
    """
    spec = _spec(model)
    runs = sorted({_normalize_init_time(t) for t in init_times})
    hours = {run.hour for run in runs}
    derived: dict[int, int] = {}
    for hour in CYCLE_HOURS:
        if hour not in hours:
            if CYCLE_PARTNER[hour] not in hours:
                raise ValueError(
                    f"reference runs {[r.isoformat() for r in runs]} cover neither "
                    f"{hour:02d}Z nor {CYCLE_PARTNER[hour]:02d}Z"
                )
            derived[hour] = CYCLE_PARTNER[hour]

    lister = lister or get_lister(model, source)
    workers = max(1, min(max_workers, MAX_LISTING_WORKERS))
    params = _children(lister, "p")
    if not params:
        raise ValueError(f"no parameters found for {model} in {source}")

    excluded: list[dict[str, Any]] = []
    walk_params = []
    for param in params:
        if param in COORDINATE_PARAMS:
            continue
        if param in NON_CELL_GRID_PARAMS:
            excluded.append(
                {"param": param, "level_type": None, "reason": "edge/vertex grid coordinate"}
            )
            continue
        walk_params.append(param)
    for coord in COORDINATE_PARAMS:
        if coord not in params:
            logger.warning(f"{model} has no {coord} directory in {source}")

    logger.info(f"walking {len(walk_params)} {model} parameters in {source}")
    groups: list[_Group] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for found, skipped in pool.map(lambda p: _walk_param(lister, p), walk_params):
            groups.extend(found)
            excluded.extend(skipped)

    tasks = [(group, level, run) for group in groups for level in group.levels for run in runs]
    logger.info(f"listing steps of {len(tasks)} (field, level, run) directories")

    def list_task(task: tuple[_Group, str | None, pd.Timestamp]) -> set[int]:
        group, level, run = task
        key = group.field_key(level, 0)
        return _list_steps(lister, f"{_field_dir(key)}/r/{_run_name(run)}/s")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for (group, level, run), steps in zip(tasks, pool.map(list_task, tasks)):
            group.found.setdefault(run, {})[level] = steps

    unverified: list[str] = []
    if check_grid:
        reader = header_reader or _header_reader(model, source)
        off_grid, unverified = _check_grids(model, spec, groups, reader, workers)
        excluded.extend(off_grid)
        bad = {(e["param"], e["level_type"], e.get("wavelength")) for e in off_grid}
        groups = [g for g in groups if g.key not in bad]

    # A variable without files in an hour's reference runs is either not published at that
    # hour (ICON-ART's RSTOM and ALHFL_BS are 00Z-only, so 12Z cannot simply copy 00Z) or
    # missing from those runs. The source's run history for the variable tells them apart.
    # It is also what decides whether a derived hour inherits a variable from its partner.
    def needs_history(group: _Group) -> bool:
        hours_with = {r.hour for r, per_level in group.found.items() if any(per_level.values())}
        return bool(hours_with) and (bool(hours - hours_with) or bool(derived))

    probed = [g for g in groups if needs_history(g)]
    if probed:
        logger.info(f"listing the run history of {len(probed)} variables")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        histories = dict(
            zip(
                [g.key for g in probed],
                pool.map(lambda g: _run_history(lister, g), probed),
            )
        )

    source_hours = hours | {h for hist in histories.values() for h, n in hist.items() if n}

    variables: list[dict[str, Any]] = []
    gaps: list[str] = list(unverified)
    for group in sorted(groups, key=lambda g: (g.param, g.level_type or -1, g.wavelength or -1)):
        history = histories.get(group.key)
        entry, group_gaps = _summarise_group(model, group, runs, derived, history, source_hours)
        gaps.extend(group_gaps)
        if entry is None:
            excluded.append(
                _with_wavelength(
                    {
                        "param": group.param,
                        "level_type": group.level_type,
                        "reason": "no files in the reference runs",
                    },
                    group.wavelength,
                )
            )
            continue
        variables.append(entry)
    if not variables:
        raise ValueError(f"no fields found for {model} in the reference runs")
    for gap in gaps:
        logger.warning(f"{model}: {gap}")
    if gaps and strict:
        raise IncompleteReferenceRun(
            f"{len(gaps)} gaps in the {model} reference runs, e.g. {gaps[:3]}; pick complete "
            "runs or pass strict=False"
        )

    invariant = _invariant_params(variables)
    ids = _step_set_ids(tuple(s) for v in variables for s in v["steps"].values())
    for v in variables:
        v["steps"] = {hour: ids[tuple(steps)] for hour, steps in v["steps"].items()}

    return {
        "format_version": SCHEMA_FORMAT_VERSION,
        "model": model,
        "n_cells": spec.n_cells,
        "grid_number": spec.grid_number,
        "source": source,
        "reference_runs": [run.strftime(_RUN_FORMAT) for run in runs],
        "derived_cycles": {f"{h:02d}": f"{p:02d}" for h, p in sorted(derived.items())},
        "generated_at": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "coordinate_params": list(COORDINATE_PARAMS),
        "invariant_params": invariant,
        "step_sets": {sid: list(steps) for steps, sid in sorted(ids.items(), key=lambda i: i[1])},
        "variables": variables,
        "excluded": sorted(
            excluded,
            key=lambda e: (e["param"], str(e["level_type"]), e.get("wavelength") or -1),
        ),
    }


def _with_wavelength(entry: dict[str, Any], wavelength: int | None) -> dict[str, Any]:
    """Add ``wavelength`` to a schema entry only when set, leaving other entries as they were."""
    if wavelength is not None:
        entry["wavelength"] = wavelength
    return entry


def _check_grids(
    model: str,
    spec: ICONModelSpec,
    groups: list[_Group],
    reader: HeaderReader,
    workers: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Find groups whose first file is not on the model's cell grid.

    Returns:
        Exclusion entries for the off-grid groups, and messages for groups whose header
        could not be read. Those are kept, but count as gaps so a strict discovery fails
        rather than freezing a field it could not check.
    """

    def first_file(group: _Group) -> str | None:
        for run in sorted(group.found):
            for level in group.levels:
                steps = group.found[run].get(level)
                if steps:
                    return field_path(model, group.field_key(level, min(steps)), run)
        return None

    def check(group: _Group) -> dict[str, Any] | str | None:
        path = first_file(group)
        if path is None:
            return None
        try:
            points, grid = parse_grib2_grid(reader(path))
        except (OSError, ValueError, ListingError) as exc:
            return f"{group.param}: could not read the grid of {path}: {exc}"
        if points != spec.n_cells or (grid is not None and grid != spec.grid_number):
            return _with_wavelength(
                {
                    "param": group.param,
                    "level_type": group.level_type,
                    "reason": (
                        f"not on the cell grid: {points} points on grid {grid}, model has "
                        f"{spec.n_cells} cells on grid {spec.grid_number}"
                    ),
                },
                group.wavelength,
            )
        return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(check, groups))
    off_grid = [r for r in results if isinstance(r, dict)]
    unverified = [r for r in results if isinstance(r, str)]
    return off_grid, unverified


def _run_history(lister: Lister, group: _Group) -> Counter[int]:
    """Count the runs per cycle hour the source holds for one variable."""
    level = next(
        (lv for lv in group.levels if any(per.get(lv) for per in group.found.values())),
        group.levels[0],
    )
    key = group.field_key(level, 0)
    runs = (_parse_run(name) for name in _children(lister, f"{_field_dir(key)}/r"))
    return Counter(run.hour for run in runs if run is not None)


def _summarise_group(
    model: str,
    group: _Group,
    runs: Sequence[pd.Timestamp],
    derived: Mapping[int, int],
    history: Counter[int] | None = None,
    source_hours: Collection[int] = CYCLE_HOURS,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Collapse one group's listings into levels and per-hour steps.

    Args:
        model: Model name, for log messages.
        group: Listings of one (param, level_type).
        runs: Reference runs.
        derived: Hours without a reference run, mapped to the partner they copy.
        history: Runs per cycle hour the source holds for the variable, if listed.
        source_hours: Cycle hours the source holds any run of. A derived hour drops a
            variable only if the source has runs at that hour, just none of the variable.

    Returns:
        The variable entry (None if no reference run has a file) and the gaps found. A gap
        is a reference run lacking the variable although the source holds it at that hour
        in other runs, a run lacking levels another run has, levels lacking steps other
        levels of the same run have, or same-hour runs disagreeing. A reference run still
        being published or mirrored shows up here.
    """
    name = f"{group.param}" + (f"@{group.level_type}" if group.level_type else "")
    if group.wavelength is not None:
        name += f"@{group.wavelength}nm"
    present = {run: {lv for lv, steps in group.found.get(run, {}).items() if steps} for run in runs}
    levels: set[str | None] = set().union(*present.values())
    if not levels:
        return None, []
    unused = [lv for lv in group.levels if lv not in levels]
    if unused:
        logger.debug(f"{model} {name}: level directories without reference files: {unused}")

    gaps: list[str] = []
    by_hour: dict[int, set[int]] = {}
    for run in runs:
        per_level = group.found.get(run, {})
        run_steps: set[int] = set().union(*per_level.values()) if per_level else set()
        if present[run]:
            missing = sorted((str(lv) for lv in levels - present[run]), key=_level_sort_key)
            if missing:
                gaps.append(f"{name}: {run:%Y-%m-%dT%H:%M} lacks levels {missing}")
            ragged = sorted(
                (str(lv) for lv in present[run] if per_level[lv] != run_steps),
                key=_level_sort_key,
            )
            if ragged:
                gaps.append(f"{name}: {run:%Y-%m-%dT%H:%M} levels {ragged} lack some steps")
        hour_steps = by_hour.setdefault(run.hour, set())
        if hour_steps and run_steps and hour_steps != run_steps:
            gaps.append(f"{name}: {run.hour:02d}Z reference runs disagree on steps")
        hour_steps |= run_steps

    for hour in sorted(by_hour):
        hour_runs = [run for run in runs if run.hour == hour]
        lacking = ", ".join(f"{run:%Y-%m-%dT%H:%M}" for run in hour_runs if not present[run])
        if not lacking:
            continue
        if by_hour[hour]:
            gaps.append(f"{name}: no files in {lacking}, other {hour:02d}Z reference runs have it")
        elif history is None or history[hour]:
            held = "some" if history is None else history[hour]
            gaps.append(f"{name}: no files in {lacking}, the source holds {held} {hour:02d}Z runs")
        else:
            logger.info(f"{model} {name}: not published at {hour:02d}Z")
    for hour, partner in derived.items():
        if history is not None and not history[hour] and hour in source_hours and by_hour[partner]:
            logger.info(f"{model} {name}: not published at {hour:02d}Z, not copying {partner:02d}Z")
            by_hour[hour] = set()
        else:
            by_hour[hour] = by_hour[partner]
    level_list = sorted((lv for lv in levels if lv is not None), key=_level_sort_key)
    entry = {
        "param": group.param,
        "level_type": group.level_type,
        "levels": level_list,
        "steps": {f"{h:02d}": sorted(by_hour.get(h, ())) for h in CYCLE_HOURS},
    }
    return _with_wavelength(entry, group.wavelength), gaps


def _invariant_params(variables: Sequence[Mapping[str, Any]]) -> list[str]:
    """Return params whose every variable is published at step 0 only."""
    step_zero: dict[str, bool] = {}
    for v in variables:
        steps = [tuple(s) for s in v["steps"].values() if s]
        only_zero = bool(steps) and all(s == (0,) for s in steps)
        step_zero[v["param"]] = step_zero.get(v["param"], True) and only_zero
    return sorted(p for p, flag in step_zero.items() if flag)


# ---------------------------------------------------------------------------------------
# Frozen schemas
# ---------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class VariableSpec:
    """One cataloged variable: a parameter on one level type, at one wavelength.

    Attributes:
        param: DWD parameter name.
        level_type: ``typeOfFirstFixedSurface``, or None for single-level fields.
        levels: Levels as they appear in paths, sorted numerically; empty if single-level.
        steps: Union of the steps published in any cycle.
        wavelength: Wavelength in nanometres for the ``wvl1/<nm>/`` fields, else None.
        dtype: ``"float16"`` or ``"float32"``, the precision it is stored at; see
            :func:`classify_precision`. Schemas without the key store everything as float32.
    """

    param: str
    level_type: int | None
    levels: tuple[str, ...]
    steps: tuple[int, ...]
    wavelength: int | None = None
    dtype: str = "float32"


class ICONSchema:
    """The frozen field layout of one ICON model, loaded from its JSON.

    Attributes:
        model: Model name.
        n_cells: Cells on the model grid.
        grid_number: GRIB2 ``numberOfGridUsed``.
        variables: One entry per (param, level_type, wavelength), sorted; excludes
            non-cell-grid fields and the coordinate parameters.
        invariant_params: Parameters published at step 0 only (also in ``variables``).
        coordinate_params: Cell-centre latitude and longitude parameters.
        step_axis: Sorted union of every variable's steps.
        reference_runs: Runs the schema was discovered from.
        generated_at: When the schema was discovered.
        excluded: Fields deliberately left out, with the reason.
    """

    def __init__(self, data: Mapping[str, Any]) -> None:
        """Build a schema from the dict :func:`discover_schema` returns.

        Args:
            data: Parsed schema JSON.

        Raises:
            ValueError: If the data is malformed or of an unknown format version.
        """
        if data.get("format_version") != SCHEMA_FORMAT_VERSION:
            raise ValueError(f"unsupported schema format {data.get('format_version')!r}")
        self.model: str = data["model"]
        self.n_cells: int = int(data["n_cells"])
        self.grid_number: int = int(data["grid_number"])
        coords = tuple(data.get("coordinate_params", COORDINATE_PARAMS))
        if len(coords) != 2:
            raise ValueError(f"expected two coordinate params, got {coords}")
        self.coordinate_params: tuple[str, str] = (coords[0], coords[1])
        self.invariant_params: tuple[str, ...] = tuple(data["invariant_params"])
        self.reference_runs: tuple[pd.Timestamp, ...] = tuple(
            pd.Timestamp(r) for r in data.get("reference_runs", ())
        )
        self.generated_at: str | None = data.get("generated_at")
        self.excluded: tuple[dict[str, Any], ...] = tuple(data.get("excluded", ()))

        step_sets = {sid: tuple(int(s) for s in steps) for sid, steps in data["step_sets"].items()}
        cycle_steps: dict[tuple[str, int | None, int | None], dict[int, tuple[int, ...]]] = {}
        variables: list[VariableSpec] = []
        for entry in data["variables"]:
            wavelength = entry.get("wavelength")
            wavelength = None if wavelength is None else int(wavelength)
            key = (entry["param"], entry["level_type"], wavelength)
            if key in cycle_steps:
                raise ValueError(f"duplicate variable {key}")
            try:
                per_hour = {int(h): step_sets[sid] for h, sid in entry["steps"].items()}
            except KeyError as exc:
                raise ValueError(f"{key} references unknown step set {exc}") from None
            if set(per_hour) != set(CYCLE_HOURS):
                raise ValueError(f"{key} must give steps for hours {CYCLE_HOURS}")
            cycle_steps[key] = per_hour
            union = tuple(sorted(set().union(*per_hour.values())))
            levels = tuple(str(lv) for lv in entry["levels"])
            if (entry["level_type"] is None) != (not levels):
                raise ValueError(f"{key}: levels must be given exactly for levelled fields")
            dtype = entry.get("dtype", FLOAT32)
            if dtype not in STORAGE_DTYPES:
                raise ValueError(f"{key}: dtype must be one of {STORAGE_DTYPES}, got {dtype!r}")
            variables.append(
                VariableSpec(entry["param"], entry["level_type"], levels, union, wavelength, dtype)
            )
        self.variables: tuple[VariableSpec, ...] = tuple(variables)
        self._cycle_steps = cycle_steps
        self.step_axis: tuple[int, ...] = tuple(sorted(set().union(*(v.steps for v in variables))))

    def __repr__(self) -> str:
        return (
            f"ICONSchema(model={self.model!r}, variables={len(self.variables)}, "
            f"steps={len(self.step_axis)})"
        )

    def variable(
        self, param: str, level_type: int | None, wavelength: int | None = None
    ) -> VariableSpec:
        """Return the spec of one variable.

        Raises:
            KeyError: If the schema has no such variable.
        """
        for v in self.variables:
            if v.param == param and v.level_type == level_type and v.wavelength == wavelength:
                return v
        raise KeyError((param, level_type, wavelength))

    def steps_for_cycle(
        self, param: str, level_type: int | None, hour: int, wavelength: int | None = None
    ) -> tuple[int, ...]:
        """Return the steps one variable has in runs initialised at ``hour``.

        Args:
            param: DWD parameter name.
            level_type: Level type, or None for single-level fields.
            hour: Init hour, one of :data:`CYCLE_HOURS`.
            wavelength: Wavelength in nanometres, for the ``wvl1/`` fields.

        Raises:
            KeyError: If the schema has no such variable.
            ValueError: If ``hour`` is not a cycle hour.
        """
        if hour not in CYCLE_HOURS:
            raise ValueError(f"{hour} is not an ICON cycle hour")
        return self._cycle_steps[(param, level_type, wavelength)][hour]

    def expected_fields(self, init_time: pd.Timestamp) -> list[FieldKey]:
        """Return every field a complete run at ``init_time`` publishes.

        Ordered by variable, then level, then step. The coordinate parameters are not
        included; see :meth:`coordinate_fields`.

        Args:
            init_time: Model run.
        """
        hour = _normalize_init_time(init_time).hour
        fields: list[FieldKey] = []
        for v in self.variables:
            steps = self.steps_for_cycle(v.param, v.level_type, hour, v.wavelength)
            for level in v.levels or (None,):
                fields.extend(
                    FieldKey(v.param, v.level_type, level, step, v.wavelength) for step in steps
                )
        return fields

    def coordinate_fields(self) -> list[FieldKey]:
        """Return the step-0 files of the cell-centre coordinates."""
        return [FieldKey(p, None, None, 0) for p in self.coordinate_params]


def schema_path(model: str) -> pathlib.Path:
    """Return the source-tree path of a model's committed schema JSON."""
    return pathlib.Path(__file__).with_name(_SCHEMA_DIR) / f"{model}.json"


@lru_cache(maxsize=None)
def load_schema(model: str) -> ICONSchema:
    """Load the committed schema of a model.

    Args:
        model: Model name, a key of :data:`MODELS`.

    Raises:
        ValueError: If the model is unknown or its schema does not match :data:`MODELS`.
        FileNotFoundError: If no schema is committed for the model.
    """
    spec = _spec(model)
    resource = importlib.resources.files("planetary_datasets.providers").joinpath(
        _SCHEMA_DIR, f"{model}.json"
    )
    schema = ICONSchema(json.loads(resource.read_text(encoding="utf-8")))
    if (schema.model, schema.n_cells, schema.grid_number) != (
        spec.model,
        spec.n_cells,
        spec.grid_number,
    ):
        raise ValueError(f"schema for {model} does not match {spec}")
    return schema


# ---------------------------------------------------------------------------------------
# Storage precision
# ---------------------------------------------------------------------------------------

FLOAT16 = "float16"
FLOAT32 = "float32"

#: The dtypes a variable may be stored as.
STORAGE_DTYPES: tuple[str, str] = (FLOAT16, FLOAT32)

#: Largest finite float16.
FLOAT16_MAX = 65504.0

#: How far inside :data:`FLOAT16_MAX` the largest sampled value must stay: a few runs do
#: not see a variable's extremes, and a value that overflows is stored as inf.
FLOAT16_HEADROOM = 4.0

#: Smallest normal float16. Below it float16 loses relative precision bit by bit, down to
#: zero at 6e-8, so a field that lives there (a mixing ratio in kg/kg) is wiped out.
FLOAT16_TINY = float(np.finfo(np.float16).tiny)

#: Largest fraction of non-zero values allowed below :data:`FLOAT16_TINY`.
FLOAT16_MAX_SUBNORMAL_FRACTION = 1e-3

#: Parameters always stored at full precision, whatever their sampled range says.
#:
#: DWD packs every field at 16 bits per value, so float16, with 11 significant bits, is
#: never bit-exact; it is used where the loss is far below what the field means (a
#: temperature to an eighth of a kelvin, a wind to a centimetre per second). The fields
#: listed here are the ones where it is not, or where the sampled range cannot be trusted
#: to show it:
#:
#: * Moisture: specific humidity, the hydrometeors and their columns (``Q*``, ``TQ*``).
#:   Mixing ratios sit at 1e-3 to 1e-8 kg/kg, below float16's normal range, and are the
#:   inputs of every derived humidity and precipitation diagnostic.
#: * Geopotential and heights in metres (``FI``, ``HHL``, ``HSURF``, cloud base and top,
#:   freezing level, ceiling, subgrid orography). Large offsets, where float16's
#:   relative precision is metres to tens of metres, and level heights are differenced.
#: * Pressure (``P``, ``PS``, ``PMSL``): about 1e5 Pa, beyond float16's range entirely.
#: * Every ICON-ART dust, aerosol and deposition field (anything containing ``DUST``,
#:   ``AER_*``, ``TAOD*``, the ``ACC*`` accumulations): concentrations and fluxes many
#:   orders of magnitude apart, mostly far below float16's normal range.
#: * The cell coordinates ``CLAT``/``CLON`` (stored as float32 coordinates regardless).
FULL_PRECISION_PARAMS: frozenset[str] = frozenset(
    {
        # Geopotential and geometric heights.
        "FI",
        "HHL",
        "HSURF",
        "HZEROCL",
        "HBAS_CON",
        "HTOP_CON",
        "HTOP_DC",
        "CEILING",
        "SSO_STDH",
        "SNOWLMT",
        "HPBL",
        "MH",
        # Pressure.
        "P",
        "PS",
        "PMSL",
        # Coordinates.
        "CLAT",
        "CLON",
    }
)

#: Parameter name prefixes always stored at full precision; see :data:`FULL_PRECISION_PARAMS`.
FULL_PRECISION_PREFIXES: tuple[str, ...] = ("Q", "TQ", "AER_", "TAOD", "ACC")

#: Substrings that mark a parameter as always full precision.
FULL_PRECISION_SUBSTRINGS: tuple[str, ...] = ("DUST",)


def is_full_precision(param: str) -> bool:
    """Whether ``param`` is always stored as float32. Case-insensitive.

    Args:
        param: DWD parameter name.
    """
    name = param.upper()
    return (
        name in FULL_PRECISION_PARAMS
        or name.startswith(FULL_PRECISION_PREFIXES)
        or any(part in name for part in FULL_PRECISION_SUBSTRINGS)
    )


@dataclasses.dataclass
class PrecisionStats:
    """What sampled fields of one variable say about storing it as float16.

    Attributes:
        samples: Fields decoded.
        max_abs: Largest finite absolute value.
        nonzero: Finite non-zero values seen.
        below_normal: Of those, how many are smaller in magnitude than float16's smallest
            normal number.
        fp16_max_abs_error: Largest absolute error of a float16 round trip, over values
            that do not overflow.
        fp16_max_rel_error: Largest relative error of a float16 round trip, over non-zero
            values that do not overflow.
    """

    samples: int = 0
    max_abs: float = 0.0
    nonzero: int = 0
    below_normal: int = 0
    fp16_max_abs_error: float = 0.0
    fp16_max_rel_error: float = 0.0

    @property
    def subnormal_fraction(self) -> float:
        """Fraction of non-zero values below float16's normal range."""
        return self.below_normal / self.nonzero if self.nonzero else 0.0

    def add(self, values: np.ndarray) -> None:
        """Fold one decoded field into the statistics."""
        finite = np.asarray(values, dtype=np.float32)
        finite = finite[np.isfinite(finite)]
        self.samples += 1
        if not finite.size:
            return
        magnitude = np.abs(finite)
        self.max_abs = max(self.max_abs, float(magnitude.max()))
        nonzero = magnitude > 0
        self.nonzero += int(nonzero.sum())
        self.below_normal += int((nonzero & (magnitude < FLOAT16_TINY)).sum())
        with np.errstate(over="ignore"):
            half = finite.astype(np.float16)
        representable = np.isfinite(half)
        error = np.abs(half[representable].astype(np.float32) - finite[representable])
        if error.size:
            self.fp16_max_abs_error = max(self.fp16_max_abs_error, float(error.max()))
            rel = nonzero[representable]
            if rel.any():
                relative = error[rel] / magnitude[representable][rel]
                self.fp16_max_rel_error = max(self.fp16_max_rel_error, float(relative.max()))

    def merge(self, other: "PrecisionStats") -> "PrecisionStats":
        """Combine two sets of statistics."""
        return PrecisionStats(
            samples=self.samples + other.samples,
            max_abs=max(self.max_abs, other.max_abs),
            nonzero=self.nonzero + other.nonzero,
            below_normal=self.below_normal + other.below_normal,
            fp16_max_abs_error=max(self.fp16_max_abs_error, other.fp16_max_abs_error),
            fp16_max_rel_error=max(self.fp16_max_rel_error, other.fp16_max_rel_error),
        )

    def as_json(self) -> dict[str, Any]:
        """A compact summary for the schema JSON, to four significant figures."""

        def short(value: float) -> float:
            return float(f"{value:.4g}")

        return {
            "samples": self.samples,
            "max_abs": short(self.max_abs),
            "subnormal_fraction": short(self.subnormal_fraction),
            "fp16_max_abs_error": short(self.fp16_max_abs_error),
            "fp16_max_rel_error": short(self.fp16_max_rel_error),
        }


def classify_precision(param: str, stats: PrecisionStats | None) -> tuple[str, str]:
    """Decide whether a variable is stored as float16 or float32.

    float16 only when every one of these holds; float32 otherwise:

    1. ``param`` is not always full precision (:func:`is_full_precision`);
    2. it was sampled at all;
    3. its largest sampled magnitude is at most :data:`FLOAT16_MAX` /
       :data:`FLOAT16_HEADROOM`, so extremes the sample missed still fit;
    4. at most :data:`FLOAT16_MAX_SUBNORMAL_FRACTION` of its non-zero values are below
       float16's smallest normal number. Exact zeros are fine.

    Args:
        param: DWD parameter name.
        stats: Sampled statistics, or None if nothing could be sampled.

    Returns:
        ``(dtype, reason)``.
    """
    if is_full_precision(param):
        return FLOAT32, "full-precision list"
    if stats is None or stats.samples == 0:
        return FLOAT32, "not sampled"
    if stats.max_abs > FLOAT16_MAX / FLOAT16_HEADROOM:
        return FLOAT32, "range"
    if stats.subnormal_fraction > FLOAT16_MAX_SUBNORMAL_FRACTION:
        return FLOAT32, "underflow"
    return FLOAT16, "fits"


VariableKey = tuple[str, "int | None", "int | None"]


def _variable_key(param: str, level_type: int | None, wavelength: int | None) -> VariableKey:
    return (param, level_type, wavelength)


def precision_samples(
    schema: ICONSchema, spec: VariableSpec, runs: Sequence[pd.Timestamp]
) -> list[tuple[FieldKey, pd.Timestamp]]:
    """The fields sampled for one variable.

    Per run, the first, middle and last step of its cycle, at its top, middle and bottom
    level: at most nine fields a run.
    """

    def ends(items: Sequence) -> list:
        return list(dict.fromkeys([items[0], items[len(items) // 2], items[-1]]))

    samples = []
    for run in runs:
        steps = schema.steps_for_cycle(spec.param, spec.level_type, run.hour, spec.wavelength)
        if not steps:
            continue
        for level in ends(spec.levels) if spec.levels else [None]:
            for step in ends(steps):
                key = FieldKey(spec.param, spec.level_type, level, step, spec.wavelength)
                samples.append((key, run))
    return samples


#: Reads and decodes one field to float32, or returns None if it is missing.
FieldReader = Callable[[str], "np.ndarray | None"]


def _field_reader(source: Source) -> FieldReader:
    from planetary_datasets.common.download import read_bytes
    from planetary_datasets.common.grib import decode_grib_field

    options = storage_options(source)

    def read(uri: str) -> np.ndarray | None:
        data = read_bytes(uri, storage_options=options)
        return None if data is None else decode_grib_field(data, dtype=np.float32)

    return read


def measure_precision(
    model: str,
    runs: Sequence[pd.Timestamp],
    source: Source,
    *,
    schema: ICONSchema | None = None,
    reader: FieldReader | None = None,
    max_workers: int = MAX_LISTING_WORKERS,
) -> dict[VariableKey, PrecisionStats]:
    """Sample real fields of every variable and gather their float16 statistics.

    Args:
        model: Model name, a key of :data:`MODELS`.
        runs: Runs to sample, e.g. one 00Z and one 12Z.
        source: ``"archive"`` or ``"live"``.
        schema: Schema to sample; defaults to the committed one.
        reader: Field reader to use instead of the network, for tests.
        max_workers: Concurrent reads, at most :data:`MAX_LISTING_WORKERS`.

    Returns:
        Statistics per ``(param, level_type, wavelength)``; variables whose every sample
        was missing have ``samples == 0``.
    """
    schema = schema or load_schema(model)
    runs = [_normalize_init_time(r) for r in runs]
    reader = reader or _field_reader(source)
    jobs = [
        (_variable_key(spec.param, spec.level_type, spec.wavelength), key, run)
        for spec in schema.variables
        for key, run in precision_samples(schema, spec, runs)
    ]
    logger.info(f"sampling {len(jobs)} {model} fields from {source} runs {runs}")
    results: dict[VariableKey, PrecisionStats] = {
        _variable_key(s.param, s.level_type, s.wavelength): PrecisionStats()
        for s in schema.variables
    }

    def one(job: tuple[VariableKey, FieldKey, pd.Timestamp]) -> tuple[VariableKey, PrecisionStats]:
        variable, key, run = job
        stats = PrecisionStats()
        values = reader(field_uri(model, key, run, source))
        if values is not None:
            stats.add(values)
        return variable, stats

    workers = max(1, min(max_workers, MAX_LISTING_WORKERS))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for variable, stats in pool.map(one, jobs):
            results[variable] = results[variable].merge(stats)
    return results


def apply_precision(
    data: Mapping[str, Any], results: Mapping[VariableKey, PrecisionStats]
) -> dict[str, Any]:
    """Record each variable's dtype and its statistics in a schema's JSON data.

    Args:
        data: Schema JSON, as :func:`discover_schema` returns it.
        results: Output of :func:`measure_precision`.

    Returns:
        A copy of ``data`` whose variable entries carry ``dtype`` and ``precision``.
    """
    out = json.loads(json.dumps(data))
    for entry in out["variables"]:
        key = _variable_key(entry["param"], entry["level_type"], entry.get("wavelength"))
        stats = results.get(key)
        dtype, reason = classify_precision(entry["param"], stats)
        entry["dtype"] = dtype
        entry["precision"] = {**(stats or PrecisionStats()).as_json(), "reason": reason}
    return out


def carry_precision(new: Mapping[str, Any], old: Mapping[str, Any]) -> dict[str, Any]:
    """Copy ``dtype`` and ``precision`` from an older schema's matching variables.

    So that rediscovering a schema does not silently send every variable back to float32;
    variables new to the schema stay float32 until ``precision`` is run again.
    """
    previous = {
        _variable_key(e["param"], e["level_type"], e.get("wavelength")): e
        for e in old.get("variables", ())
    }
    out = json.loads(json.dumps(new))
    for entry in out["variables"]:
        match = previous.get(
            _variable_key(entry["param"], entry["level_type"], entry.get("wavelength"))
        )
        if match is not None:
            for field in ("dtype", "precision"):
                if field in match:
                    entry[field] = match[field]
    return out


# ---------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------


def dump_schema(schema: Mapping[str, Any]) -> str:
    """Serialise a schema with one variable or step set per line, so diffs stay readable.

    Args:
        schema: Output of :func:`discover_schema`.

    Returns:
        JSON text ending in a newline.
    """
    parts = []
    for key, value in schema.items():
        if key in ("variables", "excluded") and value:
            body = ",\n  ".join(json.dumps(item) for item in value)
            text = f"[\n  {body}\n ]"
        elif key == "step_sets" and value:
            body = ",\n  ".join(f"{json.dumps(k)}: {json.dumps(v)}" for k, v in value.items())
            text = f"{{\n  {body}\n }}"
        else:
            text = json.dumps(value)
        parts.append(f" {json.dumps(key)}: {text}")
    return "{\n" + ",\n".join(parts) + "\n}\n"


def default_reference_runs(
    runs: Sequence[pd.Timestamp], now: pd.Timestamp, min_age: pd.Timedelta
) -> list[pd.Timestamp]:
    """Pick the newest run of each cycle hour at least ``min_age`` old.

    Args:
        runs: Available runs.
        now: Current time, naive UTC.
        min_age: Minimum age, so the runs are fully published or mirrored.

    Raises:
        ValueError: If no old enough run exists for one of the hours.
    """
    chosen = []
    for hour in CYCLE_HOURS:
        candidates = [r for r in runs if r.hour == hour and r <= now - min_age]
        if not candidates:
            raise ValueError(f"no {hour:02d}Z run older than {min_age}")
        chosen.append(max(candidates))
    return chosen


def _summarise_runs(model: str, source: Source) -> str:
    runs = list_runs(model, source)
    if not runs:
        return f"{model} {source}: no runs"
    return f"{model} {source}: {len(runs)} runs, {runs[0]} -> {runs[-1]}"


def _precision_command(
    model: str, runs: Sequence[pd.Timestamp], source: Source, output: pathlib.Path, workers: int
) -> int:
    """Measure, classify and record every variable's storage dtype in ``output``."""
    data = json.loads(output.read_text(encoding="utf-8"))
    schema = ICONSchema(data)
    results = measure_precision(model, runs, source, schema=schema, max_workers=workers)
    updated = apply_precision(data, results)
    ICONSchema(updated)
    output.write_text(dump_schema(updated), encoding="utf-8")
    by_dtype: dict[str, list[str]] = {FLOAT16: [], FLOAT32: []}
    for entry in updated["variables"]:
        name = entry["param"] + (f"@{entry['level_type']}" if entry["level_type"] else "")
        if entry.get("wavelength"):
            name += f"@{entry['wavelength']}nm"
        by_dtype[entry["dtype"]].append(f"{name}({entry['precision']['reason']})")
    for dtype, names in by_dtype.items():
        logger.info(f"{model} {dtype} ({len(names)}): {', '.join(names)}")
    logger.info(f"wrote {output}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point. Returns the exit code."""
    parser = argparse.ArgumentParser(prog="python -m planetary_datasets.providers.icon_v1_catalog")
    sub = parser.add_subparsers(dest="command", required=True)

    schema_cmd = sub.add_parser("schema", help="discover a model's schema and write its JSON")
    schema_cmd.add_argument("model", choices=sorted(MODELS))
    schema_cmd.add_argument("--runs", nargs="+", help="reference runs, e.g. 2026-10-11T00:00")
    schema_cmd.add_argument(
        "--source",
        choices=("archive", "live"),
        default="live",
        help="where to discover from; the archive mirror often lacks parts of runs",
    )
    schema_cmd.add_argument("--output", type=pathlib.Path, help="defaults to the package JSON")
    schema_cmd.add_argument(
        "--min-age-hours",
        type=float,
        default=None,
        help="age of automatically chosen runs (default 36 archive, 5 live)",
    )
    schema_cmd.add_argument("--no-grid-check", action="store_true")
    schema_cmd.add_argument(
        "--allow-gaps",
        action="store_true",
        help="write the schema even if a reference run looks incomplete",
    )

    precision_cmd = sub.add_parser(
        "precision", help="sample real fields and record each variable's storage dtype"
    )
    precision_cmd.add_argument("model", choices=sorted(MODELS))
    precision_cmd.add_argument("--source", choices=("archive", "live"), default="live")
    precision_cmd.add_argument(
        "--runs", nargs="+", help="runs to sample; defaults to the newest 00Z and 12Z"
    )
    precision_cmd.add_argument("--min-age-hours", type=float, default=None)
    precision_cmd.add_argument("--output", type=pathlib.Path, help="defaults to the package JSON")
    precision_cmd.add_argument("--max-workers", type=int, default=MAX_LISTING_WORKERS)

    runs_cmd = sub.add_parser("runs", help="summarise the runs each source holds")
    runs_cmd.add_argument("models", nargs="*", choices=sorted(MODELS))
    runs_cmd.add_argument("--source", choices=("archive", "live"), action="append")

    args = parser.parse_args(argv)
    if args.command == "runs":
        for model in args.models or sorted(MODELS):
            for source in args.source or ("archive", "live"):
                logger.info(_summarise_runs(model, source))
        return 0

    if args.runs:
        runs = [_normalize_init_time(r) for r in args.runs]
    else:
        hours = args.min_age_hours
        if hours is None:
            hours = 36.0 if args.source == "archive" else 5.0
        now = pd.Timestamp.now("UTC").tz_localize(None)
        runs = default_reference_runs(
            list_runs(args.model, args.source), now, pd.Timedelta(hours, "h")
        )
    output = args.output or schema_path(args.model)
    if args.command == "precision":
        if not args.runs:
            runs = [r for r in runs if r.hour in (0, 12)]
        return _precision_command(args.model, runs, args.source, output, args.max_workers)

    logger.info(f"discovering {args.model} from {args.source} runs {[str(r) for r in runs]}")
    schema = discover_schema(
        args.model,
        runs,
        args.source,
        check_grid=not args.no_grid_check,
        strict=not args.allow_gaps,
    )
    if output.exists():
        schema = carry_precision(schema, json.loads(output.read_text(encoding="utf-8")))
    ICONSchema(schema)  # validate before writing
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(dump_schema(schema), encoding="utf-8")
    logger.info(
        f"wrote {output}: {len(schema['variables'])} variables, "
        f"{len(schema['excluded'])} exclusions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
