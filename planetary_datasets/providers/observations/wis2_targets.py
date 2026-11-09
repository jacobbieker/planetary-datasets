"""Archive targets: the WIS2 store, built from the objects as each archive window streams.

:mod:`~planetary_datasets.providers.observations.wis2_archive` streams every object of a
12-hour window into a tar. Each registered target sees those objects too and turns the
ones it wants into groups of one Icechunk store, :data:`STORE_PREFIX`
(``bkr/obs/wis2.icechunk``). Every target writes groups of its own; nothing is stored at
the root but the store's description (its attributes)::

    aircraft                      aircraft reports (AMDAR)            wis2_aircraft
    land/reports                  land station reports (BUFR)         wis2_land
    land/hydrometric              river level and discharge           wis2_land_hydrometric
    land/radar/<node>_<quantity>  weather-radar polar volumes         wis2_land_radar
    land/synop_tac                land SYNOP text reports             wis2_tac
    ocean/surface                 buoys, ships, wave buoys            wis2_ocean
    ocean/profiles                Argo and buoy profiles (ragged)     wis2_ocean
    ocean/ship_tac                SHIP text reports                   wis2_tac
    upper_air/soundings           radiosondes, dropsondes (ragged)    wis2_upper_air
    upper_air/wind_profiler       wind profiles (ragged)              wis2_wind_profiler
    satellite/<instrument>        sounder radiances (atms, iasi, ...) wis2_satellite
    satellite/<product>           other satellite products            wis2_satellite_products

The parent groups (``land``, ``land/radar``, ``ocean``, ``upper_air``, ``satellite``) hold
no arrays; ``land/radar`` keeps the radar ledgers. Each group keeps its own ledger of the
windows written (``partitions_written``): that ledger, not the archive's per-window target
record, says what the store holds. Every observation group carries per-observation quality
flags (:mod:`~planetary_datasets.providers.observations.wis2_qc`). Open the whole store
with ``xr.open_datatree(store, engine="zarr", consolidated=False)``, one group with
``xr.open_zarr(store, group="land/reports", consolidated=False)``.

Each target:

* :meth:`ArchiveTarget.wants` is called for every object (keep it cheap: a key match);
* :meth:`ArchiveTarget.feed` gets each wanted object's bytes as it streams past; it must
  not block the tar for long and never raises;
* :meth:`ArchiveTarget.finish` is called once the tar is complete, to decode what is left,
  deduplicate and append to the store. It returns a summary. A target that fails is
  logged by the archiver and never fails the tar.

The same targets run over an already archived tar (``wis2_archive --from-tar KEY``).

Writing a target: a module ``planetary_datasets/providers/observations/wis2_<name>.py``
that subclasses :class:`QueuedTarget` (decoding happens on a bounded background thread)
and registers a factory::

    @register("aircraft")
    class AircraftTarget(QueuedTarget):
        store_prefix = STORE_PREFIX   # every target writes groups of the one store

        def wants(self, key, size):
            return AIRCRAFT_KEY.match(key) is not None

        def decode(self, key, body, arrived):      # on the worker thread
            self.rows += ...

        def write(self, window_start, window_end, partition):
            ...append to the store...
            return {"rows": n}

Modules named ``wis2_*`` in this package are imported by :func:`load_targets`, so a new
target needs no edit elsewhere. :func:`rows_from_bufr` is a generic ecCodes BUFR decoder
targets can share.
"""

from __future__ import annotations

import abc
import datetime as dt
import importlib
import pkgutil
import queue
import threading
from typing import Any, Callable, Iterable, Iterator

import numpy as np
from loguru import logger

from planetary_datasets.config import Config

#: Modules of this package that are not targets.
NOT_TARGETS = {"wis2_archive", "wis2_targets", "wis2_qc"}
#: Objects waiting to be decoded per target; a full queue makes the archive wait.
QUEUE_SIZE = 20_000

#: name -> factory taking the configuration.
TARGETS: dict[str, Callable[[Config], "ArchiveTarget"]] = {}

#: The one store every WIS2 archive target writes, each into groups of its own.
STORE_PREFIX = "bkr/obs/wis2.icechunk"
#: Groups that only hold other groups, created (in this order) before any is written, so
#: writers of sibling groups never race to create their parent.
PARENT_GROUPS = ("land", "land/radar", "ocean", "upper_air", "satellite")
#: The store's own attributes: what it is and where everything is.
ROOT_ATTRS = {
    "title": "Observations from the WMO WIS2 Global Cache",
    "source": "WMO WIS2 Global Cache (s3://wis2globalcache), as archived in bkr/obs/wis2",
    "layout": (
        "one group per kind of observation: aircraft; land/reports, land/hydrometric, "
        "land/radar/<node>_<quantity>, land/synop_tac; ocean/surface, ocean/profiles, "
        "ocean/ship_tac; upper_air/soundings, upper_air/wind_profiler; "
        "satellite/<instrument or product>. Open one with "
        "xr.open_zarr(store, group='land/reports', consolidated=False), all with "
        "xr.open_datatree(store, engine='zarr', consolidated=False)"
    ),
    "ledger": "each group lists the archive windows it holds in partitions_written",
    "quality_control": (
        "values are as reported, never altered or masked; observation groups carry "
        "quality_flag (level_quality_flag on the levels of profiles) and <variable>_quality "
        "flags, described by their flag_masks and flag_meanings"
    ),
    "Conventions": "CF-1.11",
}


def _layout_missing(store) -> tuple[bool, list[str]]:
    """Whether the root's attributes differ from :data:`ROOT_ATTRS`, and parents absent."""
    import zarr

    try:
        root = zarr.open_group(store, mode="r")
    except (FileNotFoundError, KeyError, zarr.errors.GroupNotFoundError):
        return True, list(PARENT_GROUPS)
    attrs_differ = any(root.attrs.get(k) != v for k, v in ROOT_ATTRS.items())
    absent = []
    for path in PARENT_GROUPS:
        try:
            zarr.open_group(store, path=path, mode="r")
        except (FileNotFoundError, KeyError, zarr.errors.GroupNotFoundError):
            absent.append(path)
    return attrs_differ, absent


def ensure_layout(repo) -> None:
    """Create the parent groups and the root's attributes, if not there yet.

    Cheap once the store is laid out: one read-only look, no commit. The parents are made
    before any group is written, so writers of sibling groups (the satellite groups, four
    at a time) never both create ``satellite`` and conflict.
    """
    import icechunk
    import zarr

    from planetary_datasets.common.multidim import COMMIT_ATTEMPTS

    if _layout_missing(repo.readonly_session("main").store) == (False, []):
        return
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        session = repo.writable_session("main")
        attrs_differ, absent = _layout_missing(session.store)
        if not attrs_differ and not absent:
            return
        root = zarr.open_group(session.store, mode="a")
        if attrs_differ:
            root.attrs.update(ROOT_ATTRS)
        for path in absent:
            zarr.open_group(session.store, path=path, mode="a")
        try:
            session.commit("wis2: lay out the store", rebase_with=icechunk.ConflictDetector())
            return
        except (icechunk.ConflictError, icechunk.RebaseFailedError):
            if attempt == COMMIT_ATTEMPTS:
                raise


def store_repo(config: Config):
    """The WIS2 store's repository, laid out (:func:`ensure_layout`) and ready to write."""
    repo = config.icechunk_repo(STORE_PREFIX)
    ensure_layout(repo)
    return repo


def register(name: str):
    """Class decorator registering an :class:`ArchiveTarget` under ``name``."""

    def decorate(cls):
        cls.name = name
        cls.registered = True
        TARGETS[name] = cls
        return cls

    return decorate


def load_targets() -> dict[str, Callable[[Config], "ArchiveTarget"]]:
    """Import every ``wis2_*`` module of this package, so their targets register."""
    from planetary_datasets.providers import observations

    for module in pkgutil.iter_modules(observations.__path__):
        if module.name.startswith("wis2_") and module.name not in NOT_TARGETS:
            try:
                imported = importlib.import_module(f"{observations.__name__}.{module.name}")
            except Exception as exc:  # noqa: BLE001 - one broken target must not stop the rest
                logger.exception(f"wis2 targets: {module.name} could not be imported: {exc}")
                continue
            # A module imported before (into another registry) still contributes its classes.
            for value in vars(imported).values():
                if isinstance(value, type) and value.__dict__.get("registered"):
                    TARGETS.setdefault(value.name, value)
    return TARGETS


def build_targets(config: Config, names: Iterable[str] | None = None) -> list["ArchiveTarget"]:
    """Instances of the targets named (all registered when None)."""
    load_targets()
    names = list(TARGETS) if names is None else list(names)
    unknown = [n for n in names if n not in TARGETS]
    if unknown:
        raise ValueError(f"unknown WIS2 archive target(s) {unknown}; known: {sorted(TARGETS)}")
    return [TARGETS[n](config) for n in names]


class ArchiveTarget(abc.ABC):
    """One store built from the objects of each archive window."""

    #: Registered name, set by :func:`register`.
    name: str = ""
    registered: bool = False
    #: The store this target writes: every target writes groups of :data:`STORE_PREFIX`.
    store_prefix: str = STORE_PREFIX

    def __init__(self, config: Config):
        """A target writing with ``config``."""
        self.config = config

    def prepare(self, partition: str) -> None:
        """Called with the window's ledger entry before its objects stream past.

        A target whose store already holds some of the window (re-running it over an
        archived tar) can then decline those objects in :meth:`wants`. Nothing by default.
        """

    @abc.abstractmethod
    def wants(self, key: str, size: int) -> bool:
        """Whether this target needs the object at ``key``. Called for every object."""

    @abc.abstractmethod
    def feed(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Take one wanted object. Must not raise; should not block for long."""

    @abc.abstractmethod
    def finish(
        self, window_start: dt.datetime | None, window_end: dt.datetime | None, partition: str
    ) -> dict[str, Any]:
        """Write what this window yielded as ledger entry ``partition``; a summary back."""


class QueuedTarget(ArchiveTarget):
    """A target whose objects are decoded on a background thread fed by a bounded queue.

    Subclasses implement :meth:`wants`, :meth:`decode` (on the worker thread; an exception
    there is logged and counted, the object skipped) and :meth:`write` (after the window).
    """

    def __init__(self, config: Config):
        """A target writing with ``config``; its worker starts on the first object."""
        super().__init__(config)
        self.decoded = 0
        self.failed = 0
        self._queue: queue.Queue = queue.Queue(QUEUE_SIZE)
        self._thread: threading.Thread | None = None

    @abc.abstractmethod
    def decode(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Decode one object, keeping its rows on ``self``."""

    @abc.abstractmethod
    def write(
        self, window_start: dt.datetime | None, window_end: dt.datetime | None, partition: str
    ) -> dict[str, Any]:
        """Append what was decoded to the store."""

    def feed(self, key: str, body: bytes, arrived: dt.datetime | None) -> None:
        """Queue an object for the worker thread (waits while the queue is full)."""
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._work, name=f"wis2-{self.name}", daemon=True
            )
            self._thread.start()
        self._queue.put((key, body, arrived))

    def _work(self) -> None:
        while (item := self._queue.get()) is not None:
            try:
                self.decode(*item)
                self.decoded += 1
            except Exception as exc:  # noqa: BLE001 - one bad object must not stop a window
                self.failed += 1
                if self.failed <= 20:
                    logger.warning(f"wis2 {self.name}: {item[0]}: {exc}")

    def finish(
        self, window_start: dt.datetime | None, window_end: dt.datetime | None, partition: str
    ) -> dict[str, Any]:
        """Let the worker finish the queue, then write; a summary back."""
        if self._thread is not None:
            self._queue.put(None)
            self._thread.join()
            self._thread = None
        summary = self.write(window_start, window_end, partition)
        return {"objects": self.decoded, "undecodable_objects": self.failed, **(summary or {})}


def run_targets(
    targets: list[ArchiveTarget],
    window_start: dt.datetime | None,
    window_end: dt.datetime | None,
    partition: str,
) -> dict[str, dict]:
    """Finish every target, logging (never raising) a failure."""
    summaries = {}
    for target in targets:
        try:
            summaries[target.name] = target.finish(window_start, window_end, partition)
            logger.info(f"wis2 {target.name} {partition}: {summaries[target.name]}")
        except Exception as exc:  # noqa: BLE001 - a target never fails the archive
            logger.exception(f"wis2 {target.name} {partition}: failed: {exc}")
            summaries[target.name] = {"error": f"{type(exc).__name__}: {exc}"}
    return summaries


class Router:
    """Hands each archived object to the targets that want it."""

    def __init__(self, targets: list[ArchiveTarget]):
        """A router over ``targets``."""
        self.targets = targets

    def __call__(self, key: str, size: int, body: bytes, arrived: dt.datetime | None) -> None:
        """Hand one object to every target that wants it; never raises."""
        data = None
        for target in self.targets:
            try:
                if target.wants(key, size):
                    # The archive fetches into obstore's Bytes, which has no bytes methods;
                    # targets get plain bytes, copied once and only for objects they want.
                    if data is None:
                        data = body if isinstance(body, bytes) else bytes(body)
                    target.feed(key, data, arrived)
            except Exception as exc:  # noqa: BLE001 - a target never fails the archive
                logger.warning(f"wis2 {target.name}: {key}: {exc}")


# ----------------------------------------------------------------------------- BUFR

#: ecCodes' markers for a missing value.
MISSING_LONG = 2147483647
MISSING_DOUBLE = -1e100


def split_bufr(data: bytes) -> Iterator[bytes]:
    """The BUFR messages in a bulletin or file, by their section 0 lengths.

    Text before, between and after messages (GTS headers, padding) is skipped.
    """
    start = data.find(b"BUFR")
    while start != -1 and start + 8 <= len(data):
        length = int.from_bytes(data[start + 4 : start + 7], "big")
        if length < 8 or start + length > len(data):
            return
        yield data[start : start + length]
        start = data.find(b"BUFR", start + length)


def clean(value):
    """An ecCodes value with missing markers as None and strings stripped."""
    if isinstance(value, np.ndarray):
        return [clean(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (float, np.floating)):
        return None if not np.isfinite(value) or value <= MISSING_DOUBLE / 10 else float(value)
    if isinstance(value, (int, np.integer)):
        return None if int(value) == MISSING_LONG else int(value)
    return value


def _get(handle, key: str):
    import eccodes

    try:
        if eccodes.codes_get_size(handle, key) > 1:
            return eccodes.codes_get_array(handle, key)
        return eccodes.codes_get(handle, key)
    except eccodes.KeyValueNotFoundError:
        return None


def bufr_header(message: bytes) -> dict[str, Any]:
    """Section 1 of a message: category, sub-categories, centre, template, subsets."""
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        return {
            key: clean(_get(handle, key))
            for key in (
                "dataCategory",
                "internationalDataSubCategory",
                "dataSubCategory",
                "bufrHeaderCentre",
                "numberOfSubsets",
                "compressedData",
                "unexpandedDescriptors",
            )
        }
    finally:
        eccodes.codes_release(handle)


def rows_from_bufr(
    message: bytes,
    keys: Iterable[str],
    repeated: Iterable[str] = (),
    subset_start: str | None = None,
) -> list[dict[str, Any]]:
    """One dict per subset (report) of a BUFR message, keyed by ecCodes element name.

    Args:
        message: One BUFR message (see :func:`split_bufr`).
        keys: Element names wanted (e.g. ``airTemperature``). A subset takes the first
            occurrence of each; one it lacks is absent from its dict.
        repeated: Of ``keys``, those to collect every occurrence of within a subset, as a
            list in message order (levels of a profile, say).
        subset_start: The element each subset begins with. Uncompressed messages number
            elements across subsets, so subsets are cut where this element appears; by
            default the first wanted-or-not data element of the message, which is the
            first element of the template.

    Missing values are None. Raises whatever ecCodes raises for a malformed message.
    """
    import eccodes

    keys, repeated = set(keys), set(repeated)
    handle = eccodes.codes_new_from_message(message)
    try:
        eccodes.codes_set(handle, "unpack", 1)
        subsets = eccodes.codes_get(handle, "numberOfSubsets")
        if eccodes.codes_get(handle, "compressedData"):
            return _compressed(handle, subsets, keys, repeated)
        rows: list[dict[str, Any]] = []
        iterator = eccodes.codes_bufr_keys_iterator_new(handle)
        try:
            while eccodes.codes_bufr_keys_iterator_next(iterator):
                name = eccodes.codes_bufr_keys_iterator_get_name(iterator)
                if not name.startswith("#") or "->" in name:
                    continue
                bare = name.split("#", 2)[2]
                if subset_start is None:
                    subset_start = bare
                if bare == subset_start:
                    rows.append({})
                if not rows or bare not in keys:
                    continue
                if bare in repeated:
                    rows[-1].setdefault(bare, []).append(clean(_get(handle, name)))
                elif bare not in rows[-1]:
                    rows[-1][bare] = clean(_get(handle, name))
        finally:
            eccodes.codes_bufr_keys_iterator_delete(iterator)
        return rows
    finally:
        eccodes.codes_release(handle)


def _compressed(handle, subsets: int, keys: set[str], repeated: set[str]) -> list[dict]:
    """Compressed messages: each occurrence's values come for all subsets at once."""
    rows: list[dict] = [{} for _ in range(subsets)]

    def per_subset(name: str) -> list | None:
        values = _get(handle, name)
        if values is None:
            return None
        values = clean(np.atleast_1d(values)) if not isinstance(values, str) else [clean(values)]
        return values * subsets if len(values) == 1 else values

    for key in keys:
        if key in repeated:
            occurrence = 1
            while (values := per_subset(f"#{occurrence}#{key}")) is not None:
                for row, value in zip(rows, values):
                    row.setdefault(key, []).append(value)
                occurrence += 1
        else:
            values = per_subset(f"#1#{key}")
            if values is not None:
                for row, value in zip(rows, values):
                    row[key] = value
    return rows
