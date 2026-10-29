"""Archive targets: stores built from the WIS2 objects as each archive window streams.

:mod:`~planetary_datasets.providers.observations.wis2_archive` streams every object of a
12-hour window into a tar. Each registered target sees those objects too and turns the
ones it wants into an Icechunk store (aircraft reports, ocean, upper-air, land
observations, ...):

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
        store_prefix = "bkr/obs/wis2_aircraft.icechunk"

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
NOT_TARGETS = {"wis2_archive", "wis2_targets"}
#: Objects waiting to be decoded per target; a full queue makes the archive wait.
QUEUE_SIZE = 20_000

#: name -> factory taking the configuration.
TARGETS: dict[str, Callable[[Config], "ArchiveTarget"]] = {}


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
    #: The store this target writes, e.g. ``bkr/obs/wis2_aircraft.icechunk``.
    store_prefix: str = ""

    def __init__(self, config: Config):
        self.config = config

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
        self.targets = targets

    def __call__(self, key: str, size: int, body: bytes, arrived: dt.datetime | None) -> None:
        for target in self.targets:
            try:
                if target.wants(key, size):
                    target.feed(key, body, arrived)
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
