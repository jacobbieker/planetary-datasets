"""Satellite products other than sounder radiances, for ``bkr/obs/wis2_satellite.icechunk``.

The satellite archive target (:mod:`.wis2_satellite`) writes these beside its radiance
groups, one group per product family, from both the WIS2 ``space-based-observations``
topics and the GTS bulletins the DWD and JMA relays republish:

=============  =====================================  =============================  =======
Group          Product                                WIS2 instrument topics         GTS
=============  =====================================  =============================  =======
gnss_ro        GNSS radio occultation (310026):       ``ro`` (Metop-SG), ``gras``,   ``IUT``
               bending angle, refractivity, and       ``gnos-2`` (FY-3),
               where sent, temperature/pressure       ``stratos`` (Spire)
amv            atmospheric motion vectors (310077)    ``seviri``, ``fci``,           ``IEV``
                                                      ``avhrr-3``, ``viirs``,        ``INV``
                                                      ``imager`` (INSAT)             ``IRV``
                                                                                     ``IUV``
                                                                                     ``IXV``
altimetry      radar altimetry (340010/11/17/19):     ``sral``, ``poseidon-3b``,     ``ISZ``
               ranges, wave height, backscatter,      ``poseidon-3c``,
               corrections                            ``poseidon-4``, ``altika``
scatterometer  scatterometer surface winds (312xxx)   --                             ``ISX``
sar_ocean      Sentinel-1 SAR ocean winds, swell      ``sar-c``                      --
               partitions and wave spectra
ami_csr        GK-2A AMI clear-sky radiances (KMA)    ``ami``                        --
olci_wv        Sentinel-3 OLCI total column water     ``olci``                       --
               vapour
=============  =====================================  =============================  =======

A message is only taken into a group when its own header agrees (BUFR data category and
template), whatever the object is called.

Layout. One row per BUFR subset (dimension ``observation``: one wind vector, one
altimeter measurement, one occultation). Every element of the message is kept, named in
snake case after its ecCodes key: an element that occurs once in the subset is a column
on ``observation``; one that occurs more often is a 2-D variable on
``(observation, <name>_n)``, its occurrences in message order (an occultation's levels,
an altimeter's bands, a wind's quality indicators). Elements replicated together share
their index, so ``bending_angle[:, i]`` goes with ``impact_parameter``'s and
``mean_frequency``'s entries in the same loop. Delayed replication can shift what an
index means between sources of one group, so read a 2-D variable alongside
``bufr_template``. A subset shorter than the widest in the store is padded with fill.

Values are the BUFR values in the units BUFR gives (SI): floats are float32 when every
value, rounded to its BUFR decimal places, reads back unchanged, else float64; integers
take the narrowest type holding them with the minimum as fill; code and flag tables stay
integers. Added per row: ``time`` (the first date and time in the subset, milliseconds),
``bufr_template``, ``originating_centre``, ``source_key``, ``source_centre``,
``arrival_time``, ``first_arrival_time`` and ``observation_identity``.

The same observation reaches the cache several times (the originating centre's topic and
each GTS relay). Its identity is a hash of every value decoded, so identical copies
collapse to one row while anything that differs is kept; ``first_arrival_time`` is the
earliest arrival of the copies collapsed, ``arrival_time`` the arrival of the copy kept.
"""

from __future__ import annotations

import dataclasses
import re
from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

#: ecCodes' missing value for integer elements.
MISSING_LONG = 2147483647
#: Rows whose provenance and derived columns are not part of an observation's identity.
DERIVED = (
    "time",
    "bufr_template",
    "originating_centre",
    "source_key",
    "source_centre",
    "arrival_time",
    "first_arrival_time",
    "observation_identity",
)
TOPIC = re.compile(
    r"^data/(?P<centre>[^/]+)/data/core/weather/(?:experimental/)?space-based-observations/"
    r"(?P<platform>[^/]+)/(?P<instrument>[^/]+)/(?P<name>[^/]+)$"
)
GTS = re.compile(r"-gts-to-wis2/data/core/(?P<t1>[A-Z])/(?P<t2>[A-Z])/(?P<a1>[A-Z])/")


@dataclasses.dataclass(frozen=True)
class Product:
    """One group: where its objects come from and which messages it takes."""

    group: str
    title: str
    topics: tuple[str, ...] = ()
    #: GTS T1T2A1 of the relayed bulletins (``ISX``: BUFR, surface, satellite).
    gts: tuple[str, ...] = ()
    #: BUFR data categories taken; empty takes any (a topic that only carries this).
    categories: tuple[int, ...] = ()
    #: Top-level template descriptors, or their leading digits (``"340"``); empty: any.
    templates: tuple[str, ...] = ()

    def takes(self, category: int, descriptors: tuple[int, ...]) -> bool:
        if self.categories and category not in self.categories:
            return False
        if not self.templates:
            return True
        return any(str(d).zfill(6).startswith(t) for d in descriptors for t in self.templates)


PRODUCTS = {
    p.group: p
    for p in (
        Product(
            "gnss_ro",
            "GNSS radio occultation profiles",
            ("ro", "gras", "gnos-2", "stratos"),
            ("IUT",),
            (3,),
            ("310026",),
        ),
        Product(
            "amv",
            "atmospheric motion vectors",
            ("seviri", "fci", "avhrr-3", "viirs", "imager"),
            ("IEV", "INV", "IRV", "IUV", "IXV"),
            (5,),
            ("310077", "310014"),
        ),
        Product(
            "altimetry",
            "radar altimetry",
            ("sral", "poseidon-3b", "poseidon-3c", "poseidon-4", "altika"),
            ("ISZ",),
            (12,),
            ("340",),
        ),
        Product("scatterometer", "scatterometer surface winds", (), ("ISX",), (12,), ("312",)),
        Product("sar_ocean", "Sentinel-1 SAR ocean winds and waves", ("sar-c",)),
        Product("ami_csr", "GK-2A AMI clear-sky radiances", ("ami",)),
        Product("olci_wv", "Sentinel-3 OLCI total column water vapour", ("olci",)),
    )
}
BY_TOPIC = {topic: p for p in PRODUCTS.values() for topic in p.topics}
BY_GTS = {heading: p for p in PRODUCTS.values() for heading in p.gts}


def product_for(key: str) -> Product | None:
    """The product group the object at ``key`` may hold, or None."""
    match = TOPIC.match(key)
    if match is not None:
        return BY_TOPIC.get(match["instrument"])
    match = GTS.search(key)
    if match is not None:
        return BY_GTS.get(match["t1"] + match["t2"] + match["a1"])
    return None


def snake(name: str) -> str:
    """An ecCodes key in snake case: ``bendingAngle`` -> ``bending_angle``."""
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return name.lower()


# ----------------------------------------------------------------------------- decoding


def _occurrences(handle) -> dict[str, int]:
    """Each data element's occurrences per subset, in template order."""
    import eccodes

    counts: dict[str, int] = {}
    iterator = eccodes.codes_bufr_keys_iterator_new(handle)
    try:
        while eccodes.codes_bufr_keys_iterator_next(iterator):
            name = eccodes.codes_bufr_keys_iterator_get_name(iterator)
            if name.startswith("#") and "->" not in name:
                bare = name.split("#", 2)[2]
                counts[bare] = counts.get(bare, 0) + 1
    finally:
        eccodes.codes_bufr_keys_iterator_delete(iterator)
    return counts


def _values(handle, name: str, count: int, subsets: int) -> np.ndarray:
    """``(subsets, count)`` values of every occurrence of ``name``."""
    import eccodes

    if eccodes.codes_get_native_type(handle, f"#1#{name}") is str:
        columns = []
        for k in range(1, count + 1):
            values = np.atleast_1d(
                np.asarray(eccodes.codes_get_string_array(handle, f"#{k}#{name}"))
            )
            columns.append(np.repeat(values, subsets) if values.size == 1 else values)
        out = np.stack(columns, 1).astype(object)
        return np.vectorize(lambda v: str(v).strip(), otypes=[object])(out)
    whole = np.atleast_1d(eccodes.codes_get_array(handle, name))
    if whole.size == count * subsets:
        out = whole.reshape(count, subsets).T
    elif whole.size == count and subsets == 1:
        out = whole.reshape(1, count)
    else:
        columns = []
        for k in range(1, count + 1):
            values = np.atleast_1d(eccodes.codes_get_array(handle, f"#{k}#{name}"))
            columns.append(np.repeat(values, subsets) if values.size == 1 else values)
        out = np.stack(columns, 1)
    if out.dtype.kind == "f":
        return np.where(out <= -1e99, np.nan, out)
    return out.astype("int64")


def _decimals(handle, name: str, count: int) -> int:
    import eccodes

    places = 0
    for k in range(1, count + 1):
        try:
            places = max(places, int(eccodes.codes_get(handle, f"#{k}#{name}->scale")))
        except Exception:  # noqa: BLE001 - not every element has a scale key
            return 6
    return places


def _times(columns: dict[str, np.ndarray], rows: int) -> np.ndarray:
    """The subset's first date and time, to the millisecond (NaT where incomplete)."""

    def first(name: str) -> np.ndarray | None:
        values = columns.get(name)
        if values is None:
            return None
        values = values if values.ndim == 1 else values[:, 0]
        return np.asarray(values, dtype="float64")

    parts = {k: first(k) for k in ("year", "month", "day", "hour", "minute")}
    if any(v is None for v in parts.values()):
        return np.full(rows, np.datetime64("NaT", "ms"))
    seconds = first("second")
    if seconds is None:
        micro = first("seconds_within_a_minute_microsecond")
        seconds = micro / 1e6 if micro is not None else np.zeros(rows)
    valid = np.all([np.isfinite(v) & (v != MISSING_LONG) for v in parts.values()], axis=0)
    valid &= np.isfinite(seconds) & (seconds != MISSING_LONG)
    whole = np.floor(np.where(valid, seconds, 0)).astype("int64")
    stamps = pd.to_datetime(
        {
            k: np.where(valid, v, 1 if k in ("month", "day") else 0).astype("int64")
            for k, v in parts.items()
            if k != "year"
        }
        | {"year": np.where(valid, parts["year"], 1970).astype("int64"), "second": whole},
        errors="coerce",
    ).to_numpy("datetime64[ms]")
    stamps = stamps + np.round((np.where(valid, seconds, 0) - whole) * 1000).astype(
        "timedelta64[ms]"
    )
    return np.where(valid, stamps, np.datetime64("NaT", "ms"))


def decode_message(message: bytes, product: Product) -> dict[str, np.ndarray] | None:
    """One BUFR message as columns, None when it is not this product's.

    Returns 1-D arrays for elements that occur once per subset and ``(subsets, n)``
    arrays for repeated ones, keyed by snake-case name, with ``<name>.decimals``.
    """
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        category = int(eccodes.codes_get(handle, "dataCategory"))
        descriptors = tuple(
            int(d) for d in eccodes.codes_get_array(handle, "unexpandedDescriptors")
        )
        if not product.takes(category, descriptors):
            return None
        subsets = int(eccodes.codes_get(handle, "numberOfSubsets"))
        if subsets > 1 and not eccodes.codes_get(handle, "compressedData"):
            return _uncompressed(handle, subsets, product)
        eccodes.codes_set(handle, "unpack", 1)
        out: dict[str, np.ndarray] = {}
        for name, count in _occurrences(handle).items():
            if count == 0:
                continue
            values = _values(handle, name, count, subsets)
            column = snake(name)
            if column in DERIVED:
                column = f"bufr_{column}"
            out[column] = values[:, 0] if count == 1 else values
            if values.dtype.kind == "f":
                out[f"{column}.decimals"] = np.array(_decimals(handle, name, count))
        out["time"] = _times(out, subsets)
        out["bufr_template"] = np.full(
            subsets, " ".join(str(d).zfill(6) for d in descriptors), dtype=object
        )
        out["originating_centre"] = np.full(
            subsets, int(eccodes.codes_get(handle, "bufrHeaderCentre")), dtype="int64"
        )
        return out
    finally:
        eccodes.codes_release(handle)


def _uncompressed(handle, subsets: int, product: Product) -> dict[str, np.ndarray] | None:
    """An uncompressed multi-subset message, one extracted subset at a time."""
    import eccodes

    eccodes.codes_set(handle, "unpack", 1)
    parts = []
    for index in range(1, subsets + 1):
        eccodes.codes_set(handle, "extractSubset", index)
        eccodes.codes_set(handle, "doExtractSubsets", 1)
        single = eccodes.codes_clone(handle)
        try:
            decoded = decode_message(eccodes.codes_get_message(single), product)
        finally:
            eccodes.codes_release(single)
        if decoded is not None:
            parts.append(decoded)
    return concat(parts) if parts else None


def concat(parts: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    """Decoded messages joined along ``observation``; a 2-D element padded to the widest.

    An element that is single in one part and repeated in another becomes 2-D throughout.
    """
    if len(parts) == 1:
        return parts[0]
    names: dict[str, None] = {}
    for part in parts:
        names.update(dict.fromkeys(part))
    out: dict[str, np.ndarray] = {}
    for name in names:
        if name.endswith(".decimals"):
            out[name] = np.array(max(int(p[name]) for p in parts if name in p))
            continue
        present = [p[name] for p in parts if name in p]
        two_d = any(v.ndim == 2 for v in present)
        width = max((v.shape[1] for v in present if v.ndim == 2), default=1)
        kind = present[0].dtype
        columns = []
        for part in parts:
            rows = part["time"].shape[0]
            values = part.get(name)
            if values is None:
                values = np.full((rows, width) if two_d else rows, _fill(kind), dtype=kind)
            elif two_d:
                values = _widen(values if values.ndim == 2 else values[:, None], width)
            columns.append(values)
        out[name] = np.concatenate(columns)
    return out


def _fill(dtype: np.dtype):
    if dtype.kind == "f":
        return np.nan
    if dtype.kind in "iu":
        return MISSING_LONG
    if dtype.kind == "M":
        return np.datetime64("NaT")
    return ""


def _widen(values: np.ndarray, width: int) -> np.ndarray:
    if values.shape[1] >= width:
        return values
    pad = np.full(
        (values.shape[0], width - values.shape[1]), _fill(values.dtype), dtype=values.dtype
    )
    return np.concatenate([values, pad], axis=1)


def identities(arrays: dict[str, np.ndarray]) -> np.ndarray:
    """Each row's identity: a hash of every value decoded (provenance left out)."""
    rows = arrays["time"].shape[0]
    frame: dict[str, Any] = {}
    for name in sorted(arrays):
        if name.endswith(".decimals") or name in DERIVED:
            continue
        values = arrays[name]
        if values.ndim == 1:
            frame[name] = values
        else:
            for k in range(values.shape[1]):
                frame[f"{name}#{k}"] = values[:, k]
    if not frame:
        return np.zeros(rows, dtype="int64")
    table = pd.DataFrame(frame)
    return pd.util.hash_pandas_object(table, index=False).to_numpy().view("int64")


def decode_object(key: str, body: bytes, product: Product) -> tuple[dict | None, Counter]:
    """Every message of one object that belongs to ``product``, joined; and counts."""
    from planetary_datasets.providers.observations.wis2_targets import split_bufr

    counts: Counter = Counter()
    parts = []
    for message in split_bufr(body):
        counts["messages"] += 1
        try:
            decoded = decode_message(message, product)
        except Exception:  # noqa: BLE001 - one bad message must not lose the object
            counts["undecodable_messages"] += 1
            continue
        if decoded is None:
            counts[f"{product.group}_messages_of_other_products"] += 1
            continue
        if decoded["time"].size:
            parts.append(decoded)
    if not parts:
        return None, counts
    arrays = concat(parts)
    arrays["observation_identity"] = identities(arrays)
    counts[f"{product.group}_observations"] += int(arrays["time"].size)
    return arrays, counts
