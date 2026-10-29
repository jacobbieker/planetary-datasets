"""Store each float variable in the narrowest dtype that holds it exactly.

Observation products often ship values in wider types than they need: positions and
times as float64 that were float32 at source, flags as float, small counts as int32. The
compressor in :func:`planetary_datasets.common.store.build_encoding` is lossless, so the
dtype is the only place precision could be lost; this module narrows a dtype only when
every value survives the round trip bit for bit (NaN for NaN).

The narrowing is decided once, when a store is created, from the first partition written
(:func:`narrowest_dtypes`). A later partition can hold a value the narrowed type cannot,
so every write checks its values against the store's dtypes first
(:func:`cast_exactly`) and raises :class:`LossyCastError` rather than letting zarr round
them silently.
"""

from __future__ import annotations

from typing import Iterable, Mapping

import numpy as np
import xarray as xr

#: Narrower float types to try, narrowest first.
FLOAT_LADDER = (np.dtype("float16"), np.dtype("float32"))


class LossyCastError(ValueError):
    """Writing a variable in the store's dtype would change some of its values."""


def _round_trips(values: np.ndarray, dtype: np.dtype) -> bool:
    with np.errstate(over="ignore", invalid="ignore"):
        narrowed = values.astype(dtype)
    back = narrowed.astype(values.dtype)
    if values.dtype.kind == "f":
        return bool(np.array_equal(back, values, equal_nan=True))
    return bool(np.array_equal(back, values))


def narrowest_dtype(values: np.ndarray, fill_value=None) -> np.dtype:
    """The narrowest float dtype that represents every value, and the fill, exactly.

    Floats step down to float32 or float16. Integers keep their dtype: their range in one
    partition says little about the next (a fill value or a count may be absent from the
    first), and under bitshuffle the unused high bytes compress to almost nothing anyway.
    Anything else, and an empty array, keeps its dtype.
    """
    values = np.asarray(values)
    if values.size == 0 or values.dtype.kind != "f":
        return values.dtype
    if fill_value is not None:
        values = np.append(values.ravel(), np.asarray(fill_value, dtype=values.dtype))
    for candidate in FLOAT_LADDER:
        if candidate.itemsize < values.dtype.itemsize and _round_trips(values, candidate):
            return candidate
    return values.dtype


def narrowest_dtypes(ds: xr.Dataset, skip: Iterable[str] = ()) -> dict[str, np.dtype]:
    """:func:`narrowest_dtype` for every float variable of ``ds`` not in ``skip``.

    A variable's ``_FillValue`` attribute (present when a file is read without CF
    decoding) must survive the narrowing too.
    """
    skip = set(skip)
    return {
        name: narrowest_dtype(ds[name].values, ds[name].attrs.get("_FillValue"))
        for name in ds.variables
        if name not in skip and ds[name].dtype.kind == "f"
    }


def cast_exactly(ds: xr.Dataset, dtypes: Mapping[str, np.dtype]) -> xr.Dataset:
    """``ds`` with each variable in ``dtypes`` cast to it, or :class:`LossyCastError`.

    Encoded fill values and dtypes are dropped with the cast, so the stored dtype is the
    one chosen here rather than whatever the source file declared.
    """
    out = ds.copy()
    for name, dtype in dtypes.items():
        if name not in out.variables or out[name].dtype == dtype:
            continue
        values = out[name].values
        if not _round_trips(values, np.dtype(dtype)):
            raise LossyCastError(
                f"{name}: {values.dtype} values do not all fit {np.dtype(dtype)} exactly"
            )
        variable = out[name].variable.astype(dtype)
        variable.encoding = {}
        out[name] = variable
    return out


__all__ = [
    "FLOAT_LADDER",
    "LossyCastError",
    "cast_exactly",
    "narrowest_dtype",
    "narrowest_dtypes",
]
