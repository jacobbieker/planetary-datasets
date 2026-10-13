"""Reading GRIB: whole files with cfgrib, and single fields with eccodes.

cfgrib splits a GRIB file into one dataset per level type (``cfgrib.open_datasets``),
merging the hypercubes it finds for each with ``xr.merge`` and no explicit ``compat``.
Current xarray warns on every such call that the default is changing; the call is
inside cfgrib, so the warning cannot be fixed at its source and would otherwise repeat
for every file a provider opens.

Archives that hold one GRIB2 field per file (DWD's ICON tree) are cheaper to read
without cfgrib: :func:`grib_field_info` reads a message's headers and
:func:`decode_grib_field` decodes its values, into a caller's buffer if given, both
from the message's bytes with no temporary files and no xarray. eccodes is a heavy
native dependency, so, like cfgrib, it is imported only when one of them is called.
"""

from __future__ import annotations

import contextlib
import dataclasses
import warnings
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    import numpy.typing as npt

#: The xarray FutureWarnings about ``combine``/``merge`` defaults changing.
_COMBINE_DEFAULT_WARNINGS = (
    r".*default value for compat will change.*",
    r".*default value for join will change.*",
)


@contextlib.contextmanager
def quiet_combine_defaults() -> Iterator[None]:
    """Silence xarray's warnings about upcoming ``compat``/``join`` defaults.

    Only those warnings are suppressed, and only inside the block. Code that calls
    ``xr.merge`` itself should pass ``compat`` and ``join`` explicitly instead.
    """
    with warnings.catch_warnings():
        for message in _COMBINE_DEFAULT_WARNINGS:
            warnings.filterwarnings("ignore", message=message, category=FutureWarning)
        yield


def open_grib_datasets(path: str, **kwargs: Any) -> list[xr.Dataset]:
    """Open every hypercube in a GRIB file, one dataset per level type.

    Args:
        path: Local path of the GRIB file.
        **kwargs: Passed through to ``cfgrib.open_datasets``, e.g. ``backend_kwargs``.

    Returns:
        The datasets cfgrib finds in the file, in its order.
    """
    import cfgrib

    with quiet_combine_defaults():
        return cfgrib.open_datasets(str(path), **kwargs)


#: Bytes-like objects a GRIB message can be passed as.
GribMessage = Union[bytes, bytearray, memoryview]

#: Section 0 of a GRIB2 message: ``GRIB``, two reserved bytes, the discipline, the
#: edition, then the total message length as an 8-byte big-endian integer.
_GRIB2_SECTION0_LENGTH = 16
_GRIB_END_MARKER = b"7777"

#: Packings eccodes 2.49 can unpack straight to float32; the rest (JPEG, PNG, IEEE)
#: only unpack to float64.
_FLOAT32_PACKINGS = frozenset(
    {
        "grid_ccsds",
        "grid_simple",
        "grid_complex",
        "grid_complex_spatial_differencing",
        "grid_second_order",
    }
)

#: ``gridDefinitionTemplateNumber`` of an unstructured grid (ICON's triangles).
_UNSTRUCTURED_GRID_TEMPLATE = 101

#: Code table 4.5 entry for "missing"; eccodes does not report it as a missing value.
_MISSING_SURFACE_TYPE = 255


@dataclasses.dataclass(frozen=True)
class GribFieldInfo:
    """The header of a single GRIB2 field, read without decoding its values.

    Attributes:
        short_name: eccodes ``shortName`` from the definitions eccodes ships. For DWD's
            ICON fields this is the ECMWF name where one exists (``T_2M`` is ``"2t"``,
            ``T`` is ``"t"``, ``TOT_PREC`` is ``"tp"``) and DWD's own name otherwise
            (``W_SO`` is ``"W_SO"``); ``"unknown"`` if eccodes has no name for it.
        level_type: ``typeOfFirstFixedSurface``: 1 surface, 100 isobaric, 103 height
            above ground, 106 depth below land, 150 generalised vertical (model level).
            ``None`` if the message has no first fixed surface.
        level: Value of the first fixed surface in its native unit: Pa for 100, metres
            for 103 and 106, the level number for 150, 0.0 for the surface. For a layer
            (soil, a model layer between half levels) this is its top, the value DWD's
            paths put under ``lv1``. ``None`` if missing.
        step_hours: Forecast step in hours, from ``endStep`` whatever unit the message
            encodes it in. For an accumulation or other statistic over a window this is
            the end of the window, e.g. 6 for ``TOT_PREC`` accumulated over 0-6 h.
        number_of_values: ``numberOfDataPoints``, the length of the decoded field with
            missing points included (``numberOfValues`` counts only the packed ones).
        grid_number: ``numberOfGridUsed`` of an unstructured grid (template 3.101),
            e.g. 26 for global ICON; ``None`` for any other grid.
        grid_uuid: ``uuidOfHGrid`` of an unstructured grid as 32 hex characters;
            ``None`` for any other grid.
        init_time: Reference time, ``dataDate`` + ``dataTime``.
    """

    short_name: str
    level_type: Optional[int]
    level: Optional[float]
    step_hours: int
    number_of_values: int
    grid_number: Optional[int]
    grid_uuid: Optional[str]
    init_time: pd.Timestamp


def _check_single_grib2_message(message: GribMessage) -> memoryview:
    """Check ``message`` is exactly one complete GRIB2 message.

    eccodes reads the first message in a buffer and ignores whatever follows, and it
    can crash the interpreter on a truncated header, so the framing is checked here
    before eccodes sees the bytes.

    Args:
        message: The bytes to check.

    Returns:
        A flat byte view of ``message``.

    Raises:
        ValueError: If ``message`` is not one complete GRIB2 message with nothing after.
    """
    view = memoryview(message).cast("B")
    size = len(view)
    if size < _GRIB2_SECTION0_LENGTH or view[:4] != b"GRIB":
        raise ValueError(f"no GRIB message in {size} bytes")
    edition = view[7]
    if edition != 2:
        raise ValueError(f"expected a GRIB2 message, got GRIB edition {edition}")
    total_length = int.from_bytes(view[8:_GRIB2_SECTION0_LENGTH], "big")
    if total_length > size:
        raise ValueError(f"truncated GRIB2 message: header says {total_length} bytes, got {size}")
    if total_length < size:
        raise ValueError(
            f"expected exactly one GRIB2 message, but {size - total_length} bytes follow "
            f"the first {total_length}-byte message"
        )
    if view[total_length - len(_GRIB_END_MARKER) : total_length] != _GRIB_END_MARKER:
        raise ValueError("GRIB2 message does not end with 7777")
    return view


@contextlib.contextmanager
def _grib_handle(message: GribMessage) -> Iterator[int]:
    """Open one GRIB2 message with eccodes and release it afterwards.

    Args:
        message: Exactly one GRIB2 message.

    Yields:
        The eccodes handle. eccodes holds its own copy of the bytes.

    Raises:
        ValueError: If ``message`` is not exactly one GRIB2 message eccodes can open.
    """
    import eccodes

    view = _check_single_grib2_message(message)
    try:
        handle = eccodes.codes_new_from_message(view)
    except eccodes.CodesInternalError as err:
        raise ValueError(f"eccodes could not open the GRIB2 message: {err}") from err
    try:
        total_length = eccodes.codes_get(handle, "totalLength", ktype=int)
        if total_length != len(view):
            raise ValueError(
                f"expected exactly one GRIB2 message of {len(view)} bytes, "
                f"eccodes read {total_length}"
            )
        yield handle
    finally:
        eccodes.codes_release(handle)


def _get_optional_int(handle: int, key: str) -> Optional[int]:
    """Integer value of ``key``, or ``None`` if it is absent or set to missing."""
    import eccodes

    if not eccodes.codes_is_defined(handle, key) or eccodes.codes_is_missing(handle, key):
        return None
    return int(eccodes.codes_get(handle, key, ktype=int))


def _first_surface_value(handle: int) -> Optional[float]:
    """Value of the first fixed surface, ``scaledValue / 10**scaleFactor``."""
    scaled = _get_optional_int(handle, "scaledValueOfFirstFixedSurface")
    factor = _get_optional_int(handle, "scaleFactorOfFirstFixedSurface")
    if scaled is None or factor is None:
        return None
    # Dividing by an exact power of ten keeps e.g. 1e-2 m equal to the literal 0.01.
    return scaled / 10**factor if factor >= 0 else float(scaled * 10**-factor)


def _step_hours(handle: int) -> int:
    """``endStep`` in whole hours, whatever unit the message encodes it in.

    Raises:
        ValueError: If the step is not a whole number of hours.
    """
    import eccodes

    # stepUnits only picks the unit eccodes reports steps in; the message is unchanged.
    eccodes.codes_set(handle, "stepUnits", "m")
    minutes = int(eccodes.codes_get(handle, "endStep", ktype=int))
    hours, remainder = divmod(minutes, 60)
    if remainder:
        raise ValueError(f"forecast step of {minutes} min is not a whole number of hours")
    return hours


def grib_field_info(message: GribMessage) -> GribFieldInfo:
    """Read the header of a single-field GRIB2 message without decoding its values.

    Args:
        message: Exactly one GRIB2 message, e.g. the contents of one DWD ICON file.

    Returns:
        The field's name, level, step, grid and reference time.

    Raises:
        ValueError: If ``message`` is not exactly one GRIB2 message, or its forecast
            step is not a whole number of hours.
    """
    import eccodes

    with _grib_handle(message) as handle:
        level_type = _get_optional_int(handle, "typeOfFirstFixedSurface")
        if level_type == _MISSING_SURFACE_TYPE:
            level_type = None
        level = _first_surface_value(handle) if level_type is not None else None
        grid_number: Optional[int] = None
        grid_uuid: Optional[str] = None
        if _get_optional_int(handle, "gridDefinitionTemplateNumber") == _UNSTRUCTURED_GRID_TEMPLATE:
            grid_number = _get_optional_int(handle, "numberOfGridUsed")
            grid_uuid = str(eccodes.codes_get(handle, "uuidOfHGrid", ktype=str))
        date = int(eccodes.codes_get(handle, "dataDate", ktype=int))
        time = int(eccodes.codes_get(handle, "dataTime", ktype=int))
        return GribFieldInfo(
            short_name=str(eccodes.codes_get(handle, "shortName", ktype=str)),
            level_type=level_type,
            level=level,
            step_hours=_step_hours(handle),
            number_of_values=int(eccodes.codes_get(handle, "numberOfDataPoints", ktype=int)),
            grid_number=grid_number,
            grid_uuid=grid_uuid,
            init_time=pd.Timestamp(
                year=date // 10000,
                month=date // 100 % 100,
                day=date % 100,
                hour=time // 100,
                minute=time % 100,
            ),
        )


def decode_grib_field(
    message: GribMessage,
    dtype: npt.DTypeLike = np.float32,
    out: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Decode the values of a single-field GRIB2 message.

    Points the bitmap marks as missing come back as NaN. For a float32 target in a
    packing eccodes can unpack to float32 (CCSDS, simple, complex), eccodes decodes
    straight to float32; that matches decoding to float64 and casting, since packed
    values carry far fewer significant bits than float32 holds. Otherwise eccodes
    decodes to float64 and the result is cast.

    Peak memory beyond ``message`` and ``out`` is one decoded copy of the field (4
    bytes per point as float32, 8 for the float64 fallback) plus eccodes' copy of the
    message and its unpacking scratch (about 2 bytes per point for 16-bit CCSDS). For
    global ICON (2,949,120 points, a 3 MB message) that measured 20 MiB for float32
    and 32 MiB for float64. Decoding many such fields in a loop, the allocator holds
    on to some freed blocks: RSS settled about 140 MiB above the first call's on
    macOS and then stayed flat.

    Args:
        message: Exactly one GRIB2 message, e.g. the contents of one DWD ICON file.
        dtype: Floating dtype of the returned array. Ignored when ``out`` is given.
        out: A 1-D floating array of length ``numberOfDataPoints``, e.g. one row of a
            preallocated buffer. The values are written into it and it is returned.

    Returns:
        The field as a 1-D array of ``numberOfDataPoints`` values: ``out`` if given,
        else a new array of ``dtype``.

    Raises:
        ValueError: If ``message`` is not exactly one GRIB2 message, the target dtype
            is not floating point, or ``out`` is not 1-D or has the wrong length.
    """
    import eccodes

    target = np.dtype(out.dtype if out is not None else dtype)
    if target.kind != "f":
        raise ValueError(f"missing points decode to NaN, so dtype must be floating, not {target}")
    if out is not None and out.ndim != 1:
        raise ValueError(f"out must be 1-D, got shape {out.shape}")

    with _grib_handle(message) as handle:
        size = int(eccodes.codes_get(handle, "numberOfDataPoints", ktype=int))
        if out is not None and out.shape[0] != size:
            raise ValueError(f"out has {out.shape[0]} values but the field has {size}")
        # Missing points, from a bitmap or complex packing's own missing-value
        # management, decode to missingValue; the message itself is unchanged.
        eccodes.codes_set(handle, "missingValue", np.nan)
        packing = eccodes.codes_get(handle, "packingType", ktype=str)
        as_float32 = target.itemsize <= 4 and packing in _FLOAT32_PACKINGS
        values = eccodes.codes_get_array(
            handle, "values", ktype=np.float32 if as_float32 else float
        )

    if values.shape != (size,):
        raise ValueError(f"eccodes decoded {values.shape} values for a field of {size}")
    if out is None:
        return values.astype(target, copy=False)
    np.copyto(out, values, casting="same_kind")
    return out
