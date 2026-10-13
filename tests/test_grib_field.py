"""Decoding single-field GRIB2 messages from bytes, as DWD's ICON archives store them.

Every message here is built in memory from eccodes' GRIB2 sample, so the tests need no
network: an unstructured ICON-style grid (template 3.101), pressure, model, soil and
surface levels, steps encoded in hours and in minutes, accumulations, bitmaps and the
CCSDS packing DWD uses.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from planetary_datasets.common.grib import GribFieldInfo, decode_grib_field, grib_field_info

eccodes = pytest.importorskip("eccodes")

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

#: The global ICON R03B07 grid's number and UUID, as DWD encodes them.
ICON_GRID_NUMBER = 26
ICON_GRID_UUID = "a27b8de618c411e4820ab5b098c6a5c0"

#: The value eccodes uses to mark missing points when building a message.
SENTINEL = 9999.0


def _values(n: int = 1000) -> np.ndarray:
    return np.linspace(200.0, 300.0, n)


def _message(
    values: np.ndarray | None = None,
    *,
    packing: str = "grid_simple",
    unstructured: bool = True,
    level_type: int = 100,
    scaled_value: int | None = 85000,
    scale_factor: int = 0,
    step: int = 3,
    step_unit: str = "h",
    start_step: int | None = None,
    data_date: int = 20261011,
    data_time: int = 1200,
) -> bytes:
    """One GRIB2 message; points of ``values`` equal to :data:`SENTINEL` go in a bitmap.

    With ``start_step`` the field is an accumulation over ``start_step``-``step``.
    """
    values = _values() if values is None else values
    handle = eccodes.codes_grib_new_from_samples("GRIB2")
    try:
        eccodes.codes_set(handle, "centre", 78)
        if unstructured:
            eccodes.codes_set(handle, "gridDefinitionTemplateNumber", 101)
            eccodes.codes_set(handle, "numberOfGridUsed", ICON_GRID_NUMBER)
            eccodes.codes_set(handle, "numberOfGridInReference", 1)
            eccodes.codes_set(handle, "uuidOfHGrid", ICON_GRID_UUID)
            eccodes.codes_set(handle, "numberOfDataPoints", values.size)
        else:
            # The sample's own regular lat/lon grid fixes the number of points.
            assert values.size == eccodes.codes_get(handle, "numberOfDataPoints")
        if start_step is not None:
            eccodes.codes_set(handle, "productDefinitionTemplateNumber", 8)
            eccodes.codes_set(handle, "typeOfStatisticalProcessing", 1)
        eccodes.codes_set(handle, "typeOfFirstFixedSurface", level_type)
        if scaled_value is None:
            eccodes.codes_set_missing(handle, "scaleFactorOfFirstFixedSurface")
            eccodes.codes_set_missing(handle, "scaledValueOfFirstFixedSurface")
        else:
            eccodes.codes_set(handle, "scaleFactorOfFirstFixedSurface", scale_factor)
            eccodes.codes_set(handle, "scaledValueOfFirstFixedSurface", scaled_value)
        eccodes.codes_set(handle, "dataDate", data_date)
        eccodes.codes_set(handle, "dataTime", data_time)
        eccodes.codes_set(handle, "stepUnits", step_unit)
        if start_step is not None:
            eccodes.codes_set(handle, "startStep", start_step)
        eccodes.codes_set(handle, "endStep", step)
        if np.any(values == SENTINEL):
            eccodes.codes_set(handle, "bitmapPresent", 1)
            eccodes.codes_set(handle, "missingValue", SENTINEL)
        eccodes.codes_set(handle, "bitsPerValue", 16)
        eccodes.codes_set_values(handle, values)
        if packing != "grid_simple":
            eccodes.codes_set(handle, "packingType", packing)
        return eccodes.codes_get_message(handle)
    finally:
        eccodes.codes_release(handle)


def _with_bitmap(n: int = 1000) -> tuple[np.ndarray, np.ndarray]:
    """Values with every seventh point missing, and the mask of missing points."""
    values = _values(n)
    missing = np.zeros(n, dtype=bool)
    missing[::7] = True
    values[missing] = SENTINEL
    return values, missing


def _packing_tolerance(values: np.ndarray) -> float:
    """Generous bound on 16-bit packing error for ``values``."""
    present = values[values != SENTINEL]
    return float(present.max() - present.min()) / 2**15


class TestGribFieldInfo:
    def test_an_icon_pressure_level_header(self):
        info = grib_field_info(_message(packing="grid_ccsds"))
        assert info == GribFieldInfo(
            short_name="t",
            level_type=100,
            level=85000.0,
            step_hours=3,
            number_of_values=1000,
            grid_number=ICON_GRID_NUMBER,
            grid_uuid=ICON_GRID_UUID,
            init_time=pd.Timestamp("2026-10-11T12:00"),
        )

    def test_it_never_touches_the_values(self, monkeypatch):
        def no_decoding(*args, **kwargs):
            raise AssertionError("grib_field_info decoded the values")

        for name in ("codes_get_array", "codes_get_values", "codes_get_double_array"):
            monkeypatch.setattr(eccodes, name, no_decoding)
        assert grib_field_info(_message()).number_of_values == 1000

    @pytest.mark.parametrize(
        ("level_type", "scaled_value", "scale_factor", "expected"),
        [
            (106, 1, 2, 0.01),  # soil depth in metres, W_SO's lv1/0.01
            (106, 81, 2, 0.81),
            (150, 100, 0, 100.0),  # model level number
            (103, 2, 0, 2.0),  # height above ground, T_2M
            (1, 0, 0, 0.0),  # ground surface, TOT_PREC
        ],
    )
    def test_levels_come_in_the_native_unit(self, level_type, scaled_value, scale_factor, expected):
        info = grib_field_info(
            _message(level_type=level_type, scaled_value=scaled_value, scale_factor=scale_factor)
        )
        assert info.level_type == level_type
        assert info.level == expected

    def test_a_missing_level_value_is_none(self):
        info = grib_field_info(_message(level_type=1, scaled_value=None))
        assert info.level_type == 1
        assert info.level is None

    def test_a_missing_level_type_is_none(self):
        info = grib_field_info(_message(level_type=255, scaled_value=None))
        assert info.level_type is None
        assert info.level is None

    def test_a_step_encoded_in_minutes_is_read_in_hours(self):
        message = _message(step=180, step_unit="m")
        handle = eccodes.codes_new_from_message(message)
        try:
            assert eccodes.codes_get(handle, "indicatorOfUnitOfTimeRange") == 0
        finally:
            eccodes.codes_release(handle)
        assert grib_field_info(message).step_hours == 3

    def test_an_accumulation_reports_the_end_of_its_window(self):
        info = grib_field_info(_message(start_step=0, step=6, level_type=1, scaled_value=0))
        assert info.step_hours == 6

    def test_a_step_that_is_not_whole_hours_is_rejected(self):
        with pytest.raises(ValueError, match="90 min"):
            grib_field_info(_message(step=90, step_unit="m"))

    def test_a_regular_grid_has_no_grid_number_or_uuid(self):
        info = grib_field_info(_message(_values(496), unstructured=False))
        assert info.grid_number is None
        assert info.grid_uuid is None
        assert info.number_of_values == 496

    def test_midnight_reference_time(self):
        info = grib_field_info(_message(data_time=0))
        assert info.init_time == pd.Timestamp("2026-10-11T00:00")

    def test_number_of_values_counts_the_missing_points(self):
        values, _ = _with_bitmap()
        assert grib_field_info(_message(values)).number_of_values == values.size


class TestDecodeGribField:
    @pytest.mark.parametrize("packing", ["grid_simple", "grid_ccsds"])
    def test_values_round_trip_within_packing_precision(self, packing):
        values = _values()
        message = _message(values, packing=packing)
        decoded = decode_grib_field(message)
        assert decoded.dtype == np.float32
        assert decoded.shape == values.shape
        np.testing.assert_allclose(decoded, values, rtol=0, atol=_packing_tolerance(values))

    @pytest.mark.parametrize("packing", ["grid_simple", "grid_ccsds"])
    def test_bitmap_points_are_nan(self, packing):
        values, missing = _with_bitmap()
        decoded = decode_grib_field(_message(values, packing=packing))
        assert decoded.shape == values.shape
        np.testing.assert_array_equal(np.isnan(decoded), missing)
        np.testing.assert_allclose(
            decoded[~missing], values[~missing], rtol=0, atol=_packing_tolerance(values)
        )

    def test_complex_packing_missing_values_without_a_bitmap_are_nan(self):
        values, missing = _with_bitmap(496)
        handle = eccodes.codes_grib_new_from_samples("GRIB2")
        try:
            eccodes.codes_set(handle, "packingType", "grid_complex_spatial_differencing")
            eccodes.codes_set(handle, "bitmapPresent", 0)
            eccodes.codes_set(handle, "missingValueManagementUsed", 1)
            eccodes.codes_set(handle, "missingValue", SENTINEL)
            eccodes.codes_set_values(handle, values)
            assert eccodes.codes_get(handle, "bitmapPresent") == 0
            message = eccodes.codes_get_message(handle)
        finally:
            eccodes.codes_release(handle)
        decoded = decode_grib_field(message)
        np.testing.assert_array_equal(np.isnan(decoded), missing)

    def test_the_message_really_is_ccsds_packed(self):
        handle = eccodes.codes_new_from_message(_message(packing="grid_ccsds"))
        try:
            assert eccodes.codes_get(handle, "dataRepresentationTemplateNumber") == 42
        finally:
            eccodes.codes_release(handle)

    def test_float32_matches_decoding_to_float64_and_casting(self):
        message = _message(packing="grid_ccsds")
        as_float64 = decode_grib_field(message, dtype=np.float64)
        assert as_float64.dtype == np.float64
        np.testing.assert_array_equal(decode_grib_field(message), as_float64.astype(np.float32))

    def test_a_constant_field_decodes(self):
        decoded = decode_grib_field(_message(np.zeros(1000), packing="grid_ccsds"))
        np.testing.assert_array_equal(decoded, np.zeros(1000, dtype=np.float32))

    @pytest.mark.parametrize("dtype", [np.float32, np.float64])
    def test_out_is_filled_in_place_and_returned(self, dtype):
        values, missing = _with_bitmap()
        buffer = np.full((3, values.size), -1.0, dtype=dtype)
        result = decode_grib_field(_message(values, packing="grid_ccsds"), out=buffer[1])
        assert result is not None and np.shares_memory(result, buffer)
        assert result.base is buffer
        np.testing.assert_array_equal(np.isnan(buffer[1]), missing)
        np.testing.assert_allclose(
            buffer[1, ~missing], values[~missing], rtol=0, atol=_packing_tolerance(values)
        )
        assert (buffer[0] == -1).all() and (buffer[2] == -1).all()

    def test_out_can_be_a_strided_view(self):
        values = _values()
        buffer = np.zeros((values.size, 2), dtype=np.float32)
        decode_grib_field(_message(values), out=buffer[:, 0])
        np.testing.assert_allclose(buffer[:, 0], values, rtol=0, atol=_packing_tolerance(values))
        assert (buffer[:, 1] == 0).all()

    def test_out_of_the_wrong_length_is_rejected(self):
        out = np.zeros(999, dtype=np.float32)
        with pytest.raises(ValueError, match="999"):
            decode_grib_field(_message(), out=out)
        assert (out == 0).all()

    def test_out_must_be_one_dimensional(self):
        with pytest.raises(ValueError, match="1-D"):
            decode_grib_field(_message(), out=np.zeros((10, 100), dtype=np.float32))

    @pytest.mark.parametrize("dtype", [np.int32, np.uint16, bool])
    def test_a_non_floating_dtype_is_rejected(self, dtype):
        with pytest.raises(ValueError, match="floating"):
            decode_grib_field(_message(), dtype=dtype)
        with pytest.raises(ValueError, match="floating"):
            decode_grib_field(_message(), out=np.zeros(1000, dtype=dtype))

    @pytest.mark.parametrize("wrap", [bytearray, memoryview])
    def test_any_bytes_like_message_is_accepted(self, wrap):
        message = _message()
        np.testing.assert_array_equal(decode_grib_field(wrap(message)), decode_grib_field(message))
        assert grib_field_info(wrap(message)) == grib_field_info(message)


class TestOneMessageExactly:
    """A truncated download or a concatenated file must fail loudly, never crash or pass."""

    @pytest.fixture(params=[grib_field_info, decode_grib_field], ids=["info", "decode"])
    def read(self, request):
        return request.param

    def test_two_messages_are_rejected(self, read):
        message = _message()
        with pytest.raises(ValueError, match="exactly one"):
            read(message + message)

    def test_trailing_bytes_are_rejected(self, read):
        with pytest.raises(ValueError, match="exactly one"):
            read(_message() + b"\0\0")

    @pytest.mark.parametrize("keep", [12, 16, 100, -1])
    def test_a_truncated_message_is_rejected(self, read, keep):
        message = _message(packing="grid_ccsds")
        with pytest.raises(ValueError):
            read(message[:keep])

    @pytest.mark.parametrize("data", [b"", b"garbage", b"xx" + b"GRIB" + bytes(20)])
    def test_bytes_without_a_message_are_rejected(self, read, data):
        with pytest.raises(ValueError, match="no GRIB message"):
            read(data)

    def test_a_missing_end_marker_is_rejected(self, read):
        message = _message()
        with pytest.raises(ValueError, match="7777"):
            read(message[:-4] + b"8888")

    def test_grib1_is_rejected(self, read):
        handle = eccodes.codes_grib_new_from_samples("GRIB1")
        try:
            message = eccodes.codes_get_message(handle)
        finally:
            eccodes.codes_release(handle)
        with pytest.raises(ValueError, match="edition 1"):
            read(message)

    def test_every_handle_opened_is_released(self, monkeypatch):
        """Including when a check fails after eccodes has opened the message."""
        opened, released = [], []
        real_new, real_release = eccodes.codes_new_from_message, eccodes.codes_release

        def new(message):
            handle = real_new(message)
            opened.append(handle)
            return handle

        def release(handle):
            released.append(handle)
            real_release(handle)

        good, odd_step = _message(), _message(step=90, step_unit="m")
        monkeypatch.setattr(eccodes, "codes_new_from_message", new)
        monkeypatch.setattr(eccodes, "codes_release", release)
        grib_field_info(good)
        decode_grib_field(good)
        with pytest.raises(ValueError, match="whole number of hours"):
            grib_field_info(odd_step)
        with pytest.raises(ValueError, match="field has 1000"):
            decode_grib_field(good, out=np.zeros(3, dtype=np.float32))
        assert len(opened) == 4
        assert released == opened


def test_importing_the_module_does_not_import_eccodes():
    """The heavy native eccodes library loads when a field is read, not on import."""
    code = (
        "import sys; import planetary_datasets.common.grib; "
        "sys.exit('eccodes' in sys.modules or 'cfgrib' in sys.modules)"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True)
    assert result.returncode == 0, result.stderr.decode()
