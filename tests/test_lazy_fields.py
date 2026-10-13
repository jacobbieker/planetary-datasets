"""Tests for the lazy, memory-bounded field dataset."""

from __future__ import annotations

import threading
import weakref

import dask
import numcodecs.blosc
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from planetary_datasets.common.lazy_fields import (
    WRITE_OVERHEAD_FACTOR,
    FieldVariable,
    bounded_compute,
    choose_step_chunk,
    estimate_peak_bytes,
    lazy_field_dataset,
    missing_report,
    read_failures,
)
from planetary_datasets.common.store import write_to_icechunk
from planetary_datasets.memory import require_dataset_fits

N_VALUES = 50
N_STEPS = 7
N_LEVELS = 3
LEVEL_DIMS = ("init_time", "step", "isobaricInhPa", "values")


def _value(uri: str) -> float:
    """The deterministic value a synthetic field holds, recoverable from its URI."""
    return float(sum(int(part) * 10**k for k, part in enumerate(reversed(uri.split("/")[1:]))))


def _uri(name: str, init: pd.Timestamp, index: tuple[int, ...]) -> str:
    """A URI that encodes the field's position, so the reader can fill it deterministically."""
    return "/".join([f"{name}-{init:%Y%m%d%H}", *(str(i) for i in index)])


def _fill(uri: str, row: np.ndarray) -> None:
    row[:] = _value(uri) + np.arange(row.size, dtype=row.dtype)


def simple_reader(uri: str, row: np.ndarray) -> bool:
    """Fills each row with a ramp offset by a value derived from its URI."""
    _fill(uri, row)
    return True


def _expected(variable: FieldVariable, init: pd.Timestamp, missing=()) -> np.ndarray:
    out = np.full(variable.shape, np.nan, dtype=variable.dtype)
    for index in np.ndindex(*variable.shape[:-1]):
        uri = variable.uri_for(index)
        if uri is not None and uri not in missing:
            _fill(uri, out[index])
    return out


def _variables(init: pd.Timestamp, step_chunk: int = 3, skip=lambda name, index: False):
    def uri_for(name):
        def inner(index):
            return None if skip(name, index) else _uri(name, init, index)

        return inner

    return [
        FieldVariable(
            "t",
            LEVEL_DIMS,
            (1, N_STEPS, N_LEVELS, N_VALUES),
            (1, step_chunk, 1, N_VALUES),
            uri_for("t"),
            attrs={"units": "K"},
        ),
        FieldVariable(
            "tp",
            ("init_time", "step", "values"),
            (1, N_STEPS, N_VALUES),
            (1, step_chunk, N_VALUES),
            uri_for("tp"),
        ),
        FieldVariable(
            "hsurf", ("init_time", "values"), (1, N_VALUES), (1, N_VALUES), uri_for("hsurf")
        ),
    ]


def _coords(init: pd.Timestamp) -> dict:
    return {
        "init_time": pd.DatetimeIndex([init]),
        "step": pd.to_timedelta(np.arange(N_STEPS) * 3, "h"),
        "isobaricInhPa": np.array([1000.0, 850.0, 500.0]),
        "latitude": ("values", np.linspace(-90, 90, N_VALUES)),
        "longitude": ("values", np.linspace(0, 360, N_VALUES, endpoint=False)),
    }


INIT = pd.Timestamp("2026-01-01T00:00")


def test_field_variable_validates_layout():
    """A variable must end with a whole-field values dim and be floating point."""
    with pytest.raises(ValueError, match="end with"):
        FieldVariable("x", ("step", "level"), (2, 3), (1, 3), lambda i: None)
    with pytest.raises(ValueError, match="whole field"):
        FieldVariable("x", ("step", "values"), (2, 10), (1, 5), lambda i: None)
    with pytest.raises(ValueError, match="same length"):
        FieldVariable("x", ("step", "values"), (2, 10), (1,), lambda i: None)
    with pytest.raises(ValueError, match="floating point"):
        FieldVariable("x", ("values",), (10,), (10,), lambda i: None, dtype=np.int16)


def test_dask_blocks_are_the_declared_chunks():
    """Each dask block is one declared chunk, with one task per block."""
    ds = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader)

    assert ds["t"].data.chunks == ((1,), (3, 3, 1), (1, 1, 1), (N_VALUES,))
    assert ds["tp"].data.chunks == ((1,), (3, 3, 1), (N_VALUES,))
    assert ds["hsurf"].data.chunks == ((1,), (N_VALUES,))
    # One task per block, nothing else in the graph.
    assert len(ds["t"].data.dask) == 3 * 3
    assert ds["t"].attrs == {"units": "K"}


def test_task_names_are_deterministic():
    """Task names depend on the variable and token, not on object identity."""
    one = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader, token="run-a")
    two = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader, token="run-a")
    other = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader, token="run-b")

    later = INIT + pd.Timedelta(6, "h")
    next_run = lazy_field_dataset(_variables(later), _coords(later), simple_reader, token="run-a")

    assert one["t"].data.name == two["t"].data.name
    assert one["t"].data.name != other["t"].data.name
    assert one["t"].data.name != next_run["t"].data.name
    assert one["t"].data.name != one["tp"].data.name


def test_compute_matches_reader_output():
    """Computing the dataset gives exactly what the reader wrote, row by row."""
    variables = _variables(INIT)
    ds = lazy_field_dataset(variables, _coords(INIT), simple_reader, fetch_workers=4)

    with bounded_compute(2):
        loaded = ds.compute()

    for variable in variables:
        np.testing.assert_array_equal(loaded[variable.name].values, _expected(variable, INIT))


def test_missing_uris_and_reader_misses_are_nan():
    """A None URI or a reader returning False leaves the field NaN."""

    def skip(name, index):
        return name == "t" and index[1:] == (4, 1)

    absent_at_source = {_uri("t", INIT, (0, 0, 2)), _uri("tp", INIT, (0, 6))}

    def reader(uri, row):
        # Write garbage first: a miss must not leave a half-written row behind.
        row[:] = -1.0
        if uri in absent_at_source:
            return False
        _fill(uri, row)
        return True

    variables = _variables(INIT, skip=skip)
    ds = lazy_field_dataset(variables, _coords(INIT), reader, fetch_workers=3)

    assert missing_report(ds) == {"t": 1, "tp": 0, "hsurf": 0}
    loaded = ds.compute(scheduler="sync")
    assert read_failures(ds) == {"t": 1, "tp": 1, "hsurf": 0}

    t = loaded["t"].values
    assert np.isnan(t[0, 4, 1]).all()
    assert np.isnan(t[0, 0, 2]).all()
    assert int(np.isnan(t).all(axis=-1).sum()) == 2
    assert np.isnan(loaded["tp"].values[0, 6]).all()
    for variable in variables:
        np.testing.assert_array_equal(
            loaded[variable.name].values, _expected(variable, INIT, missing=absent_at_source)
        )


def test_reader_errors_propagate():
    """A transient reader error fails the compute and names the URI."""

    def reader(uri, row):
        if uri.endswith("/1/2"):
            raise ConnectionError("transient")
        _fill(uri, row)
        return True

    ds = lazy_field_dataset(_variables(INIT), _coords(INIT), reader, fetch_workers=2)

    with pytest.raises(ConnectionError, match="transient") as info:
        ds["t"].compute(scheduler="sync")
    assert any("while reading" in note for note in getattr(info.value, "__notes__", []))


def test_reader_must_return_a_bool():
    """A reader that forgets to return True fails loudly instead of storing NaN."""

    def reader(uri, row):
        _fill(uri, row)

    ds = lazy_field_dataset(_variables(INIT), _coords(INIT), reader, fetch_workers=2)

    with pytest.raises(TypeError, match="must return True"):
        ds["tp"].compute(scheduler="sync")


def test_reports_follow_each_dataset():
    """Reports come from each dataset's own graph, even when two share a token."""
    calls = []

    def missing_reader(uri, row):
        calls.append(uri)
        return False

    one = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader, token="same")
    two = lazy_field_dataset(_variables(INIT), _coords(INIT), missing_reader, token="same")
    two.compute(scheduler="sync")
    del two

    assert read_failures(one) == {"t": 0, "tp": 0, "hsurf": 0}
    assert missing_report(one) == {"t": 0, "tp": 0, "hsurf": 0}
    # A subset no longer maps onto its loader's index space, so it is not reported.
    assert "t" not in missing_report(one.isel(step=slice(0, 2)))


def test_default_names_depend_on_the_source():
    """Without a token, the same coordinates read from different URIs get distinct names."""

    def moved(index):
        return "mirror/" + _uri("t", INIT, index)

    here = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader)
    again = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader)
    variable = _variables(INIT)[0]
    elsewhere = lazy_field_dataset(
        [FieldVariable(variable.name, variable.dims, variable.shape, variable.chunks, moved)],
        _coords(INIT),
        simple_reader,
    )

    assert here["t"].data.name == again["t"].data.name
    assert here["t"].data.name != elsewhere["t"].data.name


class BlockTracker:
    """A reader that counts block buffers alive at once.

    Every row a reader receives is a view into its block's buffer, so ``row.base`` is the
    buffer. Registering a finalizer on it counts a block from its first read until it is
    garbage collected - the span during which it costs memory - not merely while it runs.

    The first ``rendezvous`` blocks wait for each other before reading, so the test
    proves that many really were in flight together instead of relying on timing; if
    they cannot all be running at once, the barrier times out and the write fails.
    """

    def __init__(self, rendezvous: int) -> None:
        """Count blocks; the first ``rendezvous`` of them must run concurrently."""
        self.lock = threading.Lock()
        self.barrier = threading.Barrier(rendezvous, timeout=30)
        self.rendezvous = rendezvous
        self.buffers: weakref.WeakValueDictionary[int, np.ndarray] = weakref.WeakValueDictionary()
        self.alive = 0
        self.peak = 0
        self.blocks = 0

    def _released(self) -> None:
        with self.lock:
            self.alive -= 1

    def __call__(self, uri: str, row: np.ndarray) -> bool:
        """Register the row's block buffer, then fill the row."""
        buffer = row.base
        assert buffer is not None, "reader should receive a view into the block buffer"
        with self.lock:
            first_read = self.buffers.get(id(buffer)) is not buffer
            if first_read:
                self.buffers[id(buffer)] = buffer
                weakref.finalize(buffer, self._released)
                self.alive += 1
                self.blocks += 1
                self.peak = max(self.peak, self.alive)
            meet = first_read and self.blocks <= self.rendezvous
        if meet:
            self.barrier.wait()
        _fill(uri, row)
        return True


@pytest.mark.parametrize("num_workers", [1, 2, 3])
def test_bounded_compute_limits_blocks_in_flight(local_config, num_workers):
    """Exactly ``num_workers`` block buffers are alive at the peak of a write."""
    tracker = BlockTracker(rendezvous=num_workers)
    variables = _variables(INIT, step_chunk=1)
    ds = lazy_field_dataset(variables, _coords(INIT), tracker, fetch_workers=1)
    repo = local_config.icechunk_repo("bounded.icechunk")

    with bounded_compute(num_workers):
        assert write_to_icechunk(repo, ds, append_dim="init_time")

    assert tracker.blocks == N_STEPS * N_LEVELS + N_STEPS + 1
    # At least num_workers by the rendezvous, and never more.
    assert tracker.peak == num_workers


def test_bounded_compute_cannot_overlap():
    """A second bounded_compute while one is active raises rather than clobbering it."""
    with bounded_compute(2):
        with pytest.raises(RuntimeError, match="already active"):
            with bounded_compute(1):
                pass
        assert dask.config.get("num_workers") == 2
    with bounded_compute(1):
        pass


def test_bounded_compute_sets_and_restores_settings():
    """The dask and blosc settings apply inside the block and are restored after."""
    blosc_before = numcodecs.blosc.get_nthreads()
    scheduler_before = dask.config.get("scheduler", None)

    with bounded_compute(1):
        assert dask.config.get("scheduler") == "threads"
        assert dask.config.get("num_workers") == 1
        assert dask.config.get("pool")._max_workers == 1
        assert numcodecs.blosc.get_nthreads() == 1

    assert numcodecs.blosc.get_nthreads() == blosc_before
    assert dask.config.get("scheduler", None) == scheduler_before
    with pytest.raises(ValueError):
        with bounded_compute(0):
            pass


def test_choose_step_chunk():
    """Step chunks fit the budget, are evened out, and stay within [1, n_steps]."""
    field = 2_949_120 * 4  # an ICON-Global float32 field, 11.25 MiB
    # 150 MiB holds 13 fields; 113 steps in 13s is nine chunks, the last of 9 steps.
    assert choose_step_chunk(2_949_120, 4, 113, target_mb=150) == 13
    four_fields = 4 * 1024 * 4 / 2**20
    # Evened out: 9 steps at a budget of 4 is 3+3+3 rather than 4+4+1...
    assert choose_step_chunk(1024, 4, 9, target_mb=four_fields) == 3
    # ...but never at the cost of an extra chunk: 10 steps still need three.
    assert choose_step_chunk(1024, 4, 10, target_mb=four_fields) == 4
    # Always at least one step, never more than there are.
    assert choose_step_chunk(2_949_120, 4, 113, target_mb=field / 2**20 / 2) == 1
    assert choose_step_chunk(10, 4, 5, target_mb=1000) == 5
    with pytest.raises(ValueError):
        choose_step_chunk(10, 4, 0)


def test_estimate_peak_bytes_and_memory_guard(local_config):
    """The estimate is blocks in flight times the overhead factor plus coordinates."""
    ds = lazy_field_dataset(_variables(INIT), _coords(INIT), simple_reader, fetch_workers=4)
    block = 3 * N_VALUES * 4
    coords = sum(int(ds[c].nbytes) for c in ds.coords)

    assert estimate_peak_bytes(ds, 2) == int(2 * block * WRITE_OVERHEAD_FACTOR) + coords
    assert estimate_peak_bytes(ds, 2, reader_bytes=100) == (
        int(2 * (block * WRITE_OVERHEAD_FACTOR + 4 * 100)) + coords
    )
    # The lazy dataset is judged on its blocks, not its total size.
    assert require_dataset_fits(ds, concurrency=4) <= 4 * block / 1024**3 + 1e-9


def _store_chunks(repo, name: str) -> tuple[int, ...]:
    group = zarr.open_group(repo.readonly_session("main").store, mode="r", zarr_format=3)
    return group[name].chunks


def test_write_and_append_round_trip(local_config):
    """A write and an append through write_to_icechunk read back exactly."""
    repo = local_config.icechunk_repo("lazy.icechunk")
    second = INIT + pd.Timedelta(6, "h")

    def skip(name, index):
        return name == "t" and index[1:] == (2, 0)

    for init in (INIT, second):
        ds = lazy_field_dataset(
            _variables(init, skip=skip),
            _coords(init),
            simple_reader,
            fetch_workers=2,
            token=str(init),
        )
        with bounded_compute(2):
            assert write_to_icechunk(repo, ds, append_dim="init_time")

    assert _store_chunks(repo, "t") == (1, 3, 1, N_VALUES)
    assert _store_chunks(repo, "tp") == (1, 3, N_VALUES)
    assert _store_chunks(repo, "hsurf") == (1, N_VALUES)

    stored = xr.open_zarr(
        repo.readonly_session("main").store, consolidated=False, decode_timedelta=True
    ).load()
    assert list(stored.init_time.values) == [INIT.to_datetime64(), second.to_datetime64()]
    np.testing.assert_array_equal(stored["latitude"].values, np.linspace(-90, 90, N_VALUES))
    assert stored["t"].attrs["units"] == "K"
    for position, init in enumerate((INIT, second)):
        for variable in _variables(init, skip=skip):
            np.testing.assert_array_equal(
                stored[variable.name].isel(init_time=[position]).values,
                _expected(variable, init),
            )

    # The same run again is recognised as stored and skipped.
    ds = lazy_field_dataset(_variables(second), _coords(second), simple_reader)
    with bounded_compute(2):
        assert not write_to_icechunk(repo, ds, append_dim="init_time")
