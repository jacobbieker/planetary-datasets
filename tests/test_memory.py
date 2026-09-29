"""Memory estimation and guards."""

from __future__ import annotations

import os
import time

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from planetary_datasets import memory

MB = 1e6 / 1024**3  # 1 MB in GiB, the unit the estimates report in.


@pytest.fixture
def ceiling(monkeypatch):
    """Set ``MEMORY_CEILING_GB``, which the guards read through the process-wide config."""
    return lambda gb: monkeypatch.setenv("MEMORY_CEILING_GB", str(gb))


def _eager_8mb() -> xr.Dataset:
    return xr.Dataset({"x": (("a", "b"), np.zeros((1000, 1000), dtype="float64"))})


def _mixed() -> xr.Dataset:
    """A tiny chunked variable beside a 32 MB eager one."""
    return xr.Dataset(
        {
            "small_chunked": (("a", "b"), da.zeros((100, 100), dtype="float64", chunks=(10, 10))),
            "big_eager": (("c", "d"), np.zeros((2000, 2000), dtype="float64")),
        }
    )


def _bulk_in_coords() -> xr.Dataset:
    """A finely chunked variable on a 32 MB eager 2-D coordinate, as geostationary data is."""
    return xr.Dataset(
        {"radiance": (("y", "x"), da.zeros((2000, 2000), dtype="float32", chunks=(10, 10)))},
        coords={"latitude": (("y", "x"), np.zeros((2000, 2000), dtype="float64"))},
    )


def test_host_and_process_memory_are_reported():
    assert memory.total_memory_gb() > 0
    assert 0 < memory.available_memory_gb() <= memory.total_memory_gb()
    assert memory.process_tree_rss_gb() > 0


def test_estimate_matches_a_known_array_size():
    ds = _eager_8mb()
    assert memory.estimate_dataset_gb(ds) == pytest.approx(8 * MB, rel=0.01)
    # Not chunked, so the peak is the whole thing.
    assert memory.estimate_peak_gb(ds) == pytest.approx(memory.estimate_dataset_gb(ds))


def test_estimate_does_not_load_a_lazy_dataset():
    # 1e11 bytes lazy, i.e. 93.1 GiB. Estimating it must not try to allocate it.
    ds = xr.Dataset({"x": (("a", "b"), da.zeros((100_000, 125_000), dtype="float64", chunks=1000))})
    assert memory.estimate_dataset_gb(ds) == pytest.approx(1e5 * MB, rel=0.01)


def test_peak_estimate_uses_chunks_for_a_lazy_dataset():
    """Regression: sizing a long lazy concat by its total rejected small streaming jobs."""
    # 100 chunks of 8 MB each: total 800 MB, largest chunk 8 MB.
    ds = xr.Dataset({"x": (("a", "b"), da.zeros((100_000, 1000), dtype="float64", chunks=(1000, 1000)))})
    assert memory.largest_chunk_gb(ds) == pytest.approx(8 * MB, rel=0.01)
    assert memory.estimate_dataset_gb(ds) == pytest.approx(800 * MB, rel=0.01)
    assert memory.estimate_peak_gb(ds, concurrency=4) == pytest.approx(4 * 8 * MB, rel=0.01)


def test_peak_never_exceeds_the_total():
    # One single chunk: 4x concurrency must not inflate past the real total.
    ds = xr.Dataset({"x": (("a",), da.zeros(1000, dtype="float64", chunks=1000))})
    assert memory.estimate_peak_gb(ds, concurrency=4) == pytest.approx(memory.estimate_dataset_gb(ds))


@pytest.mark.parametrize(
    "make",
    [
        # Regression: skipping unchunked variables sized a mixed dataset at nearly zero.
        pytest.param(_mixed, id="eager-variable"),
        # Regression: sizing only ``data_vars`` hid the 2-D geostationary coordinates. A
        # 21696x21696 float64 latitude array is 3.8 GB and dwarfs any single data chunk.
        pytest.param(_bulk_in_coords, id="eager-coordinate"),
    ],
)
def test_an_eager_array_counts_as_one_whole_chunk(make):
    assert memory.largest_chunk_gb(make()) == pytest.approx(32 * MB, rel=0.01)


@pytest.mark.parametrize(
    ("ceiling_gb", "make"),
    [
        # The whole point of require_dataset_fits: refuse before the OOM killer does.
        pytest.param(0.001, _bulk_in_coords, id="bulk-in-coords"),
        pytest.param(0.001, _mixed, id="mixed"),
        pytest.param(0.001, _eager_8mb, id="oversized-eager"),
        # Rejection is driven by the peak footprint, so it needs genuinely large chunks: a
        # single 8 GB chunk cannot be streamed around.
        pytest.param(
            1,
            lambda: xr.Dataset(
                {"x": (("a", "b"), da.zeros((1_000_000, 1000), dtype="float64", chunks=(1_000_000, 1000)))}
            ),
            id="oversized-chunk",
        ),
    ],
)
def test_require_dataset_fits_rejects_what_does_not_fit(ceiling, ceiling_gb, make):
    ceiling(ceiling_gb)
    with pytest.raises(memory.MemoryLimitExceeded):
        memory.require_dataset_fits(make(), what="too big")


def test_a_streaming_job_is_no_longer_rejected(ceiling):
    ceiling(1)
    # 100 GB total, 8 MB chunks: peaks well under the 1 GB ceiling.
    ds = xr.Dataset({"x": (("a", "b"), da.zeros((12_500_000, 1000), dtype="float64", chunks=(1000, 1000)))})
    assert memory.require_dataset_fits(ds, what="streaming") < 1.0


def test_require_memory_passes_when_it_fits(ceiling):
    ceiling(100)
    memory.require_memory(1.0, what="small job")


def test_require_memory_raises_when_too_big(ceiling):
    ceiling(1)
    with pytest.raises(memory.MemoryLimitExceeded, match="needs ~50.0 GB"):
        memory.require_memory(50.0, what="huge job")


def test_memory_guard_reports_peak_and_allows_normal_work():
    with memory.memory_guard(ceiling_gb=1e6, what="test", interval=0.01) as usage:
        _ = [0] * 1000
    assert usage.peak_gb > 0
    assert usage.final_gb > 0


def test_memory_guard_raises_after_consecutive_breaches():
    # A ceiling of zero is breached by every sample.
    with pytest.raises(memory.MemoryLimitExceeded, match="memory ceiling"):
        with memory.memory_guard(ceiling_gb=0.0, what="always over", interval=0.01, strikes=2):
            time.sleep(0.2)


def test_memory_guard_does_not_mask_the_body_exception():
    with pytest.raises(ValueError, match="from the body"):
        with memory.memory_guard(ceiling_gb=0.0, what="failing", interval=0.01, strikes=1):
            raise ValueError("from the body")


def test_configure_malloc_arenas_sets_the_variable(monkeypatch):
    monkeypatch.delenv("MALLOC_ARENA_MAX", raising=False)
    memory.configure_malloc_arenas(2)
    assert os.environ["MALLOC_ARENA_MAX"] == "2"


@pytest.mark.parametrize(("needed_gb", "expected"), [(0.001, True), (1e9, False)])
def test_wait_for_memory_reports_whether_it_became_available(needed_gb, expected):
    assert memory.wait_for_memory(needed_gb, timeout=0) is expected
