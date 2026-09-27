"""Memory estimation and guards."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from planetary_datasets import memory


def test_host_memory_is_reported():
    assert memory.total_memory_gb() > 0
    assert 0 < memory.available_memory_gb() <= memory.total_memory_gb()


def test_process_tree_rss_is_positive():
    assert memory.process_tree_rss_gb() > 0


def test_estimate_matches_a_known_array_size():
    # 1000 x 1000 float64 is 8 MB.
    ds = xr.Dataset({"x": (("a", "b"), np.zeros((1000, 1000), dtype="float64"))})
    assert memory.estimate_dataset_gb(ds) == pytest.approx(8e6 / 1024**3, rel=0.01)


def test_estimate_does_not_load_a_lazy_dataset():
    dask = pytest.importorskip("dask.array")
    # 1e11 bytes lazy, i.e. 93.1 GiB. Estimating it must not try to allocate it.
    ds = xr.Dataset({"x": (("a", "b"), dask.zeros((100_000, 125_000), dtype="float64", chunks=1000))})
    assert memory.estimate_dataset_gb(ds) == pytest.approx(1e11 / 1024**3, rel=0.01)


def test_largest_chunk_is_measured_not_the_total():
    dask = pytest.importorskip("dask.array")
    # 100 chunks of 8 MB each: total 800 MB, largest chunk 8 MB.
    ds = xr.Dataset({"x": (("a", "b"), dask.zeros((100_000, 1000), dtype="float64", chunks=(1000, 1000)))})
    assert memory.largest_chunk_gb(ds) == pytest.approx(8e6 / 1024**3, rel=0.01)
    assert memory.estimate_dataset_gb(ds) == pytest.approx(8e8 / 1024**3, rel=0.01)


def test_peak_estimate_uses_chunks_for_a_lazy_dataset():
    """Regression: sizing a long lazy concat by its total rejected small streaming jobs."""
    dask = pytest.importorskip("dask.array")
    ds = xr.Dataset({"x": (("a", "b"), dask.zeros((100_000, 1000), dtype="float64", chunks=(1000, 1000)))})
    peak = memory.estimate_peak_gb(ds, concurrency=4)
    assert peak == pytest.approx(4 * 8e6 / 1024**3, rel=0.01)
    assert peak < memory.estimate_dataset_gb(ds)


def test_an_eager_variable_counts_as_one_whole_chunk():
    """Regression: skipping unchunked variables sized a mixed dataset at nearly zero."""
    dask = pytest.importorskip("dask.array")
    ds = xr.Dataset(
        {
            "small_chunked": (("a", "b"), dask.zeros((100, 100), dtype="float64", chunks=(10, 10))),
            "big_eager": (("c", "d"), np.zeros((2000, 2000), dtype="float64")),
        }
    )
    # The eager variable is 32 MB and fully resident; the chunked one's chunk is 800 B.
    assert memory.largest_chunk_gb(ds) == pytest.approx(32e6 / 1024**3, rel=0.01)


def test_a_mixed_dataset_is_not_waved_through(monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "0.001")
    from planetary_datasets import config

    config.reset_config_cache()
    dask = pytest.importorskip("dask.array")
    ds = xr.Dataset(
        {
            "small_chunked": (("a", "b"), dask.zeros((100, 100), dtype="float64", chunks=(10, 10))),
            "big_eager": (("c", "d"), np.zeros((2000, 2000), dtype="float64")),
        }
    )
    with pytest.raises(memory.MemoryLimitExceeded):
        memory.require_dataset_fits(ds, what="mixed")


def test_peak_estimate_falls_back_to_total_when_not_chunked():
    ds = xr.Dataset({"x": (("a", "b"), np.zeros((1000, 1000), dtype="float64"))})
    assert memory.estimate_peak_gb(ds) == pytest.approx(memory.estimate_dataset_gb(ds))


def test_peak_never_exceeds_the_total():
    dask = pytest.importorskip("dask.array")
    # One single chunk: 4x concurrency must not inflate past the real total.
    ds = xr.Dataset({"x": (("a",), dask.zeros(1000, dtype="float64", chunks=1000))})
    assert memory.estimate_peak_gb(ds, concurrency=4) == pytest.approx(memory.estimate_dataset_gb(ds))


def test_a_streaming_job_is_no_longer_rejected(monkeypatch):
    dask = pytest.importorskip("dask.array")
    monkeypatch.setenv("MEMORY_CEILING_GB", "1")
    from planetary_datasets import config

    config.reset_config_cache()
    # 100 GB total, 8 MB chunks: peaks well under the 1 GB ceiling.
    ds = xr.Dataset(
        {"x": (("a", "b"), dask.zeros((12_500_000, 1000), dtype="float64", chunks=(1000, 1000)))}
    )
    assert memory.require_dataset_fits(ds, what="streaming") < 1.0


def test_require_memory_passes_when_it_fits(monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "100")
    from planetary_datasets import config

    config.reset_config_cache()
    memory.require_memory(1.0, what="small job")


def test_require_memory_raises_when_too_big(monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "1")
    from planetary_datasets import config

    config.reset_config_cache()
    with pytest.raises(memory.MemoryLimitExceeded, match="needs ~50.0 GB"):
        memory.require_memory(50.0, what="huge job")


def test_require_dataset_fits_rejects_oversized_chunks(monkeypatch):
    """Rejection is driven by the peak footprint, so it needs genuinely large chunks."""
    dask = pytest.importorskip("dask.array")
    monkeypatch.setenv("MEMORY_CEILING_GB", "1")
    from planetary_datasets import config

    config.reset_config_cache()
    # A single 8 GB chunk cannot be streamed around.
    ds = xr.Dataset({"x": (("a", "b"), dask.zeros((1_000_000, 1000), dtype="float64", chunks=(1_000_000, 1000)))})
    with pytest.raises(memory.MemoryLimitExceeded):
        memory.require_dataset_fits(ds, what="oversized chunk")


def test_require_dataset_fits_rejects_an_oversized_eager_dataset(monkeypatch):
    monkeypatch.setenv("MEMORY_CEILING_GB", "0.001")
    from planetary_datasets import config

    config.reset_config_cache()
    # Already resident, so the total is the footprint.
    ds = xr.Dataset({"x": (("a", "b"), np.zeros((1000, 1000), dtype="float64"))})
    with pytest.raises(memory.MemoryLimitExceeded):
        memory.require_dataset_fits(ds, what="oversized eager")


def test_memory_guard_reports_peak_and_allows_normal_work():
    with memory.memory_guard(ceiling_gb=1e6, what="test", interval=0.01) as usage:
        _ = [0] * 1000
    assert usage.peak_gb > 0
    assert usage.final_gb > 0


def test_memory_guard_raises_after_consecutive_breaches():
    # A ceiling of zero is breached by every sample.
    with pytest.raises(memory.MemoryLimitExceeded, match="memory ceiling"):
        with memory.memory_guard(ceiling_gb=0.0, what="always over", interval=0.01, strikes=2):
            import time

            time.sleep(0.2)


def test_memory_guard_does_not_mask_the_body_exception():
    with pytest.raises(ValueError, match="from the body"):
        with memory.memory_guard(ceiling_gb=0.0, what="failing", interval=0.01, strikes=1):
            raise ValueError("from the body")


def test_configure_malloc_arenas_sets_the_variable(monkeypatch):
    monkeypatch.delenv("MALLOC_ARENA_MAX", raising=False)
    memory.configure_malloc_arenas(2)
    import os

    assert os.environ["MALLOC_ARENA_MAX"] == "2"


def test_wait_for_memory_returns_immediately_when_available():
    assert memory.wait_for_memory(0.001, timeout=0) is True


def test_wait_for_memory_times_out_when_impossible():
    assert memory.wait_for_memory(1e9, timeout=0) is False
