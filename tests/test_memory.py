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


def test_require_dataset_fits_rejects_an_oversized_dataset(monkeypatch):
    dask = pytest.importorskip("dask.array")
    monkeypatch.setenv("MEMORY_CEILING_GB", "1")
    from planetary_datasets import config

    config.reset_config_cache()
    ds = xr.Dataset({"x": (("a", "b"), dask.zeros((100_000, 125_000), dtype="float64", chunks=1000))})
    with pytest.raises(memory.MemoryLimitExceeded):
        memory.require_dataset_fits(ds, what="oversized")


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
