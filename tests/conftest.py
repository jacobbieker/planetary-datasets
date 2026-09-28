"""Shared fixtures. Every test runs against a local store, never the network."""

from __future__ import annotations

import pytest

from planetary_datasets import config as config_module

CREDENTIAL_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_PROFILE",
    "AWS_REGION",
    "ICECHUNK_BUCKET",
    "ICECHUNK_PREFIX",
    "ICECHUNK_LOCAL_PATH",
    "MEMORY_FRACTION",
    "MEMORY_CEILING_GB",
    "PLANETARY_DATASETS_DATA_DIR",
    "PLANETARY_DATASETS_SCRATCH_DIR",
    # Archive roots the radar providers read from; an operator who has these set in their
    # shell would otherwise see the default-path tests fail.
    "UK_RADAR_ARCHIVE_DIR",
    "FMI_RADAR_ARCHIVE_DIR",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Isolate each test from the developer's real environment and .env file."""
    for var in CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)
    config_module.reset_config_cache()
    yield
    config_module.reset_config_cache()


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """A Config writing every store under tmp_path."""
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    config_module.reset_config_cache()
    return config_module.load_config(env_file=tmp_path / "nonexistent.env")


@pytest.fixture
def sample_dataset():
    """A small dataset with a time dimension, suitable for store round-trips."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    return xr.Dataset(
        {
            "temperature": (("time", "latitude", "longitude"), np.zeros((1, 4, 5), dtype="float32")),
            "pressure": (("time", "latitude", "longitude"), np.ones((1, 4, 5), dtype="float32")),
        },
        coords={
            "time": pd.DatetimeIndex(["2026-01-01T00:00"]),
            "latitude": np.linspace(-10, 10, 4),
            "longitude": np.linspace(0, 20, 5),
        },
    )
