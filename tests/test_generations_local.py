"""Generation listing works against local stores and never creates a store."""

from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd
import xarray as xr

from planetary_datasets.common.generations import (
    existing_generations,
    prefix_for_date,
    store_exists,
)
from planetary_datasets.common.store import write_to_icechunk

BASE = "bkr/test/gen.icechunk"


def _write(config, prefix):
    ds = xr.Dataset(
        {"v": (("time",), np.zeros(1, "float32"))},
        coords={"time": pd.DatetimeIndex(["2026-01-01"])},
    )
    write_to_icechunk(config.icechunk_repo(prefix), ds, append_dim="time", message="x")


def test_local_listing_ignores_s3_credentials(local_config):
    """A config holding AWS keys passed them to the local filesystem, which refused them."""
    config = dataclasses.replace(
        local_config,
        credentials=dataclasses.replace(
            local_config.credentials, aws_access_key_id="k", aws_secret_access_key="s"
        ),
    )
    dated = prefix_for_date(BASE, pd.Timestamp("2026-07-22"))
    _write(config, BASE)
    _write(config, dated)
    assert existing_generations(config, BASE) == [BASE, dated]


def test_store_exists_never_creates_a_store(local_config):
    stores = local_config.local_store_path("bkr/test")
    assert store_exists(local_config, "bkr/test/absent.icechunk") is False
    assert not (stores / "absent.icechunk").exists()
    _write(local_config, BASE)
    assert store_exists(local_config, BASE) is True
