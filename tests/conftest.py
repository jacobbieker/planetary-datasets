"""Shared fixtures. Every test runs against a local store, never the network."""

from __future__ import annotations

import pathlib
import sys

import pytest

# ``dags`` is not an installed package; the tests import it from the checkout.
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from planetary_datasets import config as config_module  # noqa: E402

#: Everything the code reads from the environment that changes what a test sees:
#: destinations, credentials, archive and staging roots, image names, tuning knobs. A
#: developer with any of these set in their shell would otherwise get different results.
ISOLATED_ENV_VARS = [
    # Stores and config.
    "ICECHUNK_BUCKET",
    "ICECHUNK_PREFIX",
    "ICECHUNK_LOCAL_PATH",
    "ICECHUNK_ENDPOINT_URL",
    "ICECHUNK_FORCE_PATH_STYLE",
    "ICECHUNK_ALLOW_HTTP",
    "MEMORY_FRACTION",
    "MEMORY_CEILING_GB",
    "PLANETARY_DATASETS_DATA_DIR",
    "PLANETARY_DATASETS_SCRATCH_DIR",
    "HF_REPO_ID",
    # Credentials.
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_REQUEST_CHECKSUM_CALCULATION",
    "CDSAPI_KEY",
    "CDSAPI_URL",
    "CDSAPI_RC",
    "COPERNICUSMARINE_SERVICE_USERNAME",
    "COPERNICUSMARINE_SERVICE_PASSWORD",
    "DESTINE_PAT",
    "EARTHDATA_USERNAME",
    "EARTHDATA_PASSWORD",
    "ECMWF_API_KEY",
    "ECMWF_API_EMAIL",
    "ECMWF_API_URL",
    "EUMETSAT_CONSUMER_KEY",
    "EUMETSAT_CONSUMER_SECRET",
    "GPM_PPS_USERNAME",
    "GPM_PPS_PASSWORD",
    "HF_TOKEN",
    "NREL_API_KEY",
    "NREL_EMAIL",
    "PC_SDK_SUBSCRIPTION_KEY",
    "VIRES_TOKEN",
    # Archive and staging roots, private buckets, and the images that fill them.
    "UK_RADAR_ARCHIVE_DIR",
    "FMI_RADAR_ARCHIVE_DIR",
    "OPERA_ARCHIVE_DIR",
    "E2S_OBS_ARCHIVE_DIR",
    "KENDA_ARCHIVE_DIR",
    "AMDAR_BUFR_DIR",
    "AMDAR_PB2NC_CONFIG",
    "PB2NC_BINARY",
    "IGRA_STATION_DIR",
    "MEPS_ANDOYA_BUCKET",
    "IFS_REGRID_BUCKET",
    "IFS_REGRID_REGION",
    "GOES_SOURCE_REGION",
    "GOES_STORE_ROOT",
    "GOES_VIRTUAL_END_DATE",
    "EARTH2STUDIO_CACHE",
    "EARTH2STUDIO_IMAGE",
    "METEOSWISS_KENDA_IMAGE",
    "DAGSTER_PIPES_CONTEXT",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Isolate each test from the developer's real environment and .env file."""
    for var in ISOLATED_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    config_module.reset_config_cache()
    yield
    config_module.reset_config_cache()


@pytest.fixture
def local_config(tmp_path, monkeypatch):
    """Point the process-wide config at ``tmp_path`` and return it.

    Stores go under ``tmp_path/stores`` and the data directory (archives, staging,
    rosters) under ``tmp_path/data``. ``REPO_ROOT`` is pointed at ``tmp_path`` too, so a
    developer's real ``.env`` cannot leak back in through ``load_dotenv``. The memory
    ceiling is generous so the memory guard never trips on a test dataset.
    """
    monkeypatch.setattr(config_module, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "stores"))
    monkeypatch.setenv("PLANETARY_DATASETS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("MEMORY_CEILING_GB", "512")
    config_module.reset_config_cache()
    return config_module.get_config()


@pytest.fixture
def load_env_config(tmp_path, monkeypatch):
    """Load a config from the given environment, with no ``.env`` file behind it.

    For tests of the configuration itself: ``load_env_config(ICECHUNK_PREFIX="x")``.
    """

    def _load(**env):
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return config_module.load_config(env_file=tmp_path / "absent.env")

    return _load


class FakeDockerClient:
    """Stands in for ``PipesDockerClient``: records each run, reports a materialisation."""

    def __init__(self):
        self.calls: list[dict] = []

    def run(self, **kwargs):
        import dagster as dg

        self.calls.append(kwargs)

        class Invocation:
            def get_materialize_result(self):
                return dg.MaterializeResult(metadata={"fake": True})

        return Invocation()


@pytest.fixture
def fake_docker_client():
    """A :class:`FakeDockerClient` to pass as the ``pipes_docker_client`` resource."""
    return FakeDockerClient()


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
