"""Planetary datasets: downloaders and processors for open environmental data.

Each dataset is a :class:`~planetary_datasets.base.BaseProvider` subclass under
``planetary_datasets.providers``. Providers are driven either by the Dagster assets in
``dags/`` or directly::

    from planetary_datasets.providers.hawaii_nam import HawaiiNAMProvider

    HawaiiNAMProvider().run_partition(pd.Timestamp("2026-01-01T00:00"))

Configuration comes from the environment and an optional ``.env`` file; see
:mod:`planetary_datasets.config` and ``.env.example``.
"""

from planetary_datasets.base import BaseProvider
from planetary_datasets.config import Config, MissingCredential, get_config, load_config
from planetary_datasets.memory import (
    MemoryLimitExceeded,
    available_memory_gb,
    estimate_dataset_gb,
    memory_guard,
    require_memory,
    total_memory_gb,
)

__all__ = [
    "BaseProvider",
    "Config",
    "MemoryLimitExceeded",
    "MissingCredential",
    "available_memory_gb",
    "estimate_dataset_gb",
    "get_config",
    "load_config",
    "memory_guard",
    "require_memory",
    "total_memory_gb",
]

__version__ = "0.0.1"
