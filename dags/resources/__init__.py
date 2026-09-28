"""Dagster resources exposing the shared configuration and memory helpers.

Assets can keep calling :func:`planetary_datasets.config.get_config` directly — these
resources exist so that configuration and memory limits are *visible* in the Dagster UI
and overridable per code location, rather than being invisible process-wide globals.

Usage from an asset::

    @dg.asset
    def my_asset(context, config: PlanetaryConfigResource, memory: MemoryResource):
        memory.require(8.0, what="my_asset")
        repo = config.icechunk_repo("bkr/dmi/x.icechunk")
"""

from __future__ import annotations

import contextlib
import pathlib
from typing import Any, Iterator

import dagster as dg

from planetary_datasets import memory as memory_module
from planetary_datasets.config import Config, get_config, load_config


class PlanetaryConfigResource(dg.ConfigurableResource):
    """Wraps :class:`planetary_datasets.config.Config` as a Dagster resource.

    Attributes:
        env_file: Optional dotenv file to load instead of the repository's ``.env``.
            Leave unset to use the process-wide configuration.
    """

    env_file: str | None = None

    @property
    def config(self) -> Config:
        """The resolved configuration."""
        if self.env_file:
            return load_config(env_file=self.env_file)
        return get_config()

    @property
    def data_dir(self) -> pathlib.Path:
        """Directory for persistent downloads."""
        return self.config.data_dir

    @property
    def scratch_dir(self) -> pathlib.Path:
        """Directory for temporary working files."""
        return self.config.scratch_dir

    @property
    def use_local_store(self) -> bool:
        """True when stores are written to the filesystem instead of S3."""
        return self.config.use_local_store

    def store_path(self, prefix: str) -> str:
        """Resolve a store prefix to an ``s3://`` URI or a local directory."""
        return self.config.store_path(prefix)

    def icechunk_repo(self, prefix: str):
        """Open or create the icechunk repository for a store prefix."""
        return self.config.icechunk_repo(prefix)

    def require(self, *names: str) -> tuple[str, ...]:
        """Return the named credentials, raising if any are unset."""
        return self.config.credentials.require(*names)


class MemoryResource(dg.ConfigurableResource):
    """Exposes the host memory budget and the guards built on it.

    Attributes:
        fraction: Override for the fraction of available memory a single job may use.
            Leave unset to use ``MEMORY_FRACTION`` from the configuration.
    """

    fraction: float | None = None

    def available_gb(self) -> float:
        """Memory currently free on the host, in GB."""
        return memory_module.available_memory_gb()

    def total_gb(self) -> float:
        """Total physical memory on the host, in GB."""
        return memory_module.total_memory_gb()

    def budget_gb(self) -> float:
        """The memory a single job is allowed to use, in GB."""
        return memory_module.memory_budget_gb(self.fraction)

    def require(self, needed_gb: float, what: str = "job") -> None:
        """Raise before starting work that will not fit in the budget."""
        budget = self.budget_gb()
        if needed_gb > budget:
            raise memory_module.MemoryLimitExceeded(
                f"{what} needs ~{needed_gb:.1f} GB but only {budget:.1f} GB is budgeted "
                f"({self.available_gb():.1f} GB available of {self.total_gb():.1f} GB total)."
            )

    def estimate_dataset_gb(self, ds: Any) -> float:
        """Estimate a lazy dataset's in-memory size in GB."""
        return memory_module.estimate_dataset_gb(ds)

    @contextlib.contextmanager
    def guard(self, ceiling_gb: float | None = None, what: str = "job") -> Iterator[Any]:
        """Watch memory while the block runs and abort past ``ceiling_gb``."""
        with memory_module.memory_guard(
            ceiling_gb=ceiling_gb if ceiling_gb is not None else self.budget_gb(),
            what=what,
        ) as usage:
            yield usage


__all__ = ["MemoryResource", "PlanetaryConfigResource"]
