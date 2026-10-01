"""Base provider interface.

A provider knows how to fetch the inputs for one partition of a dataset and turn them into
an :class:`xarray.Dataset`. Everything else — opening the store, skipping work that is
already done, choosing compression, appending safely, watching memory — is handled here so
each provider stays small.

Subclasses set :attr:`name`, :attr:`append_dim` and :attr:`store_prefix`, and implement
:meth:`fetch` and :meth:`process`. Dagster assets call :meth:`run_partition`.
"""

from __future__ import annotations

import contextlib
import pathlib
import tempfile
from abc import ABC, abstractmethod
from typing import Iterator, List

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.common.store import ALIGNMENT_COORDS, axis_is_sorted
from planetary_datasets.common.store import (
    bitround_dataset as _bitround_dataset,
)
from planetary_datasets.common.store import (
    missing_timesteps as _missing_timesteps,
)
from planetary_datasets.common.store import (
    sort_append_axis as _sort_append_axis,
)
from planetary_datasets.common.store import (
    write_to_icechunk as _write_to_icechunk,
)
from planetary_datasets.config import Config, get_config
from planetary_datasets.memory import memory_guard, require_dataset_fits
from planetary_datasets.providers._timestamps import to_naive_utc


class BaseProvider(ABC):
    """Abstract base provider used by the Dagster assets and the CLI.

    Attributes:
        name: Short identifier, used in logs and as the Dagster asset name.
        append_dim: Dimension new data is appended along, usually ``time`` or ``init_time``.
        store_prefix: Location of the store relative to the configured bucket, e.g.
            ``bkr/dmi/hawaii_nams.icechunk``. Resolved to S3 or a local directory by
            :class:`~planetary_datasets.config.Config`.
    """

    name: str
    append_dim: str = "time"
    store_prefix: str

    #: Guard the process step with a memory ceiling. Set False for providers that manage
    #: their own memory, such as the virtualized ingests.
    guard_memory: bool = True

    #: Coordinates that must match the store exactly before an append. Override to add the
    #: ones a dataset actually carries — ``step`` for a forecast, ``station`` for a station
    #: table, a projected ``x``/``y`` for a radar grid. Providers previously reimplemented
    #: :meth:`write_to_icechunk` solely to pass this through.
    alignment_coords: tuple[str, ...] = ALIGNMENT_COORDS

    def __init__(self, config: Config | None = None):
        self._config = config
        self._repo: icechunk.Repository | None = None

    @property
    def config(self) -> Config:
        """Configuration for this provider, defaulting to the process-wide config."""
        return self._config if self._config is not None else get_config()

    @property
    def store_path(self) -> str:
        """Fully resolved store location, for logging and diagnostics."""
        return self.config.store_path(self.store_prefix)

    @abstractmethod
    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Return input URIs or local paths for the given partition timestamp.

        Return an empty list when nothing is available for the partition; that is treated
        as "nothing to do", not as a failure.
        """

    @abstractmethod
    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Turn the fetched inputs into a dataset ready to write."""

    def get_icechunk_repo(self) -> icechunk.Repository:
        """Open or create the repository this provider writes to.

        The handle is cached: a backfill of one day can be ~96 partitions, and reopening
        the store for each one is pure overhead.
        """
        if self._repo is None:
            self._repo = self.config.icechunk_repo(self.store_prefix)
        return self._repo

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Return the timesteps in ``desired`` that are not yet stored."""
        return _missing_timesteps(
            self.get_icechunk_repo(), list(desired), append_dim=self.append_dim
        )

    def appendable(self, start: pd.Timestamp) -> bool:
        """True when the store would still accept a partition starting at ``start``.

        Partitions may arrive in any order, so the only one refused is one already stored.
        This used to also refuse anything at or before the store's last step, because the
        writer could only append in time order; it now appends out of order and the axis
        is sorted afterwards by the store's reorder asset, so an hour behind the end is
        ordinary work rather than a permanent gap. Staging pipelines check this before
        downloading a partition.
        """
        return bool(self.missing_timesteps(pd.DatetimeIndex([to_naive_utc(start)])))

    def axis_sorted(self) -> bool:
        """True when the store's append axis is in order.

        False after an out-of-order write — a backfill partition that ran behind one
        already stored, or a provider revisiting an hour the upstream archive was still
        filling in. The data is correct either way; until :meth:`sort_axis` runs, a
        ``.sel`` over a *slice* of the store is unreliable.
        """
        return axis_is_sorted(self.get_icechunk_repo(), append_dim=self.append_dim)

    def sort_axis(self) -> bool:
        """Put the store's append axis back in order. Returns True if it changed.

        Remaps chunks onto their sorted positions by rewriting the manifest, so it costs
        the same whether the store holds a day or a decade.

        **Nothing else may be writing to the store while this runs.** It moves chunks, so
        a concurrent writer that had already looked up a position would write to the wrong
        one. It is deliberately not called from :meth:`run_partition`: in Dagster it is the
        separate ``<name>-reorder`` asset, which shares the ingest's concurrency pool so
        the two cannot overlap, and which a human materialises when a backfill is done.
        """
        return _sort_append_axis(self.get_icechunk_repo(), append_dim=self.append_dim)

    #: Mantissa bits to keep in floating point fields, or None to store them exactly. See
    #: :func:`~planetary_datasets.common.store.bitround`. Declared per provider because
    #: what is negligible depends on the quantity: 12 bits is a part in 4096, which is
    #: below model precision for most geophysical fields and a large saving on disk.
    keepbits: int | None = None

    #: Substrings of variable names that are never rounded, whatever :attr:`keepbits` says.
    #: For quantities where the loss is not negligible however small it looks: a wind
    #: component is differenced with its partner to get a direction, so an error that is
    #: tiny next to the wind speed is not tiny next to the difference, and a pressure sits
    #: on a large offset where a *relative* precision of one in 4096 is tens of pascals.
    keepbits_exact: tuple[str, ...] = ()

    #: Explicit per-variable overrides, by exact name. Consulted before :attr:`keepbits`.
    keepbits_by_variable: dict[str, int | None] = {}

    def keepbits_for(self, variable: str) -> int | None:
        """Mantissa bits to keep for one variable, or None to store it exactly."""
        if variable in self.keepbits_by_variable:
            return self.keepbits_by_variable[variable]
        if any(pattern in variable for pattern in self.keepbits_exact):
            return None
        return self.keepbits

    def prepare_for_write(self, processed: xr.Dataset) -> xr.Dataset:
        """Last chance to reshape a dataset before it is written.

        Applies :meth:`keepbits_for` per variable when rounding is configured. Override to
        rechunk or reorder; call ``super().prepare_for_write(...)`` from the override to
        keep the rounding.
        """
        if self.keepbits is not None or self.keepbits_by_variable:
            processed = _bitround_dataset(processed, self.keepbits_for)
        return processed

    def write_to_icechunk(self, repo: icechunk.Repository, processed: xr.Dataset) -> bool:
        """Write a processed dataset.

        Most providers should set :attr:`alignment_coords` or override
        :meth:`prepare_for_write` rather than replacing this.
        """
        return _write_to_icechunk(
            repo,
            self.prepare_for_write(processed),
            append_dim=self.append_dim,
            # atleast_1d: a provider may hand back a scalar append coordinate, which the
            # writer itself tolerates.
            message=f"{self.name}: {np.atleast_1d(processed[self.append_dim].values)[0]}",
            alignment_coords=self.alignment_coords,
        )

    @staticmethod
    @contextlib.contextmanager
    def local_tempdir() -> Iterator[pathlib.Path]:
        """Yield a temporary directory that is removed on exit.

        This is a context manager rather than a plain function: returning the path of a
        bare ``TemporaryDirectory`` lets it be finalised as soon as the reference goes out
        of scope, deleting the directory while the caller still expects it to exist.
        """
        with tempfile.TemporaryDirectory(prefix="planetary-datasets-") as td:
            yield pathlib.Path(td)

    def run_partition(self, it: pd.Timestamp, check_present: bool = True) -> bool:
        """Fetch, process and write one partition.

        Returns True if data was written, False if there was nothing to do. This is the
        method Dagster assets call.

        Args:
            it: Partition timestamp.
            check_present: Skip the "already stored?" query. :meth:`run_range` sets this
                False because it has already filtered the timestamps.
        """
        repo = self.get_icechunk_repo()

        if check_present and not self.missing_timesteps(pd.DatetimeIndex([it])):
            logger.debug(f"{self.name}: {it} already in {self.describe_store()}, skipping")
            return False

        with self.local_tempdir() as temp_dir:
            input_files = self.fetch(it, temp_dir=temp_dir)
            if not input_files:
                logger.debug(f"{self.name}: no input files for {it}, skipping")
                return False

            logger.info(f"{self.name}: processing {len(input_files)} file(s) for {it}")
            if self.guard_memory:
                # memory_guard raises on exit, so the commit stays outside it: a breach
                # then prevents the write instead of following it.
                with memory_guard(what=f"{self.name} {it}"):
                    processed = self.process(input_files, it, temp_dir=temp_dir)
                    require_dataset_fits(processed, what=f"{self.name} {it}")
            else:
                processed = self.process(input_files, it, temp_dir=temp_dir)

            return self.write_to_icechunk(self.store_for(processed, repo), processed)

    def describe_store(self) -> str:
        """Where this provider's data lives, for a log line.

        Its own store, for most providers. Overridden where "the store" is not one place:
        a provider that picks between generations has to name the ones it actually looked
        in, or a skip reads as though it came from a store that was never consulted.
        """
        return self.store_path

    def store_for(self, processed: xr.Dataset, repo: icechunk.Repository):
        """The repository ``processed`` should be written to.

        A hook, taken after the partition has been processed rather than before, so that a
        provider may choose its store from the *shape* of what it is about to write. The
        default ignores ``processed`` and keeps the repository opened at the top of
        :meth:`run_partition`; see
        :class:`~planetary_datasets.common.generations.GenerationalStoreMixin`, which uses
        this to roll a store forward when an upstream model is upgraded.
        """
        return repo

    def run_range(self, timestamps: pd.DatetimeIndex) -> int:
        """Run every missing partition in ``timestamps``. Returns the number written.

        A failure on one partition is logged and does not stop the rest, matching how the
        original scripts behaved over archive gaps.
        """
        written = 0
        # Filter once here rather than re-reading the time coordinate per partition.
        for it in self.missing_timesteps(timestamps):
            try:
                if self.run_partition(it, check_present=False):
                    written += 1
            except Exception as exc:  # noqa: BLE001 - one bad partition must not stop a backfill
                logger.exception(f"{self.name}: partition {it} failed: {exc}")
        return written
