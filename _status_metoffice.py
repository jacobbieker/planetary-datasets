"""Report how far each Met Office store has been filled.

    python _status_metoffice.py [name ...]

For every generation of every named store, print the append-dimension range and how many
steps are in it. Read-only. Run from the repository root so `planetary_datasets` resolves
here rather than to the editable install's worktree.
"""

import sys

import pandas as pd
import xarray as xr

from planetary_datasets.common.generations import existing_generations
from planetary_datasets.providers.metoffice import PROVIDERS
from planetary_datasets.providers.observations.uk_marine import UKMarineObservationsProvider

ALL = {**PROVIDERS, "uk_marine": UKMarineObservationsProvider}


def report(name: str) -> None:
    provider = ALL[name]()
    base = getattr(provider, "base_store_prefix", provider.store_prefix)
    prefixes = existing_generations(provider.config, base) or [base]
    print(f"\n=== {name}  ({base})")
    for prefix in prefixes:
        try:
            repo = provider.config.icechunk_repo(prefix)
            session = repo.readonly_session("main")
            ds = xr.open_zarr(session.store, consolidated=False)
        except Exception as exc:  # noqa: BLE001 - a missing or damaged store is a finding
            print(f"  {prefix:70s} unreadable: {type(exc).__name__}: {str(exc)[:90]}")
            continue
        dim = provider.append_dim
        if dim not in ds.coords:
            print(f"  {prefix:70s} no {dim!r} coordinate; dims={dict(ds.sizes)}")
            continue
        times = pd.DatetimeIndex(ds[dim].values)
        sorted_note = "" if times.is_monotonic_increasing else "  UNSORTED"
        print(f"  {prefix:70s} {len(times):7d} steps  {times.min()} -> {times.max()}{sorted_note}")


if __name__ == "__main__":
    for name in sys.argv[1:] or list(ALL):
        try:
            report(name)
        except Exception as exc:  # noqa: BLE001
            print(f"\n=== {name}  FAILED {type(exc).__name__}: {str(exc)[:160]}")
