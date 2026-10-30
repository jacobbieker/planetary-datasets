"""Repair renamed stores whose group-level ``coordinates`` attribute kept old names.

    python scripts/repair_group_coordinates.py [--dry-run] [PREFIX ...]

The naming pass renamed arrays and their own ``coordinates`` attributes but not the
dataset-level one xarray writes for coordinates no variable references, so those
coordinates (``Freq``, ``Plevel`` in the MiRS stores) came back as data variables and the
writers refused every partition as a variable mismatch. Metadata only, one commit per
store that needs it.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys

from planetary_datasets.config import get_config

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "rename_store_variables", HERE / "rename_store_variables.py"
)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


def tokens_for(prefix: str) -> dict:
    names, dims, _ = tool.plan_store(prefix)
    return {**dims, **{o.rsplit("/", 1)[-1]: n.rsplit("/", 1)[-1] for o, n in names.items()}}


def repair(prefix: str, dry_run: bool = False) -> int:
    import icechunk
    import zarr

    tokens = tokens_for(prefix)
    if not tokens:
        return 0
    repo = get_config().icechunk_repo(prefix)
    if dry_run:
        group = zarr.open_group(repo.readonly_session("main").store, mode="r")
        return sum(
            1
            for node in tool._groups(group)
            if node.attrs.get("coordinates")
            and tool._rename_tokens(node.attrs["coordinates"], tokens) != node.attrs["coordinates"]
        )
    session = repo.writable_session("main")
    changed = tool.rename_group_coordinates(zarr.open_group(session.store, mode="a"), tokens)
    if changed:
        session.commit(
            "naming: group-level coordinates renamed", rebase_with=icechunk.ConflictDetector()
        )
    return changed


def main(argv=None) -> int:
    from concurrent.futures import ThreadPoolExecutor

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("prefixes", nargs="*")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    prefixes = args.prefixes or sorted(
        set(tool._table(str(tool.STORE_NAMES))) | set(tool._table(str(tool.STORE_DIMS)))
    )

    def one(prefix):
        try:
            return prefix, repair(prefix, args.dry_run), None
        except Exception as exc:  # noqa: BLE001 - report and carry on
            return prefix, 0, f"{type(exc).__name__}: {str(exc)[:120]}"

    with ThreadPoolExecutor(16) as pool:
        for prefix, changed, error in pool.map(one, prefixes):
            if error:
                print(f"ERR {prefix}: {error}", flush=True)
            elif changed:
                print(
                    f"{'would fix' if args.dry_run else 'fixed'} {prefix}: {changed} group(s)",
                    flush=True,
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
