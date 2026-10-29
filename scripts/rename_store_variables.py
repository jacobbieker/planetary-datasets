"""Rename the variables and dimensions of existing stores, and declare ERA5 units.

    python scripts/rename_store_variables.py [--dry-run] [--workers N] [PREFIX ...]

Applies ``planetary_datasets/naming/store_{names,dims,units}.json`` to each store (all of
them, or the prefixes given), metadata only:

1. One commit rewrites every array's ``zarr.json``: dimensions renamed, ``source_name``
   recorded, a unit conversion declared as ``scale_factor``/``add_offset``/``units``
   (composed with any the variable already has), and ``coordinates`` attributes updated.
2. One rearrange commit moves each array to its new name, through a temporary name so
   that swaps and chains of renames never collide.

No chunk is read or written. A store already renamed (its root attribute
``naming_applied`` set) is skipped, so the script can be re-run after an interruption.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor

from loguru import logger

from planetary_datasets.common.naming import (
    SOURCE_NAME_ATTR,
    STORE_DIMS,
    STORE_NAMES,
    STORE_UNITS,
    _rename_tokens,
    _table,
)
from planetary_datasets.config import get_config

APPLIED_ATTR = "naming_applied"
TEMPORARY = ".__renaming"


def _paths(group, prefix=""):
    for name, array in group.arrays():
        yield prefix + name, array
    for name, sub in group.groups():
        yield from _paths(sub, prefix + name + "/")


def plan_store(prefix: str) -> tuple[dict, dict, dict]:
    names = _table(str(STORE_NAMES)).get(prefix, {})
    dims = _table(str(STORE_DIMS)).get(prefix, {})
    units = _table(str(STORE_UNITS)).get(prefix, {})
    return names, dims, units


def rename_store(prefix: str, dry_run: bool = False, upgrade_v1: bool = False) -> str:
    """Rename one store. Returns a one-line summary."""
    import zarr
    from zarr.core.buffer import cpu, default_buffer_prototype
    from zarr.core.sync import sync

    names, dims, units = plan_store(prefix)
    if not (names or dims or units):
        return f"{prefix}: nothing to do"
    names = dict(names)
    repo = get_config().icechunk_repo(prefix)
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    if root.attrs.get(APPLIED_ATTR):
        return _finish_moves(repo, prefix, names, dict(_paths(root)), dry_run)
    import icechunk

    # Moving arrays needs the version 2 format; checked before anything is written, so a
    # store is never left with its metadata renamed and its arrays not.
    if repo.spec_version == icechunk.SpecVersion.v1:
        if upgrade_v1:
            if dry_run:
                return f"{prefix}: would upgrade to format 2, then rename"
            repo = icechunk.upgrade_icechunk_repository(repo, dry_run=False)
        elif names or dims:
            return f"{prefix}: SKIPPED, icechunk format 1 cannot move arrays (--upgrade-v1)"
    arrays = dict(_paths(root))
    # A coordinate named for a renamed dimension goes with it, in any group.
    for path in arrays:
        group, _, leaf = path.rpartition("/")
        if leaf in dims and path not in names:
            names[path] = f"{group}/{dims[leaf]}" if group else dims[leaf]
    # The new name of each array path, and of any dimension or coordinate token.
    tokens = {**dims, **{o.rsplit("/", 1)[-1]: n.rsplit("/", 1)[-1] for o, n in names.items()}}
    if dry_run:
        return (
            f"{prefix}: would rename {len(names)} variable(s), {len(dims)} dimension(s), "
            f"convert {len(units)}"
        )

    session = repo.writable_session("main")
    store = session.store
    for path in arrays:
        key = f"{path}/zarr.json"
        meta = json.loads(sync(store.get(key, prototype=default_buffer_prototype())).to_bytes())
        changed = False
        if meta.get("dimension_names"):
            renamed = [dims.get(d, d) for d in meta["dimension_names"]]
            changed |= renamed != meta["dimension_names"]
            meta["dimension_names"] = renamed
        attributes = meta.setdefault("attributes", {})
        new = names.get(path, path)
        if new != path:
            attributes.setdefault(SOURCE_NAME_ATTR, path.rsplit("/", 1)[-1])
            changed = True
        if "coordinates" in attributes:
            coordinates = _rename_tokens(attributes["coordinates"], tokens)
            changed |= coordinates != attributes["coordinates"]
            attributes["coordinates"] = coordinates
        conversion = units.get(new)
        if conversion:
            scale, offset = float(conversion["scale_factor"]), float(conversion["add_offset"])
            old_scale = float(attributes.get("scale_factor", 1.0))
            old_offset = float(attributes.get("add_offset", 0.0))
            # decoded = (raw * old_scale + old_offset) * scale + offset
            attributes["scale_factor"] = old_scale * scale
            attributes["add_offset"] = old_offset * scale + offset
            attributes.setdefault(
                "source_units", attributes.get("units", conversion.get("from_units"))
            )
            attributes["units"] = conversion["units"]
            changed = True
        if changed:
            sync(store.set(key, cpu.Buffer.from_bytes(json.dumps(meta).encode())))
    group = zarr.open_group(store, mode="a")
    group.attrs[APPLIED_ATTR] = "store_names.json"
    session.commit(
        f"naming: dimensions, attributes and units ({len(dims)} dim(s), {len(units)} conversion(s))"
    )

    moves = _move(repo, names, arrays)
    return (
        f"{prefix}: renamed {len(moves)} variable(s), {len(dims)} dimension(s), "
        f"converted {len(units)}"
    )


def _move(repo, names: dict, arrays: dict) -> dict:
    """Move each array still at its source name to its new name, in one commit."""
    moves = {path: new for path, new in names.items() if path in arrays and path != new}
    if moves:
        rearrange = repo.rearrange_session("main")
        for path in moves:
            rearrange.move(f"/{path}", f"/{path}{TEMPORARY}")
        for path, new in moves.items():
            rearrange.move(f"/{path}{TEMPORARY}", f"/{new}")
        rearrange.commit(f"naming: {len(moves)} variable(s) renamed")
    return moves


def _finish_moves(repo, prefix: str, names: dict, arrays: dict, dry_run: bool) -> str:
    """Complete a rename interrupted between its metadata commit and its moves."""
    if any(path.endswith(TEMPORARY) for path in arrays):
        raise RuntimeError(f"{prefix}: arrays left at temporary names; inspect by hand")
    dims = _table(str(STORE_DIMS)).get(prefix, {})
    for path in arrays:
        group, _, leaf = path.rpartition("/")
        if leaf in dims and path not in names:
            names[path] = f"{group}/{dims[leaf]}" if group else dims[leaf]
    # An array still at its source path carries that path's own name as its source_name;
    # one moved there (a swap) carries the other's.
    pending = {
        path: new
        for path, new in names.items()
        if path in arrays
        and path != new
        and arrays[path].attrs.get(SOURCE_NAME_ATTR) == path.rsplit("/", 1)[-1]
    }
    if not pending:
        return f"{prefix}: already renamed"
    # The moves are one commit, so none of them happened; a target that exists and is not
    # itself being moved away means the store changed since and needs a person.
    if any(new in arrays and new not in pending for new in pending.values()):
        raise RuntimeError(f"{prefix}: some targets already exist; inspect by hand")
    if dry_run:
        return f"{prefix}: would finish {len(pending)} interrupted move(s)"
    _move(repo, pending, arrays)
    return f"{prefix}: finished {len(pending)} interrupted move(s)"


def check_store(prefix: str) -> str | None:
    """An error opening the renamed store with xarray, or None."""
    import xarray as xr
    import zarr

    repo = get_config().icechunk_repo(prefix)
    store = repo.readonly_session("main").store
    root = zarr.open_group(store, mode="r")
    groups = [""] + [p for p, _ in root.groups()]
    for group in groups:
        try:
            node = root[group] if group else root
            if not list(node.array_keys()):
                continue
            xr.open_zarr(
                store,
                group=group or None,
                consolidated=False,
                chunks=None,
                decode_times=True,
                # Names, dimensions and attributes decode without reading any data; indexing
                # a large observation store would load every row.
                create_default_indexes=False,
            ).close()
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            message = str(exc).splitlines()[0][:200]
            # Virtual coordinates need the source bucket's credentials to index.
            if "virtual" in message.lower() or "credentials" in message.lower():
                continue
            return f"{prefix}/{group}: {type(exc).__name__}: {message}"
    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("prefixes", nargs="*")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument(
        "--upgrade-v1",
        action="store_true",
        help="upgrade format-1 stores to format 2 first (readers then need icechunk >= 2)",
    )
    args = parser.parse_args(argv)
    prefixes = args.prefixes or sorted(
        set(_table(str(STORE_NAMES))) | set(_table(str(STORE_DIMS))) | set(_table(str(STORE_UNITS)))
    )
    failures = []

    def one(prefix):
        try:
            summary = (
                "checked"
                if args.check_only
                else rename_store(prefix, args.dry_run, args.upgrade_v1)
            )
            problem = None if args.dry_run else check_store(prefix)
            if problem:
                failures.append(problem)
                return f"{summary}; CHECK FAILED {problem}"
            return summary
        except Exception as exc:  # noqa: BLE001 - one store must not stop the rest
            failures.append(f"{prefix}: {exc}")
            logger.error(f"{prefix}: {traceback.format_exc()}")
            return f"{prefix}: FAILED {type(exc).__name__}: {str(exc)[:200]}"

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for line in pool.map(one, prefixes):
            print(line, flush=True)
    print(f"{len(prefixes)} store(s), {len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
