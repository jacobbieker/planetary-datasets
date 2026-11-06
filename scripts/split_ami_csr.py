"""Drop GK-2A's motion vectors from ``ami_csr`` once ``amv`` holds every one of them.

    python scripts/split_ami_csr.py            # verify only: counts, nothing written
    python scripts/split_ami_csr.py --apply    # verify, then rewrite ami_csr in one commit

Until the satellite target filtered ``ami_csr`` by data category, the group took every
object under the ``ami`` topic: KMA's clear-sky radiances (``A_INR*``, data category 101)
and its atmospheric motion vectors (``A_INV*``, template 310077). The vectors now go to
``amv``, and a re-run over the archived windows (ledger entry ``<window>:ami``) adds the
ones ``amv`` lacked. This script then rewrites ``ami_csr`` to hold only the other rows.

It refuses to write unless every AMV row it would drop has its ``observation_identity``
in ``amv``, checked against the same snapshot the rewrite starts from. The rewrite
compacts every array in place to the rows kept, removes arrays that hold no value for
them (the vectors' own elements) and trims a 2-D array's trailing columns that hold none,
keeping the group's attributes and ledger. It is one commit: a failure, or an append to
``ami_csr`` meanwhile (a conflict), leaves the group as it was. Run it between windows.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

import numpy as np
from loguru import logger

from planetary_datasets.config import get_config
from planetary_datasets.providers.observations import wis2_satellite as sat

SOURCE = "ami_csr"
DESTINATION = "amv"
#: ``bufr_template`` of the rows that leave ``ami_csr``.
AMV_TEMPLATES = ("310077",)
#: Rows read at once, in multiples of an array's chunk.
BLOCK_ROWS = 1_000_000


class NotVerified(RuntimeError):
    """An AMV row ``ami_csr`` holds is not in ``amv``; nothing was written."""


def _step(array) -> int:
    chunk = int(array.chunks[0]) if array.chunks else BLOCK_ROWS
    return max(chunk, BLOCK_ROWS // chunk * chunk)


def amv_mask(node) -> np.ndarray:
    """True for each row of ``ami_csr`` that is a motion vector (by its template)."""
    templates = node["bufr_template"]
    rows = templates.shape[0]
    mask = np.zeros(rows, dtype=bool)
    step = _step(templates)
    for start in range(0, rows, step):
        block = np.asarray(templates[start : start + step])
        for template in AMV_TEMPLATES:
            mask[start : start + block.size] |= block == template
    return mask


def _identities(node, mask: np.ndarray | None = None) -> np.ndarray:
    ids = node["observation_identity"]
    rows = ids.shape[0]
    out = []
    step = _step(ids)
    for start in range(0, rows, step):
        block = np.asarray(ids[start : start + step], dtype="int64")
        out.append(block if mask is None else block[mask[start : start + block.size]])
    return np.concatenate(out) if out else np.zeros(0, dtype="int64")


def verify(store) -> dict[str, Any]:
    """Counts of ``ami_csr``'s rows and of its AMV rows missing from ``amv``."""
    import zarr

    source = zarr.open_group(store, path=SOURCE, mode="r")
    destination = zarr.open_group(store, path=DESTINATION, mode="r")
    mask = amv_mask(source)
    wanted = _identities(source, mask)
    held = _identities(destination)
    held.sort()
    if held.size:
        present = held[np.searchsorted(held, wanted).clip(max=held.size - 1)] == wanted
    else:
        present = np.zeros(wanted.size, dtype=bool)
    absent = np.unique(wanted[~present])
    return {
        "rows": int(mask.size),
        "amv_rows": int(mask.sum()),
        "csr_rows": int((~mask).sum()),
        "amv_rows_missing_from_amv": int((~present).sum()),
        "missing_identities_sample": absent[:20].tolist(),
        "amv_rows_in_destination": int(held.size),
    }


def _fill(array):
    value = array.attrs.get("_FillValue", array.fill_value)
    return None if value is None else value


def _has_data(values: np.ndarray, fill) -> np.ndarray:
    """Per column (or for a 1-D block, as one), whether any value is not missing."""
    if values.dtype.kind == "f":
        present = ~np.isnan(values)
    elif values.dtype.kind in "iu":
        present = values != fill if fill is not None else np.ones(values.shape, dtype=bool)
    else:
        present = np.asarray(values).astype(object) != ""
    if values.ndim == 1:
        return np.array([bool(present.any())])
    return present.any(axis=0)


def compact(array, keep: np.ndarray) -> np.ndarray:
    """Move the kept rows of ``array`` to its start, in order; returns columns with data.

    Rows are read in ascending blocks and written at or before where they were read, so
    nothing is overwritten before it is read.
    """
    rows = array.shape[0]
    width = array.shape[1] if array.ndim == 2 else 1
    used = np.zeros(width, dtype=bool)
    fill = _fill(array)
    written = 0
    step = _step(array)
    for start in range(0, rows, step):
        block = np.asarray(array[start : start + step])
        kept = block[keep[start : start + block.shape[0]]]
        if kept.shape[0]:
            array[written : written + kept.shape[0]] = kept
            used |= _has_data(kept, fill)
            written += kept.shape[0]
    return used


def split(repo, apply: bool = False) -> dict[str, Any]:
    """Verify, and with ``apply`` rewrite ``ami_csr`` to its non-AMV rows (one commit).

    Raises:
        NotVerified: an AMV row is not in ``amv``, or ``ami_csr`` has no other rows.
    """
    import icechunk
    import zarr

    # Verify-only reads a read-only session; a rewrite verifies the snapshot it starts from.
    session = repo.writable_session("main") if apply else None
    summary = verify(session.store if session else repo.readonly_session("main").store)
    logger.info(f"split_ami_csr: {summary}")
    if summary["amv_rows_missing_from_amv"]:
        raise NotVerified(
            f"{summary['amv_rows_missing_from_amv']} AMV rows of {SOURCE} are not in "
            f"{DESTINATION} (e.g. identities {summary['missing_identities_sample']}); "
            "run the satellite target over the archived windows first"
        )
    if not summary["csr_rows"]:
        raise NotVerified(f"{SOURCE} holds no rows but motion vectors; nothing to keep")
    if session is None or not summary["amv_rows"]:
        return {**summary, "applied": False}

    node = zarr.open_group(session.store, path=SOURCE, mode="a")
    keep = ~amv_mask(node)
    kept = int(keep.sum())
    removed, trimmed = [], {}
    for name in sorted(node.array_keys()):
        array = node[name]
        if array.shape[0] != keep.size:
            continue
        used = compact(array, keep)
        if not used.any():
            del node[name]
            removed.append(name)
            continue
        if array.ndim == 2:
            width = int(np.flatnonzero(used).max()) + 1
            if width < array.shape[1]:
                trimmed[name] = (array.shape[1], width)
            array.resize((kept, width))
        else:
            array.resize((kept, *array.shape[1:]))
    node.attrs["split_from_amv"] = (
        f"{summary['amv_rows']} GK-2A AMV rows (template {', '.join(AMV_TEMPLATES)}) moved "
        f"to {DESTINATION}, each verified there by observation_identity"
    )
    session.commit(
        f"{SOURCE}: motion vectors dropped, {summary['amv_rows']} rows verified in {DESTINATION}",
        rebase_with=icechunk.ConflictDetector(),
    )
    return {**summary, "applied": True, "removed_arrays": removed, "trimmed": trimmed}


def main(argv=None) -> int:
    """Command line: verify, and with ``--apply`` rewrite."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="rewrite ami_csr once verified")
    args = parser.parse_args(argv)
    repo = get_config().icechunk_repo(sat.STORE_PREFIX)
    try:
        summary = split(repo, apply=args.apply)
    except NotVerified as exc:
        logger.error(f"split_ami_csr: refused: {exc}")
        return 1
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
