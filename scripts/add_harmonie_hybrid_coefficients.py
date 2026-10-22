"""Add the hybrid-level coefficients to the existing DMI HARMONIE model-level stores.

New runs carry ``ap``/``b`` (full levels, on ``hybrid``) and ``ap_half``/``b_half``
(half levels, on ``hybrid_half``) as coordinates; see
:func:`~planetary_datasets.providers.dmi_harmonie.with_hybrid_coefficients`. The stores
written before that lack them, and since ``hybrid_half`` is a new dimension the schema
fingerprint of a new run would no longer match, starting a new generation. This writes the
same coordinates into every existing generation, once, so the next run appends as usual.

The coefficients are read from the newest model-level file on ``dmi-opendata``: only the
first megabytes are fetched, enough for its first GRIB message.

Run it with nothing writing the model-level store (the noon catch-up writes it)::

    python scripts/add_harmonie_hybrid_coefficients.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
import tempfile

import numpy as np
import s3fs
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk

from planetary_datasets.providers.dmi_harmonie import (
    BUCKET,
    DMIHarmonieModelLevelProvider,
    hybrid_coefficients,
    with_hybrid_coefficients,
)

#: Bytes of a model-level file fetched to read its first message's ``pv`` array.
HEAD_BYTES = 64 * 2**20
COORDS = ("ap", "b", "ap_half", "b_half")


def latest_coefficients() -> tuple[np.ndarray, np.ndarray, str]:
    """``(A, B)`` half-level coefficients from the newest model-level file, and its key."""
    fs = s3fs.S3FileSystem(anon=True)
    key = sorted(fs.ls(f"{BUCKET}/forecastdata/HARMONIE_IG_ML/", detail=False))[-1]
    with tempfile.NamedTemporaryFile(suffix=".grib") as head:
        head.write(fs.cat_file(key, start=0, end=HEAD_BYTES))
        head.flush()
        a_half, b_half = hybrid_coefficients(head.name)
    return a_half, b_half, key


def add_to_store(repo, a_half: np.ndarray, b_half: np.ndarray, dry_run: bool) -> str:
    """Write the coefficients into one store; what was done, for the log."""
    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="r")
    if "ap" in group:
        return "already has them"
    levels = group["hybrid"].shape[0]
    stub = xr.Dataset(coords={"hybrid": np.arange(1, levels + 1)})
    static = with_hybrid_coefficients(stub, a_half, b_half)
    static = xr.Dataset(coords={name: static[name] for name in COORDS})
    if dry_run:
        return f"would add {', '.join(COORDS)} for {levels} levels"
    to_icechunk(static, session, mode="a")
    # Name them in the coordinates attribute of every model-level field: an append rewrites
    # the group's own list of coordinates with only those no variable claims, and ap/b on
    # the hybrid axis would otherwise come back as data variables.
    group = zarr.open_group(session.store, mode="a")
    for name, array in group.arrays():
        dims = array.metadata.dimension_names or ()
        if "hybrid" in dims and name not in ("hybrid", "ap", "b"):
            have = str(array.attrs.get("coordinates", "")).split()
            array.attrs["coordinates"] = " ".join(dict.fromkeys([*have, "ap", "b"]))
    session.commit("Add hybrid-level coefficients ap, b, ap_half, b_half from the GRIB pv array")
    return f"added {', '.join(COORDS)} for {levels} levels"


def main(argv: list[str] | None = None) -> int:
    """Add the coefficients to every model-level generation."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    a_half, b_half, key = latest_coefficients()
    print(f"coefficients from {key}: {a_half.size} half levels", flush=True)
    provider = DMIHarmonieModelLevelProvider()
    for prefix in provider.generations():
        repo = provider.config.icechunk_repo(prefix)
        print(f"{prefix}: {add_to_store(repo, a_half, b_half, args.dry_run)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
