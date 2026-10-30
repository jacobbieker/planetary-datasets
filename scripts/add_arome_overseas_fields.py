"""Give the AROME overseas stores the fields Météo-France added to IP5 in October 2026.

    python scripts/add_arome_overseas_fields.py [--dry-run] [SLUG ...]

Divergence and pseudo-adiabatic potential temperature arrived in the overseas IP5 paquet
on their own pressure-level subsets. The xarray writer refuses a partition whose
variables differ from the store's, so each store gets the two arrays first, laid out like
its other pressure-level fields (same shape, chunks and compression). Nothing is written
but metadata: every time already stored reads as missing for them, and later partitions
fill them. Stop the stores' noon catch-ups first.
"""

from __future__ import annotations

import argparse
import sys

import numpy as np

SLUGS = ("caribbean", "french_guiana", "indian_ocean", "new_caledonia", "polynesia")
#: An existing pressure-level array to copy the layout from.
TEMPLATE = "vorticity"
FIELDS = {
    "divergence": (
        np.float32,
        {
            "long_name": "Divergence",
            "units": "s**-1",
            "standard_name": "divergence_of_wind",
            "GRIB_shortName": "d",
            "GRIB_paramId": 155,
            "GRIB_name": "Divergence",
        },
    ),
    "pseudo_adiabatic_potential_temperature": (
        np.float16,
        {
            "long_name": "Pseudo-adiabatic potential temperature",
            "units": "K",
            "GRIB_shortName": "papt",
            "GRIB_paramId": 3014,
            "GRIB_name": "Pseudo-adiabatic potential temperature",
        },
    ),
}


def add_fields(repo, dry_run: bool = False) -> list[str]:
    """Create the missing fields in one commit. Returns the names created."""
    import icechunk
    import zarr
    from xarray.backends.zarr import FillValueCoder

    session = repo.writable_session("main")
    group = zarr.open_group(session.store, mode="a")
    template = group[TEMPLATE]
    created = []
    for name, (dtype, attrs) in FIELDS.items():
        if name in group:
            continue
        created.append(name)
        if dry_run:
            continue
        coordinates = template.attrs.get("coordinates")
        group.create_array(
            name,
            shape=template.shape,
            chunks=template.chunks,
            dtype=dtype,
            compressors=(zarr.codecs.BloscCodec(cname="zstd", clevel=9, shuffle="bitshuffle"),),
            fill_value=np.nan,
            dimension_names=template.metadata.dimension_names,
            attributes={
                **attrs,
                **({"coordinates": coordinates} if coordinates else {}),
                "_FillValue": FillValueCoder.encode(np.nan, np.dtype(dtype)),
            },
        )
    if created and not dry_run:
        session.commit(
            f"arome overseas: add {', '.join(created)} (published since October 2026)",
            rebase_with=icechunk.ConflictDetector(),
        )
    return created


def main(argv=None) -> int:
    from planetary_datasets.config import get_config

    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("slugs", nargs="*", default=list(SLUGS))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    for slug in args.slugs:
        repo = get_config().icechunk_repo(f"bkr/dmi/arome_{slug}.icechunk")
        print(slug, add_fields(repo, args.dry_run) or "nothing to add", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
