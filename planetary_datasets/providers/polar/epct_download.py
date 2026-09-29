r"""Download EPS products from the EUMETSAT Data Store and convert them with the Data Tailor.

Runs in the ``docker/epct`` image, since ``epct`` is only published on the ``eumetsat``
conda channel with pins the project environment cannot meet, so this imports nothing
from ``planetary_datasets``. The
tailored netCDF lands in ``<target>/<product>/<YYYYmmddTHHMM>/``, which the
:class:`~planetary_datasets.providers.polar._eumdac.EumdacProvider` publishes::

    python -m planetary_datasets.providers.polar.epct_download --collection \
        EO:EUM:DAT:METOP:AMSUL1 --product AMSAL1 --start 2026-09-28T00:00 \
        --end 2026-09-29T00:00 --target /data/epct
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import pathlib
import shutil
import sys


def stage_dir(target: pathlib.Path, product: str, start: dt.datetime) -> pathlib.Path:
    """Where a partition's tailored files are staged."""
    return target / product / start.strftime("%Y%m%dT%H%M")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--product", required=True, help="Data Tailor product, e.g. AMSAL1")
    parser.add_argument("--start", required=True, type=dt.datetime.fromisoformat)
    parser.add_argument("--end", required=True, type=dt.datetime.fromisoformat)
    parser.add_argument("--target", required=True, type=pathlib.Path)
    args = parser.parse_args(argv)

    out = stage_dir(args.target, args.product, args.start)
    if out.is_dir():
        print(f"{out} already staged")
        return 0

    import eumdac
    from epct import api

    token = eumdac.AccessToken(
        (os.environ["EUMETSAT_CONSUMER_KEY"], os.environ["EUMETSAT_CONSUMER_SECRET"])
    )
    collection = eumdac.DataStore(token).get_collection(args.collection)
    products = list(collection.search(dtstart=args.start, dtend=args.end))
    print(f"{len(products)} product(s) in {args.collection} for {args.start} - {args.end}")

    # Built beside the final directory and renamed, so a partial run is never published.
    part = out.with_name(out.name + ".part")
    shutil.rmtree(part, ignore_errors=True)
    raw = part / "raw"
    for scratch in ("raw", "workspace", "log"):
        (part / scratch).mkdir(parents=True)
    archives = []
    for product in products:
        try:
            with product.open() as src, open(raw / src.name, "wb") as dst:
                shutil.copyfileobj(src, dst)
            archives.append(str(raw / src.name))
        except Exception as exc:  # noqa: BLE001 - one bad product must not lose the day
            print(f"skipping {product}: {exc}", file=sys.stderr)
    if products and not archives:
        raise RuntimeError(f"none of {len(products)} product(s) downloaded")
    if archives:
        # run_chain logs a failed customisation rather than raising it.
        outputs = api.run_chain(
            product_paths=archives,
            chain_config={"product": args.product, "format": "netcdf4_satellite"},
            target_dir=str(part),
            workspace_dir=str(part / "workspace"),
            log_dir=str(part / "log"),
        )
        if not outputs:
            raise RuntimeError(f"the Data Tailor produced nothing from {len(archives)} product(s)")
    for scratch in ("raw", "workspace", "log"):
        shutil.rmtree(part / scratch, ignore_errors=True)
    part.rename(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
