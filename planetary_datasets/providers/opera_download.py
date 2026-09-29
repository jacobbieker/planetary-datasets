r"""Fetch one hour of EUMETNET OPERA radar composites and stage it as netCDF.

This is the download half of the OPERA pipeline; :mod:`planetary_datasets.providers.opera`
is the processing half, and reads the files this writes.

It runs inside the ``docker/earth2studio`` image rather than the project environment. The
ODIM HDF5 decoding comes from NVIDIA's ``earth2studio``, which pulls in ``torch`` and pins
``netcdf4<1.7.3``, neither of which fits the project environment. The module therefore
imports nothing from ``planetary_datasets`` and defers its ``earth2studio`` import to the
point of use, so the image needs only this file and the project can still import the
product definitions from it.

Two products, each matching an existing store:

``rainfall``
    Instantaneous rain rate and the one-hour accumulation, every 15 minutes. The
    accumulation is in metres, as ``earth2studio`` reports it.
``dbz``
    Composite reflectivity every 5 minutes. Only the CIRRUS/NIMBUS era (from July 2024) is
    on the 1 km grid, and earlier hours are on a different one, so they are refused.

Each hour is written to ``<target>/<product>/YYYY/MM/DD/opera_<product>_<YYYYMMDDhhmm>.nc``
via a temporary file, so the processing step never sees a partial write.

Run it as::

    python -m planetary_datasets.providers.opera_download \
        --product rainfall --time 2026-09-29T06:00 --target /data/opera

Under Dagster Pipes it also reports what it staged back to the launching asset.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import os
import pathlib
import sys
from typing import Any, Callable, Sequence

from loguru import logger

DEFAULT_TARGET = "/data/opera"

#: Start of the CIRRUS/NIMBUS era: 5-minute composites, and reflectivity on a 1 km grid.
CIRRUS_START = dt.datetime(2024, 7, 1)


@dataclasses.dataclass(frozen=True)
class Product:
    """One OPERA store's worth of variables.

    Attributes:
        name: Short name, used in paths and asset names.
        variables: Output variable name -> ``earth2studio`` OPERA lexicon name.
        units: Output variable name -> units.
        step_minutes: Spacing of the frames within the hour.
        earliest: First hour this product can be fetched for.
    """

    name: str
    variables: dict[str, str]
    units: dict[str, str]
    step_minutes: int
    earliest: dt.datetime

    def frame_times(self, hour: dt.datetime) -> list[dt.datetime]:
        """The frame times making up the hour starting at ``hour``."""
        return [hour + dt.timedelta(minutes=m) for m in range(0, 60, self.step_minutes)]


PRODUCTS: dict[str, Product] = {
    "rainfall": Product(
        name="rainfall",
        variables={"rainfall_rate": "tprate", "accumulated_rainfall_1hour": "tp01"},
        units={"rainfall_rate": "mm h-1", "accumulated_rainfall_1hour": "m"},
        step_minutes=15,
        # The OPERA archive starts in October 2011, but the ODYSSEY files before 2018 were
        # never part of the store this feeds.
        earliest=dt.datetime(2018, 1, 1),
    ),
    "dbz": Product(
        name="dbz",
        variables={"dbz": "refc"},
        units={"dbz": "dBZ"},
        step_minutes=5,
        earliest=CIRRUS_START,
    ),
}


class IncompleteHour(RuntimeError):
    """Some of an hour's frames could not be fetched."""


def staged_path(target: str | os.PathLike, product: str, hour: dt.datetime) -> pathlib.Path:
    """Where an hour of ``product`` is staged under ``target``."""
    return (
        pathlib.Path(target)
        / product
        / hour.strftime("%Y/%m/%d")
        / f"opera_{product}_{hour:%Y%m%d%H%M}.nc"
    )


def to_naive_utc(value: str | dt.datetime) -> dt.datetime:
    """Parse a time, returning naive UTC as ``earth2studio`` expects."""
    when = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(value)
    if when.tzinfo is not None:
        when = when.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return when


def _opera_source():
    """Build the ``earth2studio`` OPERA source, which only the container has installed."""
    from earth2studio.data import OPERA

    return OPERA(cache=False, verbose=False)


def fetch_hour(
    product: Product, hour: dt.datetime, source: Callable | None = None
) -> tuple[Any, list[dt.datetime]]:
    """Fetch every frame of one hour.

    The whole hour is requested in one call, which earth2studio fetches concurrently.
    Only if that fails is it retried frame by frame, so that a single frame missing from
    the archive is reported by name instead of failing the hour anonymously.

    Returns:
        The hour as an ``xarray.Dataset`` (``None`` if no frame could be fetched), and
        the frame times that could not be.
    """
    import xarray as xr

    source = source or _opera_source()
    lexicon = list(product.variables.values())
    frames = product.frame_times(hour)
    missing: list[dt.datetime] = []
    try:
        array = source(frames, lexicon)
    except Exception as exc:  # noqa: BLE001 - earth2studio surfaces I/O errors as many types
        logger.warning(f"OPERA {product.name} {hour:%Y-%m-%dT%H}: {exc}; retrying per frame")
        parts = []
        for when in frames:
            try:
                parts.append(source([when], lexicon))
            except Exception as frame_exc:  # noqa: BLE001 - as above
                logger.warning(f"OPERA {product.name} {when:%Y-%m-%dT%H:%M}: {frame_exc}")
                missing.append(when)
        if not parts:
            return None, missing
        # Every frame carries the same grid; compare it once rather than per frame.
        array = xr.concat(parts, dim="time", join="exact", coords="minimal", compat="override")

    ds = xr.Dataset(
        {name: array.sel(variable=code, drop=True) for name, code in product.variables.items()}
    )
    ds = ds.rename({"_lat": "latitude", "_lon": "longitude"})
    for name, units in product.units.items():
        ds[name].attrs.update(units=units, opera_variable=product.variables[name])
    ds.attrs.update(
        source="EUMETNET OPERA composite, via earth2studio",
        product=product.name,
        crs=("+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 +y_0=-2100000.0 +ellps=WGS84"),
    )
    return ds, missing


def write_staged(ds, path: pathlib.Path) -> pathlib.Path:
    """Write ``ds`` to ``path`` atomically, compressed, one chunk per frame."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    encoding = {
        name: {"zlib": True, "complevel": 4, "chunksizes": (1, *ds[name].shape[1:])}
        for name in ds.data_vars
    }
    try:
        ds.to_netcdf(partial, engine="netcdf4", format="NETCDF4", encoding=encoding)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    return path


def download_opera(
    product: str,
    hour: str | dt.datetime,
    target: str | os.PathLike,
    allow_partial: bool = False,
    source: Callable | None = None,
) -> dict[str, Any]:
    """Fetch one hour of ``product`` and stage it under ``target``.

    Args:
        product: A key of :data:`PRODUCTS`.
        hour: Start of the hour, UTC. A naive value is taken as UTC.
        target: Staging root.
        allow_partial: Stage an hour with some frames missing instead of failing.
        source: Stand-in for the ``earth2studio`` OPERA source, for tests.

    Returns:
        A JSON-serialisable summary of what was staged.

    Raises:
        IncompleteHour: When frames are missing and ``allow_partial`` is False, or when
            none could be fetched at all.
    """
    spec = PRODUCTS[product]
    hour = to_naive_utc(hour)
    if hour.minute or hour.second or hour.microsecond:
        raise ValueError(f"{hour} is not the start of an hour")
    if hour < spec.earliest:
        raise ValueError(f"OPERA {product} is only staged from {spec.earliest:%Y-%m-%d}")

    ds, missing = fetch_hour(spec, hour, source)
    label = f"OPERA {product} {hour:%Y-%m-%dT%H:%M}"
    if ds is None:
        raise IncompleteHour(f"{label}: no frames could be fetched")
    if missing and not allow_partial:
        raise IncompleteHour(
            f"{label}: missing {len(missing)} of {len(spec.frame_times(hour))} frames: "
            + ", ".join(f"{t:%H:%M}" for t in missing)
        )

    path = write_staged(ds, staged_path(target, product, hour))
    summary = {
        "product": product,
        "hour": f"{hour:%Y-%m-%dT%H:%M}",
        "path": str(path),
        "frames": int(ds.sizes["time"]),
        "missing_frames": [f"{t:%H:%M}" for t in missing],
        "grid": [int(ds.sizes["y"]), int(ds.sizes["x"])],
    }
    logger.info(f"{label}: staged {summary}")
    return summary


def pipes_metadata(summary: dict[str, Any]) -> dict[str, Any]:
    """Tag container values as JSON for Dagster Pipes.

    Pipes reads an untagged dict as a ``{raw_value, type}`` wrapper and rejects anything
    else, so collections have to be tagged explicitly.
    """
    return {
        key: {"raw_value": value, "type": "json"} if isinstance(value, (dict, list)) else value
        for key, value in summary.items()
    }


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--product", required=True, choices=sorted(PRODUCTS))
    parser.add_argument("--time", required=True, help="Start of the hour, ISO 8601, UTC.")
    parser.add_argument(
        "--target",
        default=os.environ.get("OPERA_ARCHIVE_DIR", DEFAULT_TARGET),
        help=f"Staging root (default: $OPERA_ARCHIVE_DIR or {DEFAULT_TARGET}).",
    )
    parser.add_argument(
        "--allow-partial", action="store_true", help="Stage an hour with frames missing."
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; reports through Dagster Pipes when launched by it."""
    args = _parse_args(argv)

    def run() -> dict[str, Any]:
        return download_opera(
            args.product, args.time, args.target, allow_partial=args.allow_partial
        )

    if not os.environ.get("DAGSTER_PIPES_CONTEXT"):
        run()
        return 0

    from dagster_pipes import open_dagster_pipes

    with open_dagster_pipes() as pipes:
        pipes.report_asset_materialization(metadata=pipes_metadata(run()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
