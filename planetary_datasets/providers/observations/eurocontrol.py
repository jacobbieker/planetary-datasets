"""Eurocontrol R&D Archive flight points, combined into per-archive parquet files.

The Eurocontrol R&D Archive (https://www.eurocontrol.int/dashboard/rnd-data-archive) is
distributed as monthly zip bundles that unpack to three gzipped CSVs per month::

    <base_dir>/202312/Flights_20231201_20231231.csv.gz
    <base_dir>/202312/Flight_Points_Actual_20231201_20231231.csv.gz
    <base_dir>/202312/Flight_Points_Filed_20231201_20231231.csv.gz

The archive is licence-gated, so the files must be downloaded by hand; there is no URL
to fetch here and therefore no provider and no credential. What these helpers do is the
part that was worth keeping from ``one_offs/eurocontrol_combine.py``: attach the aircraft
metadata from ``Flights`` onto each flight point by ``ECTRL ID`` and stream the months
into one parquet file per point type.

``base_dir`` defaults to the configured ``PLANETARY_DATASETS_DATA_DIR``; set
``PLANETARY_DATASETS_DATA_DIR`` rather than passing an absolute path.

polars is used for the streaming concat and is imported lazily: it is not a declared
dependency of the package, and everything else here must stay importable without it.
"""

from __future__ import annotations

import pathlib
from typing import Iterable, List, Sequence

from loguru import logger

from planetary_datasets.config import get_config

#: Columns carried over from ``Flights`` onto each flight point.
FLIGHT_METADATA_COLUMNS: Sequence[str] = (
    "ECTRL ID",
    "AC Operator",
    "AC Type",
    "AC Registration",
    "ICAO Flight Type",
    "STATFOR Market Segment",
)

#: Point type -> the filename stem Eurocontrol ships it under.
POINT_KINDS = {"flown": "Flight_Points_Actual", "filed": "Flight_Points_Filed"}


def _polars():
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "The Eurocontrol helpers need polars. Install it with "
            "`pixi add polars` or `pip install polars`."
        ) from exc
    return pl


def _default_base_dir() -> pathlib.Path:
    return get_config().data_dir / "eurocontrol"


def month_files(base_dir: pathlib.Path, year_month: str) -> dict[str, pathlib.Path]:
    """Paths of the three CSVs for a ``YYYYMM`` month, whether or not they exist.

    The end-of-month day is read from the directory listing rather than computed:
    Eurocontrol occasionally ships a partial final month whose filename stops short of
    the calendar month end.
    """
    folder = base_dir / year_month
    found: dict[str, pathlib.Path] = {}
    for key, stem in {"flights": "Flights", **POINT_KINDS}.items():
        matches = sorted(folder.glob(f"{stem}_{year_month}01_*.csv.gz"))
        if matches:
            found[key] = matches[0]
    return found


def combine_month(base_dir: pathlib.Path, year_month: str):
    """Return ``{"filed": LazyFrame, "flown": LazyFrame}`` for one month, or ``{}``.

    Each frame is the flight points left-joined to the aircraft metadata on ``ECTRL ID``.
    """
    pl = _polars()
    files = month_files(base_dir, year_month)
    if "flights" not in files or not any(kind in files for kind in POINT_KINDS):
        logger.info(f"missing Eurocontrol files for {year_month}, skipping")
        return {}

    metadata = pl.scan_csv(files["flights"]).select(list(FLIGHT_METADATA_COLUMNS))
    out = {}
    for kind in POINT_KINDS:
        if kind not in files:
            continue
        out[kind] = pl.scan_csv(files[kind]).join(metadata, on="ECTRL ID", how="left")
    return out


def combine_flight_points(
    year_months: Iterable[str],
    base_dir: str | pathlib.Path | None = None,
    out_dir: str | pathlib.Path | None = None,
    compression: str = "zstd",
    compression_level: int = 15,
) -> List[pathlib.Path]:
    """Join and stream many months into one parquet per point type.

    Args:
        year_months: ``YYYYMM`` strings to include.
        base_dir: Directory holding the per-month folders. Defaults to
            ``<data_dir>/eurocontrol``.
        out_dir: Where the combined files are written. Defaults to ``base_dir``.
        compression: Parquet codec.
        compression_level: Codec level.

    Returns:
        The parquet files written, one per point type that had any input.
    """
    pl = _polars()
    base = pathlib.Path(base_dir) if base_dir is not None else _default_base_dir()
    target = pathlib.Path(out_dir) if out_dir is not None else base
    target.mkdir(parents=True, exist_ok=True)

    per_kind: dict[str, list] = {kind: [] for kind in POINT_KINDS}
    for year_month in year_months:
        for kind, frame in combine_month(base, year_month).items():
            per_kind[kind].append(frame)

    written: List[pathlib.Path] = []
    for kind, frames in per_kind.items():
        if not frames:
            continue
        path = target / f"eurocontrol_flight_points_{kind}.parquet"
        # sink_parquet streams, so an archive larger than memory still completes; the
        # original script read whole quarters into RAM and had to batch by hand.
        pl.concat(frames, how="vertical_relaxed").sink_parquet(
            path, compression=compression, compression_level=compression_level
        )
        logger.info(f"wrote {path}")
        written.append(path)
    return written


def combine_parquets(
    paths: Sequence[str | pathlib.Path],
    out_path: str | pathlib.Path,
    compression: str = "zstd",
    compression_level: int = 15,
) -> pathlib.Path:
    """Stream several parquet files into one, without loading them into memory.

    Used to roll the per-hour OpenSky parquet exports up into a single file.
    """
    pl = _polars()
    if not paths:
        raise ValueError("no parquet files to combine")
    out_path = pathlib.Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pl.scan_parquet([str(p) for p in paths]).sink_parquet(
        out_path, compression=compression, compression_level=compression_level
    )
    logger.info(f"wrote {out_path}")
    return out_path
