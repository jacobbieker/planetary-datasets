r"""Download MeteoSwiss KENDA-CH1 GRIB2 files from the Open Government Data API.

This is the download half of the KENDA pipeline; :mod:`planetary_datasets.providers.kenda`
is the processing half, and reads the directory this writes into.

It runs inside the ``docker/meteoswiss-kenda`` image rather than the project environment.
The STAC search goes through MeteoSwiss's ``meteodata-lab``, which pins ``numpy<2.4``,
``pandas<3`` and an exact ``eccodes``, none of which the project environment can satisfy.
The module therefore imports nothing from ``planetary_datasets`` and defers its
``meteodatalab`` import to the point of use, so the image needs only this file and the
project can still import (and test) it.

KENDA publishes one GRIB2 file per variable per hour, and the two steps carry disjoint
variable sets: step ``0`` is the analysis of the instantaneous fields, step ``1`` the
one-hour forecast of the accumulated and flux fields. Asking for a variable at the wrong
step returns nothing, so only the right step is queried for each.

MeteoSwiss keeps roughly the last 24 hours online. A file that is already on disk with a
matching checksum is never looked up again, so re-running an hour that has since aged out
of the API still succeeds as long as it was downloaded while it was there. The two
constants files are the exception: MeteoSwiss republishes them in place, so every run
compares the local copy against the checksum the server reports.

Run it as::

    python -m planetary_datasets.providers.kenda_download \\
        --ref-time 2026-09-29T06:00 --target /data/meteoswiss

Under Dagster Pipes it also reports what it fetched back to the launching asset.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import os
import pathlib
import sys
from typing import Any, Iterable, Sequence
from urllib.parse import urlparse

from loguru import logger

COLLECTION = "ogd-analysis-kenda-ch1"
COLLECTION_ID = f"ch.meteoschweiz.{COLLECTION}"

#: Instantaneous fields, published at step 0 only.
ANALYSIS_VARIABLES: tuple[str, ...] = (
    "CAPE_ML",
    "CAPE_MU",
    "CIN_ML",
    "CIN_MU",
    "CLC",
    "DEN",
    "H_SNOW",
    "P",
    "PMSL",
    "QC",
    "QV",
    "T",
    "TD_2M",
    "TKE",
    "TWATER",
    "U",
    "V",
    "W",
)

#: Accumulated and flux fields, published at step 1 only.
FORECAST_VARIABLES: tuple[str, ...] = (
    "ASOB_S",
    "ASOB_S_OS",
    "ASWDIFD_S",
    "ASWDIFU_S",
    "ASWDIFU_S_OS",
    "ASWDIR_S",
    "ASWDIR_S_OS",
    "ATHB_S",
    "ATHD_S",
    "ATHU_S",
    "AUMFL_S",
    "AVMFL_S",
    "GRAU_GSP",
    "RAIN_GSP",
    "SNOW_GSP",
    "TOT_PREC",
    "TQC",
    "TQI",
    "TQV",
    "VMAX_10M",
)

VARIABLES_BY_STEP: dict[int, tuple[str, ...]] = {
    0: ANALYSIS_VARIABLES,
    1: FORECAST_VARIABLES,
}

#: ISO 8601 durations the OGD API expects for each step.
LEAD_TIMES: dict[int, str] = {0: "P0DT0H", 1: "P0DT1H"}

#: Must match the names :mod:`planetary_datasets.providers.kenda` looks for.
CONSTANTS: tuple[str, ...] = (
    "horizontal_constants_kenda-ch1.grib2",
    "vertical_constants_kenda-ch1.grib2",
)

DEFAULT_TARGET = "/data/meteoswiss"

_CHUNK_BYTES = 1 << 20


class IncompleteDownload(RuntimeError):
    """Some of an hour's files are not published (yet)."""


@dataclasses.dataclass
class DownloadReport:
    """What one run of :func:`download_kenda` did."""

    ref_time: dt.datetime
    downloaded: list[str] = dataclasses.field(default_factory=list)
    present: list[str] = dataclasses.field(default_factory=list)
    missing: dict[int, list[str]] = dataclasses.field(default_factory=dict)

    @property
    def complete(self) -> bool:
        """True when every requested variable is on disk."""
        return not any(self.missing.values())

    def summary(self) -> dict[str, Any]:
        """A JSON-serialisable summary, for logs and Dagster metadata."""
        return {
            "ref_time": self.ref_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "downloaded": len(self.downloaded),
            "already_present": len(self.present),
            "missing": {str(step): names for step, names in self.missing.items() if names},
        }


def data_filename(ref_time: dt.datetime, step: int, variable: str) -> str:
    """The filename MeteoSwiss publishes a variable under.

    For example ``kenda-ch1-202609290600-0-t-ctrl.grib2``.
    """
    return f"kenda-ch1-{ref_time:%Y%m%d%H%M}-{step}-{variable.lower()}-ctrl.grib2"


def sidecar_path(path: pathlib.Path) -> pathlib.Path:
    """Where the checksum for ``path`` is kept.

    Same convention as ``meteodatalab.ogd_api.download_from_ogd``, so files in an archive
    it populated are recognised as complete rather than downloaded again.
    """
    return path.with_suffix(".sha256")


def file_sha256(path: pathlib.Path) -> str:
    """Hex SHA-256 of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def is_complete(path: pathlib.Path) -> bool:
    """True when ``path`` exists and matches its recorded checksum."""
    sidecar = sidecar_path(path)
    if not (path.is_file() and sidecar.is_file()):
        return False
    return sidecar.read_text().strip() == file_sha256(path)


def download_file(
    url: str, target: pathlib.Path, session, reuse: bool = False
) -> tuple[pathlib.Path, bool]:
    """Download ``url`` into the directory ``target``, verifying its checksum.

    The body goes to a ``.part`` file that is only renamed into place once it has been
    verified, so an interrupted transfer never leaves a truncated GRIB file where the
    processing step would pick it up.

    Args:
        url: Asset URL. MeteoSwiss hands out presigned GET URLs, which refuse ``HEAD``.
        target: Directory to write into.
        session: A ``requests``-compatible session.
        reuse: Keep an existing local copy when its checksum matches the one the server
            reports, closing the response without reading the body. Used for the
            constants, which MeteoSwiss republishes in place.

    Returns:
        The local path, and whether it was (re)downloaded.
    """
    path = target / pathlib.Path(urlparse(url).path).name
    partial = path.with_name(path.name + ".part")

    response = session.get(url, stream=True, timeout=120)
    response.raise_for_status()
    expected = response.headers.get("X-Amz-Meta-Sha256")

    if reuse and expected is not None and is_complete(path):
        if sidecar_path(path).read_text().strip() == expected:
            response.close()
            return path, False
        logger.info(f"{path.name} has been republished; downloading it again")

    digest = hashlib.sha256()
    try:
        with partial.open("wb") as fh:
            for chunk in response.iter_content(_CHUNK_BYTES):
                fh.write(chunk)
                digest.update(chunk)
        actual = digest.hexdigest()
        if expected is not None and expected != actual:
            raise OSError(f"checksum mismatch for {path.name}: expected {expected}, got {actual}")
        sidecar_path(path).write_text(actual)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    return path, True


def _ogd_api():
    """Import ``meteodatalab.ogd_api``, which only the container has installed."""
    from meteodatalab import ogd_api

    return ogd_api


def _zulu(ref_time: dt.datetime) -> str:
    return ref_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_utc(value: str | dt.datetime) -> dt.datetime:
    """Parse a reference time, treating a naive value as UTC."""
    ref_time = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(value)
    if ref_time.tzinfo is None:
        return ref_time.replace(tzinfo=dt.timezone.utc)
    return ref_time.astimezone(dt.timezone.utc)


def download_kenda(
    ref_time: str | dt.datetime,
    target: str | os.PathLike,
    steps: Iterable[int] = (0, 1),
    variables: Sequence[str] | None = None,
    ogd_api=None,
) -> DownloadReport:
    """Download one hour of KENDA-CH1, plus the constants every hour needs.

    Args:
        ref_time: Analysis time. A naive value is taken as UTC.
        target: Directory to write into; created if needed.
        steps: Which of step 0 (analysis) and step 1 (+1 h forecast) to fetch.
        variables: Restrict to these variables. Each is only requested at the step that
            publishes it.
        ogd_api: Stand-in for ``meteodatalab.ogd_api``, for tests.

    Returns:
        A report of what was downloaded, what was already there and what is missing.
        Missing files are not an error here; the caller decides whether a partial hour
        is acceptable.
    """
    ref_time = to_utc(ref_time)
    target = pathlib.Path(target)
    target.mkdir(parents=True, exist_ok=True)
    wanted = {v.upper() for v in variables} if variables is not None else None
    report = DownloadReport(ref_time=ref_time)
    api = ogd_api

    for step in steps:
        if step not in VARIABLES_BY_STEP:
            raise ValueError(f"KENDA publishes steps {sorted(VARIABLES_BY_STEP)}, not {step}")
        missing = report.missing.setdefault(step, [])
        for variable in VARIABLES_BY_STEP[step]:
            if wanted is not None and variable not in wanted:
                continue
            path = target / data_filename(ref_time, step, variable)
            if is_complete(path):
                report.present.append(path.name)
                continue

            api = api or _ogd_api()
            request = api.Request(
                collection=COLLECTION,
                variable=variable,
                ref_time=_zulu(ref_time),
                perturbed=False,
                lead_time=LEAD_TIMES[step],
            )
            try:
                urls = api.get_asset_urls(request)
                for url in urls:
                    path, _ = download_file(url, target, api.session)
                    report.downloaded.append(path.name)
            except (OSError, ValueError) as exc:
                # requests' exceptions are OSErrors; ValueError is what get_asset_urls
                # raises for an asset URL it cannot parse.
                logger.warning(f"KENDA {variable} step {step} at {_zulu(ref_time)}: {exc}")
                missing.append(variable)
                continue
            if not urls:
                logger.info(f"KENDA {variable} step {step} at {_zulu(ref_time)}: not published")
                missing.append(variable)

    if report.downloaded or report.present:
        api = api or _ogd_api()
        for name in CONSTANTS:
            url = api.get_collection_asset_url(COLLECTION_ID, name)
            path, fetched = download_file(url, target, api.session, reuse=True)
            if fetched:
                report.downloaded.append(path.name)

    return report


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--ref-time", required=True, help="Analysis time, ISO 8601, UTC.")
    parser.add_argument(
        "--target",
        default=os.environ.get("KENDA_ARCHIVE_DIR", DEFAULT_TARGET),
        help=f"Directory to download into (default: $KENDA_ARCHIVE_DIR or {DEFAULT_TARGET}).",
    )
    parser.add_argument(
        "--steps", type=int, nargs="+", default=[0, 1], choices=sorted(VARIABLES_BY_STEP)
    )
    parser.add_argument("--variables", nargs="+", help="Only these variables.")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Succeed even when some variables are not published.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace, pipes=None) -> DownloadReport:
    """Download, then fail if the hour is incomplete unless that was allowed."""
    report = download_kenda(args.ref_time, args.target, steps=args.steps, variables=args.variables)
    summary = report.summary()
    logger.info(f"KENDA {summary}")
    if not report.complete and not args.allow_partial:
        # Failing rather than succeeding short: the processing step merges whatever is
        # on disk, and an hour committed with some variables absent would fix the store's
        # schema to that subset.
        raise IncompleteDownload(f"KENDA {summary['ref_time']} is incomplete: {summary['missing']}")
    if pipes is not None:
        pipes.report_asset_materialization(metadata=pipes_metadata(summary))
    return report


def pipes_metadata(summary: dict[str, Any]) -> dict[str, Any]:
    """Tag container values as JSON for Dagster Pipes.

    Pipes reads an untagged dict as a ``{raw_value, type}`` wrapper and rejects anything
    else, including an empty one, so collections have to be tagged explicitly.
    """
    return {
        key: {"raw_value": value, "type": "json"} if isinstance(value, (dict, list)) else value
        for key, value in summary.items()
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; reports through Dagster Pipes when launched by it."""
    args = _parse_args(argv)
    if not os.environ.get("DAGSTER_PIPES_CONTEXT"):
        run(args)
        return 0

    from dagster_pipes import open_dagster_pipes

    with open_dagster_pipes() as pipes:
        run(args, pipes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
