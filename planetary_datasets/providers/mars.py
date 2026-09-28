"""Download data from ECMWF's MARS archive.

Credentials come from :func:`planetary_datasets.config.get_config` --
``ECMWF_API_KEY`` and ``ECMWF_API_EMAIL`` in ``.env`` or the environment, with
``ECMWF_API_URL`` optional. If none are configured the ``ecmwfapi`` client's own
``~/.ecmwfapirc`` fallback is used, so an existing machine keeps working.
See https://api.ecmwf.int/v1/key/ to get a key.

Retrievals land in ``<data_dir>/mars`` by default, where ``data_dir`` is
``PLANETARY_DATASETS_DATA_DIR``; nothing here hardcodes a machine path.
"""

import pathlib

import pandas as pd
from loguru import logger

from planetary_datasets.config import MissingCredential, get_config

# All 137 model levels
MODEL_LEVELS = "/".join(str(level) for level in range(1, 138))

# Default analysis parameters (GRIB codes)
ANALYSIS_ML_PARAMS = "75/76/77/129/130/131/132/133/135/138/152/155/203/246/247/248"

ANALYSIS_TIMES = "00:00:00/06:00:00/12:00:00/18:00:00"

# Forecast parameters (GRIB codes) used to fill the hours between analyses
FORECAST_ML_PARAMS = "75/76/77/130/131/132/133/135/138/152/155/246/247/248/260290"

# Steps 1-5 from each analysis time fill the gaps to the next analysis
FORECAST_STEPS = "1/2/3/4/5"

# Only the 00 and 12 UTC forecasts are archived in the "oper"/"wave" streams.
# The 06 and 18 UTC runs are short cut-off forecasts, archived in "scda"
# (atmosphere) and "scwv" (ocean wave). Asking for a 06/18 forecast from
# oper/wave matches nothing, and MARS then fails the whole request with
# "ERROR 89 (MARS_EXPECTED_FIELDS): Expected <n>, got 0".
SHORT_CUTOFF_STREAMS = {"oper": "scda", "wave": "scwv"}

# Ocean wave analysis parameters (GRIB codes, table 140)
WAVE_PARAMS = (
    "98.140/99.140/100.140/101.140/102.140/103.140/104.140/105.140/112.140/113.140/"
    "114.140/115.140/116.140/117.140/118.140/119.140/120.140/121.140/122.140/123.140/"
    "124.140/125.140/126.140/127.140/128.140/129.140/207.140/208.140/209.140/211.140/"
    "212.140/214.140/215.140/216.140/217.140/218.140/219.140/220.140/221.140/222.140/"
    "223.140/224.140/225.140/226.140/227.140/228.140/229.140/230.140/231.140/232.140/"
    "233.140/234.140/235.140/236.140/237.140/238.140/239.140/244.140/245.140/246.140/"
    "247.140/248.140/249.140/252.140/253.140/254.140/140131/140132/140133/140134"
)

# Wave analyses are 3-hourly
WAVE_TIMES = "00:00:00/03:00:00/06:00:00/09:00:00/12:00:00/15:00:00/18:00:00/21:00:00"

# 2D wave spectra: param and its direction/frequency bins
SPECTRA_PARAM = "251.140"
SPECTRA_DIRECTIONS = "/".join(str(d) for d in range(1, 37))
SPECTRA_FREQUENCIES = "/".join(str(f) for f in range(1, 30))


def default_target_dir() -> pathlib.Path:
    """Directory the ``output_*.grib`` retrievals are written to.

    ``<data_dir>/mars``, so the location moves with
    ``PLANETARY_DATASETS_DATA_DIR`` rather than being fixed to one machine's
    array.
    """
    return get_config().data_dir / "mars"


#: Where ``ecmwfapi`` points when no URL is configured.
DEFAULT_ECMWF_API_URL = "https://api.ecmwf.int/v1"


def _ecmwf_service_class():
    """``ecmwfapi.ECMWFService``, imported on use.

    Deferred so that importing this module -- which the Dagster code location
    and the pipeline's planning both do -- needs nothing but the
    configuration. Only the process that actually talks to MARS needs the
    client. Patched wholesale in tests.
    """
    from ecmwfapi import ECMWFService  # noqa: PLC0415

    return ECMWFService


def mars_service(service: str = "mars"):
    """An ``ECMWFService`` authenticated from the configuration.

    Uses ``ECMWF_API_KEY``/``ECMWF_API_EMAIL`` (and ``ECMWF_API_URL``, which
    defaults to `DEFAULT_ECMWF_API_URL`). All three have to be passed together:
    ``ECMWFService`` discards the ones it was given as soon as any is ``None``
    and re-reads them itself.

    When no key is configured, falls back to ``~/.ecmwfapirc`` if the machine
    has one, so adopting the configuration does not break an existing install.
    Without either, this raises rather than letting ``ecmwfapi`` fall back to
    anonymous access, which fails later with an opaque authorisation error.
    """
    creds = get_config().credentials
    try:
        key, email = creds.require("ecmwf_api_key", "ecmwf_api_email")
    except MissingCredential as exc:
        if not (pathlib.Path.home() / ".ecmwfapirc").is_file():
            raise
        logger.debug(f"{exc} Falling back to ~/.ecmwfapirc")
        return _ecmwf_service_class()(service)
    return _ecmwf_service_class()(
        service,
        url=creds.ecmwf_api_url or DEFAULT_ECMWF_API_URL,
        key=key,
        email=email,
    )


def forecast_stream(time: str, stream: str = "oper") -> str:
    """The MARS stream holding the forecast initialised at `time` ("HH:MM:SS").

    See `SHORT_CUTOFF_STREAMS`: asking the wrong stream for a 06/18Z forecast
    matches no fields and fails the request outright.
    """
    if time.startswith(("06", "18")):
        return SHORT_CUTOFF_STREAMS[stream]
    return stream


def build_mars_date(start: pd.Timestamp, end: pd.Timestamp | None = None) -> str:
    """Build a MARS date value, either a single date or a from/to range."""
    if end is None or end == start:
        return start.strftime("%Y-%m-%d")
    return f"{start.strftime('%Y-%m-%d')}/to/{end.strftime('%Y-%m-%d')}"


def build_mars_request(
    start: pd.Timestamp,
    end: pd.Timestamp | None = None,
    *,
    mars_class: str = "od",
    stream: str = "oper",
    expver: str = "1",
    type_: str = "an",
    levtype: str | None = "ml",
    levelist: str = MODEL_LEVELS,
    param: str = ANALYSIS_ML_PARAMS,
    time: str = ANALYSIS_TIMES,
    step: str | None = None,
    domain: str | None = None,
    direction: str | None = None,
    frequency: str | None = None,
    grid: str | None = None,
    area: str | None = None,
    format_: str | None = None,
) -> dict[str, str]:
    """Build a MARS request dictionary.

    Args:
        start: First date to retrieve.
        end: Last date to retrieve (inclusive). If None, only `start` is retrieved.
        mars_class: MARS class (e.g. "od" for operational data).
        stream: Forecast stream (e.g. "oper").
        expver: Experiment version, "1" for operational data.
        type_: Data type (e.g. "an" for analysis, "fc" for forecast).
        levtype: Level type (e.g. "ml" for model levels, "pl", "sfc"). Use None
            to omit levtype/levelist entirely (e.g. for wave data).
        levelist: Levels as a "/"-separated string. Ignored for levtype="sfc".
        param: Parameters as a "/"-separated string of GRIB codes or shortnames.
        time: Times as a "/"-separated string.
        step: Optional forecast steps as a "/"-separated string, for type_="fc".
        domain: Optional MARS domain, e.g. "g" (global) for wave data.
        direction: Optional wave spectra direction bins as a "/"-separated string.
        frequency: Optional wave spectra frequency bins as a "/"-separated string.
        grid: Optional target grid, e.g. "0.25/0.25".
        area: Optional area subset "north/west/south/east".
        format_: Optional output format, e.g. "netcdf".

    Returns:
        The MARS request as a dictionary, suitable for ECMWFService("mars").execute().
    """
    request = {
        "class": mars_class,
        "date": build_mars_date(start, end),
        "expver": expver,
        "param": param,
        "stream": stream,
        "time": time,
        "type": type_,
    }
    if levtype is not None:
        request["levtype"] = levtype
        if levtype != "sfc":
            request["levelist"] = levelist
    if step is not None:
        request["step"] = step
    if domain is not None:
        request["domain"] = domain
    if direction is not None:
        request["direction"] = direction
    if frequency is not None:
        request["frequency"] = frequency
    if grid is not None:
        request["grid"] = grid
    if area is not None:
        request["area"] = area
    if format_ is not None:
        request["format"] = format_
    return request


def retrieve_mars(request: dict[str, str], target: str | pathlib.Path) -> str:
    """Execute a MARS request, writing the result to `target`.

    Downloads to a temporary file and renames it on success, so `target` only
    exists once the retrieval is complete. This makes skip-if-exists checks
    safe against partial downloads from interrupted runs.

    The temporary file is removed when the retrieval fails. A single MARS target
    can be 75GB and nothing else ever looks at it: every cleanup and discovery
    path in this repo globs `output_*.grib`, which does not match `.grib.tmp`, so
    a leaked partial would sit on the data array invisibly until the free-space
    check parked all further downloads for good.

    Args:
        request: MARS request dictionary, e.g. from build_mars_request().
        target: Path to write the retrieved data to.

    Returns:
        The path to the retrieved file.
    """
    target = pathlib.Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.with_name(target.name + ".tmp")
    logger.info(f"Submitting MARS request: {request} -> {target}")
    server = mars_service()
    try:
        server.execute(request, str(tmp_target))
    except BaseException:
        # BaseException, not Exception: a KeyboardInterrupt part way through a
        # multi-hour retrieval is the most likely way this leaks.
        tmp_target.unlink(missing_ok=True)
        raise
    tmp_target.rename(target)
    logger.info(f"MARS retrieval complete: {target}")
    return str(target)


def retrieve_mars_chunked(
    start: pd.Timestamp,
    end: pd.Timestamp,
    days_per_request: int = 3,
    target_template: str = "output_{start}_{end}.grib",
    skip_existing: bool = True,
    target_dir: str | pathlib.Path | None = None,
    **request_kwargs,
) -> list[str]:
    """Retrieve a date range from MARS in multi-day chunks.

    MARS limits requests to 75GB. The default ml-level analysis request is
    roughly 13-20GB per day, so 3 days per request keeps each chunk safely
    under the limit while minimising the number of MARS requests.

    Args:
        start: First date to retrieve.
        end: Last date to retrieve (inclusive).
        days_per_request: Number of days per MARS request. Reduce this if
            adding params/levels/times pushes a chunk over 75GB.
        target_template: Target filename template, formatted with {start} and
            {end} as YYYYMMDD.
        skip_existing: Skip chunks whose target file already exists.
        target_dir: Directory to write into. Defaults to `default_target_dir`.
        **request_kwargs: Overrides passed through to build_mars_request().

    Returns:
        List of paths to the retrieved files.
    """
    directory = pathlib.Path(target_dir) if target_dir is not None else default_target_dir()
    targets = []
    days = pd.date_range(start=start, end=end, freq="1D")
    for i in range(0, len(days), days_per_request):
        chunk_start, chunk_end = days[i], days[min(i + days_per_request - 1, len(days) - 1)]
        target = str(
            directory
            / target_template.format(
                start=chunk_start.strftime("%Y%m%d"), end=chunk_end.strftime("%Y%m%d")
            )
        )
        if skip_existing and pathlib.Path(target).exists():
            logger.info(f"{target} already exists, skipping")
            targets.append(target)
            continue
        request = build_mars_request(start=chunk_start, end=chunk_end, **request_kwargs)
        targets.append(retrieve_mars(request, target))
    return targets


def retrieve_mars_hourly(
    start: pd.Timestamp,
    end: pd.Timestamp,
    days_per_group: int = 6,
    analysis_days_per_request: int = 3,
    forecast_days_per_request: int = 2,
    wave_days_per_request: int = 6,
    spectra_days_per_request: int = 2,
    spectra_forecast_days_per_request: int = 3,
    skip_existing: bool = True,
    target_dir: str | pathlib.Path | None = None,
) -> list[str]:
    """Retrieve hourly atmospheric and wave data from MARS between start and end.

    Downloads are grouped by date: all products for one group of days are
    retrieved before moving on to the next group. Per group, in order:

    1. Model-level analyses at 00/06/12/18.
    2. Model-level forecast steps 1-5 per init time, filling in-between hours.
    3. 3-hourly ocean wave analyses.
    4. 3-hourly 2D wave spectra analyses.
    5. 2D wave spectra forecast steps 1-5 per init time.

    Each MARS request must stay under 75GB; chunk sizes are chosen to be as
    large as possible while staying safely below that:

    - ml analysis is ~13-20GB/day -> 3 days per request.
    - ml forecast is ~23GB/day per init time -> 2 days per request, and the
      four init times cannot share one request (~92GB/day combined).
    - wave analysis is small (single-level fields, ~1-3GB/day) -> the whole
      group in one request.
    - wave spectra analysis is 36x29 bins x 8 times = 8352 fields at ~3.96MB
      each, ~33.1GB/day (measured by MARS) -> 2 days per request (~66GB).
    - wave spectra forecast is 36x29 bins x 5 steps = 5220 fields per init
      time, ~20.7GB/day -> 3 days per request per init time (~62GB).

    If MARS rejects a request as too large, reduce the corresponding
    days_per_request.

    Args:
        start: First date to retrieve.
        end: Last date to retrieve (inclusive).
        days_per_group: Days per group; all products for a group are fetched
            before the next group starts.
        analysis_days_per_request: Days per ml analysis MARS request.
        forecast_days_per_request: Days per ml forecast MARS request (per init time).
        wave_days_per_request: Days per wave analysis MARS request.
        spectra_days_per_request: Days per wave spectra analysis MARS request.
        spectra_forecast_days_per_request: Days per wave spectra forecast MARS
            request (per init time).
        skip_existing: Skip chunks whose target file already exists.
        target_dir: Directory to write into. Defaults to `default_target_dir`.

    Returns:
        List of paths to the retrieved files.
    """
    directory = pathlib.Path(target_dir) if target_dir is not None else default_target_dir()
    targets = []
    days = pd.date_range(start=start, end=end, freq="1D")
    for i in range(0, len(days), days_per_group):
        group_start = days[i]
        group_end = days[min(i + days_per_group - 1, len(days) - 1)]
        # 1. Model-level analyses
        targets += retrieve_mars_chunked(
            start=group_start,
            end=group_end,
            days_per_request=analysis_days_per_request,
            target_template="output_an_{start}_{end}.grib",
            skip_existing=skip_existing,
            target_dir=directory,
        )
        # 2. Model-level forecast steps filling the in-between hours
        for time in ANALYSIS_TIMES.split("/"):
            targets += retrieve_mars_chunked(
                start=group_start,
                end=group_end,
                days_per_request=forecast_days_per_request,
                target_template=f"output_fc_{time[:2]}z_{{start}}_{{end}}.grib",
                skip_existing=skip_existing,
                target_dir=directory,
                type_="fc",
                stream=forecast_stream(time),
                param=FORECAST_ML_PARAMS,
                time=time,
                step=FORECAST_STEPS,
            )
        # 3. Ocean wave analyses
        targets += retrieve_mars_chunked(
            start=group_start,
            end=group_end,
            days_per_request=wave_days_per_request,
            target_template="output_wave_an_{start}_{end}.grib",
            skip_existing=skip_existing,
            target_dir=directory,
            stream="wave",
            domain="g",
            levtype=None,
            param=WAVE_PARAMS,
            time=WAVE_TIMES,
        )
        # 4. 2D wave spectra analyses
        targets += retrieve_mars_chunked(
            start=group_start,
            end=group_end,
            days_per_request=spectra_days_per_request,
            target_template="output_spectra_an_{start}_{end}.grib",
            skip_existing=skip_existing,
            target_dir=directory,
            stream="wave",
            domain="g",
            levtype=None,
            param=SPECTRA_PARAM,
            time=WAVE_TIMES,
            direction=SPECTRA_DIRECTIONS,
            frequency=SPECTRA_FREQUENCIES,
        )
        # 5. 2D wave spectra forecast steps filling the in-between hours
        for time in ANALYSIS_TIMES.split("/"):
            targets += retrieve_mars_chunked(
                start=group_start,
                end=group_end,
                days_per_request=spectra_forecast_days_per_request,
                target_template=f"output_spectra_fc_{time[:2]}z_{{start}}_{{end}}.grib",
                skip_existing=skip_existing,
                target_dir=directory,
                stream=forecast_stream(time, "wave"),
                domain="g",
                levtype=None,
                type_="fc",
                param=SPECTRA_PARAM,
                time=time,
                step=FORECAST_STEPS,
                direction=SPECTRA_DIRECTIONS,
                frequency=SPECTRA_FREQUENCIES,
            )
    return targets


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start", required=True, help="First date to retrieve, e.g. 2026-08-01")
    parser.add_argument("--end", required=True, help="Last date to retrieve, inclusive")
    parser.add_argument(
        "--target-dir",
        default=None,
        help="Directory to write the GRIB into (default: <PLANETARY_DATASETS_DATA_DIR>/mars)",
    )
    args = parser.parse_args()
    retrieve_mars_hourly(
        start=pd.Timestamp(args.start),
        end=pd.Timestamp(args.end),
        target_dir=args.target_dir,
    )
