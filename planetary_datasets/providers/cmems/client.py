"""Thin, credential-safe wrapper around the Copernicus Marine (CMEMS) toolbox.

Seven standalone scripts (``cop_marine.py``, ``d_cop_marine3..6.py``,
``download_cop_marine_2.py``, ``download_coperinicus_marine.py``) each called
``copernicusmarine.get`` with the *same* hardcoded username and password, differing only in
the dataset id and a machine-specific output directory. All of that collapses to the
helpers here: the dataset ids live in :data:`BULK_DATASETS`, credentials come from
:class:`~planetary_datasets.config.Config` and downloads land under ``cfg.data_dir``.
"""

from __future__ import annotations

import pathlib
from typing import Iterable, Sequence

import pandas as pd
from loguru import logger

from planetary_datasets.config import Config, get_config

#: Native (already-gridded) CMEMS datasets that are mirrored wholesale with
#: ``copernicusmarine.get``. Keys are the short names used by the Dagster assets and the
#: CLI; values are the catalogue dataset ids.
BULK_DATASETS: dict[str, str] = {
    # GLOBAL_MULTIYEAR_WAV_001_032 — wave reanalysis, 0.2 degree, 3 hourly.
    "global_wave_reanalysis": "cmems_mod_glo_wav_my_0.2deg_PT3H-i",
    # GLOBAL_ANALYSISFORECAST_PHY_001_024 — analysis and forecast, 0.083 degree.
    "global_phy_hourly": "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
    "global_phy_sea_level": "cmems_mod_glo_phy_anfc_merged-sl_PT1H-i",
    "global_phy_surface_currents": "cmems_mod_glo_phy_anfc_merged-uv_PT1H-i",
    "global_phy_temperature": "cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i",
    "global_phy_salinity": "cmems_mod_glo_phy-so_anfc_0.083deg_PT6H-i",
    "global_phy_currents": "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i",
    # WIND_GLO_PHY_L4_NRT_012_004 — near real time global ocean winds.
    "global_wind_nrt": "cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H",
}

#: Datasets the analysis/forecast mirror job pulls, in the order the original script used.
ANALYSIS_FORECAST_DATASETS: tuple[str, ...] = (
    "global_phy_sea_level",
    "global_phy_hourly",
    "global_phy_surface_currents",
    "global_phy_temperature",
    "global_phy_salinity",
    "global_phy_currents",
)


def resolve_dataset_id(dataset: str) -> str:
    """Return the CMEMS dataset id for a short name, or ``dataset`` if it is already one.

    Args:
        dataset: Short name from :data:`BULK_DATASETS`, or a raw catalogue dataset id.
    """
    return BULK_DATASETS.get(dataset, dataset)


def credentials(config: Config | None = None) -> tuple[str, str]:
    """Return the configured Copernicus Marine username and password.

    Raises:
        planetary_datasets.config.MissingCredential: When either is unset. Failing here is
            deliberate: the toolbox otherwise falls back to an anonymous request and
            reports an opaque authentication error much later.
    """
    from planetary_datasets.config import MissingCredential

    cfg = config if config is not None else get_config()
    try:
        user, password = cfg.credentials.require(
            "copernicusmarine_username", "copernicusmarine_password"
        )
    except MissingCredential as exc:
        # Re-raised naming the variables as they actually appear in the environment; the
        # generic message derives them from the field names, which are shorter.
        raise MissingCredential(
            "Copernicus Marine credentials are not configured. Set "
            "COPERNICUSMARINE_SERVICE_USERNAME and COPERNICUSMARINE_SERVICE_PASSWORD in "
            f".env or the environment. ({exc})"
        ) from exc
    return user, password


def dataset_dir(dataset: str, config: Config | None = None) -> pathlib.Path:
    """Local mirror directory for a dataset, under the configured data directory."""
    cfg = config if config is not None else get_config()
    return cfg.data_dir / "cmems" / resolve_dataset_id(dataset)


def _get_response_paths(result) -> list[pathlib.Path]:
    """Extract the downloaded file paths from a ``copernicusmarine.get`` response."""
    paths: list[pathlib.Path] = []
    for file in getattr(result, "files", None) or []:
        file_path = getattr(file, "file_path", None)
        if file_path is not None:
            paths.append(pathlib.Path(str(file_path)))
    return paths


def download_dataset(
    dataset: str,
    output_directory: str | pathlib.Path | None = None,
    file_filter: str | None = None,
    skip_existing: bool = True,
    no_directories: bool = False,
    config: Config | None = None,
    **kwargs,
) -> list[pathlib.Path]:
    """Mirror a native CMEMS dataset to local disk with ``copernicusmarine.get``.

    Args:
        dataset: Short name from :data:`BULK_DATASETS` or a raw catalogue dataset id.
        output_directory: Where to write. Defaults to ``<data_dir>/cmems/<dataset_id>``.
        file_filter: Optional glob applied to the remote file list, e.g. ``"*2026/*"`` to
            pull a single year. Passed to the toolbox's ``filter`` argument.
        skip_existing: Leave files that are already on disk alone.
        no_directories: Flatten the remote directory tree into ``output_directory``.
        config: Configuration override, mostly for tests.
        **kwargs: Passed straight through to ``copernicusmarine.get``.

    Returns:
        Paths of the files the toolbox reports as downloaded.
    """
    import copernicusmarine

    cfg = config if config is not None else get_config()
    dataset_id = resolve_dataset_id(dataset)
    user, password = credentials(cfg)

    out = (
        pathlib.Path(output_directory)
        if output_directory is not None
        else dataset_dir(dataset_id, cfg)
    )
    out.mkdir(parents=True, exist_ok=True)

    logger.info(f"cmems: downloading {dataset_id} into {out}")
    result = copernicusmarine.get(
        dataset_id=dataset_id,
        output_directory=str(out),
        no_directories=no_directories,
        skip_existing=skip_existing,
        filter=file_filter,
        username=user,
        password=password,
        **kwargs,
    )
    paths = _get_response_paths(result)
    logger.info(f"cmems: {dataset_id} -> {len(paths)} file(s)")
    return paths


def download_datasets(
    datasets: Iterable[str],
    config: Config | None = None,
    **kwargs,
) -> dict[str, list[pathlib.Path]]:
    """Mirror several datasets, continuing past ones that fail.

    The original scripts wrapped each dataset in ``try``/``except`` so one unavailable
    product did not abort the whole nightly mirror; that behaviour is kept.

    Returns:
        Mapping of dataset id to downloaded paths. Datasets that failed map to an empty
        list.
    """
    results: dict[str, list[pathlib.Path]] = {}
    for dataset in datasets:
        dataset_id = resolve_dataset_id(dataset)
        try:
            results[dataset_id] = download_dataset(dataset, config=config, **kwargs)
        except Exception as exc:  # noqa: BLE001 - one bad product must not stop the mirror
            logger.error(f"cmems: failed to download {dataset_id}: {exc}")
            results[dataset_id] = []
    return results


def _permanent_errors() -> tuple[type[BaseException], ...]:
    """Toolbox exceptions that mean the request itself is wrong, not that data is missing.

    These must not be swallowed: a misspelled dataset id or variable would otherwise look
    exactly like an archive gap and the provider would report success while writing
    nothing, forever.
    """
    import copernicusmarine

    names = (
        "DatasetNotFound",
        "DatasetVersionNotFound",
        "DatasetVersionPartNotFound",
        "ProductNotFound",
        "VariableDoesNotExistInTheDataset",
        "FormatNotSupported",
        "ServiceNotSupported",
        "ServiceDoesNotExistForCommand",
        "WrongFieldsError",
    )
    return tuple(
        error
        for error in (getattr(copernicusmarine, name, None) for name in names)
        if isinstance(error, type) and issubclass(error, BaseException)
    )


def subset_day(
    dataset_id: str,
    variables: Sequence[str] | None,
    day: pd.Timestamp,
    output_directory: str | pathlib.Path,
    output_filename: str,
    config: Config | None = None,
    **kwargs,
) -> pathlib.Path | None:
    """Download one UTC day of a dataset as a single NetCDF via ``copernicusmarine.subset``.

    Args:
        dataset_id: Catalogue dataset id.
        variables: Variables to request, or None for all of them.
        day: Midnight-UTC timestamp of the day to fetch.
        output_directory: Directory to write into; created if missing.
        output_filename: File name to write.
        config: Configuration override, mostly for tests.
        **kwargs: Passed straight through to ``copernicusmarine.subset``.

    Returns:
        Path of the downloaded file, or None when the day is unavailable or the download
        failed transiently.

    Raises:
        Exception: Toolbox errors that mean the request is malformed - an unknown dataset
            id or variable - are re-raised rather than reported as a missing day.
    """
    import copernicusmarine

    cfg = config if config is not None else get_config()
    user, password = credentials(cfg)

    out_dir = pathlib.Path(output_directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    expected = out_dir / output_filename

    try:
        result = copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=list(variables) if variables else None,
            start_datetime=day,
            # 23:59 rather than the next midnight, so a day is never fetched twice and
            # appends along time cannot collide with the following partition.
            end_datetime=day + pd.Timedelta(23, "h") + pd.Timedelta(59, "min"),
            output_directory=str(out_dir),
            output_filename=output_filename,
            file_format="netcdf",
            username=user,
            password=password,
            **kwargs,
        )
    except _permanent_errors():
        logger.error(f"cmems: request for {dataset_id} is not valid, not retrying")
        raise
    except Exception as exc:  # noqa: BLE001 - archive gaps and transient errors are normal
        logger.error(f"cmems: failed to subset {dataset_id} for {day}: {exc}")
        return None

    file_path = getattr(result, "file_path", None)
    path = pathlib.Path(str(file_path)) if file_path is not None else expected
    if not path.exists():
        logger.error(f"cmems: subset of {dataset_id} at {day} produced no file at {path}")
        return None
    return path
