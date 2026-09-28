"""GPM/TRMM CORRA combined radar-radiometer precipitation retrievals.

CORRA (the Combined Radar-Radiometer Algorithm) fuses the precipitation radar and the
microwave imager on the same platform into a single level-2 swath retrieval. Two products
cover the full record:

``2B-TRMM-CORRA``
    TRMM PR + TMI, 1997-12 to 2015-04. :class:`TRMMCorraProvider`.
``2B-GPM-CORRA``
    GPM DPR + GMI, 2014-03 onwards. :class:`GPMCorraProvider`.

Granules are found and downloaded with the ``gpm`` package, which needs both a NASA PPS
account and a NASA Earthdata account. Those come from ``GPM_PPS_USERNAME`` /
``GPM_PPS_PASSWORD`` and ``EARTHDATA_USERNAME`` / ``EARTHDATA_PASSWORD``; they are applied
to the in-process ``gpm`` configuration rather than written to ``~/.config_gpm_api.yaml``,
so running an ingest never leaves credentials on the host.

A second download path is kept for the case where PPS is unavailable: GES DISC serves the
same granules over HTTPS behind Earthdata Login, which :func:`gesdisc_download` mirrors with
``wget`` and a URS cookie jar.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
from typing import List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.common.store import missing_periods
from planetary_datasets.config import Config, get_config

#: GES DISC is the Earthdata mirror of the PPS research archive.
GESDISC_ROOT = "https://gpm1.gesdisc.eosdis.nasa.gov/data"

#: One day, with an explicit unit. ``pd.Timedelta(days=1)`` carries numpy's "generic" unit,
#: which is deprecated in arithmetic against a datetime.
ONE_DAY = pd.Timedelta(1, "D")


def gpm_base_dir(config: Config | None = None) -> pathlib.Path:
    """Directory the ``gpm`` package keeps its local archive in.

    ``gpm`` insists the path ends in ``GPM``, so this is always ``<data_dir>/GPM``.
    """
    cfg = config if config is not None else get_config()
    return pathlib.Path(cfg.data_dir) / "GPM"


def configure_gpm(
    config: Config | None = None,
    base_dir: str | os.PathLike | None = None,
    setup_ges_disc: bool = False,
) -> pathlib.Path:
    """Apply credentials and the archive location to the in-process ``gpm`` configuration.

    Args:
        config: Configuration to read credentials from. Defaults to the process-wide one.
        base_dir: Override the local archive directory. Must end in ``GPM``.
        setup_ges_disc: Also write the ``~/.netrc`` and ``~/.urs_cookies`` entries that
            Earthdata Login needs for GES DISC downloads. Off by default because it writes
            the Earthdata password to the home directory.

    Returns:
        The archive directory, which is created if it does not exist.
    """
    import gpm

    cfg = config if config is not None else get_config()
    pps_username, pps_password = cfg.credentials.require("gpm_pps_username", "gpm_pps_password")
    earthdata_username, earthdata_password = cfg.credentials.require(
        "earthdata_username", "earthdata_password"
    )

    directory = pathlib.Path(base_dir) if base_dir is not None else gpm_base_dir(cfg)
    directory.mkdir(parents=True, exist_ok=True)

    gpm.config.set(
        {
            "base_dir": str(directory),
            "username_pps": pps_username,
            "password_pps": pps_password,
            "username_earthdata": earthdata_username,
            "password_earthdata": earthdata_password,
        }
    )

    if setup_ges_disc:
        from gpm.configs import set_ges_disc_authentification

        set_ges_disc_authentification(earthdata_username, earthdata_password)

    logger.debug(f"gpm configured against {directory}")
    return directory


def gesdisc_download(
    day: pd.Timestamp,
    dest_dir: str | os.PathLike,
    collection: str = "GPM_L2/GPM_2BCMB.07",
    cookies: str | os.PathLike = "~/.urs_cookies",
    timeout: float | None = None,
) -> pathlib.Path:
    """Mirror one day of a GES DISC collection with ``wget``.

    The fallback for when PPS is down. Earthdata Login redirects to an authentication host
    and back, so the cookie jar has to be both read and written; that is the whole reason
    this shells out to ``wget`` instead of using fsspec.

    Returns the directory the day was mirrored into.
    """
    day = pd.Timestamp(day)
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    cookies = pathlib.Path(cookies).expanduser()

    url = f"{GESDISC_ROOT}/{collection}/{day:%Y}/{day.timetuple().tm_yday:03d}/"
    args = [
        "wget",
        "--load-cookies", str(cookies),
        "--save-cookies", str(cookies),
        "--keep-session-cookies",
        "--content-disposition",
        "--recursive",
        "--continue",
        "--no-parent",
        "--no-verbose",
        url,
        "-P", str(dest_dir),
    ]
    logger.info(f"mirroring {url}")
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        # wget returns 8 for "server issued an error response", which includes the 404 of a
        # day with no granules. That is a gap, not a failure.
        logger.warning(f"wget exited {result.returncode} for {url}: {result.stderr.strip()[:400]}")
    return dest_dir


class CorraProvider(BaseProvider):
    """One day of CORRA swath retrievals, appended along the granule time axis.

    Subclasses pin :attr:`product` and :attr:`scan_mode`; everything else is shared.
    """

    #: GPM product acronym, see ``gpm.available_products(product_categories="CMB")``.
    product: str
    #: Which of the product's scan modes to read. CORRA files carry more than one.
    scan_mode: str
    #: GES DISC collection used by the fallback download path.
    gesdisc_collection: str
    #: First and last day the product covers.
    start_date: pd.Timestamp
    end_date: pd.Timestamp | None = None

    append_dim = "time"
    partition_freq = "D"

    def __init__(
        self,
        config: Config | None = None,
        variables: Sequence[str] | None = None,
        n_threads: int = 4,
        transfer_tool: str = "CURL",
        version: int | None = None,
        base_dir: str | os.PathLike | None = None,
    ):
        super().__init__(config=config)
        self.variables = list(variables) if variables is not None else None
        self.n_threads = n_threads
        self.transfer_tool = transfer_tool
        self.version = version
        self._base_dir = pathlib.Path(base_dir) if base_dir is not None else None

    @property
    def base_dir(self) -> pathlib.Path:
        """Local ``gpm`` archive directory for this provider."""
        return self._base_dir if self._base_dir is not None else gpm_base_dir(self.config)

    def configure(self) -> pathlib.Path:
        """Push credentials and the archive directory into the ``gpm`` configuration."""
        return configure_gpm(self.config, base_dir=self._base_dir)

    def covers(self, it: pd.Timestamp) -> bool:
        """True when the product was flying on the given day."""
        it = pd.Timestamp(it).normalize()
        if it < pd.Timestamp(self.start_date).normalize():
            return False
        return self.end_date is None or it <= pd.Timestamp(self.end_date).normalize()

    def missing_timesteps(self, desired: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """Return the days not yet in the store.

        The store's time axis holds per-granule scan times, so a partition day is never
        literally one of its values. Presence has to be judged a day at a time, otherwise
        every run would re-download and re-append the whole archive.
        """
        missing = missing_periods(
            self.get_icechunk_repo(),
            list(desired),
            unit="D",
            append_dim=self.append_dim,
        )
        return [pd.Timestamp(t) for t in missing]

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        import gpm

        it = pd.Timestamp(it).normalize()
        if not self.covers(it):
            logger.debug(f"{self.name}: {it:%Y-%m-%d} is outside the product's lifetime")
            return []

        self.configure()
        start = it.to_pydatetime()
        end = (it + ONE_DAY).to_pydatetime()

        download_error: Exception | None = None
        try:
            gpm.download(
                product=self.product,
                product_type="RS",
                start_time=start,
                end_time=end,
                version=self.version,
                n_threads=self.n_threads,
                transfer_tool=self.transfer_tool,
                progress_bar=False,
                check_integrity=True,
                remove_corrupted=True,
                retry=2,
                verbose=False,
            )
        except Exception as exc:  # noqa: BLE001 - granules already on disk may still do
            download_error = exc
            logger.warning(f"{self.name}: download failed for {it:%Y-%m-%d}: {exc}")

        filepaths = gpm.find_files(
            storage="LOCAL",
            product=self.product,
            product_type="RS",
            start_time=start,
            end_time=end,
            version=self.version,
            verbose=False,
        )
        if not filepaths and download_error is not None:
            # An empty result means "no granules that day", which is a normal gap. But if
            # the transfer itself failed there is nothing to distinguish an outage or an
            # expired password from a gap, and a whole backfill would otherwise report
            # success while writing nothing.
            raise RuntimeError(
                f"{self.name}: download failed for {it:%Y-%m-%d} and no granules are on disk"
            ) from download_error

        logger.info(f"{self.name}: {len(filepaths)} granule(s) for {it:%Y-%m-%d}")
        return [str(p) for p in filepaths]

    def open_granule(self, filepath: str) -> xr.Dataset:
        """Open one granule's swath for the configured scan mode."""
        import gpm

        return gpm.open_granule_dataset(
            filepath,
            scan_mode=self.scan_mode,
            variables=self.variables,
            decode_cf=True,
            chunks={},
        )

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        it = pd.Timestamp(it).normalize()
        granules: list[xr.Dataset] = []
        for filepath in sorted(input_files):
            try:
                granules.append(self.open_granule(filepath))
            except Exception as exc:  # noqa: BLE001 - one bad granule must not lose the day
                logger.warning(f"{self.name}: could not open {filepath}: {exc}")

        if not granules:
            raise ValueError(f"{self.name}: no readable granules for {it:%Y-%m-%d}")

        ds = xr.concat(granules, dim="along_track", coords="minimal", compat="override")
        return swath_to_time_series(ds, day=it)


def swath_to_time_series(ds: xr.Dataset, day: pd.Timestamp | None = None) -> xr.Dataset:
    """Turn a level-2 swath into something appendable along ``time``.

    ``time`` is a coordinate on ``along_track`` in the source files. Promoting it to the
    dimension makes the store a continuous, sorted record that new days extend, which is
    what an append-along-time store needs.

    When ``day`` is given the result is clipped to that day: granules straddle midnight, so
    without clipping consecutive partitions would overlap and write the same scans twice.
    """
    if "time" not in ds.coords:
        raise ValueError("swath has no time coordinate to index by")

    if "along_track" in ds.dims and "time" not in ds.dims:
        ds = ds.swap_dims({"along_track": "time"})
    ds = ds.drop_vars("along_track", errors="ignore")

    times = pd.DatetimeIndex(ds["time"].values)
    keep = ~times.isna()
    if day is not None:
        day = pd.Timestamp(day).normalize()
        keep &= (times >= day) & (times < day + ONE_DAY)
    if not keep.any():
        raise ValueError(
            f"swath has no scans inside {day:%Y-%m-%d}"
            if day is not None
            else "swath has no valid times"
        )
    ds = ds.isel(time=np.flatnonzero(keep))

    ds = ds.sortby("time")
    # Consecutive granules can repeat a scan at their shared boundary.
    unique = ~pd.DatetimeIndex(ds["time"].values).duplicated()
    if not unique.all():
        ds = ds.isel(time=np.flatnonzero(unique))
    return ds


class GPMCorraProvider(CorraProvider):
    """GPM DPR + GMI combined retrieval, 2014-03 onwards."""

    name = "gpm-corra"
    product = "2B-GPM-CORRA"
    scan_mode = "KuGMI"
    gesdisc_collection = "GPM_L2/GPM_2BCMB.07"
    store_prefix = "bkr/gpm/corra_gpm.icechunk"
    start_date = pd.Timestamp("2014-03-09")


class TRMMCorraProvider(CorraProvider):
    """TRMM PR + TMI combined retrieval, 1997-12 to the end of the mission in 2015-04."""

    name = "trmm-corra"
    product = "2B-TRMM-CORRA"
    scan_mode = "KuTMI"
    gesdisc_collection = "TRMM_L2/GPM_2BCMBTRMM.07"
    store_prefix = "bkr/gpm/corra_trmm.icechunk"
    start_date = pd.Timestamp("1997-12-08")
    end_date = pd.Timestamp("2015-04-01")


#: Every CORRA provider, keyed by name, for the Dagster assets and the CLI.
PROVIDERS: dict[str, type[CorraProvider]] = {
    cls.name: cls for cls in (GPMCorraProvider, TRMMCorraProvider)
}


__all__ = [
    "GESDISC_ROOT",
    "PROVIDERS",
    "CorraProvider",
    "GPMCorraProvider",
    "TRMMCorraProvider",
    "configure_gpm",
    "gesdisc_download",
    "gpm_base_dir",
    "swath_to_time_series",
]
