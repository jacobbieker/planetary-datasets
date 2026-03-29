from __future__ import annotations

import os
from typing import List, Optional

import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider

# Match the constants used in the Dagster assets; make configurable via env var
BASE_URL = "s3://noaa-gmgsi-pds/"
ARCHIVE_FOLDER = os.environ.get("GMGSI_ARCHIVE", "/ext_data/gmgsi/")


class GMGSIProvider(BaseProvider):
    """Provider for NOAA GMGSI v3 global mosaic (v3r0_blend).

    This provider implements the minimal `fetch` and `process` methods used by
    `planetary_datasets.base.BaseProvider`.
    """

    name = "gmgsi_v3"
    append_dim = "time"
    # Default icechunk path; override if you want a different store
    icechunk_path = "s3://us-west-2.opendata.source.coop/bkr/gmgi/gmgsi_v3.icechunk"

    def fetch(self, it: pd.Timestamp, channels: Optional[List[str]] = None, tmpdir: Optional[str] = None, **kwargs) -> List[str]:
        """Download GMGSI v3 files for the given partition timestamp.

        Returns a list of local file paths. If any expected file is missing the
        function returns an empty list (so callers can skip processing).
        """
        if channels is None:
            channels = ["VIS", "WV", "LW", "SW"]

        fs = s3fs.S3FileSystem(anon=True)
        downloaded_files: List[str] = []

        # If a tmpdir was provided by BaseProvider.run_partition, prefer that
        # for temporary downloads. Otherwise fall back to the persistent
        # ARCHIVE_FOLDER (keeps behavior compatible with previous implementation).
        use_dir = tmpdir if tmpdir is not None else ARCHIVE_FOLDER

        for channel in channels:
            if channel == "VIS":
                pattern = (
                    f"{BASE_URL}GMGSI_VIS/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/"
                    f"GLOBCOMPVIS_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"
                )
            elif channel == "WV":
                pattern = (
                    f"{BASE_URL}GMGSI_WV/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/"
                    f"GLOBCOMPWV_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"
                )
            elif channel == "LW":
                # LW files use the LIR name
                pattern = (
                    f"{BASE_URL}GMGSI_LW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/"
                    f"GLOBCOMPLIR_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"
                )
            elif channel == "SW":
                # SW files use the SIR name
                pattern = (
                    f"{BASE_URL}GMGSI_SW/{it.year}/{it.month:02}/{it.day:02}/{it.hour:02}/"
                    f"GLOBCOMPSIR_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"
                )
            else:
                raise ValueError(f"Unrecognized channel {channel!r}")

            matches = list(fs.glob(pattern))
            if not matches:
                logger.warning("GMGSI v3: no match for pattern %s", pattern)
                return []

            # fs.glob may return a path without the s3:// prefix in this environment,
            # so prepend it to get a valid s3 URI for fs.get
            s3_uri = f"s3://{matches[0]}"
            # compute local path within chosen directory
            # matches[0] may be a list element or a string; ensure it's a string
            matched = matches[0] if isinstance(matches[0], str) else str(matches[0])
            local_name = os.path.basename(matched)
            local_uri = os.path.join(use_dir, local_name)
            os.makedirs(os.path.dirname(local_uri), exist_ok=True)
            if not os.path.exists(local_uri):
                fs.get(s3_uri, local_uri)
                logger.info("Downloaded %s to %s", s3_uri, local_uri)
            else:
                logger.debug("Already downloaded %s", local_uri)
            downloaded_files.append(local_uri)

        return downloaded_files

    def process(self, input_files: List[str], it: pd.Timestamp, tmpdir: Optional[str] = None):
        """Read local GMGSI v3 files and return a merged xarray.Dataset.

        The processing mirrors the logic used previously in the Dagster asset:
        - fill NaN dqf with 255
        - rename `data` -> channel name and `dqf` -> `<channel>_dqf`
        - drop other data variables
        - cast to uint8
        """
        if not input_files:
            raise ValueError("No input files provided to GMGSIProvider.process")

        datasets_to_merge: List[xr.Dataset] = []

        for fp in input_files:
            ds = xr.open_dataset(fp)
            ds = ds.load()

            fname = os.path.basename(fp)
            if "GLOBCOMPVIS" in fname:
                if "dqf" in ds:
                    ds["dqf"] = ds["dqf"].fillna(255)
                ds = ds.rename({"data": "vis", "dqf": "vis_dqf"})
                keep = {"vis", "vis_dqf"}
            elif "GLOBCOMPWV" in fname:
                if "dqf" in ds:
                    ds["dqf"] = ds["dqf"].fillna(255)
                ds = ds.rename({"data": "wv", "dqf": "wv_dqf"})
                keep = {"wv", "wv_dqf"}
            elif "GLOBCOMPLIR" in fname:
                if "dqf" in ds:
                    ds["dqf"] = ds["dqf"].fillna(255)
                ds = ds.rename({"data": "lwir", "dqf": "lwir_dqf"})
                keep = {"lwir", "lwir_dqf"}
            elif "GLOBCOMPSIR" in fname:
                if "dqf" in ds:
                    ds["dqf"] = ds["dqf"].fillna(255)
                ds = ds.rename({"data": "swir", "dqf": "swir_dqf"})
                keep = {"swir", "swir_dqf"}
            else:
                raise ValueError(f"Unrecognized GMGSI v3 filename: {fname}")

            # drop unexpected data variables
            for dv in list(ds.data_vars):
                if dv not in keep:
                    # drop_vars accepts a name or list of names — pass a list to
                    # avoid mypy/lsp complaints about Hashable vs str
                    ds = ds.drop_vars([dv])

            datasets_to_merge.append(ds.astype(np.uint8))

        merged = xr.merge(datasets_to_merge)

        # Try best-effort cleanup of the downloaded files, but only when the
        # files were downloaded into a temporary directory provided by the
        # BaseProvider orchestration. When tmpdir is None we assume files are
        # persistent archive files and should not be removed.
        if tmpdir is not None:
            try:
                parent = os.path.dirname(input_files[0])
                for fp in input_files:
                    try:
                        os.remove(fp)
                    except Exception:
                        pass
                if os.path.isdir(parent) and not os.listdir(parent):
                    try:
                        os.rmdir(parent)
                    except Exception:
                        pass
            except Exception:
                logger.debug("Failed to fully clean up GMGSI v3 temp files", exc_info=True)

        return merged


def get_global_mosaic_v3(time: pd.Timestamp, channels: Optional[List[str]] = None) -> xr.Dataset:
    """Convenience wrapper used by existing assets — delegates to GMGSIProvider."""
    provider = GMGSIProvider()
    files = provider.fetch(time, channels=channels)
    if not files:
        raise FileNotFoundError(f"No GMGSI v3 inputs found for {time}")
    return provider.process(files, time)


__all__ = ["GMGSIProvider", "get_global_mosaic_v3"]

if __name__ == "__main__":
    import pandas as pd
    date_range = pd.date_range("2025-03-01", "2026-06-02", freq="1H")
    provider = GMGSIProvider()
    for it in date_range:
        print(f"Processing {it}")
        provider.run_partition(it)