"""Global CMEMS ocean providers built on the locally mirrored native files.

``wave_reanalysis.py`` and ``wavey.py`` were a duplicate pair: both renamed variables to
their ``standard_name``, both built the same zstd/bitshuffle encoding and both appended one
day at a time to an Icechunk store, one over the global wave reanalysis and the other over
the three global analysis/forecast products merged onto a common grid. Both are replaced by
the two providers here, which share :func:`rename_to_standard_names` and inherit the
encoding and append logic from :class:`~planetary_datasets.base.BaseProvider`.

Inputs come from the local mirror maintained by
:func:`~planetary_datasets.providers.cmems.client.download_dataset`. When a day is not
mirrored yet, :meth:`CMEMSGlobalWaveReanalysisProvider.fetch` pulls just that day.
"""

from __future__ import annotations

import pathlib
from typing import List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.providers.cmems.client import (
    BULK_DATASETS,
    dataset_dir,
    download_dataset,
)


def rename_to_standard_names(ds: xr.Dataset) -> xr.Dataset:
    """Rename data variables to their CF ``standard_name``.

    CMEMS ships terse variable names (``VHM0``, ``zos``) that differ between products even
    when they hold the same field, so the products cannot be merged as-is. Renaming to the
    standard name makes them line up.

    Variables without a ``standard_name``, and any whose standard name would collide with a
    name already in use, keep their original name.
    """
    renames: dict[str, str] = {}
    taken = {str(v) for v in ds.variables}
    for var in ds.data_vars:
        standard_name = str(ds[var].attrs.get("standard_name", var))
        if standard_name == str(var):
            continue
        if standard_name in taken:
            logger.warning(
                f"not renaming {var} to {standard_name}: that name is already in use"
            )
            continue
        renames[var] = standard_name
        taken.discard(str(var))
        taken.add(standard_name)
    return ds.rename(renames) if renames else ds


def _day_files(root: pathlib.Path, day: pd.Timestamp) -> list[pathlib.Path]:
    """Every NetCDF under ``root`` whose *valid date* is ``day``.

    CMEMS names its files ``<model>_<validdate><hour>_R<productiondate>.nc``, so a plain
    substring match on ``YYYYMMDD`` also catches the file produced on that day, which holds
    the *previous* day's data. Matches where the date only ever appears as the ``R`` stamp
    are therefore dropped; otherwise every partition would ingest the day before it as
    well and duplicate those timesteps in the store.
    """
    if not root.exists():
        return []
    stamp = f"{day:%Y%m%d}"
    matches = []
    for path in sorted(root.rglob(f"*{stamp}*.nc")):
        name = path.name
        valid_date_occurrence = False
        start = name.find(stamp)
        while start != -1:
            if start == 0 or name[start - 1] != "R":
                valid_date_occurrence = True
                break
            start = name.find(stamp, start + 1)
        if valid_date_occurrence:
            matches.append(path)
    return matches


class CMEMSGlobalWaveReanalysisProvider(BaseProvider):
    """Daily partitions of the global wave reanalysis (0.2 degree, 3 hourly).

    Product ``GLOBAL_MULTIYEAR_WAV_001_032``, dataset
    ``cmems_mod_glo_wav_my_0.2deg_PT3H-i``. The native files are mirrored locally and this
    provider concatenates one day of them into the Icechunk store.
    """

    name = "cmems_global_wave_reanalysis"
    append_dim = "time"
    store_prefix = "bkr/cmems/global_wave_reanalysis.icechunk"
    dataset_id = BULK_DATASETS["global_wave_reanalysis"]
    #: Wave fields carry no useful precision past float16 and dominate the store size.
    dtype = "float16"
    #: Pull the day from CMEMS when it is not in the local mirror yet.
    download_if_missing: bool = True

    @property
    def source_dir(self) -> pathlib.Path:
        """Local mirror directory for the reanalysis files."""
        return dataset_dir(self.dataset_id, self.config)

    def fetch(
        self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs
    ) -> List[str]:
        """Return the mirrored files for one day, downloading them first if needed.

        Args:
            it: Partition timestamp (midnight UTC of the day to ingest).
            temp_dir: Unused; the mirror lives under the configured data directory so it
                survives between runs.

        Returns:
            Sorted paths of that day's NetCDF files, empty when the day is unavailable.
        """
        files = _day_files(self.source_dir, it)
        if not files and self.download_if_missing:
            logger.info(f"{self.name}: {it:%Y-%m-%d} not mirrored, downloading")
            try:
                download_dataset(
                    self.dataset_id,
                    file_filter=f"*{it:%Y%m%d}*",
                    config=self.config,
                )
            except Exception as exc:  # noqa: BLE001 - archive gaps are normal
                logger.error(f"{self.name}: download for {it} failed: {exc}")
                return []
            files = _day_files(self.source_dir, it)
        if not files:
            return []
        return [str(f) for f in files]

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Concatenate one day of reanalysis files and shape them for the store.

        Args:
            input_files: Paths returned by :meth:`fetch`.
            it: Partition timestamp, used only for logging.
            temp_dir: Unused, part of the interface.

        Returns:
            Dataset renamed to standard names, downcast, de-duplicated along time and
            chunked one timestep per chunk.
        """
        ds = xr.open_mfdataset(
            input_files,
            preprocess=rename_to_standard_names,
            combine="nested",
            concat_dim=self.append_dim,
        )
        ds = ds.drop_duplicates(self.append_dim).sortby(self.append_dim)
        ds = ds.astype(self.dtype)
        logger.debug(f"{self.name}: {it:%Y-%m-%d} -> {dict(ds.sizes)}")
        return ds.chunk({self.append_dim: 1, "latitude": -1, "longitude": -1})


class CMEMSGlobalOceanForecastProvider(BaseProvider):
    """Daily partitions of the merged global ocean analysis and forecast.

    Product ``GLOBAL_ANALYSISFORECAST_PHY_001_024``. Three datasets are combined per day:

    * ``cmems_mod_glo_phy_anfc_0.083deg_PT1H-m`` — hourly means (the base fields),
    * ``cmems_mod_glo_phy_anfc_merged-uv_PT1H-i`` — hourly surface currents,
    * ``cmems_mod_glo_phy_anfc_merged-sl_PT1H-i`` — hourly sea level.

    The sea-level dataset defines the target grid: the other two are interpolated onto it
    before the merge, as the three products are published on slightly different grids.

    All three publish some of the same fields, so precedence is explicit and most-specific
    first: sea level comes from the sea-level product, currents from the currents product,
    and the base product contributes only what neither of the others provides (temperature
    and salinity).
    """

    name = "cmems_global_ocean_forecast"
    append_dim = "time"
    store_prefix = "bkr/cmems/global_ocean_forecast.icechunk"

    base_dataset_id = BULK_DATASETS["global_phy_hourly"]
    currents_dataset_id = BULK_DATASETS["global_phy_surface_currents"]
    sea_level_dataset_id = BULK_DATASETS["global_phy_sea_level"]

    #: Currents and sea surface height are stored at reduced precision; everything else
    #: stays float32.
    low_precision_vars: tuple[str, ...] = (
        "eastward_sea_water_velocity",
        "northward_sea_water_velocity",
        "sea_surface_height_above_geoid",
    )

    @property
    def dataset_ids(self) -> tuple[str, str, str]:
        """The three source dataset ids, in (base, currents, sea level) order."""
        return (self.base_dataset_id, self.currents_dataset_id, self.sea_level_dataset_id)

    def source_dir_for(self, dataset_id: str) -> pathlib.Path:
        """Local mirror directory for one of the three source datasets."""
        return dataset_dir(dataset_id, self.config)

    def fetch(
        self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs
    ) -> List[str]:
        """Return the mirrored files for one day across all three datasets.

        A day is only usable when every dataset has a file for it, so an incomplete day
        returns an empty list and is retried on a later run.

        Args:
            it: Partition timestamp (midnight UTC of the day to ingest).
            temp_dir: Unused; inputs come from the persistent local mirror.

        Returns:
            One path per source dataset, or an empty list when the day is incomplete.
        """
        files: list[str] = []
        for dataset_id in self.dataset_ids:
            matches = _day_files(self.source_dir_for(dataset_id), it)
            if not matches:
                logger.debug(f"{self.name}: no {dataset_id} file for {it:%Y-%m-%d}")
                return []
            if len(matches) > 1:
                # Several files for one day means the mirror holds more than one
                # production run and there is no way to tell which is authoritative, so
                # the day is skipped rather than silently ingesting the wrong one.
                logger.warning(
                    f"{self.name}: {len(matches)} files for {dataset_id} on "
                    f"{it:%Y-%m-%d} ({[m.name for m in matches]}), skipping the day"
                )
                return []
            files.append(str(matches[0]))
        return files

    def _select(self, input_files: Sequence[str], dataset_id: str) -> str | None:
        """Pick the input file belonging to ``dataset_id`` by its mirror path."""
        for path in input_files:
            if dataset_id in str(path):
                return path
        return None

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the three source datasets for one day onto the sea-level grid.

        Args:
            input_files: Paths returned by :meth:`fetch`, in any order: each is matched
                back to its dataset by the mirror directory it sits in.
            it: Partition timestamp, used only for logging.
            temp_dir: Unused, part of the interface.

        Returns:
            Merged dataset, chunked one timestep per chunk.

        Raises:
            ValueError: When an input file for one of the three datasets is missing.
        """
        opened: dict[str, xr.Dataset] = {}
        for dataset_id in self.dataset_ids:
            path = self._select(input_files, dataset_id)
            if path is None:
                raise ValueError(f"{self.name}: no input file for {dataset_id} at {it}")
            # Opened lazily: the base product is a global 1/12 degree hourly day, far too
            # large to interpolate eagerly. Without ``chunks`` the interp below would
            # allocate the whole day as NumPy arrays and take the host down before the
            # memory guard ever sees the dataset.
            ds = xr.open_dataset(path, chunks={self.append_dim: 1})
            opened[dataset_id] = rename_to_standard_names(ds).sortby("latitude").sortby(
                "longitude"
            )

        sea_level = opened[self.sea_level_dataset_id]
        base = opened[self.base_dataset_id]
        currents = opened[self.currents_dataset_id]

        # All three products publish currents and sea surface height, so after renaming to
        # standard names they collide and ``xr.merge`` raises. The most specific product
        # wins: sea level from the sea-level product, currents from the currents product,
        # everything else (temperature, salinity) from the base product.
        currents = currents.drop_vars(
            [v for v in currents.data_vars if v in sea_level.data_vars]
        )
        base = base.drop_vars(
            [
                v
                for v in base.data_vars
                if v in sea_level.data_vars or v in currents.data_vars
            ]
        )

        # The sea level product carries the depth coordinate the store is built around;
        # give it to the others so the merge does not produce a second depth axis. Only
        # safe when the axes are the same length, which they are for these three products.
        if "depth" in sea_level.coords:
            for name, ds in (("base", base), ("currents", currents)):
                if "depth" in ds.dims and ds.sizes["depth"] != sea_level.sizes.get("depth"):
                    raise ValueError(
                        f"{self.name}: {name} has {ds.sizes['depth']} depth levels but the "
                        f"sea level product has {sea_level.sizes.get('depth')}"
                    )
            if "depth" in base.dims:
                base = base.assign_coords(depth=sea_level["depth"])
            if "depth" in currents.dims:
                currents = currents.assign_coords(depth=sea_level["depth"])

        base = base.interp_like(sea_level)
        currents = currents.interp_like(sea_level)

        merged = xr.merge([base, currents, sea_level]).astype(np.float32)
        for var in self.low_precision_vars:
            if var in merged.data_vars:
                merged[var] = merged[var].astype(np.float16)

        chunks = {self.append_dim: 1, "latitude": -1, "longitude": -1}
        if "depth" in merged.dims:
            chunks["depth"] = 1
        logger.debug(f"{self.name}: {it:%Y-%m-%d} -> {dict(merged.sizes)}")
        return merged.chunk(chunks)
