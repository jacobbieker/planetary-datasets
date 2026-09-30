"""NOAA GMGSI — Global Mosaic of Geostationary Satellite Imagery.

GMGSI stitches the operational geostationary fleet (GOES-East/West, Meteosat, Himawari)
into a single hourly global image on a common 3000 x 4999 grid. NOAA publishes it to the
public ``noaa-gmgsi-pds`` bucket in two eras:

* **v1** (2021-07-13 onwards) — ``GLOBCOMP<CH>_nc.YYYYMMDDHH``, five channels including the
  shortwave-reflectance ``ssr`` product, no quality flags.
* **v3** (2025-03-10 17:00 onwards) — ``GLOBCOMP<CH>_v3r0_blend_s<start>_e<end>_c<created>.nc``,
  four channels, each with a companion ``dqf`` quality flag.

Both eras are served by the same code path; :class:`GMGSIProvider` handles v3 and
:class:`GMGSILegacyProvider` handles v1. Imagery is 8-bit, so everything is stored as
``uint8`` with a missing-data flag of 255 in the ``*_dqf`` variables.
"""

from __future__ import annotations

import os
import pathlib
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider

#: Public NOAA Open Data bucket holding the mosaics. No credentials are needed.
SOURCE_BUCKET = "noaa-gmgsi-pds"

#: Value written into the quality flags where the source file has no data.
DQF_FILL = 255

#: First hour available in each era of the archive.
V1_START = pd.Timestamp("2021-07-13T00:00")
V3_START = pd.Timestamp("2025-03-10T17:00")


@dataclass(frozen=True)
class Channel:
    """One GMGSI channel: where it lives in the bucket and what to call it."""

    #: Output variable name, e.g. ``lwir``.
    name: str
    #: Bucket sub-directory, e.g. ``GMGSI_LW``.
    directory: str
    #: Filename stem, e.g. ``GLOBCOMPLIR``. It does not always match the directory.
    stem: str


#: v3 channels. The longwave and shortwave products are filed under ``LW``/``SW`` but named
#: ``LIR``/``SIR``, which is why the directory and the stem are tracked separately.
V3_CHANNELS: tuple[Channel, ...] = (
    Channel("vis", "GMGSI_VIS", "GLOBCOMPVIS"),
    Channel("wv", "GMGSI_WV", "GLOBCOMPWV"),
    Channel("lwir", "GMGSI_LW", "GLOBCOMPLIR"),
    Channel("swir", "GMGSI_SW", "GLOBCOMPSIR"),
)

#: v1 channels, which additionally include the shortwave reflectance mosaic.
V1_CHANNELS: tuple[Channel, ...] = (
    Channel("vis", "GMGSI_VIS", "GLOBCOMPVIS"),
    Channel("ssr", "GMGSI_SSR", "GLOBCOMPSSR"),
    Channel("wv", "GMGSI_WV", "GLOBCOMPWV"),
    Channel("lwir", "GMGSI_LW", "GLOBCOMPLIR"),
    Channel("swir", "GMGSI_SW", "GLOBCOMPSIR"),
)

#: Variables in the source files that carry no data we keep.
_DROP_VARS = ("quality_information",)


def _anon_s3():
    """Filesystem for the public bucket. Anonymous so a stale AWS profile cannot break it."""
    import s3fs

    return s3fs.S3FileSystem(anon=True)


class GMGSIProvider(BaseProvider):
    """NOAA GMGSI v3 (``v3r0_blend``) hourly global mosaic."""

    name = "gmgsi_v3"
    append_dim = "time"
    # "gmgi" is a typo, but it is the prefix the published store already lives under on
    # source.coop; renaming it would orphan every existing reader.
    store_prefix = "bkr/gmgi/gmgsi_v3.icechunk"

    #: Channels to fetch. Override to build a store with a subset.
    channels: Sequence[Channel] = V3_CHANNELS
    #: First hour this era covers; earlier partitions have nothing to fetch.
    archive_start: pd.Timestamp = V3_START

    def key_pattern(self, channel: Channel, it: pd.Timestamp) -> str:
        """Glob pattern matching the one object for ``channel`` at ``it``.

        v3 filenames embed observation-start, observation-end and creation timestamps, so
        the hour alone does not give the full key and a glob is unavoidable.
        """
        return (
            f"{SOURCE_BUCKET}/{channel.directory}/{it.year}/{it.month:02}/{it.day:02}/"
            f"{it.hour:02}/{channel.stem}_v3r0_blend_s{it.strftime('%Y%m%d%H')}*"
        )

    def archive_dir(self, temp_dir: pathlib.Path | None) -> pathlib.Path:
        """Where downloads land: the partition's temp dir, else the configured data dir."""
        if temp_dir is not None:
            return pathlib.Path(temp_dir)
        return pathlib.Path(self.config.data_dir) / "gmgsi"

    def fetch(
        self,
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        channels: Iterable[Channel] | None = None,
        **kwargs,
    ) -> List[str]:
        """Download every channel for one hour.

        Returns an empty list when any channel is missing: a mosaic built from a partial
        set of channels would not line up with the rest of the store, so a gap is better
        left as a gap until NOAA backfills it.
        """
        it = pd.Timestamp(it)
        if it < self.archive_start:
            logger.debug(f"{self.name}: {it} is before the archive starts ({self.archive_start})")
            return []

        wanted = tuple(channels) if channels is not None else tuple(self.channels)
        dest_dir = self.archive_dir(temp_dir)
        fs = _anon_s3()

        downloaded: List[str] = []
        for channel in wanted:
            pattern = self.key_pattern(channel, it)
            matches = sorted(fs.glob(pattern))
            if not matches:
                logger.warning(f"{self.name}: no object matching {pattern}, skipping {it}")
                return []
            if len(matches) > 1:
                # Reprocessed hours can leave more than one creation time behind; the
                # newest key sorts last because the creation stamp is the final field.
                logger.debug(f"{self.name}: {len(matches)} objects for {channel.name} at {it}")
            key = str(matches[-1])
            dest = dest_dir / channel.directory / os.path.basename(key)
            if _download(fs, key, dest) is None:
                return []
            downloaded.append(str(dest))

        return downloaded

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Merge the per-channel files into one ``uint8`` dataset for the hour."""
        if not input_files:
            raise ValueError(f"{self.name}: no input files to process for {it}")

        by_stem = {channel.stem: channel for channel in self.channels}
        parts = [_open_channel(path, by_stem) for path in sorted(input_files)]

        # compat="equals" rather than "override": every channel is on the same grid, and a
        # channel that is not should fail loudly instead of silently taking the first one.
        merged = xr.merge(parts, compat="equals", join="exact", combine_attrs="drop_conflicts")
        merged = merged.rename({"lat": "latitude", "lon": "longitude"})
        return merged.chunk({"time": 1, "yc": -1, "xc": -1})

    def timestamps(self, start=None, end=None, freq: str = "1h") -> pd.DatetimeIndex:
        """Hourly partitions for this era, clipped to when the archive begins."""
        start = self.archive_start if start is None else max(pd.Timestamp(start), self.archive_start)
        end = pd.Timestamp.now("UTC").tz_localize(None).floor("h") if end is None else pd.Timestamp(end)
        return pd.date_range(start, end, freq=freq)


class GMGSILegacyProvider(GMGSIProvider):
    """NOAA GMGSI v1 hourly global mosaic, the era before the v3 blend.

    v1 keys are fully determined by the hour and carry no quality flags, but it has the
    extra ``ssr`` shortwave-reflectance channel.
    """

    name = "gmgsi_v1"
    store_prefix = "bkr/gmgi/gmgsi.icechunk"
    channels: Sequence[Channel] = V1_CHANNELS
    archive_start: pd.Timestamp = V1_START

    def key_pattern(self, channel: Channel, it: pd.Timestamp) -> str:
        return (
            f"{SOURCE_BUCKET}/{channel.directory}/{it.year}/{it.month:02}/{it.day:02}/"
            f"{it.hour:02}/{channel.stem}_nc.{it.strftime('%Y%m%d%H')}"
        )


def _open_channel(path: str, by_stem: dict[str, Channel]) -> xr.Dataset:
    """Load one channel file and normalise it to ``<channel>`` / ``<channel>_dqf``."""
    filename = os.path.basename(path)
    matched = [channel for stem, channel in by_stem.items() if filename.startswith(stem)]
    if not matched:
        raise ValueError(f"unrecognised GMGSI filename: {filename}")
    channel = matched[0]

    with xr.open_dataset(path) as raw:
        ds = raw.load()

    ds = ds.drop_vars([v for v in _DROP_VARS if v in ds.variables])

    renames = {"data": channel.name}
    if "dqf" in ds:
        # Gaps between the contributing satellites come through as NaN; 255 keeps the flag
        # meaningful once the dataset is cast to uint8.
        ds["dqf"] = ds["dqf"].fillna(DQF_FILL)
        renames["dqf"] = f"{channel.name}_dqf"
    ds = ds.rename(renames)

    keep = set(renames.values())
    ds = ds.drop_vars([v for v in ds.data_vars if v not in keep])

    # Only the data variables are cast; the lat/lon/time coordinates keep their dtypes.
    return ds.astype(np.uint8)


def _download(fs, key: str, dest: pathlib.Path) -> pathlib.Path | None:
    """Copy one object out of S3, skipping it if already present.

    The object is written to a ``.part`` file and renamed once complete, so an interrupted
    run cannot leave a truncated file that the skip-if-present check would accept.
    """
    if dest.is_file() and dest.stat().st_size > 0:
        logger.debug(f"already downloaded {dest.name}")
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    try:
        fs.get(f"s3://{key}", str(part))
        os.replace(part, dest)
    except Exception as exc:  # noqa: BLE001 - any transport error means "no data this hour"
        part.unlink(missing_ok=True)
        logger.warning(f"failed to download s3://{key}: {exc}")
        return None
    logger.debug(f"downloaded s3://{key} to {dest}")
    return dest


def rewrite_store_skipping_bad_chunks(
    source: str | os.PathLike,
    destination: str | os.PathLike,
    max_workers: int | None = None,
) -> list[int]:
    """Rewrite a Zarr store, leaving out the timesteps that will not read.

    A handful of GMGSI timesteps in the original archive were written with corrupt
    compression and raise on read. This salvages the rest: every timestep is read once to
    see whether it decompresses, the destination is created with only the good ones on its
    time axis, and those are then copied in parallel.

    Unreadable steps are dropped rather than left as fill values, because the imagery is
    ``uint8`` and a fill of zero is an entirely plausible all-black image that a reader
    could not tell apart from real data.

    Args:
        source: Existing Zarr store to read from.
        destination: Store to create. Overwritten if it exists.
        max_workers: Processes to use. ``None`` lets the executor decide.

    Returns:
        The time indices of ``source`` that could not be read and were left out.
    """
    source, destination = str(source), str(destination)

    with xr.open_zarr(source, consolidated=False, decode_timedelta=True) as probe:
        n_times = probe.sizes["time"]

    readable = _map(_timestep_reads, [source] * n_times, range(n_times), max_workers=max_workers)

    good = [i for i, ok in enumerate(readable) if ok]
    failed = [i for i, ok in enumerate(readable) if not ok]
    if failed:
        logger.warning(f"{len(failed)}/{n_times} timesteps could not be read: {failed}")
    if not good:
        raise ValueError(f"no readable timesteps in {source}")

    template = xr.open_zarr(source, consolidated=False, decode_timedelta=True).isel(time=good)
    for var in list(template.variables):
        template[var] = template[var].drop_encoding()
    template.to_zarr(destination, compute=False, mode="w", consolidated=False)
    template.close()

    copied = _map(
        _copy_timestep, [source] * len(good), [destination] * len(good), good, max_workers=max_workers
    )

    # A step that read cleanly during probing should always copy; if one does not, it is
    # still a hole in the output and belongs in the returned list.
    failed.extend(itime for itime, ok in zip(good, copied) if not ok)
    return sorted(failed)


def _map(fn, *iterables, max_workers: int | None) -> list:
    """Apply ``fn`` across ``iterables``, in worker processes unless ``max_workers`` is 1.

    Decompressing Zarr chunks releases the GIL only partially, so processes are worth the
    overhead for a whole-store rewrite. One worker stays in-process: there is nothing to
    parallelise, and tracebacks stay readable.
    """
    if max_workers == 1:
        return [fn(*args) for args in zip(*iterables)]

    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(fn, *iterables))


def _timestep_reads(source: str, itime: int) -> bool:
    """True when one timestep of ``source`` decompresses without error."""
    try:
        with xr.open_zarr(source, consolidated=False, decode_timedelta=True) as ds:
            ds.isel(time=itime).load()
    except Exception as exc:  # noqa: BLE001 - a bad chunk must not stop the salvage
        logger.warning(f"timestep {itime} could not be read: {exc}")
        return False
    return True


def _copy_timestep(source: str, destination: str, itime: int) -> bool:
    """Copy one timestep between Zarr stores. Returns False if it could not be copied.

    ``region="auto"`` matches on the ``time`` coordinate value, so the index in ``source``
    does not need to line up with the index in the (shorter) destination.
    """
    try:
        # isel with a list keeps the time dimension, which region="auto" needs.
        ds = xr.open_zarr(source, consolidated=False, decode_timedelta=True).isel(time=[itime])
        ds.to_zarr(destination, region="auto", consolidated=False)
    except Exception as exc:  # noqa: BLE001 - a bad chunk must not stop the salvage
        logger.warning(f"timestep {itime} could not be copied: {exc}")
        return False
    return True


__all__ = [
    "Channel",
    "GMGSILegacyProvider",
    "GMGSIProvider",
    "V1_CHANNELS",
    "V1_START",
    "V3_CHANNELS",
    "V3_START",
    "rewrite_store_skipping_bad_chunks",
]
