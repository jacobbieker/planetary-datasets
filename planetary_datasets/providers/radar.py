"""European ground weather radar composites.

Two national precipitation radar archives that arrive as files on disk rather than over an
API, and previously had one throwaway script each:

``UKRadarProvider``
    The Met Office RADARNET/Nimrod 1 km rain-rate composite for the UK and Ireland,
    distributed as ODIM HDF5 (``*_ODIM_ng_radar_rainrate_composite_1km_UK.h5``) at a
    fifteen-minute cadence. One partition is one hour, so four frames. Downloaded from the
    Met Office's public bucket; see below.
``FMIRadarProvider``
    The Finnish Meteorological Institute 1 km precipitation *accumulation* rasters
    (``*_FIN-ACRR<n>H-3067-1KM.tif``), one GeoTIFF per accumulation window. One partition is
    one hour and merges the 1 h, 12 h and 24 h products into a single timestep.

The UK composite is fetched from ``s3://met-office-radar-obs-data`` (anonymous, keyed
``radar/YYYY/MM/DD/YYYYMMDDhhmm_ODIM_ng_radar_rainrate_composite_1km_UK.h5``, archived from
2024-11-21). Finland is not downloaded here: it is published through a bulk channel and was
already staged on local disk by the original script. Point either provider at a staging
area instead with:

``UK_RADAR_ARCHIVE_DIR``
    Directory holding the ODIM HDF5 files. Unset by default, which is what selects the
    bucket; set it to read a locally staged archive (a CEDA RADARNET pull, say) instead.
``FMI_RADAR_ARCHIVE_DIR``
    Directory holding the FMI GeoTIFFs. Defaults to ``<data_dir>/fmi_radar``.

``<data_dir>`` is ``PLANETARY_DATASETS_DATA_DIR``. Either tree may be flat or nested by
date; files are located by their ``YYYYMMDDhhmm`` filename stamp either way.

A partition that finds none of its files is reported as absent and skipped. A partition
that finds *some* of them raises by default, because both archives are populated
incrementally and committing half an hour would leave a gap. Set ``allow_partial=True`` on
a provider instance to write what is there instead; the partition is then judged complete
only when every expected timestep is stored, so a short commit is revisited rather than
skipped for ever, and an FMI hour missing a product still writes the full variable set with
that product all-NaN so the store's schema does not get pinned to a subset.
"""

from __future__ import annotations

import os
import pathlib
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Sequence

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

from planetary_datasets.base import BaseProvider
from planetary_datasets.config import Config

#: Leading ``YYYYMMDDhhmm`` stamp shared by both naming conventions.
STAMP_RE = re.compile(r"(?<!\d)(\d{12})(?!\d)")

#: The grid axes must line up with the store exactly, on top of the coordinates
#: :mod:`planetary_datasets.common.store` already checks. Both of these archives are
#: single-domain, so a mismatch means the upstream grid was redefined.
RADAR_ALIGNMENT_COORDS = ("latitude", "longitude", "x", "y")


class RadarArchiveNotConfigured(RuntimeError):
    """The directory a radar provider reads from does not exist.

    Distinct from "no data for this partition": a missing archive root is a configuration
    error, and treating it as absent data would silently mark a whole backfill as done.
    """


class IncompletePartition(RuntimeError):
    """Some, but not all, of a partition's input files are present."""


def to_naive_utc(value) -> pd.Timestamp:
    """Normalise a timestamp to tz-naive UTC.

    Dagster hands partition starts over as tz-aware. Stored radar times are naive, and a
    tz-aware timestamp never compares equal to them, so every partition would look missing
    and every write would duplicate. Normalising at the provider boundary is the fix.
    """
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def stamp_of(path: str | os.PathLike) -> pd.Timestamp:
    """Return the observation time encoded in a radar filename.

    Both archives prefix the filename with ``YYYYMMDDhhmm``.
    """
    name = pathlib.Path(path).name
    match = STAMP_RE.search(name)
    if match is None:
        raise ValueError(f"no YYYYMMDDhhmm timestamp in filename {name!r}")
    return pd.to_datetime(match.group(1), format="%Y%m%d%H%M")


#: Sub-directory layouts these archives are commonly staged in, as strftime templates
#: relative to the archive root. The empty string is the flat layout.
DATE_LAYOUTS: tuple[str, ...] = (
    "",
    "%Y/%m/%d/%H",
    "%Y/%m/%d",
    "%Y/%m",
    "%Y",
    "%Y%m%d",
    "%Y-%m-%d",
)


def find_by_pattern(
    root: pathlib.Path,
    pattern: str,
    when: pd.Timestamp | None = None,
    hint: str | None = None,
) -> tuple[List[pathlib.Path], str | None]:
    """Find files matching ``pattern`` for ``when``, avoiding a full walk where possible.

    A handful of known date layouts are probed directly before falling back to ``rglob``.
    Without that, every partition of a date-nested archive would walk the whole tree, and a
    five-year UK backfill is tens of thousands of partitions over a tree of comparable size.

    Returns the matches together with the strftime layout they were found under, which the
    caller can feed back in as ``hint`` so an archive with an unrecognised structure is only
    walked once.
    """
    layouts = list(DATE_LAYOUTS)
    if hint is not None and hint in layouts:
        layouts.insert(0, layouts.pop(layouts.index(hint)))

    if when is not None:
        for layout in layouts:
            directory = root / when.strftime(layout) if layout else root
            if not directory.is_dir():
                continue
            found = sorted(directory.glob(pattern))
            if found:
                return found, layout

    # Unrecognised layout: walk once. The caller caches nothing in this case because there
    # is no template to cache, but a miss here means the partition is genuinely absent.
    return sorted(root.rglob(pattern)), None


def _plain(value):
    """Convert a numpy scalar to a Python scalar so it survives attribute serialisation."""
    return value.item() if isinstance(value, np.generic) else value


class LocalArchiveRadarProvider(BaseProvider):
    """Shared behaviour for radar providers reading a staged directory of files.

    Subclasses supply :attr:`archive_env`, :attr:`archive_subdir`, a glob
    :attr:`file_pattern` templated on the partition, and the usual
    :meth:`~planetary_datasets.base.BaseProvider.process`.
    """

    #: Environment variable naming the directory the raw files live in.
    archive_env: str
    #: Directory under the configured data dir used when the variable is unset.
    archive_subdir: str
    #: Glob matching one partition's files, formatted with ``hour``/``stamp`` keys.
    file_pattern: str
    #: Length of the window one partition covers.
    partition_freq: str = "1h"
    #: Chunk length along ``time``; the grids are large, so frames are stored one per chunk.
    time_chunk: int = 1

    def __init__(self, config: Config | None = None, allow_partial: bool = False):
        """Build a provider.

        Args:
            config: Configuration override; the process-wide one is used when omitted.
            allow_partial: Write a partition even when only some of its files are present.
                Off by default so an archive that is still filling is retried rather than
                committed short.
        """
        super().__init__(config)
        self.allow_partial = allow_partial
        # strftime layout the last successful lookup used, tried first on the next one so a
        # long backfill over a nested archive does not re-probe every candidate directory.
        self._layout_hint: str | None = None

    @property
    def archive_dir(self) -> pathlib.Path:
        """Directory the raw files are read from."""
        raw = (os.environ.get(self.archive_env) or "").strip()
        if raw:
            return pathlib.Path(raw).expanduser()
        return self.config.data_dir / self.archive_subdir

    def window(self, it: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Half-open ``[start, end)`` interval the partition covers."""
        start = to_naive_utc(it)
        return start, start + pd.tseries.frequencies.to_offset(self.partition_freq)

    def expected_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """Timestamps this partition should contain. Subclasses may narrow this."""
        start, end = self.window(it)
        return pd.date_range(start, end, freq=self.partition_freq, inclusive="left")

    def candidates(self, it: pd.Timestamp) -> List[pathlib.Path]:
        """Files on disk belonging to this partition."""
        root = self.archive_dir
        if not root.is_dir():
            raise RadarArchiveNotConfigured(
                f"{self.name}: {root} is not a directory. Set {self.archive_env} to the "
                "directory holding the raw radar files."
            )
        start, end = self.window(it)
        pattern = self.file_pattern.format(
            hour=start.strftime("%Y%m%d%H"), stamp=start.strftime("%Y%m%d%H%M")
        )
        matches, layout = find_by_pattern(root, pattern, when=start, hint=self._layout_hint)
        if layout is not None:
            self._layout_hint = layout
        found = []
        for path in matches:
            try:
                when = stamp_of(path)
            except ValueError:
                logger.debug(f"{self.name}: ignoring unparseable filename {path.name}")
                continue
            if start <= when < end:
                found.append(path)
        return found

    def incomplete(self, it: pd.Timestamp, found: Sequence[pathlib.Path]) -> str | None:
        """Describe what is missing from a partial partition, or None when complete."""
        missing = set(self.expected_times(it)) - {stamp_of(p) for p in found}
        if not missing:
            return None
        return ", ".join(str(t) for t in sorted(missing))

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Locate this partition's files in the local archive.

        Returns an empty list only when the partition is genuinely absent. A partially
        present partition raises so it is retried once the rest has landed, rather than
        being committed short and never revisited.
        """
        found = self.candidates(it)
        if not found:
            logger.debug(f"{self.name}: nothing in {self.archive_dir} for {to_naive_utc(it)}")
            return []

        missing = self.incomplete(it, found)
        if missing is not None:
            message = f"{self.name}: {to_naive_utc(it)} is incomplete, missing {missing}"
            if not self.allow_partial:
                raise IncompletePartition(message)
            logger.warning(f"{message}; writing the {len(found)} file(s) that are present")
        return [str(p) for p in found]

    def partition_stored(self, it: pd.Timestamp) -> bool:
        """True when this partition needs no further work.

        Normally the partition start being present is enough: a complete hour is written in
        one commit, so its first timestep standing in for the rest is safe. Under
        :attr:`allow_partial` it is not — an hour committed from four frames would make the
        start present and the remaining eight would never be revisited — so every expected
        timestep is checked instead.
        """
        if self.allow_partial:
            wanted = self.expected_times(it)
        else:
            wanted = pd.DatetimeIndex([to_naive_utc(it)])
        return not self.missing_timesteps(wanted)

    def run_partition(self, it, check_present: bool = True) -> bool:
        """Run one partition, normalising the timestamp first.

        Dagster passes a tz-aware partition start. The stored times are naive UTC, so
        without this the "already stored?" check never matches and every run appends a
        duplicate timestep.
        """
        it = to_naive_utc(it)
        if check_present and self.partition_stored(it):
            logger.debug(f"{self.name}: {it} already in {self.store_path}, skipping")
            return False
        # The presence check has been done here, with the right notion of "complete".
        return super().run_partition(it, check_present=False)

    def run_range(self, timestamps) -> int:
        """Run every partition in ``timestamps`` that is not already complete.

        Reimplemented rather than delegated because the base class filters on the partition
        start alone, which is the wrong question under :attr:`allow_partial`.
        """
        written = 0
        for it in pd.DatetimeIndex([to_naive_utc(t) for t in timestamps]):
            if self.partition_stored(it):
                continue
            try:
                if self.run_partition(it, check_present=False):
                    written += 1
            except Exception as exc:  # noqa: BLE001 - one bad hour must not stop a backfill
                logger.exception(f"{self.name}: partition {it} failed: {exc}")
        return written

    def missing_timesteps(self, desired) -> List[pd.Timestamp]:
        """Return the timesteps in ``desired`` that are not yet stored, as naive UTC."""
        return super().missing_timesteps(pd.DatetimeIndex([to_naive_utc(t) for t in desired]))

    #: Radar grids carry projected x/y rather than latitude/longitude.
    alignment_coords = RADAR_ALIGNMENT_COORDS

    def prepare_for_write(self, processed: xr.Dataset) -> xr.Dataset:
        return self.chunk_for_write(processed)

    def chunk_for_write(self, ds: xr.Dataset) -> xr.Dataset:
        """Apply the store's chunking to a processed dataset."""
        chunks = {self.append_dim: self.time_chunk}
        chunks.update({dim: -1 for dim in ds.dims if dim != self.append_dim})
        return ds.chunk(chunks)


class S3ArchiveRadarProvider(LocalArchiveRadarProvider):
    """A radar archive published in a public S3 bucket rather than staged on disk.

    One object per frame, at a key that is a pure function of the frame's timestamp, so a
    partition's objects are named rather than listed — a listing per partition would be an
    extra round trip for every hour of a multi-year backfill, and the key layout is fixed.
    Objects that are absent are simply not returned, which is what lets the inherited
    :meth:`~LocalArchiveRadarProvider.fetch` tell an absent hour from a half-published one.

    The local archive is still honoured: set :attr:`archive_env` and the files are read
    from disk exactly as before, without touching the network. That is the escape hatch for
    anyone holding a bulk pull of the same product (CEDA, for the UK), and it is why
    :attr:`archive_subdir` remains meaningful.
    """

    #: Bucket holding the frames, read anonymously.
    s3_bucket: str
    #: ``strftime`` template for one frame's key within the bucket.
    s3_key_format: str

    @property
    def archive_dir(self) -> pathlib.Path:
        """Directory the raw files are read from, when one is configured.

        Unlike the local-archive base this does *not* fall back to ``<data_dir>/<subdir>``:
        that fallback is what made an unset variable indistinguishable from "use the
        default staging directory", and the resulting ``RadarArchiveNotConfigured`` was
        raised on every partition of a source that has a perfectly good bucket behind it.
        """
        raw = (os.environ.get(self.archive_env) or "").strip()
        return pathlib.Path(raw).expanduser() if raw else self.config.data_dir / self.archive_subdir

    @property
    def use_local_archive(self) -> bool:
        """True when a staged directory is configured and should be read instead of S3."""
        return bool((os.environ.get(self.archive_env) or "").strip())

    def s3_keys(self, it: pd.Timestamp) -> Dict[pd.Timestamp, str]:
        """Object key for every frame the partition starting at ``it`` should contain."""
        return {when: when.strftime(self.s3_key_format) for when in self.expected_times(it)}

    def _filesystem(self):
        import s3fs

        return s3fs.S3FileSystem(anon=True)

    def candidates(self, it: pd.Timestamp) -> List[pathlib.Path]:
        """Delegate to the local archive; only reached when one is configured."""
        return super().candidates(it)

    def fetch(self, it: pd.Timestamp, temp_dir: pathlib.Path | None = None, **kwargs) -> List[str]:
        """Download this partition's frames from the bucket, skipping any not published.

        Falls through to the local archive when one is configured. Otherwise the same
        completeness rules apply as on disk: nothing published is an absent partition, some
        of it published raises so the hour is retried once the rest lands.
        """
        if self.use_local_archive:
            return super().fetch(it, temp_dir=temp_dir, **kwargs)

        directory = pathlib.Path(temp_dir) if temp_dir is not None else self.config.scratch_dir
        directory.mkdir(parents=True, exist_ok=True)
        filesystem = self._filesystem()

        found: List[pathlib.Path] = []
        for when, key in sorted(self.s3_keys(it).items()):
            remote = f"{self.s3_bucket}/{key}"
            local = directory / pathlib.PurePosixPath(key).name
            try:
                if not filesystem.exists(remote):
                    logger.debug(f"{self.name}: {when} not published at s3://{remote}")
                    continue
                filesystem.get(remote, str(local))
            except FileNotFoundError:
                # Raced with the publisher between exists() and get(); treat as absent.
                logger.debug(f"{self.name}: {when} vanished from s3://{remote} mid-fetch")
                continue
            found.append(local)

        if not found:
            logger.debug(f"{self.name}: nothing in s3://{self.s3_bucket} for {to_naive_utc(it)}")
            return []

        missing = self.incomplete(it, found)
        if missing is not None:
            message = f"{self.name}: {to_naive_utc(it)} is incomplete, missing {missing}"
            if not self.allow_partial:
                raise IncompletePartition(message)
            logger.warning(f"{message}; writing the {len(found)} frame(s) that are present")
        return [str(p) for p in found]


def odim_grid(where: dict) -> Dict[str, np.ndarray]:
    """Build projected ``x``/``y`` cell-centre coordinates from an ODIM ``/where`` group.

    ODIM composites describe their grid with a PROJ string and the lat/lon of the four
    corners rather than with an origin and a transform, so the corners are projected and
    the axes rebuilt from the stated cell size. The two left corners (and the two top
    corners) are averaged to cancel the round-trip error, which is sub-millimetre on a
    1 km grid but would otherwise differ between the axes.
    """
    import pyproj

    required = (
        "projdef", "xsize", "ysize", "xscale", "yscale",
        "UL_lon", "UL_lat", "LL_lon", "LL_lat", "UR_lon", "UR_lat",
    )
    absent = [key for key in required if key not in where]
    if absent:
        raise ValueError(f"ODIM /where group is missing {absent}; cannot rebuild the grid")

    crs = pyproj.CRS.from_proj4(str(where["projdef"]))
    transformer = pyproj.Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    ul_x, ul_y = transformer.transform(float(where["UL_lon"]), float(where["UL_lat"]))
    ll_x, _ = transformer.transform(float(where["LL_lon"]), float(where["LL_lat"]))
    _, ur_y = transformer.transform(float(where["UR_lon"]), float(where["UR_lat"]))

    xsize, ysize = int(where["xsize"]), int(where["ysize"])
    xscale, yscale = float(where["xscale"]), float(where["yscale"])
    # Round to the millimetre so two files describing the same domain produce bit-identical
    # axes; an append is rejected outright if they do not match.
    x0 = round((ul_x + ll_x) / 2.0, 3)
    y0 = round((ul_y + ur_y) / 2.0, 3)
    # ODIM row 0 is the northern edge, so y descends.
    return {
        "x": x0 + (np.arange(xsize) + 0.5) * xscale,
        "y": y0 - (np.arange(ysize) + 0.5) * yscale,
    }


def open_uk_radar(filename: str) -> xr.Dataset:
    """Read one Met Office ODIM HDF5 rain-rate composite into a dataset.

    The rate is stored as raw counts with a ``gain``/``offset`` pair and a ``nodata``
    sentinel in ``/dataset1/data1/what``; all three are applied here so the store holds
    physical mm/h. ``undetect`` is left alone: it means "measured, no rain", which is a real
    zero rather than a gap.

    Raises:
        ValueError: if the file is not a rain-rate product. RADARNET also publishes
            reflectivity and accumulation composites under near-identical filenames, and
            writing one of those into the rain-rate store would mislabel it as mm/h.
    """
    with xr.open_datatree(filename, engine="h5netcdf", phony_dims="sort") as tree:
        where = dict(tree["/where"].attrs)
        what = dict(tree["/what"].attrs)
        data_what = dict(tree["/dataset1/data1/what"].attrs)
        ds = tree["/dataset1/data1"].to_dataset().load()

    quantity = str(data_what.get("quantity", "RATE"))
    if quantity != "RATE":
        raise ValueError(
            f"{pathlib.Path(filename).name} holds ODIM quantity {quantity!r}, not 'RATE'; "
            "this reader only handles the rain-rate composite"
        )

    time = pd.to_datetime(f"{what['date']}{what['time']}", format="%Y%m%d%H%M%S")

    ds = ds.rename({"data": "rainfall_rate", "phony_dim_0": "y", "phony_dim_1": "x"})
    rate = ds["rainfall_rate"].astype("float32")

    nodata = data_what.get("nodata")
    if nodata is not None:
        rate = rate.where(rate != float(nodata))
    gain = float(data_what.get("gain", 1.0))
    offset = float(data_what.get("offset", 0.0))
    if gain != 1.0 or offset != 0.0:
        rate = rate * gain + offset

    ds["rainfall_rate"] = rate.astype("float16")
    ds["rainfall_rate"].attrs = {
        "units": "mm h-1",
        "long_name": "radar rainfall rate composite",
        "quantity": quantity,
    }

    grid = odim_grid(where)
    ds = ds.assign_coords(y=grid["y"], x=grid["x"])
    ds["x"].attrs = {"units": "m", "standard_name": "projection_x_coordinate"}
    ds["y"].attrs = {"units": "m", "standard_name": "projection_y_coordinate"}

    ds = ds.expand_dims(time=[time])
    # gain/offset/nodata are deliberately dropped: they have been applied, and keeping them
    # would invite a second application downstream.
    ds.attrs = {
        "crs": str(where["projdef"]),
        "source": str(what.get("source", "")),
        "object": str(what.get("object", "")),
        "xscale": _plain(where.get("xscale")),
        "yscale": _plain(where.get("yscale")),
    }
    return ds


def gdal_metadata(attrs: dict) -> Dict[str, str]:
    """Parse the ``GDAL_METADATA`` XML blob rioxarray leaves on an FMI raster.

    The original scripts pulled these values out with ``str.split`` on fragments of the
    markup, which silently returned the wrong field whenever the item order changed.
    """
    raw = attrs.get("GDAL_METADATA")
    if not raw:
        raise ValueError("raster has no GDAL_METADATA; not an FMI accumulation product")
    root = ET.fromstring(raw)
    return {item.get("name", ""): (item.text or "").strip() for item in root.iter("Item")}


def open_fmi_radar(filename: str) -> xr.Dataset:
    """Read one FMI precipitation-accumulation GeoTIFF into a dataset.

    The variable is named after the accumulation window declared in the file's own
    metadata, e.g. ``rainfall_rate_accumulation_24h``, so a mislabelled filename cannot put
    the wrong product under the right name.
    """
    import rioxarray  # noqa: F401 - registers the rio accessor / open_rasterio

    array = rioxarray.open_rasterio(filename)
    try:
        meta = gdal_metadata(array.attrs)
        accumulation = int(float(meta["Accumulation time"]))
        gain = float(meta.get("Gain", 1.0))
        offset = float(meta.get("Offset", 0.0))
        nodata = meta.get("Nodata")
        time = pd.to_datetime(meta["Observation time"], format="%Y%m%d%H%M")

        name = f"rainfall_rate_accumulation_{accumulation}h"
        # Band 1 is the only band; select before the arithmetic so nothing is done twice.
        values = array.isel(band=0, drop=True).astype("float32")
        if nodata is not None:
            values = values.where(values != float(nodata))
        values = values * gain + offset

        # Read eagerly: the arithmetic above is lazy over the rasterio handle, which is
        # closed as soon as this block exits.
        ds = values.astype("float16").to_dataset(name=name).load()
    finally:
        array.close()

    ds = ds.drop_encoding()
    ds[name].attrs = {
        "units": "mm",
        "long_name": f"radar precipitation accumulation over {accumulation} h",
        "accumulation_hours": accumulation,
    }
    ds = ds.expand_dims(time=[time])
    ds.attrs = {"crs": "EPSG:3067", "institution": "Finnish Meteorological Institute"}
    return ds


class UKRadarProvider(S3ArchiveRadarProvider):
    """Met Office 1 km rain-rate composite, one hour of fifteen-minute frames."""

    name = "uk_radar"
    store_prefix = "bkr/precipradar/uk_radar.icechunk"
    archive_env = "UK_RADAR_ARCHIVE_DIR"
    archive_subdir = "uk_radar"
    file_pattern = "{hour}*.h5"

    s3_bucket = "met-office-radar-obs-data"
    s3_key_format = "radar/%Y/%m/%d/%Y%m%d%H%M_ODIM_ng_radar_rainrate_composite_1km_UK.h5"

    #: Cadence of the composite as published in the bucket. Four frames make up one hourly
    #: partition. The CEDA RADARNET archive of the same product is five-minute, so override
    #: this on an instance when pointing ``UK_RADAR_ARCHIVE_DIR`` at one of those.
    step_freq: str = "15min"

    def expected_times(self, it: pd.Timestamp) -> pd.DatetimeIndex:
        """The four fifteen-minute frames that make up one hourly partition."""
        start, end = self.window(it)
        return pd.date_range(start, end, freq=self.step_freq, inclusive="left")

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read the hour's frames and concatenate them along ``time``.

        ``join="exact"`` because the default outer join would answer a mid-hour grid change
        by NaN-padding the union of the two grids. That union is then rejected by the
        alignment check at write time, which only logs and returns False, so the hour would
        vanish while the partition still reported success.
        """
        frames = [open_uk_radar(path) for path in sorted(input_files)]
        ds = xr.concat(frames, dim="time", join="exact") if len(frames) > 1 else frames[0]
        return ds.sortby("time")


class FMIRadarProvider(LocalArchiveRadarProvider):
    """FMI 1 km precipitation accumulations, merging the 1 h, 12 h and 24 h products."""

    name = "fmi_radar"
    store_prefix = "bkr/precipradar/fmi_radar.icechunk"
    archive_env = "FMI_RADAR_ARCHIVE_DIR"
    archive_subdir = "fmi_radar"
    file_pattern = "{stamp}_*ACRR*.tif"

    #: Accumulation windows, in hours, that make up one timestep. Narrow this on a subclass
    #: or instance if only some of the products are archived.
    accumulations: tuple[int, ...] = (1, 12, 24)

    def incomplete(self, it: pd.Timestamp, found: Sequence[pathlib.Path]) -> str | None:
        """A partition is complete when every configured accumulation window is present.

        Completeness is judged on the filenames rather than on the file contents so that an
        absent product is reported without opening anything.
        """
        matches = (re.search(r"ACRR(\d+)H", pathlib.Path(p).name, re.IGNORECASE) for p in found)
        present = {int(match.group(1)) for match in matches if match is not None}
        missing = sorted(set(self.accumulations) - present)
        if not missing:
            return None
        return ", ".join(f"{hours}h" for hours in missing)

    def process(
        self,
        input_files: List[str],
        it: pd.Timestamp,
        temp_dir: pathlib.Path | None = None,
        **kwargs,
    ) -> xr.Dataset:
        """Read each accumulation window and merge them into a single timestep.

        The result always carries one variable per entry in :attr:`accumulations`, even
        under :attr:`allow_partial`: a window with no file becomes an all-NaN field. The
        store's variable set is fixed by its first write, and
        :func:`~planetary_datasets.common.store.write_to_icechunk` refuses to append a
        dataset whose variables differ — so letting a partial hour create a two-variable
        store would silently reject every complete hour afterwards.
        """
        products = [open_fmi_radar(path) for path in sorted(input_files)]
        times = {pd.Timestamp(p["time"].values[0]) for p in products}
        if len(times) != 1:
            raise ValueError(
                f"{self.name}: accumulation products disagree on the "
                f"observation time: {sorted(times)}"
            )
        # join="exact" so two products on different grids fail loudly instead of being
        # outer-joined into a mostly-NaN union.
        ds = xr.merge(products, join="exact", compat="equals", combine_attrs="override")

        template = products[0][list(products[0].data_vars)[0]]
        for hours in self.accumulations:
            name = f"rainfall_rate_accumulation_{hours}h"
            if name not in ds.data_vars:
                logger.warning(
                    f"{self.name}: no {hours}h product for {sorted(times)[0]}, filling with NaN"
                )
                ds[name] = xr.full_like(template, np.nan)
                ds[name].attrs = {
                    "units": "mm",
                    "long_name": f"radar precipitation accumulation over {hours} h",
                    "accumulation_hours": hours,
                }
        return ds


#: Every provider in this module, for the Dagster layer and the CLI.
PROVIDERS: tuple[type[LocalArchiveRadarProvider], ...] = (UKRadarProvider, FMIRadarProvider)


def provider_by_name(name: str) -> type[LocalArchiveRadarProvider]:
    """Look up a provider class by its :attr:`name`."""
    for provider in PROVIDERS:
        if provider.name == name:
            return provider
    raise KeyError(f"unknown radar provider {name!r}; known: {[p.name for p in PROVIDERS]}")


__all__ = [
    "DATE_LAYOUTS",
    "FMIRadarProvider",
    "IncompletePartition",
    "LocalArchiveRadarProvider",
    "PROVIDERS",
    "RadarArchiveNotConfigured",
    "S3ArchiveRadarProvider",
    "UKRadarProvider",
    "find_by_pattern",
    "gdal_metadata",
    "odim_grid",
    "open_fmi_radar",
    "open_uk_radar",
    "provider_by_name",
    "stamp_of",
    "to_naive_utc",
]
