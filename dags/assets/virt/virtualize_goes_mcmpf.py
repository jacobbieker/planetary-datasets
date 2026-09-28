#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "virtualizarr>=2.4.0",
#     "obstore",
#     "obspec-utils>=0.9.0",
#     "xarray",
#     "numpy",
#     "h5py",
#     "icechunk>=1.0",
#     "s3fs",
#     "tqdm",
#     "python-dotenv",
#     "psutil",
# ]
# ///
"""Virtualize the GOES-West ABI L2 MCMIPF archive into a single Icechunk store.

This is a standalone PEP-723 script, not a Dagster asset: ``dags/definitions.py``
deliberately skips ``dags.assets.virt`` when it discovers asset modules. Run it with::

    uv run --script dags/assets/virt/virtualize_goes_mcmpf.py

It doubles as the worked example for the two obspec-utils store wrappers, which is what
makes virtualizing tens of thousands of remote NetCDF files tolerable:

1. ``SplittingReadableStore`` turns one ``get()`` into many parallel ``get_ranges()``
   calls, trading S3's per-request latency for its high aggregate bandwidth.
2. ``CachingReadableStore`` keeps whole files in memory after first access, so the many
   small metadata reads VirtualiZarr makes are served locally instead of over the wire.

They compose as ``SplittingReadableStore -> CachingReadableStore -> VirtualiZarr``:
fetch fast, then reuse.

Credentials and the destination store come from :mod:`planetary_datasets.config`, i.e.
from the environment or ``.env``. Set ``ICECHUNK_LOCAL_PATH`` to write to a local
directory instead of the bucket.
"""

from __future__ import annotations

import datetime as dt
import pathlib
import sys

import icechunk
import s3fs
import tqdm
import virtualizarr as vz
import xarray as xr
from obspec_utils.obspec import BufferedStoreReader
from obspec_utils.registry import ObjectStoreRegistry
from obspec_utils.wrappers import CachingReadableStore, SplittingReadableStore
from obstore.store import from_url

# Run straight from a checkout without installing the package first.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

from planetary_datasets.config import get_config  # noqa: E402

#: Destination store, relative to the configured bucket.
STORE_PREFIX = "bkr/geo/goes-west-mcmpif.icechunk"

#: The GOES-West spacecraft, in order, with the years each one covers. The years must
#: not overlap: the store is appended along ``time`` in iteration order, so a year
#: covered by both spacecraft would write the same days twice and out of order.
#: GOES-18 took over on 2023-01-04, so 2023 belongs to it.
SOURCES: tuple[tuple[str, tuple[int, ...]], ...] = (
    ("noaa-goes17", (2019, 2020, 2021, 2022)),
    ("noaa-goes18", (2023, 2024, 2025, 2026)),
)

#: First and last operational day of each GOES spacecraft, for reference.
HISTORY_RANGE = {
    "goes16": (dt.datetime(2017, 12, 18, tzinfo=dt.UTC), dt.datetime(2025, 4, 7, tzinfo=dt.UTC)),
    "goes17": (dt.datetime(2019, 2, 12, tzinfo=dt.UTC), dt.datetime(2023, 1, 4, tzinfo=dt.UTC)),
    "goes18": (dt.datetime(2023, 1, 4, tzinfo=dt.UTC), None),
    "goes19": (dt.datetime(2025, 4, 7, tzinfo=dt.UTC), None),
}

#: Per-band statistics we do not keep.
_DROPPED_BAND_VARS = (
    "band_id",
    "min_reflectance_factor",
    "max_reflectance_factor",
    "mean_reflectance_factor",
    "std_dev_reflectance_factor",
    "min_brightness_temperature",
    "max_brightness_temperature",
    "mean_brightness_temperature",
    "std_dev_brightness_temperature",
    "outlier_pixel_count",
)

#: Low-dimensional coordinates and metadata that are cheap to materialise.
_LOADABLE_VARS = (
    "y",
    "x",
    "t",
    "band",
    "x_image",
    "y_image",
    "x_image_bounds",
    "y_image_bounds",
    "time_bounds",
    "goes_imager_projection",
    "nominal_satellite_subpoint_lat",
    "nominal_satellite_subpoint_lon",
    "nominal_satellite_height",
    "geospatial_lat_lon_extent",
    "percent_uncorrectable_GRB_errors",
    "percent_uncorrectable_L0_errors",
    "dynamic_algorithm_input_data_container",
    "algorithm_product_version_container",
)


def per_band_var_names(var_name: str) -> list[str]:
    """Expand a variable name across all 16 GOES ABI bands."""
    return [f"{var_name}_C{i:02}" for i in range(1, 17)]


def drop_variables() -> list[str]:
    """Every per-band statistic variable to drop while virtualizing."""
    return [n for base in _DROPPED_BAND_VARS for n in per_band_var_names(base)]


def loadable_variables() -> list[str]:
    """Variables to load eagerly rather than reference."""
    return [*_LOADABLE_VARS, *per_band_var_names("band_wavelength")]


def open_repository() -> icechunk.Repository:
    """Open the destination repository with the source buckets registered.

    The NOAA buckets have to be declared as virtual chunk containers. Without them a
    reader of the finished store cannot resolve the chunk references back to the source
    files. Saving the config makes that permanent, so readers do not have to repeat the
    registration themselves.
    """
    config = icechunk.RepositoryConfig(
        storage=icechunk.StorageSettings(
            unsafe_use_conditional_update=False,
            unsafe_use_conditional_create=False,
            unsafe_use_metadata=False,
        )
    )
    for bucket, _ in SOURCES:
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=f"s3://{bucket}/",
                # The NOAA archives are public; reading them must not be signed with
                # whatever credentials happen to be configured for the destination.
                store=icechunk.s3_store(region="us-east-1", anonymous=True),
            )
        )

    storage = get_config().icechunk_storage(STORE_PREFIX)
    repo = icechunk.Repository.open_or_create(storage, config)
    repo.save_config()
    return repo


def build_store(bucket: str):
    """Compose the splitting and caching wrappers around a public bucket."""
    base_store = from_url(f"s3://{bucket}", region="us-east-1", skip_signature=True)
    # Parallel range requests first, then cache the assembled file so VirtualiZarr's
    # many small metadata reads never go back to the network.
    splitting_store = SplittingReadableStore(base_store)
    return CachingReadableStore(splitting_store, max_size=512 * 1024 * 1024)


def store_is_empty(repo: icechunk.Repository) -> bool:
    """True when the store has no ``time`` coordinate yet, so the next write creates it.

    Asked of the repository rather than tracked in a variable: this script takes days to
    run and is restarted often, and a resumed run that believed the store was empty
    would overwrite the whole archive with a single day.
    """
    try:
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    except Exception:  # noqa: BLE001 - nothing committed yet
        return True
    return "time" not in ds.coords


def virtualize_day(
    fs: s3fs.S3FileSystem,
    repo: icechunk.Repository,
    bucket: str,
    year: int,
    day: int,
    *,
    first_write: bool,
) -> bool:
    """Virtualize one day of MCMIPF files and append them to the store.

    Returns:
        The new value of ``first_write``: False once a day has been committed.
    """
    urls = [f"s3://{p}" for p in fs.glob(f"s3://{bucket}/ABI-L2-MCMIPF/{year}/{day:03d}/*/*.nc")]
    if not urls:
        return first_write

    caching_store = build_store(bucket)
    registry = ObjectStoreRegistry({f"s3://{bucket}": caching_store})
    parser = vz.parsers.HDFParser(
        drop_variables=drop_variables(), reader_factory=BufferedStoreReader
    )

    try:
        vdses = [
            vz.open_virtual_dataset(
                url, registry=registry, parser=parser, loadable_variables=loadable_variables()
            )
            for url in urls
        ]
        vds = xr.combine_nested(vdses, concat_dim="t", coords="minimal", compat="override")
        vds = vds.rename({"t": "time"})

        session = repo.writable_session("main")
        if first_write:
            vds.vz.to_icechunk(session.store)
        else:
            vds.vz.to_icechunk(session.store, append_dim="time")
        session.commit(f"Wrote {bucket} {year} day {day:03d}")
        # Only after the commit: a failed commit leaves the store uninitialised, and
        # flipping the flag early would make the next day append to nothing.
        first_write = False
    except Exception as exc:  # noqa: BLE001 - a gap in the archive must not stop the run
        print(f"{bucket} {year} day {day:03d} failed: {exc}")
    finally:
        caching_store.clear_cache()

    return first_write


def main() -> None:
    """Virtualize every GOES-West day into the store, resuming where it left off."""
    fs = s3fs.S3FileSystem(anon=True)
    repo = open_repository()
    first_write = store_is_empty(repo)

    for bucket, years in SOURCES:
        for year in years:
            for day in tqdm.tqdm(range(1, 367), total=366, desc=f"{bucket} {year}"):
                first_write = virtualize_day(
                    fs, repo, bucket, year, day, first_write=first_write
                )


if __name__ == "__main__":
    main()
