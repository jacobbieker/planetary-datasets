"""This gather and uses the global mosaic of geostationary satellites from NOAA on AWS"""

import datetime as dt
import os
from typing import TYPE_CHECKING, Optional

import dagster as dg
import dask.array
import fsspec
import s3fs
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import icechunk
from virtualizarr import open_virtual_dataset

if TYPE_CHECKING:
    import datetime as dt

"""Icechunk of virtualizarr of the GOES satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "./"

goes19_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2024-10-10-00:00",
    end_offset=-2,
)  # Day 284 of 2024 is first day of data, hour 19:00

goes18_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2022-07-28-00:00",
    end_offset=-2,
)  # Day 209 of 2022 00:00 is first day of data

goes17_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2024-08-28-00:00",
    end_date="2024-01-15-10:00",
)  # Day 240 of 2018 00:00 until 2023 15 10:00

goes16_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2017-02-28-00:00",
    end_offset=-2,
)  # Day 059 of 2017 00:00 is first day of data, '-Reproc' data is available from 2018 004 until 2024 363 22:00

goes16_reproc_partitions_def: dg.TimeWindowPartitionsDefinition = dg.HourlyPartitionsDefinition(
    start_date="2018-01-04-00:00",
    end_date="2024-12-28-22:00",
)  # Day 059 of 2017 00:00 is first day of data, '-Reproc' data is available from 2018 004 until 2024 363 22:00

# Add a different partition, by band
bands = [str(i) for i in range(1, 17)]
band_partition: dg.StaticPartitionsDefinition = dg.StaticPartitionsDefinition(bands)

goes16_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes16_partitions_def, "band": band_partition}
)
goes17_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes17_partitions_def, "band": band_partition}
)
goes18_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes18_partitions_def, "band": band_partition}
)
goes19_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes19_partitions_def, "band": band_partition}
)
goes16_reproc_two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": goes16_reproc_partitions_def, "band": band_partition}
)


@dg.asset(
    name="goes16-virtualizarr",
    description="Create Virtualizarr reference of GOES-16 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "goes-icechunk",
    },
    partitions_def=goes16_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def goes16_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes16"

    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


@dg.asset(
    name="goes17-virtualizarr",
    description="Create Virtualizarr reference of GOES-17 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "goes-icechunk",
    },
    partitions_def=goes17_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def goes17_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes17"

    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


@dg.asset(
    name="goes18-virtualizarr",
    description="Create Virtualizarr reference of GOES-18 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "goes-icechunk",
    },
    partitions_def=goes18_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def goes18_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes18"

    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


@dg.asset(
    name="goes19-virtualizarr",
    description="Create Virtualizarr reference of GOES-19 satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "goes-icechunk",
    },
    partitions_def=goes18_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def goes19_virtualizarr_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes19"

    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    create_and_write_virtualizarr(files, satellite, band)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


@dg.asset(
    name="goes16-reproc-virtualizarr",
    description="Create Virtualizarr reference of GOES-16 Reproc satellite data from NOAA on AWS",
    tags={
        "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
        "dagster/priority": "1",
        "dagster/concurrency_key": "goes-icechunk",
    },
    partitions_def=goes16_reproc_two_dimensional_partitions,
    # automation_condition=dg.AutomationCondition.eager(),
)
def goes16_virtualizarr_reproc_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    it: dt.datetime = context.partition_time_window.start
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    band = keys_by_dimension["band"]

    fs = s3fs.S3FileSystem(anon=True)
    # Get the day of year
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes16"

    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    create_and_write_virtualizarr(files, satellite + "_reproc", band)

    return dg.MaterializeResult(
        metadata={"date": it, "band": band},
    )


def create_and_write_virtualizarr(files: list[str], satellite: str, band: int, it: dt.datetime):
    files = ["s3://" + f for f in files]
    # print(files)
    virtual_datasets = [
        open_virtual_dataset(
            filepath,
            loadable_variables=["t", "x", "y"],
            reader_options={"storage_options": {"anon": True}},
        )
        for filepath in files
    ]

    vd = xr.concat(
        virtual_datasets, dim="t", coords="minimal", compat="override", combine_attrs="override"
    )
    vd.x.encoding = {}
    vd.y.encoding = {}
    if not os.path.exists(
        os.path.join(
            f"{ARCHIVE_FOLDER}",
            f"{it.strftime('%Y%m%d%H')}",
        )
    ):
        os.makedirs(
            os.path.join(
                f"{ARCHIVE_FOLDER}",
                f"{it.strftime('%Y%m%d%H')}",
            )
        )
    store_location = os.path.join(
        f"{ARCHIVE_FOLDER}",
        f"{it.strftime('%Y%m%d%H')}",
        f"{satellite}_band{str(band).zfill(2)}_test.parquet",
    )
    vd.virtualize.to_kerchunk(store_location, format="parquet")


def _make_async(fs):
    """Convert a sync FSSpec filesystem to an async FFSpec filesystem
    If the filesystem class supports async operations, a new async instance is created
    from the existing instance.
    If the filesystem class does not support async operations, the existing instance
    is wrapped with AsyncFileSystemWrapper.
    """
    import fsspec
    from packaging.version import parse as parse_version

    fsspec_version = parse_version(fsspec.__version__)
    if fs.async_impl and fs.asynchronous:
        # Already an async instance of an async filesystem, nothing to do
        return fs
    if fs.async_impl:
        # Convert sync instance of an async fs to an async instance
        import json

        fs_dict = json.loads(fs.to_json())
        fs_dict["asynchronous"] = True
        return fsspec.AbstractFileSystem.from_json(json.dumps(fs_dict))

    # Wrap sync filesystems with the async wrapper
    if type(fs) is fsspec.implementations.local.LocalFileSystem and not fs.auto_mkdir:
        raise ValueError(
            f"LocalFilesystem {fs} was created with auto_mkdir=False but Zarr requires the filesystem to automatically create directories"
        )
    if fsspec_version < parse_version("2024.12.0"):
        raise ImportError(
            "The filesystem '{fs}' is synchronous, and the required "
            "AsyncFileSystemWrapper is not available. Upgrade fsspec to version "
            "2024.12.0 or later to enable this functionality."
        )

    return fsspec.implementations.asyn_wrapper.AsyncFileSystemWrapper(fs, asynchronous=True)


if __name__ == "__main__":
    # Test the asset
    fs = s3fs.S3FileSystem(anon=True)
    # fs = fsspec.filesystem("local")
    """
    it = pd.Timestamp("2025-01-01-00:00")
    it: pd.Timestamp = pd.Timestamp(it)
    satellite = "goes19"
    while it < pd.Timestamp.now():
        for band in [str(i) for i in range(1, 17)]:
            band = 8
            files = sorted(
                fs.glob(
                    f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"))
            #files = sorted(fs.glob(f"/Users/jacob/Development/planetary-datasets/GOES/*/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}*"))
            print(files)
            create_and_write_virtualizarr(files, satellite, band, it)
        it += pd.Timedelta("1h")
        exit()
    #"""
    # exit()

    from kerchunk import hdf, combine, df
    from kerchunk.combine import MultiZarrToZarr
    import fsspec.implementations.reference
    from fsspec.implementations.reference import LazyReferenceMapper
    from tempfile import TemporaryDirectory
    import ujson

    import xarray as xr

    band = 8
    it = pd.Timestamp("2025-01-01-01:00")
    satellite = "goes19"
    files = sorted(
        fs.glob(
            f"noaa-{satellite}/ABI-L1b-RadF/{it.year}/{str(it.dayofyear).zfill(3)}/{str(it.hour).zfill(2)}/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_*.nc"
        )
    )
    files = ["s3://" + f for f in files]
    # Create LazyReferenceMapper to pass to MultiZarrToZarr
    fs = fsspec.filesystem("file")
    out_dir = "combined_0.parq"
    os.makedirs(out_dir)
    # out = LazyReferenceMapper.create(record_size=1000, root=out_dir, fs=fs)

    # Create references from input files
    single_ref_sets = [
        hdf.SingleHdf5ToZarr(_, storage_options={"anon": True}).translate() for _ in files
    ]
    # Write to disk first
    # for f in files:
    #    with open(f.split("/")[-1]+".json", "wb") as f2:
    #        encoding = hdf.SingleHdf5ToZarr(f, storage_options={"anon": True}).translate()
    #        f2.write(ujson.dumps(encoding).encode())
    # exit()
    # print(single_ref_sets)
    out_dict = MultiZarrToZarr(
        single_ref_sets,
        remote_protocol="s3",
        concat_dims=["t"],
        identical_dims=["lat", "lon"],
        remote_options={"anon": True},
        # out=out
    ).translate()

    # Write out to disk
    with open("combined.json", "wb") as f:
        f.write(ujson.dumps(out_dict).encode())

    # out.flush()

    backend_args = {
        "consolidated": False,
        "storage_options": {
            "fo": "combined.json",
            "remote_protocol": "s3",
            "target_protocol": "local",
            "remote_options": {"anon": True},
        },
    }
    print(xr.open_dataset("reference://", engine="zarr", backend_kwargs=backend_args))
    exit()
    df.refs_to_dataframe(out_dict, out_dir)

    fs = fsspec.implementations.reference.ReferenceFileSystem(
        out_dir,
        remote_protocol="s3",
        target_protocol="file",
        lazy=True,
        remote_options={"anon": True},
    )
    fs_map = fs.get_mapper()
    if not fs_map.fs.async_impl or not fs_map.fs.asynchronous:
        fs_map.fs = _make_async(fs_map.fs)
    import zarr

    store = zarr.storage.FsspecStore(fs_map.fs, path=fs_map.root)
    ds = xr.open_dataset(store, engine="zarr", backend_kwargs={"consolidated": False})
    print(ds)
