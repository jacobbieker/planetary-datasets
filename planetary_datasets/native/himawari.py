import datetime as dt
import fsspec
import zipfile
import pandas as pd
import random
import shutil
from huggingface_hub import HfApi
import os
import xarray as xr
import zarr


def get_himawari_files(time: dt.datetime, raw_location: str, output_location: str) -> list[str]:
    """
    Generate a Kerchunk file from a Himawari file
    """
    so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="first")
    fs_read = fsspec.filesystem("s3", anon=True, skip_instance_cache=True)
    fs2 = fsspec.filesystem("")
    # Do it per day vs per minute, only want ones that are every 10 minutes
    raw_location = f"{raw_location}/AHI-L2-FLDK-ISatSS/{time.strftime('%Y')}/{time.strftime('%m')}/{time.strftime('%d')}/*/*.nc"
    files = fs_read.glob(raw_location)
    # Remove any files where the minute of day is not a multiple of 10
    files = ["s3://" + f for f in files if int(f.split("/")[-2]) % 10 == 0]
    # Do the same for the cloud and wind vectors
    raw_location = f"{raw_location}/AHI-L2-FLDK-Clouds/{time.strftime('%Y')}/{time.strftime('%m')}/{time.strftime('%d')}/*/*.nc"
    files_cloud = fs_read.glob(raw_location)
    files_cloud = ["s3://" + f for f in files_cloud if int(f.split("/")[-2]) % 10 == 0]
    raw_location = f"{raw_location}/AHI-L2-FLDK-Winds/{time.strftime('%Y')}/{time.strftime('%m')}/{time.strftime('%d')}/*/*.nc"
    files_wind = fs_read.glob(raw_location)
    files_wind = ["s3://" + f for f in files_wind if int(f.split("/")[-2]) % 10 == 0]
    # Combine the files
    files = sorted(files + files_cloud + files_wind)
    # Save to a directory locally
    output_dir = os.path.join(output_location, time.strftime("%Y%m%d"))
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_files = []
    for file_url in files:
        with fsspec.open(file_url, **so) as infile:
            outfile = file_url.split("/")[-1]
            outfile = os.path.join(output_location, outfile)
            with fs2.open(outfile, "wb") as f:
                f.write(infile.read())
            output_files.append(outfile)
    return output_files


def make_himawari_zarr(time: dt.datetime, files: list[str], output_location: str) -> str:
    # Combine the files
    ds = xr.open_mfdataset(files, engine="h5netcdf", combine="nested", concat_dim="time")
    day_outname = time.strftime("%Y%m%d")
    zip_name = day_outname + ".zarr.zip"
    with zarr.storage.ZipStore(day_outname + ".zarr.zip", mode="w") as store:
        # encodings
        enc = {
            variable: {
                "codecs": [zarr.codecs.BytesCodec(), zarr.codecs.ZstdCodec()],
            }
            for variable in ds.data_vars
        }
        ds.to_zarr(store, mode="w", compute=True, encoding=enc, zarr_format=3, consolidated=True)
    api = HfApi(token="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")
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=f"data/{time.strftime("%Y")}/{zip_name}",
        repo_id=repo_id,
        repo_type="dataset",
    )
    os.remove(zip_name)
    ds.close()


def upload_to_hf(zip_name, hf_token, repo_id):
    api = HfApi(token=hf_token)
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=f"data/{zip_name.split('/')[-1][:4]}/{zip_name}",
        repo_id=repo_id,
        repo_type="dataset",
    )
    os.remove(zip_name)


if __name__ == "__main__":
    import argparse
    from dask.distributed import LocalCluster

    cluster = LocalCluster()
    client = cluster.get_client()
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-location", type=str, default="s3://noaa-himawari9")
    parser.add_argument("--output-location", type=str, default="himawari9")
    parser.add_argument("--upload-to-hf", action="store_true")
    parser.add_argument("--hf-token", type=str, default="")
    args = parser.parse_args()
    assert args.raw_location in ["s3://noaa-himawari9", "s3://noaa-himawari8"]
    if args.raw_location == "s3://noaa-himawari9":
        repo_id = "jacobbieker/himawari9-native"
        start_date = "2022-12-01"
        end_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    elif args.raw_location == "s3://noaa-himawari8":
        repo_id = "jacobbieker/himawari8-native"
        start_date = "2020-01-01"
        end_date = "2022-12-31"
    else:
        ValueError(f"Unknown {args.raw_location=}")
    # From 2020 to end of 2022, which Himwarai 9 comes online
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    start_idx = random.randint(0, len(date_range))
    for day in date_range:
        os.mkdir(args.output_location)
        saved_files = get_himawari_files(
            day, raw_location=args.raw_location, output_location=args.output_location
        )
        make_himawari_zarr(day, saved_files, args.output_location)
        zip_name = zip_jsons(day, args.output_location)
        shutil.rmtree(args.output_location)
