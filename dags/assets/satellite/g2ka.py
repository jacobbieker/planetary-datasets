import datetime as dt
import os
import shutil
import zipfile

import fsspec
import pandas as pd
import xarray as xr
import zarr
from huggingface_hub import HfApi

"""
--raw-location /run/media/jacob/Square1/europe_clouds_raw --output-location /run/media/jacob/Square1/europe_clouds_zarr --product-id EO:EUM:DAT:MSG:CLM --bands cloud

"""


def get_gk2a_files(time: dt.datetime, raw_location: str, output_location: str) -> list[str]:
    """
    Generate a Kerchunk file from a Himawari file
    """
    so = dict(mode="rb", anon=True, default_fill_cache=False, default_cache_type="first")
    fs_read = fsspec.filesystem("s3", anon=True, skip_instance_cache=True)
    fs2 = fsspec.filesystem("")
    # Do it per day vs per minute, only want ones that are every 10 minutes
    raw_location = f"{raw_location}/AMI/L1B/FD/{time.strftime('%Y%m')}/{time.strftime('%d')}/*/*.nc"
    files = fs_read.glob(raw_location)
    # Remove any files where the minute of day is not a multiple of 10
    files = ["s3://" + f for f in files]
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


def make_gk2a_zarr(time: dt.datetime, files: list[str], output_location: str) -> str:
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
    api = HfApi(token="")
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=f"data/{time.strftime('%Y')}/{zip_name}",
        repo_id=repo_id,
        repo_type="dataset",
    )
    os.remove(zip_name)
    ds.close()


def zip_jsons(time, output_folder):
    zip_name = f"{time.strftime('%Y%m%d')}.zip"
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zip_ref:
        for folder_name, subfolders, filenames in os.walk(output_folder):
            for filename in filenames:
                print(filename)
                file_path = os.path.join(folder_name, filename)
                zip_ref.write(file_path, arcname=os.path.relpath(file_path, output_folder))

    zip_ref.close()
    return zip_name


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
    parser.add_argument("--raw-location", type=str, default="s3://noaa-gk2a-pds")
    parser.add_argument("--output-location", type=str, default="gk2a")
    parser.add_argument("--upload-to-hf", action="store_false")
    parser.add_argument("--hf-token", type=str, default="")
    args = parser.parse_args()
    repo_id = "jacobbieker/gk2a-native"
    start_date = "2023-02-23"
    end_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    for day in date_range[::-1]:
        os.mkdir(args.output_location)
        files = get_gk2a_files(
            day, raw_location=args.raw_location, output_location=args.output_location
        )
        make_gk2a_zarr(day, files, args.output_location)
        shutil.rmtree(args.output_location)
