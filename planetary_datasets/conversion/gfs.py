import datetime as dt
import fsspec
import ujson
import os, zipfile
import pandas as pd
import random
import shutil
from huggingface_hub import HfApi

from kerchunk.grib2 import scan_grib

import dask.bag as db


def make_json_name(
    json_dir, file_url, message_number
):  # create a unique name for each reference file
    date = file_url.split("/")[3].split(".")[1]
    name = file_url.split("/")[5].split(".")[1:3]
    return f"{json_dir}{date}_{name[0]}_{name[1]}_message{message_number}.json"


def gen_json(file_url):
    try:
        out = scan_grib(
            file_url, storage_options={"anon": True}
        )  # create the reference using scan_grib
        for i, message in enumerate(
            out
        ):  # scan_grib outputs a list containing one reference file per grib message
            out_file_name = make_json_name("jsons/", file_url, i)  # get name
            with fsspec.open(out_file_name, "w") as f:
                f.write(ujson.dumps(message))  # write to file
    except:
        pass


def generate_individual_gfs_kerchunk(time: dt.datetime, raw_location: str, output_location: str):
    """
    Generate a Kerchunk file from a GFS file

    Args:
        time: datetime of the HRRR file
        raw_location: location of the HRRR file
        output_location: location to save the Kerchunk file
    """
    if "s3://" in raw_location:
        protocol = "s3"
    elif "gs://" in raw_location:
        protocol = "gs"
    elif "abfs://" in raw_location or "az://" in raw_location:
        protocol = "abfs"
    else:
        raise ValueError(f"Protocol for {raw_location=} not recognized")
    fs_read = fsspec.filesystem(protocol, anon=True, skip_instance_cache=True)
    fs_write = fsspec.filesystem(output_location)
    json_dir = f"jsons"
    fs_write.mkdir(json_dir)
    files = fs_read.glob(
        f"{raw_location}/gfs.{time.strftime('%Y%m%d')}/{time.strftime('%H')}/gfs.t{time.strftime('%H')}z.pgrb*0p25*f*"
    )  # select second last run to ensure it is a complete forecast
    files = sorted(
        ["s3://" + f for f in files if ".idx" not in f and "atmos" not in f]
    )  # Remove index files from it
    second_check_files = fs_read.glob(
        f"{raw_location}/gfs.{time.strftime('%Y%m%d')}/{time.strftime('%H')}/atmos/gfs.t{time.strftime('%H')}z.pgrb*0p25*f*"
    )  # select second last run to ensure it is a complete forecast
    second_check_files = sorted(
        ["s3://" + f for f in second_check_files if ".idx" not in f]
    )  # Remove index files from it
    # Get the analysis files as well
    files_anl = fs_read.glob(
        f"{raw_location}/gfs.{time.strftime('%Y%m%d')}/{time.strftime('%H')}/gfs.t{time.strftime('%H')}z.pgrb*0p25.anl")
    files_anl = sorted(
        ["s3://" + f for f in files_anl if ".idx" not in f and "atmos" not in f]
    )  # Remove index files from it
    second_check_anl = fs_read.glob(
        f"{raw_location}/gfs.{time.strftime('%Y%m%d')}/{time.strftime('%H')}/atmos/gfs.t{time.strftime('%H')}z.pgrb*0p25.anl")
    second_check_anl = sorted(
        ["s3://" + f for f in second_check_anl if ".idx" not in f]
    )  # Remove index files from it
    files = files + second_check_files + files_anl + second_check_anl
    bag = db.from_sequence(files)
    bag_map = bag.map(gen_json)
    _ = bag_map.compute()
    return json_dir


def zip_jsons(time, output_folder):
    zip_name = f"{time.strftime('%Y%m%d%H')}.zip"
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zip_ref:
        for folder_name, subfolders, filenames in os.walk(output_folder):
            for filename in filenames:
                print(filename)
                file_path = os.path.join(folder_name, filename)
                zip_ref.write(file_path, arcname=os.path.relpath(file_path, output_folder))

    zip_ref.close()
    return zip_name


def upload_to_hf(zip_name, hf_token):
    api = HfApi(token=hf_token)
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=f"data/{zip_name.split('/')[-1][:4]}/{zip_name}",
        repo_id="jacobbieker/gfs-kerchunk",
        repo_type="dataset",
    )
    os.remove(zip_name)


if __name__ == "__main__":
    import argparse
    from dask.distributed import LocalCluster

    cluster = LocalCluster()
    client = cluster.get_client()
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-location", type=str, default="s3://noaa-gfs-bdp-pds")
    parser.add_argument("--output-location", type=str, default="")
    parser.add_argument("--upload-to-hf", action="store_true")
    parser.add_argument("--hf-token", type=str, default="")
    args = parser.parse_args()
    date_range = pd.date_range(
        start="2021-02-26",
        end=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
        freq="6H",
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        print(day)
        json_dir = generate_individual_gfs_kerchunk(day, args.raw_location, args.output_location)
        zip_name = zip_jsons(day, json_dir)
        if args.upload_to_hf:
            upload_to_hf(zip_name, args.hf_token)
            shutil.rmtree(json_dir)
