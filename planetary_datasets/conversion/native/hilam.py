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
    date = file_url.split("/")[-5:-1]
    name = file_url.split("/")[-1].split(".")[-2].split("-")[-2]
    level = file_url.split("/")[-6].split("-")[-2]
    return f"{json_dir}{date[0]}{date[1]}{date[2]}{date[3]}_{name}_{level}_message{message_number}.json"


def gen_json(file_url, output_location):
    with fsspec.open(file_url, "rb", anon=True) as f:
        print(f"Downloading: {file_url}")
        with open(f"{file_url.split('/')[-1]}", "wb") as f2:
            f2.write(f.read())
        print(f"Downloaded: {file_url}")
    out = scan_grib(
        f"{file_url.split('/')[-1]}", storage_options={"anon": True}
    )  # create the reference using scan_grib
    for i, message in enumerate(
        out
    ):  # scan_grib outputs a list containing one reference file per grib message
        out_file_name = make_json_name(output_location, file_url, i)  # get name
        print(out_file_name)
        message['templates']['u'] = file_url
        with fsspec.open(out_file_name, "w") as f:
            f.write(ujson.dumps(message))  # write to file
    os.remove(f"{file_url.split('/')[-1]}")


def generate_individual_hilam_kerchunk(time: dt.datetime, output_location: str):
    """
    Generate a Kerchunk file from a GFS file

    Args:
        time: datetime of the HRRR file
        output_location: location to save the Kerchunk file
    """
    protocol = "s3"
    fs_read = fsspec.filesystem(protocol, anon=True, skip_instance_cache=True)
    raw_location = "s3://fmi-opendata-rcrhirlam-surface-grib"
    files = fs_read.glob(
        f"{raw_location}/{time.strftime('%Y')}/{time.strftime('%m')}/{time.strftime('%d')}/{time.strftime('%H')}/*"
    )  # select second last run to ensure it is a complete forecast
    files = sorted(
        ["s3://" + f for f in files]
    )  # Remove index files from it
    # Get the analysis files as well
    raw_location = "s3://fmi-opendata-rcrhirlam-pressure-grib"
    files_prs = fs_read.glob(
        f"{raw_location}/{time.strftime('%Y')}/{time.strftime('%m')}/{time.strftime('%d')}/{time.strftime('%H')}/*")
    files_prs = sorted(
        ["s3://" + f for f in files_prs]
    )  # Remove index files from it
    if len(files) == 0 or len(files_prs) == 0:
        return None
    # Local filenames would be the same, so surface first, then pressure
    bag = db.from_sequence(files)
    bag_map = bag.map(gen_json, output_location=output_location)
    _ = bag_map.compute()
    bag = db.from_sequence(files_prs)
    bag_map = bag.map(gen_json, output_location=output_location)
    _ = bag_map.compute()
    return output_location


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
        repo_id="jacobbieker/hilam-native",
        repo_type="dataset",
    )
    os.remove(zip_name)


if __name__ == "__main__":
    import argparse
    from dask.distributed import LocalCluster

    cluster = LocalCluster()
    client = cluster.get_client()
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-location", type=str, default="hilam/")
    parser.add_argument("--upload-to-hf", action="store_false")
    parser.add_argument("--hf-token", type=str, default="")
    args = parser.parse_args()
    date_range = pd.date_range(
        start="2017-04-17",
        end="2022-10-31",
        freq="6H",
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        os.mkdir(args.output_location)
        generate_individual_hilam_kerchunk(
            day, output_location=args.output_location
        )
        zip_name = zip_jsons(day, args.output_location)
        if args.upload_to_hf:
            upload_to_hf(zip_name, args.hf_token)
            shutil.rmtree(args.output_location)
