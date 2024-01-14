"""Convert EUMETSAT raw imagery files to Zarr"""
try:
    import satip
except ImportError:
    print("Please install Satip to continue")

# TODO 4 ones to do, 0 Degree, IODC, Cloud Mask 0 Deg, Cloud Mask IODC


import pandas as pd
import subprocess
import os
from satip.eumetsat import DownloadManager
from satip.scale_to_zero_to_one import ScaleToZeroToOne
from satip.utils import serialize_attrs, convert_scene_to_dataarray
import datetime as dt
from huggingface_hub import HfApi
import random
from satpy import Scene
import xarray as xr
import numpy as np
import zarr
from ocf_blosc2 import Blosc2
import zipfile
import shutil
import psutil

scaler = ScaleToZeroToOne(
    mins=np.array(
        [
            -2.5118103,
            -64.83977,
            63.404694,
            2.844452,
            199.10002,
            -17.254883,
            -26.29155,
            -1.1009827,
            -2.4184198,
            199.57048,
            198.95093,
        ]
    ),
    maxs=np.array(
        [
            69.60857,
            339.15588,
            340.26526,
            317.86752,
            313.2767,
            315.99194,
            274.82297,
            93.786545,
            101.34922,
            249.91806,
            286.96323,
        ]
    ),
    variable_order=[
        "IR_016",
        "IR_039",
        "IR_087",
        "IR_097",
        "IR_108",
        "IR_120",
        "IR_134",
        "VIS006",
        "VIS008",
        "WV_062",
        "WV_073",
    ],
)
hrv_scaler = ScaleToZeroToOne(
    variable_order=["HRV"], maxs=np.array([103.90016]), mins=np.array([-1.2278595])
)


def download_product_range(
    api_key: str,
    api_secret: str,
    data_dir: str,
    product_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
):
    download_manager = DownloadManager(user_key=api_key, user_secret=api_secret, data_dir=data_dir)
    start_str = start_date.strftime("%Y-%m-%d %H:%M:00")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:00")
    date_range = pd.date_range(
        start=pd.Timestamp(start_str), end=pd.Timestamp(end_str), freq="15min"
    )
    filenames_downloaded = []
    for filename in os.listdir(data_dir):
        filenames_downloaded.append(filename.split("/")[-1])
    for date in date_range:
        start_date = date
        end_date = date + dt.timedelta(minutes=30)
        datasets = download_manager.identify_available_datasets(
            start_date=start_date.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end_date.tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
            product_id=product_id,
        )
        filtered_datasets = []
        for dataset in datasets:
            if dataset["id"] not in filenames_downloaded:
                filtered_datasets.append(dataset)
        datasets = filtered_datasets
        download_manager.download_datasets(datasets, product_id=product_id)


def process_hrv(scene):
    scene.load(["HRV"])
    print("Loaded HRV")
    hrv_dataarray: xr.DataArray = convert_scene_to_dataarray(
        scene, band="HRV", area="RSS", calculate_osgb=False
    ).load()
    print("Loaded HRV DataArray into memory")
    del scene
    attrs = serialize_attrs(hrv_dataarray.attrs)
    hrv_dataarray = hrv_scaler.rescale(hrv_dataarray)
    hrv_dataarray = hrv_dataarray.transpose(
        "time", "y_geostationary", "x_geostationary", "variable"
    )
    # Larger chunks as there are too many small ones in the current thing
    hrv_dataarray = hrv_dataarray.chunk(
        {"time": 1, "y_geostationary": 1024, "x_geostationary": 1024, "variable": 1}
    )
    hrv_dataset = hrv_dataarray.to_dataset(name="data")
    hrv_dataset.attrs.update(attrs)
    print("Returning HRV Dataset")
    return hrv_dataset


def process_nonhrv(scene):
    scene.load(
        [
            "IR_016",
            "IR_039",
            "IR_087",
            "IR_097",
            "IR_108",
            "IR_120",
            "IR_134",
            "VIS006",
            "VIS008",
            "WV_062",
            "WV_073",
        ]
    )
    print("Loaded non-HRV")
    nonhrv_dataarray: xr.DataArray = convert_scene_to_dataarray(
        scene, band="IR_016", area="RSS", calculate_osgb=False
    ).load()
    print("Loaded non-HRV DataArray into memory")
    del scene
    attrs = serialize_attrs(nonhrv_dataarray.attrs)
    nonhrv_dataarray = scaler.rescale(nonhrv_dataarray)
    nonhrv_dataarray = nonhrv_dataarray.transpose(
        "time", "y_geostationary", "x_geostationary", "variable"
    )
    # Larger chunks as there are too many small ones in the current thing
    nonhrv_dataarray = nonhrv_dataarray.chunk(
        {"time": 1, "y_geostationary": 512, "x_geostationary": 512, "variable": -1}
    )
    nonhrv_dataset = nonhrv_dataarray.to_dataset(name="data")
    nonhrv_dataset.attrs.update(attrs)
    print("Returning non-HRV Dataset")
    return nonhrv_dataset


def process_cloud_mask(scene):
    scene.load(["cloud_mask"])
    cloud_mask_dataarray: xr.DataArray = convert_scene_to_dataarray(
        scene, band="cloud_mask", area="RSS", calculate_osgb=False
    ).load()
    del scene
    attrs = serialize_attrs(cloud_mask_dataarray.attrs)
    cloud_mask_dataarray = cloud_mask_dataarray.transpose(
        "time", "y_geostationary", "x_geostationary", "variable"
    )
    # Larger chunks as there are too many small ones in the current thing
    cloud_mask_dataarray = cloud_mask_dataarray.chunk(
        {"time": 1, "y_geostationary": 1024, "x_geostationary": 1024, "variable": 1}
    )
    cloud_mask_dataset = cloud_mask_dataarray.to_dataset(name="data")
    cloud_mask_dataset.attrs.update(attrs)
    return cloud_mask_dataset


def save_to_zarr(dataset, path):
    encoding = {var: {"compressor": Blosc2("zstd", clevel=9)} for var in dataset.data_vars}
    # make sure variable is string
    dataset = dataset.assign_coords({"variable": dataset.coords["variable"].astype(str)})
    print(f"Saving to Zarr: {path}")
    with zarr.ZipStore(path) as store:
        dataset.to_zarr(store, compute=True, mode="w", encoding=encoding, consolidated=True)
    return path


def download_and_process_eumetsat_day(
    day: dt.datetime,
    end_date: dt.datetime,
    product_id: str,
    raw_location: str,
    output_location: str,
    api_key: str,
    api_secret: str,
    bands: str = "hrv",
):
    """Download and process a single day of EUMETSAT data

    Each timestamp stays its own zarr.zip, and then they are all zipped together for the day, before being uploaded

    HRV is padded to the full disk with  reader_kwargs={'fill_disk': True}

    Args:
        day (dt.datetime): Day to download
        raw_location (str): Location of raw data
        output_location (str): Location to save processed data
    """
    download_product_range(
        api_key=api_key,
        api_secret=api_secret,
        data_dir=raw_location,
        product_id=product_id,
        start_date=pd.Timestamp(day),
        end_date=pd.Timestamp(end_date),
    )
    processed_filenames = []
    for filename in os.listdir(raw_location):
        # All Native or GRIB2, if cloud mask
        if ".nat" in filename:
            scene = Scene(
                filenames={"seviri_l1b_native": [os.path.join(raw_location, filename)]},
                reader_kwargs={"fill_disk": True},
            )
            if bands == "hrv":
                hrv_dataset = process_hrv(scene)
                now_time = pd.Timestamp(hrv_dataset["time"].values[0]).strftime("%Y%m%d%H%M")
                processed_filenames.append(
                    save_to_zarr(
                        hrv_dataset, os.path.join(output_location, f"{now_time}_hrv.zarr.zip")
                    )
                )
                del hrv_dataset
            else:
                nonhrv_dataset = process_nonhrv(scene)
                now_time = pd.Timestamp(nonhrv_dataset["time"].values[0]).strftime("%Y%m%d%H%M")
                processed_filenames.append(
                    save_to_zarr(
                        nonhrv_dataset, os.path.join(output_location, f"{now_time}_nonhrv.zarr.zip")
                    )
                )
                del nonhrv_dataset
        elif ".grb" in filename:
            scene = Scene(filenames={"seviri_l2_grib": [os.path.join(raw_location, filename)]})
            cloud_mask_dataset = process_cloud_mask(scene)
            now_time = pd.Timestamp(cloud_mask_dataset["time"].values[0]).strftime("%Y%m%d%H%M")
            processed_filenames.append(
                save_to_zarr(
                    cloud_mask_dataset,
                    os.path.join(output_location, f"{now_time}_cloud_mask.zarr.zip"),
                )
            )

    # Remove the raw files location
    shutil.rmtree(raw_location)
    # Processed filenames are files from the day
    processed_filenames = [
        os.path.join(output_location, filename) for filename in os.listdir(output_location)
    ]
    return processed_filenames


def zip_zarrs(time, output_folder, bands: str = "hrv"):
    zip_name = f"{time.strftime('%Y%m%d%H%M')}_{bands}.zip"
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zip_ref:
        for folder_name, subfolders, filenames in os.walk(output_folder):
            for filename in filenames:
                print(filename)
                file_path = os.path.join(folder_name, filename)
                zip_ref.write(file_path, arcname=os.path.relpath(file_path, output_folder))
    zip_ref.close()
    return zip_name


def upload_to_hf(zip_name, hf_token, repo_id, path_in_repo=None):
    api = HfApi(token=hf_token)
    if path_in_repo is None:
        path_in_repo = f"data/{zip_name.split('/')[-1][:4]}/{zip_name}"
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=path_in_repo,
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
    parser.add_argument("--raw-location", type=str, default="native/")
    parser.add_argument("--output-location", type=str, default="zarrs/")
    parser.add_argument("--product-id", type=str, default="EO:EUM:DAT:MSG:HRSEVIRI")
    parser.add_argument("--api-key", type=str, default="")
    parser.add_argument("--api-secret", type=str, default="")
    parser.add_argument("--upload-to-hf", action="store_false")
    parser.add_argument("--hf-token", type=str, default="")
    parser.add_argument("--bands", type=str, default="hrv")
    args = parser.parse_args()
    assert args.product_id in [
        "EO:EUM:DAT:MSG:CLM-IODC",
        "EO:EUM:DAT:MSG:CLM",
        "EO:EUM:DAT:MSG:HRSEVIRI",
        "EO:EUM:DAT:MSG:HRSEVIRI-IODC",
        "EO:EUM:DAT:MSG:MSG15-RSS",
        "EO:EUM:DAT:MSG:RSS-CLM",
    ]
    # Get the proper date range for the type of product
    if args.product_id == "EO:EUM:DAT:MSG:CLM-IODC":
        start_str = "2017-02-01"
        repo_id = "jacobbieker/eumetsat-cloudmask-iodc"
        is_mask = True
    elif args.product_id == "EO:EUM:DAT:MSG:CLM":
        start_str = "2020-09-01"
        repo_id = "jacobbieker/eumetsat-cloudmask-0deg"
        is_mask = True
    elif args.product_id == "EO:EUM:DAT:MSG:HRSEVIRI":
        start_str = "2004-01-19"
        repo_id = "jacobbieker/eumetsat-0deg"
        is_mask = False
    elif args.product_id == "EO:EUM:DAT:MSG:HRSEVIRI-IODC":
        start_str = "2017-02-01"
        repo_id = "jacobbieker/eumetsat-iodc"
        is_mask = False
    elif args.product_id == "EO:EUM:DAT:MSG:MSG15-RSS":
        start_str = "2008-05-13"
        repo_id = "jacobbieker/eumetsat-rss"
        is_mask = False
    elif args.product_id == "EO:EUM:DAT:MSG:RSS-CLM":
        start_str = "2013-02-28"
        repo_id = "jacobbieker/eumetsat-rss-cloudmask"
        is_mask = True
    print(f"Total Memory: {psutil.virtual_memory().total / 1024 / 1024 / 1024} GB")
    date_range = pd.date_range(
        start=start_str,
        end=(dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
        freq="1D" if is_mask else "15min",
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        os.mkdir(args.output_location)
        end_date = day + dt.timedelta(days=1) if is_mask else day + dt.timedelta(minutes=15)
        final_filenames = download_and_process_eumetsat_day(
            day,
            end_date,
            raw_location=args.raw_location,
            output_location=args.output_location,
            product_id=args.product_id,
            api_key=args.api_key,
            api_secret=args.api_secret,
            bands=args.bands,
        )
        if not is_mask:
            for zip_name in final_filenames:
                path_in_repo = f"data/{zip_name.split('/')[-1][:4]}/{day.strftime('%m')}/{day.strftime('%d')}/{zip_name.split('/')[-1]}"
                upload_to_hf(zip_name, args.hf_token, repo_id=repo_id, path_in_repo=path_in_repo)
                shutil.rmtree(args.output_location)
        else:
            zip_name = zip_zarrs(day, args.output_location, bands=args.bands)
            upload_to_hf(zip_name, args.hf_token, repo_id=repo_id)
            shutil.rmtree(args.output_location)
