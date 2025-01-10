import pandas as pd
import xarray as xr

from satpy import Scene
import glob
from pathlib import Path
import zipfile
import tempfile
import matplotlib.pyplot as plt
import zarr
import os

# Import EUMDAC and dependent libraries to begin
import eumdac
import datetime
import shutil

# Insert your personal key and secret


credentials = (consumer_key, consumer_secret)

token = eumdac.AccessToken(credentials)

datastore = eumdac.DataStore(token)

start = datetime.datetime(2025, 1, 2, 0, 0)
end = datetime.datetime(2025, 1, 2, 0, 9)


def download_and_process_normal_and_high_res(start, end) -> xr.DataTree:
    token = eumdac.AccessToken(credentials)

    datastore = eumdac.DataStore(token)
    low_resolution_collection = datastore.get_collection('EO:EUM:DAT:0662')
    high_resolution_collection = datastore.get_collection('EO:EUM:DAT:0665')

    low_res_products = low_resolution_collection.search(dtstart=start,dtend=end)
    high_res_products = high_resolution_collection.search(dtstart=start,dtend=end)

    print(f'Found Datasets: {low_res_products.total_results} low-res datasets and {high_res_products.total_results} high-res datasets for the given time range')
    high_res_output_filenames = []
    low_res_output_filenames = []
    for product in low_res_products:
        with product.open() as fsrc, \
                open(fsrc.name, mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            low_res_output_filenames.append(fsrc.name)
            print(f'Download of low-res product {product} finished.')
    for product in high_res_products:
        with product.open() as fsrc, \
                open(fsrc.name, mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            high_res_output_filenames.append(fsrc.name)
            print(f'Download of high-res product {product} finished.')
    # Load the high and low resolution scenes
    # Sort both of them by name
    low_res_output_filenames.sort()
    high_res_output_filenames.sort()
    # Load the high and low resolution scenes
    datatrees = []
    low_2 = []
    low_1 = []
    high_1 = []
    high_500 = []
    for i in range(len(low_res_output_filenames)):
        low_res_output_filenames[i] = Path(low_res_output_filenames[i])
        high_res_output_filenames[i] = Path(high_res_output_filenames[i])
        mtg_datatree = unzip_folders_and_load_to_xarray(high_res_output_filenames[i], low_res_output_filenames[i])
        print(mtg_datatree)
        save_datatree(mtg_datatree["/hr_05km"], mtg_datatree["/hr_1km"], mtg_datatree["/1km"], mtg_datatree["/2km"])
        low_1.append(mtg_datatree["/1km"])
        low_2.append(mtg_datatree["/2km"])
        high_1.append(mtg_datatree["/hr_1km"])
        high_500.append(mtg_datatree["/hr_05km"])
    # Concatenate each of the nodes based on their time dimension, have to do each node on its own
    # works
    low_1_concatenated = xr.concat(low_1, dim="time").sortby("time")
    low_2_concatenated = xr.concat(low_2, dim="time").sortby("time")
    high_1_concatenated = xr.concat(high_1, dim="time").sortby("time")
    high_500_concatenated = xr.concat(high_500, dim="time").sortby("time")
    concatenated_datatree = convert_to_datatree(high_500_concatenated, high_1_concatenated, low_1_concatenated, low_2_concatenated)
    print(concatenated_datatree)
    # Write to disk



def unzip_folders_and_load_to_xarray(high_res_zip, low_res_zip) -> xr.DataTree:
    zf = zipfile.ZipFile(low_res_zip)
    high_res_zf = zipfile.ZipFile(high_res_zip)
    with tempfile.TemporaryDirectory() as tempdir:
        zf.extractall(tempdir)
        # Get all netCDF in temp director
        filenames = list(glob.glob(f"{tempdir}/*.nc"))
        km_1_0_xr, km_2_0_xr = load_normal_res_scene(filenames)
    with tempfile.TemporaryDirectory() as tempdir:
        high_res_zf.extractall(tempdir)
        filenames = list(glob.glob(f"{tempdir}/*.nc"))
        km_0_5_xr, km_0_1_xr = load_high_res_scene(filenames)
    # Save these to disk as Zarr DataTree
    mtg_datatree = convert_to_datatree(km_0_5_xr, km_0_1_xr, km_1_0_xr, km_2_0_xr)
    return mtg_datatree

def load_high_res_scene(filenames: list[str]) -> tuple[xr.Dataset, xr.Dataset]:
    scn = Scene(reader="fci_l1c_nc", filenames=filenames)
    scn.load(["ir_105", "ir_38", "nir_22", "vis_06"])
    # Different resolution for 2 of them
    km_0_5_xr = scn.to_xarray(["vis_06", "nir_22"]).load().chunk({"x": 1392, "y": 1392})
    km_1_0_xr = scn.to_xarray(["ir_105", "ir_38"]).load().chunk({"x": 1392, "y": 1392})
    km_1_0_xr = km_1_0_xr.drop_vars(["mtg_fci_fdss_1km"])
    km_0_5_xr = km_0_5_xr.drop_vars(["mtg_fci_fdss_500m"])
    # Add a time dimension with the same time taken from the attributes
    km_0_5_xr = km_0_5_xr.expand_dims("time")
    km_1_0_xr = km_1_0_xr.expand_dims("time")
    start_time = pd.Timestamp(km_1_0_xr["ir_105"].attrs["start_time"])
    end_time = pd.Timestamp(km_1_0_xr["ir_105"].attrs["end_time"])
    km_1_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    km_0_5_xr["time"] = [start_time + (end_time - start_time) / 2]
    return km_0_5_xr, km_1_0_xr


def load_normal_res_scene(filenames: list[str]) -> tuple[xr.Dataset, xr.Dataset]:
    scn = Scene(reader="fci_l1c_nc", filenames=filenames)
    scn.load(["ir_105", "ir_123", "ir_133", "ir_38", "ir_87", "ir_97", "nir_22", "nir_13", "nir_16", "vis_04", "vis_05",
              "vis_06", "vis_08", "vis_09", "wv_63", "wv_73"])
    km_1_0_xr = scn.to_xarray(
        ["vis_04", "vis_05", "vis_06", "vis_08", "vis_09", "nir_22", "nir_13", "nir_16", ]).load().chunk(
        {"x": 1392, "y": 1392})
    km_2_0_xr = scn.to_xarray(["ir_105", "ir_123", "ir_133", "ir_38", "ir_87", "ir_97", "wv_63", "wv_73"]).load().chunk(
        {"x": 1392, "y": 1392})
    km_2_0_xr = km_2_0_xr.drop_vars(["mtg_fci_fdss_2km"])
    km_1_0_xr = km_1_0_xr.drop_vars(["mtg_fci_fdss_1km"])
    km_2_0_xr = km_2_0_xr.expand_dims("time")
    km_1_0_xr = km_1_0_xr.expand_dims("time")
    # Get the timestamp halfway between the start and end time
    start_time = pd.Timestamp(km_2_0_xr["ir_105"].attrs["start_time"])
    end_time = pd.Timestamp(km_2_0_xr["ir_105"].attrs["end_time"])
    km_2_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    km_1_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    return km_1_0_xr, km_2_0_xr


def convert_to_datatree(high_res_500m: xr.Dataset, high_res_1km: xr.Dataset, low_res_1km: xr.Dataset, low_res_2km: xr.Dataset) -> xr.DataTree:
    # Make DataArray of just the time coordinate for the root
    time_ds = xr.Dataset({"time": high_res_1km["time"]}, coords={"time": high_res_1km["time"].values})
    full_dataset = xr.DataTree.from_dict(
        {"/": time_ds, "/hr_1km": high_res_1km, "/hr_05km": high_res_500m, "/1km": low_res_1km, "/2km": low_res_2km})
    return full_dataset

def save_datatree(high_res_500m: xr.Dataset, high_res_1km: xr.Dataset, low_res_1km: xr.Dataset, low_res_2km: xr.Dataset) -> xr.DataTree:
    full_dataset = xr.DataTree.from_dict(
        {"hr_1km": high_res_1km, "hr_05km": high_res_500m, "1km": low_res_1km, "2km": low_res_2km})
    print(full_dataset)
    # Write to Zarr v3 format
    # Add compression for each of them chunkwise
    # Get the timestamp from any of them
    timestamp = high_res_1km["time"].values[0]
    # Get it as a string YYYYMMDDTHH:MM:SS
    timestamp = pd.Timestamp(timestamp).strftime("%Y%m%dT%H%M%S")
    encoding = {f"/hr_1km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (5568,5568)}
        for v in high_res_1km.data_vars if "mtg_fci" not in v}}
    encoding.update({f"/hr_05km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (5568,5568)}
        for v in high_res_500m.data_vars if "mtg_fci" not in v}})
    encoding.update({f"/1km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (5568,5568)}
        for v in low_res_1km.data_vars if "mtg_fci" not in v}})
    encoding.update({f"/2km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (5568,5568)}
        for v in low_res_2km.data_vars if "mtg_fci" not in v}})
    full_dataset.to_zarr(f"mtg_datatree_v3_{timestamp}.zarr", mode="w", compute=True, encoding=encoding, zarr_format=3)
    return full_dataset

download_and_process_normal_and_high_res(start, end)
