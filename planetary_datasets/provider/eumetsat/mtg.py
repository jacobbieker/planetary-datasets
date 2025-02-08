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
import numpy as np
import dask
from numcodecs.zarr3 import BitRound
from ocf_blosc2.ocf_blosc2_v3 import Blosc2


# Insert your personal key and secret
# Insert your personal key and secret
consumer_key = 'wMN6OrRdp5FufHFavHxA5TUwkzca'
consumer_secret = 'lAFCASbfE_bzd6alfIMdfrwhMkAa'

credentials = (consumer_key, consumer_secret)

token = eumdac.AccessToken(credentials)

datastore = eumdac.DataStore(token)

start = datetime.datetime(2025, 1, 1, 0, 0)
end = datetime.datetime(2025, 1, 1, 23, 59)

MTG_MINS = {
    "ir_133": 182,
    "wv_63": 185,
    "ir_105": 180,
    "ir_38": 138,
    "ir_97": 204,
    "wv_73": 184,
    "ir_123": 180,
    "ir_87": 182,
    "vis_08": -2,
    "nir_16": -3,
    "nir_22": -7,
    "vis_09": -2,
    "vis_06": -6,
    "vis_05": -2,
    "nir_13": -5,
    "vis_04": -2,
}
MTG_MAXS = {
    "ir_133": 301,
    "wv_63": 261,
    "ir_105": 338,
    "ir_38": 452,
    "ir_97": 297,
    "wv_73": 282,
    "ir_123": 334,
    "ir_87": 334,
    "vis_08": 130,
    "nir_16": 109,
    "nir_22": 109,
    "vis_09": 87,
    "vis_06": 130,
    "vis_05": 130,
    "nir_13": 87,
    "vis_04": 130,
}

# Make into DataSet
MTG_MINS = xr.Dataset(MTG_MINS)
MTG_MAXS = xr.Dataset(MTG_MAXS)

def download_mtg_data(start, end):
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


def write_zarr_outline(data: xr.Dataset, path: str, shard_size: int, chunk_size=1392):
    print("Path Not Existing")
    #zarr_date_range = pd.date_range("2024-12-01 00:05:00", "2024-12-02 00:05:00", freq="10min")

    variables = list(data.data_vars)

    # Set all to float 16
    for v in variables:
        data[v] = data[v].astype(np.float16)
    # Scale between 0 and 1
    #for v in variables:
    #    data[v] = (data[v] - MTG_MINS[v]) / (MTG_MAXS[v] - MTG_MINS[v])

    # Have to do it on a per-dataset option
    # Hacky, as based off the resolution, but then should be at least correct here
    # These bitrounding is calculated based off a single frame at noon December 1st 2024, so might need more of these to be sure
   # """
    if len(data.x.values) == 5568*4:
        pass
    elif len(data.x.values) == 5568*2:
        if "ir_105" in data.data_vars: # High resolution 1km data
            pass
            # Make these float16 in the dataset
            #data["ir_105"] = data["ir_105"].astype(np.float16)
            #data["ir_38"] = data["ir_38"].astype(np.float16)
        else: # Low resolution 1km data
            data = data.drop_vars(["nir_22", "vis_06"])
            # data["vis_04"] = data["vis_04"].astype(np.float16)
            #data["vis_05"] = data["vis_05"].astype(np.float16)
            #data["vis_06"] = data["vis_06"].astype(np.float16)
            #data["vis_08"] = data["vis_08"].astype(np.float16)
            # data["vis_09"] = data["vis_09"].astype(np.float16)
            # data["nir_22"] = data["nir_22"].astype(np.float16)
            # data["nir_13"] = data["nir_13"].astype(np.float16)
            #data["nir_16"] = data["nir_16"].astype(np.float16)
            # Make these float16 in the dataset
    else: # low resolution 2km data
        # Drop the low resolution ones that have counterpoints in the high resolution stuff
        data = data.drop_vars(["ir_105", "ir_38"])
    #"""

    encoding = {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "filters": BitRound(keepbits=9)} for v in data.data_vars} # "shards": (1, shard_size, shard_size) #



    print(data)
    print(encoding)
    #import xbitinfo as xb
    #bitinfo = xb.get_bitinformation(data, dim="x",
    #                                implementation="python")  # calling bitinformation.jl.bitinformation
    #print(bitinfo)
    #keepbits = xb.get_keepbits(bitinfo, inflevel=0.999)  # get number of mantissa bits to keep for 99% real information
    #print(keepbits)
    # Write the data to the path
    data.to_zarr(path, mode="w", compute=True, zarr_format=3, encoding=encoding)
    """
    # Create empty dask arrays of the same size as the data
    dask_arrays = []
    dummies_4d_var = dask.array.zeros(
        (len(zarr_date_range), data.x.shape[0], data.y.shape[0]), chunks=(1, chunk_size, chunk_size),
        dtype=np.float32)
    default_dataarray = xr.DataArray(dummies_4d_var, coords={"time": zarr_date_range,
                                                             "x": data.x.values,
                                                             "y": data.y.values},
                                     dims=["time", "x", "y"])
    dummy_dataset = xr.Dataset({v: default_dataarray for v in variables},
                               coords={"time": zarr_date_range,
                                       "x": data.x.values,
                                       "y": data.y.values})
    # Set metadata from attrs in real data
    for v in variables:
        dummy_dataset[v].attrs = data[v].attrs
    #dummy_dataset.coords["longitude"] = data.longitude.values
    #dummy_dataset.coords["latitude"] = data.latitude
    print(dummy_dataset)
    # storage_options={"endpoint_url": "https://data.source.coop"}
    dummy_dataset.chunk({"time": 1, "x": chunk_size, "y": chunk_size}).to_zarr(path, mode="w",
                                                                                              compute=False,
                                                                                              zarr_format=3,
                                                                                              encoding=encoding)
    """


def process_normal_and_high_res(low_res_output_filenames: list, high_res_output_filenames: list) -> xr.DataTree:
    # Load the high and low resolution scenes
    # Sort both of them by name
    low_res_output_filenames.sort()
    high_res_output_filenames.sort()
    zarr_date_range = pd.date_range("2024-12-01 00:05:00", "2024-12-02 00:05:00", freq="10min")
    # Load the high and low resolution scenes
    datatrees = []
    low_2 = []
    low_1 = []
    high_1 = []
    high_500 = []
    time_ds = []
    low_res_output_filenames[0] = Path(low_res_output_filenames[0])
    high_res_output_filenames[0] = Path(high_res_output_filenames[0])
    mtg_datatree = unzip_folders_and_load_to_xarray(high_res_output_filenames[0], low_res_output_filenames[0])
    #low_1.append(mtg_datatree["/1km"].ds)
    #low_2.append(mtg_datatree["/2km"].ds)
    #high_1.append(mtg_datatree["/hr_1km"].ds)
    #high_500.append(mtg_datatree["/hr_05km"].ds)
    #time_ds.append(mtg_datatree["/"].ds)
    path = "/run/media/jacob/Elements/MTG_float16_blosc_keepbit9_min/mtg"
    # Write metadata to disk for all of this to disk, to then write the regions
    if not os.path.exists(path):
        print("Path Not Existing")
        #write_zarr_outline(mtg_datatree[0], path+"_hr_1km.zarr", shard_size=5568*2)
        #write_zarr_outline(mtg_datatree[1], path+"_hr_500m.zarr", shard_size=5568*4)
        #write_zarr_outline(mtg_datatree[2], path+"_lr_1km.zarr", shard_size=5568*2)
        #write_zarr_outline(mtg_datatree[3], path+"_lr_2km.zarr", shard_size=5568)


    for i in range(len(low_res_output_filenames)): # First one already written
        low_res_output_filenames[i] = Path(low_res_output_filenames[i])
        high_res_output_filenames[i] = Path(high_res_output_filenames[i])
        mtg_datatree = unzip_folders_and_load_to_xarray(high_res_output_filenames[i], low_res_output_filenames[i])
        write_zarr_outline(mtg_datatree[0], path + f"_hr_1km_{i}.zarr", shard_size=5568 * 2)
        write_zarr_outline(mtg_datatree[1], path + f"_hr_500m_{i}.zarr", shard_size=5568 * 4)
        write_zarr_outline(mtg_datatree[2], path + f"_lr_1km_{i}.zarr", shard_size=5568 * 2)
        write_zarr_outline(mtg_datatree[3], path + f"_lr_2km_{i}.zarr", shard_size=5568)
        # Get the time
        #time_idx = np.where(zarr_date_range == mtg_datatree["/"].time.values[0])[0][0]
        #print(time_idx)
        # Write the data to the zarr
        #mtg_datatree[2].to_zarr(path+"_lr_1km.zarr", mode="a", append_dim="time")
        #mtg_datatree[3].to_zarr(path+"_lr_2km.zarr", mode="a", append_dim="time")
        #mtg_datatree[0].to_zarr(path+"_hr_1km.zarr", mode="a", append_dim="time")
        #mtg_datatree[1].to_zarr(path+"_hr_500m.zarr", mode="a", append_dim="time")
        #mtg_datatree["/2km"].ds.to_zarr(path + "_lr_2km.zarr",
        #                                region={"time": slice(time_idx, time_idx + 1), "x": "auto", "y": "auto"}, )
        #mtg_datatree["/hr_1km"].ds.to_zarr(path + "_hr_1km.zarr",
        #                                     region={"time": slice(time_idx, time_idx + 1), "x": "auto", "y": "auto"}, )
        #mtg_datatree["/hr_05km"].ds.to_zarr(path + "_hr_500m.zarr",
        #                                        region={"time": slice(time_idx, time_idx + 1), "x": "auto", "y": "auto"}, )
        print(f"Finished Writing: {low_res_output_filenames[i]}")
        # Now close the dataset to close all files



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
    #mtg_datatree = convert_to_datatree(km_0_5_xr, km_0_1_xr, km_1_0_xr, km_2_0_xr)
    # Set all their latitude and longitude to float32
    # Drop latitude and longitude
    km_0_5_xr = km_0_5_xr.drop_vars(["longitude", "latitude"])
    km_0_1_xr = km_0_1_xr.drop_vars(["longitude", "latitude"])
    km_1_0_xr = km_1_0_xr.drop_vars(["longitude", "latitude"])
    km_2_0_xr = km_2_0_xr.drop_vars(["longitude", "latitude"])
    return km_0_1_xr, km_0_5_xr, km_1_0_xr, km_2_0_xr

def load_high_res_scene(filenames: list[str]) -> tuple[xr.Dataset, xr.Dataset]:
    scn = Scene(reader="fci_l1c_nc", filenames=filenames)
    scn.load(["ir_105", "ir_38", "nir_22", "vis_06"])
    # Different resolution for 2 of them
    km_0_5_xr = scn.to_xarray(["vis_06", "nir_22"]).load().chunk({"x": 5568, "y": 5568})
    km_1_0_xr = scn.to_xarray(["ir_105", "ir_38"]).load().chunk({"x": 5568, "y": 5568})
    km_1_0_xr = km_1_0_xr.drop_vars(["mtg_fci_fdss_1km"])
    km_0_5_xr = km_0_5_xr.drop_vars(["mtg_fci_fdss_500m"])
    # Add a time dimension with the same time taken from the attributes
    km_0_5_xr = km_0_5_xr.expand_dims("time")
    km_1_0_xr = km_1_0_xr.expand_dims("time")
    start_time = pd.Timestamp(km_1_0_xr["ir_105"].attrs["start_time"])
    end_time = pd.Timestamp(km_1_0_xr["ir_105"].attrs["end_time"])
    km_1_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    km_0_5_xr["time"] = [start_time + (end_time - start_time) / 2]
    #km_0_5_xr = km_0_5_xr.drop_vars(["longitude", "latitude"])
    #km_1_0_xr = km_1_0_xr.drop_vars(["longitude", "latitude"])
    km_1_0_xr["ir_38"] = km_1_0_xr["ir_38"].astype(np.float16)
    km_1_0_xr["ir_105"] = km_1_0_xr["ir_105"].astype(np.float16)
    km_0_5_xr["nir_22"] = km_0_5_xr["nir_22"].astype(np.float16)
    km_0_5_xr["vis_06"] = km_0_5_xr["vis_06"].astype(np.float16)
    # Set latitude and longitude to float32
    return km_0_5_xr, km_1_0_xr


def load_normal_res_scene(filenames: list[str]) -> tuple[xr.Dataset, xr.Dataset]:
    scn = Scene(reader="fci_l1c_nc", filenames=filenames)
    scn.load(["ir_105", "ir_123", "ir_133", "ir_38", "ir_87", "ir_97", "nir_22", "nir_13", "nir_16", "vis_04", "vis_05",
              "vis_06", "vis_08", "vis_09", "wv_63", "wv_73"])
    km_1_0_xr = scn.to_xarray(
        ["vis_04", "vis_05", "vis_06", "vis_08", "vis_09", "nir_22", "nir_13", "nir_16", ]).load().chunk(
        {"x": 5568, "y": 5568})
    km_2_0_xr = scn.to_xarray(["ir_105", "ir_123", "ir_133", "ir_38", "ir_87", "ir_97", "wv_63", "wv_73"]).load().chunk(
        {"x": 5568, "y": 5568})
    km_2_0_xr = km_2_0_xr.drop_vars(["mtg_fci_fdss_2km"])
    km_1_0_xr = km_1_0_xr.drop_vars(["mtg_fci_fdss_1km"])
    km_2_0_xr = km_2_0_xr.expand_dims("time")
    km_1_0_xr = km_1_0_xr.expand_dims("time")
    # Get the timestamp halfway between the start and end time
    start_time = pd.Timestamp(km_2_0_xr["ir_105"].attrs["start_time"])
    end_time = pd.Timestamp(km_2_0_xr["ir_105"].attrs["end_time"])
    km_2_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    km_1_0_xr["time"] = [start_time + (end_time - start_time) / 2]
    km_2_0_xr["ir_105"] = km_2_0_xr["ir_105"].astype(np.float16)
    km_2_0_xr["ir_123"] = km_2_0_xr["ir_123"].astype(np.float16)
    km_2_0_xr["ir_133"] = km_2_0_xr["ir_133"].astype(np.float16)
    km_2_0_xr["ir_38"] = km_2_0_xr["ir_38"].astype(np.float16)
    km_2_0_xr["ir_87"] = km_2_0_xr["ir_87"].astype(np.float16)
    km_2_0_xr["ir_97"] = km_2_0_xr["ir_97"].astype(np.float16)
    km_2_0_xr["wv_63"] = km_2_0_xr["wv_63"].astype(np.float16)
    km_1_0_xr["vis_05"] = km_1_0_xr["vis_05"].astype(np.float16)
    km_1_0_xr["vis_06"] = km_1_0_xr["vis_06"].astype(np.float16)
    km_1_0_xr["vis_08"] = km_1_0_xr["vis_08"].astype(np.float16)
    km_1_0_xr["vis_09"] = km_1_0_xr["vis_09"].astype(np.float16)
    km_1_0_xr["nir_22"] = km_1_0_xr["nir_22"].astype(np.float16)
    km_1_0_xr["nir_13"] = km_1_0_xr["nir_13"].astype(np.float16)
    km_1_0_xr["nir_16"] = km_1_0_xr["nir_16"].astype(np.float16)
    # Drop longitude and latitude
    #km_1_0_xr = km_1_0_xr.drop_vars(["longitude", "latitude"])
    #km_2_0_xr = km_2_0_xr.drop_vars(["longitude", "latitude"])
    return km_1_0_xr, km_2_0_xr


def convert_to_datatree(high_res_500m: xr.Dataset, high_res_1km: xr.Dataset, low_res_1km: xr.Dataset, low_res_2km: xr.Dataset, time_ds: xr.Dataset = None) -> xr.DataTree:
    # Make DataArray of just the time coordinate for the root
    if time_ds is None:
        print(high_res_1km["ir_105"].attrs["time_parameters"])
        # Convert the string representation of a dictionary to a dictionary
        time_parameter_dict = eval(high_res_1km["ir_105"].attrs["time_parameters"])
        time_ds = xr.Dataset({"timestamps": high_res_1km["time"]}, coords={"time": high_res_1km["time"].values})
        # Add observation_start_time as a data variable
        time_ds["observation_start_time"] = xr.DataArray([pd.Timestamp(time_parameter_dict["observation_start_time"])], dims=["time"])
        time_ds["observation_end_time"] = xr.DataArray([pd.Timestamp(time_parameter_dict["observation_end_time"])], dims=["time"])
    # Also get the actual start of the scanning, and end of scanning, and include here too
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
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (1, 5568,5568)}
        for v in high_res_1km.data_vars if "mtg_fci" not in v}}
    encoding.update({f"/hr_05km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (1, 5568,5568)}
        for v in high_res_500m.data_vars if "mtg_fci" not in v}})
    encoding.update({f"/1km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (1, 5568,5568)}
        for v in low_res_1km.data_vars if "mtg_fci" not in v}})
    encoding.update({f"/2km": {
        v: {"compressors": zarr.codecs.BloscCodec(cname='zstd', clevel=5, shuffle=zarr.codecs.BloscShuffle.bitshuffle), "shards": (1, 5568,5568)}
        for v in low_res_2km.data_vars if "mtg_fci" not in v}})
    full_dataset.to_zarr(f"mtg_datatree_v3_{timestamp}.zarr", mode="w", compute=True, encoding=encoding, zarr_format=3)
    return full_dataset


def open_datatree_v3_mtg(root_zarr: str) -> xr.DataTree:
    time_ds = xr.open_zarr(root_zarr)
    high_res_1km = xr.open_zarr(root_zarr, group="hr_1km")
    high_res_500m = xr.open_zarr(root_zarr, group="hr_05km")
    low_res_1km = xr.open_zarr(root_zarr, group="1km")
    low_res_2km = xr.open_zarr(root_zarr, group="2km")
    full_dataset = xr.DataTree.from_dict(
        {"/": time_ds, "/hr_1km": high_res_1km, "/hr_05km": high_res_500m, "/1km": low_res_1km, "/2km": low_res_2km})
    return full_dataset


low_res_files = sorted(list(glob.glob("/run/media/jacob/Elements/*FDHSI*.zip")))
high_res_files = sorted(list(glob.glob("/run/media/jacob/Elements/*HRFI*.zip")))
process_normal_and_high_res(low_res_files, high_res_files)
