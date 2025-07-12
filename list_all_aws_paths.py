import os.path

import numpy as np
import s3fs
import fsspec
import xarray as xr
import matplotlib.pyplot as plt
import glob
import pandas as pd
from virtualizarr import open_virtual_dataset
import icechunk
from icechunk import Session
from virtualizarr.codecs import get_codecs


def open_h9(filepath: str) -> xr.Dataset:
    ds = open_virtual_dataset(filepath, loadable_variables=["x", "y"])
    #ds = xr.open_dataset(filepath)
    ds["time"] = pd.to_datetime(ds.attrs["start_date_time"], format="%Y%j%H%M%S")
    ds = ds.set_coords("time")
    ds["band"] = int(ds.attrs["channel_id"])
    ds = ds.set_coords("band")
    ds["central_wavelength"] = [float(ds.attrs["central_wavelength"])]
    ds = ds.set_coords("central_wavelength")
    ds = ds.set_coords("fixedgrid_projection")
    # Also add all attributes as a data var
    attrs_to_keep = {}
    for key, value in ds.attrs.items():
        if "satellite" in key and "satellite_id" not in key:
            attrs_to_keep[key] = value
    ds.attrs = attrs_to_keep
    return ds

def hierarchical_concat_h9_005(vdses: list[xr.Dataset]) -> list[xr.Dataset, xr.Dataset, xr.Dataset]:
    combined_vds = xr.combine_by_coords(vdses[14:74], coords='minimal', compat='override')
    combined_vds_top = xr.combine_by_coords(vdses[6:14], coords='minimal', compat='override')
    combined_vds_bottom = xr.combine_by_coords(vdses[74:82], coords='minimal', compat='override')
    combined_vds_bottom2 = xr.combine_by_coords(vdses[82:], coords='minimal', compat='override')
    combined_vds_top2 = xr.combine_by_coords(vdses[0:6], coords='minimal', compat='override')
    combined_middle = xr.concat([combined_vds_top, combined_vds_bottom], dim='y', coords='minimal', compat='override',
                                combine_attrs='override')
    combined_edges = xr.concat([combined_vds_top2, combined_vds_bottom2], dim='y', coords='minimal',
                                 compat='override', combine_attrs='override')
    # Return from moiddle out
    return combined_vds, combined_middle, combined_edges


def hierarchical_concat_h9_010(vdses: list[xr.Dataset]) -> list[xr.Dataset, xr.Dataset, xr.Dataset]:
    combined_vds = xr.combine_by_coords(vdses[14:74], coords='minimal', compat='override')
    combined_vds_top = xr.combine_by_coords(vdses[6:14], coords='minimal', compat='override')
    combined_vds_bottom = xr.combine_by_coords(vdses[74:82], coords='minimal', compat='override')
    #print(vdses[82:])
    # TODO How to deal with the missing 2 tiles?
    combined_vds_bottom2 = xr.combine_by_coords(vdses[83:], coords='minimal', compat='override')
    combined_vds_top2 = xr.combine_by_coords(vdses[1:6], coords='minimal', compat='override')
    combined_middle = xr.concat([combined_vds_top, combined_vds_bottom], dim='y', coords='minimal', compat='override',
                                combine_attrs='override')
    combined_edges = xr.concat([combined_vds_top2, combined_vds_bottom2], dim='y', coords='minimal',
                                 compat='override', combine_attrs='override')
    return combined_vds, combined_middle, combined_edges

combined_centers = []
combined_middles = []
combined_outers = []
combined_brightness = []
combined_reflectance = []
# 1,2,4 can be combined
# 3 is own resolution
# 5, 6 can be combined
for band in [5,6,7,8,9,10,11,12,13,14,15,16]: #[1,2,4]:
    vdses = []
    for t in range(1, 89):
        midnight_files = sorted(glob.glob(f"/Users/jacob/Development/planetary-datasets/JAXA/0000/OR_HFD-020-B*-M1C{str(band).zfill(2)}-T0{str(t).zfill(2)}_GH9_*.nc"))
        next_midnight_files = sorted(glob.glob(f"/Users/jacob/Development/planetary-datasets/JAXA/2350/OR_HFD-020-B*-M1C{str(band).zfill(2)}-T0{str(t).zfill(2)}_GH9_*.nc"))
        if len(midnight_files) == 0:
            continue
        ds = open_h9(midnight_files[0])
        ds2 = open_h9(next_midnight_files[0])
        ds = xr.concat([ds, ds2], dim="time", coords='minimal',
                compat='override',
                combine_attrs='override')
        vdses.append(ds)
    combined_vds, combined_middle, combined_edges = hierarchical_concat_h9_005(vdses)
    # Print the codec for the center combined
    combined_centers.append(combined_vds)
    combined_middles.append(combined_middle)
    combined_outers.append(combined_edges)

# Now combine along band dimension
# Get all codecs
"""
codecs = []
for dset in combined_centers:
    darray: xr.DataArray = dset["Sectorized_CMI"]
    arr = darray.data
    codecs.append(get_codecs(arr))
# Get unique codecs
print(codecs)
unique_codecs = []
for i, codec in enumerate(codecs):
    if codec not in unique_codecs:
        unique_codecs.append(codec)
codecs = unique_codecs
print(codecs)
"""
for band in [5]: #for codec in codecs:
    indicies = []
    for i, vd in enumerate(combined_centers):
        if vd["band"] == band:
            indicies.append(i)
        #band_codec = get_codecs(vd["Sectorized_CMI"].data)
        #if band_codec == codec:
        #    indicies.append(i)
    # TODO Add complete NaN Manifest Arrays to make the full disk a square
    # Combine all same codecs together for all 3 sets
    sub_centers = [combined_centers[i] for i in range(len(combined_centers)) if i in indicies]
    sub_middles = [combined_middles[i] for i in range(len(combined_middles)) if i in indicies]
    sub_outers = [combined_outers[i] for i in range(len(combined_outers)) if i in indicies]
    sub_centers_combo = xr.concat(sub_centers, dim='band', coords='minimal', compat='override', combine_attrs='override')
    sub_middles_combo = xr.concat(sub_middles, dim='band', coords='minimal', compat='override', combine_attrs='override')
    sub_outers_combo = xr.concat(sub_outers, dim='band', coords='minimal', compat='override', combine_attrs='override')
    # Print the encoding of x and y variables
    print(sub_centers_combo.x.encoding)
    print(sub_centers_combo.y.encoding)
    # Update to include the scale factor and add offset to the x and y variables
    # Remove offset and scale factor from encoding
    print(sub_centers_combo.x.values)
    for dset in [sub_centers_combo, sub_middles_combo, sub_outers_combo]:
        #dset["x"] = dset.x.values * dset.x.encoding["scale_factor"] + dset.x.encoding["add_offset"]
        #dset["y"] = dset.y.values * dset.y.encoding["scale_factor"] + dset.y.encoding["add_offset"]
        dset.x.encoding = {}
        dset.y.encoding = {}
    print(sub_centers_combo.x.values)
    print(sub_centers_combo.y.values)
    storage = icechunk.local_filesystem_storage("combined_centers3")

    # Add the scale and offset from the encoding to the x and y coords, and then remove the encoding?
    # By default, local virtual references and public remote virtual references can be read wihtout extra configuration.
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    #from icechunk.xarray import to_icechunk
    #to_icechunk(sub_centers_combo, session)
    # write the virtual dataset to the session with the IcechunkStore
    sub_centers_combo.virtualize.to_icechunk(session.store)
    session.commit("Wrote first dataset")


    # Now open them up
    for p in ["combined_centers3",]:
        session = repo.readonly_session("main")
        ds = xr.open_zarr(
            session.store,
            zarr_version=3,
            consolidated=False,
            chunks={},
        )
        print(ds)
        print(ds.x.values)
        print(ds.y.values)
        import matplotlib.pyplot as plt
        ds["Sectorized_CMI"].isel(time=0, band=0).plot()
        plt.show()
    print(sub_centers_combo)
    #print(sub_middles_combo)
    #print(sub_outers_combo)

exit()


fs = s3fs.S3FileSystem(anon=True)

def write_timestamp(*, itime: int, session: Session) -> Session:
    # pass a list to isel to preserve the time dimension
    ds = xr.tutorial.open_dataset("rasm").isel(time=[itime])
    # region="auto" tells Xarray to infer which "region" of the output arrays to write to.
    ds.to_zarr(session.store, region="auto", consolidated=False)
    return session

from concurrent.futures import ProcessPoolExecutor
from icechunk.distributed import merge_sessions

if __name__ == "__main__":
    session = repo.writable_session("main")
    with ProcessPoolExecutor() as executor:
        # opt-in to successful pickling of a writable session
        with session.allow_pickling():
            # submit the writes
            futures = [
                executor.submit(write_timestamp, itime=i, session=session)
                for i in range(ds.sizes["time"])
            ]
            # grab the Session objects from each individual write task
            sessions = [f.result() for f in futures]

    # manually merge the remote sessions in to the local session
    session = merge_sessions(session, *sessions)
    print(session.commit("finished writes"))

# Create repos for each band of GOES-19
repos = []
for band in range(1, 17):
    if os.path.exists(f'goes19_band{str(band).zfill(2)}'):
        repos.append(icechunk.Repository.open(icechunk.local_filesystem_storage(f'goes19_band{str(band).zfill(2)}')))
    else:
        storage = icechunk.local_filesystem_storage(
            path=f'goes19_band{str(band).zfill(2)}',
        )
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-east-1")))
        credentials = icechunk.containers_credentials(s3=icechunk.s3_credentials(anonymous=True))
        repo = icechunk.Repository.create(storage, config, credentials)
        repos.append(repo)
# List all files for Feb 2025 for band 4
# Do all of 2025
for i in range(1, 366):
    vds = []
    vds_05 = []
    vds_1 = []
    vds_2 = []
    for band in range(1, 17):
        files = sorted(fs.glob(f"noaa-goes19/ABI-L1b-RadF/2025/{str(i).zfill(3)}/*/OR_ABI-L1b-RadF-M6C{str(band).zfill(2)}_G19_*.nc"))
        print(len(files))
        files = ["s3://" + f for f in files]
        virtual_datasets = [
                open_virtual_dataset(filepath, loadable_variables=["t", "y", "x", "band", "band_id"], reader_options={'storage_options': {"anon": True}}) for filepath in files
            ]

        vd = xr.concat(
            virtual_datasets,
            dim='t',
            coords='minimal',
            compat='override',
            combine_attrs='override'
        )
    # vd = xr.concat(vds_1, dim='band', compat="override", coords="minimal", combine_attrs='override')
    #vd = xr.concat(virtual_datasets, dim='t', compat="override", coords="all")
    # Load from previous store, if it exists, so i > 1
    if i > 1:
        session = repos[band-1].writable_session("main")
        # write the virtual dataset to the session with the IcechunkStore
        vd.virtualize.to_icechunk(session.store, append_dim="t")
    else:
        session = repos[band-1].writable_session("main")
        vd.virtualize.to_icechunk(session.store)
    session.commit(f"Add GOES-19 Band {band} for Day {str(i).zfill(3)}")

print(vd)
# Write to disk
vd.virtualize.to_kerchunk('goes19.json', format='json')
exit()
#gmgsi = xr.open_zarr("s3://bkr/gmgi/gmgsi_v3.zarr", storage_options={"anon": True, "client_kwargs": {"endpoint_url": "https://data.source.coop"}})
#print(gmgsi)
# Write the last 24*7 timesteps to a new local zarr
#gmgsi.isel(time=slice(-24*7, None)).to_zarr("gmgsi_last_week.zarr", mode="w", zarr_format=3)

#exit()
types = ["ir087", "ir096", "ir105", "ir112", "ir123", "ir133", "nr013", "nr016", "sw038", "vi004", "vi005", "vi006", "vi008", "wv063", "wv069", "wv073"]


#vd = open_virtual_dataset("/Users/jacob/Development/planetary-datasets/GOES/00/OR_ABI-L1b-RadF-M6C01_G19_s20250010000205_e20250010009513_c20250010009543.nc")
#print(vd)

#virtual_datasets = [
#    open_virtual_dataset(filepath, loadable_variables=['x', 'y']) #xr.open_dataset(filepath)
#    for filepath in glob.glob(f"/Users/jacob/Development/planetary-datasets/JAXA/*/OR_HFD-005-B11-M1C03-T001_GH9_*.nc")
#]
vds_05 = []
vds_1 = []
vds_2 = []
# ["t", "y", "x", "number_of_time_bounds", "number_of_image_bounds", "number_of_harmonization_coefficients", "num_star_looks", "band", "band_wavelength", "band_id", "t_star_look", "band_wavelength_star_look"]
for band in range(1, 17):
    virtual_datasets = [
        open_virtual_dataset(filepath, loadable_variables=["t", "y", "x", "band", "band_id"]) for filepath in sorted(list(glob.glob(f"/Users/jacob/Development/planetary-datasets/GOES/*/OR_ABI-L1b-RadF-M6C{band:02d}_G19_*.nc")))
    ]
    vd = xr.concat(virtual_datasets, dim='t', compat="override", coords="all")
    if band == 2:
        vds_05.append(vd)
    elif band in [1, 3, 5]:
        vds_1.append(vd)
    else:
        vds_2.append(vd)
#virtual_datasets = [
#    open_virtual_dataset(filepath, loadable_variables=["t", "y", "x", "number_of_time_bounds", "number_of_image_bounds", "number_of_harmonization_coefficients", "num_star_looks", "band", "band_wavelength", "band_id", "t_star_look", "band_wavelength_star_look"]) for filepath in sorted(list(glob.glob(f"/Users/jacob/Development/planetary-datasets/GOES/*/OR_ABI-L1b-RadF-M6C01_G19_*.nc")))
#]
vd_05 = vds_05[0]
print(vd_05)
print(vds_2)
vd_2 = xr.concat(vds_2, dim='band_id', compat="override", coords="all")
print(vd_2)
print(vds_1)
vd_1 = xr.concat(vds_1, dim='band_id', compat="override", coords="all")
print(vd_1)

exit()
#print(virtual_datasets)
print(len(virtual_datasets))
print(virtual_datasets[0].data_vars)
print(virtual_datasets[0].attrs)
# For each of the virtual datasets, set the "time" coordinate to the middle of the time_coverage_start and time_coverage_end attrs
vds = []
#for vd in virtual_datasets:
#    end = pd.Timestamp(vd.attrs["time_coverage_end"])
#    start = pd.Timestamp(vd.attrs["time_coverage_start"])
#    print(end - start)
#    vd["time"] = pd.Timestamp(vd.attrs["time_coverage_start"]) + pd.Timedelta(pd.Timestamp(vd.attrs["time_coverage_end"]) - pd.Timestamp(vd.attrs["time_coverage_start"]))
#    vd = vd.set_coords('time')
    #print(vd)
#    vds.append(vd)
#vd = xr.concat(vds, dim='time')
#print(vd)
#exit()
"""
TODO: 
1. Generate virtualizarr datasets for each of the tiles in Himawari-8 and -9, combining all the bands together per tile.
2. End user will have to then load and combine at the end, but that is the tradeoff of not having to reprocess the full dataset

1. G2KA seems to not support virtualizarr because of chunk issues, need to do something there

1. GOES does support it it seems, so then just need to generate and concatenate along time for each band and then all bands together


"""
#print(virtual_datasets)
print(len(virtual_datasets))
vd = xr.concat(virtual_datasets, dim='t', compat="override", coords="all")
#vd = xr.combine_by_coords(virtual_datasets, combine_attrs="drop_conflicts")
#vd = open_virtual_dataset(f"/Users/jacob/Development/planetary-datasets/JAXA/0000/OR_HFD-005-B11-M1C03-T001_GH9_s20250010000000_c20250010009090.nc")
print(vd)
exit()

for t in types:
    virtual_datasets = [
    open_virtual_dataset(filepath, filetype="netCDF4", )
    for filepath in glob.glob(f'/Users/jacob/Development/planetary-datasets/G2KA/*/*{t}*.nc')
]

    # this Dataset wraps a bunch of virtual ManifestArray objects directly
    virtual_ds = xr.combine_nested(virtual_datasets, concat_dim=['time'])
    print(virtual_ds)

exit()
# Get list of all files for each year in the GOES archives, gather the time from the file names

for t in types:
    data = xr.open_dataset(f"/Users/jacob/Development/planetary-datasets/G2KA/00/gk2a_ami_le1b_{t}_fd020ge_202501010000.nc")
    print(data)
    print(data.data_vars)
    data["image_pixel_values"].plot()
    plt.show()