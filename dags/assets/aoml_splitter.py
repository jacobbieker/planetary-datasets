import icechunk
import xarray as xr
import numpy as np
from icechunk.xarray import to_icechunk
import zarr


platform_types = ['C-MAN WEATHER STATIONS', 'DRIFTING BUOYS', 'DRIFTING BUOYS (GENERIC)',
 'GLIDERS', 'GLOSS', 'ICE BUOYS', 'MOORED BUOYS', 'MOORED BUOYS (GENERIC)',
 'PROFILING FLOATS AND GLIDERS', 'PROFILING FLOATS AND GLIDERS (GENERIC)',
 'RESEARCH', 'SHIPS', 'SHIPS (GENERIC)',
 'SHORE AND BOTTOM STATIONS (GENERIC)', 'TAGGED ANIMAL',
 'TIDE GAUGE STATIONS (GENERIC)', 'TROPICAL MOORED BUOYS',
 'TSUNAMI WARNING STATIONS', 'UNCREWED SURFACE VEHICLE', 'UNKNOWN',
 'VOLUNTEER OBSERVING SHIPS', 'VOLUNTEER OBSERVING SHIPS (GENERIC)',
 'VOSCLIM', 'WEATHER AND OCEAN OBS', 'WEATHER BUOYS', 'WEATHER OBS']
bouys = [p_type for p_type in platform_types if "BUOY" in p_type]
ships = [p_type for p_type in platform_types if "SHIP" in p_type or "UNCREWED SURFACE VEHICLE" in p_type or "VOSCLIM" in p_type]
stations = [p_type for p_type in platform_types if "STATION" in p_type]
obs = [p_type for p_type in platform_types if "OBS" in p_type]
gliders = [p_type for p_type in platform_types if "GLIDER" in p_type or "FLOAT" in p_type]


for obs_type in [("stations", stations),("gliders", gliders),]:
    # Select all bouys
    dses = []
    first_write = True
    for year in range(2012, 2026):
        storage = icechunk.local_filesystem_storage(f"/data/AOML/aoml_{obs_type[0]}_{year}.icechunk")
        repo = icechunk.Repository.open_or_create(storage)
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store, consolidated=False)
        times = len(ds.time.values)
        if times % 100000 != 0:
            print(f"Warning: {obs_type[0]} data for {year} has {times} time steps, which is not a multiple of 100000. Truncating to be a multiple.")
            ds = ds.isel(time=slice(0, times - (times % 100000)))
        bouy_ds = ds.sortby("time")
        bouy_ds = bouy_ds.drop_encoding()
        bouy_ds["platform_type"] = bouy_ds["platform_type"].astype("U35")
        bouy_ds["platform_type"] = bouy_ds["platform_type"].astype("U64")
        bouy_ds = bouy_ds.chunk({"time": 100000})
        variables = list(bouy_ds.data_vars)
        encoding = {
            "time": {
                "units": "seconds since 1970-01-01",
                "calendar": "standard",
                "dtype": "int64",
            }
        }
        encoding.update(
            {
                v: {
                    "compressors": zarr.codecs.BloscCodec(
                        cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                    )
                }
                for v in variables
            }
        )
        # Save to icechunk
        storage = icechunk.local_filesystem_storage(f"/data/AOML/aoml_{obs_type[0]}.icechunk")
        repo = icechunk.Repository.open_or_create(storage)
        session = repo.writable_session("main")
        try:
            if first_write:
                to_icechunk(bouy_ds, session, encoding=encoding)
                first_write = False
            else:
                to_icechunk(bouy_ds, session, append_dim="time")
            snapshot_id = session.commit(f"Wrote {obs_type[0]} data")
            print(snapshot_id)
        except Exception as e:
            print(f"Failed to write {obs_type[0]} data: {e}")
            continue

        #dses.append(ds)
        #bouy_ds = ds.where(ds.platform_type.isin(obs_type[1]).compute(), drop=True).compute()
        #print(ds)
        #print(ds.data_vars)
        # Move platform type, platform id, latitude, longitude to data variables
        #bouy_ds = ds.reset_coords(["platform_type", "platform_id", "latitude", "longitude", "platform_code"])
       # print(bouy_ds)
        # Drop all data variables where all values are NaN
        #bouy_ds = bouy_ds.dropna(dim='time', how='all')
        # Drop data vars if all NaN
        #for var in bouy_ds.data_vars:
        #   if bouy_ds[var].isnull().all():
        #       bouy_ds = bouy_ds.drop_vars(var)
        # Concatenate all datasets
        #dses.append(bouy_ds)
    # Concatenate all datasets
    """
    bouy_ds = xr.concat(dses, dim="time")
    bouy_ds = bouy_ds.sortby("time")
    bouy_ds = bouy_ds.drop_encoding()
    bouy_ds["platform_type"] = bouy_ds["platform_type"].astype("U35")
    bouy_ds["platform_type"] = bouy_ds["platform_type"].astype("U64")
    bouy_ds = bouy_ds.chunk({"time": 100000})
    variables = list(bouy_ds.data_vars)
    encoding = {
        "time": {
            "units": "seconds since 1970-01-01",
            "calendar": "standard",
            "dtype": "int64",
        }
    }
    encoding.update(
        {
            v: {
                "compressors": zarr.codecs.BloscCodec(
                    cname="zstd", clevel=9, shuffle=zarr.codecs.BloscShuffle.bitshuffle
                )
            }
            for v in variables
        }
    )
    # Save to icechunk
    storage = icechunk.local_filesystem_storage(f"/data/AOML/aoml_{obs_type[0]}.icechunk")
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")
    try:
        to_icechunk(bouy_ds, session, encoding=encoding)
        snapshot_id = session.commit(f"Wrote {obs_type[0]} data")
        print(snapshot_id)
    except Exception as e:
        print(f"Failed to write {obs_type[0]} data: {e}")
        continue
    """