import icechunk
import xarray as xr
import numpy as np

dses = []
for year in range(2012, 2026):
    storage = icechunk.local_filesystem_storage(f"/data/AOML/aoml_{year}.icechunk")
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")
    ds = xr.open_zarr(session.store, consolidated=False)
    dses.append(ds)
ds = xr.concat(dses, dim="time")
print(ds)
print(ds.data_vars)
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

for obs_type in [("bouys", bouys), ("ships", ships), ("stations", stations), ("obs", obs), ("gliders", gliders)]:
    # Select all bouys
    bouy_ds = ds.where(ds.platform_type.isin(obs_type[1]), drop=True)
    print(bouy_ds)
    # Move platform type, platform id, latitude, longitude to data variables
    bouy_ds = bouy_ds.reset_index(["platform_type", "platform_id", "latitude", "longitude"])
    print(bouy_ds)
    # Drop all data variables where all values are NaN
    bouy_ds = bouy_ds.dropna(dim='time', how='all')
    # Drop data vars if all NaN
    for var in bouy_ds.data_vars:
        if bouy_ds[var].isnull().all():
            bouy_ds = bouy_ds.drop_vars(var)
    bouy_ds = bouy_ds.sortby("time").chunk({"time": 100000})
    # Save to icechunk
    storage = icechunk.local_filesystem_storage(f"/data/AOML/aoml_{obs_type[0]}.icechunk")
    repo = icechunk.Repository.open_or_create(storage)
    session = repo.writable_session("main")
    try:
        bouy_ds.vz.to_icechunk(session.store)
        snapshot_id = session.commit(f"Wrote {obs_type[0]} data for {year}")
        print(snapshot_id)
    except Exception as e:
        print(f"Failed to write {obs_type[0]} data: {e}")
        continue