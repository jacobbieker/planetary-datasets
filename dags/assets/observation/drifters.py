"Download observations from GTS for ships and drifters from NOAA OSMC RealTime dataset."
import xarray as xr
import os
import requests
import pandas as pd

date_range = pd.date_range("2000-01-01", "2025-06-30", freq="3ME")
for date in date_range[::-1]:
    start_month = date.strftime("%Y-%m-01")
    end_month = (date + pd.DateOffset(months=3)).strftime("%Y-%m-%d")
    time_part_of_url = f"%22&time%3E={start_month}T00%3A00%3A00Z&time%3C={end_month}T23%3A59%3A59Z"
    ships = "&platform_type=%22SHIPS%20(GENERIC)"
    drifters = "&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)"
    moored = "&platform_type=%22MOORED%20BUOYS%20(GENERIC)"
    base_url = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur"
    ship_url = base_url+ships + time_part_of_url
    drifter_url = base_url+drifters+time_part_of_url
    moored_url = base_url+moored+time_part_of_url

    if os.path.exists(f"Ships/{date.strftime('%Y-%m')}_ships.nc"):
        try:
            xr.open_dataset(f"Ships/{date.strftime('%Y-%m')}_ships.nc").load() # If this succeeds, it was successful
        except:
            # Request and save the ship data
            print(f"Requesting Ships data for {start_month} - {end_month}")
            ship_response = requests.get(ship_url)
            print(f"Got Ship data for {start_month} - {end_month}")
            # Write binary file to disk
            with open(f"Ships/{date.strftime('%Y-%m')}_ships.nc", "wb") as f:
                f.write(ship_response.content)
    else:
        # Request and save the ship data
        print(f"Requesting Ships data for {start_month} - {end_month}")
        ship_response = requests.get(ship_url)
        print(f"Got Ship data for {start_month} - {end_month}")
        # Write binary file to disk
        with open(f"Ships/{date.strftime('%Y-%m')}_ships.nc", "wb") as f:
            f.write(ship_response.content)

    if os.path.exists(f"Drifters/{date.strftime('%Y-%m')}_drifters.nc"):
        try:
            xr.open_dataset(f"Drifters/{date.strftime('%Y-%m')}_drifters.nc").load()  # If this succeeds, it was successful
        except:
            # Do for drifters
            print(f"Requesting Drifter data for {start_month} - {end_month}")
            drifter_response = requests.get(drifter_url)
            print(f"Got Drifter data for {start_month} - {end_month}")
            with open(f"Drifters/{date.strftime('%Y-%m')}_drifters.nc", "wb") as f:
                f.write(drifter_response.content)
    else:
        # Do for drifters
        print(f"Requesting Drifter data for {start_month} - {end_month}")
        drifter_response = requests.get(drifter_url)
        print(f"Got Drifter data for {start_month} - {end_month}")
        with open(f"Drifters/{date.strftime('%Y-%m')}_drifters.nc", "wb") as f:
            f.write(drifter_response.content)

    if os.path.exists(f"Moored/{date.strftime('%Y-%m')}_moored.nc"):
        try:
            xr.open_dataset(f"Moored/{date.strftime('%Y-%m')}_moored.nc").load()  # If this succeeds, it was successful
        except:
            # Do for drifters
            print(f"Requesting Moored data for {start_month} - {end_month}")
            drifter_response = requests.get(moored_url)
            print(f"Got Moored data for {start_month} - {end_month}")
            with open(f"Moored/{date.strftime('%Y-%m')}_moored.nc", "wb") as f:
                f.write(drifter_response.content)
    else:
        # Do for drifters
        print(f"Requesting Moored data for {start_month} - {end_month}")
        drifter_response = requests.get(moored_url)
        print(f"Got Moored data for {start_month} - {end_month}")
        with open(f"Moored/{date.strftime('%Y-%m')}_moored.nc", "wb") as f:
            f.write(drifter_response.content)



# Drifters (Generic)
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)%22&time%3E=2025-01-01T00%3A00%3A00Z"

# Ships (Generic)
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur%22&time%3E=2025-01-01T00%3A00%3A00Z"

# Ships (Generic) All of 2024
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur"

# Drifter (Generic) All of 2024
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)%22&time%3E=2024-01-01T00%3A00%3A00Z&time%3C=2024-12-31T23%3A59%3A59Z"