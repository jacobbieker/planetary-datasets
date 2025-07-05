"Download observations from GTS for ships and drifters from NOAA OSMC RealTime dataset."
import xarray as xr
import os
import requests
import pandas as pd

date_range = pd.date_range("2011-01-01", "2025-06-30", freq="3ME")
for date in date_range[::-1]:
    start_month = date.strftime("%Y-%m-01")
    end_month = (date + pd.DateOffset(months=2)).strftime("%Y-%m-%d")
    time_part_of_url = f"%22&time%3E={start_month}T00%3A00%3A00Z&time%3C={end_month}T23%3A59%3A59Z"
    ships = "&platform_type=%22SHIPS%20(GENERIC)"
    drifters = "&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)"
    moored = "&platform_type=%22MOORED%20BUOYS%20(GENERIC)"
    shore = "&platform_type=%22SHORE%20AND%20BOTTOM%20STATIONS%20(GENERIC)"
    tide = "&platform_type=%22TIDE%20GAUGE%20STATIONS%20(GENERIC)"
    glider = "&platform_type=%22PROFILING%20FLOATS%20AND%20GLIDERS%20(GENERIC)"
    weather = "&platform_type=%22WEATHER%20BUOYS"
    station = "&platform_type=%22C-MAN%20WEATHER%20STATIONS"
    base_url = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur"
    complementary_url = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_id%2Cplatform_code%2Cplatform_type%2Ctime%2Clatitude%2Clongitude%2Cobservation_depth%2Csss%2Cztmp%2Czsal%2Cwindspd%2Cwinddir%2Cwvht%2Cwaterlevel%2Cclouds%2Csea_water_elec_conductivity%2Csea_water_pressure%2Crlds%2Crsds%2Cwaterlevel_met_res%2Cwaterlevel_wrt_lcd%2Cwater_col_ht%2Cwind_to_direction"
    no_id_complementary_url = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Cobservation_depth%2Csss%2Cztmp%2Czsal%2Cwindspd%2Cwinddir%2Cwvht%2Cwaterlevel%2Cclouds%2Csea_water_elec_conductivity%2Csea_water_pressure%2Crlds%2Crsds%2Cwaterlevel_met_res%2Cwaterlevel_wrt_lcd%2Cwater_col_ht%2Cwind_to_direction"
    extended_url = "https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_id%2Cplatform_code%2Cplatform_type%2Ccountry%2Ctime%2Clatitude%2Clongitude%2Cobservation_depth%2Csst%2Catmp%2Cprecip%2Csss%2Cztmp%2Czsal%2Cslp%2Cwindspd%2Cwinddir%2Cwvht%2Cwaterlevel%2Cclouds%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur%2Csea_water_elec_conductivity%2Csea_water_pressure%2Crlds%2Crsds%2Cwaterlevel_met_res%2Cwaterlevel_wrt_lcd%2Cwater_col_ht%2Cwind_to_direction%2Clon360"
    ship_url = extended_url+ships + time_part_of_url
    drifter_url = extended_url+drifters+time_part_of_url
    moored_url = extended_url+moored+time_part_of_url
    shore_url = extended_url+shore+time_part_of_url
    tide_url = extended_url+tide+time_part_of_url
    glider_url = extended_url+glider+time_part_of_url
    weather_url = extended_url+weather+time_part_of_url
    station_url = extended_url+station+time_part_of_url

    pre_thing = "Extended"
    for folder in ["Glider", "Ships", "Drifters", "Moored", "Shore", "Tide", "Weather", "Station"]:
        if not os.path.exists(f"{pre_thing}{folder}"):
            os.makedirs(f"{pre_thing}{folder}")

    try:
        if os.path.exists(f"{pre_thing}Station/{date.strftime('%Y-%m')}_station.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Station/{date.strftime('%Y-%m')}_station.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Station data for {start_month} - {end_month}")
                drifter_response = requests.get(station_url)
                print(f"Got Station data for {start_month} - {end_month}")
                with open(f"{pre_thing}Station/{date.strftime('%Y-%m')}_station.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Station data for {start_month} - {end_month}")
            drifter_response = requests.get(station_url)
            print(f"Got Station data for {start_month} - {end_month}")
            with open(f"{pre_thing}Station/{date.strftime('%Y-%m')}_station.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Weather/{date.strftime('%Y-%m')}_weather.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Weather/{date.strftime('%Y-%m')}_weather.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Weather data for {start_month} - {end_month}")
                drifter_response = requests.get(weather_url)
                print(f"Got Weather data for {start_month} - {end_month}")
                with open(f"{pre_thing}Weather/{date.strftime('%Y-%m')}_weather.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Weather data for {start_month} - {end_month}")
            drifter_response = requests.get(weather_url)
            print(f"Got Weather data for {start_month} - {end_month}")
            with open(f"{pre_thing}Weather/{date.strftime('%Y-%m')}_weather.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Glider/{date.strftime('%Y-%m')}_glider.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Glider/{date.strftime('%Y-%m')}_glider.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Glider data for {start_month} - {end_month}")
                drifter_response = requests.get(glider_url)
                print(f"Got Glider data for {start_month} - {end_month}")
                with open(f"{pre_thing}Glider/{date.strftime('%Y-%m')}_glider.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Glider data for {start_month} - {end_month}")
            drifter_response = requests.get(glider_url)
            print(f"Got Glider data for {start_month} - {end_month}")
            with open(f"{pre_thing}Glider/{date.strftime('%Y-%m')}_glider.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Ships/{date.strftime('%Y-%m')}_ships.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Ships/{date.strftime('%Y-%m')}_ships.nc").load() # If this succeeds, it was successful
            except:
                # Request and save the ship data
                print(f"Requesting Ships data for {start_month} - {end_month}")
                ship_response = requests.get(ship_url)
                print(f"Got Ship data for {start_month} - {end_month}")
                # Write binary file to disk
                with open(f"{pre_thing}Ships/{date.strftime('%Y-%m')}_ships.nc", "wb") as f:
                    f.write(ship_response.content)
        else:
            # Request and save the ship data
            print(f"Requesting Ships data for {start_month} - {end_month}")
            ship_response = requests.get(ship_url)
            print(f"Got Ship data for {start_month} - {end_month}")
            # Write binary file to disk
            with open(f"{pre_thing}Ships/{date.strftime('%Y-%m')}_ships.nc", "wb") as f:
                f.write(ship_response.content)

        if os.path.exists(f"{pre_thing}Drifters/{date.strftime('%Y-%m')}_drifters.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Drifters/{date.strftime('%Y-%m')}_drifters.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Drifter data for {start_month} - {end_month}")
                drifter_response = requests.get(drifter_url)
                print(f"Got Drifter data for {start_month} - {end_month}")
                with open(f"{pre_thing}Drifters/{date.strftime('%Y-%m')}_drifters.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Drifter data for {start_month} - {end_month}")
            drifter_response = requests.get(drifter_url)
            print(f"Got Drifter data for {start_month} - {end_month}")
            with open(f"{pre_thing}Drifters/{date.strftime('%Y-%m')}_drifters.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Moored/{date.strftime('%Y-%m')}_moored.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Moored/{date.strftime('%Y-%m')}_moored.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Moored data for {start_month} - {end_month}")
                drifter_response = requests.get(moored_url)
                print(f"Got Moored data for {start_month} - {end_month}")
                with open(f"{pre_thing}Moored/{date.strftime('%Y-%m')}_moored.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Moored data for {start_month} - {end_month}")
            drifter_response = requests.get(moored_url)
            print(f"Got Moored data for {start_month} - {end_month}")
            with open(f"{pre_thing}Moored/{date.strftime('%Y-%m')}_moored.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Shore/{date.strftime('%Y-%m')}_shore.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Shore/{date.strftime('%Y-%m')}_shore.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Shore data for {start_month} - {end_month}")
                drifter_response = requests.get(shore_url)
                print(f"Got Shore data for {start_month} - {end_month}")
                with open(f"{pre_thing}Shore/{date.strftime('%Y-%m')}_shore.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Shore data for {start_month} - {end_month}")
            drifter_response = requests.get(shore_url)
            print(f"Got Shore data for {start_month} - {end_month}")
            with open(f"{pre_thing}Shore/{date.strftime('%Y-%m')}_shore.nc", "wb") as f:
                f.write(drifter_response.content)

        if os.path.exists(f"{pre_thing}Tide/{date.strftime('%Y-%m')}_tide.nc"):
            try:
                xr.open_dataset(f"{pre_thing}Tide/{date.strftime('%Y-%m')}_tide.nc").load()  # If this succeeds, it was successful
            except:
                # Do for drifters
                print(f"Requesting Tide data for {start_month} - {end_month}")
                drifter_response = requests.get(tide_url)
                print(f"Got Tide data for {start_month} - {end_month}")
                with open(f"{pre_thing}Tide/{date.strftime('%Y-%m')}_tide.nc", "wb") as f:
                    f.write(drifter_response.content)
        else:
            # Do for drifters
            print(f"Requesting Tide data for {start_month} - {end_month}")
            drifter_response = requests.get(tide_url)
            print(f"Got Tide data for {start_month} - {end_month}")
            with open(f"{pre_thing}Tide/{date.strftime('%Y-%m')}_tide.nc", "wb") as f:
                f.write(drifter_response.content)
    except:
        continue



# Drifters (Generic)
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)%22&time%3E=2025-01-01T00%3A00%3A00Z"

# Ships (Generic)
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur%22&time%3E=2025-01-01T00%3A00%3A00Z"

# Ships (Generic) All of 2024
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur"

# Drifter (Generic) All of 2024
"https://erddap.aoml.noaa.gov/gdp/erddap/tabledap/OSMC_RealTime.nc?platform_type%2Ctime%2Clatitude%2Clongitude%2Csst%2Catmp%2Cprecip%2Csss%2Cslp%2Cwindspd%2Cwinddir%2Cdewpoint%2Cuo%2Cvo%2Cwo%2Crainfall_rate%2Chur&platform_type=%22DRIFTING%20BUOYS%20(GENERIC)%22&time%3E=2024-01-01T00%3A00%3A00Z&time%3C=2024-12-31T23%3A59%3A59Z"