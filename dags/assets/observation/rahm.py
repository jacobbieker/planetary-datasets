import cdsapi
import xarray as xr
import os

for year in range(2019, 1978, -1):
    for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
        dataset = "insitu-observations-igra-baseline-network"
        if os.path.exists(f"rahm_{year}_{month}.nc"):
            try:
                xr.open_dataset(f"rahm_{year}_{month}.nc").load()
                continue
            except Exception as e:
                try:
                    request = {
                        "archive": "radiosounding_harmonization_dataset_version_1",
                        "variable": [
                            "air_dewpoint_depression",
                            "air_temperature",
                            "ascent_speed",
                            "eastward_wind_speed",
                            "frost_point_temperature",
                            "geopotential_height",
                            "northward_wind_speed",
                            "relative_humidity",
                            "solar_zenith_angle",
                            "water_vapour_volume_mixing_ratio",
                            "wind_from_direction",
                            "wind_speed",
                        ],
                        "year": [year],
                        "month": [month],
                        "day": [
                            "01",
                            "02",
                            "03",
                            "04",
                            "05",
                            "06",
                            "07",
                            "08",
                            "09",
                            "10",
                            "11",
                            "12",
                            "13",
                            "14",
                            "15",
                            "16",
                            "17",
                            "18",
                            "19",
                            "20",
                            "21",
                            "22",
                            "23",
                            "24",
                            "25",
                            "26",
                            "27",
                            "28",
                            "29",
                            "30",
                            "31",
                        ],
                        "data_format": "netcdf",
                    }

                    client = cdsapi.Client()
                    client.retrieve(dataset, request, target=f"rahm_{year}_{month}.nc")
                except Exception as e:
                    print(e)
                    continue
        else:
            try:
                request = {
                    "archive": "radiosounding_harmonization_dataset_version_1",
                    "variable": [
                        "air_dewpoint_depression",
                        "air_temperature",
                        "ascent_speed",
                        "eastward_wind_speed",
                        "frost_point_temperature",
                        "geopotential_height",
                        "northward_wind_speed",
                        "relative_humidity",
                        "solar_zenith_angle",
                        "water_vapour_volume_mixing_ratio",
                        "wind_from_direction",
                        "wind_speed",
                    ],
                    "year": [year],
                    "month": [month],
                    "day": [
                        "01",
                        "02",
                        "03",
                        "04",
                        "05",
                        "06",
                        "07",
                        "08",
                        "09",
                        "10",
                        "11",
                        "12",
                        "13",
                        "14",
                        "15",
                        "16",
                        "17",
                        "18",
                        "19",
                        "20",
                        "21",
                        "22",
                        "23",
                        "24",
                        "25",
                        "26",
                        "27",
                        "28",
                        "29",
                        "30",
                        "31",
                    ],
                    "data_format": "netcdf",
                }

                client = cdsapi.Client()
                client.retrieve(dataset, request, target=f"rahm_{year}_{month}.nc")
            except Exception as e:
                print(e)
                continue
