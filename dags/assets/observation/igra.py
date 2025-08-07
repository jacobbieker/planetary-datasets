import cdsapi
import xarray as xr
import os

for year in range(2020, 1978, -1):
    for i, month in enumerate(
        [
            [
                "01",
                "02",
            ],
            [
                "03",
                "04",
            ],
            [
                "05",
                "06",
            ],
            [
                "07",
                "08",
            ],
            [
                "09",
                "10",
            ],
            ["11", "12"],
        ]
    ):
        dataset = "insitu-observations-igra-baseline-network"
        if os.path.exists(f"igra_{year}_{i}.nc"):
            try:
                xr.open_dataset(f"igra_{year}_{i}.nc").load()
                continue
            except Exception as e:
                try:
                    request = {
                        "archive": "integrated_global_radiosonde_archive_version_2",
                        "variable": [
                            "air_dewpoint_depression",
                            "air_temperature",
                            "geopotential_height",
                            "relative_humidity",
                            "wind_from_direction",
                            "wind_speed",
                        ],
                        "year": [year],
                        "month": month,
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
                    client.retrieve(dataset, request, target=f"igra_{year}_{i}.nc")
                except Exception as e:
                    print(e)
                    continue
        else:
            try:
                request = {
                    "archive": "integrated_global_radiosonde_archive_version_2",
                    "variable": [
                        "air_dewpoint_depression",
                        "air_temperature",
                        "geopotential_height",
                        "relative_humidity",
                        "wind_from_direction",
                        "wind_speed",
                    ],
                    "year": [year],
                    "month": month,
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
                client.retrieve(dataset, request, target=f"igra_{year}_{i}.nc")
            except Exception as e:
                print(e)
                continue
