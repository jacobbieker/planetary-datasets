import cdsapi
import xarray as xr
import os

for year in range(2023, 2000, -1):
    for i, month in enumerate(
        [
            [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
            ],
            ["07", "08", "09", "10", "11", "12"],
        ]
    ):
        dataset = "insitu-observations-igra-baseline-network"
        if os.path.exists(f"gnss_{year}_{i}.nc"):
            try:
                xr.open_dataset(f"gnss_{year}_{i}.nc").load()
                continue
            except Exception as e:
                try:
                    dataset = "insitu-observations-gnss"
                    request = {
                        "network_type": "igs_daily",
                        "variable": ["total_column_water_vapour", "zenith_total_delay"],
                        "year": year,
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
                    client.retrieve(dataset, request, target=f"gnss_{year}_{i}.nc")
                except Exception as e:
                    print(e)
                    continue
        else:
            try:
                dataset = "insitu-observations-gnss"
                request = {
                    "network_type": "igs_daily",
                    "variable": ["total_column_water_vapour", "zenith_total_delay"],
                    "year": year,
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
                client.retrieve(dataset, request, target=f"gnss_{year}_{i}.nc")
            except Exception as e:
                print(e)
                continue
