import cdsapi
import datetime as dt
import os
import pandas as pd
import random
import shutil

c = cdsapi.Client(url="", key="")


def get_marine_observation(day: dt.datetime, raw_location: str, output_location: str, cds_key: str):
    """Get marine obsevations from CDS"""
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=cds_key)
    c.retrieve(
        "insitu-observations-surface-marine",
        {
            "format": "csv-obs.zip",
            "variable": [
                "air_pressure_at_sea_level",
                "air_temperature",
                "dew_point_temperature",
                "water_temperature",
                "wind_from_direction",
                "wind_speed",
            ],
            "data_quality": [
                "failed",
                "passed",
            ],
            "month": f'{day.strftime("%m")}',
            "day": [
                f'{day.strftime("%d")}',
            ],
            "year": f'{day.strftime("%Y")}',
        },
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.zip',
    )


def get_surface_observations(
    day: dt.datetime, raw_location: str, output_location: str, cds_key: str
):
    """Get surface observations from CDS"""
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=cds_key)
    c.retrieve(
        "insitu-observations-surface-land",
        {
            "format": "zip",
            "time_aggregation": "sub_daily",
            "variable": [
                "air_pressure",
                "air_pressure_at_sea_level",
                "air_temperature",
                "dew_point_temperature",
                "wind_from_direction",
                "wind_speed",
            ],
            "usage_restrictions": [
                "restricted",
                "unrestricted",
            ],
            "data_quality": [
                "failed",
                "passed",
            ],
            "month": f'{day.strftime("%m")}',
            "day": [
                f'{day.strftime("%d")}',
            ],
            "year": f'{day.strftime("%Y")}',
        },
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.zip',
    )


def get_gruan_data():
    c.retrieve(
        "insitu-observations-gruan-reference-network",
        {
            "format": "csv-lev.zip",
            "year": "2011",
            "month": "06",
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
            ],
            "variable": [
                "air_temperature",
                "air_temperature_post_processing_radiation_correction",
                "air_temperature_random_uncertainty",
                "air_temperature_systematic_uncertainty",
                "air_temperature_total_uncertainty",
                "altitude",
                "altitude_total_uncertainty",
                "eastward_wind_component",
                "frost_point_temperature",
                "geopotential_height",
                "northward_wind_component",
                "relative_humidity",
                "relative_humidity_effective_vertical_resolution",
                "relative_humidity_post_processing_radiation_correction",
                "relative_humidity_random_uncertainty",
                "relative_humidity_systematic_uncertainty",
                "relative_humidity_total_uncertainty",
                "shortwave_radiation",
                "shortwave_radiation_total_uncertainty",
                "vertical_speed_of_radiosonde",
                "water_vapor_volume_mixing_ratio",
                "wind_from_direction",
                "wind_from_direction_total_uncertainty",
                "wind_speed",
                "wind_speed_total_uncertainty",
            ],
        },
        "download.csv-lev.zip",
    )


def get_woudc_data():
    c.retrieve(
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        {
            "observation_type": "vertical_profile",
            "format": "csv-lev.zip",
            "year": "2000",
            "month": [
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
            ],
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
            "variable": [
                "air_temperature",
                "geopotential_height",
                "ozone_partial_pressure",
                "relative_humidity",
                "wind_from_direction",
                "wind_speed",
            ],
        },
        "download.csv-lev.zip",
    )


def get_gnss_data():
    c.retrieve(
        "insitu-observations-gnss",
        {
            "network_type": "igs",
            "format": "csv-lev.zip",
            "variable": [
                "total_column_water_vapour",
                "total_column_water_vapour_combined_uncertainty",
                "total_column_water_vapour_era5",
                "zenith_total_delay",
                "zenith_total_delay_random_uncertainty",
            ],
            "year": "2000",
            "month": [
                "10",
                "11",
            ],
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
        },
        "download.csv-lev.zip",
    )


def get_igra_data():
    c.retrieve(
        "insitu-observations-igra-baseline-network",
        {
            "archive_type": "global_radiosonde_archive",
            "format": "csv-lev.zip",
            "variable": [
                "air_dewpoint_depression",
                "air_temperature",
                "geopotential_height",
                "relative_humidity",
                "wind_from_direction",
                "wind_speed",
            ],
            "year": "2000",
            "month": "01",
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
        },
        "download.csv-lev.zip",
    )


def get_harmonized_igra_data():
    c.retrieve(
        "insitu-observations-igra-baseline-network",
        {
            "archive_type": "harmonized_global_radiosonde_archive",
            "format": "csv-lev.zip",
            "variable": [
                "air_dewpoint_depression",
                "air_temperature",
                "air_temperature_total_uncertainty",
                "ascent_speed",
                "eastward_wind_component",
                "eastward_wind_component_total_uncertainty",
                "frost_point_temperature",
                "geopotential_height",
                "northward_wind_component",
                "northward_wind_component_total_uncertainty",
                "relative_humidity",
                "relative_humidity_total_uncertainty",
                "solar_zenith_angle",
                "water_vapor_volume_mixing_ratio",
                "wind_from_direction",
                "wind_from_direction_total_uncertainty",
                "wind_speed",
                "wind_speed_total_uncertainty",
            ],
            "year": "2000",
            "month": "01",
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
        },
        "download.csv-lev.zip",
    )

def get_epn_data():
    c.retrieve(
        'insitu-observations-gnss',
        {
            'network_type': 'epn',
            'format': 'csv-lev.zip',
            'variable': [
                'total_column_water_vapour', 'total_column_water_vapour_combined_uncertainty',
                'total_column_water_vapour_era5',
                'zenith_total_delay', 'zenith_total_delay_random_uncertainty',
            ],
            'year': '2000',
            'month': [
                '10', '11',
            ],
            'day': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
                '13', '14', '15',
                '16', '17', '18',
                '19', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '30',
                '31',
            ],
        },
        'download.csv-lev.zip')

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-location", type=str, default="obs/")
    parser.add_argument("--output-location", type=str, default="obs/")
    parser.add_argument("--upload-to-hf", action="store_false")
    parser.add_argument("--hf-token", type=str, default="")
    parser.add_argument("--type", type=str, default="surface")
    parser.add_argument("--cds-key", type=str, default="")
    args = parser.parse_args()
    start_date = "2001-01-01"
    if args.type == "surface":
        get_data = get_surface_observations
        end_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        raw_location = "surface/"
        output_location = "surface/"
    elif args.type == "marine":
        get_data = get_marine_observation
        end_date = "2010-12-31"
        raw_location = "marine/"
        output_location = "marine/"

    date_range = pd.date_range(
        start=start_date,
        end=end_date,
        freq="1D",
    )
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        print(day)
        if not os.path.exists(output_location):
            os.mkdir(output_location)
        if not os.path.exists(raw_location):
            os.mkdir(raw_location)
        get_data(
            day, raw_location=raw_location, output_location=output_location, cds_key=args.cds_key
        )
