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
        'insitu-observations-surface-marine',
        {
            'format': 'csv-obs.zip',
            'variable': [
                'air_pressure_at_sea_level', 'air_temperature', 'dew_point_temperature',
                'water_temperature', 'wind_from_direction', 'wind_speed',
            ],
            'data_quality': [
                'failed', 'passed',
            ],
            'month': f'{day.strftime("%m")}',
            'day': [
                f'{day.strftime("%d")}',
            ],
            'year': f'{day.strftime("%Y")}',
        },
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.zip')


def get_surface_observations(day: dt.datetime, raw_location: str, output_location: str, cds_key: str):
    """Get surface observations from CDS"""
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=cds_key)
    c.retrieve(
        'insitu-observations-surface-land',
        {
            'format': 'zip',
            'time_aggregation': 'sub_daily',
            'variable': [
                'air_pressure', 'air_pressure_at_sea_level', 'air_temperature',
                'dew_point_temperature', 'wind_from_direction', 'wind_speed',
            ],
            'usage_restrictions': [
                'restricted', 'unrestricted',
            ],
            'data_quality': [
                'failed', 'passed',
            ],
            'month': f'{day.strftime("%m")}',
            'day': [
                f'{day.strftime("%d")}',
            ],
            'year': f'{day.strftime("%Y")}',
        },
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.zip')


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
        get_data(day, raw_location=raw_location, output_location=output_location, cds_key=args.cds_key)
