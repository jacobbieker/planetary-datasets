import cdsapi
import datetime as dt
import os
import pandas as pd
import random
import shutil
import zarr
import xarray as xr
from huggingface_hub import HfApi


def get_eac4_day(day: dt.datetime, raw_location: str, output_location: str, cds_key: str):
    """Get surface observations from CDS"""
    c = cdsapi.Client(url="https://ads.atmosphere.copernicus.eu/api/v2", key=cds_key)
    c.retrieve(
        "cams-global-reanalysis-eac4",
        {
            "date": f'{day.strftime("%Y-%m-%d")}/{day.strftime("%Y-%m-%d")}',
            "format": "grib",
            "pressure_level": [
                "1",
                "2",
                "3",
                "5",
                "7",
                "10",
                "20",
                "30",
                "50",
                "70",
                "100",
                "150",
                "200",
                "250",
                "300",
                "400",
                "500",
                "600",
                "700",
                "800",
                "850",
                "900",
                "925",
                "950",
                "1000",
            ],
            "variable": [
                "black_carbon_aerosol_optical_depth_550nm",
                "carbon_monoxide",
                "dust_aerosol_0.03-0.55um_mixing_ratio",
                "dust_aerosol_0.55-0.9um_mixing_ratio",
                "dust_aerosol_0.9-20um_mixing_ratio",
                "dust_aerosol_optical_depth_550nm",
                "ethane",
                "formaldehyde",
                "hydrogen_peroxide",
                "hydrophilic_black_carbon_aerosol_mixing_ratio",
                "hydrophilic_organic_matter_aerosol_mixing_ratio",
                "hydrophobic_black_carbon_aerosol_mixing_ratio",
                "hydrophobic_organic_matter_aerosol_mixing_ratio",
                "hydroxyl_radical",
                "isoprene",
                "nitric_acid",
                "nitrogen_dioxide",
                "nitrogen_monoxide",
                "organic_matter_aerosol_optical_depth_550nm",
                "ozone",
                "particulate_matter_10um",
                "particulate_matter_1um",
                "particulate_matter_2.5um",
                "peroxyacetyl_nitrate",
                "propane",
                "sea_salt_aerosol_0.03-0.5um_mixing_ratio",
                "sea_salt_aerosol_0.5-5um_mixing_ratio",
                "sea_salt_aerosol_5-20um_mixing_ratio",
                "sea_salt_aerosol_optical_depth_550nm",
                "specific_humidity",
                "sulphate_aerosol_mixing_ratio",
                "sulphate_aerosol_optical_depth_550nm",
                "sulphur_dioxide",
                "surface_geopotential",
                "total_aerosol_optical_depth_1240nm",
                "total_aerosol_optical_depth_469nm",
                "total_aerosol_optical_depth_550nm",
                "total_aerosol_optical_depth_670nm",
                "total_aerosol_optical_depth_865nm",
                "total_column_carbon_monoxide",
                "total_column_ethane",
                "total_column_formaldehyde",
                "total_column_hydrogen_peroxide",
                "total_column_hydroxyl_radical",
                "total_column_isoprene",
                "total_column_methane",
                "total_column_nitric_acid",
                "total_column_nitrogen_dioxide",
                "total_column_nitrogen_monoxide",
                "total_column_ozone",
                "total_column_peroxyacetyl_nitrate",
                "total_column_propane",
                "total_column_sulphur_dioxide",
                "total_column_water_vapour",
            ],
            "time": [
                "00:00",
                "03:00",
                "06:00",
                "09:00",
                "12:00",
                "15:00",
                "18:00",
                "21:00",
            ],
        },
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.grib',
    )

    # Convert to zarr
    ds = xr.open_dataset(
        f'{os.path.join(raw_location,day.strftime("%Y%m%d"))}.grib', engine="cfgrib"
    )
    print(ds)


def upload_to_hf(zip_name, hf_token, repo_id):
    api = HfApi(token=hf_token)
    api.upload_file(
        path_or_fileobj=zip_name,
        path_in_repo=f"data/{zip_name.split('/')[-1][:4]}/{zip_name}",
        repo_id=repo_id,
        repo_type="dataset",
    )
    os.remove(zip_name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-location", type=str, default="eac4_raw/")
    parser.add_argument("--output-location", type=str, default="eac4_zarr/")
    parser.add_argument("--upload-to-hf", action="store_false")
    parser.add_argument("--hf-token", type=str, default="")
    parser.add_argument("--cds-key", type=str, default="")
    args = parser.parse_args()
    start_date = "2003-01-01"
    end_date = (dt.datetime.now() - dt.timedelta(days=360)).strftime("%Y-%m-%d")
    date_range = pd.date_range(
        start=start_date,
        end=end_date,
        freq="1D",
    )
    output_location = args.output_location
    raw_location = args.raw_location
    start_idx = random.randint(0, len(date_range))
    for day in date_range[start_idx:]:
        print(day)
        if not os.path.exists(output_location):
            os.mkdir(output_location)
        if not os.path.exists(raw_location):
            os.mkdir(raw_location)
        get_eac4_day(
            day, raw_location=raw_location, output_location=output_location, cds_key=args.cds_key
        )
        exit()
