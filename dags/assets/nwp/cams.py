"""NetCDF archive of Atmospheric Quality data from CAMS, covering Europe.

CAMS is Copernicus' Atmospheric Monitoring Service, which provides
forecasts of atmospheric quality.

Sourced via CDS API from Copernicus ADS (https://ads.atmosphere.copernicus.eu).
This asset is updated weekly, and surfaced as a zipped NetCDF file for each week
per variable. It is downloaded using the cdsapi Python package
(https://github.com/ecmwf/cdsapi).
"""

import datetime as dt
import pathlib
from typing import Any

import cdsapi
import dagster as dg

ARCHIVE_FOLDER = "/ext_data/cams-europe"

partitions_def: dg.TimeWindowPartitionsDefinition = dg.WeeklyPartitionsDefinition(
    start_date="2020-02-08",
    end_offset=-2,
)


@dg.asset(
    name="cams-europe",
    description=__doc__,
    key_prefix=["air"],
    metadata={
        "archive_folder": dg.MetadataValue.text(ARCHIVE_FOLDER),
        "area": dg.MetadataValue.text("europe"),
        "source": dg.MetadataValue.text("copernicus-ads"),
        "model": dg.MetadataValue.text("cams"),
        "format": dg.MetadataValue.text("netcdf"),
        "expected_runtime": dg.MetadataValue.text("6 hours"),
    },
    compute_kind="python",
    automation_condition=dg.AutomationCondition.on_cron(
        cron_schedule=partitions_def.get_cron_schedule(
            hour_of_day=7,
        ),
    ),
    tags={
        "dagster/max_runtime": str(60 * 60 * 24 * 4),  # Should take about 2 days
        "dagster/priority": "1",
        "dagster/concurrency_key": "copernicus-ads",
    },
    partitions_def=partitions_def,
)
def cams_eu_raw_asset(context: dg.AssetExecutionContext) -> dg.Output[list[pathlib.Path]]:
    """Downloads CAMS Europe air quality forecast data from Copernicus ADS."""
    it_start: dt.datetime = context.partition_time_window.start
    it_end: dt.datetime = context.partition_time_window.end
    execution_start = dt.datetime.now(tz=dt.UTC)
    stored_files: list[pathlib.Path] = []

    variables: list[str] = [
        "alder_pollen",
        "ammonia",
        "birch_pollen",
        "carbon_monoxide",
        "dust",
        "grass_pollen",
        "nitrogen_dioxide",
        "nitrogen_monoxide",
        "non_methane_vocs",
        "olive_pollen",
        "ozone",
        "particulate_matter_10um",
        "particulate_matter_2.5um",
        "peroxyacyl_nitrates",
        "pm10_wildfires",
        "ragweed_pollen",
        "secondary_inorganic_aerosol",
        "sulphur_dioxide",
    ]

    for var in variables:
        dst: pathlib.Path = (
            pathlib.Path(ARCHIVE_FOLDER) / "raw" / f"{it_start:%Y%m%d}-{it_end:%Y%m%d}_{var}.nc.zip"
        )
        dst.parent.mkdir(parents=True, exist_ok=True)

        if dst.exists():
            context.log.info(
                "File already exists, skipping download",
                extra={
                    "file": dst.as_posix(),
                },
            )
            stored_files.append(dst)
            continue

        request: dict[str, Any] = {
            "date": [f"{it_start:%Y-%m-%d}/{it_end:%Y-%m-%d}"],
            "type": ["forecast"],
            "time": ["00:00"],
            "model": ["ensemble"],
            "leadtime_hour": [str(x) for x in range(0, 97)],
            "data_format": ["netcdf_zip"],
            "level": ["0", "50", "250", "500", "1000", "3000", "5000"],
            "variable": [var],
        }

        context.log.info(
            "Reqesting file from Copernicus ADS via CDS API",
            extra={
                "request": request,
                "target": dst.as_posix(),
            },
        )
        client = cdsapi.Client()
        client.retrieve(
            name="cams-europe-air-quality-forecast",
            request=request,
            target=dst.as_posix(),
        )
        context.log.info(
            f"Downloaded file {dst.as_posix()} from Copernicus ADS via CDS API",
            extra={
                "file": dst.as_posix(),
                "size": dst.stat().st_size,
            },
        )
        stored_files.append(dst)

    if len(stored_files) == 0:
        raise Exception(
            "No remote files found for this partition key. See logs for more details.",
        )

    elapsed_time: dt.timedelta = dt.datetime.now(tz=dt.UTC) - execution_start

    return dg.Output(
        value=stored_files,
        metadata={
            "files": dg.MetadataValue.text(", ".join([f.as_posix() for f in stored_files])),
            "partition_size": dg.MetadataValue.int(sum([f.stat().st_size for f in stored_files])),
            "elapsed_time_hours": dg.MetadataValue.float(elapsed_time / dt.timedelta(hours=1)),
        },
    )


"""
CAMS Global Greenhouse Gase emissions
import cdsapi

dataset = "cams-global-greenhouse-gas-forecasts"
request = {
    "variable": [
        "total_column_carbon_monoxide",
        "accumulated_carbon_dioxide_ecosystem_respiration",
        "accumulated_carbon_dioxide_gross_primary_production",
        "accumulated_carbon_dioxide_net_ecosystem_exchange",
        "flux_of_carbon_dioxide_ecosystem_respiration",
        "flux_of_carbon_dioxide_gross_primary_production",
        "flux_of_carbon_dioxide_net_ecosystem_exchange",
        "gpp_coefficient_from_biogenic_flux_adjustment_system",
        "rec_coefficient_from_biogenic_flux_adjustment_system",
        "carbon_dioxide",
        "carbon_monoxide",
        "methane"
    ],
    "pressure_level": [
        "10", "20", "30",
        "50", "70", "100",
        "150", "200", "250",
        "300", "400", "500",
        "600", "700", "800",
        "850", "900", "925",
        "950", "1000"
    ],
    "date": ["2025-02-21/2025-02-21"],
    "leadtime_hour": [
        "0",
        "3",
        "6",
        "9",
        "12",
        "15",
        "18",
        "21",
        "24",
        "27",
        "30",
        "33",
        "36",
        "39",
        "42",
        "45",
        "48",
        "51",
        "54",
        "57",
        "60",
        "63",
        "66",
        "69",
        "72",
        "75",
        "78",
        "81",
        "84",
        "87",
        "90",
        "93",
        "96",
        "99",
        "102",
        "105",
        "108",
        "111",
        "114",
        "117",
        "120"
    ],
    "data_format": "grib"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()

CAMS Global Reanalysis
import cdsapi

dataset = "cams-global-reanalysis-eac4"
request = {
    "date": ["2023-12-31/2023-12-31"],
    "time": [
        "00:00", "03:00", "06:00",
        "09:00", "12:00", "15:00",
        "18:00", "21:00"
    ],
    "data_format": "grib",
    "variable": [
        "black_carbon_aerosol_optical_depth_550nm",
        "dust_aerosol_optical_depth_550nm",
        "organic_matter_aerosol_optical_depth_550nm",
        "particulate_matter_1um",
        "particulate_matter_2.5um",
        "particulate_matter_10um",
        "sea_salt_aerosol_optical_depth_550nm",
        "sulphate_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_469nm",
        "total_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_670nm",
        "total_aerosol_optical_depth_865nm",
        "total_aerosol_optical_depth_1240nm",
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
        "total_column_water_vapour"
    ]
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()

CAMS Global Composition Forecasts (All single level ones, need to do it in chunks of 20 lead time out to 120)
import cdsapi

dataset = "cams-global-atmospheric-composition-forecasts"
request = {
    "variable": [
        "ammonium_aerosol_optical_depth_550nm",
        "black_carbon_aerosol_optical_depth_550nm",
        "dust_aerosol_optical_depth_550nm",
        "nitrate_aerosol_optical_depth_550nm",
        "organic_matter_aerosol_optical_depth_550nm",
        "particulate_matter_1um",
        "particulate_matter_2.5um",
        "particulate_matter_10um",
        "sea_salt_aerosol_optical_depth_550nm",
        "secondary_organic_aerosol_optical_depth_550nm",
        "sulphate_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_469nm",
        "total_aerosol_optical_depth_550nm",
        "total_aerosol_optical_depth_670nm",
        "total_aerosol_optical_depth_865nm",
        "total_aerosol_optical_depth_1240nm",
        "total_column_carbon_monoxide",
        "total_column_chlorine_monoxide",
        "total_column_chlorine_nitrate",
        "total_column_ethane",
        "total_column_formaldehyde",
        "total_column_hydrogen_chloride",
        "total_column_hydrogen_cyanide",
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
        "total_column_volcanic_sulphur_dioxide",
        "total_column_water_vapour",
        "asymmetry_factor_340nm",
        "asymmetry_factor_355nm",
        "asymmetry_factor_380nm",
        "asymmetry_factor_400nm",
        "asymmetry_factor_440nm",
        "asymmetry_factor_469nm",
        "asymmetry_factor_500nm",
        "asymmetry_factor_532nm",
        "asymmetry_factor_550nm",
        "asymmetry_factor_645nm",
        "asymmetry_factor_670nm",
        "asymmetry_factor_800nm",
        "asymmetry_factor_858nm",
        "asymmetry_factor_865nm",
        "asymmetry_factor_1020nm",
        "asymmetry_factor_1064nm",
        "asymmetry_factor_1240nm",
        "asymmetry_factor_1640nm",
        "asymmetry_factor_2130nm",
        "dust_aerosol_0.03-0.55um_optical_depth_550nm",
        "dust_aerosol_0.55-9um_optical_depth_550nm",
        "dust_aerosol_9-20um_optical_depth_550nm",
        "hydrophilic_black_carbon_aerosol_optical_depth_550nm",
        "hydrophilic_organic_matter_aerosol_optical_depth_550nm",
        "hydrophobic_black_carbon_aerosol_optical_depth_550nm",
        "hydrophobic_organic_matter_aerosol_optical_depth_550nm",
        "nitrate_coarse_mode_aerosol_optical_depth_550nm",
        "nitrate_fine_mode_aerosol_optical_depth_550nm",
        "sea_salt_aerosol_0.03-0.5um_optical_depth_550nm",
        "sea_salt_aerosol_0.5-5um_optical_depth_550nm",
        "sea_salt_aerosol_5-20um_optical_depth_550nm",
        "single_scattering_albedo_340nm",
        "single_scattering_albedo_355nm",
        "single_scattering_albedo_380nm",
        "single_scattering_albedo_400nm",
        "single_scattering_albedo_440nm",
        "single_scattering_albedo_469nm",
        "single_scattering_albedo_500nm",
        "single_scattering_albedo_532nm",
        "single_scattering_albedo_550nm",
        "single_scattering_albedo_645nm",
        "single_scattering_albedo_670nm",
        "single_scattering_albedo_800nm",
        "single_scattering_albedo_858nm",
        "single_scattering_albedo_865nm",
        "single_scattering_albedo_1020nm",
        "single_scattering_albedo_1064nm",
        "single_scattering_albedo_1240nm",
        "single_scattering_albedo_1640nm",
        "single_scattering_albedo_2130nm",
        "total_absorption_aerosol_optical_depth_340nm",
        "total_absorption_aerosol_optical_depth_355nm",
        "total_absorption_aerosol_optical_depth_380nm",
        "total_absorption_aerosol_optical_depth_400nm",
        "total_absorption_aerosol_optical_depth_440nm",
        "total_absorption_aerosol_optical_depth_469nm",
        "total_absorption_aerosol_optical_depth_500nm",
        "total_absorption_aerosol_optical_depth_532nm",
        "total_absorption_aerosol_optical_depth_550nm",
        "total_absorption_aerosol_optical_depth_645nm",
        "total_absorption_aerosol_optical_depth_670nm",
        "total_absorption_aerosol_optical_depth_800nm",
        "total_absorption_aerosol_optical_depth_858nm",
        "total_absorption_aerosol_optical_depth_865nm",
        "total_absorption_aerosol_optical_depth_1020nm",
        "total_absorption_aerosol_optical_depth_1064nm",
        "total_absorption_aerosol_optical_depth_1240nm",
        "total_absorption_aerosol_optical_depth_1640nm",
        "total_absorption_aerosol_optical_depth_2130nm",
        "total_aerosol_optical_depth_340nm",
        "total_aerosol_optical_depth_355nm",
        "total_aerosol_optical_depth_380nm",
        "total_aerosol_optical_depth_400nm",
        "total_aerosol_optical_depth_440nm",
        "total_aerosol_optical_depth_500nm",
        "total_aerosol_optical_depth_532nm",
        "total_aerosol_optical_depth_645nm",
        "total_aerosol_optical_depth_800nm",
        "total_aerosol_optical_depth_858nm",
        "total_aerosol_optical_depth_1020nm",
        "total_aerosol_optical_depth_1064nm",
        "total_aerosol_optical_depth_1640nm",
        "total_aerosol_optical_depth_2130nm",
        "total_fine_mode_aerosol_optical_depth_340nm",
        "total_fine_mode_aerosol_optical_depth_355nm",
        "total_fine_mode_aerosol_optical_depth_380nm",
        "total_fine_mode_aerosol_optical_depth_400nm",
        "total_fine_mode_aerosol_optical_depth_440nm",
        "total_fine_mode_aerosol_optical_depth_469nm",
        "total_fine_mode_aerosol_optical_depth_500nm",
        "total_fine_mode_aerosol_optical_depth_532nm",
        "total_fine_mode_aerosol_optical_depth_550nm",
        "total_fine_mode_aerosol_optical_depth_645nm",
        "total_fine_mode_aerosol_optical_depth_670nm",
        "total_fine_mode_aerosol_optical_depth_800nm",
        "total_fine_mode_aerosol_optical_depth_858nm",
        "total_fine_mode_aerosol_optical_depth_865nm",
        "total_fine_mode_aerosol_optical_depth_1020nm",
        "total_fine_mode_aerosol_optical_depth_1064nm",
        "total_fine_mode_aerosol_optical_depth_1240nm",
        "total_fine_mode_aerosol_optical_depth_1640nm",
        "total_fine_mode_aerosol_optical_depth_2130nm",
        "total_column_acetone",
        "total_column_acetone_product",
        "total_column_acetonitrile",
        "total_column_aldehydes",
        "total_column_amine",
        "total_column_ammonia",
        "total_column_ammonium",
        "total_column_asymmetric_chlorine_dioxide_radical",
        "total_column_bromine",
        "total_column_bromine_atom",
        "total_column_bromine_monochloride",
        "total_column_bromine_monoxide",
        "total_column_bromine_nitrate",
        "total_column_bromochlorodifluoromethane",
        "total_column_chlorine",
        "total_column_chlorine_atom",
        "total_column_chlorine_dioxide",
        "total_column_chlorodifluoromethane",
        "total_column_chloropentafluoroethane",
        "total_column_dibromomethane",
        "total_column_dichlorine_dioxide",
        "total_column_dichlorodifluoromethane",
        "total_column_dichlorotetrafluoroethane",
        "total_column_dimethyl_sulfide",
        "total_column_dinitrogen_pentoxide",
        "total_column_ethanol",
        "total_column_ethene",
        "total_column_formic_acid",
        "total_column_glyoxal",
        "total_column_hydrogen_bromide",
        "total_column_hydrogen_fluoride",
        "total_column_hydroperoxy_radical",
        "total_column_hypobromous_acid",
        "total_column_hypochlorous_acid",
        "total_column_hypropo2",
        "total_column_ic3h7o2",
        "total_column_lead",
        "total_column_methacrolein_mvk",
        "total_column_methacrylic_acid",
        "total_column_methane_sulfonic_acid",
        "total_column_methanol",
        "total_column_methyl_bromide",
        "total_column_methyl_chloride",
        "total_column_methyl_chloroform",
        "total_column_methyl_glyoxal",
        "total_column_methyl_peroxide",
        "total_column_methylperoxy_radical",
        "total_column_nitrate",
        "total_column_nitrate_radical",
        "total_column_nitrogen_oxides_transp",
        "total_column_nitryl_chloride",
        "total_column_no_to_alkyl_nitrate_operator",
        "total_column_no_to_no2_operator",
        "total_column_olefins",
        "total_column_organic_ethers",
        "total_column_organic_nitrates",
        "total_column_paraffins",
        "total_column_pernitric_acid",
        "total_column_peroxides",
        "total_column_peroxy_acetyl_radical",
        "total_column_polar_stratospheric_cloud",
        "total_column_propene",
        "total_column_radon",
        "total_column_stratospheric_ozone",
        "total_column_terpenes",
        "total_column_tetrachloromethane",
        "total_column_tribromomethane",
        "total_column_trichlorofluoromethane",
        "total_column_trichlorotrifluoroethane",
        "total_column_trifluorobromomethane",
        "total_column_water_vapour_chemistry",
        "vertically_integrated_mass_of_ammonium_aerosol",
        "vertically_integrated_mass_of_coarse_mode_nitrate_aerosol",
        "vertically_integrated_mass_of_dust_aerosol_0.03-0.55um",
        "vertically_integrated_mass_of_dust_aerosol_0.55-9um",
        "vertically_integrated_mass_of_dust_aerosol_9-20um",
        "vertically_integrated_mass_of_fine_mode_nitrate_aerosol",
        "vertically_integrated_mass_of_hydrophilic_black_carbon_aerosol",
        "vertically_integrated_mass_of_hydrophilic_organic_matter_aerosol",
        "vertically_integrated_mass_of_hydrophobic_black_carbon_aerosol",
        "vertically_integrated_mass_of_hydrophobic_organic_matter_aerosol",
        "vertically_integrated_mass_of_sea_salt_aerosol_0.03-0.5um",
        "vertically_integrated_mass_of_sea_salt_aerosol_0.5-5um",
        "vertically_integrated_mass_of_sea_salt_aerosol_5-20um",
        "vertically_integrated_mass_of_sulphate_aerosol"
    ],
    "date": ["2025-02-21/2025-02-21"],
    "time": ["00:00"],
    "leadtime_hour": [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
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
        "20"
    ],
    "type": ["forecast"],
    "data_format": "grib"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()

"""
