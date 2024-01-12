import cdsapi

c = cdsapi.Client()

c.retrieve(
    'cams-global-reanalysis-eac4',
    {
        'date': '2003-01-01/2003-01-01',
        'format': 'grib',
        'pressure_level': [
            '1', '2', '3',
            '5', '7', '10',
            '20', '30', '50',
            '70', '100', '150',
            '200', '250', '300',
            '400', '500', '600',
            '700', '800', '850',
            '900', '925', '950',
            '1000',
        ],
        'variable': [
            'black_carbon_aerosol_optical_depth_550nm', 'carbon_monoxide', 'dust_aerosol_0.03-0.55um_mixing_ratio',
            'dust_aerosol_0.55-0.9um_mixing_ratio', 'dust_aerosol_0.9-20um_mixing_ratio', 'dust_aerosol_optical_depth_550nm',
            'ethane', 'formaldehyde', 'hydrogen_peroxide',
            'hydrophilic_black_carbon_aerosol_mixing_ratio', 'hydrophilic_organic_matter_aerosol_mixing_ratio', 'hydrophobic_black_carbon_aerosol_mixing_ratio',
            'hydrophobic_organic_matter_aerosol_mixing_ratio', 'hydroxyl_radical', 'isoprene',
            'nitric_acid', 'nitrogen_dioxide', 'nitrogen_monoxide',
            'organic_matter_aerosol_optical_depth_550nm', 'ozone', 'particulate_matter_10um',
            'particulate_matter_1um', 'particulate_matter_2.5um', 'peroxyacetyl_nitrate',
            'propane', 'sea_salt_aerosol_0.03-0.5um_mixing_ratio', 'sea_salt_aerosol_0.5-5um_mixing_ratio',
            'sea_salt_aerosol_5-20um_mixing_ratio', 'sea_salt_aerosol_optical_depth_550nm', 'specific_humidity',
            'sulphate_aerosol_mixing_ratio', 'sulphate_aerosol_optical_depth_550nm', 'sulphur_dioxide',
            'surface_geopotential', 'total_aerosol_optical_depth_1240nm', 'total_aerosol_optical_depth_469nm',
            'total_aerosol_optical_depth_550nm', 'total_aerosol_optical_depth_670nm', 'total_aerosol_optical_depth_865nm',
            'total_column_carbon_monoxide', 'total_column_ethane', 'total_column_formaldehyde',
            'total_column_hydrogen_peroxide', 'total_column_hydroxyl_radical', 'total_column_isoprene',
            'total_column_methane', 'total_column_nitric_acid', 'total_column_nitrogen_dioxide',
            'total_column_nitrogen_monoxide', 'total_column_ozone', 'total_column_peroxyacetyl_nitrate',
            'total_column_propane', 'total_column_sulphur_dioxide', 'total_column_water_vapour',
        ],
        'time': [
            '00:00', '03:00', '06:00',
            '09:00', '12:00', '15:00',
            '18:00', '21:00',
        ],
    },
    'download.grib')