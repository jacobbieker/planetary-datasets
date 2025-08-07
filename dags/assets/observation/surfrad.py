import pvlib
import pandas as pd
import os

filename = "https://gml.noaa.gov/aftp/data/radiation/surfrad/bon/2020/bon20115.dat"

# Possible years are 1995 to present
# Each is the day of the year in terms of <location>/<year>/<location><year><dayofyear>.dat
# Locations are available, although not all times and such, for
"""
AOD:
bon, dra, fpk, gwn, psu, sxf, tbl, was

type: aod, (ends in .aod, not .dat)

Other:
bon, dra, fpk, gwn, psu, red, rut, slv, sxf, tbl, was

for year in range(2002, 2025):
    for day_of_year in range(1, 366):
        for site in ['bon', 'dra', 'fpk', 'gwn', 'psu', 'red', 'rut', 'slv', 'sxf', 'tbl', 'was']:
            output_path = f'surfrad_csvs/surfrad_{site}_{year}_{day_of_year:03}.csv'
            filename = f'https://gml.noaa.gov/aftp/data/radiation/surfrad/{site}/{year}/{site}{str(year)[2:]}{day_of_year:03}.dat'
            if os.path.exists(output_path):
                print(f'surfrad_{site}_{year}_{day_of_year:03}.csv already exists')
                continue
            try:
                data, meta = pvlib.iotools.read_surfrad(filename)
                data.to_csv(output_path)
            except Exception as e:
                print(e, filename)
                continue
        #for aod_site in ['bon', 'dra', 'fpk', 'gwn', 'psu', 'sxf', 'tbl', 'was']:
        #    try:
        #        filename = f'https://gml.noaa.gov/aftp/data/radiation/surfrad/aod/{aod_site}/{year}/{aod_site}_{str(year)}{day_of_year:03}.aod'
        #        data, meta = pvlib.iotools.read_surfrad(filename)
        #        data.to_csv(f'surfrad_aod_{aod_site}_{year}_{day_of_year:03}.csv')
        #    except Exception as e:
        #        print(e, filename)
        #        continue

"""
