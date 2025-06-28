import urllib.request
import io
import zipfile
import os
import pandas as pd
import warnings

PVLIVE_BASE_URL = 'https://zenodo.org/record/15013388/files/'


def get_pvlive(station, start, end):
    """
    Retrieve ground measured irradiance data from the PV-Live network.

    The PV-Live network consists of 40 solar irradiance measurement stations
    across the German state of Baden-Württemberg. All stations measure global
    horizontal irradiance and temperature with a pyranometer, and global tilted
    irradiance in east, south, and west direction with tilt angles of 25° with
    three photovoltaic reference cells in minute resolution [1]_.

    Data is available from Zenodo [2]_.

    Parameters
    ----------
    station: int
        Station number (integer). All stations can be requested by specifying
        station='all'.
    start: datetime-like
        First day of the requested period
    end: datetime-like
        Last day of the requested period

    Returns
    -------
    data: DataFrame
        timeseries data from the PV-LIVE measurement network.
    metadata: dict
        metadata (not currently available).

    Warns
    -----
    UserWarning
        If one or more requested files are missing a UserWarning is returned.

    Notes
    -----
    Data is returned for the entire months between and including start and end.

    Examples
    --------
    References
    ----------
    .. [1] `High resolution measurement network of global horizontal and tilted solar
        irradiance in southern Germany with a new quality control scheme. Elke Lorenz
        et al. 2022. Solar Energy.
        <https://doi.org/10.1016/j.solener.2021.11.023/>`_
    .. [2] `Zenodo
       <https://doi.org/10.5281/zenodo.4036728>`_
    """  # noqa: E501

    months = pd.date_range(start, end.replace(day=1) + pd.DateOffset(months=1), freq='1ME')

    dfs_inter_months = []  # Initialize list for holding dataframes
    for m in months:
        # Generate URL to the monthly ZIP archive
        url = f"{PVLIVE_BASE_URL}pvlive_{m.year}-{m.month:02}.zip?download=1"
        try:
            remotezip = urllib.request.urlopen(url)
        except urllib.error.HTTPError as e:
            if 'not found' in e.msg.lower():
                warnings.warn('File was not found. The selected time period is probably '
                              'outside the available time period')
                continue
            else:
                raise
        zipinmemory = io.BytesIO(remotezip.read())
        zip = zipfile.ZipFile(zipinmemory)

        dfs_intra_months = []
        for filename in zip.namelist():
            basename = os.path.basename(filename)  # Extract file basename
            # Parse file if extension is file starts wtih 'tng' and ends with '.tsv'
            if basename.startswith('tng') & basename.endswith('.tsv'):
                # Extract station number from file name
                station_number = int(basename[6:8])
                if (station == 'all') | (station == station_number):
                    # Read data into pandas dataframe
                    dfi = pd.read_csv(io.StringIO(zip.read(filename).decode("utf-8")),
                                      sep='\t', index_col=[0], parse_dates=[0])

                    if station == 'all':
                        # Add station number to column names (MultiIndex)
                        dfi.columns = [[station_number] * dfi.shape[1], dfi.columns]
                    # Add dataframe to list
                    dfs_intra_months.append(dfi)
        dfs_inter_months.append(pd.concat(dfs_intra_months, axis='columns'))
    data = pd.concat(dfs_inter_months, axis='rows')

    meta = {}
    return data, meta


if __name__ == "__main__":
    # Download for all the stations from 2020-09 to present
    for date in pd.date_range('2020-09-01', pd.Timestamp.today(), freq='1ME'):
        data, meta = get_pvlive('all', date, date + pd.DateOffset(months=1))
        data.to_csv(f'pvlive_{date.year}-{date.month:02}.csv')