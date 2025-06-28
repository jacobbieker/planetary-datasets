import pvlib
import pandas as pd
"""
for station_id in ['TLH', ]: #'STE', 'ORT',  'SLC', 'HNX', 'SEA', 'BIS', 'MSN',  'ABQ']:
    data, meta = pvlib.iotools.get_solrad(
        station=station_id,  # station identifier
        start=pd.Timestamp(year=2015,month=1,day=1),
        end=pd.Timestamp.now())
    print(data)
    print(meta)
    data.to_csv(f'solrad_{station_id}_1min.csv')

"""
"""
Stations are:
ABQ: Albuquerque, NM
SLC: Salt Lake City, UT
HNX: Hanford, CA
SEA: Seattle, WA
BIS: Bismarck, ND
MSN: Madison, WI
ORT
TLH
STE
"""