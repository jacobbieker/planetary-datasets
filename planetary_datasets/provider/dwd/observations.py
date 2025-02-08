from wetterdienst import Wetterdienst
import xarray as xr
import datetime as dt
from planetary_datasets.base import AbstractSource, AbstractConvertor
from pathlib import Path
from tqdm import tqdm
import zarr

from wetterdienst.provider.dwd.observation import DwdObservationRequest, DwdObservationDataset, DwdObservationPeriod, DwdObservationResolution

request = DwdObservationRequest(
    parameter=[DwdObservationDataset.PRECIPITATION, DwdObservationDataset.TEMPERATURE_AIR, DwdObservationDataset.WIND_EXTREME, DwdObservationDataset.SOLAR, DwdObservationDataset.WIND],
    resolution=DwdObservationResolution.MINUTE_10,
    period=DwdObservationPeriod.HISTORICAL
)

request = DwdObservationRequest(
    parameter=[DwdObservationDataset.TEMPERATURE_AIR, DwdObservationDataset.CLOUD_TYPE, DwdObservationDataset.CLOUDINESS, DwdObservationDataset.DEW_POINT,
               DwdObservationDataset.PRECIPITATION, DwdObservationDataset.PRESSURE, DwdObservationDataset.TEMPERATURE_SOIL, #DwdObservationDataset.SOLAR,
               DwdObservationDataset.SUN, DwdObservationDataset.VISIBILITY, DwdObservationDataset.WEATHER_PHENOMENA, DwdObservationDataset.WIND,
               DwdObservationDataset.WIND_SYNOPTIC, DwdObservationDataset.WIND_EXTREME, DwdObservationDataset.MOISTURE, DwdObservationDataset.URBAN_TEMPERATURE_AIR,
               DwdObservationDataset.URBAN_PRECIPITATION, DwdObservationDataset.URBAN_PRESSURE, DwdObservationDataset.URBAN_TEMPERATURE_SOIL, DwdObservationDataset.URBAN_SUN,
               DwdObservationDataset.URBAN_WIND],
    resolution=DwdObservationResolution.HOURLY,
    period=DwdObservationPeriod.HISTORICAL
)

stations = request.all()

df = stations.df.to_pandas().to_xarray()
print(df)
meta = request.all().df
data = []
i = 0
import os
for result in tqdm(request.all().values.query(), total=meta.shape[0]):
    i += 1
    if os.path.exists(f"/run/media/jacob/data/DWD_Obs/{i}.zarr"):
        continue
    try:
        df = result.df.to_pandas()
        df.date = df.date.map(lambda date: date.to_datetime64())
        df = df.set_index(["station_id", "dataset", "parameter", "date"])
        ds = df.to_xarray()
        ds.to_zarr(f"/run/media/jacob/data/DWD_Obs/{i}.zarr", mode="w")
        print(ds)
    except Exception as e:
        print(e)
        continue
#ds = xr.concat(data, dim="station_id")
#ds.to_zarr(store, mode="w")
exit()
values = request.values.all().df
values = values.to_pandas()
values = values.to_xarray()
print(values)
exit()
#for result in stations.values.query():
#    print(result.df.drop_nulls().head())

request = DwdObservationRequest(
    parameter=[DwdObservationDataset.TEMPERATURE_AIR, DwdObservationDataset.CLOUD_TYPE, DwdObservationDataset.CLOUDINESS, DwdObservationDataset.DEW_POINT,
               DwdObservationDataset.PRECIPITATION, DwdObservationDataset.PRESSURE, DwdObservationDataset.TEMPERATURE_SOIL, DwdObservationDataset.SOLAR,
               DwdObservationDataset.SUN, DwdObservationDataset.VISIBILITY, DwdObservationDataset.WEATHER_PHENOMENA, DwdObservationDataset.WIND,
               DwdObservationDataset.WIND_SYNOPTIC, DwdObservationDataset.WIND_EXTREME, DwdObservationDataset.MOISTURE, DwdObservationDataset.URBAN_TEMPERATURE_AIR,
               DwdObservationDataset.URBAN_PRECIPITATION, DwdObservationDataset.URBAN_PRESSURE, DwdObservationDataset.URBAN_TEMPERATURE_SOIL, DwdObservationDataset.URBAN_SUN,
               DwdObservationDataset.URBAN_WIND],
    resolution=DwdObservationResolution.HOURLY,
    period=DwdObservationPeriod.HISTORICAL
)

stations = request.all()

df = stations.df
print(df)

#for result in stations.values.query():
#    print(result.df.drop_nulls().head())
