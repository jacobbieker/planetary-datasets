from wetterdienst.provider.geosphere.observation import GeosphereObservationRequest, GeosphereObservationDataset, GeosphereObservationResolution, GeosphereObservationParameter
from planetary_datasets.base import AbstractSource, AbstractConvertor
import os
from tqdm import tqdm
request = GeosphereObservationRequest(
    parameter=GeosphereObservationParameter.HOURLY.WIND_SPEED,
    resolution=GeosphereObservationResolution.HOURLY,
    start_date="2010-01-01",
    end_date="2024-01-01"
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
    if os.path.exists(f"/run/media/jacob/data/Geosphere_Obs/{i}_hourly.zarr"):
        continue
    try:
        df = result.df.to_pandas()
        df.date = df.date.map(lambda date: date.to_datetime64())
        df = df.set_index(["station_id", "dataset", "parameter", "date"])
        ds = df.to_xarray()
        ds.to_zarr(f"/run/media/jacob/data/Geosphere_Obs/{i}_hourly.zarr", mode="w")
        print(ds)
    except Exception as e:
        print(e)
        continue