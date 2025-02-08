from wetterdienst.provider.eccc.observation import EcccObservationRequest, EcccObservationDataset
from planetary_datasets.base import AbstractSource, AbstractConvertor
from tqdm import tqdm
request = EcccObservationRequest(
            parameter=EcccObservationDataset.HOURLY,
            resolution="hourly",
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
    if os.path.exists(f"/run/media/jacob/data/ECCC_Obs/{i}.zarr"):
        continue
    try:
        df = result.df.to_pandas()
        df.date = df.date.map(lambda date: date.to_datetime64())
        df = df.set_index(["station_id", "dataset", "parameter", "date"])
        ds = df.to_xarray()
        ds.to_zarr(f"/run/media/jacob/data/ECCC_Obs/{i}.zarr", mode="w")
        print(ds)
    except Exception as e:
        print(e)
        continue
class EcccObservationSource(AbstractSource):
    def __init__(self, source_location: Path, raw_location: Path, start_date: datetime, end_date: datetime, **kwargs):
        super().__init__(source_location, raw_location, start_date, end_date, **kwargs)
        self.request = EcccObservationRequest(
            parameter=EcccObservationDataset.HOURLY,
            resolution=EcccObservationResolution.HOURLY,
            period=EcccObservationPeriod.HISTORICAL,
            start_date=start_date,
            end_date=end_date
        )
        self.station = self.request.all()

    def get(self, timestamp: datetime) -> Union[Path, xr.Dataset, xr.DataArray]:
        pass

    def process(self, files: list[Path]) -> Union[Path, xr.Dataset, xr.DataArray]:
        pass

    def check_integrity(self, local_path: Path) -> bool:
        pass