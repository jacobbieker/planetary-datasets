from subprocess import Popen
import pandas as pd
import datetime as dt
import fsspec
from planetary_datasets.base import AbstractSource, AbstractConvertor


class MRMSSource(AbstractSource):
    def __init__(self, source_location: str, raw_location: str,  **kwargs):
        super().__init__(source_location, raw_location,  **kwargs)
        self.observation_type = kwargs.get("observation_type", "PrecipRate")

    def get(self, timestamp: dt.datetime) -> list[str]:
        base_url = f"https://mtarchive.geol.iastate.edu/{timestamp.strftime('%Y')}/{timestamp.strftime('%m')}/{timestamp.strftime('%d')}/mrms/ncep/{self.observation_type}"
        files = self.list_files(base_url)
        output_paths = []
        for file in files:
            output_paths.append(self.download_file(file, self.raw_location / file.split("/")[-1]))
        return output_paths


    def list_files(self, url) -> list[str]:
        fs = fsspec.filesystem("https")
        return fs.ls(url)

    def process(self) -> str:
        pass

    def check_integrity(self, local_path: str) -> bool:
        pass


def get_mrms(day: dt.datetime):
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/GaugeCorr_QPE_01H/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/PrecipFlag/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/PrecipRate/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/RadarOnly_QPE_01H/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()
    path = f"https://mtarchive.geol.iastate.edu/{day.strftime('%Y')}/{day.strftime('%m')}/{day.strftime('%d')}/mrms/ncep/MultiSensor_QPE_01H_Pass2/"
    args = ["wget", "-r", "-nc", "--no-parent", path]
    process = Popen(args)
    process.wait()


if __name__ == "__main__":
    date_range = pd.date_range(
        start="2016-01-01", end=dt.datetime.now().strftime("%Y-%m-%d"), freq="D"
    )
    for day in date_range:
        try:
            mrms_obs = get_mrms(day)
        except Exception as e:
            print(e)
            continue
