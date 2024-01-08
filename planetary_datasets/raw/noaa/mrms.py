from subprocess import Popen
import pandas as pd
import datetime as dt


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
