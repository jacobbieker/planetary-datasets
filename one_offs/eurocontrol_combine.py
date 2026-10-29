import glob
import os
import pathlib

import pandas as pd
import polars as pl

flights = pl.scan_parquet(list(glob.glob("/Volumes/T7/opensky_2*.parquet")))
flights.sink_parquet(
    "/Volumes/T7/opensky_open_data_2.parquet", compression="zstd", compression_level=15
)
exit()
# march_1_flights = pl.read_parquet(list(glob.glob("/Volumes/T7/part-*.parquet")))
# print(march_1_flights)
# march_1_flights.write_parquet("/Volumes/T7/opensky_2026-03-01.parquet")
# exit()
filed_flights = pl.scan_parquet(
    list(glob.glob("/Volumes/T7/eucontrol_flight_points_filed_*.parquet"))
)
actual_flights = pl.scan_parquet(
    list(glob.glob("/Volumes/T7/eucontrol_flight_points_flown_*.parquet"))
)
# Write it out to disk as a single parquet file
filed_flights.sink_parquet("/Volumes/T7/eucontrol_flight_points_filed.parquet", compression="zstd")
actual_flights.sink_parquet("/Volumes/T7/eucontrol_flight_points_flown.parquet", compression="zstd")
exit()

date_range = pd.date_range(start="2015-03-01", end="2024-06-01", freq="3ME")
print(date_range)
# /Volumes/T7/202312/Flight_Points_Actual_20231201_20231231.csv.gz
# /Volumes/T7/202312/Flights_20231201_20231231.csv.gz
base_dir = pathlib.Path("/Volumes/T7/")
i = 0
filed = []
flown = []
for date in date_range:
    year_month = date.strftime("%Y%m")
    end_year_month = date.strftime("%Y%m%d")
    folder = base_dir / f"{year_month}"
    actual_points = folder / f"Flight_Points_Actual_{year_month}01_{end_year_month}.csv.gz"
    filed_points = folder / f"Flight_Points_Filed_{year_month}01_{end_year_month}.csv.gz"
    flights = folder / f"Flights_{year_month}01_{end_year_month}.csv.gz"
    if not actual_points.exists() or not filed_points.exists():
        print(f"Missing files for {year_month}, skipping")
        continue
    print(f"Processing {year_month}")
    df_actual = pl.read_csv(actual_points.as_posix())
    df_filed = pl.read_csv(filed_points.as_posix())
    flights = pl.read_csv(flights.as_posix())
    # print(df_actual)
    # print(df_filed)
    # print(flights.columns)
    # print(flights.head())
    # print(flights[["ECTRL ID", "AC Operator", "AC Type", "AC Registration", "ICAO Flight Type", "STATFOR Market Segment"]])
    flight_subset = flights[
        [
            "ECTRL ID",
            "AC Operator",
            "AC Type",
            "AC Registration",
            "ICAO Flight Type",
            "STATFOR Market Segment",
        ]
    ]
    # Combine based on ECTRL ID with the df_filed and df_actual
    df_actual = df_actual.join(flight_subset, on="ECTRL ID", how="left")
    df_filed = df_filed.join(flight_subset, on="ECTRL ID", how="left")
    # print(df_actual)
    # print(df_filed)
    filed.append(df_filed)
    flown.append(df_actual)
    if len(filed) > 8:
        filed = pl.concat(filed)
        flown = pl.concat(flown)
        # Save out to disk now as Parquet file
        filed.write_parquet(f"/Volumes/T7/eucontrol_flight_points_filed_{i}.parquet")
        flown.write_parquet(f"/Volumes/T7/eucontrol_flight_points_flown_{i}.parquet")
        i += 1
        filed = []
        flown = []


filed = pl.concat(filed)
flown = pl.concat(flown)
# Save out to disk now as Parquet file
filed.write_parquet(f"/Volumes/T7/eucontrol_flight_points_filed_{i}.parquet")
flown.write_parquet(f"/Volumes/T7/eucontrol_flight_points_flown_{i}.parquet")
