from isd import Batch

batch = Batch.from_path("/home/jacob/010020-99999-2016.gz").to_data_frame()
#with isd.io.open("/home/jacob/010020-99999-2016.gz") as records_iterator:
#    records = records_iterator.to_data_frame()

print(batch)
print(batch.columns)
# Convert pandas to xarray
records = batch.to_xarray()
print(records)
# Set the latitude and longitude as coordinates, as well as datetime instead of index
records = records.set_coords(["latitude", "longitude", "datetime"])
print(records)
# Set datetime as the dimension, and remove index
records = records.set_index({"index": "datetime"}).rename_dims({"index": "datetime"})
print(records)
# Add coordinate that is the ID of the station
# Create a combination of the usaf_id and ncei_id as the station_id
records = records.assign_coords(station_id=records["usaf_id"] + "-" + records["ncei_id"])
print(records)
# Drop the id data variables
records = records.drop_vars(["usaf_id", "ncei_id"])
# Set the station_id as a single element
records["station_id"] = records["station_id"].isel(datetime=0)
print(records)
# Do the same for latitude and longitude
records["latitude"] = records["latitude"].isel(datetime=0)
records["longitude"] = records["longitude"].isel(datetime=0)
# Rename index coordinate to time_utc
records = records.rename_dims({"datetime": "time"}).rename_vars({"index": "time"})
print(records)

# Now concatenate records together by station ID, then by time
