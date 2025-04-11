import os
import time

import dagster as dg
import requests

"""Zarr archive of satellite image data from GMGSI global mosaic of geostationary satellites from NOAA on AWS"""

ARCHIVE_FOLDER = "/Users/jacob/Development/planetary-datasets/dags/assets/observation/NSRDB/"
if os.getenv("ENVIRONMENT", "local") == "pb":
    ARCHIVE_FOLDER = "/data/NSRDB/"

def generate_points(last_point: int, chunks: int = 200) -> list[list[str]]:
    """Generate the points for the NSRDB API"""
    points = []
    # Add points in sets of 200, like in the example code
    for i in range(0, last_point+1, chunks):
        set_of_points = ""
        for j in range(i, i + chunks):
            set_of_points += f"{j},"
        set_of_points = set_of_points[:-1]
        points.append(set_of_points)
    return points

def try_download_file(url: str, local_path: str) -> bool:
    """Try to download the file from the given url and save it to the local path"""
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

himawari8_partitions_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(8683127)))],
)
himawari7_partition_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(2170781)))],
)
iodc_partitions_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(102299)))],
)
goes_10min_partition_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(9462459)))],
)
goes_30min_partition_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
   [str(i) for i in range(len(generate_points(2018266, chunks=50)))],
)
mtg_recent_partition_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(3869543)))],
)
mtg_longer_partition_def: dg.PartitionsDefinition = dg.StaticPartitionsDefinition(
    [str(i) for i in range(len(generate_points(2693286)))],
)

def get_response_json_and_handle_errors(response: requests.Response) -> dict:
    """Takes the given response and handles any errors, along with providing
    the resulting json

    Parameters
    ----------
    response : requests.Response
        The response object

    Returns
    -------
    dict
        The resulting json
    """
    if response.status_code != 200:
        print(f"An error has occurred with the server or the request. The request response code/status: {response.status_code} {response.reason}")
        print(f"The response body: {response.text}")
        exit(1)

    try:
        response_json = response.json()
    except:
        print(f"The response couldn't be parsed as JSON, likely an issue with the server, here is the text: {response.text}")
        exit(1)

    if len(response_json['errors']) > 0:
        errors = '\n'.join(response_json['errors'])
        print(f"The request errored out, here are the errors: {errors}")
        exit(1)
    return response_json

@dg.asset(name="nsrdb-himawari8", description="Download NSRDB data for Himawari 8",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=himawari8_partitions_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_himawari8_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/himawari-download.json?"
    input_data = {
        'attributes': 'dhi,dni,ghi,solar_zenith_angle,cloud_type',
        'interval': '10',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    name = ['2016,2017,2018,2019,2020']
    location_ids = generate_points(8683127)[partition_key]
    input_data['years'] = [name]
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"himawari8_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="nsrdb-himawari7", description="Download NSRDB data for Himawari 8",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=himawari7_partition_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_himawari7_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/himawari7-download.json?"
    input_data = {
        'attributes': 'dhi,dni,ghi,cloud_type,solar_zenith_angle',
        'interval': '30',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(2170781)[partition_key]
    input_data['years'] = ['2011,2012,2013,2014,2015']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"himawari7_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="nsrdb-goes-10min", description="Download NSRDB data for GOES 10min",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=goes_10min_partition_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_goes10min_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-full-disc-v4-0-0-download.json?"
    input_data = {
        'attributes': 'aod,cloud_type,dhi,dni,ghi,solar_zenith_angle',
        'interval': '10',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(9462459)[partition_key]
    input_data['years'] = ['2019,2020,2021']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"goes_10min_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="nsrdb-goes-30min", description="Download NSRDB data for GOES 10min",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=goes_30min_partition_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_goes30min_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-GOES-aggregated-v4-0-0-download.json?"
    input_data = {
        'attributes': 'dhi,ghi,dni,cloud_type,solar_zenith_angle',
        'interval': '30',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(2018266, chunks=50)[partition_key]
    input_data['years'] = ['1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"goes_30min_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="nsrdb-iodc-60min", description="Download NSRDB data for GOES 10min",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=iodc_partitions_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_iodc_60min_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/suny-india-download.json?"
    input_data = {
        'attributes': 'dhi,dni,ghi,solar_zenith_angle',
        'interval': '60',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(102299)[partition_key]
    input_data['years'] = ['2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"iodc_60min_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )

@dg.asset(name="nsrdb-mtg-15min", description="Download NSRDB data for GOES 10min",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=mtg_recent_partition_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_mtg_15min_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/msg-iodc-download.json?"
    input_data = {
        'attributes': 'ghi,dhi,dni,solar_zenith_angle,cloud_type',
        'interval': '15',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(3869543)[partition_key]
    input_data['years'] = ['2017,2018,2019']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"mtg_15min_recent_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
            time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )
@dg.asset(name="nsrdb-mtg-15min-longer", description="Download NSRDB data for GOES 10min",
          tags={
              "dagster/max_runtime": str(60 * 60 * 10),  # Should take 6 ish hours
              "dagster/priority": "1",
              "dagster/concurrency_key": "download",
          },
          partitions_def=mtg_longer_partition_def,
automation_condition=dg.AutomationCondition.eager(),
          )
def nsrdb_mtg_15min_longer_download_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Dagster asset for downloading GMGSI global mosaic of geostationary satellites from NOAA on AWS"""
    partition_key = int(context.partition_key)
    BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/nsrdb-msg-v1-0-0-download.json?"
    input_data = {
        'attributes': 'ghi,dhi,dni,solar_zenith_angle,cloud_type',
        'interval': '15',
        'api_key': API_KEY,
        'email': EMAIL,
    }
    location_ids = generate_points(2693286)[partition_key]
    input_data['years'] = ['2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022']
    input_data['location_ids'] = location_ids
    headers = {
        'x-api-key': API_KEY
    }
    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
    download_url = data['outputs']['downloadUrl']
    not_downloaded = True
    while not_downloaded:
        try:
            downloaded_files = []
            local_path = os.path.join(ARCHIVE_FOLDER, f"mtg_15min_recent_key_{partition_key}.zip")
            if try_download_file(download_url, local_path):
                downloaded_files.append(local_path)
                not_downloaded = False
                time.sleep(600)  # Wait for 10 minutes before retrying again
        except Exception as e:
            print(f"Failed to download {download_url}: {e}")
            time.sleep(600)  # Wait for 10 minutes before retrying again
    # Return the paths as a materialization
    return dg.MaterializeResult(
        metadata={
            "files": downloaded_files
        },
    )