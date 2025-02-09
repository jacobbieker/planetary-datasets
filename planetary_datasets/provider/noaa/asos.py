"""
Example script that scrapes data from the IEM ASOS download service.

More help on CGI parameters is available at:

    https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?help

Requires: Python 3
"""

import json
import os
import sys
import time
import urllib.error
from datetime import datetime, timedelta
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 12
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py?"


def download_data(uri):
    """Fetch the data from the IEM

    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.

    Args:
      uri (string): URL to fetch

    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print(f"download_data({uri}) failed with {exp}")
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_stations_from_filelist(filename):
    """Build a listing of stations from a simple file listing the stations.

    The file should simply have one station per line.
    """
    if not os.path.isfile(filename):
        print(f"Filename {filename} does not exist, aborting!")
        sys.exit()
    with open(filename, encoding="ascii") as fh:
        stations = [line.strip() for line in fh]
    return stations


def get_stations_from_networks():
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    states = (
        "AK AL AR AZ CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN "
        "MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT "
        "WA WI WV WY"
    )
    networks = [f"{state}_ASOS" for state in states.split()]

    for network in networks:
        # Get metadata
        uri = (
            "https://mesonet.agron.iastate.edu/"
            f"geojson/network/{network}.geojson"
        )
        data = urlopen(uri)
        jdict = json.load(data)
        for site in jdict["features"]:
            stations.append(site["properties"]["sid"])  # noqa
    return stations


def download_alldata():
    """An alternative method that fetches all available data.

    Service supports up to 24 hours worth of data at a time."""
    # timestamps in UTC to request data for
    startts = datetime(2013, 1, 1)
    endts = datetime(2025, 12, 31)
    interval = timedelta(days=31)

    service = SERVICE + "data=all&tz=Etc/UTC&format=comma&latlon=yes&"

    now = startts
    while now < endts:
        thisurl = service
        thisurl += now.strftime("year1=%Y&month1=%m&day1=%d&")
        thisurl += (now + interval).strftime("year2=%Y&month2=%m&day2=%d&")
        print(f"Downloading: {now}")
        data = download_data(thisurl)
        outfn = f"/Users/jacob/Development/asos_1min/{now:%Y%m%d}.txt"
        with open(outfn, "w", encoding="ascii") as fh:
            fh.write(data)
        now += interval

def main():
    """Our main method"""
    # timestamps in UTC to request data for
    startts = datetime(2000, 1, 1)
    endts = datetime(2025, 1, 31)
    interval = timedelta(days=1)

    # https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py?tz=UTC&year1=2025&month1=1&day1=1&hour1=0&minute1=0&year2=2025&month2=2&day2=8&hour2=0&minute2=0

    # https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py?station=AMW&vars=tmpf&sts=2022-01-01T00:00Z&ets=2023-01-01T00:00Z&sample=1hour&what=download&tz=UTC

    service = SERVICE + "tz=UTC&vars=tmpf&vars=dwpf&vars=sknt&vars=drct&vars=gust_drct&vars=gust_sknt&vars=vis1_coeff&vars=vis1_nd&vars=vis2_coeff&vars=vis2_nd&vars=vis3_coeff&vars=vis3_nd&vars=ptype&vars=precip&vars=pres1&vars=pres2&vars=pres3&sample=1min&what=download&delim=comma&gis=yes&"

    stations = get_stations_from_networks()
    now = endts
    while now > startts:
        thisurl = service
        thisurl += (now - interval).strftime("year1=%Y&month1=%m&day1=%d&")
        thisurl += now.strftime("year2=%Y&month2=%m&day2=%d&")
        for station in stations:
            uri = f"{thisurl}&station={station}"
            print(f"Downloading: {station}: {(now-interval):%Y%m%d%H%M}_{now:%Y%m%d%H%M}, URL: {uri}")
            try:
                data = download_data(uri)
                outfn = f"/Users/jacob/Development/asos_1min/{station}_{(now-interval):%Y%m%d%H%M}_{now:%Y%m%d%H%M}.txt"
                with open(outfn, "w", encoding="ascii") as fh:
                    fh.write(data)
                print("Download successful")
            except urllib.error.URLError as e:
                print(e)
                continue
        now -= interval


if __name__ == "__main__":
    main()
    #download_alldata()