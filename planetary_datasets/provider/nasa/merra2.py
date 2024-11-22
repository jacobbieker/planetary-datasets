import contextlib
import os
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth


class MERRA2Downloader:
    """
    A class to download MERRA2 data collections from NASA GES DISC.
    Requires a .netrc file with NASA EarthData login credentials.
    """

    def __init__(self, gesdisc_username: str, gesdisc_password: str) -> None:
        self.base_url = "https://goldsmr4.gesdisc.eosdis.nasa.gov/data/MERRA2"
        self.collection_id = "400"  # MERRA2 collection ID

        # Map collection names to their corresponding stream IDs and variables
        self.collections = {
            "tavg1_2d_slv_Nx": {
                "stream": "M2T1NXSLV.5.12.4",
                "variables": ["PS", "QV2M", "SLP", "T2M", "TQI", "TQL", "TQV", "TS", "U10M", "V10M"],
            },
            "tavg1_2d_lnd_Nx": {"stream": "M2T1NXLND.5.12.4", "variables": ["GWETROOT", "LAI"]},
            "inst3_3d_asm_Np": {
                "stream": "M2I3NXGAS.5.12.4",
                "variables": ["CLOUD", "H", "OMEGA", "QI", "QL", "QV", "T", "U", "V"],
            },
            "inst3_3d_asm_Nv": {"stream": "M2I3NXGAS.5.12.4", "variables": ["PL"]},
            "const_2d_asm_Nx": {"stream": "M2C0NXASM.5.12.4", "variables": ["FRLAND", "FROCEAN", "PHIS"]},
            "const_2d_ctm_Nx": {"stream": "M2C0NXCTM.5.12.4", "variables": ["FRACI"]},
            "tavg1_2d_flx_Nx": {"stream": "M2T1NXFLX.5.12.4", "variables": ["EFLUX", "HFLUX", "Z0M"]},
            "tavg1_2d_rad_Nx": {
                "stream": "M2T1NXRAD.5.12.4",
                "variables": ["LWGAB", "LWGEM", "LWTUP", "SWGNT", "SWTNT"],
            },
        }

        self.auth = HTTPBasicAuth(username=gesdisc_username, password=gesdisc_password)

    def generate_url(self, collection, date):
        """Generate URL for MERRA2 data file."""
        year = date.year
        month = date.month
        day = date.day

        stream = self.collections[collection]["stream"]

        # Handle different collection types
        if collection.startswith("const"):
            # Constant collections have a different format
            filename = f"MERRA2_{self.collection_id}.{collection}.00000000.nc4"
        else:
            # Regular collections - using the collection name in the filename
            filename = f"MERRA2_{self.collection_id}.{collection}.{year}{month:02d}{day:02d}.nc4"

        return f"{self.base_url}/{stream}/{year}/{month:02d}/{filename}"

    def download_file(self, url, output_dir):
        """Download a single file from NASA GES DISC."""
        filename = url.split("/")[-1]
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(output_path):
            return

        response = requests.get(url, auth=self.auth, stream=True)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    def download_collection(self, collection, start_date, end_date, output_dir):
        """Download a collection for a specified date range."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if collection not in self.collections:
            raise ValueError(f"Unknown collection: {collection}")

        current_date = start_date
        while current_date <= end_date:
            url = self.generate_url(collection, current_date)
            print(url)
            with contextlib.suppress(requests.exceptions.RequestException):
                self.download_file(url, output_dir)

            # Increment date unless it's a constant collection
            if not collection.startswith("const"):
                current_date += timedelta(days=1)
            else:
                break  # Only need to download constant collections once

    def download_all_collections(self, start_date, end_date, base_output_dir):
        """Download all collections for a specified date range."""
        for collection in self.collections:
            output_dir = os.path.join(base_output_dir, collection)
            self.download_collection(collection, start_date, end_date, output_dir)


# Example usage
if __name__ == "__main__":
    # Initialize downloader
    downloader = MERRA2Downloader(gesdisc_username="jbieker", gesdisc_password="4H7xD9b5ytEaK")

    # Set date range
    start_date = datetime(2024, 10, 1)
    end_date = datetime(2024, 10, 1)

    # Set output directory
    output_base_dir = "/Volumes/T7 Shield/MERRA2/"

    # Download all collections
    downloader.download_all_collections(start_date, end_date, output_base_dir)
