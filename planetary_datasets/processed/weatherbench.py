"""Access prepared ERA5, IFS, etc. from WeatherBench2, already in Zarr format"""

import xarray as xr
import gcsfs
import fsspec

