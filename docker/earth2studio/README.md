# earth2studio downloader image

One image for every data source this project reads through NVIDIA's
[earth2studio](https://github.com/NVIDIA/earth2studio): the OPERA radar
composites and the catalogue's
[direct observation](https://nvidia.github.io/earth2studio/main/userguide/about/catalog/?tab=data&data_class=Direct+Observations)
sources. earth2studio needs `torch` and `netcdf4<1.7.3`, which the project
environment cannot take, so it runs here and only stages files; publishing to
the stores happens in the project environment.

```bash
docker run ... planetary-datasets/earth2studio:latest opera --product dbz --time 2026-09-29T06:00
docker run ... planetary-datasets/earth2studio:latest obs --dataset iem_asos --time 2026-09-28T00:00
```

`torch` is installed from the CPU-only index, and earth2studio (with its `data`
extra) is pinned to a commit. `EARTH2STUDIO_IMAGE` overrides the tag the Dagster
assets run.

## Build

```bash
./build.sh                                      # planetary-datasets/earth2studio:{latest,YYYYMMDD}
REGISTRY=ghcr.io/<owner> PUSH=1 ./build.sh      # and push
```

## Direct observations

`planetary_datasets/providers/earth2studio_download.py` holds one `Dataset`
entry per stored product: 62 datasets covering all 30 observation sources.
Each entry becomes two Dagster assets in `dags/assets/earth2studio_obs.py`:

```
earth2studio_obs/<name>_download   (this image, via PipesDockerClient)
    -> earth2studio_obs/<name>     (publishes, then deletes the staging)
```

- **DataFrame sources** are cast to the source's own `SCHEMA` and published as a
  partitioned Parquet dataset, `bkr/obs/<name>.parquet/date=YYYY-MM-DD/part-<stamp>.parquet`.
  Empty partitions are still written, so a quiet hour is not fetched again.
- **Gridded sources** are staged in batches of frames and appended to
  `bkr/obs/<name>.icechunk` one batch at a time. Static 2-D
  latitude/longitude is written once, not on every append.
- **Swaths** (VIIRS, Sentinel-3 SYNERGY) are listed first, requested granule by
  granule, and stacked along `time` with per-granule latitude/longitude.

Staging lives under `E2S_OBS_ARCHIVE_DIR` (default `<data_dir>/earth2studio`),
mounted at `/data/earth2studio`. A partition is staged once its JSON manifest
exists; the manifest is written last.

The MetOp, MTG LI and MTG FCI datasets need `EUMETSAT_CONSUMER_KEY` and
`EUMETSAT_CONSUMER_SECRET`; the asset passes only those to only those
containers. Planetary Computer sources use `PC_SDK_SUBSCRIPTION_KEY` when it is
set. Everything else is anonymous.

Datasets marked `manual` are left out of the scheduled jobs (tag
`planetary/schedule: manual`): every partition is heavy, so they are meant to
be backfilled deliberately.

| Dataset | earth2studio source | Stored as | Partition | From | Scheduled |
|---|---|---|---|---|---|
| `nomads_gdas_conv` | `NomadsGDASObsConv` | Parquet | 6h | 2026-09-28 | yes |
| `nnja_conv` | `NNJAObsConv` | Parquet | 1D | 1979-01-01 | yes |
| `nnja_satwnd` | `NNJAObsSatwnd` | Parquet | 6h | 1979-01-01 | manual |
| `ufs_conv` | `UFSObsConv` | Parquet | 1D | 1980-01-01 | yes |
| `ufs_sat` | `UFSObsSat` | Parquet | 1D | 1980-01-01 | manual |
| `ghcn_daily` | `GHCNDaily` | Parquet | 1MS | 1750-01-01 | yes |
| `ghcn_hourly` | `GHCNHourly` | Parquet | 1YS | 1901-01-01 | manual |
| `isd_earth2studio` | `ISD` | Parquet | 1YS | 1901-01-01 | manual |
| `iem_asos` | `IEM_ASOS` | Parquet | 1D | 1928-01-01 | yes |
| `ibtracs` | `IBTrACS` | Parquet | 1YS | 1842-01-01 | yes |
| `goes_glm_east_event` | `GOESGLM` | Parquet | 1h | 2018-02-13 | yes |
| `goes_glm_east_group` | `GOESGLM` | Parquet | 1h | 2018-02-13 | yes |
| `goes_glm_east_flash` | `GOESGLM` | Parquet | 1h | 2018-02-13 | yes |
| `goes_glm_west_event` | `GOESGLM` | Parquet | 1h | 2018-12-10 | yes |
| `goes_glm_west_group` | `GOESGLM` | Parquet | 1h | 2018-12-10 | yes |
| `goes_glm_west_flash` | `GOESGLM` | Parquet | 1h | 2018-12-10 | yes |
| `meteosat_li_flash` | `MeteosatLI` | Parquet | 1h | 2024-07-04 | yes |
| `meteosat_li_group` | `MeteosatLI` | Parquet | 1h | 2024-07-04 | yes |
| `meteosat_li_event` | `MeteosatLI` | Parquet | 1h | 2024-07-04 | yes |
| `jpss_atms` | `JPSS_ATMS` | Parquet | 1h | 2023-09-06 | yes |
| `jpss_cris_n20` | `JPSS_CRIS` | Parquet | 10min | 2023-09-06 | manual |
| `jpss_cris_n21` | `JPSS_CRIS` | Parquet | 10min | 2023-09-06 | manual |
| `jpss_cris_npp` | `JPSS_CRIS` | Parquet | 10min | 2023-09-06 | manual |
| `metop_amsua` | `MetOpAMSUA` | Parquet | 1D | 2007-06-01 | yes |
| `metop_mhs` | `MetOpMHS` | Parquet | 1D | 2007-06-01 | yes |
| `metop_avhrr_a` | `MetOpAVHRR` | Parquet | 1h | 2007-06-01 | manual |
| `metop_avhrr_b` | `MetOpAVHRR` | Parquet | 1h | 2013-01-01 | manual |
| `metop_avhrr_c` | `MetOpAVHRR` | Parquet | 1h | 2019-01-01 | manual |
| `metop_iasi_a` | `MetOpIASI` | Parquet | 1h | 2007-06-01 | manual |
| `metop_iasi_b` | `MetOpIASI` | Parquet | 1h | 2013-01-01 | manual |
| `metop_iasi_c` | `MetOpIASI` | Parquet | 1h | 2019-01-01 | manual |
| `nnja_sat_amsua` | `NNJAObsSat` | Parquet | 1h | 1998-01-01 | manual |
| `nnja_sat_amsub` | `NNJAObsSat` | Parquet | 1h | 1998-01-01 | manual |
| `nnja_sat_mhs` | `NNJAObsSat` | Parquet | 1h | 2005-01-01 | manual |
| `nnja_sat_atms` | `NNJAObsSat` | Parquet | 1h | 2012-01-01 | manual |
| `nnja_sat_airs` | `NNJAObsSat` | Parquet | 1h | 2002-01-01 | manual |
| `nnja_sat_iasi` | `NNJAObsSat` | Parquet | 1h | 2008-01-01 | manual |
| `nnja_sat_cris` | `NNJAObsSat` | Parquet | 1h | 2018-01-01 | manual |
| `goes_east_fd` | `GOES` | Icechunk | 1h | 2019-04-02 | manual |
| `goes_east_conus` | `GOES` | Icechunk | 1h | 2017-12-18 | yes |
| `goes_west_fd` | `GOES` | Icechunk | 1h | 2019-02-12 | manual |
| `goes_west_pacus` | `GOES` | Icechunk | 1h | 2019-02-12 | yes |
| `pc_goes_east_fd` | `PlanetaryComputerGOES` | Icechunk | 1h | 2019-04-02 | manual |
| `pc_goes_east_conus` | `PlanetaryComputerGOES` | Icechunk | 1h | 2017-12-18 | manual |
| `pc_goes_west_fd` | `PlanetaryComputerGOES` | Icechunk | 1h | 2019-02-12 | manual |
| `pc_goes_west_pacus` | `PlanetaryComputerGOES` | Icechunk | 1h | 2019-02-12 | manual |
| `goes_glm_grid_east` | `GOESGLMGrid` | Icechunk | 1h | 2018-02-13 | yes |
| `goes_glm_grid_west` | `GOESGLMGrid` | Icechunk | 1h | 2018-12-10 | yes |
| `himawari_ahi_fd` | `HimawariAHI` | Icechunk | 1h | 2015-07-07 | manual |
| `meteosat_fci_2km` | `MeteosatFCI` | Icechunk | 1h | 2024-01-16 | manual |
| `mrms_conus` | `MRMS` | Icechunk | 1h | 2020-10-14 | yes |
| `nclimgrid_daily` | `NClimGridDaily` | Icechunk | 1MS | 1951-01-01 | yes |
| `pc_oisst` | `PlanetaryComputerOISST` | Icechunk | 1MS | 1981-09-01 | yes |
| `pc_modis_fire_mod` | `PlanetaryComputerMODISFire` | Icechunk | 1D | 2000-02-24 | yes |
| `pc_modis_fire_myd` | `PlanetaryComputerMODISFire` | Icechunk | 1D | 2002-07-04 | yes |
| `jpss_viirs_noaa20_i` | `JPSS` | Icechunk (granule stack) | 1h | 2018-01-05 | manual |
| `jpss_viirs_noaa20_m` | `JPSS` | Icechunk (granule stack) | 1h | 2018-01-05 | manual |
| `jpss_viirs_noaa21_i` | `JPSS` | Icechunk (granule stack) | 1h | 2023-02-10 | manual |
| `jpss_viirs_noaa21_m` | `JPSS` | Icechunk (granule stack) | 1h | 2023-02-10 | manual |
| `jpss_viirs_snpp_i` | `JPSS` | Icechunk (granule stack) | 1h | 2012-01-19 | manual |
| `jpss_viirs_snpp_m` | `JPSS` | Icechunk (granule stack) | 1h | 2012-01-19 | manual |
| `pc_sentinel3_aod` | `PlanetaryComputerSentinel3AOD` | Icechunk (granule stack) | 1D | 2020-04-16 | yes |

### Quirks worked around

These are earth2studio behaviours found while building this; each is handled in
the registry or the downloader rather than patched upstream.

- **Inclusive windows with sub-second times.** Each partition is requested with
  the upper tolerance equal to its full length, then cut to `[start, end)`.
- **Files that start before the window** (GLM, ATMS, CrIS) carry the
  partition's first observations; `lead` widens the request below the start.
- **GDAS/NNJA/UFS cycle files cover `[T-3h, T+3h)`**, which their planners do
  not assume. Hourly partitions come back empty, so these use daily or
  cycle-aligned 6-hour partitions, which the planners handle correctly.
- **NOMADS keeps two days** of GDAS; `nomads_gdas_conv` cannot backfill. Use
  `nnja_conv` for history.
- **`get_stations_bbox` returns only the eastern hemisphere** for a global box;
  station lists are read from the metadata directly.
- **Sentinel-3 AOD** returns the newest granule within ±12 h, not the one asked
  for. A subclass narrows the search and picks the nearest granule.
- **MODIS Fire** returns FireMask for every variable, so only `fmask` is
  stored. It also switches silently from Terra to Aqua, so the platform is
  pinned per store through the item-id filter.
- **OISST and MODIS** must be requested at 00:00. From 12:00 the next day's
  item matches first.
- **GOES-16 full disk ran every 15 minutes until 2019-04-02**, so the 10-minute
  full-disk stores start then instead of filling with duplicated frames.
- **MRMS** fills a missing frame from a neighbour up to 10 minutes away; it is
  limited to 1 minute.
- **`async_timeout` bounds download and decode together**, so it is raised
  well above the 600 s default.
- **Several sources share a fixed temporary cache directory**, so each
  container gets a private `EARTH2STUDIO_CACHE`.

## OPERA

Fetches one hour of the pan-European OPERA radar composite from the public
[Open Radar Data archive](https://eumetnet.github.io/openradardata-documentation/)
and stages it as one compressed netCDF file. The processing half,
`planetary_datasets.providers.opera`, appends the staged hour to its icechunk
store and deletes the file.

| Product | Variables | Frames per hour | Grid | Store |
|---|---|---|---|---|
| `rainfall` | `rainfall_rate` (mm h-1), `accumulated_rainfall_1hour` (m) | 4 (15 min) | 2 km, 2200x1900 | `bkr/precipradar/opera_rainfall.icechunk` |
| `dbz` | `dbz` (dBZ) | 12 (5 min) | 1 km, 4400x3800 | `bkr/precipradar/opera_dbz.icechunk` |

Reflectivity is only on the 1 km grid from July 2024 (the CIRRUS era), so earlier
hours are refused rather than written onto the wrong grid.

### How Dagster uses it

```
radar/opera_rainfall_download  (this image)  ->  radar/opera_rainfall
radar/opera_dbz_download       (this image)  ->  radar/opera_dbz
```

The download asset first checks whether the hour is already in the store and
skips the container if so, so a backfill over stored hours costs nothing. It
mounts `OPERA_ARCHIVE_DIR` (default `<PLANETARY_DATASETS_DATA_DIR>/opera`) at
`/data/opera` and runs as the Dagster user. `EARTH2STUDIO_IMAGE` overrides the
image tag.

Partitions start where the existing stores do (rainfall 2018-01-01, reflectivity
2025-01-01): the store only accepts appends in time order.

That also means hours before a store's latest time cannot be filled. Both
existing stores were written out of order by the original scripts (rainfall runs
to 2026-07-06 18:45 but has gaps back to 2018; reflectivity to 2026-07-23 00:55),
so the download asset skips any hour before the store's end with a warning
rather than staging a file the writer would reject.

An hour with any frame missing fails, and is retried three times (30, 60,
120 min). `--allow-partial` stages what is there instead.


### Run by hand

```bash
docker run --rm --user "$(id -u):$(id -g)" -v /data/opera:/data/opera \
  planetary-datasets/earth2studio:latest opera --product dbz --time 2026-09-29T06:00
```

Output: `/data/opera/<product>/YYYY/MM/DD/opera_<product>_<YYYYMMDDhhmm>.nc`.
