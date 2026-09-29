# EUMETNET OPERA radar downloader image

Fetches one hour of the pan-European OPERA radar composite from the public
[Open Radar Data archive](https://eumetnet.github.io/openradardata-documentation/)
with NVIDIA's `earth2studio`, and stages it as one compressed netCDF file.

It is the download half of the OPERA pipeline. The processing half,
`planetary_datasets.providers.opera`, runs in the project environment, appends
the staged hour to its icechunk store and deletes the file. They are split
because `earth2studio` depends on `torch` and pins `netcdf4<1.7.3`.

| Product | Variables | Frames per hour | Grid | Store |
|---|---|---|---|---|
| `rainfall` | `rainfall_rate` (mm h-1), `accumulated_rainfall_1hour` (m) | 4 (15 min) | 2 km, 2200x1900 | `bkr/precipradar/opera_rainfall.icechunk` |
| `dbz` | `dbz` (dBZ) | 12 (5 min) | 1 km, 4400x3800 | `bkr/precipradar/opera_dbz.icechunk` |

Reflectivity is only on the 1 km grid from July 2024 (the CIRRUS era), so earlier
hours are refused rather than written onto the wrong grid.

## How Dagster uses it

```
radar/opera_rainfall_download  (this image)  ->  radar/opera_rainfall
radar/opera_dbz_download       (this image)  ->  radar/opera_dbz
```

The download asset first checks whether the hour is already in the store and
skips the container if so, so a backfill over stored hours costs nothing. It
mounts `OPERA_ARCHIVE_DIR` (default `<PLANETARY_DATASETS_DATA_DIR>/opera`) at
`/data/opera` and runs as the Dagster user. `OPERA_RADAR_IMAGE` overrides the
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

## Build

```bash
./build.sh                                      # planetary-datasets/opera-radar:{latest,YYYYMMDD}
REGISTRY=ghcr.io/<owner> PUSH=1 ./build.sh      # and push
```

`torch` is installed from the CPU-only index; the OPERA source never uses a GPU.
`earth2studio` is pinned to the commit the original scripts ran.

## Run by hand

```bash
docker run --rm --user "$(id -u):$(id -g)" -v /data/opera:/data/opera \
  planetary-datasets/opera-radar:latest --product dbz --time 2026-09-29T06:00
```

Output: `/data/opera/<product>/YYYY/MM/DD/opera_<product>_<YYYYMMDDhhmm>.nc`.
