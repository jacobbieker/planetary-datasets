# MeteoSwiss KENDA-CH1 downloader image

Downloads one hour of the MeteoSwiss KENDA-CH1 1 km analysis from the
[Open Government Data STAC API](https://data.geo.admin.ch/api/stac/v1) into a
local archive, as GRIB2 plus the horizontal/vertical constants.

It is the download half of the KENDA pipeline. The processing half,
`planetary_datasets.providers.kenda`, runs in the project environment and reads
the same directory. They are split because MeteoSwiss's `meteodata-lab` pins
`numpy<2.4`, `pandas<3` and `eccodes==2.47.0`, which the project environment
cannot satisfy.

## How Dagster uses it

```
nwp/kenda_download  (this image, via PipesDockerClient)
   ├── nwp/kenda_analysis   (step 0 -> bkr/dmi/kenda_switzerland.icechunk)
   └── nwp/kenda_forecast   (step 1 -> bkr/dmi/kenda_forecast_switzerland.icechunk)
```

All three share the hourly `kenda_partitions`, so the scheduled job for them runs
the download first. The asset mounts `<PLANETARY_DATASETS_DATA_DIR>/meteoswiss`
at `/data/meteoswiss` and runs the container as the Dagster user, so the files
stay owned by that user. `METEOSWISS_KENDA_IMAGE` overrides the image tag.

An hour that is not fully published fails, and the asset retries it four times
(15, 30, 60, 120 min). Staging a partial hour would let the processing assets
commit a subset of variables, which then fixes the store's schema to that subset.

## Build

```bash
./build.sh                                      # planetary-datasets/meteoswiss-kenda:{latest,YYYYMMDD}
REGISTRY=ghcr.io/<owner> PUSH=1 ./build.sh      # and push
```

## Run by hand

```bash
docker run --rm --user "$(id -u):$(id -g)" -v /data/meteoswiss:/data/meteoswiss \
  planetary-datasets/meteoswiss-kenda:latest --ref-time 2026-09-29T06:00
```

| Flag | Default | Meaning |
|---|---|---|
| `--ref-time` | required | Analysis hour, UTC |
| `--target` | `$KENDA_ARCHIVE_DIR` (`/data/meteoswiss`) | Archive directory |
| `--steps` | `0 1` | Analysis (0) and/or +1 h forecast (1) |
| `--variables` | all | Restrict to some variables |
| `--allow-partial` | off | Exit 0 even when variables are missing |

## Behaviour

- Step 0 carries the 18 instantaneous fields and step 1 the 20 accumulated/flux
  fields; each variable is only requested at the step that publishes it.
- A file already on disk with a matching `.sha256` sidecar is never looked up
  again, so hours that have aged out of the API (it keeps about a day) still
  re-run cleanly. The sidecars use `meteodata-lab`'s naming, so an archive it
  populated is recognised.
- The constants files are republished in place, so they are checked against the
  server's checksum on every run and replaced when it changes.
- Downloads go to a `.part` file and are renamed into place only after the
  checksum verifies.
