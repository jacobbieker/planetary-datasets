# Geostationary virtual-reference ingest — EC2 runbook

Walks a geostationary imager archive backwards from an anchor date, splitting
it into eras that virtualizarr can actually concatenate, and writes one
Icechunk store of **virtual references** per era to Source Cooperative. No
pixel data is copied — only chunk manifests pointing back at the NOAA buckets.

Two missions are supported:

| Mission | Product | Cadence | Notes |
|---|---|---|---|
| GOES-16/17/18/19 | `ABI-L1b-RadF` (+ Reproc for 16/17) | 10 min | Several codec eras; C02 is 21696² |
| GK-2A | `AMI/L1B/FD` | 10 min | One codec era 2023→2026; `vi006` is 22000² |

Himawari is **not** supported. Its complete full-disk archive
(`AHI-L1b-FLDK`) is Himawari Standard Data — `.DAT.bz2`, a custom binary
format under whole-file bz2 — so there are no per-chunk byte ranges to
reference. The netCDF alternative (`AHI-L2-FLDK-ISatSS`) splits every scene
into 88 tiles, ~88× the per-day cost, and is calibrated imagery rather than
L1b radiance.

**Image:** `033040503982.dkr.ecr.us-west-2.amazonaws.com/goes-virtual-ingest:latest`
(also tagged `:20260926`, 284 MB compressed, **linux/amd64 only**)

## Requirements

- **x86_64 instance, not Graviton.** The image is amd64 only.
- ECR pull access to account `033040503982` in `us-west-2` (IAM user creds or
  an instance role with `AmazonEC2ContainerRegistryReadOnly`).
- Source Cooperative write keys, passed at run time. **They are not in the
  image.**
- Little disk: the container writes only logs. Stores live in S3.

## Run

```bash
# The container runs as uid 57439; a bind-mounted host directory keeps host
# ownership, so chown it before the first run or /data will be read-only.
sudo mkdir -p /mnt/goes-logs && sudo chown -R 57439:57439 /mnt/goes-logs

AWS_REGION=us-west-2
REGISTRY=033040503982.dkr.ecr.us-west-2.amazonaws.com
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $REGISTRY

docker run -d --name goes-virtual --restart unless-stopped \
  -v /mnt/goes-logs:/data \
  -e SC_ACCESS_KEY_ID=<source-coop-key> \
  -e SC_SECRET_ACCESS_KEY=<source-coop-secret> \
  -e END_DATE=2026-09-26 \
  -e MAIN_WORKERS=6 \
  -e BUDGET_GB=50 \
  $REGISTRY/goes-virtual-ingest:latest
```

`END_DATE` **must be pinned and kept the same across restarts.** It becomes the
store-name suffix for the newest era, so changing it mints a whole new set of
stores instead of resuming the existing ones.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SC_ACCESS_KEY_ID` / `SC_SECRET_ACCESS_KEY` | *required* | Source Cooperative write keys |
| `END_DATE` | today (UTC) | Anchor for the backwards walk; suffixes the newest store |
| `SATELLITES` | `goes16 goes17 goes18 goes19` | Which archives to run |
| `MAIN_WORKERS` | `3` | Parallel channels per satellite (excluding C02) |
| `OTHER_CHANNELS` | `16 15 14 13 12 11 10 9 8 7 6 5 4 3 1` | Channels for the main tier |
| `RUN_C02` | `1` | Run C02 in its own single-worker process |
| `BUDGET_GB` | `64` | Memory ceiling for the whole job |
| `SC_BUCKET` | `us-west-2.opendata.source.coop` | Destination bucket |
| `SC_PREFIX_ROOT` | `bkr/geo/virtualized` | Destination prefix |
| `BATCH_SIZE` | `1` | Days per Icechunk commit |
| `RUN_GK2A` | `0` | Set to `1` to also run GK-2A AMI full-disk |
| `GK2A_BANDS` | 15 bands (all but `vi006`) | Bands for the GK-2A main tier |
| `GK2A_WORKERS` | `$MAIN_WORKERS` | Parallel bands for GK-2A |
| `RUN_GK2A_VI006` | `1` | Run `vi006` in its own single-worker pass |

C02 (ABI) and `vi006` (AMI) always run alone: their 21696²/22000² grids carry
roughly 16× the chunk-manifest entries of a 2 km channel.

Output stores are named:
- `s3://$SC_BUCKET/$SC_PREFIX_ROOT/goes{N}_radf_C{CH}_{ERA_END_DATE}.icechunk`
- `s3://$SC_BUCKET/$SC_PREFIX_ROOT/gk2a_ami_fd_{band}_{ERA_END_DATE}.icechunk`

GK-2A stores carry `image_pixel_values(t, y, x)` as uint16 counts. The files
have no time coordinate and no x/y arrays, so `t` is reconstructed from the
`observation_start_time` attribute (seconds since J2000) and cross-checked
against the filename slot, while the fixed-grid navigation (`nav_cfac`,
`nav_coff`, `nav_lfac`, `nav_loff`, `nav_sub_longitude`, …) and a CF
`gk2a_imager_projection` grid mapping are carried per timestep so the
projection stays reconstructable as the spacecraft's INR drifts.

## Single-satellite / ad-hoc runs

Any argument passed to the container goes straight to the ingest CLI instead of
the orchestrator:

```bash
docker run --rm $REGISTRY/goes-virtual-ingest:latest \
  --satellite goes19 --channel 13 --max-eras 1 \
  --storage s3 --bucket us-west-2.opendata.source.coop \
  --prefix bkr/geo/virtualized/goes19_radf.icechunk \
  --access-key-id <key> --secret-access-key <secret> \
  --end-date 2026-09-26
```

Prefix the arguments with `gk2a` to reach the GK-2A CLI instead:

```bash
docker run --rm $REGISTRY/goes-virtual-ingest:latest gk2a \
  --band ir087 --max-eras 1 \
  --storage s3 --bucket us-west-2.opendata.source.coop \
  --prefix bkr/geo/virtualized/gk2a_ami_fd.icechunk \
  --access-key-id <key> --secret-access-key <secret> \
  --end-date 2026-09-26
```

Useful flags: `--max-eras 1` (only what still combines with the anchor),
`--start-date` (bound the walk), `--forward` (old oldest-first whole-archive
mode), `--check` (summarise the stores written).

## Monitoring

```bash
docker logs -f goes-virtual                  # launch banner + pids
tail -f /mnt/goes-logs/goes18_main.log       # per-satellite progress
tail -f /mnt/goes-logs/logs/watchdog.log     # memory against BUDGET_GB
ls     /mnt/goes-logs/logs/                  # per-channel skip/error/era records
```

Progress markers in a satellite log:
- `era anchor` — the walk has fixed the reference day for an era
- `does not combine — ...` — a day that cannot join the current era
- `Era boundary at YYYY-MM-DD` — era closed, its store is about to be written
- `##########  ... era N ... -> store suffix` — that era's ingest starting
- `Ingested GOES-NN ... for YYYY-MM-DD` — a committed day

## Restarts

Safe and resumable. Each store records its last committed `t`; a restart skips
days at or before it. Stop cleanly with `docker stop goes-virtual` — the
entrypoint traps `SIGTERM` and kills the spawned channel workers, which a plain
pattern-kill misses (they run as `python -c from multiprocessing...`, not as the
module).

## ECR retention

`ecr_lifecycle_policy.json` keeps the 3 most recent date-tagged builds (the
running one plus two rollbacks) and expires untagged manifests after 14 days.
Apply it after creating a repository in a new region:

```bash
aws ecr put-lifecycle-policy --repository-name goes-virtual-ingest \
  --region us-east-1 --lifecycle-policy-text file://ecr_lifecycle_policy.json
```

The untagged rule is safe despite every push leaving two untagged manifests
(the image and its attestation): ECR does not evaluate manifests that a live
manifest list still references. Confirmed with
`start-lifecycle-policy-preview` using `untagged / imageCountMoreThan 1`, which
expired nothing. Preview before changing these rules — it is free and does not
delete.

`build_and_push.sh` applies this policy on every push, so a repository created
in a new region picks it up automatically.

## Sizing and region

Throughput is dominated by HDF **metadata** round-trips to the NOAA buckets, not
by bandwidth — nothing is downloaded in bulk. Measured ~15–17 min per
channel-day (144 files) from a home connection.

- Roughly 112,000 channel-days across the four archives. Budget accordingly and
  scale `MAIN_WORKERS` up: on a 32-vCPU / 64 GB box, `MAIN_WORKERS=6` with
  `BUDGET_GB=50` is a reasonable start.
- **The NOAA GOES buckets are in `us-east-1`.** Since reads dominate and the
  writes are small manifests, an instance in `us-east-1` will be materially
  faster than one in `us-west-2`, at the cost of cross-region writes to Source
  Cooperative. `build_and_push.sh` pushes to both regions, so the instances
  pull from ECR in their own region rather than across one.
