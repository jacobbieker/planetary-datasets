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
stores instead of resuming the existing ones. The Dagster assets in
`dags/assets/goes_virtual.py` read the same anchor from
`GOES_VIRTUAL_END_DATE`, or from a `goes_virtual/end_date` run tag, and warn
when neither is set.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `SC_ACCESS_KEY_ID` / `SC_SECRET_ACCESS_KEY` | *required* | Source Cooperative write keys. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are used when these are unset |
| `SC_BUCKET` | `ICECHUNK_BUCKET`, else `us-west-2.opendata.source.coop` | Destination bucket |
| `ICECHUNK_ENDPOINT_URL` | unset | Set to `https://data.source.coop` to address source.coop by its own endpoint (`SC_BUCKET=bkr`) rather than the AWS-hosted dotted bucket |
| `SC_PREFIX_ROOT` | `GOES_STORE_ROOT`, else `bkr/geo/virtualized` | Store prefix root |
| `END_DATE` | today (UTC) | Anchor for the backwards walk; suffixes the newest store |
| `SATELLITES` | `goes16 goes17 goes18 goes19` | Which archives to run |
| `MAIN_WORKERS` | `3` | Parallel channels per satellite (excluding C02) |
| `OTHER_CHANNELS` | `16 15 14 13 12 11 10 9 8 7 6 5 4 3 1` | Channels for the main tier |
| `RUN_C02` | `1` | Run C02 in its own single-worker process |
| `BUDGET_GB` | `MEMORY_CEILING_GB`, else `64` | Memory ceiling for the whole job |
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

The GOES CLI reads its destination and credentials from the shared
`planetary_datasets` config, so nothing secret goes on the command line where
`ps` can read it:

```bash
docker run --rm \
  -e ICECHUNK_BUCKET=us-west-2.opendata.source.coop \
  -e AWS_REGION=us-west-2 \
  -e AWS_ACCESS_KEY_ID=<source-coop-key> \
  -e AWS_SECRET_ACCESS_KEY=<source-coop-secret> \
  $REGISTRY/goes-virtual-ingest:latest \
  --satellite goes19 --channel 13 --max-eras 1 \
  --end-date 2026-09-26
```

Set `ICECHUNK_LOCAL_PATH` instead to write the whole run to a mounted
directory, which is how the smoke runs are done. `--storage s3` with explicit
`--bucket` / `--access-key-id` still works for ad-hoc use.

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

## Live append (keeping the stores current)

The backwards walk fills the archive a day at a time. To keep the live stores
current with sub-day latency, a scheduler (the operational Dagster) runs the
`append` subcommand every 30 minutes or so:

```bash
docker run --rm \
  -e SC_ACCESS_KEY_ID=<source-coop-key> \
  -e SC_SECRET_ACCESS_KEY=<source-coop-secret> \
  $REGISTRY/goes-virtual-ingest:latest \
  append goes19 --channels all --lookback-minutes 180
```

`<satellite>` is `goes16`–`goes19`. `--channels` takes `C01`..`C16` (comma or
space separated), bare numbers, or `all`, which includes C02. C02's manifest is
~16× a 2 km channel's, so a scheduler may give it its own job:
`append goes19 --channels C02 --lookback-minutes 180`.

For each channel it reads the last committed `t` of the **live** store
(`$SC_PREFIX_ROOT/goes{N}_radf_C{CH}.icechunk`, no era suffix), lists NOAA's
hour directories (`ABI-L1b-RadF/YYYY/DDD/HH/`) from that time through now —
never further back than `--lookback-minutes`, and across midnight where the
window crosses it — and appends every scan strictly newer than the last commit,
in one commit per channel. A commit lost to a concurrent writer is retried from
a fresh session; each retry re-reads the store, so nothing is written twice.

Before writing, every new scan is checked against the scan the store ends with,
using the same combine test as the backwards walk:

- an odd scan between good ones is skipped (`SKIPPED` in the channel log);
- fewer than 3 trailing scans that do not combine are left for the next run
  (`DEFERRED`), as one bad file must not split an era;
- 3 or more that do not combine with the store but do with each other are a new
  codec era. The live store is frozen as `..._C{CH}_{LAST_DAY}.icechunk` and a
  new live store started (`CODEC_CHANGE`). It refuses rather than rename onto a
  store that holds data, and puts the old live store back if the new era's
  first write fails. Only the config storage (this entrypoint, or
  `--storage config`) rolls over; explicit `--storage s3`/`local` fails the
  channel with `CodecEraChange` instead;
- a scan that will not open is held back, with everything after it, for the
  next run, rather than written past.

Icechunk only appends along `t`, so a scan NOAA publishes *after* a newer one
has been appended can no longer be added; it is left out.

Exit status and output:

- exit 0 when nothing is new, or when at least one channel succeeded;
- non-zero only when every channel failed (a failed channel never stops the
  rest);
- the last line on stdout is a JSON summary:
  `{"satellite": "goes19", "channels": {"C13": {"appended": 3, "last": "2026-10-06T13:55:07.161776"}, "C02": {"appended": 0, "last": null, "error": "..."}}}`.
  `dropped`, `deferred` and `new_era` appear on a channel when they apply.

The job is meant to run next to the NOAA buckets in `us-east-1`, while the
stores are in `us-west-2`: the store region is `SC_REGION`, else `us-west-2`,
and deliberately not the task's own `AWS_REGION`. NOAA reads are anonymous.
Set `ICECHUNK_LOCAL_PATH` to append to local stores instead. The same mode is
`--append-latest --lookback-minutes N` on the GOES CLI.

### GK-2A live append

`append <satellite>` keeps the live (un-suffixed) stores current between
backfills. It is what the operational Dagster schedule runs every 30 minutes:

```bash
docker run --rm \
  -e SC_ACCESS_KEY_ID=<source-coop-key> -e SC_SECRET_ACCESS_KEY=<source-coop-secret> \
  $REGISTRY/goes-virtual-ingest:latest \
  append gk2a --channels all --lookback-minutes 180
```

- `--channels` takes AMI band names, comma-separated (`ir105,wv063`), or `all`
  for all 16 including `vi006`. `vi006` can run on its own (`--channels vi006`).
- For each band it reads the newest committed `t` from
  `$SC_PREFIX_ROOT/gk2a_ami_fd_<band>.icechunk` (root group), lists only the
  `AMI/L1B/FD/YYYYMM/DD/HH/` directories from there (no further back than the
  lookback) to now, across midnight if need be, and appends the strictly newer
  scans in one commit. The codec check, the uncompressed-file filter and the
  codec-outlier repair are the backfill's; a codec change fails the band
  rather than mixing eras in one store.
- A commit that loses a race with another writer is retried from a fresh
  session (three attempts).
- A store whose last `t` is older than the lookback is still appended to
  from the window on, but the skipped stretch is reported as
  `"gap": {"from": ..., "to": ...}` in that band's entry (and logged as a
  warning): the store only appends along `t`, so it cannot be filled later.
- A missing live store fails that band: seeding one from a few hours of data
  would block the backfill. Pass `--create-missing` to start one anyway.
- The last stdout line is a JSON summary:
  `{"satellite": "gk2a", "channels": {"ir105": {"appended": 3, "last": "2026-10-06T13:40:32"}, ...}, "errors": {}}`.
  A failed band carries an `error` in its entry and in `errors`; the others
  still run. The exit status is non-zero only when every band failed, and 0
  when there was simply nothing new.
- Credentials and destination come from the same `SC_*` / `AWS_*` /
  `ICECHUNK_BUCKET` fallbacks as the orchestrated mode. NOAA is read
  anonymously. `ICECHUNK_LOCAL_PATH` sends the writes to local disk instead.

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
