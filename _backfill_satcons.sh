#!/bin/bash
# Fill and keep current one satellite-consumer store, month by month, NEWEST to OLDEST, then
# walk again. satellite-consumer skips timesteps the store already holds, so a complete month
# costs a listing and gaps anywhere in the range are filled. Unlike the Himawari lanes this
# writes one store, so there is one lane per store (a store takes one writer).
#
#     ./_backfill_satcons.sh <satellite> <res m> <store name> <oldest YYYYMM> [newest YYYYMM]
#
# Newest defaults to the current month (re-evaluated every walk), so the lane also keeps the
# store up to date. Needs ~/satellite-consumer and the AWS/EUMETSAT keys in .env.
cd "$(dirname "$0")"
SAT=${1:?satellite, e.g. odegree-12}
RES=${2:?resolution in metres}
STORE=${3:?store name, e.g. mtg_1000m}
OLDEST=${4:?oldest month YYYYMM}
FIXED_NEWEST=${5:-}
PIXI=${PIXI:-~/.pixi/bin/pixi}
CONSUMER=${SATCONS_DIR:-~/satellite-consumer}
SCRATCH=$PWD/data/satcons_raw/$STORE
LISTINGS=$PWD/data/satcons_listing_cache
set -a; source .env; set +a
mkdir -p "$SCRATCH" "$LISTINGS"
target="s3://us-west-2.opendata.source.coop/bkr/geo/${STORE}.icechunk"
# Low-memory mode (one worker) for 500 m unless the wrapper says otherwise; SATCONS_MAX_WORKERS
# and SATCONS_BUFFER_SIZE pass straight through to satellite-consumer when set.
low=${SATCONS_LOW_MEMORY:-false}; [[ -z ${SATCONS_LOW_MEMORY:-} && $RES == 500 ]] && low=true

while true; do
  month=${FIXED_NEWEST:-$(date -u '+%Y%m')}
  while (( month >= OLDEST )); do
    y=${month:0:4}; m=${month:4:2}
    start="$y-$m-01T00:00:00Z"
    end=$(date -u -d "$y-$m-01 +1 month" '+%Y-%m-%dT00:00:00Z')
    # The current month only up to two hours ago, so a half-published slot is not stored.
    now_edge=$(date -u -d '-2 hours' '+%Y-%m-%dT%H:00:00Z')
    [[ $end > $now_edge ]] && end=$now_edge
    echo "=== $(date -u '+%F %T') $STORE $month start -> $target"
    # The store's readable variable names (planetary_datasets/naming), for satellite-consumer.
    renames="$PWD/data/satcons_names_${STORE}.json"
    PYTHONPATH=$PWD $PIXI run python scripts/store_names_json.py "bkr/geo/${STORE}.icechunk" "$renames"
    (cd "$SCRATCH" && SATCONS_SATELLITE=$SAT SATCONS_RESOLUTION=$RES SATCONS_RENAME_JSON=$renames \
      SATCONS_START_TIMESTAMP=$start SATCONS_END_TIMESTAMP=$end SATCONS_RAW_PATH=. \
      SATCONS_ZARR_PATH=$target SATCONS_ICECHUNK=True SATCONS_LOW_MEMORY=$low \
      SATCONS_S3_LISTING_CACHE_DIR=$LISTINGS \
      $PIXI run --manifest-path "$CONSUMER/pyproject.toml" sat-consumer)
    echo "=== $(date -u '+%F %T') $STORE $month exited $?"
    find "$SCRATCH" -mindepth 1 -delete
    month=$(date -u -d "$y-$m-01 -1 month" '+%Y%m')
  done
  echo "=== $(date -u '+%F %T') $STORE reached $OLDEST; walking again in an hour"
  sleep 3600
done
