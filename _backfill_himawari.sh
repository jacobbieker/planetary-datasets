#!/bin/bash
# Backfill Himawari AHI (real L1b data, via satellite-consumer) into monthly shards,
# bkr/geo/himawari_<res>m_backfill_<YYYYMM>.icechunk, walking from NEWEST to OLDEST month.
# satellite-consumer skips timesteps a shard already holds, so a month is resumable and a
# finished one costs only a listing. Himawari-8 before 2022-12-13, Himawari-9 after; the
# consumer picks the bucket by date.
#
#     ./_backfill_himawari.sh <res m: 500|1000|2000> <newest YYYYMM> <oldest YYYYMM> \
#         [<lane> <lanes>]
#
# With <lanes> > 1, this lane takes every <lanes>-th month starting at offset <lane>, so
# several lanes can share a resolution without ever touching the same shard. One process
# is ~1.7 CPUs and ~9 GB at 2000m and takes about a day and a half per month.
#
# Needs ~/satellite-consumer (pixi env installed) and AWS/EUMETSAT keys in .env.
cd "$(dirname "$0")"
RES=${1:?resolution in metres}
NEWEST=${2:?newest month YYYYMM}
OLDEST=${3:?oldest month YYYYMM}
LANE=${4:-0}
LANES=${5:-1}
PIXI=${PIXI:-~/.pixi/bin/pixi}
CONSUMER=${SATCONS_DIR:-~/satellite-consumer}
SCRATCH=$PWD/data/himawari_raw/$RES-${4:-0}
LISTINGS=$PWD/data/himawari_listing_cache
set -a; source .env; set +a
mkdir -p "$SCRATCH" "$LISTINGS"

month=$NEWEST
index=0
while (( month >= OLDEST )); do
  y=${month:0:4}; m=${month:4:2}
  if (( index++ % LANES != LANE )); then
    month=$(date -u -d "$y-$m-01 -1 month" '+%Y%m')
    continue
  fi
  start="$y-$m-01T00:00:00Z"
  end=$(date -u -d "$y-$m-01 +1 month" '+%Y-%m-%dT00:00:00Z')
  store="s3://us-west-2.opendata.source.coop/bkr/geo/himawari_${RES}m_backfill_${month}.icechunk"
  low=false; [[ $RES == 500 ]] && low=true
  echo "=== $(date -u '+%F %T') himawari ${RES}m $month (lane $LANE/$LANES) start -> $store"
  (cd "$SCRATCH" && SATCONS_SATELLITE=himawari SATCONS_RESOLUTION=$RES \
    SATCONS_START_TIMESTAMP=$start SATCONS_END_TIMESTAMP=$end SATCONS_RAW_PATH=. \
    SATCONS_ZARR_PATH=$store SATCONS_ICECHUNK=True SATCONS_LOW_MEMORY=$low \
    SATCONS_S3_LISTING_CACHE_DIR=$LISTINGS \
    $PIXI run --manifest-path "$CONSUMER/pyproject.toml" sat-consumer)
  echo "=== $(date -u '+%F %T') himawari ${RES}m $month exited $?"
  find "$SCRATCH" -mindepth 1 -delete
  month=$(date -u -d "$y-$m-01 -1 month" '+%Y%m')
done
echo "=== $(date -u '+%F %T') himawari ${RES}m lane $LANE/$LANES reached $OLDEST; idle"
sleep infinity
