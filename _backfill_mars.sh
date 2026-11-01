#!/bin/bash
# MARS archive: keep the IFS stores (bkr/ifs/ifs_native*.icechunk and the 0.25 and 1 degree
# regrids) filled from MARS_START (2025-01-01) to yesterday, in passes.
#
# Each pass first extends the stores' time axes to cover its range (a manifest rewrite; see
# mars_icechunk.extend_time_axis). That is only safe with nothing else writing the stores,
# which holds here because the pipeline runs in the foreground between extensions -- but
# also stop any other machine ingesting into them first. Then the pipeline fills the range:
# six retrievals in flight, because the ECMWF queue limits it at first; once GRIB backs up on
# disk, the ingest does, so eight ingest workers. MEMORY_CEILING_GB is the total for them, so
# 128 is 16 GB each; they peak around 4.5 GB. Ingested timesteps are not fetched again, so a
# restart resumes.
#
# A pass that finishes (everything retrievable ingested; a file on an unavailable ECMWF tape
# is skipped and tried again next pass) waits MARS_REPEAT_SECONDS, a day, and the next pass
# moves the end up to the new yesterday. ECMWF credentials come from .env or ~/.ecmwfapirc.
#
#     setsid nohup ./_backfill_mars.sh >> ~/backfill-logs/mars_pipeline.log 2>&1 < /dev/null &
cd "$(dirname "$0")"
export PYTHONPATH=$PWD MEMORY_CEILING_GB=${MEMORY_CEILING_GB:-128}
PIXI=${PIXI:-~/.pixi/bin/pixi}
START=${MARS_START:-2025-01-01}
REPEAT=${MARS_REPEAT_SECONDS:-86400}
while true; do
  END=$(date -u -d yesterday +%F)
  echo "=== $(date -u '+%F %T') mars: extending the store time axes to $START..$END"
  if ! $PIXI run python -m planetary_datasets.providers.mars_icechunk --init-only \
        --axis-start "${START}T00" --axis-end "${END}T23" \
     || ! $PIXI run python -m planetary_datasets.providers.mars_icechunk --regrid 0.25 1 \
        --init-only; then
    echo "=== $(date -u '+%F %T') mars: extending the time axes failed; retrying in 600s"
    sleep 600
    continue
  fi
  if $PIXI run python -m planetary_datasets.providers.mars_pipeline \
      --download --download-threads 6 --ingest-workers 8 --start "$START" --end "$END"; then
    echo "=== $(date -u '+%F %T') mars: pass to $END complete; next pass in $((REPEAT / 3600)) h"
    sleep "$REPEAT"
  else
    echo "=== $(date -u '+%F %T') mars_pipeline exited $?; restarting in 600s"
    sleep 600
  fi
done
