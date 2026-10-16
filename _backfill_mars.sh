#!/bin/bash
# MARS archive: fill the timesteps missing from 2026-06-01 to the end of the IFS stores'
# time axis (bkr/ifs/ifs_native*.icechunk). Six retrievals in flight at once, because the
# ECMWF queue rather than the ingest is what limits it. MEMORY_CEILING_GB is the total for
# the three ingest workers, so 48 is 16 GB each; they peak around 4.5 GB.
#
# Restarts after a failure, which resumes: ingested timesteps are not fetched again. Stops
# once the range is complete. ECMWF credentials come from .env or ~/.ecmwfapirc.
#
#     setsid nohup ./_backfill_mars.sh >> ~/backfill-logs/mars_pipeline.log 2>&1 < /dev/null &
cd "$(dirname "$0")"
export PYTHONPATH=$PWD MEMORY_CEILING_GB=${MEMORY_CEILING_GB:-48}
PIXI=${PIXI:-~/.pixi/bin/pixi}
until $PIXI run python -m planetary_datasets.providers.mars_pipeline \
    --download --download-threads 6 --start 2026-06-01 --end 2026-09-26; do
  echo "=== $(date -u '+%F %T') mars_pipeline exited $?; restarting in 600s"
  sleep 600
done
echo "=== $(date -u '+%F %T') mars_pipeline complete"
