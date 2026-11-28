#!/bin/bash
# SondeHub radiosonde flights -> bkr/obs/sondehub_flights.icechunk, newest day first back to
# 2018, resumable from sondehub_date_state.json and sondehub_processed_serials.txt (copy them
# along when moving machines). Once the walk is complete it waits a day and runs again,
# which picks up the days published since.
#
# sondehub_icechunk.py resolves store credentials through boto3, not Config, so the Source
# Cooperative keys in .env are exported for it.
#
#     setsid nohup ./_backfill_sondehub.sh >> ~/backfill-logs/sondehub.log 2>&1 < /dev/null &
cd "$(dirname "$0")"
set -a; source .env; set +a
PIXI=${PIXI:-~/.pixi/bin/pixi}
# Four download workers: the archive is now served from SondeHub's own CloudFront
# (history.sondehub.org), whose egress SondeHub pays for, and the walk back to 2018 is done.
while true; do
  echo "=== $(date -u '+%F %T') sondehub start"
  $PIXI run python sondehub_t.py --workers 4
  rc=$?
  echo "=== $(date -u '+%F %T') sondehub exited $rc"
  if (( rc == 0 )); then sleep 86400; else sleep 600; fi
done
