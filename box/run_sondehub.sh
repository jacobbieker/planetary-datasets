#!/bin/bash
# SondeHub radiosonde flights -> bkr/obs/sondehub_flights.icechunk, newest day first back
# to 2018. Resumable from sondehub_date_state.json / sondehub_processed_serials.txt.
# boto3 resolves the store credentials, so the Source Coop keys in .env are exported.
cd ~/planetary-datasets
set -a; source .env; set +a
while true; do
  echo "=== $(date -u "+%F %T") sondehub start"
  ~/.pixi/bin/pixi run python sondehub_t.py --workers 16
  rc=$?
  echo "=== $(date -u "+%F %T") sondehub exited $rc"
  if (( rc == 0 )); then sleep 86400; else sleep 600; fi
done
