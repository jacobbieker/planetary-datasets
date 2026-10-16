#!/bin/bash
# MeteoSwiss KENDA-CH1: stage each recent hour with the meteoswiss-kenda image and publish
# both stores; see _backfill_kenda.py. Restarts after a crash.
#
#     setsid nohup ./_backfill_kenda.sh >> ~/backfill-logs/kenda.log 2>&1 < /dev/null &
cd "$(dirname "$0")"
export PYTHONPATH=$PWD
PIXI=${PIXI:-~/.pixi/bin/pixi}
while true; do
  echo "=== $(date -u '+%F %T') kenda start"
  $PIXI run python _backfill_kenda.py
  echo "=== $(date -u '+%F %T') kenda exited $?; restarting in 600s"
  sleep 600
done
