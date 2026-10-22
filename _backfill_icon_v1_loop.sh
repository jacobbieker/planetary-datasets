#!/bin/bash
# Keep one ICON v1 store current: walk the archived runs and those DWD still lists with
# scripts/icon_v1_backfill.py (runs already stored are skipped, runs below its completeness
# threshold are logged and left for later), then wait PAUSE seconds (12 h) and walk again.
#
#     ./_backfill_icon_v1_loop.sh <variant>      # global_art, eu_art, ...
cd "$(dirname "$0")"
export PYTHONPATH=$PWD
PIXI=${PIXI:-~/.pixi/bin/pixi}
VARIANT=${1:?usage: $0 <variant>}
PAUSE=${PAUSE:-43200}
while true; do
  echo "=== $(date -u '+%F %T') icon_v1_$VARIANT start"
  $PIXI run python scripts/icon_v1_backfill.py "$VARIANT" \
    --skipped-log "logs/icon_v1_${VARIANT}_skipped.tsv"
  echo "=== $(date -u '+%F %T') icon_v1_$VARIANT exited $?; next pass in ${PAUSE}s"
  sleep "$PAUSE"
done
