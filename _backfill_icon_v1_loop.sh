#!/bin/bash
# Keep one ICON v1 store current: walk the archived runs and those DWD still lists with
# scripts/icon_v1_backfill.py (runs already stored are skipped, runs below its completeness
# threshold are logged and, if read from the archive, not planned again).
#
# A pass writes at most LIMIT runs (4), the ones only DWD's live server has first, then
# the next pass is planned a minute later, so a run DWD publishes while an archive backlog
# drains is picked up within a few hours instead of expiring. Once a pass finds nothing to
# do the loop waits PAUSE seconds (12 h).
#
#     ./_backfill_icon_v1_loop.sh <variant>      # global, global_art, eu_art, ...
cd "$(dirname "$0")"
export PYTHONPATH=$PWD
PIXI=${PIXI:-~/.pixi/bin/pixi}
VARIANT=${1:?usage: $0 <variant>}
PAUSE=${PAUSE:-43200}
LIMIT=${LIMIT:-4}
while true; do
  echo "=== $(date -u '+%F %T') icon_v1_$VARIANT start"
  todo=$($PIXI run python scripts/icon_v1_backfill.py "$VARIANT" --limit "$LIMIT" --not-skipped \
    --skipped-log "logs/icon_v1_${VARIANT}_skipped.tsv" 2>&1 \
    | tee /dev/stderr | grep -oE '[0-9]+ to do' | head -1)
  echo "=== $(date -u '+%F %T') icon_v1_$VARIANT pass done (${todo:-? to do} at its start)"
  if [[ ${todo%% *} == 0 ]]; then
    sleep "$PAUSE"
  else
    sleep 60
  fi
done
