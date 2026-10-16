#!/bin/bash
# Start every continuous backfill lane on the backfill box, each under a restart loop.
#
#     ./_backfill_box.sh start          # start any lane that is not already running (cron
#                                       # runs this every 15 minutes as a watchdog)
#     ./_backfill_box.sh status         # which lanes run, and each log's last line
#     ./_backfill_box.sh stop           # stop every lane
#     ./_backfill_box.sh noon           # the daily short-retention catch-up (cron, 12:00 UTC)
#
# Crontab on the box:
#     CRON_TZ=UTC
#     0 12 * * * <repo>/_backfill_box.sh noon
#     */15 * * * * <repo>/_backfill_box.sh start >> ~/backfill-logs/box.log 2>&1
#     @reboot sleep 60 && <repo>/_backfill_box.sh start >> ~/backfill-logs/box.log 2>&1
#
# One lane is one store and one writer. Lanes never share a store, and nothing else may
# write to these stores while they run -- Dagster included.
#
# A lane that exits is restarted: after RETRY_PAUSE on a failure streak (exit 2) or a crash,
# after UNAVAILABLE_PAUSE when its Data Store collection is not served yet (exit 3, MTG IRS
# 1199). Restarts resume, because stored partitions are skipped.
#
# Logs: ~/backfill-logs/<lane>.log. Lane PIDs: ~/backfill-logs/<lane>.pid.

set -u
cd "$(dirname "$0")"

LOGS=~/backfill-logs
PIXI=~/.pixi/bin/pixi
RETRY_PAUSE=${RETRY_PAUSE:-600}
UNAVAILABLE_PAUSE=${UNAVAILABLE_PAUSE:-86400}
export PYTHONPATH=$PWD
mkdir -p "$LOGS"

# lane name -> command. Threads are per lane, sized for ~20 lanes on 96 cores (m7i.24xlarge);
# the box is CPU-bound, so halve them on a 48-core machine.
declare -A LANES=(
  [metoffice_global_deterministic_10km]="_backfill_lanes.py metoffice_global_deterministic_10km --threads 8"
  [metoffice_global_deterministic_10km_6hourly_24hr]="_backfill_lanes.py metoffice_global_deterministic_10km_6hourly_24hr --threads 8"
  [metoffice_uk_deterministic_2km]="_backfill_lanes.py metoffice_uk_deterministic_2km --threads 8"
  [metoffice_global_wave]="_backfill_lanes.py metoffice_global_wave --threads 4"
  [metoffice_global_ocean_hourly]="_backfill_lanes.py metoffice_global_ocean_hourly --threads 4"
  [metoffice_nws_wave]="_backfill_lanes.py metoffice_nws_wave --threads 4"
  [metoffice_nws_ocean_surface_hourly]="_backfill_lanes.py metoffice_nws_ocean_surface_hourly --threads 4"
  [metoffice_nws_ocean_depth_hourly]="_backfill_lanes.py metoffice_nws_ocean_depth_hourly --threads 8"
  # The marine observations: a pass every 12 hours over the bucket's rolling ten days.
  [uk_marine]="_backfill_lanes.py uk_marine --threads 1 --pause 43200"
  [mtg_irs_l1]="_backfill_mtg_irs.py mtg_irs_l1 --follow --threads 12 --downloads 16"
  [mtg_irs_l1_lac4_hourly]="_backfill_mtg_irs.py mtg_irs_l1_lac4_hourly --follow --threads 12 --downloads 16"
  # GMGSI and GEOS-CF v2 are near-realtime: once caught up, a pass every two hours.
  [gmgsi_v3]="_backfill_gmgsi.py --follow --period 7200"
  [geos_v2]="_backfill_lanes.py geos_v2 --threads 8 --pause 7200"
  # Virtual references (gribberish) to NOAA's HRRR Alaska and NAM Hawaii nests, from each
  # archive's first run, then a pass every two hours.
  [hrrr_alaska_sfc]="_backfill_lanes.py hrrr_alaska_sfc --threads 2 --pause 7200"
  [hrrr_alaska_prs]="_backfill_lanes.py hrrr_alaska_prs --threads 2 --pause 7200"
  [hrrr_alaska_nat]="_backfill_lanes.py hrrr_alaska_nat --threads 2 --pause 7200"
  [nam_hawaii]="_backfill_lanes.py nam_hawaii --threads 2 --pause 7200"
  # AOML / OSMC marine observations: bkr/aoml/v2, 2012 onward, then a pass every two hours.
  [osmc_ragged]="_backfill_lanes.py osmc_ragged --threads 4 --pause 7200"
  [ifs_analysis]="_backfill_lanes.py ifs_analysis --threads 8"
  [opera_rainfall]="_backfill_lanes.py opera_rainfall --threads 4"
  [opera_dbz]="_backfill_lanes.py opera_dbz --threads 4"
  [metop_sg_mws]="_backfill_lanes.py metop_sg_mws --threads 4"
  [metop_sg_ro]="_backfill_lanes.py metop_sg_ro --threads 4"
  [metop_sg_metimage]="_backfill_lanes.py metop_sg_metimage --threads 8"
)

# Jobs with launchers of their own, which do their own restarting: the MARS archive (stops
# once its range is complete) and the SondeHub flights (re-runs daily).
declare -A SCRIPTS=(
  [mars_pipeline]="_backfill_mars.sh"
  [sondehub]="_backfill_sondehub.sh"
)

# The short-retention stores. Their sources keep a few days at most, so they are caught up
# once a day rather than given a lane: SILAM dust, both DMI HARMONIE stores, every AROME
# domain and the MEPS model levels.
NOON_STORES="silam dmi_harmonie arome meps"

lane_loop() {
  local name=$1 cmd=$2 rc
  # A virtual-reference commit touches hundreds of arrays at once; 1024 descriptors is not
  # enough for HRRR's native-level files.
  ulimit -n 65536
  while true; do
    echo "=== $(date -u '+%F %T') $name start: $cmd"
    $PIXI run python $cmd
    rc=$?
    echo "=== $(date -u '+%F %T') $name exited $rc"
    if (( rc == 3 )); then
      sleep "$UNAVAILABLE_PAUSE"
    else
      sleep "$RETRY_PAUSE"
    fi
  done
}

running() {
  local pidfile=$LOGS/$1.pid
  [[ -f $pidfile ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

case "${1:-status}" in
start)
  for name in "${!LANES[@]}"; do
    if running "$name"; then
      echo "$name already running"
      continue
    fi
    # setsid: the loop leads its own process group, so stop can take down the python too.
    setsid bash -c "$(declare -f lane_loop); PIXI=$PIXI RETRY_PAUSE=$RETRY_PAUSE UNAVAILABLE_PAUSE=$UNAVAILABLE_PAUSE lane_loop $name '${LANES[$name]}'" \
      >> "$LOGS/$name.log" 2>&1 < /dev/null &
    echo $! > "$LOGS/$name.pid"
    echo "started $name"
  done
  for name in "${!SCRIPTS[@]}"; do
    if running "$name"; then
      echo "$name already running"
      continue
    fi
    PIXI=$PIXI setsid ./"${SCRIPTS[$name]}" >> "$LOGS/$name.log" 2>&1 < /dev/null &
    echo $! > "$LOGS/$name.pid"
    echo "started $name"
  done
  ;;
stop)
  for name in "${!LANES[@]}" "${!SCRIPTS[@]}"; do
    if running "$name"; then
      kill -- -"$(cat "$LOGS/$name.pid")" 2>/dev/null && echo "stopped $name"
    fi
    rm -f "$LOGS/$name.pid"
  done
  ;;
status)
  for name in $(printf '%s\n' "${!LANES[@]}" "${!SCRIPTS[@]}" | sort); do
    state=stopped; running "$name" && state=running
    last=$(grep -E '^\[|pass|exited|caught up|FAILED' "$LOGS/$name.log" 2>/dev/null | tail -1 | cut -c1-110)
    printf '%-50s %-8s %s\n' "$name" "$state" "$last"
  done
  ;;
noon)
  # flock: a catch-up still running at the next noon is left to finish.
  exec flock -n "$LOGS/noon.lock" $PIXI run python one_offs/silam_harmonie_catchup.py $NOON_STORES \
    >> "$LOGS/noon_catchup.log" 2>&1
  ;;
*)
  echo "usage: $0 start|stop|status|noon" >&2
  exit 2
  ;;
esac
