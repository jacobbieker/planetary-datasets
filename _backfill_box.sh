#!/bin/bash
# Start every continuous backfill lane on the backfill box, each under a restart loop.
#
#     ./_backfill_box.sh start          # start any lane that is not already running (cron
#                                       # runs this every 15 minutes as a watchdog)
#     ./_backfill_box.sh status         # which lanes run, and each log's last line
#     ./_backfill_box.sh stop           # stop every lane
#     ./_backfill_box.sh noon           # the daily short-retention catch-up (cron, 12:00 UTC)
#     ./_backfill_box.sh guard          # pause/resume Himawari lanes by memory (cron, */10)
#
# Crontab on the box:
#     CRON_TZ=UTC
#     0 12 * * * <repo>/_backfill_box.sh noon
#     */10 * * * * <repo>/_backfill_box.sh guard >> ~/backfill-logs/guard.log 2>&1
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
#: guard: pause a Himawari lane when less than this much memory is available (GB), or
#: when anything else has been killed for memory since the last check ...
GUARD_MIN_AVAILABLE_GB=${GUARD_MIN_AVAILABLE_GB:-40}
#: ... and resume one once this much is available with no such kill for an hour.
GUARD_RESUME_AVAILABLE_GB=${GUARD_RESUME_AVAILABLE_GB:-150}
GUARD_QUIET_MINUTES=${GUARD_QUIET_MINUTES:-60}
#: guard restarts a running lane whose log has been silent for its pause between passes
#: plus this long: a process can hang without dying (GOME and MTG IRS did, for 40 hours).
GUARD_STALL_MINUTES=${GUARD_STALL_MINUTES:-180}
#: Pause between passes for the scripts that do not take --pause (seconds).
#: sondehub waits a day after a complete walk; mars_pipeline can sit silent for hours
#: while ECMWF works through its queue, and restarting it abandons the queued requests.
declare -A SCRIPT_PAUSE=([kenda]=1800 [icon_v1_global]=43200 [icon_v1_global_art]=43200
  [icon_v1_eu_art]=43200 [sondehub]=86400 [mars_pipeline]=43200)
#: Lanes whose memory creeps up over a day (the virtual-store writers grow by ~40 GB a
#: day). guard restarts one once its process holds more than GUARD_LEAKY_GB; a restart
#: costs at most the run in progress, and frees the memory Himawari would otherwise lose.
GUARD_LEAKY="hafs_a_parent hafs_a_storm hafs_b_parent hafs_b_storm nam_hawaii
  hrrr_alaska_sfc hrrr_alaska_prs hrrr_alaska_nat osmc_ragged"
GUARD_LEAKY_GB=${GUARD_LEAKY_GB:-25}
#: guard also removes abandoned planetary-datasets-* scratch untouched for this long.
GUARD_TMP_STALE_MINUTES=${GUARD_TMP_STALE_MINUTES:-360}
#: Himawari lanes in the order guard pauses them: the extra lanes of each resolution
#: first, then the last 500 m lane, then the last 1000 m lane; himawari_2000m_0 always
#: runs. Lanes resume in the reverse order.
GUARD_PAUSE_ORDER=${GUARD_PAUSE_ORDER-"himawari_500m_3 himawari_500m_2 himawari_500m_1 himawari_1000m_3 himawari_1000m_2 himawari_1000m_1 himawari_2000m_5 himawari_2000m_4 himawari_2000m_3 himawari_2000m_2 himawari_2000m_1 himawari_500m_0 himawari_1000m_0"}
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
  # NCEP ADP upper-air (AMDAR/aircraft + radiosondes) from GDEX d337000, from 1997; decodes
  # with MET pb2nc in the dtcenter/met Docker image.
  [ncep_upper_air]="_backfill_lanes.py ncep_upper_air --threads 2 --pause 21600"
  # MRMS outside CONUS (dynamical.org archives CONUS), SILAM aerosol, MetOp AVHRR and GOME,
  # and the met.no Nordic radar mosaic.
  [mrms_alaska]="_backfill_lanes.py mrms_alaska --threads 2"
  [mrms_carib]="_backfill_lanes.py mrms_carib --threads 2"
  [mrms_guam]="_backfill_lanes.py mrms_guam --threads 2"
  [mrms_hawaii]="_backfill_lanes.py mrms_hawaii --threads 2"
  [silam_aerosol]="_backfill_lanes.py silam_aerosol --threads 4 --pause 21600"
  [metop_avhrr]="_backfill_lanes.py metop_avhrr --threads 4"
  [metop_gome]="_backfill_lanes.py metop_gome --threads 2"
  # PAUSED 2026-10-17, by request, to free the CPU for the JPSS and AMPS catch-ups (~1 h per
  # day of radar from 2020). Uncomment to resume; it picks up where it stopped.
  # [nordic_radar]="_backfill_lanes.py nordic_radar --threads 2 --pause 21600"
  # GDEX-PAUSED 2026-10-21: data.gdex.ucar.edu started resetting the box's connections (it
  # still answers other hosts) with nine lanes reading it at once. The AMPS, NCEP upper-air
  # and IFS analysis lanes are paused until it answers again; resume them a few at a time.
  # A resume on 2026-10-21 failed within minutes (one probe answered, sustained reads were
  # reset again). The lanes now read GDEX gently (one download at a time per lane; resume at
  # most three at once, then the other AMPS domains one at a time). Its HTTP 500s are
  # Apache mod_qos refusing an address that requests too fast, so requests to GDEX are also
  # spaced two seconds apart per process and a refusal backs off for minutes
  # (common.download.HOST_MIN_INTERVAL). Resumed 2026-10-21: AMPS d5, NCEP upper air and
  # IFS analysis; AMPS d3 2026-10-22 after a night with only two retried refusals; AMPS d6
  # 2026-10-23 after a day with none.
  # AMPS WRF24. The nests d3/d6 (2.67 km) and d5 (0.89 km) backfill from 2024 only; d5
  # resumed 2026-10-18 and d3/d6 2026-10-19 on the 192-vCPU box.
  # GDEX-PAUSED [amps_d1]="_backfill_lanes.py amps_d1 --threads 4 --pause 21600"
  # GDEX-PAUSED [amps_d2]="_backfill_lanes.py amps_d2 --threads 4 --pause 21600"
  [amps_d3]="_backfill_lanes.py amps_d3 --threads 8 --pause 21600"
  [amps_d5]="_backfill_lanes.py amps_d5 --threads 8 --pause 21600"
  [amps_d6]="_backfill_lanes.py amps_d6 --threads 4 --pause 21600"
  # GDEX-PAUSED [amps_drake]="_backfill_lanes.py amps_drake --threads 4 --pause 21600"
  # GDEX-PAUSED [amps_nz]="_backfill_lanes.py amps_nz --threads 4 --pause 21600"
  # JPSS products NOAA keeps ~90 days: VIIRS M-band SDR and the ATMS-derived L2 stores.
  [jpss_viirs_mband_n20]="_backfill_lanes.py jpss_viirs_mband_n20 --threads 8"
  [jpss_viirs_mband_n21]="_backfill_lanes.py jpss_viirs_mband_n21 --threads 8"
  [jpss_viirs_mband_snpp]="_backfill_lanes.py jpss_viirs_mband_snpp --threads 8"
  [jpss_atms_sfr_n20]="_backfill_lanes.py jpss_atms_sfr_n20 --threads 2"
  [jpss_atms_sfr_n21]="_backfill_lanes.py jpss_atms_sfr_n21 --threads 2"
  [jpss_atms_sfr_snpp]="_backfill_lanes.py jpss_atms_sfr_snpp --threads 2"
  [jpss_mirs_img_n20]="_backfill_lanes.py jpss_mirs_img_n20 --threads 2"
  [jpss_mirs_img_n21]="_backfill_lanes.py jpss_mirs_img_n21 --threads 2"
  [jpss_mirs_img_snpp]="_backfill_lanes.py jpss_mirs_img_snpp --threads 2"
  [jpss_mirs_snd_n20]="_backfill_lanes.py jpss_mirs_snd_n20 --threads 2"
  [jpss_mirs_snd_n21]="_backfill_lanes.py jpss_mirs_snd_n21 --threads 2"
  [jpss_mirs_snd_snpp]="_backfill_lanes.py jpss_mirs_snd_snpp --threads 2"
  # 8 downloads, not 16: EUMETSAT's Data Store throttles a user's concurrent connections, and
  # the MetOp-SG lanes share that allowance.
  [mtg_irs_l1]="_backfill_mtg_irs.py mtg_irs_l1 --follow --threads 32 --downloads 8"
  [mtg_irs_l1_lac4_hourly]="_backfill_mtg_irs.py mtg_irs_l1_lac4_hourly --follow --threads 12 --downloads 8"
  # GMGSI and GEOS-CF v2 are near-realtime: once caught up, a pass every two hours.
  [gmgsi_v3]="_backfill_gmgsi.py --follow --period 7200"
  [geos_v2]="_backfill_lanes.py geos_v2 --threads 8 --pause 7200"
  # Virtual references (gribberish) to NOAA's HRRR Alaska and NAM Hawaii nests, from each
  # archive's first run, then a pass every two hours.
  [hrrr_alaska_sfc]="_backfill_lanes.py hrrr_alaska_sfc --threads 2 --pause 7200"
  [hrrr_alaska_prs]="_backfill_lanes.py hrrr_alaska_prs --threads 2 --pause 7200"
  [hrrr_alaska_nat]="_backfill_lanes.py hrrr_alaska_nat --threads 2 --pause 7200"
  [nam_hawaii]="_backfill_lanes.py nam_hawaii --threads 2 --pause 7200"
  # HAFS-A / HAFS-B virtual stores (bkr/dmi/virtualized/hafs_*), from 2023-06-19T18.
  [hafs_a_storm]="_backfill_lanes.py hafs_a_storm --threads 2 --pause 7200"
  [hafs_a_parent]="_backfill_lanes.py hafs_a_parent --threads 2 --pause 7200"
  [hafs_b_storm]="_backfill_lanes.py hafs_b_storm --threads 2 --pause 7200"
  [hafs_b_parent]="_backfill_lanes.py hafs_b_parent --threads 2 --pause 7200"
  # AOML / OSMC marine observations: bkr/aoml/v2, 2012 onward, then a pass every two hours.
  [osmc_ragged]="_backfill_lanes.py osmc_ragged --threads 4 --pause 7200"
  [ifs_analysis]="_backfill_lanes.py ifs_analysis --threads 8"
  # DWD ICON-D2-RUC: a rolling three-day local mirror of each run's first hour (~29 GB a
  # day), and the three cadence stores written from it, each every two hours. Moved here
  # 2026-10-19.
  [icon_d2_ruc_mirror]="-m planetary_datasets.providers.icon_d2_ruc_mirror --pause 7200"
  # ICON-ART and ICON-ART-EU: every run DWD lists (about a day) copied whole into the Source
  # Cooperative archive (bkr/icon/<model>/p/...), which scripts/icon_v1_backfill.py reads.
  # ~180 GB a day; a pass every three hours so no run expires first.
  [icon_v1_mirror]="-m planetary_datasets.providers.icon_v1_mirror icon-art icon-art-eu --pause 10800"
  [icon_d2_ruc_60min]="_backfill_lanes.py icon_d2_ruc_60min --threads 4 --pause 7200"
  [icon_d2_ruc_15min]="_backfill_lanes.py icon_d2_ruc_15min --threads 4 --pause 7200"
  [icon_d2_ruc_5min]="_backfill_lanes.py icon_d2_ruc_5min --threads 2 --pause 7200"
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
  # MeteoSwiss KENDA keeps ~a day upstream: a rolling 36 h window every 30 min.
  [kenda]="_backfill_kenda.sh"
  # MTG FCI (Meteosat-12) satellite-consumer stores, one lane each: every month from now
  # back to the store's start, filling gaps and then keeping the store current.
  [mtg_500m]="_backfill_mtg_500m.sh"
  [mtg_highres_1000m]="_backfill_mtg_highres_1000m.sh"
  [mtg_1000m]="_backfill_mtg_1000m.sh"
  # NOT STARTED: another host still writes mtg_2000m live (2026-10-24); a second writer
  # would race its commits. Enable once that host stops writing it.
  # [mtg_2000m]="_backfill_mtg_2000m.sh"
  # IODC SEVIRI 1 km: stalled at 2026-06-15 with 2025-12-06..2026-01-01 missing; filled
  # back to 2025-01 and kept current. (iodc_3000m_test is still written live elsewhere.)
  [iodc_1000m_test]="_backfill_iodc_1000m_test.sh"
  # MSG 0 degree SEVIRI (odegree): both stores began 2025-01-01, stalled at 2026-10-06 and
  # miss 28% (1 km) and 55% (3 km) of that span; filled back to 2020-01 and kept current.
  [msg_1000m]="_backfill_msg_1000m.sh"
  [msg_3000m]="_backfill_msg_3000m.sh"
  # MSG rapid scan (rss, every 5 minutes): both stores stalled at 2026-10-06 and miss 66%
  # (1 km) and 78% (3 km) since 2025-01; filled back to 2020-01 by request, kept current.
  [rss_1000m]="_backfill_rss_1000m.sh"
  [rss_3000m]="_backfill_rss_3000m.sh"
  # Himawari AHI real L1b data through ~/satellite-consumer into monthly shards
  # (bkr/geo/himawari_<res>m_backfill_<YYYYMM>), newest month first, back to 2020-01; each
  # resolution's months are split across its lanes (finished months cost a listing). Six
  # 2000m, four 1000m and four 500m lanes on the 192-vCPU box (2026-10-24).
  [himawari_2000m_0]="_backfill_himawari_2000m_0.sh"
  [himawari_2000m_1]="_backfill_himawari_2000m_1.sh"
  [himawari_2000m_2]="_backfill_himawari_2000m_2.sh"
  [himawari_2000m_3]="_backfill_himawari_2000m_3.sh"
  [himawari_2000m_4]="_backfill_himawari_2000m_4.sh"
  [himawari_2000m_5]="_backfill_himawari_2000m_5.sh"
  [himawari_1000m_0]="_backfill_himawari_1000m_0.sh"
  [himawari_1000m_1]="_backfill_himawari_1000m_1.sh"
  [himawari_1000m_2]="_backfill_himawari_1000m_2.sh"
  [himawari_1000m_3]="_backfill_himawari_1000m_3.sh"
  [himawari_500m_0]="_backfill_himawari_500m_0.sh"
  [himawari_500m_1]="_backfill_himawari_500m_1.sh"
  [himawari_500m_2]="_backfill_himawari_500m_2.sh"
  [himawari_500m_3]="_backfill_himawari_500m_3.sh"
  # ICON v1 Global, ICON-ART and ICON-ART-EU: a pass every 12 hours over the runs in the
  # Source Cooperative archive and those DWD's server still lists (read live, written
  # first so they cannot expire). One fresh worker per run; incomplete runs skipped and
  # logged. Global moved here from aero 2026-10-16; the ART stores' previous writer was
  # stopped 2026-10-22. The ART runs are also mirrored into the archive (icon_v1_mirror).
  [icon_v1_global]="_backfill_icon_global.sh"
  [icon_v1_global_art]="_backfill_icon_global_art.sh"
  [icon_v1_eu_art]="_backfill_icon_eu_art.sh"
)

# The short-retention stores. Their sources keep a few days at most, so they are caught up
# once a day rather than given a lane: SILAM dust, both DMI HARMONIE stores, every AROME
# domain and the MEPS model levels. Each store is its own process, so one slow or hung store
# cannot hold the rest past their upstream retention.
NOON_STORES="silam_dust dmi_harmonie_model_level dmi_harmonie arome_france_0025 arome_france_hd
  arome_new_caledonia arome_indian_ocean arome_french_guiana arome_caribbean arome_polynesia
  meps_det"
#: Himawari lanes paused when the noon catch-up starts (guard resumes them afterwards).
NOON_MAKE_ROOM="himawari_500m_3 himawari_500m_2 himawari_500m_1 himawari_1000m_3
  himawari_1000m_2"
#: A store's catch-up is killed after this, so a hang is retried at the next noon.
NOON_TIMEOUT=23h

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
    [[ -f $LOGS/$name.paused ]] && continue
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
    # Paused by guard, which resumes it when memory allows; a .paused file starting with
    # "manual" is left alone by guard (rm it to let the lane start again).
    [[ -f $LOGS/$name.paused ]] && continue
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
    [[ -f $LOGS/$name.paused ]] && state=paused
    last=$(grep -E '^\[|pass|exited|caught up|FAILED' "$LOGS/$name.log" 2>/dev/null | tail -1 | cut -c1-110)
    printf '%-50s %-8s %s\n' "$name" "$state" "$last"
  done
  ;;
guard)
  # Run from cron every 10 minutes; see GUARD_*. After pausing a lane, whatever was killed
  # is restarted straight away. Memory kills of anything but Himawari are
  # counted per log (OOM killer, a lane exiting 137, MemoryLimitExceeded, which is logged
  # without a timestamp) against the counts the previous run saw.
  state=$LOGS/.guard
  mkdir -p "$state"
  # Lanes known to creep up in memory, restarted before they crowd anything else out.
  for name in $GUARD_LEAKY; do
    running "$name" || continue
    rss=$(pgrep -f "[p]ython _backfill_lanes.py $name " | xargs -r ps -o rss= -p \
      | awk '{s += $1} END {print int(s / 1048576)}')
    (( ${rss:-0} > GUARD_LEAKY_GB )) || continue
    if [[ -n ${GUARD_DRY_RUN:-} ]]; then
      echo "would restart $name (${rss} GB)"
      continue
    fi
    pkill -f "[p]ython _backfill_lanes.py $name " \
      && echo "$(date -u '+%F %T') restarted $name to release memory (${rss} GB)"
  done
  # Hung lanes: alive but silent for longer than they ever wait between passes.
  now=$(date -u +%s)
  for name in "${!LANES[@]}" "${!SCRIPTS[@]}"; do
    running "$name" || continue
    log=$LOGS/$name.log
    [[ -f $log ]] || continue
    cmd="${LANES[$name]:-${SCRIPTS[$name]:-}}"
    pause=$(grep -oE -- '--(pause|period) [0-9]+' <<< "$cmd" | awk '{print $2}')
    pause=${pause:-${SCRIPT_PAUSE[$name]:-1800}}
    last=$(grep -E '^=== ' "$log" | tail -1)
    # A lane waiting out an unavailable source (exit 3) sleeps a day on purpose.
    [[ $last == *"exited 3"* ]] && pause=$(( pause > UNAVAILABLE_PAUSE ? pause : UNAVAILABLE_PAUSE ))
    age=$(( now - $(stat -c %Y "$log") ))
    (( age > pause + GUARD_STALL_MINUTES * 60 )) || continue
    if [[ -n ${GUARD_DRY_RUN:-} ]]; then
      echo "would restart stalled $name (silent $((age / 60)) min)"
      continue
    fi
    kill -- -"$(cat "$LOGS/$name.pid")" 2>/dev/null
    rm -f "$LOGS/$name.pid"
    echo "$(date -u '+%F %T') restarted stalled $name (log silent $((age / 60)) min)"
    restart_stalled=1
  done
  [[ -n ${restart_stalled:-} ]] && "$0" start > /dev/null
  # Scratch left by killed processes: planetary-datasets-* temp directories no process
  # holds open and nothing has touched for GUARD_TMP_STALE_MINUTES.
  inuse=$(sudo -n ls -l /proc/[0-9]*/fd /proc/[0-9]*/cwd 2>/dev/null \
    | grep -oE '/tmp/(planetary-datasets-[A-Za-z0-9_]+|planetary-datasets/[A-Za-z0-9_.-]+|mtg-irs-[A-Za-z0-9_-]+)' \
    | sort -u)
  # Also the providers' scratch_dir (/tmp/planetary-datasets/<name>-*, e.g. a killed SILAM
  # catch-up's downloads) and MTG IRS prefetch directories left by a restarted lane.
  for dir in /tmp/planetary-datasets-* /tmp/planetary-datasets/*/ /tmp/mtg-irs-*; do
    dir=${dir%/}
    [[ -d $dir ]] || continue
    grep -qx "$dir" <<< "$inuse" && continue
    [[ -n $(find "$dir" -maxdepth 0 -mmin +"$GUARD_TMP_STALE_MINUTES") ]] || continue
    [[ -n $(find "$dir" -mmin -"$GUARD_TMP_STALE_MINUTES" -print -quit) ]] && continue
    [[ -n ${GUARD_DRY_RUN:-} ]] && { echo "would remove stale scratch $dir"; continue; }
    rm -rf "$dir" && echo "$(date -u '+%F %T') removed stale scratch $dir"
  done
  available=$(awk '/MemAvailable/ {print int($2 / 1048576)}' /proc/meminfo)
  reasons=()
  killed=()
  (( available < GUARD_MIN_AVAILABLE_GB )) && reasons+=("only ${available} GB available")
  oom=$(sudo -n journalctl -k --since "-10 min" -o cat 2>/dev/null \
    | grep -oE 'Killed process [0-9]+ \([^)]+\)' | grep -v 'sat-consumer' | head -3 | tr '\n' ' ')
  [[ -n $oom ]] && { reasons+=("OOM killer: $oom"); killed+=(noon); }
  for log in "$LOGS"/*.log; do
    name=$(basename "$log" .log)
    [[ $name == himawari_* || $name == guard ]] && continue
    count=$(grep -cE 'exited 137|MemoryLimitExceeded:' "$log")
    previous=$(cat "$state/$name.count" 2>/dev/null || echo "$count")
    echo "$count" > "$state/$name.count"
    if (( count > previous )); then
      reasons+=("$name: $((count - previous)) memory kill(s)")
      [[ $name == noon_* ]] && killed+=(noon) || killed+=("$name")
    fi
  done
  if (( ${#reasons[@]} )); then
    date -u +%s > "$state/last_pressure"
    [[ -n ${GUARD_DRY_RUN:-} ]] && { echo "would pause (${available} GB): ${reasons[*]}"; exit 0; }
    paused=""
    for name in $GUARD_PAUSE_ORDER; do
      [[ -f $LOGS/$name.paused ]] && continue
      running "$name" || continue
      echo "$(date -u '+%F %T') ${reasons[*]}" > "$LOGS/$name.paused"
      kill -- -"$(cat "$LOGS/$name.pid")" 2>/dev/null
      echo "$(date -u '+%F %T') paused $name (${available} GB available): ${reasons[*]}"
      paused=$name
      break
    done
    [[ -z $paused ]] && echo "$(date -u '+%F %T') memory short, only himawari_2000m_0 left: ${reasons[*]}"
    # Then restart what was killed now, rather than leaving it for its retry pause, its
    # next pass or the next noon. The noon catch-up is simply run again: each store's lock
    # leaves any still running alone.
    for name in $(printf '%s\n' "${killed[@]}" | sort -u); do
      if [[ $name == noon ]]; then
        setsid "$0" noon < /dev/null > /dev/null 2>&1 &
        echo "$(date -u '+%F %T') relaunched the noon catch-up"
      elif [[ -n ${LANES[$name]:-}${SCRIPTS[$name]:-} ]]; then
        running "$name" && kill -- -"$(cat "$LOGS/$name.pid")" 2>/dev/null
        rm -f "$LOGS/$name.pid"
        echo "$(date -u '+%F %T') restarting $name"
      fi
    done
    (( ${#killed[@]} )) && "$0" start > /dev/null
    exit 0
  fi
  last=$(cat "$state/last_pressure" 2>/dev/null || echo 0)
  (( available >= GUARD_RESUME_AVAILABLE_GB )) || exit 0
  (( $(date -u +%s) - last >= GUARD_QUIET_MINUTES * 60 )) || exit 0
  for name in $(echo $GUARD_PAUSE_ORDER | tr ' ' '\n' | tac); do
    [[ -f $LOGS/$name.paused ]] || continue
    grep -q '^manual' "$LOGS/$name.paused" && continue
    [[ -n ${GUARD_DRY_RUN:-} ]] && { echo "would resume $name (${available} GB)"; exit 0; }
    rm -f "$LOGS/$name.paused"
    "$0" start > /dev/null
    echo "$(date -u '+%F %T') resumed $name (${available} GB available)"
    exit 0
  done
  ;;
noon)
  # The eleven catch-ups start together and peak at ~200 GB (DMI model levels alone budget
  # ~130 GB), which drove the OOM killer every noon. Make room first: pause the extra
  # Himawari lanes now; guard resumes them one by one once memory recovers.
  for name in $NOON_MAKE_ROOM; do
    [[ -f $LOGS/$name.paused ]] && continue
    running "$name" || continue
    echo "noon: $(date -u '+%F %T') paused for the noon catch-up" > "$LOGS/$name.paused"
    # Count it as memory pressure, so guard waits out its quiet hour before resuming any.
    mkdir -p "$LOGS/.guard" && date -u +%s > "$LOGS/.guard/last_pressure"
    kill -- -"$(cat "$LOGS/$name.pid")" 2>/dev/null
    echo "$(date -u '+%F %T') paused $name for the noon catch-up" >> "$LOGS/guard.log"
  done
  # One flock per store: a store still catching up at the next noon is left to finish.
  for store in $NOON_STORES; do
    flock -n "$LOGS/noon_$store.lock" timeout "$NOON_TIMEOUT" \
      $PIXI run python one_offs/silam_harmonie_catchup.py "=$store" \
      >> "$LOGS/noon_$store.log" 2>&1 &
  done
  wait
  ;;
*)
  echo "usage: $0 start|stop|status|noon|guard" >&2
  exit 2
  ;;
esac
