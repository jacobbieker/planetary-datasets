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
  # IFS analysis; AMPS d3 2026-10-22 after a night with only two retried refusals.
  # AMPS WRF24. The nests d3/d6 (2.67 km) and d5 (0.89 km) backfill from 2024 only; d5
  # resumed 2026-10-18 and d3/d6 2026-10-19 on the 192-vCPU box.
  # GDEX-PAUSED [amps_d1]="_backfill_lanes.py amps_d1 --threads 4 --pause 21600"
  # GDEX-PAUSED [amps_d2]="_backfill_lanes.py amps_d2 --threads 4 --pause 21600"
  [amps_d3]="_backfill_lanes.py amps_d3 --threads 8 --pause 21600"
  [amps_d5]="_backfill_lanes.py amps_d5 --threads 8 --pause 21600"
  # GDEX-PAUSED [amps_d6]="_backfill_lanes.py amps_d6 --threads 4 --pause 21600"
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
  [mtg_irs_l1]="_backfill_mtg_irs.py mtg_irs_l1 --follow --threads 12 --downloads 8"
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
  # ICON v1 Global from the Source Cooperative archive, oldest run first; one fresh worker
  # per run, incomplete runs skipped and logged. Moved here from aero on 2026-10-16.
  [icon_v1_global]="scripts/icon_v1_backfill.py global --skipped-log logs/icon_v1_global_skipped.tsv"
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
  # Himawari AHI real L1b data through ~/satellite-consumer into monthly shards
  # (bkr/geo/himawari_<res>m_backfill_<YYYYMM>), newest month first, back to 2020-01; each
  # resolution's months are split across its lanes. Only months no other writer has
  # started: 2000m from 2023-12, 1000m from 2024-06, 500m from 2025-11.
  [himawari_2000m_0]="_backfill_himawari_2000m_0.sh"
  [himawari_2000m_1]="_backfill_himawari_2000m_1.sh"
  [himawari_2000m_2]="_backfill_himawari_2000m_2.sh"
  [himawari_2000m_3]="_backfill_himawari_2000m_3.sh"
  [himawari_1000m_0]="_backfill_himawari_1000m_0.sh"
  [himawari_1000m_1]="_backfill_himawari_1000m_1.sh"
  [himawari_1000m_2]="_backfill_himawari_1000m_2.sh"
  [himawari_1000m_3]="_backfill_himawari_1000m_3.sh"
  [himawari_500m_0]="_backfill_himawari_500m_0.sh"
  [himawari_500m_1]="_backfill_himawari_500m_1.sh"
  [himawari_500m_2]="_backfill_himawari_500m_2.sh"
  # ICON-ART and ICON-ART-EU from the Source Cooperative archive, a pass every 12 hours.
  # NOT YET STARTED (2026-10-22): another host is still writing both stores. Uncomment once
  # it is stopped; two writers race their commits.
  # [icon_v1_global_art]="_backfill_icon_global_art.sh"
  # [icon_v1_eu_art]="_backfill_icon_eu_art.sh"
)

# The short-retention stores. Their sources keep a few days at most, so they are caught up
# once a day rather than given a lane: SILAM dust, both DMI HARMONIE stores, every AROME
# domain and the MEPS model levels. Each store is its own process, so one slow or hung store
# cannot hold the rest past their upstream retention.
NOON_STORES="silam_dust dmi_harmonie_model_level dmi_harmonie arome_france_0025 arome_france_hd
  arome_new_caledonia arome_indian_ocean arome_french_guiana arome_caribbean arome_polynesia
  meps_det"
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
  # One flock per store: a store still catching up at the next noon is left to finish.
  for store in $NOON_STORES; do
    flock -n "$LOGS/noon_$store.lock" timeout "$NOON_TIMEOUT" \
      $PIXI run python one_offs/silam_harmonie_catchup.py "=$store" \
      >> "$LOGS/noon_$store.log" 2>&1 &
  done
  wait
  ;;
*)
  echo "usage: $0 start|stop|status|noon" >&2
  exit 2
  ;;
esac
