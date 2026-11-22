#!/bin/bash
# Stand in for the EC2 boxes on pb, aero or the Mac while the boxes are down. Never run a
# lane here while a box still runs it: one lane is one store and one writer.
#
#     ./_backfill_backup.sh list                 every lane and noon store: on or off here
#     ./_backfill_backup.sh enable NAME...       switch lanes or noon stores (noon_<store>) on
#     ./_backfill_backup.sh disable NAME...      ... or off (a running one is stopped)
#     ./_backfill_backup.sh defaults HOST        switch on the suggested set for pb/aero/mac
#     ./_backfill_backup.sh on --boxes-down      start what is switched on, and a noon catch-up
#     ./_backfill_backup.sh off                  stop everything here; nothing starts again
#     ./_backfill_backup.sh status               the switched-on lanes, as _backfill_box.sh
#
# The lanes, their commands and the noon catch-up are _backfill_box.sh's, run with role
# "backup" (~/.config/planetary-box/role): it runs only what ~/.config/planetary-box/
# backup-lanes lists, and only while ~/.config/planetary-box/backup-on exists. Per-host
# settings go in ~/.config/planetary-box/host.env, for example on pb:
#
#     PIXI=/usr/bin/pixi
#     NOON_PARALLEL=1          # one noon store at a time: pb has ~30 GB to spare
#
# and on aero PIXI=~/planetary-fleet/bin/pixi (its own pixi is too old for pixi.lock). The
# Mac needs bash 5 (Homebrew) for the box script's associative arrays.
#
# Cron, if the backup should restart lanes and run noon by itself (inert while off):
#     CRON_TZ=UTC
#     0 12 * * * <repo>/_backfill_box.sh noon
#     */10 * * * * <repo>/_backfill_box.sh guard >> ~/backfill-logs/guard.log 2>&1
#     */15 * * * * <repo>/_backfill_box.sh start >> ~/backfill-logs/box.log 2>&1

set -u
cd "$(dirname "$0")"

CONFIG=~/.config/planetary-box
LANES_FILE=$CONFIG/backup-lanes
ON_FILE=$CONFIG/backup-on
LOGS=~/backfill-logs

#: Suggested sets, by the lanes' peak memory over 2026-11-15..22. Left out everywhere:
#: noon_arome_indian_ocean (~90 GB), noon_dmi_harmonie_model_level (~70 GB) and
#: noon_arome_caribbean (~40 GB), which need more memory than pb or aero can spare;
#: icon_v1_mirror (~180 GB a day through the home connection); mars_pipeline (530 GB of
#: free disk, and its queued ECMWF requests would be abandoned when the box returns).
declare -A DEFAULTS=(
  [pb]="kenda icon_v1_global icon_v1_global_art icon_v1_eu_art icon_d2_ruc_mirror
    icon_d2_ruc_60min icon_d2_ruc_15min icon_d2_ruc_5min noon_silam_dust noon_dmi_harmonie
    noon_arome_france_0025 noon_arome_france_hd noon_arome_new_caledonia
    noon_arome_french_guiana noon_arome_polynesia noon_meps_det"
  [aero]="wis2_archive uk_marine uk_radar nordic_radar"
  [mac]=""
)

box() {
  BOX_ROLE=backup ./_backfill_box.sh "$@"
}

enabled() {
  grep -v '^[[:space:]]*#' "$LANES_FILE" 2>/dev/null | grep -v '^[[:space:]]*$'
}

# Exit 2 unless every argument names a lane or noon store.
check_names() {
  local names name
  names=$(box names)
  for name in "$@"; do
    if ! grep -qx "$name" <<< "$names"; then
      echo "no lane or noon store named $name (see list)" >&2
      exit 2
    fi
  done
}

# Make this host's role "backup"; refuse to take over a host that has another role.
claim_role() {
  local role
  role=$(cat "$CONFIG/role" 2>/dev/null || echo "")
  if [[ -n $role && $role != backup ]]; then
    echo "this host has role '$role'; the backup runs only on a host without one" >&2
    exit 2
  fi
  mkdir -p "$CONFIG" "$LOGS"
  echo backup > "$CONFIG/role"
}

case "${1:-list}" in
list)
  on=$(enabled)
  state=off; [[ -f $ON_FILE ]] && state=on
  echo "backup is $state; switched on here:"
  for name in $(box names); do
    mark=" "; grep -qx "$name" <<< "$on" && mark="x"
    echo "  [$mark] $name"
  done
  ;;
enable)
  shift
  (( $# )) || { echo "usage: $0 enable NAME..." >&2; exit 2; }
  check_names "$@"
  claim_role
  for name in "$@"; do
    enabled | grep -qx "$name" || echo "$name" >> "$LANES_FILE"
  done
  if [[ -f $ON_FILE ]]; then
    box start
  fi
  ;;
disable)
  shift
  (( $# )) || { echo "usage: $0 disable NAME..." >&2; exit 2; }
  check_names "$@"
  for name in "$@"; do
    if [[ $name == noon_* ]]; then
      # The store's catch-up holds its lock; whatever holds the lock is that catch-up.
      [[ -f $LOGS/$name.lock ]] && fuser -k "$LOGS/$name.lock" > /dev/null 2>&1
    elif enabled | grep -qx "$name"; then
      box stop "$name"
    fi
  done
  remaining=$(enabled | grep -vxF -f <(printf '%s\n' "$@"))
  printf '%s\n' $remaining > "$LANES_FILE.tmp" && mv "$LANES_FILE.tmp" "$LANES_FILE"
  ;;
defaults)
  host=${2:-}
  if [[ -z $host || ! -v DEFAULTS[$host] ]]; then
    echo "usage: $0 defaults pb|aero|mac" >&2
    exit 2
  fi
  if [[ -n ${DEFAULTS[$host]} ]]; then
    "$0" enable ${DEFAULTS[$host]} || exit
  fi
  "$0" list
  ;;
on)
  if [[ ${2:-} != --boxes-down ]]; then
    echo "on: only while the boxes are down (two writers corrupt a store);" \
      "confirm with: $0 on --boxes-down" >&2
    exit 2
  fi
  claim_role
  touch "$ON_FILE"
  box start
  # The noon stores' sources keep a few days at most: catch them up now, not at 12:00.
  if enabled | grep -q '^noon_'; then
    setsid ./_backfill_box.sh noon < /dev/null > /dev/null 2>&1 &
    echo "started the noon catch-up"
  fi
  ;;
off)
  rm -f "$ON_FILE"
  box stop
  for lock in "$LOGS"/noon_*.lock; do
    [[ -f $lock ]] && fuser -k "$lock" > /dev/null 2>&1
  done
  echo "backup is off"
  ;;
status)
  box status
  ;;
*)
  echo "usage: $0 list|enable|disable|defaults|on|off|status" >&2
  exit 2
  ;;
esac
