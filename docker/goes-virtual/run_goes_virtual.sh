#!/bin/bash
# Entrypoint for the geostationary virtual-reference ingest container.
#
# With no arguments it runs the full orchestrated job: every configured
# mission, split into tiers, under a memory watchdog. With arguments it passes
# them straight through to an ingest CLI, so the image doubles as a way to run
# a single satellite/channel:
#
#   docker run ... goes-virtual --satellite goes19 --channel 13 --max-eras 1 \
#       --storage s3 --bucket ... --prefix ...
#
# Prefix the arguments with `gk2a` (or `himawari`) to reach that CLI instead.
# Those two take their destination from the config environment rather than
# from --storage/--bucket flags, so only the store name is given here:
#
#   docker run ... -e ICECHUNK_BUCKET=... -e AWS_ACCESS_KEY_ID=... \
#       goes-virtual gk2a --band ir087 --max-eras 1 \
#       --store-base bkr/geo/virtualized/gk2a_ami_fd
set -u

RUN="python -u -m planetary_datasets.providers.virtualized.ingest_goes_radf"
GK2A_CMD="python -u -m planetary_datasets.providers.virtualized.ingest_gk2a_fd"
HIMA_CMD="python -u -m planetary_datasets.providers.virtualized.ingest_himawari_isatss"
WATCHDOG="python -u /usr/local/bin/memory_watchdog.py"

# --- live append ---------------------------------------------------------------
# `append <satellite> --channels <C01,..|all> --lookback-minutes <N>` appends,
# per channel, every scan newer than the live store's last commit (and within
# the lookback), one commit per channel. It exits 0 when nothing is new, and
# non-zero only when every channel failed; its last stdout line is the JSON
# summary {"satellite": ..., "channels": {"C13": {"appended": n, "last": ...}}}.
#
# The job runs next to the NOAA buckets (us-east-1) while the stores live in
# us-west-2, so the store region is SC_REGION or us-west-2 and never the task's
# own AWS_REGION. NOAA reads are anonymous and use GOES_SOURCE_REGION.
append_writer_env() {
  local key=${SC_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}
  local secret=${SC_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}
  if [ -n "$key" ]; then export AWS_ACCESS_KEY_ID="$key"; fi
  if [ -n "$secret" ]; then export AWS_SECRET_ACCESS_KEY="$secret"; fi
  export ICECHUNK_BUCKET="${SC_BUCKET:-${ICECHUNK_BUCKET:-us-west-2.opendata.source.coop}}"
  export AWS_REGION="${SC_REGION:-us-west-2}"
  APPEND_PREFIX_ROOT=${SC_PREFIX_ROOT:-${GOES_STORE_ROOT:-bkr/geo/virtualized}}
}

if [ "${1:-}" = "append" ]; then
  if [ "$#" -lt 2 ]; then
    echo "usage: append <satellite> --channels <C01,..|all> --lookback-minutes <N>" >&2
    exit 2
  fi
  append_sat=$2
  shift 2
  case "$append_sat" in
    goes16|goes17|goes18|goes19)
      append_writer_env
      exec $RUN \
        --satellite "$append_sat" \
        --storage config \
        --prefix "${APPEND_PREFIX_ROOT}/${append_sat}_radf.icechunk" \
        --append-latest \
        "$@"
      ;;
    gk2a)
      # GK-2A's CLI names them bands; the contract calls them channels.
      append_writer_env
      gk2a_args=()
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --channels) gk2a_args+=(--bands "${2:?--channels needs a value}"); shift 2 ;;
          --channels=*) gk2a_args+=(--bands "${1#--channels=}"); shift ;;
          *) gk2a_args+=("$1"); shift ;;
        esac
      done
      exec $GK2A_CMD --append-latest --store-base "${APPEND_PREFIX_ROOT}/gk2a_ami_fd" ${gk2a_args[@]+"${gk2a_args[@]}"}
      ;;
  esac
  echo "append: unsupported satellite '$append_sat'" >&2
  exit 2
fi

# --- pass-through mode -------------------------------------------------------
if [ "$#" -gt 0 ]; then
  if [ "$1" = "gk2a" ]; then
    shift
    exec $GK2A_CMD "$@"
  fi
  if [ "$1" = "himawari" ]; then
    shift
    exec $HIMA_CMD "$@"
  fi
  exec $RUN "$@"
fi

# --- orchestrated mode -------------------------------------------------------
# Destination and credentials fall back to the shared planetary_datasets
# config variables (ICECHUNK_BUCKET / ICECHUNK_PREFIX / AWS_*), so a container
# configured with the same --env-file as the rest of the project works without
# a second set of names. The SC_* forms stay supported for existing deployments.
SC_ACCESS_KEY_ID=${SC_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-}}
SC_SECRET_ACCESS_KEY=${SC_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-}}
: "${SC_ACCESS_KEY_ID:?set SC_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID) to the Source Cooperative access key}"
: "${SC_SECRET_ACCESS_KEY:?set SC_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY) to the Source Cooperative secret key}"

SC_BUCKET=${SC_BUCKET:-${ICECHUNK_BUCKET:-us-west-2.opendata.source.coop}}
SC_REGION=${SC_REGION:-${AWS_REGION:-us-west-2}}
SC_PREFIX_ROOT=${SC_PREFIX_ROOT:-${GOES_STORE_ROOT:-bkr/geo/virtualized}}
# `-` not `:-`: an explicitly empty SATELLITES disables the GOES tiers,
# which is how a GK-2A-only container is configured.
SATELLITES=${SATELLITES-"goes16 goes17 goes18 goes19"}
MAIN_WORKERS=${MAIN_WORKERS:-3}
BATCH_SIZE=${BATCH_SIZE:-1}
BUDGET_GB=${BUDGET_GB:-${MEMORY_CEILING_GB:-64}}
OUT=${GOES_OUT:-${PLANETARY_DATASETS_DATA_DIR:-/data}}

# A bind-mounted host directory keeps its own ownership, shadowing the image's,
# so -v /some/host/dir:/data lands read-only for this user unless the host
# directory is chowned first. Fail loudly here rather than half-starting with
# every log redirect erroring.
if ! mkdir -p "$OUT/logs" 2>/dev/null || ! [ -w "$OUT" ]; then
  echo "ERROR: $OUT is not writable by uid $(id -u)." >&2
  echo "If you bind-mounted a host directory, chown it first:" >&2
  echo "    sudo chown -R $(id -u):$(id -g) /your/host/dir" >&2
  echo "Or use a named volume (-v goes-data:/data), which inherits image ownership." >&2
  exit 1
fi

# Everything except C02. C02 runs alone: its 21696^2 grid carries roughly 16x
# the chunk-manifest entries of a 2km channel.
OTHER_CHANNELS=${OTHER_CHANNELS:-"16 15 14 13 12 11 10 9 8 7 6 5 4 3 1"}
RUN_C02=${RUN_C02:-1}

# GK-2A AMI full-disk. Off by default so existing GOES deployments are
# unchanged; set RUN_GK2A=1 to add it. vi006 is the half-kilometre band
# (22000^2) and gets its own single-worker pass, as C02 does for ABI.
RUN_GK2A=${RUN_GK2A:-0}
GK2A_BANDS=${GK2A_BANDS:-"ir087 ir096 ir105 ir112 ir123 ir133 nr013 nr016 sw038 wv063 wv069 wv073 vi004 vi005 vi008"}
GK2A_WORKERS=${GK2A_WORKERS:-$MAIN_WORKERS}
RUN_GK2A_VI006=${RUN_GK2A_VI006:-1}
# Year windows cap each backwards probe at ~365 days instead of ~1300, so a
# spot reclaim discards far less unfinished work.
GK2A_BY_YEAR=${GK2A_BY_YEAR:-1}

# Himawari AHI full-disk from ISatSS tiles. Off by default. One band-day is
# ~12,500 tile files against 144 for a GOES channel-day, so this runs a year
# at a time newest-first (--by-year) and each year lands its own stores.
RUN_HIMAWARI=${RUN_HIMAWARI:-0}
HIMAWARI_SATELLITES=${HIMAWARI_SATELLITES:-"himawari9 himawari8"}
HIMAWARI_BANDS=${HIMAWARI_BANDS:-"C13 C14 C15 C16 C08 C09 C10 C11 C12 C07 C05 C06 C01 C02 C04"}
HIMAWARI_WORKERS=${HIMAWARI_WORKERS:-$MAIN_WORKERS}
RUN_HIMAWARI_C03=${RUN_HIMAWARI_C03:-1}
# Threads per scene when opening tiles. The work is S3-latency bound, so
# this scales throughput far more than CPU count suggests.
HIMAWARI_TILE_THREADS=${HIMAWARI_TILE_THREADS:-16}

# Pinned so a restart reuses the existing date-suffixed stores instead of
# minting a fresh set. Override to resume an older run.
END_DATE=${END_DATE:-$(date -u +%F)}

echo "satellites : $SATELLITES"
echo "gk2a       : $([ "$RUN_GK2A" = 1 ] && echo "enabled (${GK2A_WORKERS} workers$([ "$GK2A_BY_YEAR" = 1 ] && echo ", by-year"))" || echo "disabled")"
echo "himawari   : $([ "$RUN_HIMAWARI" = 1 ] && echo "enabled (${HIMAWARI_SATELLITES}, ${HIMAWARI_WORKERS} workers x ${HIMAWARI_TILE_THREADS} tile threads, by-year)" || echo "disabled")"
echo "end-date   : $END_DATE   (store suffix for the newest era)"
echo "bucket     : $SC_BUCKET/$SC_PREFIX_ROOT"
echo "tiers      : main=$MAIN_WORKERS workers, c02=$([ "$RUN_C02" = 1 ] && echo "1 worker" || echo "disabled")"
echo "budget     : ${BUDGET_GB}GB"
echo "logs       : $OUT/logs"
echo

# The GOES CLI reads its destination and credentials from the environment via
# planetary_datasets.config, so the keys never appear in argv. They would
# otherwise be readable from `ps` by anything in the container, and the
# watchdog logs command lines.
export ICECHUNK_BUCKET="$SC_BUCKET"
export AWS_REGION="$SC_REGION"
export AWS_ACCESS_KEY_ID="$SC_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$SC_SECRET_ACCESS_KEY"

launch() {  # satellite, tag, extra args...
  local sat=$1 tag=$2; shift 2
  $RUN \
    --satellite "$sat" \
    --storage config \
    --prefix "${SC_PREFIX_ROOT}/${sat}_radf.icechunk" \
    --end-date "$END_DATE" \
    --batch-size "$BATCH_SIZE" \
    --log-dir "$OUT/logs" \
    "$@" \
    > "$OUT/${sat}_${tag}.log" 2>&1 &
  echo "  $sat/$tag -> pid $! (log: $OUT/${sat}_${tag}.log)"
}

# Like the GOES CLI, these two resolve the bucket and credentials from the
# environment exported above; --store-base names the store within it. It is
# passed explicitly rather than left to the CLI default so that overriding
# SC_PREFIX_ROOT still moves these stores with the GOES ones.
launch_gk2a() {  # tag, extra args...
  local tag=$1; shift
  $GK2A_CMD \
    --store-base "${SC_PREFIX_ROOT}/gk2a_ami_fd" \
    --end-date "$END_DATE" \
    --batch-size "$BATCH_SIZE" \
    --log-dir "$OUT/logs" \
    $([ "$GK2A_BY_YEAR" = 1 ] && echo --by-year) \
    "$@" \
    > "$OUT/gk2a_${tag}.log" 2>&1 &
  echo "  gk2a/$tag -> pid $! (log: $OUT/gk2a_${tag}.log)"
}

launch_himawari() {  # satellite, tag, extra args...
  local sat=$1 tag=$2; shift 2
  HIMAWARI_TILE_THREADS=$HIMAWARI_TILE_THREADS $HIMA_CMD \
    --satellite "$sat" \
    --store-base "${SC_PREFIX_ROOT}/${sat}_isatss" \
    --end-date "$END_DATE" \
    --batch-size "$BATCH_SIZE" \
    --log-dir "$OUT/logs" \
    --by-year \
    "$@" \
    > "$OUT/${sat}_${tag}.log" 2>&1 &
  echo "  $sat/$tag -> pid $! (log: $OUT/${sat}_${tag}.log)"
}

pids=()
for sat in $SATELLITES; do
  # shellcheck disable=SC2086
  launch "$sat" main --channels $OTHER_CHANNELS --parallel --max-workers "$MAIN_WORKERS"
  pids+=($!)
  if [ "$RUN_C02" = "1" ]; then
    launch "$sat" c02 --channel 2
    pids+=($!)
  fi
done

if [ "$RUN_HIMAWARI" = "1" ]; then
  for hsat in $HIMAWARI_SATELLITES; do
    # shellcheck disable=SC2086
    launch_himawari "$hsat" main --bands $HIMAWARI_BANDS --parallel --max-workers "$HIMAWARI_WORKERS"
    pids+=($!)
    if [ "$RUN_HIMAWARI_C03" = "1" ]; then
      launch_himawari "$hsat" c03 --band C03
      pids+=($!)
    fi
  done
fi

if [ "$RUN_GK2A" = "1" ]; then
  # shellcheck disable=SC2086
  launch_gk2a main --bands $GK2A_BANDS --parallel --max-workers "$GK2A_WORKERS"
  pids+=($!)
  if [ "$RUN_GK2A_VI006" = "1" ]; then
    launch_gk2a vi006 --band vi006
    pids+=($!)
  fi
fi

BUDGET_GB=$BUDGET_GB GOES_OUT=$OUT $WATCHDOG >> "$OUT/logs/watchdog.log" 2>&1 &
watchdog_pid=$!
echo "  watchdog -> pid $watchdog_pid (log: $OUT/logs/watchdog.log)"
echo

# Stop the whole job if the container is asked to shut down, so no spawned
# channel worker is left running against the stores.
shutdown() {
  echo "shutting down..."
  kill "${pids[@]}" "$watchdog_pid" 2>/dev/null
  pkill -f "ingest_goes_radf" 2>/dev/null
  pkill -f "ingest_gk2a_fd" 2>/dev/null
  pkill -f "ingest_himawari_isatss" 2>/dev/null
  pkill -f "from multiprocessing" 2>/dev/null
  wait
  exit 0
}
trap shutdown TERM INT

wait "${pids[@]}"
kill "$watchdog_pid" 2>/dev/null
echo "all ingests finished"
