#!/bin/zsh
# Work through the Met Office backfill backlog.
#
#     ./_backfill_queue.sh lane_marine     > /tmp/backfill_lane_marine.log 2>&1 &
#     ./_backfill_queue.sh lane_ocean      > /tmp/backfill_lane_ocean.log 2>&1 &
#     ./_backfill_queue.sh lane_ocean_gap  > /tmp/backfill_lane_ocean_gap.log 2>&1 &
#     ./_backfill_queue.sh atmos_global    > /tmp/backfill_atmos_global.log 2>&1 &
#     ./_backfill_queue.sh atmos_24hr      > /tmp/backfill_atmos_24hr.log 2>&1 &
#     ./_backfill_queue.sh atmos_uk        > /tmp/backfill_atmos_uk.log 2>&1 &
#
# SHUT DOWN 2026-10-16, by request: no lane is running, lane_marine included. Each start
# date below is where that store actually ended, verified against the store itself, so
# restarting a lane resumes rather than re-skips.
#
# lane_marine is the one with a deadline. Its bucket keeps only a rolling ~8-10 day window,
# so every hour it stays down past that is lost for good, not merely deferred. Start it
# first when archiving resumes; `catchup` works out its own range, so it needs no date.
#
# One lane is one Icechunk writer, because a store takes a single writer and every append
# has to land in time order. Lanes that name several stores work them in sequence; the
# three atmospheric stores get a lane each instead, which is the only way to make a two
# year backlog finish in a sane time. A measured global 10 km partition is ~46 minutes, so
# 2794 of them serially is about three months.
#
# The cost of splitting the atmospheric stores is paid in bandwidth, not correctness:
# `atmos_global` and `atmos_24hr` read the same source runs and now download each file
# twice. They write different stores, so they never contend for a writer.
#
# THREADS caps every pool a partition would otherwise size from the core count. Keep it
# well below the core count when several lanes run at once -- see the docstring of
# `_backfill_metoffice.py` for what happened the time that cap was lifted.
#
# Each store resumes from the partition after the last one in its store, so a lane that is
# killed and restarted picks up where it stopped: `run_partition` skips what is already
# stored. The counts are upper bounds that run out at "now"; overshooting is free.

set -u
cd "${0:A:h}"

THREADS=${THREADS:-4}

# Run one store to completion, waiting out any outage rather than giving up on the range.
#
# A store that stops on a failure streak (see CONSECUTIVE_FAILURE_LIMIT) is retried after a
# pause instead of being abandoned for the next store in the lane. Re-running is cheap and
# safe: `run_partition` skips what is already stored, so a retry resumes rather than
# repeats. Any other exit status is the store's own business and ends its turn.
# `rc`, not `status`: in zsh `status` is a read-only alias for `$?`, and assigning to it
# aborts the function. That cost a lane on 2026-10-13 -- lane_ocean finished its wave store
# and died on the next line instead of starting the depth store. `zsh -n` does not catch
# it, because it is a runtime error rather than a syntax one.
run() {
    local name=$1 start=$2 count=$3 freq=$4 rc
    while true; do
        echo "=== $(date '+%F %T')  $name  from $start, $count x $freq"
        pixi run python _backfill_metoffice.py "$name" "$start" "$count" "$freq" "$THREADS"
        rc=$?
        echo "=== $(date '+%F %T')  $name  exited $rc"
        (( rc == 2 )) || return $rc
        echo "=== $(date '+%F %T')  $name  stopped on a failure streak; retrying in ${RETRY_PAUSE:-600}s"
        sleep "${RETRY_PAUSE:-600}"
    done
}

case "${1:?usage: $0 lane_marine|lane_ocean|atmos_global|atmos_24hr|atmos_uk}" in

# The marine observations are the only store here with a deadline: their bucket keeps a
# rolling ten-day window, while the lanes below run for weeks. Backfilling them once at the
# start would mean every hour published during those weeks ages out unrecoverably, so this
# lane catches up immediately and then goes on doing it twice a day for as long as the
# other lanes last.
#
# It is a lane of its own rather than a step inside the others on purpose. Interleaving it
# would put four writers on one Icechunk store, which takes a single writer; one lane
# touching uk_marine is what makes the repeat safe. It is also nearly free to repeat --
# `catchup` asks the bucket and the store what is actually missing, so a tick with nothing
# to do costs one listing.
lane_marine)
    while true; do
        run uk_marine catchup 0 1h
        echo "=== $(date '+%F %T')  uk_marine: next catch-up in ${MARINE_PERIOD:-43200}s"
        sleep "${MARINE_PERIOD:-43200}"
    done
    ;;

# Both wave stores are done -- global_wave on 2026-10-09, nws_wave on 2026-10-13 -- and are
# left out rather than re-skipped: a skip still costs a store query, and 585 of them is
# hours spent learning nothing. Only the depth store is left, at 58 GB a day before
# compression, which makes it the long pole of the whole backfill.
# PAUSED 2026-10-15, by request. Both ocean lanes are stopped rather than just this one:
# the gap lane below waits for this store to have no writer, so leaving it up would have
# let it start the moment this one stopped. Restart them together, this one first, so the
# gap lane's guard sees a writer and keeps waiting.
#
# The start below is where the store actually ended when it was stopped, not where the run
# originally began -- resuming from the original date would spend hours re-skipping.
lane_ocean)
    run metoffice_nws_ocean_depth_hourly   "2026-07-17"      100   1D
    ;;

# The depth store's six-month hole: Nov 2025 - Apr 2026 is all but absent, ~3936 hours,
# while everything from 2026-05-03 on is contiguous. `lane_ocean` only walks forward from
# where the store ended, so it would never reach this.
#
# Two things make this lane different from every other one here.
#
# It waits. A store takes one writer, and `lane_ocean` is filling this same store forward;
# starting both would put two writers on it. The wait is on the provider actually running
# rather than on a PID, because that is the condition that matters and it survives a lane
# being restarted.
#
# It leaves the axis unsorted. The store already holds 2026-05 onward, so every partition
# here lands behind the end -- an ordinary out-of-order write, but one that makes a sliced
# `.sel` unreliable until the store's reorder asset runs. Run that afterwards; nothing else
# may be writing while it does.
#
# The range is deliberately wider than the hole. Partly-filled days at either edge are
# revisited rather than retired (`missing_timesteps` counts a day stored only when all 24
# of its hours are), and a day that is already complete costs one store query to skip.
lane_ocean_gap)
    while pgrep -qf "_backfill_metoffice.py metoffice_nws_ocean_depth_hourly"; do
        echo "=== $(date '+%F %T')  depth store has a writer; waiting ${GAP_POLL:-900}s"
        sleep "${GAP_POLL:-900}"
    done
    echo "=== $(date '+%F %T')  depth store free; filling 2025-11 - 2026-04"
    run metoffice_nws_ocean_depth_hourly   "2025-10-25"      195   1D
    ;;

# PAUSED 2026-10-15, by request, to free the box for the SILAM catch-up. One lane per
# atmospheric store, run concurrently; each start below is where its store actually ended
# when it was stopped, so a restart resumes rather than re-skips.
atmos_global)
    run metoffice_global_deterministic_10km              "2025-01-14 00:00"  2580  6h
    ;;

atmos_24hr)
    run metoffice_global_deterministic_10km_6hourly_24hr "2025-02-12 00:00"  2470  6h
    ;;

# Hourly rather than six-hourly, so this is the longest lane by a wide margin even though
# the UK domain is a fraction of the global grid.
atmos_uk)
    run metoffice_uk_deterministic_2km                   "2025-04-27 09:00" 12950  1h
    ;;

*)
    echo "unknown lane: $1" >&2
    exit 2
    ;;
esac

echo "=== $(date '+%F %T')  lane $1 complete"
