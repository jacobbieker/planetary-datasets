#!/bin/bash
# Himawari 1000m monthly shards, 202406 back to 202001, lane 3 of 4; see _backfill_himawari.sh.
exec "$(dirname "$0")/_backfill_himawari.sh" 1000 202406 202001 3 4
