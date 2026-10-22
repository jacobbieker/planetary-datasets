#!/bin/bash
# Himawari 2000m monthly shards, 202312 back to 202001, lane 1 of 4; see _backfill_himawari.sh.
exec "$(dirname "$0")/_backfill_himawari.sh" 2000 202312 202001 1 4
