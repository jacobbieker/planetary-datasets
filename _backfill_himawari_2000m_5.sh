#!/bin/bash
# Himawari 2000m monthly shards, 202512 back to 202001, lane 5 of 6; see _backfill_himawari.sh.
exec "$(dirname "$0")/_backfill_himawari.sh" 2000 202512 202001 5 6
