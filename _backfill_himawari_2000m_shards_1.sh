#!/bin/bash
# Himawari 2000m: resume the existing monthly shards, 202512 back to 202401 (finishing the
# partial 202405, 202410, 202411, 202510 and 202511), lane 1 of 2; see _backfill_himawari.sh.
exec "$(dirname "$0")/_backfill_himawari.sh" 2000 202512 202401 1 2
