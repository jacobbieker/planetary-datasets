#!/bin/bash
# Himawari 500m monthly shards, 202511 back to 202001, one lane; see _backfill_himawari.sh.
exec "$(dirname "$0")/_backfill_himawari.sh" 500 202511 202001
