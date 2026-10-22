#!/bin/bash
# ICON v1 eu_art, walked every 12 hours; see _backfill_icon_v1_loop.sh.
exec "$(dirname "$0")/_backfill_icon_v1_loop.sh" eu_art
