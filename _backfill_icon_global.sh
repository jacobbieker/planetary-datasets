#!/bin/bash
# ICON v1 global, walked every 12 hours; see _backfill_icon_v1_loop.sh. A run is ~151,000
# fields, so it gets more blocks in flight and archive reads (in-region S3) than the ART
# models, and a larger memory ceiling; reads from DWD's live server rise only to 32.
export EXTRA_ARGS="--max-workers 8 --fetch-workers 16 --live-fetch-workers 4 --ceiling-gb 24"
exec "$(dirname "$0")/_backfill_icon_v1_loop.sh" global
