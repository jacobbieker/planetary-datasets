#!/bin/bash
# mtg_highres_1000m (odegree-12-highres, 1000m) back to 202409 and kept current; see _backfill_satcons.sh.
# More parallel downloads: these lanes waited on I/O at 2 workers (2026-10-27).
# One image decodes at a time (FCI is netCDF, which is not thread-safe): 2.5 GB peak and
# 313 s for 12 images, vs 15.2 GB and 329 s unbounded (2026-11-28). Needs
# satellite-consumer with decode_slots.
export SATCONS_MAX_WORKERS=4 SATCONS_BUFFER_SIZE=6 SATCONS_DECODE_SLOTS=1
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12-highres 1000 mtg_highres_1000m 202409
