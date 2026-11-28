#!/bin/bash
# mtg_500m (odegree-12-highres, 500m) back to 202412 and kept current; see _backfill_satcons.sh.
# One image decodes at a time (FCI is netCDF, which is not thread-safe) while 4 downloads
# run ahead on disk: 8.7 GB peak and 456 s for 12 images, vs 33.5 GB and 573 s at 2
# unbounded workers (2026-11-28). Needs satellite-consumer with decode_slots.
export SATCONS_LOW_MEMORY=false SATCONS_MAX_WORKERS=4 SATCONS_BUFFER_SIZE=4 SATCONS_DECODE_SLOTS=1
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12-highres 500 mtg_500m 202412
