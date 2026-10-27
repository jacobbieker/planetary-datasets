#!/bin/bash
# mtg_500m (odegree-12-highres, 500m) back to 202412 and kept current; see _backfill_satcons.sh.
# More parallel downloads: these lanes waited on I/O at 2 workers (2026-10-27).
export SATCONS_LOW_MEMORY=false SATCONS_MAX_WORKERS=2 SATCONS_BUFFER_SIZE=4
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12-highres 500 mtg_500m 202412
