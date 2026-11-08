#!/bin/bash
# mtg_2000m (odegree-12, 2000m) from 202409 up to the month before the lane started; see
# _backfill_satcons.sh. Another host still writes the current month live, so this lane stays
# out of it: icechunk rebases absorb the occasional overlap of commits, but the two writers
# should not be filling the same timesteps.
export SATCONS_MAX_WORKERS=4 SATCONS_BUFFER_SIZE=6
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12 2000 mtg_2000m 202409 \
  "$(date -u -d "$(date -u +%Y-%m-01) -1 month" +%Y%m)"
