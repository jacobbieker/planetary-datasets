#!/bin/bash
# mtg_2000m (odegree-12, 2000m) back to 202409 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12 2000 mtg_2000m 202409
