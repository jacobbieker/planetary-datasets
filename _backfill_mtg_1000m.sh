#!/bin/bash
# mtg_1000m (odegree-12, 1000m) back to 202409 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12 1000 mtg_1000m 202409
