#!/bin/bash
# mtg_500m (odegree-12-highres, 500m) back to 202412 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" odegree-12-highres 500 mtg_500m 202412
