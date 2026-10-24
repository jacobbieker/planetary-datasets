#!/bin/bash
# msg_1000m (odegree, 1000m) back to 202001 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" odegree 1000 msg_1000m 202001
