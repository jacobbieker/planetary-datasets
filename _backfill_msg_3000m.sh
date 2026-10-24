#!/bin/bash
# msg_3000m (odegree, 3000m) back to 202001 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" odegree 3000 msg_3000m 202001
