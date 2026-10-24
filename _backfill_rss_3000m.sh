#!/bin/bash
# rss_3000m (rss, 3000m) back to 202001 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" rss 3000 rss_3000m 202001
