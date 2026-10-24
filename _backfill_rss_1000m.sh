#!/bin/bash
# rss_1000m (rss, 1000m) back to 202501 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" rss 1000 rss_1000m 202501
