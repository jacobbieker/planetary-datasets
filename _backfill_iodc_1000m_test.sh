#!/bin/bash
# iodc_1000m_test (iodc, 1000m) back to 202501 and kept current; see _backfill_satcons.sh.
exec "$(dirname "$0")/_backfill_satcons.sh" iodc 1000 iodc_1000m_test 202501
