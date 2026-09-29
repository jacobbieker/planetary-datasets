#!/bin/bash
# Entrypoint: the first argument picks the downloader.
#
#   opera ...   planetary_datasets.providers.opera_download
#   obs ...     planetary_datasets.providers.earth2studio_download
set -euo pipefail
case "${1:-}" in
  opera) shift; exec python -m planetary_datasets.providers.opera_download "$@" ;;
  obs)   shift; exec python -m planetary_datasets.providers.earth2studio_download "$@" ;;
  *) echo "usage: {opera|obs} [args...]" >&2; exit 2 ;;
esac
