#!/usr/bin/env bash
# Remove scratch files left over from the pre-consolidation era.
#
# These are all UNTRACKED in this repository, so no commit can delete them — they only
# exist in a working tree. Hence a script rather than a change to the tree.
#
#   ./scripts/clean_working_tree.sh          # dry run, prints what would go
#   ./scripts/clean_working_tree.sh --delete # actually delete
#
# Everything here was reviewed during the consolidation and found to be exploratory
# one-offs: printing a dataset, plotting a figure, or a throwaway experiment. Anything with
# real behaviour was folded into planetary_datasets/providers/ first. Stray data files are
# listed but never auto-deleted; some are large and may still be wanted.

set -euo pipefail
cd "$(dirname "$0")/.."

DELETE=false
[[ "${1:-}" == "--delete" ]] && DELETE=true

# Exploratory scripts: inspect-and-print, plotting, or superseded experiments.
SCRATCH=(
  t.py xx.py n.py g.py ddddd.py asdfasdf.puy.py min_shard.py
  t_open_meps_radar.py g_open.py nordic_t.py npr.py dedup_csv.py
  cleaner.py andoya_cropping.py check_flight_paths.py skysight_open.py
  virtualize_nsdrb.py new_gmgsi.py tiny.py
  one_offs/aeolous.py one_offs/grid_compare.py one_offs/hurricane.py
  one_offs/meps_ice_index_check.py one_offs/vires_download.py
)

# Shell and pip accidents.
JUNK=( '=0.7.0' '=3.0.0' crontab_test cronyy changeme.txt full.json.gz )

removed=0
skipped=0

for f in "${SCRATCH[@]}" "${JUNK[@]}"; do
  if [[ ! -e "$f" ]]; then continue; fi
  if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
    echo "SKIP (tracked, remove via a commit instead): $f"
    skipped=$((skipped + 1))
    continue
  fi
  if $DELETE; then
    rm -f "$f"
    echo "deleted $f"
  else
    echo "would delete $f"
  fi
  removed=$((removed + 1))
done

echo
if $DELETE; then echo "$removed removed, $skipped skipped"; else echo "$removed would be removed, $skipped skipped (dry run; pass --delete)"; fi

echo
echo "Stray data files at the repository root — review by hand, not deleted by this script:"
ls -1 ./*.nc ./*.h5 ./*.tif ./*.parquet ./*.zip ./*.pdf ./*.docx ./*.csv ./*.txt 2>/dev/null |
  while read -r f; do
    git ls-files --error-unmatch "$f" >/dev/null 2>&1 || printf '  %8s  %s\n' "$(du -h "$f" | cut -f1)" "$f"
  done
echo
echo "They are gitignored now, so they will not be committed by accident."
