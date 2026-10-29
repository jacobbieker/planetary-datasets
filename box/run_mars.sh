#!/bin/bash
# MARS archive: fill the missing timesteps from 2026-06-01 to the end of the stores time axis.
# Restarts after a failure (resumes: ingested timesteps are skipped); stops once it completes.
cd ~/planetary-datasets
export PYTHONPATH=$PWD MEMORY_CEILING_GB=48
until ~/.pixi/bin/pixi run python -m planetary_datasets.providers.mars_pipeline \
    --download --download-threads 6 --start 2026-06-01 --end 2026-09-26; do
  echo "=== $(date -u "+%F %T") mars_pipeline exited $?; restarting in 600s"
  sleep 600
done
echo "=== $(date -u "+%F %T") mars_pipeline complete"
