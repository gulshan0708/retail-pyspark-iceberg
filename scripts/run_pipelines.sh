#!/usr/bin/env bash
# Runs the four pipelines in order. Extra arguments (e.g. --batch-date 2024-07-01) go to each one.
# In Phase 2, Airflow will make one task out of each of these spark-submit calls.
set -euo pipefail

for pipeline in pipeline1_ingest pipeline2_clean pipeline3_aggregate pipeline4_load; do
  echo "=== running ${pipeline}"
  scripts/submit.sh "pipelines/${pipeline}.py" "$@"
done