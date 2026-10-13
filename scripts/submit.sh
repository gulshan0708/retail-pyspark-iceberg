#!/usr/bin/env bash
# Runs a script inside the Spark container with the extra libraries it needs.
# Usage: scripts/submit.sh <path-to-script> [script args...]
set -euo pipefail

docker exec \
  -e PYTHONPATH=/home/iceberg/project \
  -w /home/iceberg/project \
  retail-spark \
  spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.4 \
  "$@"