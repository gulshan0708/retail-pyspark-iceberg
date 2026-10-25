#!/usr/bin/env bash
# Runs the integration tests inside the Spark container, where the catalog,
# storage and Postgres are reachable. Extra arguments go to pytest.
set -euo pipefail

# Installs pytest and chispa into the container if missing (fast when already there;
# redone only after the container is recreated).
docker exec retail-spark python3 -m pip install --quiet \
  --disable-pip-version-check --root-user-action=ignore pytest chispa

# PYTHONPATH = our project + Spark's own Python library + its py4j bridge.
# (If you ever change the Spark image, check the py4j file name with:
#  docker exec retail-spark ls /opt/spark/python/lib)
docker exec \
  -e PYTHONPATH=/home/iceberg/project:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -w /home/iceberg/project \
  retail-spark \
  python3 -m pytest -m integration -v "$@"