#!/usr/bin/env bash
# Creates the sample data, loads the Postgres source table, and uploads the raw files to
# the storage service. Run once after `docker compose up -d` (inside the activated .venv).
set -euo pipefail

echo "1/3 generating sample data"
python scripts/generate_sample_data.py

echo "2/3 loading products into Postgres"
docker exec -i retail-postgres psql -U postgres -d retail -q < data/raw/postgres/products.sql

echo "3/3 uploading the raw files to s3://raw"
NETWORK=$(docker network ls --format '{{.Name}}' | grep 'retail-net' | head -n 1 || true)
if [ -z "$NETWORK" ]; then
  echo "Could not find the Docker network. Is the stack running? (docker compose up -d)" >&2
  exit 1
fi

aws_s3() {
  docker run --rm --network "$NETWORK" \
    -v "$(pwd)/data/raw:/data" \
    -e AWS_ACCESS_KEY_ID=minioadmin \
    -e AWS_SECRET_ACCESS_KEY=minioadmin \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -e AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
    -e AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
    amazon/aws-cli --endpoint-url http://minio:9000 "$@"
}

aws_s3 s3 mb s3://raw 2>/dev/null || true   # fine if the bucket already exists
aws_s3 s3 sync /data s3://raw --exclude "postgres/*"
echo "done"