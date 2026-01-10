#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
  source "$SCRIPT_DIR/../.env"
fi

if [ -z "$BUNNY_STORAGE_ZONE" ] || [ -z "$BUNNY_ACCESS_KEY" ]; then
  echo "Error: BUNNY_STORAGE_ZONE and BUNNY_ACCESS_KEY must be set"
  echo "Create a .env file or export these variables"
  exit 1
fi

MEMORY_LIMIT="${1:-64m}"
NUM_UPLOADS="${2:-100}"
FILE_SIZE_KB="${3:-512}"

PARALLEL="${4:-10}"

echo "=== E2E Memory Test ==="
echo "Memory limit: $MEMORY_LIMIT"
echo "Uploads: $NUM_UPLOADS x ${FILE_SIZE_KB}KB files ($PARALLEL parallel)"
echo ""

cargo build --release --quiet

TESTDIR=$(mktemp -d)
TESTFILE="$TESTDIR/testfile.bin"
dd if=/dev/urandom of="$TESTFILE" bs=1K count=$FILE_SIZE_KB 2>/dev/null

echo "Starting proxy container..."
docker rm -f bunny-proxy-e2e 2>/dev/null || true

STORAGE_ZONE="$BUNNY_STORAGE_ZONE"
ACCESS_KEY="$BUNNY_ACCESS_KEY"

CA_BUNDLE=$(readlink -f /etc/ssl/certs/ca-certificates.crt 2>/dev/null || echo "/etc/ssl/certs/ca-certificates.crt")

docker run -d \
  --name bunny-proxy-e2e \
  --memory="$MEMORY_LIMIT" \
  --memory-swap="$MEMORY_LIMIT" \
  -v "$(pwd)/target/release/bunny-s3-proxy:/bunny-s3-proxy:ro" \
  -v "$CA_BUNDLE:/etc/ssl/certs/ca-certificates.crt:ro" \
  -e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
  -p 19000:9000 \
  ubuntu:24.04 \
  /bunny-s3-proxy \
    --storage-zone "$STORAGE_ZONE" \
    --region de \
    --access-key "$ACCESS_KEY" \
    --s3-access-key-id test \
    --s3-secret-access-key test \
    --listen-addr 0.0.0.0:9000

sleep 2

if ! docker ps | grep -q bunny-proxy-e2e; then
  echo "FAIL: Container crashed on startup"
  docker logs bunny-proxy-e2e 2>&1 || true
  docker rm bunny-proxy-e2e 2>/dev/null || true
  rm -rf "$TESTDIR"
  exit 1
fi

echo "Initial memory: $(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}')"
echo ""

echo "Uploading $NUM_UPLOADS files ($PARALLEL parallel)..."
FAILED=0

upload_file() {
  local i=$1
  AWS_ACCESS_KEY_ID=test \
  AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url http://127.0.0.1:19000 \
    s3 cp "$TESTFILE" "s3://$STORAGE_ZONE/e2e-test/file-$i.bin" \
    --quiet 2>/dev/null
}
export -f upload_file
export TESTFILE STORAGE_ZONE

seq 1 $NUM_UPLOADS | xargs -P $PARALLEL -I {} bash -c 'upload_file {}' &
UPLOAD_PID=$!

while kill -0 $UPLOAD_PID 2>/dev/null; do
  if ! docker ps | grep -q bunny-proxy-e2e; then
    echo "FAIL: Container died during uploads"
    FAILED=1
    kill $UPLOAD_PID 2>/dev/null || true
    break
  fi
  MEM=$(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}' 2>/dev/null || echo "N/A")
  echo "  Memory: $MEM"
  sleep 2
done

wait $UPLOAD_PID 2>/dev/null || true

if docker ps | grep -q bunny-proxy-e2e; then
  echo ""
  echo "Final memory: $(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}')"
fi

echo ""
echo "Cleaning up test files..."
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
aws --endpoint-url http://127.0.0.1:19000 \
  s3 rm "s3://$STORAGE_ZONE/e2e-test/" --recursive --quiet 2>/dev/null || true

docker stop bunny-proxy-e2e 2>/dev/null || true
docker rm bunny-proxy-e2e 2>/dev/null || true
rm -rf "$TESTDIR"

if [ $FAILED -eq 0 ]; then
  echo ""
  echo "SUCCESS: Completed $NUM_UPLOADS uploads with $MEMORY_LIMIT memory limit"
  exit 0
else
  echo ""
  echo "FAIL: Container was OOM killed"
  exit 1
fi
