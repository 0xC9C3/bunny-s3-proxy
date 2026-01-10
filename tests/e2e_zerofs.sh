#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

if [ -f ".env" ]; then
  source ".env"
fi

if [ -z "$BUNNY_STORAGE_ZONE" ] || [ -z "$BUNNY_ACCESS_KEY" ]; then
  echo "Error: BUNNY_STORAGE_ZONE and BUNNY_ACCESS_KEY must be set"
  echo "Create a .env file or export these variables"
  exit 1
fi

MEMORY_LIMIT="${1:-64m}"

echo "=== ZeroFS E2E Test ==="
echo "Memory limit: $MEMORY_LIMIT"
echo "Storage zone: $BUNNY_STORAGE_ZONE"
echo ""

echo "Building release binary..."
cargo build --release --quiet

echo "Starting proxy container..."
docker rm -f bunny-proxy-e2e 2>/dev/null || true

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
    --storage-zone "$BUNNY_STORAGE_ZONE" \
    --region de \
    --access-key "$BUNNY_ACCESS_KEY" \
    --s3-access-key-id test \
    --s3-secret-access-key test \
    --listen-addr 0.0.0.0:9000

sleep 2

if ! docker ps | grep -q bunny-proxy-e2e; then
  echo "FAIL: Container crashed on startup"
  docker logs bunny-proxy-e2e 2>&1 || true
  docker rm bunny-proxy-e2e 2>/dev/null || true
  exit 1
fi

echo "Initial memory: $(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}')"
echo ""

# Monitor memory in background
monitor_memory() {
  while docker ps | grep -q bunny-proxy-e2e 2>/dev/null; do
    MEM=$(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}' 2>/dev/null || echo "N/A")
    echo "[$(date +%H:%M:%S)] Memory: $MEM"
    sleep 5
  done
}
monitor_memory &
MONITOR_PID=$!

# Run the Rust e2e test
echo "Running ZeroFS workload test..."
echo ""

RESULT=0
BUNNY_STORAGE_ZONE="$BUNNY_STORAGE_ZONE" \
BUNNY_ACCESS_KEY="$BUNNY_ACCESS_KEY" \
cargo test --test e2e_zerofs -- --nocapture 2>&1 || RESULT=$?

# Stop monitor
kill $MONITOR_PID 2>/dev/null || true

echo ""

# Check final state
if docker ps | grep -q bunny-proxy-e2e; then
  echo "Final memory: $(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}')"
  RESTARTS=$(docker inspect bunny-proxy-e2e --format '{{.RestartCount}}' 2>/dev/null || echo "0")
  if [ "$RESTARTS" != "0" ]; then
    echo "WARNING: Container restarted $RESTARTS times (OOM?)"
    RESULT=1
  fi
else
  echo "FAIL: Container died during test"
  docker logs bunny-proxy-e2e 2>&1 | tail -20 || true
  RESULT=1
fi

# Cleanup
echo ""
echo "Cleaning up..."
docker stop bunny-proxy-e2e 2>/dev/null || true
docker rm bunny-proxy-e2e 2>/dev/null || true

if [ $RESULT -eq 0 ]; then
  echo ""
  echo "SUCCESS: Proxy handled ZeroFS workload with $MEMORY_LIMIT limit"
else
  echo ""
  echo "FAIL: Test failed"
fi

exit $RESULT
