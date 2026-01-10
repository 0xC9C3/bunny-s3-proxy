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
DURATION_SECS="${2:-60}"
PARALLEL="${3:-20}"
FILE_SIZE_KB="${4:-16}"

echo "=== E2E Memory Test (HTTP/2) ==="
echo "Memory limit: $MEMORY_LIMIT"
echo "Duration: ${DURATION_SECS}s"
echo "Parallel streams: $PARALLEL"
echo "File size: ${FILE_SIZE_KB}KB"
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

# Use h2load for HTTP/2 stress testing if available, otherwise fall back to curl
if command -v h2load &> /dev/null; then
  echo "Using h2load for HTTP/2 stress test..."

  # Create a simple PUT request body
  H2LOAD_RESULT=$(h2load -n 1000 -c $PARALLEL -m 10 \
    -d "$TESTFILE" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: 20260110T000000Z" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=test/20260110/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=fake" \
    "http://127.0.0.1:19000/$STORAGE_ZONE/e2e-test/file" 2>&1 || true)

  echo "$H2LOAD_RESULT" | tail -20
else
  echo "Using curl with HTTP/2 for stress test..."
  echo "(Install nghttp2 for better HTTP/2 testing with h2load)"
  echo ""

  # Function to upload continuously
  upload_loop() {
    local id=$1
    local end_time=$2
    local count=0
    while [ $(date +%s) -lt $end_time ]; do
      curl -s --http2-prior-knowledge \
        -X PUT \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
        -H "x-amz-date: 20260110T000000Z" \
        -H "Authorization: AWS4-HMAC-SHA256 Credential=test/20260110/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=fake" \
        --data-binary "@$TESTFILE" \
        "http://127.0.0.1:19000/$STORAGE_ZONE/e2e-test/worker-${id}-file-${count}.bin" \
        -o /dev/null -w "" || true
      count=$((count + 1))
    done
    echo "$count"
  }
  export -f upload_loop
  export TESTFILE STORAGE_ZONE

  END_TIME=$(($(date +%s) + DURATION_SECS))
  export END_TIME

  echo "Running $PARALLEL parallel HTTP/2 upload streams for ${DURATION_SECS}s..."
  echo ""

  # Start parallel uploaders in background
  PIDS=""
  for i in $(seq 1 $PARALLEL); do
    upload_loop $i $END_TIME &
    PIDS="$PIDS $!"
  done

  # Monitor memory while uploads run
  FAILED=0
  PEAK_MEM="0"
  while [ $(date +%s) -lt $END_TIME ]; do
    if ! docker ps | grep -q bunny-proxy-e2e; then
      echo ""
      echo "FAIL: Container died during test (OOM killed)"
      FAILED=1
      break
    fi

    MEM=$(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}' 2>/dev/null || echo "N/A")
    # Extract numeric memory value for peak tracking
    MEM_VAL=$(echo "$MEM" | grep -oP '^\d+(\.\d+)?' || echo "0")
    if [ "$(echo "$MEM_VAL > $PEAK_MEM" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
      PEAK_MEM="$MEM_VAL"
    fi

    ELAPSED=$(($(date +%s) - END_TIME + DURATION_SECS))
    printf "\r  Time: %ds/%ds | Memory: %s | Peak: %sMiB" "$ELAPSED" "$DURATION_SECS" "$MEM" "$PEAK_MEM"
    sleep 1
  done

  echo ""

  # Wait for all uploaders to finish
  for pid in $PIDS; do
    wait $pid 2>/dev/null || true
  done
fi

# Final status
if docker ps | grep -q bunny-proxy-e2e; then
  echo ""
  echo "Final memory: $(docker stats --no-stream bunny-proxy-e2e --format '{{.MemUsage}}')"

  # Get container stats
  RESTARTS=$(docker inspect bunny-proxy-e2e --format '{{.RestartCount}}' 2>/dev/null || echo "0")
  if [ "$RESTARTS" != "0" ]; then
    echo "WARNING: Container restarted $RESTARTS times"
    FAILED=1
  fi
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
  echo "SUCCESS: Proxy survived ${DURATION_SECS}s of HTTP/2 stress with ${MEMORY_LIMIT} limit"
  exit 0
else
  echo ""
  echo "FAIL: Proxy failed during HTTP/2 stress test"
  exit 1
fi
