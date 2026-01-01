#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONTAINER_NAME="bq-runner"

cleanup() {
    echo "Stopping container..."
    podman compose -f "$PROJECT_ROOT/compose.yaml" down 2>/dev/null || true
}
trap cleanup EXIT

echo "=== bq-runner E2E Tests ==="
echo

# Start server via podman-compose
echo "Starting server container..."
cd "$PROJECT_ROOT"
podman compose up -d --build

# Wait for server to be ready
echo "Waiting for server..."
for i in {1..50}; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        echo "  Server ready"
        break
    fi
    if [ $i -eq 50 ]; then
        echo "  Timeout waiting for server"
        podman compose logs
        exit 1
    fi
    sleep 0.2
done

# Run Clojure tests
echo
echo "Running Clojure client tests..."
cd "$SCRIPT_DIR"
clojure -M:test

echo
echo "=== E2E Tests Complete ==="
