#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yaml"

echo "=== Building images ==="
docker compose -f "$COMPOSE_FILE" build

echo "=== Starting antithesis devnet ==="
docker compose -f "$COMPOSE_FILE" up -d

echo "=== Waiting for nodes to become healthy ==="
MAX_WAIT=180
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    HEALTHY=$(docker compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null | python3 -c "
import sys, json
raw = sys.stdin.read().strip()
if not raw:
    data = []
elif raw.startswith('['):
    data = json.loads(raw)
else:
    data = [json.loads(line) for line in raw.splitlines() if line.strip()]
print(sum(1 for d in data if d.get('Health','') == 'healthy'))
" 2>/dev/null || echo 0)
    echo "  Healthy nodes: $HEALTHY/5 (${ELAPSED}s)"
    if [ "$HEALTHY" -ge 5 ]; then
        echo "=== All nodes healthy ==="
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ "$HEALTHY" -lt 5 ]; then
    echo "ERROR: Not all nodes became healthy within ${MAX_WAIT}s"
    docker compose -f "$COMPOSE_FILE" ps
    docker compose -f "$COMPOSE_FILE" logs --tail=50
    docker compose -f "$COMPOSE_FILE" down -v
    exit 1
fi

echo "=== Antithesis devnet running ==="
echo "  p1 (dingo):        localhost:${P1_PORT:-3010} (3001)"
echo "  p2 (cardano-node): localhost:${P2_PORT:-3011} (3001)"
echo "  p3 (cardano-node): localhost:${P3_PORT:-3012} (3001)"
echo "  p4 (cardano-node): localhost:${P4_PORT:-3013} (3001)"
echo "  p5 (cardano-node): localhost:${P5_PORT:-3014} (3001)"
echo "  txpump:            (internal, connects to p1 via socket)"
echo ""
echo "Press Ctrl+C to stop"

cleanup() {
    echo ""
    echo "=== Stopping antithesis devnet ==="
    docker compose -f "$COMPOSE_FILE" down -v
}
trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" logs -f
