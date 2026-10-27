#!/usr/bin/env bash
# Runs the DMQ interoperability tests against a real Intersect dmq-node.
#
# It downloads a dmq-node release (checking its SHA-256), builds Linux builds
# of Dingo and of the dmq-submit helper, starts Dingo's single-node devnet and
# dmq-node together in one container, and runs the tests tagged
# dingo_dmq_interop against them. Docker and network access are required.
#
#   internal/test/dmqinterop/run.sh
#
# Environment:
#   DMQ_NODE_VERSION       dmq-node release to test (default 0.7.2.1)
#   DMQ_INTEROP_NODE_PORT  host port for dmq-node's peer port (default 3141)
#   DMQ_INTEROP_LISTEN_PORT host port Dingo listens on for dmq-node to dial
#                          in (default 3999)
#   DMQ_INTEROP_TIMEOUT    how long a test waits for the devnet to give the
#                          pool stake dmq-node accepts (default 30m)
#   DMQ_INTEROP_WORKDIR    where to keep downloads and builds (default: a new
#                          temporary directory)
#   DMQ_INTEROP_KEEP       set to keep the container and workdir afterwards
#   DMQ_INTEROP_SETUP_ONLY set to start the environment and print the
#                          variables the tests need, without running them; the
#                          container is kept, so the tests can be rerun with
#                          go test -tags dingo_dmq_interop and those variables
#   DMQ_NODE_SEVERITY      dmq-node's log level (default Info; Debug shows
#                          every mini-protocol message)
set -euo pipefail

version="${DMQ_NODE_VERSION:-0.7.2.1}"
node_port="${DMQ_INTEROP_NODE_PORT:-3141}"
listen_port="${DMQ_INTEROP_LISTEN_PORT:-3999}"
dmq_magic=2147483650
devnet_magic=42
severity="${DMQ_NODE_SEVERITY:-Info}"
if [ -n "${DMQ_INTEROP_SETUP_ONLY:-}" ]; then DMQ_INTEROP_KEEP=1; fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
work="${DMQ_INTEROP_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/dmq-interop.XXXXXX")}"
name="dmq-interop-$$"

case "$(uname -m)" in
  arm64 | aarch64) asset=dmq-node-linux-aarch64; goarch=arm64 ;;
  x86_64 | amd64) asset=dmq-node-linux; goarch=amd64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- dingo log (tail)" >&2
    docker exec "$name" tail -n 20 /data/dingo.log 2>/dev/null >&2 || true
    echo "--- dmq-node log (tail)" >&2
    docker exec "$name" tail -n 30 /data/dmq.log 2>/dev/null >&2 || true
  fi
  if [ -z "${DMQ_INTEROP_KEEP:-}" ]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
    if [ -z "${DMQ_INTEROP_WORKDIR:-}" ]; then rm -rf "$work"; fi
  else
    echo "kept container $name and $work" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "the docker daemon is not running" >&2; exit 1; }

mkdir -p "$work/download" "$work/dmq-node" "$work/bin" "$work/cfg" "$work/keys"

echo "== downloading dmq-node $version ($asset)"
base="https://github.com/IntersectMBO/dmq-node/releases/download/$version"
curl -fsSL -o "$work/download/$asset.tar.gz" "$base/$asset.tar.gz"
curl -fsSL -o "$work/download/$asset.tar.gz.sha256sum.txt" \
  "$base/$asset.tar.gz.sha256sum.txt"
if command -v sha256sum >/dev/null; then
  (cd "$work/download" && sha256sum -c "$asset.tar.gz.sha256sum.txt")
else
  (cd "$work/download" && shasum -a 256 -c "$asset.tar.gz.sha256sum.txt")
fi
if tar -tzf "$work/download/$asset.tar.gz" | grep -E '^/|(^|/)\.\.(/|$)'; then
  echo "the archive holds an absolute or parent path" >&2
  exit 1
fi
tar -xzf "$work/download/$asset.tar.gz" -C "$work/dmq-node"

echo "== building dingo and dmq-submit for linux/$goarch"
(
  cd "$root"
  GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
    go build -o "$work/bin/dingo" ./cmd/dingo
  GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
    go build -o "$work/bin/dmq-submit" ./internal/test/dmqinterop/dmq-submit
)

echo "== starting the container"
docker run -d --name "$name" \
  -p "$node_port:3141" \
  --add-host host.docker.internal:host-gateway \
  -v "$work/dmq-node/result/bin:/opt/dmq:ro" \
  -v "$work/bin:/opt/bin:ro" \
  -v "$work/cfg:/etc/dmq" \
  ubuntu:24.04 sleep infinity >/dev/null

echo "== starting dingo's devnet (network magic $devnet_magic)"
docker exec -d "$name" sh -c \
  'mkdir -p /data && cd /data && /opt/bin/dingo devnet --data-dir /data/state > /data/dingo.log 2>&1'
for _ in $(seq 1 60); do
  if docker exec "$name" test -S /data/dingo.socket; then break; fi
  sleep 1
done
docker exec "$name" test -S /data/dingo.socket \
  || { echo "dingo did not open its socket" >&2; exit 1; }
docker cp "$name:/data/state/cardano/keys/." "$work/keys/"
chmod 600 "$work"/keys/*
docker exec "$name" cp /data/state/cardano/shelley-genesis.json \
  /etc/dmq/devnet-shelley-genesis.json

cat > "$work/cfg/config.json" <<JSON
{
  "LedgerPeers": false,
  "ShelleyGenesisFile": "/etc/dmq/devnet-shelley-genesis.json",
  "TraceOptions": {
    "": { "backends": ["Stdout HumanFormatUncoloured"], "severity": "$severity" }
  }
}
JSON
cat > "$work/cfg/topology.json" <<JSON
{
  "localRoots": [
    {
      "accessPoints": [
        { "address": "host.docker.internal", "port": $listen_port }
      ],
      "advertise": false,
      "trustable": true,
      "valency": 1
    }
  ],
  "publicRoots": []
}
JSON

echo "== starting dmq-node (DMQ network magic $dmq_magic)"
docker exec -d "$name" sh -c \
  "/opt/dmq/dmq-node --host-addr 0.0.0.0 --port 3141 \
     --dmq-network-magic $dmq_magic --cardano-network-magic $devnet_magic \
     --cardano-node-socket /data/dingo.socket --local-socket /data/dmq.sock \
     -c /etc/dmq/config.json -t /etc/dmq/topology.json > /data/dmq.log 2>&1"
for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/127.0.0.1/$node_port") 2>/dev/null; then break; fi
  sleep 1
done
(echo > "/dev/tcp/127.0.0.1/$node_port") 2>/dev/null \
  || { echo "dmq-node did not open its peer port" >&2; exit 1; }

export DMQ_INTEROP_NODE_ADDR="127.0.0.1:$node_port"
export DMQ_INTEROP_LISTEN_ADDR="0.0.0.0:$listen_port"
export DMQ_INTEROP_KEYS="$work/keys"
export DMQ_INTEROP_CFG="$work/cfg"
export DMQ_INTEROP_CONTAINER="$name"

if [ -n "${DMQ_INTEROP_SETUP_ONLY:-}" ]; then
  echo "== environment is up; rerun the tests with:"
  for v in DMQ_INTEROP_NODE_ADDR DMQ_INTEROP_LISTEN_ADDR DMQ_INTEROP_KEYS \
    DMQ_INTEROP_CFG DMQ_INTEROP_CONTAINER; do
    echo "export $v='${!v}'"
  done
  echo "go test -tags dingo_dmq_interop -count=1 -v ./internal/test/dmqinterop/"
  trap - EXIT
  exit 0
fi

echo "== running the tests (dmq-node needs two epoch changes, about 20 minutes, before it accepts the pool's messages)"
cd "$root"
go test -tags dingo_dmq_interop -count=1 -timeout 60m -v \
  ./internal/test/dmqinterop/
