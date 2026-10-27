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
#   DMQ_INTEROP_CARDANO_BIN path to a Linux dingo binary for the Cardano node
#                          dmq-node talks to (default: built from this tree).
#                          Dingo closes a node-to-client connection that has
#                          been idle for two minutes, and dmq-node holds one
#                          open and idle between epochs, so dmq-node exits
#                          within minutes against a build with that limit. Use
#                          a build from before it until dmq-node is exempted.
#   DMQ_INTEROP_NODE_MODE  set to also run a native Dingo with dmq.enabled,
#                          which dials dmq-node from its topology and listens
#                          on DMQ_INTEROP_LISTEN_PORT, and to run only the
#                          tests that use it. It binds Dingo's default ports
#                          (3001, 3002 and 12798) on this machine.
#   DMQ_INTEROP_METRICS_PORT the node's metrics port in node mode (default
#                          12798), for a machine where that port is in use.
set -euo pipefail

version="${DMQ_NODE_VERSION:-0.7.2.1}"
node_port="${DMQ_INTEROP_NODE_PORT:-3141}"
listen_port="${DMQ_INTEROP_LISTEN_PORT:-3999}"
metrics_port="${DMQ_INTEROP_METRICS_PORT:-12798}"
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

node_pid=""
node_dir=""

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- dingo log (tail)" >&2
    docker exec "$name" tail -n 20 /data/dingo.log 2>/dev/null >&2 || true
    echo "--- dmq-node log (tail)" >&2
    docker exec "$name" tail -n 30 /data/dmq.log 2>/dev/null >&2 || true
    if [ -n "$node_dir" ]; then
      echo "--- dingo node log (tail)" >&2
      tail -n 30 "$node_dir/dingo.log" 2>/dev/null >&2 || true
    fi
  fi
  if [ -z "${DMQ_INTEROP_KEEP:-}" ]; then
    if [ -n "$node_pid" ]; then kill -TERM "$node_pid" 2>/dev/null || true; fi
    docker rm -f "$name" >/dev/null 2>&1 || true
    if [ -n "$node_dir" ]; then
      for _ in $(seq 1 60); do
        kill -0 "$node_pid" 2>/dev/null || break
        sleep 1
      done
      rm -rf "$node_dir"
    fi
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
  if [ -n "${DMQ_INTEROP_CARDANO_BIN:-}" ]; then
    cp "$DMQ_INTEROP_CARDANO_BIN" "$work/bin/dingo"
  else
    GOOS=linux GOARCH="$goarch" CGO_ENABLED=0 \
      go build -o "$work/bin/dingo" ./cmd/dingo
  fi
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

start_node() {
  # The node gets a short directory: a Unix socket path is limited to about
  # a hundred bytes.
  node_dir="$(mktemp -d "${TMPDIR:-/tmp}/dmq-node.XXXXXX")"
  node_dir="$(cd "$node_dir" && pwd -P)"
  echo "== building a native dingo for node mode"
  (
    cd "$root"
    go build -o "$work/bin/dingo-host" ./cmd/dingo
    go build -o "$work/bin/dmq-submit-host" ./internal/test/dmqinterop/dmq-submit
  )
  # The devnet command writes the node's configuration and keys and starts
  # it. It strips every DINGO_ variable from the node's environment, so DMQ is
  # turned on in the written configuration instead, and the node restarted.
  # exec makes the subshell the node, so $! is the node's own process ID.
  (cd "$node_dir" && exec "$work/bin/dingo-host" devnet \
    --data-dir "$node_dir/state" > "$node_dir/devnet.log" 2>&1) &
  echo $! > "$node_dir/devnet.pid"
  for _ in $(seq 1 60); do
    if [ -S "$node_dir/dingo.socket" ]; then break; fi
    sleep 1
  done
  [ -S "$node_dir/dingo.socket" ] \
    || { echo "the devnet node did not start" >&2; exit 1; }
  # A job the shell runs in the background may ignore SIGINT, so it is
  # stopped with SIGTERM; the command waits for its node, then exits.
  devnet_pid="$(cat "$node_dir/devnet.pid")"
  kill -TERM "$devnet_pid"
  for _ in $(seq 1 120); do
    if ! kill -0 "$devnet_pid" 2>/dev/null; then break; fi
    sleep 1
  done
  if kill -0 "$devnet_pid" 2>/dev/null; then
    echo "the devnet node did not stop" >&2
    exit 1
  fi
  cat > "$work/cfg/dmq-topology-node.json" <<JSON
{
  "localRoots": [
    {
      "accessPoints": [{ "address": "127.0.0.1", "port": $node_port }],
      "valency": 1
    }
  ],
  "publicRoots": []
}
JSON
  cat >> "$node_dir/state/dingo.yaml" <<YAML
metricsPort: $metrics_port
dmq:
  enabled: true
  topic: mithril
  networkMagic: $dmq_magic
  socketPath: dingo-dmq.socket
  n2nPort: $listen_port
  listenAddress: 0.0.0.0
  listenIpv6Address: ""
  topologyFile: $work/cfg/dmq-topology-node.json
  minPeers: 1
  maxPeers: 5
YAML
  echo "== starting a native dingo with dmq.enabled (DMQ network magic $dmq_magic)"
  (cd "$node_dir" && exec "$work/bin/dingo-host" \
    --config "$node_dir/state/dingo.yaml" > "$node_dir/dingo.log" 2>&1) &
  node_pid=$!
  for _ in $(seq 1 90); do
    if [ -S "$node_dir/dingo-dmq.socket" ]; then break; fi
    sleep 1
  done
  [ -S "$node_dir/dingo-dmq.socket" ] \
    || { echo "the dingo node did not open its DMQ socket" >&2; exit 1; }
}

if [ -n "${DMQ_INTEROP_NODE_MODE:-}" ]; then
  start_node
  export DMQ_INTEROP_NODE_SOCKET="$node_dir/dingo-dmq.socket"
  export DMQ_INTEROP_NODE_SUBMIT="$work/bin/dmq-submit-host"
  export DMQ_INTEROP_NODE_METRICS="http://127.0.0.1:${metrics_port}/metrics"
fi

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
  if [ -n "${DMQ_INTEROP_NODE_MODE:-}" ]; then
    for v in DMQ_INTEROP_NODE_SOCKET DMQ_INTEROP_NODE_SUBMIT \
      DMQ_INTEROP_NODE_METRICS; do
      echo "export $v='${!v}'"
    done
    echo "# the node runs as process $node_pid; stop it with kill -TERM"
  fi
  echo "go test -tags dingo_dmq_interop -count=1 -v ./internal/test/dmqinterop/"
  trap - EXIT
  exit 0
fi

echo "== running the tests (dmq-node needs two epoch changes, about 20 minutes, before it accepts the pool's messages)"
cd "$root"
run_filter=()
if [ -n "${DMQ_INTEROP_NODE_MODE:-}" ]; then
  # The other tests listen on the port the node uses.
  run_filter=(-run 'TestInteropNode')
fi
go test -tags dingo_dmq_interop -count=1 -timeout 60m -v \
  ${run_filter[@]+"${run_filter[@]}"} ./internal/test/dmqinterop/
