#!/usr/bin/env bash
set -euo pipefail

UTXO_HD_WITH="${UTXO_HD_WITH:-mem}"

# Updates specific node's configuration depending on environment variables
# Those environment variables can be set in the service definition in the
# docker-compose file like:
#
# ```
# p2:
#   <<: *base
#   container_name: p2
#   hostname: p2.example
#   volumes:
#     - p2:/opt/cardano-node/data
#   ports:
#     - "3002:3001"
#   environment:
#     <<: *env
#     POOL_ID: "2"
#     PEER_SHARING: "false"
# ```
config_config_json() {
    local config_dir="$1"
    local peer_sharing="${2:-true}"
    CONFIG_JSON=$config_dir/configs/config.json
    # .AlonzoGenesisHash, .ByronGenesisHash, .ConwayGenesisHash, .ShelleyGenesisHash
    tmp="${CONFIG_JSON}.tmp.$$"
    jq "del(.AlonzoGenesisHash, .ByronGenesisHash, .ConwayGenesisHash, .ShelleyGenesisHash)" "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"

    # .hasEKG
    tmp="${CONFIG_JSON}.tmp.$$"
    jq "del(.hasEKG)" "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"

    # .options.mapBackends
    tmp="${CONFIG_JSON}.tmp.$$"
    jq "del(.options.mapBackends)" "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"

    # .PeerSharing
    if [ "${peer_sharing,,}" = "true" ]; then
        tmp="${CONFIG_JSON}.tmp.$$"
        jq ".PeerSharing = true" "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"
    else
        tmp="${CONFIG_JSON}.tmp.$$"
        jq ".PeerSharing = false" "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"
    fi

    # configure UTxO-HD
    # see https://ouroboros-consensus.cardano.intersectmbo.org/docs/for-developers/utxo-hd/migrating
    # FIXME: /state needs to match the --database-path in the
    # node's command
    # FIXME: We want to be able to configure this for each node separately
    # FIXME: Btw also think about how to have a nice abstraction for generating
    # the configs
    # One alternative is:
    # UTXO_HD_WITH: "hd hd hd mem mem mem"
    database_path="${NODE_DATABASE_PATH:-${CARDANO_DATABASE_PATH:-${DATABASE_PATH:-/data/db}}}"
    lmdb_path="${database_path%/}/lmdb"
    case "${UTXO_HD_WITH,,}" in
        hd)
            tmp="${CONFIG_JSON}.tmp.$$"
            jq --arg lmdb_path "${lmdb_path}" '.LedgerDB = { Backend: "V1LMDB", LiveTablesPath: $lmdb_path }' "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"
            ;;
        *)
            tmp="${CONFIG_JSON}.tmp.$$"
            jq '.LedgerDB = { Backend: "V2InMemory"}' "${CONFIG_JSON}" > "$tmp" && mv --force "$tmp" "${CONFIG_JSON}"
            ;;
    esac
}

config_topology_json() {
    # Generate a ring topology, where pool_n is connected to pool_{n-1} and pool_{n+1}

    VALENCY=2

    local num_pools=$1
    local i prev next

    for ((i=1; i<=num_pools; i++)); do
        prev=$((i - 1))
        if [ $prev -eq 0 ]; then
            prev=$num_pools
        fi

        next=$((i + 1))
        if [ $next -gt $num_pools ]; then
            next=1
        fi

        cat <<EOF > "/configs/$i/configs/topology.json"
{
  "localRoots": [
    {
      "accessPoints": [
        {"address": "p${prev}.example", "port": 3001},
        {"address": "p${next}.example", "port": 3001}
      ],
    "advertise": true,
    "trustable": true,
    "valency": ${VALENCY}
    }
  ],
    "publicRoots": [],
    "useLedgerAfterSlot": 0
}
EOF
    done
}

config_dingo_topology() {
    # Generate a custom topology for p1 (dingo): connects to p2 and p5.
    # This matches the ring topology endpoints for p1 but is written as a
    # separate file that dingo can consume directly (plain JSON format).
    # Written to dingo-topology.json so it doesn't collide with the
    # cardano-node topology.json written by config_topology_json.

    cat <<EOF > "/configs/1/configs/dingo-topology.json"
{
  "localRoots": [
    {
      "accessPoints": [
        {"address": "p5.example", "port": 3001},
        {"address": "p2.example", "port": 3001}
      ],
    "advertise": true,
    "trustable": true,
    "valency": 2
    }
  ],
    "publicRoots": [],
    "useLedgerAfterSlot": 0
}
EOF
}

compute_start_time() {
    # Set system start to now + 30s to give Docker time to start node
    # containers after the configurator exits.
    # genesis-cli.py's systemStartDelay (5s) is too short because key
    # generation takes 30+ seconds, so we override after generation.
    SYSTEM_START_UNIX=$(( $(date +%s) + 30 ))
    SYSTEM_START_ISO="$(date -d @${SYSTEM_START_UNIX} -u '+%Y-%m-%dT%H:%M:%SZ')"
}

set_start_time() {
    # Apply the pre-computed start time to a pool's genesis files.
    # Must call compute_start_time first.
    SHELLEY_GENESIS_JSON="$1/configs/shelley-genesis.json"
    BYRON_GENESIS_JSON="$1/configs/byron-genesis.json"

    # .systemStart
    tmp="${SHELLEY_GENESIS_JSON}.tmp.$$"
    jq ".systemStart = \"${SYSTEM_START_ISO}\"" "${SHELLEY_GENESIS_JSON}" > "$tmp" && mv --force "$tmp" "${SHELLEY_GENESIS_JSON}"

    # .startTime
    tmp="${BYRON_GENESIS_JSON}.tmp.$$"
    jq ".startTime = ${SYSTEM_START_UNIX}" "${BYRON_GENESIS_JSON}" > "$tmp" && mv --force "$tmp" "${BYRON_GENESIS_JSON}"
}


# Copy testnet.yaml specification
cp /testnet.yaml ./testnet.yaml

# Build testnet configuration files
uv run python3 genesis-cli.py testnet.yaml -o /tmp/testnet -c generate

# Remove dynamic topology.json
find /tmp/testnet -type f -name 'topology.json' -exec rm -f '{}' ';'

mkdir -p /configs
# Copy the CONTENTS of each numbered pool directory into the corresponding
# mount point. testnet-generation-tool outputs pools/1/..., pools/2/... etc.
# The volumes are mounted at /configs/1, /configs/2, so we must copy the
# contents (not the directories themselves) to avoid nesting (e.g.
# /configs/1/1/configs/...).
for pool_dir in /tmp/testnet/pools/*/; do
    pool_num=$(basename "$pool_dir")
    mkdir -p "/configs/$pool_num"
    cp -r "$pool_dir"* "/configs/$pool_num/"
done
cp -r /tmp/testnet/utxos/* /configs

echo "removing /configs/keys"; rm -rf /configs/keys

# Filter to only numeric directories (skip utxo-keys, testnet-config, etc.)
pools=$(ls -d /configs/[0-9]*)
number_of_pools=$(ls -d /configs/[0-9]* | wc -l)
echo "number_of_pools: $number_of_pools"

# Generate ring topology for all pools (writes all files in one pass)
config_topology_json "$number_of_pools"

# Override p1 topology for dingo (connects to p2 and p5 explicitly)
config_dingo_topology

# Override system start time AFTER key generation completes.
# genesis-cli.py's systemStartDelay (5s) is too short because key generation
# takes 30+ seconds. Set genesis to now + 30s to give Docker time to start
# the node containers after the configurator exits.
compute_start_time
echo "system start: ${SYSTEM_START_ISO} (unix: ${SYSTEM_START_UNIX})"

for pool in $pools; do
  echo "pool: $pool"
  set_start_time "$pool"
  pool_num=$(basename "$pool")
  var_name="PEER_SHARING_${pool_num}"
  peer_sharing="${!var_name:-${PEER_SHARING:-true}}"
  config_config_json "$pool" "$peer_sharing"
done

# Copy UTxO keys to shared volume for txpump access
echo "copying utxo keys to /configs/utxo-keys"
cp -r /tmp/testnet/utxos /configs/utxo-keys

# Copy testnet.yaml to shared volume for analysis/txpump genesis config
echo "copying testnet.yaml to /testnet-config/testnet.yaml"
if [ -d /testnet-config ]; then
    cp /testnet.yaml /testnet-config/testnet.yaml
fi
