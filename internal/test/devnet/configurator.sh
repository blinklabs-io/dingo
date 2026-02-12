#!/usr/bin/env bash
set -euo pipefail

UTXO_HD_WITH="mem"

# Log file

# Implement sponge-like command without the need for binary nor TMPDIR environment variable
write_file() {
    # Create temporary file
    local tmp_file="${1}_$(tr </dev/urandom -dc A-Za-z0-9 | head -c16)"

    # Redirect the output to the temporary file
    cat >"${tmp_file}"

    # Replace the original file
    mv --force "${tmp_file}" "${1}"
}

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
    PEER_SHARING="${PEER_SHARING:-true}"
    CONFIG_JSON=$1/configs/config.json
    # .AlonzoGenesisHash, .ByronGenesisHash, .ConwayGenesisHash, .ShelleyGenesisHash
    jq "del(.AlonzoGenesisHash, .ByronGenesisHash, .ConwayGenesisHash, .ShelleyGenesisHash)" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # .hasEKG
    jq "del(.hasEKG)" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # .options.mapBackends
    jq "del(.options.mapBackends)" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # .PeerSharing
    if [ "${PEER_SHARING,,}" = "true" ]; then
        jq ".PeerSharing = true" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
    else
        jq ".PeerSharing = false" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
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
    case "${UTXO_HD_WITH,,}" in
        hd)
            jq ".LedgerDB = { Backend: \"V1LMDB\", LiveTablesPath: \"/state/lmdb\"}" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
            ;;
        *)
            jq '.LedgerDB = { Backend: "V2InMemory"}' "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
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
    jq ".systemStart = \"${SYSTEM_START_ISO}\"" "${SHELLEY_GENESIS_JSON}" | write_file "${SHELLEY_GENESIS_JSON}"

    # .startTime
    jq ".startTime = ${SYSTEM_START_UNIX}" "${BYRON_GENESIS_JSON}" | write_file "${BYRON_GENESIS_JSON}"
}


# # Copy testnet.yaml specification
cp /testnet.yaml ./testnet.yaml

# # Build testnet configuration files
uv run python3 genesis-cli.py testnet.yaml -o /tmp/testnet -c generate

# # Remove dynamic topology.json
find /tmp/testnet -type f -name 'topology.json' -exec rm -f '{}' ';'

mkdir -p /configs
cp -r /tmp/testnet/pools/* /configs
cp -r /tmp/testnet/utxos/* /configs

echo "removing /configs/keys"; rm -rf /configs/keys

pools=$(ls -d /configs/*)
number_of_pools=$(ls -d /configs/* | wc -l)
echo "number_of_pools: $number_of_pools"

# Generate ring topology for all pools (writes all files in one pass)
config_topology_json "$number_of_pools"

# Override system start time AFTER key generation completes.
# genesis-cli.py's systemStartDelay (5s) is too short because key generation
# takes 30+ seconds. Set genesis to now + 30s to give Docker time to start
# the node containers after the configurator exits.
compute_start_time
echo "system start: ${SYSTEM_START_ISO} (unix: ${SYSTEM_START_UNIX})"

for pool in $pools; do
  echo "pool: $pool"
  set_start_time "$pool"
  config_config_json "$pool"
done
