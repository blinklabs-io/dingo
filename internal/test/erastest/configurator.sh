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

    # Redirect cardano-node logging to stdout so `docker compose logs -f`
    # sees it. genesis-cli.py points defaultScribes/setupScribes at a
    # FileSK under /tmp/testnet/deployment, which is invisible from outside
    # the container. Also force minSeverity=Info to match the requested
    # log level (the iohk-monitoring default can be Notice).
    jq '.minSeverity = "Info"
        | .defaultScribes = [["StdoutSK", "stdout"]]
        | .setupScribes = [{
            "scKind": "StdoutSK",
            "scName": "stdout",
            "scFormat": "ScText",
            "scRotation": null
          }]' "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

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

config_conway_genesis() {
    # Strip the configurator's default 7-member script-hash committee.
    # testnet-generation-tool seeds conway-genesis.committee.members
    # with seven scriptHash entries (minSize=7, threshold=2/3) for which
    # we have no signing material, so any HFI gov-action ratification
    # under that committee is unmeetable. Drop to a NoCommittee state
    # (empty members, minSize=0, threshold 0/1) so cardano-ledger Conway
    # gov rules grant CC approval automatically and the vanrossem test
    # driver only has to clear the SPO + DRep thresholds. The eras
    # testnet does not exercise governance, so this clearing has no
    # observable effect on its assertions.
    CONWAY_GENESIS_JSON=$1/configs/conway-genesis.json
    jq '.committee.members = {} |
        .committee.threshold = {numerator: 0, denominator: 1} |
        .committeeMinSize = 0' \
        "${CONWAY_GENESIS_JSON}" | write_file "${CONWAY_GENESIS_JSON}"
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
    # Set system start to now + 60s to give Docker time to start node
    # containers, the relay to come up, and producer→relay handshakes to
    # complete before the first leader slot fires. With less headroom,
    # both producers race to forge slot-1 on top of an empty genesis,
    # spend the early Shelley epoch in fork-resolution churn, and the
    # locally-forged blocks of the slower-to-stabilize node get rolled
    # back. genesis-cli.py's systemStartDelay (5s) is too short because
    # key generation takes 30+ seconds, so we override after generation.
    SYSTEM_START_UNIX=$(( $(date +%s) + 60 ))
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

# testnet-generation-tool drops the genesis-utxo Shelley payment / stake /
# genesis spending keys under /tmp/testnet/utxos/keys/. The docker-compose
# definition mounts an "utxo-keys" volume at /configs/utxo-keys for these,
# so route the keys there explicitly — a `cp -r /tmp/testnet/utxos/* /configs`
# silently misses the mount because it lands the files at /configs/keys (a
# local in-container path that vanishes with the configurator container).
mkdir -p /configs/utxo-keys
cp -r /tmp/testnet/utxos/keys/* /configs/utxo-keys/

echo "removing /configs/keys"; rm -rf /configs/keys

pools=$(ls -d /configs/[0-9]*)
number_of_pools=$(ls -d /configs/[0-9]* | wc -l)
echo "number_of_pools: $number_of_pools"

# Generate ring topology for all pools (writes all files in one pass)
config_topology_json "$number_of_pools"

# Override system start time AFTER key generation completes.
# genesis-cli.py's systemStartDelay (5s) is too short because key generation
# takes 30+ seconds. Set genesis to now + 60s to give Docker time to start
# the node containers after the configurator exits.
compute_start_time
echo "system start: ${SYSTEM_START_ISO} (unix: ${SYSTEM_START_UNIX})"

for pool in $pools; do
  echo "pool: $pool"
  set_start_time "$pool"
  config_config_json "$pool"
  config_conway_genesis "$pool"
done

# Test-only credentials: make config + genesis files world-readable so any
# consuming container's user can read them.
find /configs -type d -exec chmod 0755 {} +
find /configs -type f -exec chmod 0644 {} +

# cardano-node refuses to start when vrf.skey has "other" read permissions,
# so the per-pool keys directories must be 0700/0600. Pool 1 is consumed by
# the dingo container (uid 100, gid 101 in the dingo image), so chown it to
# match. Pool 2 stays root-owned for the cardano-producer container, which
# runs as root.
for pool_dir in /configs/[0-9]*; do
    keys_dir="$pool_dir/keys"
    if [ -d "$keys_dir" ]; then
        chmod 0700 "$keys_dir"
        find "$keys_dir" -type f -exec chmod 0600 {} +
    fi
done
if [ -d /configs/1/keys ]; then
    chown -R 100:101 /configs/1/keys
fi
