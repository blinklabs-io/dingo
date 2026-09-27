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

mkdir -p /configs /configs/utxo-keys
cp -r /tmp/testnet/pools/* /configs

echo "removing /configs/keys"; rm -rf /configs/keys

pools=$(ls -d /configs/[0-9]*)
number_of_pools=$(ls -d /configs/[0-9]* | wc -l)
echo "number_of_pools: $number_of_pools"

# Generate ring topology for all pools (writes all files in one pass)
config_topology_json "$number_of_pools"

# Override system start time AFTER key generation completes.
# genesis-cli.py's systemStartDelay (5s) is too short because key generation
# takes 30+ seconds. Set genesis to now + 30s to give Docker time to start
# the node containers after the configurator exits.
compute_start_time
echo "system start: ${SYSTEM_START_ISO} (unix: ${SYSTEM_START_UNIX})"

# Publish the actual runtime start (which overrides the generator's short
# delay) for txpump. Docker can mark every node healthy before this timestamp,
# so service health alone is not a safe transaction-submission barrier. The
# shared UTxO volume carries this generated file to txpump without introducing
# another volume solely for runtime metadata.
cp /testnet.yaml /configs/utxo-keys/runtime-genesis
# genesis.Load reads systemStartUnix from the first YAML document. Preserve the
# original testnet specification and add the exact timestamp used above.
sed -i "/^systemStartDelay:/a systemStartUnix: ${SYSTEM_START_UNIX}" \
  /configs/utxo-keys/runtime-genesis

# The generator leaves Byron bootStakeholders empty. Dingo builds the Byron
# PBFT trust-root set at startup even though this network hard-forks to
# Shelley at epoch zero, so give each generated genesis a valid temporary
# issuer key hash. No Byron blocks use this key.
BYRON_BOOTSTRAP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dingo-byron-bootstrap.XXXXXX")"
trap 'rm -rf "${BYRON_BOOTSTRAP_DIR}"' EXIT
BYRON_BOOTSTRAP_SKEY="${BYRON_BOOTSTRAP_DIR}/bootstrap.skey"
BYRON_BOOTSTRAP_VKEY="${BYRON_BOOTSTRAP_DIR}/bootstrap.vkey"
cardano-cli byron key keygen --secret "${BYRON_BOOTSTRAP_SKEY}"
cardano-cli byron key to-verification \
  --byron-formats \
  --secret "${BYRON_BOOTSTRAP_SKEY}" \
  --to "${BYRON_BOOTSTRAP_VKEY}"
BYRON_BOOTSTRAP_HASH="$(python3 - "${BYRON_BOOTSTRAP_VKEY}" <<'PYHASH'
import base64
import hashlib
import pathlib
import sys

key = base64.b64decode(pathlib.Path(sys.argv[1]).read_text().strip(), validate=True)
if len(key) != 64:
    raise SystemExit(f"unexpected Byron verification key length: {len(key)}")
cbor_bytes = bytes((0x58, len(key))) + key
print(hashlib.blake2b(hashlib.sha3_256(cbor_bytes).digest(), digest_size=28).hexdigest())
PYHASH
)"

for pool in $pools; do
  echo "pool: $pool"
  byron_genesis="${pool}/configs/byron-genesis.json"
  jq --arg key "${BYRON_BOOTSTRAP_HASH}" \
    '.bootStakeholders[$key] = 1' \
    "${byron_genesis}" | write_file "${byron_genesis}"
  set_start_time "$pool"
  config_config_json "$pool"
done

# Expose the Shelley genesis (updated system start) to txpump so it can
# discover the initial UTxOs from initialFunds and know the genesis start time.
cp /configs/1/configs/shelley-genesis.json /configs/utxo-keys/

# Expose genesis UTxO signing keys so txpump can sign transactions spending
# the generated genesis enterprise addresses.
cp /tmp/testnet/utxos/keys/genesis.*.skey /configs/utxo-keys/
cp /tmp/testnet/utxos/keys/genesis.*.vkey /configs/utxo-keys/
cp /tmp/testnet/utxos/keys/genesis.*.addr.info /configs/utxo-keys/

# Expose the generated cold keys only to the isolated accelerated governance
# scenario, which signs SPO voting procedures with the actual pool credentials.
mkdir -p /configs/utxo-keys/pool-keys
for pool in $pools; do
    pool_id="${pool##*/}"
    cp "${pool}/keys/cold.skey" "/configs/utxo-keys/pool-keys/pool-${pool_id}.skey"
    cp "${pool}/keys/cold.vkey" "/configs/utxo-keys/pool-keys/pool-${pool_id}.vkey"
done

# Expose genesis stake verification keys and delegated address info so the
# dingo-only harness can derive the stake credentials that genesis delegated
# to each pool. The generator's stake key layout is discovered in the CIP-50
# task; copy every plausible stake artifact so the loader can find them.
mkdir -p /configs/utxo-keys/stake
find /tmp/testnet -type f \( -name '*stake*.skey' -o -name '*stake*.vkey' -o -name '*stake*.addr*' \) \
    -exec cp {} /configs/utxo-keys/stake/ \; 2>/dev/null || true

# Test-only credentials: make config + genesis files world-readable so any
# consuming container's user can read them.
find /configs -type d -exec chmod 0755 {} +
find /configs -type f -exec chmod 0644 {} +

# Pool cold keys are used only by the host-side governance scenario. Keep
# them unreadable to node containers sharing the UTxO-key volume.
if [ -d /configs/utxo-keys/pool-keys ]; then
    chmod 0700 /configs/utxo-keys/pool-keys
    find /configs/utxo-keys/pool-keys -type f -name '*.skey' \
        -exec chmod 0600 {} +
fi

# cardano-node refuses to start when vrf.skey has "other" read permissions,
# so the per-pool keys directories must be 0700/0600. Pools listed in
# DINGO_POOL_IDS are consumed by dingo containers, which run as a non-root
# user, and get chowned below to match. Any pool NOT listed stays
# root-owned for its cardano-node container, which runs as root - e.g. pool
# 2 (and 3) in conformance mode, where DINGO_POOL_IDS defaults to "1".
for pool_dir in /configs/[0-9]*; do
    keys_dir="$pool_dir/keys"
    if [ -d "$keys_dir" ]; then
        chmod 0700 "$keys_dir"
        find "$keys_dir" -type f -exec chmod 0600 {} +
    fi
done
# Chown the key dirs of pools consumed by dingo containers to the dingo
# image's uid/gid. Conformance mode sets DINGO_POOL_IDS="1" (only pool 1
# is dingo); the all-dingo configurator sets "1 2 3".
#
# The uid/gid come from docker-compose.yml rather than being hardcoded
# here, because the authoritative value is the `adduser --uid` pin in the
# repo root Dockerfile and the two must not drift. They did drift once:
# the image moved from uid 100 to 1000 while this script still chowned to
# 100:101, so every Dingo block producer failed startup with
# "failed to read key file .../vrf.skey: permission denied" — the keys
# directory is 0700 by necessity, since cardano-node refuses to start when
# vrf.skey is group- or world-readable.
DINGO_POOL_IDS="${DINGO_POOL_IDS:-1}"
DINGO_UID="${DINGO_UID:-1000}"
DINGO_GID="${DINGO_GID:-1000}"
echo "chowning dingo pool keys to ${DINGO_UID}:${DINGO_GID} (pools: ${DINGO_POOL_IDS})"
for id in ${DINGO_POOL_IDS}; do
    if [ -d "/configs/${id}/keys" ]; then
        chown -R "${DINGO_UID}:${DINGO_GID}" "/configs/${id}/keys"
    fi
done
