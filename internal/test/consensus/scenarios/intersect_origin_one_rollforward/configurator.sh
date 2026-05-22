#!/usr/bin/env bash
#
# Per-scenario configurator for intersect_origin_one_rollforward.
#
# Single-pool genesis: invokes testnet-generation-tool with the
# scenario's testnet.yaml, applies common config tweaks (UTxO HD
# V2InMemory, stdout logging, neutered Conway committee, Shelley
# system-start delay so cardano-node has time to come up), and lands
# the pool's keys + configs under /configs/1/ for the cardano-node
# service to mount read-only.
#
# Inherits the cardano-foundation testnet-generation-tool baked into
# the shared internal/test/consensus/Dockerfile.configurator image;
# this script is mounted in at runtime.

set -euo pipefail

# Write-then-rename helper so we never observe a partial config file.
write_file() {
    local tmp_file="${1}_$(tr </dev/urandom -dc A-Za-z0-9 | head -c16)"
    cat >"${tmp_file}"
    mv --force "${tmp_file}" "${1}"
}

config_config_json() {
    local pool_dir="$1"
    local CONFIG_JSON="${pool_dir}/configs/config.json"

    # Strip absolute genesis hashes that genesis-cli.py bakes in
    # (cardano-node recomputes them on start).
    jq "del(.AlonzoGenesisHash, .ByronGenesisHash, .ConwayGenesisHash, .ShelleyGenesisHash)" \
        "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # Drop the in-memory EKG sink that the testnet-generation-tool
    # injects; it's not useful for capture and slows boot.
    jq "del(.hasEKG)" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
    jq "del(.options.mapBackends)" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # Redirect logging to stdout so `docker compose logs -f` sees it.
    jq '.minSeverity = "Info"
        | .defaultScribes = [["StdoutSK", "stdout"]]
        | .setupScribes = [{
            "scKind": "StdoutSK",
            "scName": "stdout",
            "scFormat": "ScText",
            "scRotation": null
          }]' "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"

    # Standalone capture stack: no peer-sharing, V2 in-memory ledger.
    jq ".PeerSharing = false" "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
    jq '.LedgerDB = { Backend: "V2InMemory" }' "${CONFIG_JSON}" \
        | write_file "${CONFIG_JSON}"
}

config_conway_genesis() {
    # Strip the configurator's default 7-member script-hash committee
    # so the chain doesn't sit in HFI ratification limbo. Same patch
    # erastest uses; documented there in detail.
    local pool_dir="$1"
    local CONWAY_GENESIS_JSON="${pool_dir}/configs/conway-genesis.json"
    jq '.committee.members = {} |
        .committee.threshold = {numerator: 0, denominator: 1} |
        .committeeMinSize = 0' \
        "${CONWAY_GENESIS_JSON}" | write_file "${CONWAY_GENESIS_JSON}"
}

compute_start_time() {
    # genesis-cli.py's systemStartDelay (5s) is too short because key
    # generation takes 30+ seconds. Set system start to now + 60s so
    # cardano-node has time to come up before the first leader slot.
    SYSTEM_START_UNIX=$(( $(date +%s) + 60 ))
    SYSTEM_START_ISO="$(date -d @${SYSTEM_START_UNIX} -u '+%Y-%m-%dT%H:%M:%SZ')"
}

set_start_time() {
    local pool_dir="$1"
    local SHELLEY_GENESIS_JSON="${pool_dir}/configs/shelley-genesis.json"
    local BYRON_GENESIS_JSON="${pool_dir}/configs/byron-genesis.json"
    jq ".systemStart = \"${SYSTEM_START_ISO}\"" \
        "${SHELLEY_GENESIS_JSON}" | write_file "${SHELLEY_GENESIS_JSON}"
    jq ".startTime = ${SYSTEM_START_UNIX}" \
        "${BYRON_GENESIS_JSON}" | write_file "${BYRON_GENESIS_JSON}"
}

cp /testnet.yaml ./testnet.yaml
uv run python3 genesis-cli.py testnet.yaml -o /tmp/testnet -c generate

# Strip the configurator's default per-pool topology.json — the
# scenario mounts its own static topology at runtime.
find /tmp/testnet -type f -name 'topology.json' -exec rm -f '{}' ';'

mkdir -p /configs
cp -r /tmp/testnet/pools/* /configs
mkdir -p /configs/utxo-keys
cp -r /tmp/testnet/utxos/keys/* /configs/utxo-keys/
rm -rf /configs/keys

compute_start_time
echo "system start: ${SYSTEM_START_ISO} (unix: ${SYSTEM_START_UNIX})"

POOL_DIR=/configs/1
set_start_time "${POOL_DIR}"
config_config_json "${POOL_DIR}"
config_conway_genesis "${POOL_DIR}"

# World-readable configs (test-only credentials). Keys directory needs
# tight perms or cardano-node refuses to load the VRF skey.
find /configs -type d -exec chmod 0755 {} +
find /configs -type f -exec chmod 0644 {} +
if [[ -d /configs/1/keys ]]; then
    chmod 0700 /configs/1/keys
    find /configs/1/keys -type f -exec chmod 0600 {} +
fi
