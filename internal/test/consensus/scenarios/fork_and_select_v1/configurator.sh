#!/usr/bin/env bash
#
# Per-scenario configurator for fork_and_select_v1.
#
# Generates a 2-pool genesis, then drives cardano-node through three
# forge phases to stage two divergent chains sharing a common prefix:
#
#   Phase A: pool 1 forges in isolation until tip slot >= PREFIX_KILL_SLOT.
#            Snapshot becomes the shared prefix.
#   Phase B: pool 1 extends the shared prefix until tip slot >=
#            PREFIX_KILL_SLOT + PEER_A_EXTENSION_SLOTS.
#            Snapshot becomes peer-a's chain DB.
#   Phase C: pool 2 extends the shared prefix until tip slot >=
#            PREFIX_KILL_SLOT + PEER_B_EXTENSION_SLOTS.
#            Snapshot becomes peer-b's chain DB. PEER_B_EXTENSION_SLOTS
#            is ~3x PEER_A_EXTENSION_SLOTS so peer B reliably wins
#            Praos longest-chain selection at the observation node.
#
# During each forge phase cardano-node runs as a subprocess of this
# script (not a sibling docker service) so the configurator owns the
# PID for SIGTERM and reads the local IPC socket directly. Clean
# shutdown is awaited via `wait` before snapshotting — premature
# snapshot would catch the volatile DB before the volatile-to-
# immutable flush completes.

set -euo pipefail

# ─── tunables ───────────────────────────────────────────────────────
# Slot counts deliberately kept small so each phase's wall-clock-vs-
# chain-slot gap stays inside cardano-node's GSM CaughtUp tolerance
# (~3k/f = ~45 slots at k=6, f=0.4). When the gap exceeds that, the
# Genesis State Machine stays in PreSyncing and refuses to forge.
# Rewriting systemStart between phases to close the gap is tempting
# but breaks chain validation: changing systemStart changes the
# (Byron) genesis hash, and cardano-node rejects the inherited
# chain DB on hash mismatch.
#
# At activeSlotsCoeff=0.4 with two equal-stake pools each pool wins
# ~0.225 of slots (1 - (1-0.4)^(1/2)). PEER_B_EXTENSION_SLOTS needs
# to be enough that peer B's expected block count is robustly more
# than peer A's — Praos longest-chain selection is BY BLOCK COUNT,
# not slot number, so if A and B end up with similar block counts
# the observation node may pick A on a run where pool 1's VRF wins
# happened to cluster early. With:
#   prefix expected blocks  ≈ 0.225 * 10 ≈ 2.3
#   peer A extension blocks ≈ 0.225 * 15 ≈ 3.4
#   peer B extension blocks ≈ 0.225 * 80 ≈ 18.0
# peer B has a ~14-block lead in expectation, far outside reasonable
# variance.
PREFIX_KILL_SLOT="${PREFIX_KILL_SLOT:-10}"
PEER_A_EXTENSION_SLOTS="${PEER_A_EXTENSION_SLOTS:-15}"
PEER_B_EXTENSION_SLOTS="${PEER_B_EXTENSION_SLOTS:-80}"
# Each phase's deadline. Phase C's kill slot is the biggest
# (PREFIX + B_EXT), so this must accommodate that wait.
PHASE_TIMEOUT_SECS="${PHASE_TIMEOUT_SECS:-180}"
NETWORK_MAGIC="${NETWORK_MAGIC:-42}"

# Paths inside the configurator container.
STAGING_ROOT=/staging
LOG_ROOT=/var/log
SHARED_PREFIX_DIR=/configs/shared-prefix-db
PEER_A_DATA_DIR=/peer-a-data
PEER_B_DATA_DIR=/peer-b-data

# ─── helpers ────────────────────────────────────────────────────────

# mktemp (rather than tr|head) avoids a SIGPIPE-induced exit-141
# under set -euo pipefail.
write_file() {
    local tmp_file
    tmp_file="$(mktemp "${1}.tmp.XXXXXXXX")"
    cat >"${tmp_file}"
    mv --force "${tmp_file}" "${1}"
}

log() { echo "[configurator] $*"; }

# config_config_json strips genesis-cli artifacts that confuse
# cardano-node on a forge-only run, neutralises peer-sharing, and
# pins the in-memory LedgerDB backend so the forge phases don't
# touch LMDB.
config_config_json() {
    local pool_dir="$1"
    local CONFIG_JSON="${pool_dir}/configs/config.json"
    jq 'del(.AlonzoGenesisHash, .ByronGenesisHash,
            .ConwayGenesisHash, .ShelleyGenesisHash)' \
        "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
    jq 'del(.hasEKG, .options.mapBackends)' \
        "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
    jq '.minSeverity = "Info"
        | .defaultScribes = [["StdoutSK", "stdout"]]
        | .setupScribes = [{
            "scKind": "StdoutSK",
            "scName": "stdout",
            "scFormat": "ScText",
            "scRotation": null
          }]
        | .PeerSharing = false
        | .LedgerDB = { Backend: "V2InMemory" }' \
        "${CONFIG_JSON}" | write_file "${CONFIG_JSON}"
}

# config_conway_genesis strips the default 7-member script-hash
# committee so HFI gov-actions are auto-ratified — same patch the
# smoke-test scenario applies, documented in erastest.
config_conway_genesis() {
    local pool_dir="$1"
    local CWY="${pool_dir}/configs/conway-genesis.json"
    jq '.committee.members = {} |
        .committee.threshold = {numerator: 0, denominator: 1} |
        .committeeMinSize = 0' \
        "${CWY}" | write_file "${CWY}"
}

# set_start_time applies an absolute systemStart to a pool's genesis
# files. genesis-cli.py's built-in systemStartDelay (5s) is too short
# because key generation takes >30s; we override after the fact.
set_start_time() {
    local pool_dir="$1"
    local unix_ts="$2"
    local iso_ts
    iso_ts="$(date -d "@${unix_ts}" -u '+%Y-%m-%dT%H:%M:%SZ')"
    local SHELLEY="${pool_dir}/configs/shelley-genesis.json"
    local BYRON="${pool_dir}/configs/byron-genesis.json"
    jq ".systemStart = \"${iso_ts}\"" \
        "${SHELLEY}" | write_file "${SHELLEY}"
    jq ".startTime = ${unix_ts}" \
        "${BYRON}" | write_file "${BYRON}"
}

# write_empty_topology drops a localRoots-empty topology that the
# forging cardano-node uses during each phase (no upstream peers).
write_empty_topology() {
    local path="$1"
    cat >"${path}" <<'EOF'
{
  "localRoots": [],
  "publicRoots": [],
  "useLedgerAfterSlot": -1
}
EOF
}

# run_forge_phase launches cardano-node with the supplied config +
# keys + DB path, polls cardano-cli query tip until tip slot >=
# kill_slot, sends SIGTERM, waits for clean shutdown, and snapshots
# the resulting chain DB to a destination directory. The observed
# tip slot at kill time is written to the file given by the `tip_out`
# argument so the caller can use it as the input to
# rewrite_pool_start_time for the next phase.
#
# Arguments:
#   $1 phase label (used in log lines + log file name)
#   $2 config path (cardano-node --config)
#   $3 keys dir (must contain kes.skey, vrf.skey, opcert.cert)
#   $4 DB dir (cardano-node writes here; must already exist)
#   $5 socket path (under DB dir's parent is fine)
#   $6 kill slot
#   $7 destination dir (the DB is copied here after clean shutdown)
#   $8 tip-out file (the observed final tip slot is written here)
run_forge_phase() {
    local label="$1" config="$2" keys="$3" db="$4" sock="$5"
    local kill_slot="$6" dst="$7" tip_out="$8"
    local logfile="${LOG_ROOT}/phase-${label}.log"
    local topology="${STAGING_ROOT}/topology-empty.json"
    write_empty_topology "${topology}"

    log "phase ${label}: launching cardano-node (db=${db}, kill_slot=${kill_slot})"
    cardano-node run \
        --config "${config}" \
        --topology "${topology}" \
        --database-path "${db}" \
        --socket-path "${sock}" \
        --shelley-kes-key "${keys}/kes.skey" \
        --shelley-vrf-key "${keys}/vrf.skey" \
        --shelley-operational-certificate "${keys}/opcert.cert" \
        --port 3001 \
        > "${logfile}" 2>&1 &
    local node_pid=$!

    local deadline=$(( $(date +%s) + PHASE_TIMEOUT_SECS ))
    local last_slot=0
    while true; do
        if (( $(date +%s) > deadline )); then
            log "phase ${label}: timed out waiting for tip slot >= ${kill_slot} (last observed: ${last_slot})"
            log "phase ${label}: tail of ${logfile}:"
            tail -n 80 "${logfile}" | sed "s/^/[phase-${label}] /" >&2
            kill -KILL "${node_pid}" 2>/dev/null || true
            return 1
        fi
        local slot
        slot=$(cardano-cli conway query tip \
                 --socket-path "${sock}" \
                 --testnet-magic "${NETWORK_MAGIC}" \
                 2>/dev/null | jq -r '.slot // 0' 2>/dev/null || echo 0)
        if [[ "${slot}" =~ ^[0-9]+$ ]]; then
            last_slot="${slot}"
            if (( slot >= kill_slot )); then
                log "phase ${label}: reached tip slot ${slot} (>= ${kill_slot}); shutting down"
                break
            fi
        fi
        sleep 2
    done

    kill -TERM "${node_pid}"
    # Backstop: if cardano-node doesn't exit cleanly within 30s,
    # SIGKILL it. The wait following the timeout will return with
    # whichever exit status the kernel attributes to the process.
    local wait_deadline=$(( $(date +%s) + 30 ))
    while kill -0 "${node_pid}" 2>/dev/null; do
        if (( $(date +%s) > wait_deadline )); then
            log "phase ${label}: clean shutdown timeout; SIGKILL"
            kill -KILL "${node_pid}" 2>/dev/null || true
            break
        fi
        sleep 1
    done
    wait "${node_pid}" 2>/dev/null || true
    # Force the kernel to flush dirty pages from cardano-node's
    # write-after-close. Without this, copying volatile/blocks-*.dat
    # right after exit can capture a truncated file the runtime
    # cardano-node will fail to mmap with "FsReachedEOF".
    sync

    # Snapshot only if dst is a different path. Phases B and C forge
    # directly into the final per-peer volume so no post-forge copy
    # happens — that copy was the source of intermittent immutable-
    # chunk truncation on the runtime cardano-node.
    if [[ "$(readlink -f "${db}")" != "$(readlink -f "${dst}")" ]]; then
        log "phase ${label}: snapshotting ${db} -> ${dst} (final tip slot ${last_slot})"
        mkdir -p "${dst}"
        cp -r "${db}/." "${dst}/"
        sync
    else
        log "phase ${label}: final tip slot ${last_slot} (no copy — forged in place at ${dst})"
    fi
    echo "${last_slot}" > "${tip_out}"
    log "phase ${label}: done"
}

# ─── main ───────────────────────────────────────────────────────────

mkdir -p "${STAGING_ROOT}" "${LOG_ROOT}"

log "generating 2-pool genesis"
cp /testnet.yaml ./testnet.yaml
uv run python3 genesis-cli.py testnet.yaml -o /tmp/testnet -c generate

# Strip the default per-pool topology — the forge phases use a fresh
# empty topology; the cardano-* services at runtime mount their own
# topology files from the scenario directory.
find /tmp/testnet -type f -name 'topology.json' -exec rm -f '{}' ';'

# Move pool dirs + utxo keys into /configs/. /configs/{1,2} hold the
# per-pool configs + keys; /configs/utxo-keys holds the genesis
# spending keys (mostly unused here but kept for consistency).
mkdir -p /configs
cp -r /tmp/testnet/pools/* /configs
mkdir -p /configs/utxo-keys
cp -r /tmp/testnet/utxos/keys/* /configs/utxo-keys/
rm -rf /configs/keys

# Compute initial system_start. 60s into the future to give the forge
# phase A time to spin up cardano-node before the first leader slot.
INITIAL_SYSTEM_START=$(( $(date +%s) + 60 ))
log "initial system start: $(date -d @${INITIAL_SYSTEM_START} -u '+%Y-%m-%dT%H:%M:%SZ') (unix: ${INITIAL_SYSTEM_START})"

for pool in /configs/[0-9]*; do
    log "configuring ${pool}"
    set_start_time "${pool}" "${INITIAL_SYSTEM_START}"
    config_config_json "${pool}"
    config_conway_genesis "${pool}"
done

# ─── Phase A: shared prefix (pool 1 forges) ───────────────────────────
# Forge into a staging dir; we copy out to SHARED_PREFIX_DIR so the
# original stays available as the snapshot source for phases B and C.
mkdir -p "${STAGING_ROOT}/phase-a/db"
run_forge_phase "a" \
    "/configs/1/configs/config.json" \
    "/configs/1/keys" \
    "${STAGING_ROOT}/phase-a/db" \
    "${STAGING_ROOT}/phase-a/node.socket" \
    "${PREFIX_KILL_SLOT}" \
    "${SHARED_PREFIX_DIR}" \
    "${STAGING_ROOT}/phase-a.tip"
SHARED_PREFIX_TIP_SLOT="$(cat "${STAGING_ROOT}/phase-a.tip")"

# ─── Phase B: peer A extension (pool 1 forges from shared prefix) ─────
# Forge DIRECTLY into ${PEER_A_DATA_DIR} (no post-forge copy). The
# pre-forge copy of the shared prefix is fine — those bytes are stable
# (configurator-quiesced) and have been sync'd. Avoiding the post-forge
# copy eliminates the immutable-chunk truncation that copying a
# just-shut-down chain DB intermittently produced.
mkdir -p "${PEER_A_DATA_DIR}"
cp -r "${SHARED_PREFIX_DIR}/." "${PEER_A_DATA_DIR}/"
sync
run_forge_phase "b" \
    "/configs/1/configs/config.json" \
    "/configs/1/keys" \
    "${PEER_A_DATA_DIR}" \
    "${STAGING_ROOT}/phase-b.socket" \
    $(( SHARED_PREFIX_TIP_SLOT + PEER_A_EXTENSION_SLOTS )) \
    "${PEER_A_DATA_DIR}" \
    "${STAGING_ROOT}/phase-b.tip"

# ─── Phase C: peer B extension (pool 2 forges from shared prefix) ─────
mkdir -p "${PEER_B_DATA_DIR}"
cp -r "${SHARED_PREFIX_DIR}/." "${PEER_B_DATA_DIR}/"
sync
run_forge_phase "c" \
    "/configs/2/configs/config.json" \
    "/configs/2/keys" \
    "${PEER_B_DATA_DIR}" \
    "${STAGING_ROOT}/phase-c.socket" \
    $(( SHARED_PREFIX_TIP_SLOT + PEER_B_EXTENSION_SLOTS )) \
    "${PEER_B_DATA_DIR}" \
    "${STAGING_ROOT}/phase-c.tip"

# ─── Permissions for runtime cardano-node containers ────────────────
# Configs world-readable; keys 0700/0600 (cardano-node rejects vrf.skey
# with permissive perms). Chain-DB dirs world-readable so cardano-node
# can open them; cardano-node will fix file perms on its own writes.
find /configs -type d -exec chmod 0755 {} +
find /configs -type f -exec chmod 0644 {} +
for pool_dir in /configs/[0-9]*; do
    [[ -d "${pool_dir}/keys" ]] || continue
    chmod 0700 "${pool_dir}/keys"
    find "${pool_dir}/keys" -type f -exec chmod 0600 {} +
done
chmod -R go+rX "${PEER_A_DATA_DIR}" "${PEER_B_DATA_DIR}"

log "configurator complete"
