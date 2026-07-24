#!/usr/bin/env bash
set -euo pipefail

DATABASE_URL="${DATABASE_URL:?DATABASE_URL must point to the Dingo metadata Postgres database}"
DINGO_BIN="${DINGO_BIN:-dingo}"
NETWORK="${NETWORK:-preview}"
DATA_DIR="${CARDANO_DATABASE_PATH:-${PWD}/.dingo-preview}"

export CARDANO_DATABASE_PATH="${DATA_DIR}"
export DINGO_STORAGE_MODE="${DINGO_STORAGE_MODE:-api}"
# This example forces its own Postgres metadata store, a local Badger blob
# store, and disabled node APIs. These are pinned to literal values (not
# ${VAR:-default}) so an inherited plugin env var cannot silently redirect the
# store or re-enable an API port the example intends to keep off.
export DINGO_PLUGINS_STORAGE_METADATA_PROVIDER="postgres"
export DINGO_PLUGINS_STORAGE_METADATA_CONFIG_DSN="${DATABASE_URL}"
export DINGO_PLUGINS_STORAGE_BLOB_PROVIDER="badger"
export DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT="0"
export DINGO_PLUGINS_API_MESH_CONFIG_PORT="0"
export DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT="0"

exec "${DINGO_BIN}" -n "${NETWORK}"
