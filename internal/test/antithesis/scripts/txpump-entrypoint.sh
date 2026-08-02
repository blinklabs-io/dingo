#!/usr/bin/env sh
set -eu

# The analyzer image uses uid 1000. Running txpump with the same uid keeps its
# 0640 structured log readable through the shared volume.
stake_key="/utxo-keys/stake/txpump.stake.vkey"
pool_key="/configs/keys/cold.vkey"
if [ ! -f "${stake_key}" ] || [ ! -f "${pool_key}" ]; then
  echo "txpump: delegation credentials are missing" >&2
  exit 1
fi
export TXPUMP_DELEGATION_STAKE_KEY_HASH="$(cardano-cli stake-address key-hash --stake-verification-key-file "${stake_key}")"
export TXPUMP_DELEGATION_POOL_KEY_HASH="$(cardano-cli stake-pool id --cold-verification-key-file "${pool_key}" --output-format hex)"

chown -R txpump:txpump /logs
exec su -s /bin/sh txpump -c 'exec /bin/txpump'
