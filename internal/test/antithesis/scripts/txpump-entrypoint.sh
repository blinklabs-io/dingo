#!/usr/bin/env sh
set -eu

# The analyzer image uses uid 1000. Running txpump with the same uid keeps its
# 0640 structured log readable through the shared volume.

# Delegation credentials are only needed when "delegation" is an enabled
# workload type (see internal/txpump: Config.delegationEnabled() and
# pump.go enabledTypes()). For payment-only runs the pump never touches
# delegation, so requiring/deriving the pool key hash would abort a perfectly
# valid run. txpump uses its funding keys as stake credentials. Gate the requirement on TXPUMP_TYPES accordingly.
case ",${TXPUMP_TYPES:-},"  in
  *,delegation,*)
    pool_key="/configs/keys/cold.vkey"
    if [ ! -f "${pool_key}" ]; then
      echo "txpump: delegation pool key is missing" >&2
      exit 1
    fi
    TXPUMP_DELEGATION_POOL_KEY_HASH="$(cardano-cli latest stake-pool id --cold-verification-key-file "${pool_key}" --output-hex)"
    export TXPUMP_DELEGATION_POOL_KEY_HASH
    ;;
esac

chown -R txpump:txpump /logs
exec su -s /bin/sh txpump -c 'exec /bin/txpump'
