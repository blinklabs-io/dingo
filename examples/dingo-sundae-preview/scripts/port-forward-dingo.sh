#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-dingo-preview}"
RELEASE="${RELEASE:-dingo-preview}"
LOCAL_PORT="${LOCAL_PORT:-9090}"
BIND_ADDRESS="${BIND_ADDRESS:-127.0.0.1}"

# Set BIND_ADDRESS=0.0.0.0 only when LAN access is needed for testing.
kubectl -n "${NAMESPACE}" port-forward "svc/${RELEASE}" "${LOCAL_PORT}:9090" --address "${BIND_ADDRESS}"
