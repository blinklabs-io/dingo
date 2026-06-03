#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${NAMESPACE:-dingo-preview}"
RELEASE="${RELEASE:-dingo-preview}"
CHART="${CHART:-oci://ghcr.io/blinklabs-io/helm-charts/charts/dingo}"
CHART_VERSION="${CHART_VERSION:-0.1.3}"

helm upgrade --install "${RELEASE}" "${CHART}" \
  --version "${CHART_VERSION}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --values "${ROOT_DIR}/k8s/dingo-values.yaml"

kubectl -n "${NAMESPACE}" rollout status "statefulset/${RELEASE}" --timeout=20m
kubectl -n "${NAMESPACE}" get svc,pods
