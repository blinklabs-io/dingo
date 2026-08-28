#!/usr/bin/env bash

# Select a stable Compose project for this worktree. Callers may override it
# when they need more than one DevNet in the same worktree.
devnet_compose_project() {
  if [[ -n "${COMPOSE_PROJECT_NAME:-}" ]]; then
    return
  fi
  local project_root project_hash
  project_root="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
  project_hash="$(printf '%s' "${project_root}" | cksum | awk '{print $1}')"
  export COMPOSE_PROJECT_NAME="dingo-devnet-${project_hash}"
}

# Select a stable, worktree-specific /24 for the DevNet bridge network.
# Compose project-scopes the network's *name*, but its subnet is a
# separate axis: the checked-in topology/*.json files address peers by a
# hardcoded 172.20.0.0/24 IP (devnet_render_topology rewrites those), and
# Docker refuses to create two networks with the same subnet even under
# different project names ("Pool overlaps with other one on this address
# space"). Concurrent worktrees therefore each need their own range, not
# just their own network name.
#
# 172.24-172.31 stays clear of the static subnets the antithesis/
# archive-demo (172.21.0.0/24) and erastest (172.22.0.0/24) stacks pin, and
# of Docker's own default address pool, which starts allocating from
# 172.17.0.0/16. Callers may override it, same as COMPOSE_PROJECT_NAME.
devnet_net_base() {
  if [[ -n "${DEVNET_NET_BASE:-}" ]]; then
    return
  fi
  local project_root project_hash second third
  project_root="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
  project_hash="$(printf '%s' "${project_root}" | cksum | awk '{print $1}')"
  second=$((24 + project_hash % 8))
  third=$(((project_hash / 8) % 256))
  export DEVNET_NET_BASE="172.${second}.${third}"
}

# Path to this run's rendered topology directory. Split out from
# devnet_render_topology so teardown can find and remove it without also
# needing devnet_net_base or the source topology/ files.
devnet_topology_dir() {
  export DEVNET_TOPOLOGY_DIR="${TMPDIR:-/tmp}/dingo-devnet-topology-${COMPOSE_PROJECT_NAME}"
}

# Render the checked-in topology/*.json files into DEVNET_TOPOLOGY_DIR,
# rewriting their hardcoded 172.20.0.0/24 addresses to this run's
# DEVNET_NET_BASE. docker-compose.yml mounts topology files from
# DEVNET_TOPOLOGY_DIR (falling back to ./topology, the literal 172.20.0.x
# originals, when it is unset). Must run after devnet_compose_project.
devnet_render_topology() {
  devnet_net_base
  devnet_topology_dir
  mkdir -p "${DEVNET_TOPOLOGY_DIR}"
  local src
  for src in "${SCRIPT_DIR}"/topology/*.json; do
    sed "s/172\.20\.0\./${DEVNET_NET_BASE}./g" "${src}" \
      >"${DEVNET_TOPOLOGY_DIR}/$(basename "${src}")"
  done
}
