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

# --- IPv4 CIDR helpers, used to check a candidate subnet against every
# --- network Docker already knows about (see devnet_net_base). --------

_devnet_ip_to_int() {
  local IFS=.
  # shellcheck disable=SC2206 # splitting "a.b.c.d" on IFS=. is the point
  local -a o=(${1})
  echo $(( (o[0] << 24) + (o[1] << 16) + (o[2] << 8) + o[3] ))
}

# Prints "<first> <last>", the inclusive integer host range for a CIDR.
_devnet_cidr_range() {
  local cidr="$1" addr prefix base mask
  addr="${cidr%/*}"
  prefix="${cidr#*/}"
  base=$(_devnet_ip_to_int "${addr}")
  mask=$(( (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF ))
  base=$(( base & mask ))
  echo "${base} $(( base + (1 << (32 - prefix)) - 1 ))"
}

_devnet_cidr_overlaps() {
  local a_lo a_hi b_lo b_hi
  read -r a_lo a_hi < <(_devnet_cidr_range "$1")
  read -r b_lo b_hi < <(_devnet_cidr_range "$2")
  [[ ${a_lo} -le ${b_hi} && ${b_lo} -le ${a_hi} ]]
}

# Every subnet Docker currently has allocated, one per line. Best-effort:
# an unreachable daemon just yields no exclusions, same as today.
_devnet_used_subnets() {
  local name
  docker network ls --format '{{.Name}}' 2>/dev/null | while read -r name; do
    docker network inspect "${name}" \
      --format '{{range .IPAM.Config}}{{.Subnet}}{{"\n"}}{{end}}' 2>/dev/null
  done
}

# Select a worktree-specific /24 for the DevNet bridge network. A distinct
# Compose project name scopes the network's *name*, but not its subnet:
# Docker refuses to create two networks with an overlapping subnet even
# under different project names ("Pool overlaps with other one on this
# address space"). The checked-in topology/*.json files address peers by a
# hardcoded 172.20.0.0/24 IP (devnet_render_topology rewrites those), so
# concurrent worktrees each need their own genuinely free range.
#
# Hashing the worktree path alone isn't enough to guarantee that: the hash
# space here is only 2048 /24s, two different worktrees can land on the
# same one, and an unrelated Docker network on the host (this range isn't
# reserved for DevNet) could already occupy part of it. So this walks
# forward from the hash-derived starting point and actually checks every
# subnet Docker currently reports, via _devnet_used_subnets, until it finds
# one with no overlap.
#
# 172.24-172.31 stays clear of the static subnets the antithesis/
# archive-demo (172.21.0.0/24) and erastest (172.22.0.0/24) stacks pin, and
# of Docker's own default address pool, which starts allocating from
# 172.17.0.0/16. Callers may override it, same as COMPOSE_PROJECT_NAME.
devnet_net_base() {
  if [[ -n "${DEVNET_NET_BASE:-}" ]]; then
    return
  fi
  local project_root project_hash used
  project_root="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
  project_hash="$(printf '%s' "${project_root}" | cksum | awk '{print $1}')"
  used="$(_devnet_used_subnets)"

  local total=2048 start step index second third candidate subnet overlap
  start=$((project_hash % total))
  for (( step = 0; step < total; step++ )); do
    index=$(( (start + step) % total ))
    second=$(( 24 + index / 256 ))
    third=$(( index % 256 ))
    candidate="172.${second}.${third}.0/24"
    overlap=false
    if [[ -n "${used}" ]]; then
      while IFS= read -r subnet; do
        [[ -z "${subnet}" ]] && continue
        if _devnet_cidr_overlaps "${candidate}" "${subnet}"; then
          overlap=true
          break
        fi
      done <<<"${used}"
    fi
    if [[ "${overlap}" == "false" ]]; then
      export DEVNET_NET_BASE="172.${second}.${third}"
      return
    fi
  done

  echo "devnet: no free /24 subnet found in 172.24.0.0-172.31.255.0" >&2
  return 1
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

# The host ports docker-compose.yml publishes, keyed by the env var it
# already reads for each (with a `:-` fallback to the original literal
# port). Order fixes each var's offset within the block devnet_ports
# allocates; keep in sync with docker-compose.yml's `ports:` entries.
_DEVNET_PORT_VARS=(
  DEVNET_DINGO1_PORT
  DEVNET_DINGO2_PORT
  DEVNET_DINGO3_PORT
  DEVNET_DINGO_RELAY_PORT
  DEVNET_DINGO1_NTC_PORT
  DEVNET_DINGO2_NTC_PORT
  DEVNET_DINGO3_NTC_PORT
  DEVNET_DINGO_RELAY_NTC_PORT
  DEVNET_DINGO_PORT
  DEVNET_CARDANO_PORT
  DEVNET_RELAY_PORT
)

# True (exit 0) when nothing is listening on 127.0.0.1:<port>. Best-effort,
# same TOCTOU caveat as any "check then bind" scheme, but good enough to
# steer clear of another worktree's already-running DevNet.
_devnet_port_free() {
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# Select a worktree-specific block of host ports. A distinct Compose
# project isolates containers, volumes, and (via devnet_net_base) the
# bridge subnet, but published host ports are a separate axis Compose does
# not scope by project at all: a second worktree's default ports
# (3010/3013-3015, 3020-3023, and conformance's 3010-3012) collide outright
# with "port is already allocated". Skips entirely if the caller has
# already set any of _DEVNET_PORT_VARS, so a manual override stays in full
# manual control.
devnet_ports() {
  local var
  for var in "${_DEVNET_PORT_VARS[@]}"; do
    if [[ -n "${!var:-}" ]]; then
      return
    fi
  done

  local project_root project_hash block_size base attempt candidate i port all_free
  project_root="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
  project_hash="$(printf '%s' "${project_root}" | cksum | awk '{print $1}')"
  block_size=${#_DEVNET_PORT_VARS[@]}
  # Stay clear of privileged ports and of the ephemeral range most OSes
  # start handing out around 32768-49152.
  base=$(( 20000 + (project_hash % 9000) ))

  for (( attempt = 0; attempt < 200; attempt++ )); do
    candidate=$(( base + attempt * block_size ))
    all_free=true
    for (( i = 0; i < block_size; i++ )); do
      port=$(( candidate + i ))
      if ! _devnet_port_free "${port}"; then
        all_free=false
        break
      fi
    done
    if [[ "${all_free}" == "true" ]]; then
      for (( i = 0; i < block_size; i++ )); do
        export "${_DEVNET_PORT_VARS[i]}=$(( candidate + i ))"
      done
      return
    fi
  done

  echo "devnet: could not find a free block of ${block_size} host ports" >&2
  return 1
}

# `docker compose up -d`, retrying if this run's subnet lost a race with a
# concurrent worktree between devnet_net_base's check and the network
# actually being created. On that failure the losing worktree's network
# now shows up in `docker network ls`, so recomputing DEVNET_NET_BASE (and
# re-rendering topology against it) naturally avoids it on the next try.
devnet_compose_up() {
  local compose_file="$1" attempt out
  for (( attempt = 1; attempt <= 3; attempt++ )); do
    if out=$(docker compose -f "${compose_file}" up -d 2>&1); then
      printf '%s\n' "${out}"
      return 0
    fi
    printf '%s\n' "${out}" >&2
    if [[ "${out}" != *"Pool overlaps"* ]] || [[ ${attempt} -eq 3 ]]; then
      return 1
    fi
    echo "[compose-project] subnet collided with a concurrent worktree;" \
      "picking a new one and retrying (attempt ${attempt}/3)" >&2
    unset DEVNET_NET_BASE
    devnet_render_topology
  done
}
