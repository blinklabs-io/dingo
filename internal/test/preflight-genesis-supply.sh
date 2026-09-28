#!/usr/bin/env bash

# Copyright 2026 Blink Labs Software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks a testnet.yaml spec's genesis allocation against its max lovelace
# supply before a Docker-backed suite starts. The configurator
# (cardano-foundation/testnet-generation-tool) creates both a staked and an
# unstaked genesis address per delegated-supply pot, so a spec's genesis
# circulating supply is 2x delegatedSupply regardless of poolCount. Dingo's
# genesis-reserve guard (ledger/genesis_network_state.go) rejects a chain
# whose circulating supply exceeds maxLovelaceSupply; without this
# preflight, that misconfiguration only surfaces as an opaque node-health
# timeout minutes into a Docker-backed suite.
#
# Usage: preflight-genesis-supply.sh <testnet.yaml>

set -euo pipefail

SELF="$(basename "${BASH_SOURCE[0]}")"
TESTNET_YAML="${1:?usage: ${SELF} <testnet.yaml>}"

if [[ ! -f "${TESTNET_YAML}" ]]; then
  echo "${SELF}: ${TESTNET_YAML}: no such file" >&2
  exit 1
fi

delegated_supply="$(awk -F': *' '/^delegatedSupply:/ {print $2; exit}' "${TESTNET_YAML}")"
max_lovelace_supply="$(awk -F': *' '/^maxLovelaceSupply:/ {print $2; exit}' "${TESTNET_YAML}")"

if [[ -z "${delegated_supply}" || -z "${max_lovelace_supply}" ]]; then
  echo "${SELF}: could not read delegatedSupply/maxLovelaceSupply from ${TESTNET_YAML}" >&2
  exit 1
fi

circulating=$(( delegated_supply * 2 ))
if (( circulating > max_lovelace_supply )); then
  echo "${SELF}: ${TESTNET_YAML} would generate ${circulating} lovelace of genesis" \
    "UTxOs (delegatedSupply=${delegated_supply} x2 for staked+unstaked addresses)," \
    "exceeding maxLovelaceSupply=${max_lovelace_supply}" >&2
  exit 1
fi
