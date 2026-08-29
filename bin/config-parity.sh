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

# Fail when the network configs embedded in the dingo binary have drifted
# from blinklabs-io/docker-cardano-configs.
#
# The two copies must stay byte-identical because they are the same config
# reached by two different runtime paths. `CARDANO_NETWORK=<net>` with no
# config file serves the copy embedded here via config/cardano/embed.go,
# while the release image ships the docker-cardano-configs copy as
# /opt/cardano/config/ (see the cardano-configs stage in the Dockerfile) and
# downstream tooling copies it out of there. A change landing in only one of
# them makes the Docker and binary deployments run different chains under
# the same network name.
#
# Set CARDANO_CONFIGS_DIR to an existing checkout to skip the clone; CI
# passes the path from actions/checkout. CARDANO_CONFIGS_REF selects the ref
# to clone when it does not.

set -euo pipefail

configs_repo="https://github.com/blinklabs-io/docker-cardano-configs"
configs_ref="${CARDANO_CONFIGS_REF:-main}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# Files that exist here with no counterpart in docker-cardano-configs, and
# are expected to. Anything else missing from that repository is drift.
dingo_only=(
	"preview/README.md"
)

configs_dir="${CARDANO_CONFIGS_DIR:-}"
cleanup_dir=""
if [[ -z "${configs_dir}" ]]; then
	cleanup_dir="$(mktemp -d)"
	# shellcheck disable=SC2064 # expand configs_dir now, not at trap time
	trap "rm -rf '${cleanup_dir}'" EXIT
	echo "cloning ${configs_repo} at ${configs_ref}"
	git clone --quiet --depth 1 --branch "${configs_ref}" \
		"${configs_repo}" "${cleanup_dir}/configs"
	configs_dir="${cleanup_dir}/configs"
fi

if [[ ! -d "${configs_dir}/config" ]]; then
	echo "error: ${configs_dir} does not look like a docker-cardano-configs checkout" >&2
	exit 1
fi

# Check exactly the networks config/cardano/embed.go embeds, so adding a
# network to that directive brings it under this check automatically.
embed_line="$(grep -m1 '^//go:embed ' config/cardano/embed.go)"
read -r -a networks <<<"${embed_line#//go:embed }"
if [[ ${#networks[@]} -eq 0 ]]; then
	echo "error: no networks found in config/cardano/embed.go" >&2
	exit 1
fi

drift=0
checked=0
for network in "${networks[@]}"; do
	upstream="${configs_dir}/config/${network}"
	if [[ ! -d "${upstream}" ]]; then
		echo "DRIFT ${network}: not present in docker-cardano-configs"
		drift=$((drift + 1))
		continue
	fi
	while IFS= read -r tracked; do
		relative="${tracked#config/cardano/}"
		skip=0
		for allowed in "${dingo_only[@]}"; do
			[[ "${relative}" == "${allowed}" ]] && skip=1 && break
		done
		[[ ${skip} -eq 1 ]] && continue
		checked=$((checked + 1))
		counterpart="${configs_dir}/config/${relative}"
		if [[ ! -e "${counterpart}" ]]; then
			echo "DRIFT ${relative}: missing from docker-cardano-configs"
			drift=$((drift + 1))
			continue
		fi
		if ! diff -u "${counterpart}" "${tracked}" \
			--label "docker-cardano-configs/config/${relative}" \
			--label "dingo/config/cardano/${relative}"; then
			drift=$((drift + 1))
		fi
	done < <(git ls-files "config/cardano/${network}")
done

if [[ ${drift} -ne 0 ]]; then
	cat >&2 <<EOF

${drift} file(s) differ from ${configs_repo} at ${configs_ref}.

Land the same change in both repositories. The copy embedded here serves
CARDANO_NETWORK without a config file; the docker-cardano-configs copy is
what the release image ships as /opt/cardano/config/.
EOF
	exit 1
fi

echo "${checked} file(s) across ${#networks[@]} network(s) match ${configs_repo} at ${configs_ref}"
