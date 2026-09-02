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
# from the docker-cardano-configs copy the release image ships.
#
# The two copies must stay byte-identical because they are the same config
# reached by two different runtime paths. `CARDANO_NETWORK=<net>` with no
# config file serves the copy embedded here via config/cardano/embed.go,
# while the release image ships the docker-cardano-configs copy as
# /opt/cardano/config/ and downstream tooling copies it out of there. A
# change landing in only one of them makes the Docker and binary
# deployments run different chains under the same network name.
#
# The default comparison is against the cardano-configs image tag the
# Dockerfile pins, not against that repository's main branch, because the
# pinned tag is what a built image actually contains. That also makes the
# check satisfiable inside a single dingo pull request: bump the pinned tag
# and update the embedded copy together and it goes green, with no window
# where it is red waiting on another repository to merge. Comparing against
# main instead would go green the moment that branch moved, while the image
# this repository builds still shipped the old config.
#
# Sources, in precedence order:
#   CARDANO_CONFIGS_DIR  an existing checkout or extracted config tree
#   CARDANO_CONFIGS_REF  a branch, tag, or commit ref of the repository
#   (default)            the image tag pinned in the Dockerfile

set -euo pipefail

configs_repo="https://github.com/blinklabs-io/docker-cardano-configs"
configs_image="ghcr.io/blinklabs-io/cardano-configs"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# Files that exist here with no counterpart in docker-cardano-configs, and
# are expected to. Anything else missing from that repository is drift.
dingo_only=(
	"preview/README.md"
)

configs_dir="${CARDANO_CONFIGS_DIR:-}"
configs_ref="${CARDANO_CONFIGS_REF:-}"
source_desc=""
cleanup_dir=""
if [[ -n "${configs_dir}" ]]; then
	source_desc="${configs_dir}"
else
	cleanup_dir="$(mktemp -d)"
	# shellcheck disable=SC2064 # expand cleanup_dir now, not at trap time
	trap "rm -rf '${cleanup_dir}'" EXIT
	if [[ -n "${configs_ref}" ]]; then
		source_desc="${configs_repo} at ${configs_ref}"
		echo "cloning ${source_desc}"
		git clone --quiet --filter=blob:none "${configs_repo}" \
			"${cleanup_dir}/configs"
		git -C "${cleanup_dir}/configs" checkout --quiet "${configs_ref}"
		configs_dir="${cleanup_dir}/configs"
	else
		tag="$(sed -n "s|^FROM ${configs_image}:\\([^ ]*\\) .*|\\1|p" Dockerfile | head -1)"
		if [[ -z "${tag}" ]]; then
			echo "error: no ${configs_image} tag found in Dockerfile" >&2
			exit 1
		fi
		source_desc="${configs_image}:${tag}"
		echo "extracting /config from ${source_desc}"
		container="$(docker create "${source_desc}")"
		# shellcheck disable=SC2064 # expand both now, not at trap time
		trap "docker rm --force '${container}' >/dev/null 2>&1 || true; rm -rf '${cleanup_dir}'" EXIT
		mkdir -p "${cleanup_dir}/configs"
		docker cp "${container}:/config" "${cleanup_dir}/configs/config"
		configs_dir="${cleanup_dir}/configs"
	fi
fi

# Check exactly the networks config/cardano/embed.go embeds, so adding a
# network to that directive brings it under this check automatically.
embed_line="$(grep -m1 '^//go:embed ' config/cardano/embed.go)" || {
	echo "error: no //go:embed directive found in config/cardano/embed.go" >&2
	exit 1
}
read -r -a networks <<<"${embed_line#//go:embed }"
if [[ ${#networks[@]} -eq 0 ]]; then
	echo "error: no networks found in config/cardano/embed.go" >&2
	exit 1
fi

if [[ -d "${configs_dir}/config" ]]; then
	configs_root="${configs_dir}/config"
else
	configs_root="${configs_dir}"
fi
if [[ ! -d "${configs_root}/${networks[0]}" ]]; then
	echo "error: ${configs_dir} has no config tree" >&2
	exit 1
fi

drift=0
checked=0
for network in "${networks[@]}"; do
	upstream="${configs_root}/${network}"
	if [[ ! -d "${upstream}" ]]; then
		echo "DRIFT ${network}: not present in ${source_desc}"
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
		counterpart="${configs_root}/${relative}"
		if [[ ! -e "${counterpart}" ]]; then
			echo "DRIFT ${relative}: missing from ${source_desc}"
			drift=$((drift + 1))
			continue
		fi
		if ! diff -u "${counterpart}" "${tracked}" \
			--label "cardano-configs/config/${relative}" \
			--label "dingo/config/cardano/${relative}"; then
			drift=$((drift + 1))
		fi
	done < <(git ls-files "config/cardano/${network}")
	while IFS= read -r relative; do
		skip=0
		for allowed in "${dingo_only[@]}"; do
			[[ "${network}/${relative}" == "${allowed}" ]] && skip=1 && break
		done
		[[ ${skip} -eq 1 ]] && continue
		if ! git ls-files --error-unmatch "config/cardano/${network}/${relative}" \
			>/dev/null 2>&1; then
			echo "DRIFT ${network}/${relative}: missing from dingo"
			drift=$((drift + 1))
		fi
	done < <(find "${upstream}" -type f -printf '%P\n' | sort)
done

if [[ ${drift} -ne 0 ]]; then
	cat >&2 <<EOF

${drift} file(s) differ from ${source_desc}.

Both copies must change together. Land the change in
${configs_repo}, release a
cardano-configs image containing it, then in this repository bump the
pinned tag in the Dockerfile and update config/cardano/ in the same commit.
EOF
	exit 1
fi

echo "${checked} file(s) across ${#networks[@]} network(s) match ${source_desc}"
