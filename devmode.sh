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

export CARDANO_NETWORK=devnet
export CARDANO_CONFIG=./config/cardano/devnet/config.json
export CARDANO_DATABASE_PATH=.devnet
export CARDANO_DEV_MODE=true

DEBUG=${DEBUG:-false}

conf="$(dirname "$CARDANO_CONFIG")"
now=$(date -u +%s)
echo "setting start time in $conf to $now"
sed -i -e "s/startTime\": .*,/startTime\": $now,/" "$conf/byron-genesis.json"
sed -i -e "s/systemStart\": .*,/systemStart\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ --date=@$now)\",/" "$conf/shelley-genesis.json"

echo resetting .devnet
rm -rf .devnet/*

if [[ ${DEBUG} == true ]]; then
	go run ./cmd/dingo/ --debug
else
	go run ./cmd/dingo/
fi
