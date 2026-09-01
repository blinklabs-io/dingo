//go:build linux && devnet && devnet_conformance

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package devnet

import "os"

// LoadEndpoints returns the conformance network endpoints: one Dingo
// producer plus the cardano-node reference producer and relay. Addresses
// come from environment variables set by run-tests.sh, with localhost
// defaults matching docker-compose.yml.
func LoadEndpoints() []NodeEndpoint {
	addr := func(env, def string) string {
		if v := os.Getenv(env); v != "" {
			return v
		}
		return def
	}
	return []NodeEndpoint{
		{
			Name:      "dingo-producer",
			Container: "dingo-producer",
			Address:   addr("DEVNET_DINGO_ADDR", "localhost:3010"),
			Role:      "producer",
			IsDingo:   true,
		},
		{
			Name:        "cardano-producer",
			Container:   "cardano-producer",
			Address:     addr("DEVNET_CARDANO_ADDR", "localhost:3011"),
			Role:        "producer",
			IsReference: true,
		},
		{
			Name:        "cardano-relay",
			Container:   "cardano-relay",
			Address:     addr("DEVNET_RELAY_ADDR", "localhost:3012"),
			Role:        "relay",
			IsReference: true,
		},
	}
}

// DingoProducerNtcAddr returns dingo-producer's host TCP address for the
// node-to-client (LocalStateQuery) listener. dingo-producer serves NtC on
// private port 3002 (via DINGO_PRIVATE_BIND_ADDR), mapped to host port
// 3030 by default (override with DEVNET_DINGO_NTC_ADDR or
// DEVNET_DINGO_NTC_PORT).
func DingoProducerNtcAddr() string {
	if v := os.Getenv("DEVNET_DINGO_NTC_ADDR"); v != "" {
		return v
	}
	return "localhost:3030"
}

// CardanoProducerNtcAddr returns cardano-producer's host TCP address for
// its node-to-client (LocalStateQuery) listener. cardano-node has no
// built-in TCP NtC support; the blinklabs-io/docker-cardano-node image
// bridges it with a background socat process (SOCAT_PORT=3002 forwards
// TCP to the node's unix socket), mapped to host port 3031 by default
// (override with DEVNET_CARDANO_NTC_ADDR or DEVNET_CARDANO_NTC_PORT).
func CardanoProducerNtcAddr() string {
	if v := os.Getenv("DEVNET_CARDANO_NTC_ADDR"); v != "" {
		return v
	}
	return "localhost:3031"
}
