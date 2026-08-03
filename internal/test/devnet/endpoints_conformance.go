//go:build devnet && devnet_conformance

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
		{Name: "dingo-producer", Address: addr("DEVNET_DINGO_ADDR", "localhost:3010"), Role: "producer", IsDingo: true},
		{Name: "cardano-producer", Address: addr("DEVNET_CARDANO_ADDR", "localhost:3011"), Role: "producer", IsReference: true},
		{Name: "cardano-relay", Address: addr("DEVNET_RELAY_ADDR", "localhost:3012"), Role: "relay", IsReference: true},
	}
}
