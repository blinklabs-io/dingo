//go:build devnet && !devnet_conformance

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

// LoadEndpoints returns the all-dingo network endpoints: three Dingo
// producers and one Dingo relay. Addresses come from environment variables
// set by run-tests.sh, with localhost defaults matching docker-compose.yml.
func LoadEndpoints() []NodeEndpoint {
	addr := func(env, def string) string {
		if v := os.Getenv(env); v != "" {
			return v
		}
		return def
	}
	return []NodeEndpoint{
		{Name: "dingo-1", Address: addr("DEVNET_DINGO1_ADDR", "localhost:3010"), Role: "producer", IsDingo: true},
		{Name: "dingo-2", Address: addr("DEVNET_DINGO2_ADDR", "localhost:3013"), Role: "producer", IsDingo: true},
		{Name: "dingo-3", Address: addr("DEVNET_DINGO3_ADDR", "localhost:3014"), Role: "producer", IsDingo: true},
		{Name: "dingo-relay", Address: addr("DEVNET_DINGO_RELAY_ADDR", "localhost:3015"), Role: "relay", IsDingo: true},
	}
}

// DingoNtcAddrs returns each dingo node's host TCP address for the
// node-to-client (LocalStateQuery) listener, keyed by node name. dingo
// serves NtC on private port 3002, mapped to host ports 3020-3023.
func DingoNtcAddrs() map[string]string {
	addr := func(env, def string) string {
		if v := os.Getenv(env); v != "" {
			return v
		}
		return def
	}
	return map[string]string{
		"dingo-1":     addr("DEVNET_DINGO1_NTC_ADDR", "localhost:3020"),
		"dingo-2":     addr("DEVNET_DINGO2_NTC_ADDR", "localhost:3021"),
		"dingo-3":     addr("DEVNET_DINGO3_NTC_ADDR", "localhost:3022"),
		"dingo-relay": addr("DEVNET_DINGO_RELAY_NTC_ADDR", "localhost:3023"),
	}
}
