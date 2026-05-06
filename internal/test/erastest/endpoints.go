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

//go:build erastest

// Package erastest provides a test harness for running era-transition
// integration tests against a private DevNet that traverses every Cardano
// era over a few short epochs.
package erastest

import "os"

// DefaultNetworkMagic matches the networkMagic configured in the eras
// testnet.yaml.
const DefaultNetworkMagic = 42

// NodeEndpoint describes one node the harness can reach over Ouroboros
// node-to-node TCP.
type NodeEndpoint struct {
	Name    string
	Address string // host:port
}

// DefaultEndpoints returns the eras-stack endpoints. Each endpoint may
// be overridden via an env var so a single binary can target either a
// docker-compose stack on localhost or a remote test deployment.
func DefaultEndpoints() []NodeEndpoint {
	dingo := os.Getenv("ERASTEST_DINGO_ADDR")
	if dingo == "" {
		dingo = "localhost:3020"
	}
	cardano := os.Getenv("ERASTEST_CARDANO_ADDR")
	if cardano == "" {
		cardano = "localhost:3021"
	}
	relay := os.Getenv("ERASTEST_RELAY_ADDR")
	if relay == "" {
		relay = "localhost:3022"
	}
	return []NodeEndpoint{
		{Name: "dingo-producer", Address: dingo},
		{Name: "cardano-producer", Address: cardano},
		{Name: "cardano-relay", Address: relay},
	}
}
