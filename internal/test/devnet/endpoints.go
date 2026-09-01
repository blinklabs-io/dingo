//go:build linux

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

// NodeEndpoint describes a node that the test harness can connect to
// using the Ouroboros Node-to-Node mini-protocol over TCP.
//
// This is plain data and carries only the package-wide Linux constraint, so
// the failure-capture planning in artifacts.go stays testable without a
// running DevNet. The code that dials an endpoint additionally requires the
// devnet tag.
type NodeEndpoint struct {
	Name        string
	Address     string // host:port
	Role        string // "producer" or "relay"
	IsDingo     bool   // node runs Dingo
	IsReference bool   // node runs the cardano-node reference impl
	// Container is the compose service name, used by the scenario to
	// interrupt and restart the node, and by failure capture to name the
	// service whose logs to preserve. A disruption step against an
	// endpoint with no container fails rather than being skipped: a run
	// that quietly omitted its interruption phases would not be the
	// release evidence it claims to be.
	Container string
}
