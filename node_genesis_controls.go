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

package dingo

import (
	"github.com/blinklabs-io/dingo/topology"
)

// genesisControlGroups returns the operator-configured relay control groups.
// Dingo supplies no default groups for any network: without configuration,
// witness identities come from the ledger, a peer snapshot, or the address of
// the connection.
func (n *Node) genesisControlGroups() []topology.PeerControlGroup {
	if n.config.cfg != nil {
		if groups := n.config.cfg.GenesisBootstrap.ControlGroups; groups != nil {
			return topology.ClonePeerControlGroups(groups)
		}
	}
	return nil
}
