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
	"errors"

	"github.com/blinklabs-io/dingo/ledger/forging"
)

// The reloads below run the prepare and install halves of a reload in one
// go, without the lifecycle gates, so tests can drive one step directly.

// reloadBlockProducerCredentials is ReloadBlockProducerCredentials with the
// clock and the ledger cross-check supplied, for the first pool. In sign mode
// it prepares the reload with the agent and installs it in one go.
func (n *Node) reloadBlockProducerCredentials(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
) error {
	var prep signReloadPrep
	if ks := n.primaryKeySource(); ks.signsThroughAgent() {
		if live := n.blockProducerCreds.Load(); live != nil {
			prep = n.prepareSignReload(ks, live, nil)
			defer prep.close()
		}
	}
	return n.reloadPrimaryCredentials(slot, supported, ledgerCheck, prep)
}

// reloadEveryPool reloads the first pool and, when the node forges for
// several, every other running pool, preparing the sign-mode pools' reloads
// and installing them in one go. reloadCredentials runs the two halves itself
// so the agents are checked outside the lifecycle gates.
func (n *Node) reloadEveryPool(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	skipServeKey bool,
) error {
	preps := n.prepareSignReloads()
	defer preps.close()
	return n.installEveryPool(slot, supported, ledgerCheck, skipServeKey, preps)
}

// reloadAgentSignCredentials replaces the first pool's live sign-mode
// credentials with the VRF key and an operational certificate -- the
// configured file, or agentOpCert when the agent presented a newer one --
// signed through a new agent client that expects that certificate's KES key.
// It is prepareSignReload followed by installSignReload; the reload triggers
// run the two halves themselves so the agent check happens outside the gates.
func (n *Node) reloadAgentSignCredentials(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
	agentOpCert []byte,
) error {
	live := n.blockProducerCreds.Load()
	if live == nil {
		return errors.New("block producer credentials are not loaded")
	}
	ks := n.primaryKeySource()
	prep := n.prepareSignReload(ks, live, agentOpCert)
	defer prep.close()
	return n.installSignReload(ks, live, slot, supported, ledgerCheck, prep)
}
