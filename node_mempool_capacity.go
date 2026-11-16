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
	"math/big"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/plugin"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// leiosQuorumSource is the committee view the mempool capacity consults.
type leiosQuorumSource interface {
	CommitteeCanReachQuorum(epoch uint64, tau *big.Rat) (bool, error)
}

// leiosQuorumPossibleFor reports whether the committee of epoch can reach the
// quorum threshold in force, taken from the current protocol parameters. It
// answers false whenever that cannot be established: nothing to consult, no
// threshold in the parameters, or a committee that cannot be resolved.
func leiosQuorumPossibleFor(
	source leiosQuorumSource,
	epoch uint64,
	pparams lcommon.ProtocolParameters,
) bool {
	if source == nil {
		return false
	}
	dijkstra, ok := pparams.(*gdijkstra.DijkstraProtocolParameters)
	if !ok || dijkstra == nil ||
		dijkstra.LeiosQuorumStakeThreshold == nil ||
		dijkstra.LeiosQuorumStakeThreshold.Rat == nil {
		return false
	}
	possible, err := source.CommitteeCanReachQuorum(
		epoch,
		new(big.Rat).Set(dijkstra.LeiosQuorumStakeThreshold.Rat),
	)
	return err == nil && possible
}

// leiosQuorumPossible reports whether the current epoch's committee can reach
// its quorum threshold.
func (n *Node) leiosQuorumPossible() bool {
	voteManager := n.leiosCertificateVerifier.Load()
	if voteManager == nil || n.ledgerState == nil {
		return false
	}
	return leiosQuorumPossibleFor(
		voteManager,
		n.ledgerState.CurrentEpoch(),
		n.ledgerState.GetCurrentPParams(),
	)
}

// leiosGatedMempoolCapacity returns the capacity function for a mempool whose
// configured capacity is the enlarged Leios one. The enlargement is for
// endorser blocks, which no committee short of its quorum can certify, so the
// pool keeps the non-Leios capacity until the committee can reach it. A
// configured capacity that is already smaller is never raised.
func leiosGatedMempoolCapacity(
	enlarged int64,
	quorumPossible func() bool,
) func() int64 {
	base := min(enlarged, int64(internalconfig.DefaultMempoolCapacityPraos))
	return func() int64 {
		if quorumPossible() {
			return enlarged
		}
		return base
	}
}

// mempoolDependencies returns what the mempool provider is resolved with. Only
// a Leios capacity chosen by default follows the committee's quorum.
func (n *Node) mempoolDependencies() mempool.ProviderDependencies {
	deps := mempool.ProviderDependencies{
		PromRegistry:    n.config.promRegistry,
		Validator:       n.ledgerState,
		Logger:          n.config.logger,
		EventBus:        n.eventBus,
		CurrentSlotFunc: n.ledgerState.CurrentOrTipSlot,
	}
	if n.config.mempoolCapacityLeiosDefault {
		selection := n.config.pluginSelections[plugin.CapabilityMempool]
		deps.CapacityFunc = leiosGatedMempoolCapacity(
			pluginInt64(selection.Config["capacity"]),
			n.leiosQuorumPossible,
		)
	}
	return deps
}
