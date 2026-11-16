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

// leiosQuorumState is what can be established about whether a Leios committee
// can reach its quorum threshold.
type leiosQuorumState int

const (
	// leiosQuorumNotApplicable: the parameters are not Dijkstra's, so there
	// is no committee threshold to consult.
	leiosQuorumNotApplicable leiosQuorumState = iota
	// leiosQuorumUnresolved: Dijkstra parameters, but no threshold or no
	// committee answer.
	leiosQuorumUnresolved
	leiosQuorumNotPossible
	leiosQuorumPossible
)

// leiosQuorumStateFor reports whether the committee of epoch can reach the
// quorum threshold in pparams.
func leiosQuorumStateFor(
	source leiosQuorumSource,
	epoch uint64,
	pparams lcommon.ProtocolParameters,
) leiosQuorumState {
	dijkstra, ok := pparams.(*gdijkstra.DijkstraProtocolParameters)
	if !ok || dijkstra == nil {
		return leiosQuorumNotApplicable
	}
	if source == nil ||
		dijkstra.LeiosQuorumStakeThreshold == nil ||
		dijkstra.LeiosQuorumStakeThreshold.Rat == nil {
		return leiosQuorumUnresolved
	}
	possible, err := source.CommitteeCanReachQuorum(
		epoch,
		new(big.Rat).Set(dijkstra.LeiosQuorumStakeThreshold.Rat),
	)
	switch {
	case err != nil:
		return leiosQuorumUnresolved
	case possible:
		return leiosQuorumPossible
	default:
		return leiosQuorumNotPossible
	}
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
	return leiosQuorumStateFor(source, epoch, pparams) == leiosQuorumPossible
}

// leiosQuorumPossible reports whether the current epoch's committee can reach
// its quorum threshold. It runs on the mempool's goroutines, which cannot take
// liveLifecycleMu, so it reads the ledger published for such readers rather
// than n.ledgerState, which a live restore rewrites, and it takes the epoch
// and its parameters from one consensus snapshot.
func (n *Node) leiosQuorumPossible() bool {
	voteManager := n.leiosCertificateVerifier.Load()
	ls := n.wallClockLedger.Load()
	if voteManager == nil || ls == nil {
		return false
	}
	epoch, pparams := ls.CurrentEpochAndPParams()
	return leiosQuorumPossibleFor(voteManager, epoch, pparams)
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

// Endorser-block skip reasons for a committee that cannot certify one.
const (
	leiosEBSkipQuorumNotPossible = "quorum_not_possible"
	leiosEBSkipQuorumUnresolved  = "quorum_unresolved"
)

// leiosEndorserBlockQuorumRefusal returns why no endorser block may be forged
// for slot, or "" when one may. Under Dijkstra an endorser block is worth
// forging only while the committee of the slot's epoch can reach the quorum
// threshold of the slot's parameters: no committee short of it can certify
// one. The slot's epoch and parameters decide, not the tip's, since the first
// block of an epoch is forged under the new epoch's committee. Parameters
// before Dijkstra, or none at all, are left to the forger's own capacity
// checks.
func leiosEndorserBlockQuorumRefusal(
	source leiosQuorumSource,
	epochForSlot func(uint64) (uint64, error),
	pparamsForSlot func(uint64) lcommon.ProtocolParameters,
	slot uint64,
) string {
	pparams := pparamsForSlot(slot)
	if _, ok := pparams.(*gdijkstra.DijkstraProtocolParameters); !ok {
		return ""
	}
	epoch, err := epochForSlot(slot)
	if err != nil {
		return leiosEBSkipQuorumUnresolved
	}
	switch leiosQuorumStateFor(source, epoch, pparams) {
	case leiosQuorumNotApplicable, leiosQuorumPossible:
		return ""
	case leiosQuorumNotPossible:
		return leiosEBSkipQuorumNotPossible
	case leiosQuorumUnresolved:
		return leiosEBSkipQuorumUnresolved
	default:
		return leiosEBSkipQuorumUnresolved
	}
}

// leiosEndorserBlockQuorumRefusalForSlot is leiosEndorserBlockQuorumRefusal
// against the node's vote manager and ledger. It runs on the forging
// goroutine, so like leiosQuorumPossible it reads the published ledger once
// and uses that for every lookup.
func (n *Node) leiosEndorserBlockQuorumRefusalForSlot(slot uint64) string {
	var source leiosQuorumSource
	if voteManager := n.leiosCertificateVerifier.Load(); voteManager != nil {
		source = voteManager
	}
	ls := n.wallClockLedger.Load()
	if ls == nil {
		return leiosEBSkipQuorumUnresolved
	}
	return leiosEndorserBlockQuorumRefusal(
		source,
		func(slot uint64) (uint64, error) {
			epoch, err := ls.SlotToEpoch(slot)
			return epoch.EpochId, err
		},
		ls.ProtocolParamsForSlot,
		slot,
	)
}
