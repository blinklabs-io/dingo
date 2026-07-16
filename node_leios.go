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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leios"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// leiosStakeDistributionAdapter adapts ledger.LedgerState to
// leios.StakeDistributionProvider, reusing the same txn-scoped ledger
// view as Praos leader election so the Leios committee derives from the
// identical stake snapshot rotation.
type leiosStakeDistributionAdapter struct {
	inner stakeDistributionAdapter
}

func (a *leiosStakeDistributionAdapter) GetStakeDistribution(
	epoch uint64,
) (map[string]uint64, uint64, error) {
	dist, err := a.inner.getStakeDistribution(epoch)
	if err != nil {
		return nil, 0, err
	}
	if dist == nil {
		return nil, 0, nil
	}
	return dist.PoolStakes, dist.TotalStake, nil
}

// leiosCommitteeParamsAdapter adapts ledger.LedgerState to
// leios.CommitteeParamsProvider. It revalidates the tau < sigma_c
// invariant on every read so an invalid parameter combination disables
// committee computation rather than silently mis-tallying.
type leiosCommitteeParamsAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *leiosCommitteeParamsAdapter) LeiosCommitteeParameters() (
	*big.Rat,
	*big.Rat,
	error,
) {
	if a.ledgerState == nil {
		return nil, nil, errors.New("ledger state unavailable")
	}
	pparams := a.ledgerState.GetCurrentPParams()
	dijkstraPParams, ok := pparams.(*gdijkstra.DijkstraProtocolParameters)
	if !ok {
		return nil, nil, fmt.Errorf(
			"leios committee parameters require the dijkstra era, current pparams are %T",
			pparams,
		)
	}
	return leiosCommitteeParamsFromPParams(dijkstraPParams)
}

// CIP-0164 default Leios committee parameters, used when the Dijkstra
// genesis / protocol parameters do not configure them. The Dijkstra genesis
// is immutable network configuration and the current cardano-ledger
// DijkstraGenesis does not carry these fields at all — the musashi/prototype
// genesis defines only the refScript parameters — so the reference
// implementation falls back to the CIP-0164 defaults internally rather than
// reading them from genesis. dingo mirrors that here so committee formation
// and certification work without modifying the (hash-pinned) genesis file.
//   - committee stake coverage (sigma_c) = 0.99 (top-stake coverage)
//   - quorum stake threshold  (tau)     = 0.75 ("75% certification threshold")
//
// A genesis that does configure either field overrides the corresponding
// default. See issue #2836.
var (
	defaultLeiosCommitteeStakeCoverage = big.NewRat(99, 100)
	defaultLeiosQuorumStakeThreshold   = big.NewRat(3, 4)
)

// leiosCommitteeParamsFromPParams resolves the Leios committee stake coverage
// (sigma_c) and quorum stake threshold (tau) from Dijkstra protocol
// parameters, falling back to the CIP-0164 defaults for any field the genesis
// leaves unset (see defaultLeiosCommitteeStakeCoverage /
// defaultLeiosQuorumStakeThreshold). It revalidates the configured values via
// ValidateLeiosCommitteeParameters and re-checks the tau < sigma_c invariant
// after applying defaults so a partial genesis configuration cannot yield an
// invalid combination. Both returned values are always non-nil, which is what
// lets committee formation and certification proceed on the prototype/musashi
// deployment whose genesis carries only the refScript fields (issue #2836).
func leiosCommitteeParamsFromPParams(
	dijkstraPParams *gdijkstra.DijkstraProtocolParameters,
) (*big.Rat, *big.Rat, error) {
	if err := dijkstraPParams.ValidateLeiosCommitteeParameters(); err != nil {
		return nil, nil, err
	}
	// Return fresh copies so callers cannot mutate the shared defaults.
	sigmaC := new(big.Rat).Set(defaultLeiosCommitteeStakeCoverage)
	if cov := dijkstraPParams.CommitteeStakeCoverage; cov != nil && cov.Rat != nil {
		sigmaC = cov.Rat
	}
	tau := new(big.Rat).Set(defaultLeiosQuorumStakeThreshold)
	if quorum := dijkstraPParams.QuorumStakeThreshold; quorum != nil && quorum.Rat != nil {
		tau = quorum.Rat
	}
	// Defaulting a single unset field against a configured counterpart could
	// break the tau < sigma_c invariant that ValidateLeiosCommitteeParameters
	// only enforces across configured values; re-check after defaulting.
	if tau.Cmp(sigmaC) >= 0 {
		return nil, nil, fmt.Errorf(
			"leios quorum stake threshold (%s) must be less than committee stake coverage (%s)",
			tau.RatString(), sigmaC.RatString(),
		)
	}
	return sigmaC, tau, nil
}

// initLeiosVoteManager builds and starts the Leios vote manager and wires
// it into the ouroboros component's protocol handlers. Invalid voter
// registry entries are fatal at startup.
func (n *Node) initLeiosVoteManager(ctx context.Context) error {
	registry, err := leios.NewVoterRegistry(n.config.leiosVoterPublicKeys)
	if err != nil {
		return fmt.Errorf("invalid leios voter public keys: %w", err)
	}
	mgr, err := leios.NewVoteManager(leios.VoteManagerConfig{
		Logger:   n.config.logger,
		EventBus: n.eventBus,
		StakeProvider: &leiosStakeDistributionAdapter{
			inner: stakeDistributionAdapter{
				ledgerState: n.ledgerState,
			},
		},
		EpochProvider: &epochInfoAdapter{
			ledgerState: n.ledgerState,
		},
		ParamsProvider: &leiosCommitteeParamsAdapter{
			ledgerState: n.ledgerState,
		},
		PrototypeMode: true,
		// LedgerState satisfies leios.SlotProvider directly; the slot
		// window keeps fabricated far-past/future votes away from
		// committee computation and the stake snapshot queries behind
		// it.
		SlotProvider: n.ledgerState,
		// Source the vote-acceptance past bound from the same pipeline
		// timing the pipeline manager uses, so the two components admit
		// votes over the same window and cannot drift.
		VoteWindowSlots: n.leiosPipelineTiming().VoteWindowSlots,
		Registry:        registry,
		PromRegistry:    n.config.promRegistry,
	})
	if err != nil {
		return fmt.Errorf("create leios vote manager: %w", err)
	}
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start leios vote manager: %w", err)
	}
	n.leiosVoteManager = mgr
	n.ouroboros.LeiosVotes = mgr
	n.eventBus.SubscribeFunc(leios.VoteEmittedEventType, func(evt event.Event) {
		data, ok := evt.Data.(leios.VoteEmittedEvent)
		if !ok {
			return
		}
		n.ouroboros.EnqueueLeiosPrototypeVote(data.Vote)
	})
	if n.config.leiosVoteSigningKeyFile != "" && !n.config.blockProducer {
		n.config.logger.Warn(
			"leios vote signing key configured without block producer mode; voting disabled",
			"component",
			"node",
		)
	}
	return nil
}

// leiosPipelineTiming returns the configured pipeline timing, falling back
// to the provisional defaults when no override is set.
func (n *Node) leiosPipelineTiming() leios.PipelineTiming {
	if n.config.leiosPipelineTiming != nil {
		return *n.config.leiosPipelineTiming
	}
	return leios.DefaultPipelineTiming()
}

// initLeiosPipelineManager builds and starts the Leios pipeline manager and
// wires it into the ouroboros component so received endorser blocks are
// tracked through the pipeline. It reuses the same epoch and slot adapters
// as the vote manager.
func (n *Node) initLeiosPipelineManager(ctx context.Context) error {
	mgr, err := leios.NewPipelineManager(leios.PipelineManagerConfig{
		Logger:   n.config.logger,
		EventBus: n.eventBus,
		// LedgerState satisfies leios.SlotProvider directly via
		// CurrentOrTipSlot; pipeline window decisions are slot-driven.
		SlotProvider: n.ledgerState,
		EpochProvider: &epochInfoAdapter{
			ledgerState: n.ledgerState,
		},
		Timing:       n.leiosPipelineTiming(),
		PromRegistry: n.config.promRegistry,
	})
	if err != nil {
		return fmt.Errorf("create leios pipeline manager: %w", err)
	}
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start leios pipeline manager: %w", err)
	}
	n.leiosPipelineManager = mgr
	n.ouroboros.LeiosPipeline = mgr
	return nil
}

// enableLeiosVoting enables vote emission for the block producer's pool.
// The current prototype derives the temporary BLS key from the pool ID. A
// configured key is accepted only when it matches that derivation.
func (n *Node) enableLeiosVoting(creds *forging.PoolCredentials) error {
	if n.leiosVoteManager == nil {
		return nil
	}
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])
	key, err := leios.DerivePrototypeVoteSigningKey(poolKeyHash[:])
	if err != nil {
		return fmt.Errorf("derive prototype leios vote signing key: %w", err)
	}
	if n.config.leiosVoteSigningKeyFile != "" {
		configured, loadErr := leios.LoadVoteSigningKeyFile(
			n.config.leiosVoteSigningKeyFile,
		)
		if loadErr != nil {
			return fmt.Errorf("load leios vote signing key: %w", loadErr)
		}
		if !bytes.Equal(configured.PublicKeyBytes(), key.PublicKeyBytes()) {
			return errors.New(
				"configured leios vote signing key does not match " +
					"the current prototype's pool-derived key",
			)
		}
	}
	n.leiosVoteManager.EnableVoting(poolKeyHash, key)
	n.config.logger.Info(
		"leios voting enabled",
		"component", "node",
		"pool_id", poolID.String(),
	)
	return nil
}
