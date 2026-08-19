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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leios"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
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

// leiosKeyProviderAdapter adapts ledger.LedgerState to leios.LeiosKeyProvider,
// resolving registered Leios keys for exactly the pools the caller names
// (the same set VoteManager already fetched a stake distribution for). It
// returns raw (unverified) keys -- VoteManager itself checks proof of
// possession before trusting one. This is a current-state lookup (see
// leios.LeiosKeyProvider's doc comment), not frozen to a snapshot epoch.
type leiosKeyProviderAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *leiosKeyProviderAdapter) GetLeiosKeys(
	poolKeyHashesHex []string,
) (_ map[string]*lcommon.LeiosKey, err error) {
	if a.ledgerState == nil {
		return nil, errors.New("ledger state unavailable")
	}
	if len(poolKeyHashesHex) == 0 {
		return map[string]*lcommon.LeiosKey{}, nil
	}
	poolKeyHashes := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashesHex))
	for _, poolHashHex := range poolKeyHashesHex {
		raw, decodeErr := hex.DecodeString(poolHashHex)
		if decodeErr != nil || len(raw) != len(lcommon.PoolKeyHash{}) {
			continue
		}
		poolKeyHashes = append(poolKeyHashes, lcommon.PoolKeyHash(raw))
	}
	db := a.ledgerState.Database()
	if db == nil {
		return nil, errors.New("database unavailable")
	}
	txn := db.MetadataTxn(false)
	if txn == nil {
		return nil, errors.New("metadata transaction unavailable")
	}
	defer func() {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"release leios key transaction: %w",
					rollbackErr,
				),
			)
		}
	}()
	return a.ledgerState.NewView(txn).GetLeiosKeys(poolKeyHashes)
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
	if cov := dijkstraPParams.CommitteeStakeCoverage; cov != nil &&
		cov.Rat != nil {
		sigmaC = cov.Rat
	}
	tau := new(big.Rat).Set(defaultLeiosQuorumStakeThreshold)
	if quorum := dijkstraPParams.QuorumStakeThreshold; quorum != nil &&
		quorum.Rat != nil {
		tau = quorum.Rat
	}
	// Defaulting a single unset field against a configured counterpart could
	// break the tau < sigma_c invariant that ValidateLeiosCommitteeParameters
	// only enforces across configured values; re-check after defaulting.
	if tau.Cmp(sigmaC) >= 0 {
		return nil, nil, fmt.Errorf(
			"leios quorum stake threshold (%s) must be less than committee stake coverage (%s)",
			tau.RatString(),
			sigmaC.RatString(),
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
	stakeAdapter := &leiosStakeDistributionAdapter{
		inner: stakeDistributionAdapter{
			ledgerState: n.ledgerState,
		},
	}
	mgr, err := leios.NewVoteManager(leios.VoteManagerConfig{
		Logger:        n.config.logger,
		EventBus:      n.eventBus,
		StakeProvider: stakeAdapter,
		KeyProvider: &leiosKeyProviderAdapter{
			ledgerState: n.ledgerState,
		},
		EpochProvider: &epochInfoAdapter{
			ledgerState: n.ledgerState,
		},
		ParamsProvider: &leiosCommitteeParamsAdapter{
			ledgerState: n.ledgerState,
		},
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
	// Deliberately not wired into ouroboros here. Run initializes the Leios
	// managers before it constructs Ouroboros, so reaching for the instance
	// at this point dereferences a nil pointer. attachLeiosHandlers installs
	// them once an instance exists, on both the startup and live-restore
	// paths.
	// Captured (not discarded) so quiesceForLiveLifecycleOp can unsubscribe
	// this handler before a live database restore/truncate rebuilds
	// leiosVoteManager and calls initLeiosVoteManager again -- the
	// EventBus itself is retained across that cycle, so without this a
	// stale subscription from every earlier cycle stays permanently
	// active alongside the new one, and a single emitted vote gets
	// enqueued (and diffused to peers) once per accumulated subscription.
	n.leiosVoteEmittedSubId = n.eventBus.SubscribeFunc(
		leios.VoteEmittedEventType,
		func(evt event.Event) {
			data, ok := evt.Data.(leios.VoteEmittedEvent)
			if !ok {
				return
			}
			n.ouroboros().EnqueueLeiosPrototypeVote(data.Vote)
		},
	)
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
	// See initLeiosVoteManager: attachLeiosHandlers does the wiring, once an
	// Ouroboros instance exists.
	return nil
}

// enableLeiosVoting enables vote emission for the block producer's pool,
// using the operator-configured BLS signing key. A pool started without one
// runs as a non-voting relay: upstream removed the insecure pool-derived
// key shortcut, so there is no fallback that lets a pool vote without a
// real registered key.
func (n *Node) enableLeiosVoting(creds *forging.PoolCredentials) error {
	if n.leiosVoteManager == nil {
		return nil
	}
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	if n.config.leiosVoteSigningKeyFile == "" {
		n.config.logger.Warn(
			"no leios vote signing key configured; running as a non-voting relay",
			"component",
			"node",
		)
		return nil
	}
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])
	key, err := leios.LoadVoteSigningKeyFile(
		n.config.leiosVoteSigningKeyFile,
	)
	if err != nil {
		return fmt.Errorf("load leios vote signing key: %w", err)
	}
	if err := n.leiosVoteManager.ValidateVotingKey(poolKeyHash, key); err != nil {
		// ValidateVotingKey's own error already names which sources it
		// checked (the on-chain registration and leiosVoterPublicKeys) and
		// whether the problem was "not found" or "found but mismatched," so
		// no remedy is added here: the on-chain key takes precedence over
		// leiosVoterPublicKeys, so directing every failure at the static
		// registry would be wrong advice whenever the real problem is a key
		// that no longer matches the pool's on-chain registration.
		return fmt.Errorf("validate configured leios vote signing key: %w", err)
	}
	if err := n.leiosVoteManager.EnableVoting(poolKeyHash, key); err != nil {
		return fmt.Errorf("enable leios voting: %w", err)
	}
	n.config.logger.Info(
		"leios voting enabled",
		"component", "node",
		"pool_id", poolID.String(),
	)
	return nil
}

// attachLeiosHandlers installs the optional Leios prototype handlers onto an
// Ouroboros instance.
//
// They are not OuroborosConfig fields because their managers start on their
// own path, which runs before Ouroboros is constructed during startup and
// again before Ouroboros is replaced during a live restore. Both paths call
// this immediately after they have an instance, which is the only safe point:
// earlier there is nothing to wire, and later protocol traffic could already
// be arriving unhandled.
//
// A nil manager means Leios is disabled, and is skipped rather than an error.
func (n *Node) attachLeiosHandlers(o *ouroborosPkg.Ouroboros) error {
	if o == nil {
		return errors.New("cannot attach leios handlers: ouroboros is nil")
	}
	if n.leiosVoteManager != nil {
		if err := o.SetLeiosVotes(n.leiosVoteManager); err != nil {
			return fmt.Errorf("wire leios vote manager: %w", err)
		}
	}
	if n.leiosPipelineManager != nil {
		if err := o.SetLeiosPipeline(n.leiosPipelineManager); err != nil {
			return fmt.Errorf("wire leios pipeline manager: %w", err)
		}
	}
	return nil
}
