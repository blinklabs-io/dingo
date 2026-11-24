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

// Package epochengine computes the state transitions of an epoch boundary
// over a ledgerdb.LedgerDB: stake rewards, MIR, pool retirement, donations,
// ADA pots, the epoch nonce and governance ratification.
//
// The engine owns the computation only. State that belongs to the host (the
// live set's served reads, locks, background jobs, the event bus and the
// chain) is reached through Host, and scheduling stays with each host.
package epochengine

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Config is the static configuration the boundary computation reads.
type Config struct {
	Logger            *slog.Logger
	CardanoNodeConfig *cardano.CardanoNodeConfig

	// MinPoolMargin is the CIP-23 minimum pool margin in basis points; zero
	// disables it.
	MinPoolMargin uint
	// PledgeLeverageEnabled and PledgeLeverage select the pre-Dijkstra
	// pledge-leverage setting.
	PledgeLeverageEnabled bool
	PledgeLeverage        uint
	// FullPotRewardsEnabled turns on CIP-0163 full-pot reward distribution.
	FullPotRewardsEnabled bool
	// DelegatorInactivityEnabled turns on CIP-0163 reward-account inactivity
	// and DelegatorInactivity is its window in epochs.
	DelegatorInactivityEnabled bool
	DelegatorInactivity        uint64
	// WarmGenesisStake lets the first reward rounds of a network see the
	// genesis stake.
	WarmGenesisStake bool
	// RewardUpdateStabilityWindow, when non-nil, replaces the RUPD
	// stabilisation window derived from the Shelley genesis.
	RewardUpdateStabilityWindow *uint64
	// FatalErrorFunc is called with an error that must stop the node.
	FatalErrorFunc func(err error)
	// HaltError is wrapped by errors that stop the host's pipeline for good.
	HaltError error
}

// RewardInputGuards are the host-owned counters that detect rollback-sensitive
// reward inputs changing under a calculation. A nil field reads as zero.
type RewardInputGuards struct {
	// Generation changes whenever a rollback changes a reward input.
	Generation *atomic.Uint64
	// RollbackActive is non-zero while a rollback is in progress.
	RollbackActive *atomic.Int64
	// PrecomputeFence is bumped by a boundary before it applies a round, which
	// stops background chunks from writing that round's outputs.
	PrecomputeFence *atomic.Uint64
}

func (g RewardInputGuards) generation() uint64 {
	if g.Generation == nil {
		return 0
	}
	return g.Generation.Load()
}

func (g RewardInputGuards) fence() uint64 {
	if g.PrecomputeFence == nil {
		return 0
	}
	return g.PrecomputeFence.Load()
}

// Host is the state a host owns and the engine reads or notifies.
type Host interface {
	// LedgerDB returns the store the boundary reads and writes.
	LedgerDB() ledgerdb.LedgerDB
	// EraByID resolves an era descriptor.
	EraByID(id uint) (*eras.EraDesc, bool)
	// SlotToEpoch resolves the epoch containing slot.
	SlotToEpoch(slot uint64) (models.Epoch, error)

	// StagePoolBoundary records the POOLREAP boundary of newEpoch for the
	// host's derived stores.
	StagePoolBoundary(txn ledgerdb.Txn, newEpoch, boundarySlot uint64)
	// PublishRewardFacts announces, after txn commits, that the reward facts
	// of snapshotEpoch captured at slot are available.
	PublishRewardFacts(txn ledgerdb.Txn, snapshotEpoch, slot uint64)
	// QueueRewardCreditCompaction schedules compaction of credited rounds.
	QueueRewardCreditCompaction(ctx context.Context)
	// IncSkippedStakeRewardRounds counts a reward round that cannot be applied.
	IncSkippedStakeRewardRounds()

	// RewardBlockCounts returns the per-pool and total block counts of epoch
	// for poolKeys; known is false when the host holds no counts for it.
	RewardBlockCounts(
		txn ledgerdb.Txn,
		epoch uint64,
		poolKeys []lcommon.PoolKeyHash,
	) (counts map[string]uint64, total uint64, known bool, err error)
	// RewardEpochFees returns the fees collected in epoch.
	RewardEpochFees(txn ledgerdb.Txn, epoch models.Epoch) (uint64, error)
	// RewardRollbackSeq returns the durable rollback counter of the derived
	// reward rows, zero when the host keeps none.
	RewardRollbackSeq(txn ledgerdb.Txn) (uint64, error)
	// AccountInactivityActivationFloors returns, keyed by
	// StakeCredentialRef.MapKey, the activation epoch of every ref whose
	// account carries a CIP-0163 activation stamp.
	AccountInactivityActivationFloors(
		txn ledgerdb.Txn,
		refs []models.StakeCredentialRef,
	) (map[string]uint64, error)
	// RewardLikelihoodDB returns the store of the non-myopic likelihood state.
	RewardLikelihoodDB() ledgerdb.RewardLikelihoodStore

	// TipNonceState returns the slot of the applied tip and the nonce of its
	// block, which a resumed nonce fold continues from.
	TipNonceState() (tipSlot uint64, tipBlockNonce []byte)
	// BlockNoncesInSlotRange returns the stored block nonces in [start, end).
	BlockNoncesInSlotRange(
		start, end uint64,
		txn ledgerdb.Txn,
	) ([]models.BlockNonce, error)
	// ComputeCandidateNonce folds the epoch's blocks from prevEvolvingNonce and
	// prevCandidateNonce over [epochStartSlot, epochStartSlot+epochLength) and
	// returns the frozen candidate nonce and the evolving nonce.
	ComputeCandidateNonce(
		ctx context.Context,
		txn ledgerdb.Txn,
		eraID uint,
		prevEvolvingNonce []byte,
		prevCandidateNonce []byte,
		epochStartSlot uint64,
		epochLength uint64,
	) (candidate, evolving []byte, err error)
	// EpochLabNonce returns the lab nonce an epoch spanning
	// [startSlot, endSlot) carries to the next one.
	EpochLabNonce(
		ctx context.Context,
		txn ledgerdb.Txn,
		startSlot, endSlot uint64,
		carried []byte,
	) ([]byte, error)

	// RewardInputGuards returns the host's rollback-detection counters.
	RewardInputGuards() RewardInputGuards
	// DeferStakeRewardPrecompute asks the host to retry a precompute of
	// newEpoch once its tip reaches cutoffSlot.
	DeferStakeRewardPrecompute(newEpoch, cutoffSlot, generation uint64)
	// PrecomputeChunkPools returns the pools per precompute chunk, or zero for
	// the default.
	PrecomputeChunkPools() int
	// RewardPrecomputeChunkDone is called after each non-final chunk and its
	// cursor are staged, inside the chunk's transaction and before it commits.
	// The final chunk does not call it, so completedPools never equals
	// totalPools.
	RewardPrecomputeChunkDone(completedPools, totalPools int)
	// RewardPrecomputeBeforeSave is called before a precompute chunk is saved.
	RewardPrecomputeBeforeSave()
}

// Engine runs epoch-boundary steps over a Host's LedgerDB.
type Engine struct {
	config Config
	host   Host
}

// New returns an Engine over host.
func New(config Config, host Host) *Engine {
	return &Engine{config: config, host: host}
}

func (e *Engine) ledgerDB() ledgerdb.LedgerDB {
	return e.host.LedgerDB()
}
