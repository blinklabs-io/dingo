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

package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// deriveHost is the IndexerDB's side of epochengine.Host. Everything it
// answers comes from the IndexerDB's own rows: the blocks the writer has
// indexed so far, and the facts it derived at the earlier boundaries.
// Scheduling is synchronous, so the deferral and notification hooks do
// nothing.
type deriveHost struct {
	d *Deriver
}

var _ epochengine.Host = deriveHost{}

func (h deriveHost) LedgerDB() ledgerdb.LedgerDB { return h.d.ldb }

func (h deriveHost) EraByID(id uint) (*eras.EraDesc, bool) {
	era := eras.GetEraByIdIn(h.d.eraList, id)
	return era, era != nil
}

func (h deriveHost) SlotToEpoch(slot uint64) (models.Epoch, error) {
	epochs, err := h.d.ldb.GetEpochs(h.d.txn)
	if err != nil {
		return models.Epoch{}, err
	}
	for _, e := range slices.Backward(epochs) {
		if slot >= e.StartSlot {
			return e, nil
		}
	}
	return models.Epoch{}, fmt.Errorf("no epoch holds slot %d", slot)
}

func (deriveHost) StagePoolBoundary(ledgerdb.Txn, uint64, uint64) {}

func (deriveHost) PublishRewardFacts(ledgerdb.Txn, uint64, uint64) {}

func (deriveHost) QueueRewardCreditCompaction(context.Context) {}

func (deriveHost) IncSkippedStakeRewardRounds() {}

// RewardBlockCounts counts the blocks the IndexerDB indexed for epoch. The
// IndexerDB holds every block of the chain, so no epoch's counts are unknown.
func (h deriveHost) RewardBlockCounts(
	txn ledgerdb.Txn,
	epoch uint64,
	poolKeys []lcommon.PoolKeyHash,
) (map[string]uint64, uint64, bool, error) {
	ep, err := h.d.ldb.GetEpoch(epoch, txn)
	if err != nil {
		return nil, 0, false, err
	}
	if ep == nil || ep.LengthInSlots == 0 {
		return nil, 0, true, nil
	}
	end := ep.StartSlot + uint64(ep.LengthInSlots) - 1
	return h.d.adapter.CountEpochPoolBlocks(
		epoch, poolKeys, ep.StartSlot, end, txn,
	)
}

func (h deriveHost) RewardEpochFees(
	txn ledgerdb.Txn,
	epoch models.Epoch,
) (uint64, error) {
	return epochengine.RewardEpochFees(h.d.ldb, txn, epoch)
}

func (deriveHost) RewardRollbackSeq(ledgerdb.Txn) (uint64, error) {
	return 0, nil
}

// AccountInactivityActivationFloors reports the activation epoch for every
// account the one-time activation stamped, which the reward round clamps the
// account's expiration to.
func (h deriveHost) AccountInactivityActivationFloors(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
) (map[string]uint64, error) {
	floors := map[string]uint64{}
	epoch, activated, err := h.d.engine.DelegatorInactivityActivationEpoch(txn)
	if err != nil || !activated || len(refs) == 0 {
		return floors, err
	}
	members, err := h.d.ldb.AccountInactivityActivationMembership(
		h.d.ctx, refs, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("load activation membership: %w", err)
	}
	for key := range members {
		floors[key] = epoch
	}
	return floors, nil
}

func (h deriveHost) IterateLiveUtxos(
	txn ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	return h.d.ldb.IterateLiveUtxos(h.d.ctx, txn, fn)
}

// PublishAvvmRemoval does nothing: the IndexerDB keeps no live set whose
// readers an event would reach, and the removed outputs stay in its rows.
func (deriveHost) PublishAvvmRemoval(
	context.Context,
	ledgerdb.Txn,
	uint64,
	[]models.UtxoId,
) {
}

// RewardLikelihoodDB is a store that keeps nothing: the non-myopic pool
// ranking is state of the ledger, not an indexed fact.
func (deriveHost) RewardLikelihoodDB() ledgerdb.RewardLikelihoodStore {
	return noLikelihood{}
}

type noLikelihood struct{}

func (noLikelihood) SaveRewardLikelihoodState(
	*models.RewardLikelihoodState,
	[]*models.RewardPoolLikelihood,
	ledgerdb.Txn,
) error {
	return nil
}

func (noLikelihood) GetRewardLikelihoodStateBeforeEpoch(
	uint64,
	ledgerdb.Txn,
) (*models.RewardLikelihoodState, error) {
	return nil, nil
}

func (noLikelihood) GetRewardLikelihoodStateAtSlot(
	uint64,
	ledgerdb.Txn,
) (*models.RewardLikelihoodState, error) {
	return nil, nil
}

func (noLikelihood) GetRewardPoolLikelihoods(
	uint64,
	ledgerdb.Txn,
) ([]*models.RewardPoolLikelihood, error) {
	return nil, nil
}

// TipNonceState reports no applied tip: the nonce fold of a boundary reads
// the stored block nonces of the epoch, which the writer has already indexed
// up to the boundary.
func (deriveHost) TipNonceState() (uint64, []byte) { return 0, nil }

func (h deriveHost) BlockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	return h.d.ldb.GetBlockNoncesInSlotRange(start, end, txn)
}

// ComputeCandidateNonce reads the evolving nonce the block indexer stored for
// the epoch's last block, and the one of the last block before the freeze
// cutoff for the candidate nonce. An epoch with no block carries the previous
// values.
func (h deriveHost) ComputeCandidateNonce(
	ctx context.Context,
	txn ledgerdb.Txn,
	eraID uint,
	prevEvolvingNonce []byte,
	prevCandidateNonce []byte,
	epochStartSlot uint64,
	epochLength uint64,
) ([]byte, []byte, error) {
	epochEnd := epochStartSlot + epochLength
	window := epochengine.NonceStabilityWindow(h.d.cfg.NodeConfig, eraID)
	cutoff := epochStartSlot
	if window < epochLength {
		cutoff = epochEnd - window
	}
	nonceBefore := func(bound uint64, prev []byte) ([]byte, error) {
		if bound <= epochStartSlot {
			return bytes.Clone(prev), nil
		}
		blk, err := h.d.blockBefore(ctx, txn, bound)
		if errors.Is(err, models.ErrBlockNotFound) ||
			(err == nil && blk.Slot < epochStartSlot) {
			return bytes.Clone(prev), nil
		}
		if err != nil {
			return nil, fmt.Errorf("look up block before slot %d: %w", bound, err)
		}
		nonce, err := h.d.ldb.GetBlockNonce(
			ocommon.Point{Slot: blk.Slot, Hash: blk.Hash}, txn,
		)
		if err != nil {
			return nil, fmt.Errorf("read nonce of block at slot %d: %w", blk.Slot, err)
		}
		if len(nonce) != lcommon.Blake2b256Size {
			return nil, fmt.Errorf(
				"block at slot %d has no stored nonce", blk.Slot,
			)
		}
		return nonce, nil
	}
	evolving, err := nonceBefore(epochEnd, prevEvolvingNonce)
	if err != nil {
		return nil, nil, err
	}
	candidate, err := nonceBefore(min(cutoff, epochEnd), prevCandidateNonce)
	if err != nil {
		return nil, nil, err
	}
	return candidate, evolving, nil
}

// EpochLabNonce carries the parent hash of the epoch's last block, which is
// the lab nonce the reference node carries across the boundary.
func (h deriveHost) EpochLabNonce(
	ctx context.Context,
	txn ledgerdb.Txn,
	startSlot, endSlot uint64,
	carried []byte,
) ([]byte, error) {
	last, err := h.d.blockBefore(ctx, txn, endSlot)
	if errors.Is(err, models.ErrBlockNotFound) ||
		(err == nil && last.Slot < startSlot) {
		return bytes.Clone(carried), nil
	}
	if err != nil {
		return nil, fmt.Errorf("look up boundary block: %w", err)
	}
	prev := last.PrevHash
	if len(prev) == 0 {
		decoded, err := last.Decode()
		if err != nil {
			return nil, fmt.Errorf(
				"decode block at slot %d for its parent: %w", last.Slot, err,
			)
		}
		prev = decoded.PrevHash().Bytes()
	}
	if len(prev) == 0 {
		return bytes.Clone(carried), nil
	}
	if len(prev) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"boundary block parent hash at slot %d has length %d",
			last.Slot, len(prev),
		)
	}
	genesis, err := genesisBlockHash(h.d.cfg.NodeConfig)
	if err == nil && bytes.Equal(prev, genesis) {
		return bytes.Clone(carried), nil
	}
	return bytes.Clone(prev), nil
}

func (deriveHost) RewardInputGuards() epochengine.RewardInputGuards {
	return epochengine.RewardInputGuards{}
}

func (deriveHost) DeferStakeRewardPrecompute(uint64, uint64, uint64) {}

func (deriveHost) PrecomputeChunkPools() int { return 0 }

func (deriveHost) RewardPrecomputeChunkDone(int, int) {}

func (deriveHost) RewardPrecomputeBeforeSave() {}

// blockBefore returns the last block below slot in the IndexerDB's view of the
// chain.
func (d *Deriver) blockBefore(
	_ context.Context,
	txn ledgerdb.Txn,
	slot uint64,
) (models.Block, error) {
	t := dbadapter.Unwrap(txn)
	if t == nil {
		return models.Block{}, errors.New("block lookup needs a transaction")
	}
	return database.BlockBeforeSlotTxn(t, slot)
}
