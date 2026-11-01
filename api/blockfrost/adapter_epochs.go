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

package blockfrost

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// epochStakeLabel maps a Blockfrost epoch to the epoch label Dingo stamps on
// the stake snapshot that was active during it. The snapshot labelled E is
// captured as epoch E begins and holds the stake as of the end of epoch E-1,
// which is active in epoch E+1, so epoch K runs on label K-1. Epoch 0 has no
// snapshot; ok is false there.
func epochStakeLabel(epoch uint64) (uint64, bool) {
	if epoch == 0 {
		return 0, false
	}
	return epoch - 1, true
}

// epochInfoFor assembles the Blockfrost epoch summary for an epoch row. The
// slot range is capped at tipSlot so the current epoch reports only what has
// been synced.
func (a *NodeAdapter) epochInfoFor(
	ctx context.Context,
	epoch models.Epoch,
	tipSlot uint64,
) (EpochInfo, error) {
	startTime, err := a.ledgerState.SlotToTime(epoch.StartSlot)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"get epoch start time for slot %d: %w",
			epoch.StartSlot,
			err,
		)
	}
	endTime := epochEndTime(startTime, epoch)
	endSlot := tipSlot
	if epoch.LengthInSlots > 0 {
		epochLastSlot := epoch.StartSlot + uint64(epoch.LengthInSlots) - 1
		if epochLastSlot < endSlot {
			endSlot = epochLastSlot
		}
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()
	slotRanges, ok := meta.(metadata.SlotRangeStore)
	if !ok {
		return EpochInfo{}, errors.New(
			"metadata store does not support slot-range statistics",
		)
	}
	blockStats, err := slotRanges.GetBlockSlotRangeStats(
		epoch.StartSlot, endSlot, txn.Metadata(),
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"count epoch blocks for slots %d-%d: %w",
			epoch.StartSlot,
			endSlot,
			err,
		)
	}
	blockCount := blockStats.Count
	txCount, err := slotRanges.CountTransactionsInSlotRange(
		epoch.StartSlot, endSlot, txn.Metadata(),
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"count epoch transactions for slots %d-%d: %w",
			epoch.StartSlot,
			endSlot,
			err,
		)
	}
	firstBlockTime := int64(0)
	lastBlockTime := int64(0)
	if blockCount > 0 {
		firstTime, err := a.ledgerState.SlotToTime(blockStats.FirstSlot)
		if err != nil {
			return EpochInfo{}, fmt.Errorf(
				"get first epoch block time for slot %d: %w",
				blockStats.FirstSlot,
				err,
			)
		}
		lastTime, err := a.ledgerState.SlotToTime(blockStats.LastSlot)
		if err != nil {
			return EpochInfo{}, fmt.Errorf(
				"get last epoch block time for slot %d: %w",
				blockStats.LastSlot,
				err,
			)
		}
		firstBlockTime = firstTime.Unix()
		lastBlockTime = lastTime.Unix()
	}

	fees, err := meta.SumTransactionFeesInSlotRange(
		epoch.StartSlot, endSlot, txn.Metadata(),
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"sum epoch fees for slots %d-%d: %w",
			epoch.StartSlot,
			endSlot,
			err,
		)
	}
	output, err := meta.SumTransactionOutputsInSlotRange(
		epoch.StartSlot, endSlot, txn.Metadata(),
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"sum epoch outputs for slots %d-%d: %w",
			epoch.StartSlot,
			endSlot,
			err,
		)
	}
	var activeStake *string
	if label, ok := epochStakeLabel(epoch.EpochId); ok {
		summary, err := meta.GetEpochSummary(label, txn.Metadata())
		if err != nil {
			return EpochInfo{}, fmt.Errorf(
				"get epoch summary %d: %w",
				label,
				err,
			)
		}
		if summary != nil && summary.SnapshotReady {
			stake := strconv.FormatUint(uint64(summary.TotalActiveStake), 10)
			activeStake = &stake
		}
	}
	return EpochInfo{
		Epoch:          epoch.EpochId,
		StartTime:      startTime.Unix(),
		EndTime:        endTime.Unix(),
		FirstBlockTime: firstBlockTime,
		LastBlockTime:  lastBlockTime,
		BlockCount:     blockCount,
		TxCount:        txCount,
		Output:         strconv.FormatUint(output, 10),
		Fees:           strconv.FormatUint(fees, 10),
		ActiveStake:    activeStake,
	}, nil
}

// reachedEpoch loads an epoch row and reports ErrEpochNotFound for an epoch
// that is absent or begins after the synced tip.
func (a *NodeAdapter) reachedEpoch(
	ctx context.Context,
	epoch uint64,
) (models.Epoch, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	row, err := db.GetEpoch(epoch, txn)
	if err != nil {
		return models.Epoch{}, fmt.Errorf("get epoch %d: %w", epoch, err)
	}
	if row == nil || row.StartSlot > a.ledgerState.Tip().Point.Slot {
		return models.Epoch{}, fmt.Errorf(
			"epoch %d: %w",
			epoch,
			ErrEpochNotFound,
		)
	}
	return *row, nil
}

// Epoch returns the requested epoch.
func (a *NodeAdapter) Epoch(
	ctx context.Context,
	epoch uint64,
) (EpochInfo, error) {
	row, err := a.reachedEpoch(ctx, epoch)
	if err != nil {
		return EpochInfo{}, err
	}
	return a.epochInfoFor(ctx, row, a.ledgerState.Tip().Point.Slot)
}

// currentEpochNumber is the epoch containing the synced tip.
func (a *NodeAdapter) currentEpochNumber() (uint64, error) {
	tipSlot := a.ledgerState.Tip().Point.Slot
	tipEpoch, err := a.ledgerState.SlotToEpoch(tipSlot)
	if err != nil {
		return 0, fmt.Errorf("get epoch for tip slot %d: %w", tipSlot, err)
	}
	return tipEpoch.EpochId, nil
}

// epochWindow resolves a page of epoch numbers on one side of epoch: the
// first number of the page, how many it holds, and the size of the whole
// window. A page past the end of the window holds none.
func epochWindow(
	epoch uint64,
	current uint64,
	after bool,
	params PaginationParams,
) (first uint64, count int, total int) {
	var span uint64
	if after {
		if epoch >= current {
			return 0, 0, 0
		}
		span = current - epoch
	} else {
		epoch = min(epoch, current)
		span = epoch
	}
	total = saturatingInt(span)
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return 0, 0, total
	}
	count = min(params.Count, total-offset)
	skipped := nonNegativeUint64(offset)
	if after {
		return epoch + 1 + skipped, count, total
	}
	return epoch - 1 - skipped, count, total
}

// saturatingInt converts v to int, stopping at the largest value a count in
// this API can take.
func saturatingInt(v uint64) int {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(v) // #nosec G115 -- bounded above by math.MaxInt32
}

// nonNegativeUint64 converts a pagination offset, which is never negative.
func nonNegativeUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v) // #nosec G115 -- checked non-negative above
}

func (a *NodeAdapter) epochsAround(
	ctx context.Context,
	epoch uint64,
	after bool,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	if _, err := a.reachedEpoch(ctx, epoch); err != nil {
		return nil, 0, err
	}
	current, err := a.currentEpochNumber()
	if err != nil {
		return nil, 0, err
	}
	first, count, total := epochWindow(epoch, current, after, params)
	ret := make([]EpochInfo, 0, count)
	tipSlot := a.ledgerState.Tip().Point.Slot
	for i := range count {
		number := first + uint64(i)
		if !after {
			number = first - uint64(i)
		}
		row, err := a.reachedEpoch(ctx, number)
		if err != nil {
			return nil, 0, err
		}
		info, err := a.epochInfoFor(ctx, row, tipSlot)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, info)
	}
	return ret, total, nil
}

// EpochsAfter returns the epochs following epoch in ascending order.
func (a *NodeAdapter) EpochsAfter(
	ctx context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return a.epochsAround(ctx, epoch, true, params)
}

// EpochsBefore returns the epochs preceding epoch, nearest first.
func (a *NodeAdapter) EpochsBefore(
	ctx context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochInfo, int, error) {
	return a.epochsAround(ctx, epoch, false, params)
}

func (a *NodeAdapter) epochStakeRows(
	ctx context.Context,
	epoch uint64,
	poolKeyHash []byte,
	params PaginationParams,
) ([]models.EpochStakeRow, int, error) {
	if _, err := a.reachedEpoch(ctx, epoch); err != nil {
		return nil, 0, err
	}
	label, ok := epochStakeLabel(epoch)
	if !ok {
		return []models.EpochStakeRow{}, 0, nil
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []models.EpochStakeRow{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	incomplete, err := a.ledgerState.RewardStakeInputsIncomplete(
		txn, label, label+1,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("check epoch %d stakes: %w", epoch, err)
	}
	if incomplete {
		return nil, 0, fmt.Errorf(
			"stake distribution of epoch %d: %w", epoch, ErrHistoryNotRetained,
		)
	}
	rows, total, err := db.Metadata().GetEpochStakes(
		label, poolKeyHash, params.Count, offset, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get epoch %d stakes: %w", epoch, err)
	}
	if total == 0 {
		if err := a.checkStakeHistoryRetained(
			db.Metadata(), label, epoch, txn.Metadata(),
		); err != nil {
			return nil, 0, err
		}
	}
	return rows, total, nil
}

// checkStakeHistoryRetained distinguishes a genuinely empty stake
// distribution from one whose per-credential rows are gone. The epoch summary
// is kept for every epoch while the rows are pruned outside API storage
// mode, so a ready summary that counts delegators over an epoch with no rows
// is pruned history. A distribution never captured is unavailable too.
func (a *NodeAdapter) checkStakeHistoryRetained(
	meta metadata.MetadataStore,
	label uint64,
	epoch uint64,
	txn types.Txn,
) error {
	rows, total, err := meta.GetEpochStakes(label, nil, 1, 0, txn)
	if err != nil {
		return fmt.Errorf("count epoch %d stakes: %w", epoch, err)
	}
	if total > 0 || len(rows) > 0 {
		return nil
	}
	summary, err := meta.GetEpochSummary(label, txn)
	if err != nil {
		return fmt.Errorf("get epoch summary %d: %w", label, err)
	}
	if summary != nil && summary.SnapshotReady && summary.TotalDelegators == 0 {
		return nil
	}
	return fmt.Errorf(
		"stake distribution of epoch %d: %w", epoch, ErrHistoryNotRetained,
	)
}

func (a *NodeAdapter) stakeAddressFromTag(
	tag uint8,
	key []byte,
) (string, error) {
	hash, err := lcommon.NewBlake2b224Checked(key)
	if err != nil {
		return "", fmt.Errorf("stake credential: %w", err)
	}
	credType := uint(lcommon.CredentialTypeAddrKeyHash)
	if tag == 1 {
		credType = uint(lcommon.CredentialTypeScriptHash)
	}
	return stakeAddressFromCredential(
		lcommon.Credential{
			CredType:   credType,
			Credential: lcommon.CredentialHash(hash),
		},
		a.networkID(),
	)
}

func poolIDString(poolKeyHash []byte) (string, error) {
	hash, err := lcommon.NewBlake2b224Checked(poolKeyHash)
	if err != nil {
		return "", fmt.Errorf("pool key hash: %w", err)
	}
	return lcommon.PoolId(hash).String(), nil
}

// EpochStakes returns the stake distribution active in an epoch.
func (a *NodeAdapter) EpochStakes(
	ctx context.Context,
	epoch uint64,
	params PaginationParams,
) ([]EpochStakeInfo, int, error) {
	rows, total, err := a.epochStakeRows(ctx, epoch, nil, params)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]EpochStakeInfo, 0, len(rows))
	for _, row := range rows {
		address, err := a.stakeAddressFromTag(row.CredentialTag, row.StakingKey)
		if err != nil {
			return nil, 0, err
		}
		poolID, err := poolIDString(row.PoolKeyHash)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, EpochStakeInfo{
			StakeAddress: address,
			PoolID:       poolID,
			Amount:       strconv.FormatUint(row.Stake, 10),
		})
	}
	return ret, total, nil
}

// EpochPoolStakes returns one pool's stake distribution active in an epoch.
func (a *NodeAdapter) EpochPoolStakes(
	ctx context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]EpochPoolStakeInfo, int, error) {
	poolKeyHash, err := parsePoolID(poolID)
	if err != nil {
		return nil, 0, err
	}
	rows, total, err := a.epochStakeRows(ctx, epoch, poolKeyHash, params)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]EpochPoolStakeInfo, 0, len(rows))
	for _, row := range rows {
		address, err := a.stakeAddressFromTag(row.CredentialTag, row.StakingKey)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, EpochPoolStakeInfo{
			StakeAddress: address,
			Amount:       strconv.FormatUint(row.Stake, 10),
		})
	}
	return ret, total, nil
}

// epochSlotRange is the inclusive slot range of an epoch, capped at the tip.
func (a *NodeAdapter) epochSlotRange(
	ctx context.Context,
	epoch uint64,
) (uint64, uint64, error) {
	row, err := a.reachedEpoch(ctx, epoch)
	if err != nil {
		return 0, 0, err
	}
	end := a.ledgerState.Tip().Point.Slot
	if row.LengthInSlots > 0 {
		last := row.StartSlot + uint64(row.LengthInSlots) - 1
		if last < end {
			end = last
		}
	}
	return row.StartSlot, end, nil
}

// checkBlockHistoryRetained reports ErrHistoryNotRetained when block_nonce no
// longer holds every block in the inclusive slot range, so a block list is
// never served short as though it were complete.
func checkBlockHistoryRetained(
	db *database.Database,
	txn *database.Txn,
	startSlot uint64,
	endSlot uint64,
) error {
	return checkRetained(
		database.BlockHistoryRetained, db, txn, startSlot, endSlot,
	)
}

// checkPoolBlockHistoryRetained is checkBlockHistoryRetained for one pool's
// blocks, which never include a Byron block.
func checkPoolBlockHistoryRetained(
	db *database.Database,
	txn *database.Txn,
	startSlot uint64,
	endSlot uint64,
) error {
	return checkRetained(
		database.PoolBlockHistoryRetained, db, txn, startSlot, endSlot,
	)
}

func checkRetained(
	retainedFn func(metadata.MetadataStore, types.Txn, uint64, uint64) (bool, error),
	db *database.Database,
	txn *database.Txn,
	startSlot uint64,
	endSlot uint64,
) error {
	retained, err := retainedFn(
		db.Metadata(), txn.Metadata(), startSlot, endSlot,
	)
	if err != nil {
		return fmt.Errorf("check block history: %w", err)
	}
	if !retained {
		return ErrHistoryNotRetained
	}
	return nil
}

func hexHashes(hashes [][]byte) []string {
	ret := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		ret = append(ret, hex.EncodeToString(hash))
	}
	return ret
}

// EpochBlocks returns the hashes of the blocks minted in an epoch.
func (a *NodeAdapter) EpochBlocks(
	ctx context.Context,
	epoch uint64,
	params PaginationParams,
) ([]string, int, error) {
	start, end, err := a.epochSlotRange(ctx, epoch)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []string{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	if err := checkBlockHistoryRetained(db, txn, start, end); err != nil {
		return nil, 0, fmt.Errorf("blocks of epoch %d: %w", epoch, err)
	}
	hashes, total, err := db.Metadata().GetBlockHashesInSlotRange(
		start, end, params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get epoch %d blocks: %w", epoch, err)
	}
	return hexHashes(hashes), total, nil
}

// EpochPoolBlocks returns the hashes of the blocks one pool minted in an
// epoch.
func (a *NodeAdapter) EpochPoolBlocks(
	ctx context.Context,
	epoch uint64,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	poolKeyHash, err := parsePoolID(poolID)
	if err != nil {
		return nil, 0, err
	}
	start, end, err := a.epochSlotRange(ctx, epoch)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []string{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	if err := checkBlockHistoryRetained(db, txn, start, end); err != nil {
		return nil, 0, fmt.Errorf("blocks of epoch %d: %w", epoch, err)
	}
	hashes, total, err := db.Metadata().GetPoolBlockHashesInSlotRange(
		poolKeyHash, start, end, params.Count, offset, params.Order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get epoch %d pool blocks: %w", epoch, err)
	}
	return hexHashes(hashes), total, nil
}
