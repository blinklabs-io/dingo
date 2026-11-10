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
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// neighborBlocks returns the page of blocks adjacent to the identified block
// and the number of block heights on that side of it: the ones that follow it
// up to the tip when forward is set, else the ones that precede it. Page one
// holds the nearest blocks; within a page heights ascend, as Blockfrost lists
// them, and a descending order reverses the page. Neighbors are the adjacent
// indexed entries, so gaps in the internal ID space are skipped and a Byron
// epoch boundary block, which shares its parent's height, is a neighbor of its
// own. Pages count entries, so the height total can fall short of them by the
// number of boundary blocks on that side. The total is the same for every page
// of a request.
func (a *NodeAdapter) neighborBlocks(
	ctx context.Context,
	id string,
	params PaginationParams,
	forward bool,
) ([]models.Block, int, error) {
	origin, err := a.resolveBlock(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Rollback() //nolint:errcheck
	var tipID uint64
	var available uint64
	if forward {
		tip, err := db.GetTip(txn)
		if err != nil {
			return nil, 0, fmt.Errorf("get chain tip: %w", err)
		}
		if len(tip.Point.Hash) == 0 {
			return []models.Block{}, 0, nil
		}
		tipID, err = database.BlockIDByPointMetadataTxn(txn, tip.Point)
		if err != nil {
			return nil, 0, fmt.Errorf("resolve chain tip: %w", err)
		}
		if tip.BlockNumber > origin.Number {
			available = tip.BlockNumber - origin.Number
		}
	} else {
		available = origin.Number
	}
	// #nosec G115 -- clamped to MaxInt
	total := int(min(available, uint64(math.MaxInt)))
	offset, ok := paginationOffset(params)
	if !ok {
		return []models.Block{}, total, nil
	}
	// Distinct entries have distinct IDs, so the ID span bounds how many
	// entries lie on this side of origin, and a page past it is empty.
	cursor := origin.ID
	span := origin.ID - database.BlockInitialIndex
	if forward {
		span = 0
		if tipID > origin.ID {
			span = tipID - origin.ID
		}
	}
	if uint64(offset) >= span { // #nosec G115 -- offset is non-negative
		return []models.Block{}, total, nil
	}
	if offset > 0 {
		// #nosec G115 -- offset is non-negative
		start, found, err := a.neighborPageStart(
			ctx, txn, origin, tipID, uint64(offset), forward,
		)
		if err != nil {
			return nil, 0, err
		}
		if !found {
			return []models.Block{}, total, nil
		}
		cursor = start
	}
	ret := make([]models.Block, 0, params.Count)
	for i := 0; i < params.Count; i++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		var (
			index uint64
			err   error
		)
		if forward {
			if cursor == math.MaxUint64 {
				break
			}
			index, _, err = db.BlockPointAtOrAfterIndex(cursor+1, txn)
			if err == nil && index > tipID {
				break
			}
		} else {
			if cursor == 0 {
				break
			}
			index, _, err = db.BlockPointAtOrBeforeIndex(ctx, cursor-1, txn)
		}
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				break
			}
			return nil, 0, err
		}
		cursor = index
		block, err := db.BlockByIndex(index, txn)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, block)
	}
	if forward == (params.Order == PaginationOrderDesc) {
		slices.Reverse(ret)
	}
	return ret, total, nil
}

func (a *NodeAdapter) blockInfos(ctx context.Context, blocks []models.Block) ([]BlockInfo, error) {
	ret := make([]BlockInfo, 0, len(blocks))
	for _, block := range blocks {
		info, err := a.blockInfoFromBlock(ctx, block)
		if err != nil {
			return nil, err
		}
		ret = append(ret, info)
	}
	return ret, nil
}

// BlockNext returns the blocks that follow the identified block.
func (a *NodeAdapter) BlockNext(
	ctx context.Context,
	id string,
	params PaginationParams,
) ([]BlockInfo, int, error) {
	blocks, total, err := a.neighborBlocks(ctx, id, params, true)
	if err != nil {
		return nil, 0, err
	}
	infos, err := a.blockInfos(ctx, blocks)
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

// BlockPrevious returns the blocks that precede the identified block.
func (a *NodeAdapter) BlockPrevious(
	ctx context.Context,
	id string,
	params PaginationParams,
) ([]BlockInfo, int, error) {
	blocks, total, err := a.neighborBlocks(ctx, id, params, false)
	if err != nil {
		return nil, 0, err
	}
	infos, err := a.blockInfos(ctx, blocks)
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

func (a *NodeAdapter) blockAtSlot(
	ctx context.Context,
	slot uint64,
) (models.Block, error) {
	block, err := database.BlockBySlot(ctx, a.ledgerState.Database(), slot)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return models.Block{}, fmt.Errorf(
				"block in slot %d: %w",
				slot,
				ErrBlockNotFound,
			)
		}
		return models.Block{}, err
	}
	return block, nil
}

// BlockBySlot returns the block produced in an absolute slot.
func (a *NodeAdapter) BlockBySlot(
	ctx context.Context,
	slot uint64,
) (BlockInfo, error) {
	block, err := a.blockAtSlot(ctx, slot)
	if err != nil {
		return BlockInfo{}, err
	}
	return a.blockInfoFromBlock(ctx, block)
}

func (a *NodeAdapter) blockAtEpochSlot(
	ctx context.Context,
	epoch uint64,
	epochSlot uint64,
) (models.Block, error) {
	row, err := a.ledgerState.Database().GetEpoch(epoch, nil)
	if err != nil {
		return models.Block{}, fmt.Errorf("get epoch %d: %w", epoch, err)
	}
	notFound := fmt.Errorf(
		"block in slot %d of epoch %d: %w",
		epochSlot,
		epoch,
		ErrBlockNotFound,
	)
	if row == nil || epochSlot >= uint64(row.LengthInSlots) ||
		epochSlot > math.MaxUint64-row.StartSlot {
		return models.Block{}, notFound
	}
	block, err := a.blockAtSlot(ctx, row.StartSlot+epochSlot)
	if err != nil {
		if errors.Is(err, ErrBlockNotFound) {
			return models.Block{}, notFound
		}
		return models.Block{}, err
	}
	return block, nil
}

// BlockByEpochSlot returns the block produced in a slot counted from the
// start of an epoch.
func (a *NodeAdapter) BlockByEpochSlot(
	ctx context.Context,
	epoch uint64,
	epochSlot uint64,
) (BlockInfo, error) {
	block, err := a.blockAtEpochSlot(ctx, epoch, epochSlot)
	if err != nil {
		return BlockInfo{}, err
	}
	return a.blockInfoFromBlock(ctx, block)
}

func (a *NodeAdapter) blockByID(
	ctx context.Context,
	id string,
) (models.Block, error) {
	block, err := a.resolveBlock(ctx, id)
	if err != nil {
		return models.Block{}, err
	}
	return block, nil
}

// BlockTransactionHashes returns the transaction hashes of a block in block
// order.
func (a *NodeAdapter) BlockTransactionHashes(
	ctx context.Context,
	id string,
) ([]string, error) {
	block, err := a.blockByID(ctx, id)
	if err != nil {
		return nil, err
	}
	txs, err := a.ledgerState.BlockTransactions(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("get transactions for block %x: %w", block.Hash, err)
	}
	ret := make([]string, 0, len(txs))
	for _, tx := range txs {
		ret = append(ret, tx.Hash().String())
	}
	return ret, nil
}

func transactionCBORInfos(
	txs []lcommon.Transaction,
) []BlockTransactionCBORInfo {
	ret := make([]BlockTransactionCBORInfo, 0, len(txs))
	for _, tx := range txs {
		ret = append(ret, BlockTransactionCBORInfo{
			TxHash: tx.Hash().String(),
			CBOR:   hex.EncodeToString(tx.Cbor()),
		})
	}
	return ret
}

// BlockTransactionCBOR returns the transactions of a block with their raw
// CBOR, in block order.
func (a *NodeAdapter) BlockTransactionCBOR(
	ctx context.Context,
	id string,
) ([]BlockTransactionCBORInfo, error) {
	block, err := a.blockByID(ctx, id)
	if err != nil {
		return nil, err
	}
	txs, err := a.ledgerState.BlockTransactions(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("get transactions for block %x: %w", block.Hash, err)
	}
	return transactionCBORInfos(txs), nil
}

// LatestBlockTransactionCBOR returns the transactions of the chain tip block
// with their raw CBOR, in block order.
func (a *NodeAdapter) LatestBlockTransactionCBOR(
	ctx context.Context,
) ([]BlockTransactionCBORInfo, error) {
	tip := a.ledgerState.Tip()
	block, err := a.blockByID(
		ctx,
		hex.EncodeToString(tip.Point.Hash),
	)
	if err != nil {
		return nil, err
	}
	txs, err := a.ledgerState.BlockTransactions(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("get transactions for block %x: %w", block.Hash, err)
	}
	return transactionCBORInfos(txs), nil
}

// BlockAddresses returns every address a block's transactions pay to or spend
// from. Output addresses come from the decoded block. Input addresses are
// recovered from the stored UTxOs the transactions consumed, which only the
// metadata index records. A phase-2 invalid transaction touches its
// collateral inputs and collateral return instead of its regular inputs and
// outputs.
func (a *NodeAdapter) BlockAddresses(
	ctx context.Context,
	id string,
) ([]BlockAddressInfo, error) {
	block, err := a.blockByID(ctx, id)
	if err != nil {
		return nil, err
	}
	txs, err := a.ledgerState.BlockTransactions(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("get transactions for block %x: %w", block.Hash, err)
	}
	indexed, err := a.ledgerState.Database().
		GetTransactionsByBlockHash(ctx, block.Hash, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"get transactions for block %x: %w",
			block.Hash,
			err,
		)
	}
	byHash := make(map[string]models.Transaction, len(indexed))
	for _, tx := range indexed {
		byHash[hex.EncodeToString(tx.Hash)] = tx
	}
	// The ledger applies a transaction repeated by a later endorser block
	// once, so its row stays under the earlier certifying block.
	var missing [][]byte
	for _, tx := range txs {
		if _, ok := byHash[tx.Hash().String()]; !ok {
			missing = append(missing, tx.Hash().Bytes())
		}
	}
	if len(missing) > 0 {
		applied, err := a.ledgerState.Database().
			GetTransactionsByHashes(ctx, missing, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"get transactions for block %x: %w",
				block.Hash,
				err,
			)
		}
		for _, tx := range applied {
			byHash[hex.EncodeToString(tx.Hash)] = tx
		}
	}

	outputAddresses := make([][]string, 0, len(txs))
	spent := make([][]models.Utxo, 0, len(txs))
	for _, tx := range txs {
		row, ok := byHash[tx.Hash().String()]
		if !ok {
			return nil, fmt.Errorf(
				"transaction %s of block %x is not indexed",
				tx.Hash().String(),
				block.Hash,
			)
		}
		outputs, inputs := transactionAddressEffects(tx, row)
		outputAddresses = append(outputAddresses, outputs)
		spent = append(spent, inputs)
	}
	inputCbor, err := a.transactionInputCbor(spent...)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve input CBOR for block %x: %w",
			block.Hash,
			err,
		)
	}

	touched := make(map[string][]string)
	touch := func(address string, hash string) {
		if !slices.Contains(touched[address], hash) {
			touched[address] = append(touched[address], hash)
		}
	}
	for i, tx := range txs {
		hash := tx.Hash().String()
		for _, address := range outputAddresses[i] {
			touch(address, hash)
		}
		for _, input := range spent[i] {
			ref, err := utxoRef(input)
			if err != nil {
				return nil, err
			}
			input.Cbor = inputCbor[ref]
			address, err := a.addressFromUtxo(input)
			if err != nil {
				return nil, fmt.Errorf(
					"resolve input address %x#%d: %w",
					input.TxId,
					input.OutputIdx,
					err,
				)
			}
			touch(address, hash)
		}
	}

	addresses := make([]string, 0, len(touched))
	for address := range touched {
		addresses = append(addresses, address)
	}
	slices.Sort(addresses)
	ret := make([]BlockAddressInfo, 0, len(addresses))
	for _, address := range addresses {
		ret = append(ret, BlockAddressInfo{
			Address:  address,
			TxHashes: touched[address],
		})
	}
	return ret, nil
}

// transactionAddressEffects returns the addresses a transaction pays to and
// the stored UTxOs it consumes. Only one of two sets applies: a transaction
// that passed phase-2 validation pays its regular outputs and consumes its
// regular inputs, while one that failed it only forfeits its collateral
// inputs and receives the collateral return, whatever else its body names.
func transactionAddressEffects(
	tx lcommon.Transaction,
	row models.Transaction,
) (outputs []string, spent []models.Utxo) {
	outputs = make([]string, 0, len(tx.Outputs()))
	if tx.IsValid() {
		for _, output := range tx.Outputs() {
			outputs = append(outputs, output.Address().String())
		}
		return outputs, row.Inputs
	}
	if output := tx.CollateralReturn(); output != nil {
		outputs = append(outputs, output.Address().String())
	}
	return outputs, row.Collateral
}

// isByronBlockType reports whether a block type belongs to the Byron era, the
// only era whose epoch boundary blocks share their parent's block number.
func isByronBlockType(blockType uint) bool {
	return blockType == gledger.BlockTypeByronEbb ||
		blockType == gledger.BlockTypeByronMain
}

// blockHeaderByIndex returns the ID, number and type of the block at an index
// entry from retained metadata, without its body: the metadata outlives a
// block's expiry, so locating a page on a pruned chain never needs history
// that has been removed.
func blockHeaderByIndex(
	db *database.Database,
	txn *database.Txn,
	index uint64,
) (models.Block, error) {
	point, err := db.BlockPointByIndex(index, txn)
	if err != nil {
		return models.Block{}, err
	}
	return blockHeaderByPoint(txn, point)
}

// blockHeaderAtOrAfterIndex is blockHeaderByIndex for the first entry at or
// after index.
func blockHeaderAtOrAfterIndex(
	db *database.Database,
	txn *database.Txn,
	index uint64,
) (models.Block, error) {
	_, point, err := db.BlockPointAtOrAfterIndex(index, txn)
	if err != nil {
		return models.Block{}, err
	}
	return blockHeaderByPoint(txn, point)
}

func blockHeaderByPoint(
	txn *database.Txn,
	point ocommon.Point,
) (models.Block, error) {
	metadata, err := database.BlockMetadataByPointLocalTxn(txn, point)
	if err != nil {
		return models.Block{}, err
	}
	return models.Block{
		ID:     metadata.ID,
		Number: metadata.Height,
		Type:   metadata.Type,
		Slot:   point.Slot,
		Hash:   point.Hash,
	}, nil
}

// lastByronBlock returns the last indexed block of the Byron era at or below
// highestID. Block types never return to an earlier era as the chain grows, so
// the Byron blocks form a prefix of the index and a binary search finds its
// end. False means no Byron block exists there.
func lastByronBlock(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	highestID uint64,
) (models.Block, bool, error) {
	var (
		last  models.Block
		found bool
	)
	lo, hi := uint64(database.BlockInitialIndex), highestID
	for lo <= hi {
		if err := ctx.Err(); err != nil {
			return models.Block{}, false, err
		}
		mid := lo + (hi-lo)/2
		block, err := blockHeaderAtOrAfterIndex(db, txn, mid)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return models.Block{}, false, err
		}
		if err != nil || block.ID > hi || !isByronBlockType(block.Type) {
			if mid == 0 {
				break
			}
			hi = mid - 1
			continue
		}
		last, found = block, true
		if block.ID == math.MaxUint64 {
			break
		}
		lo = block.ID + 1
	}
	return last, found, nil
}

// neighborPageStart returns the index entry that precedes the first block of a
// page that begins offset entries beyond origin, so a walk from it yields that
// page. False means no block lies there. forward selects the blocks that
// follow origin; otherwise the ones that precede it. tipID bounds the forward
// search to the applied chain.
//
// Only Byron epoch boundary blocks share a block number with a neighbor, so
// past the Byron era an entry is exactly one block number away from the next
// and the page start is found by block number. Within the Byron era the
// entries are counted one by one, which costs no more than the Byron entries
// the page skips.
func (a *NodeAdapter) neighborPageStart(
	ctx context.Context,
	txn *database.Txn,
	origin models.Block,
	tipID uint64,
	offset uint64,
	forward bool,
) (uint64, bool, error) {
	if forward {
		return a.forwardPageStart(ctx, txn, origin, tipID, offset)
	}
	return a.backwardPageStart(ctx, txn, origin, offset)
}

func (a *NodeAdapter) forwardPageStart(
	ctx context.Context,
	txn *database.Txn,
	origin models.Block,
	tipID uint64,
	offset uint64,
) (uint64, bool, error) {
	db := a.ledgerState.Database()
	cursor := origin
	remaining := offset
	if isByronBlockType(origin.Type) {
		end, _, err := lastByronBlock(ctx, db, txn, tipID)
		if err != nil {
			return 0, false, fmt.Errorf("find end of Byron era: %w", err)
		}
		cursorID := origin.ID
		for remaining > 0 && cursorID < end.ID {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
			next, _, err := db.BlockPointAtOrAfterIndex(cursorID+1, txn)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					return 0, false, nil
				}
				return 0, false, err
			}
			if next > tipID {
				return 0, false, nil
			}
			cursorID = next
			remaining--
		}
		if remaining == 0 {
			return cursorID, true, nil
		}
		if cursorID != origin.ID {
			cursor, err = blockHeaderByIndex(db, txn, cursorID)
			if err != nil {
				return 0, false, err
			}
		}
	}
	if remaining > math.MaxUint64-cursor.Number-1 {
		return 0, false, nil
	}
	block, err := database.BlockNumberBoundaryTxn(
		ctx, txn, cursor.Number+1+remaining,
		database.BlockNumberBound{HighestID: tipID, Resolved: true}, true,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("resolve page start: %w", err)
	}
	if block.ID <= cursor.ID {
		return 0, false, nil
	}
	return block.ID - 1, true, nil
}

func (a *NodeAdapter) backwardPageStart(
	ctx context.Context,
	txn *database.Txn,
	origin models.Block,
	offset uint64,
) (uint64, bool, error) {
	db := a.ledgerState.Database()
	if origin.ID <= database.BlockInitialIndex {
		return 0, false, nil
	}
	cursor := origin
	remaining := offset
	if !isByronBlockType(origin.Type) {
		end, hasByron, err := lastByronBlock(ctx, db, txn, origin.ID-1)
		if err != nil {
			return 0, false, fmt.Errorf("find end of Byron era: %w", err)
		}
		var first models.Block
		if hasByron {
			firstID, _, err := db.BlockPointAtOrAfterIndex(end.ID+1, txn)
			if err != nil {
				return 0, false, err
			}
			first, err = blockHeaderByIndex(db, txn, firstID)
			if err != nil {
				return 0, false, err
			}
		}
		// Every block from the first post-Byron one up to origin has its
		// own number, so that many entries are skipped by number.
		numeric := origin.Number
		if hasByron {
			numeric = origin.Number - first.Number
		}
		if remaining < numeric || !hasByron {
			if origin.Number <= offset {
				return 0, false, nil
			}
			block, err := database.BlockNumberBoundaryTxn(
				ctx, txn, origin.Number-1-offset,
				database.BlockNumberBound{
					HighestID: origin.ID - 1, Resolved: true,
				}, false,
			)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					return 0, false, nil
				}
				return 0, false, fmt.Errorf("resolve page start: %w", err)
			}
			return block.ID + 1, true, nil
		}
		cursor = first
		remaining -= numeric
	}
	cursorID := cursor.ID
	for ; remaining > 0; remaining-- {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if cursorID <= database.BlockInitialIndex {
			return 0, false, nil
		}
		prev, _, err := db.BlockPointAtOrBeforeIndex(ctx, cursorID-1, txn)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return 0, false, nil
			}
			return 0, false, err
		}
		cursorID = prev
	}
	return cursorID, true, nil
}
