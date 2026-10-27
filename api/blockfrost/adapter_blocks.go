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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// neighborBlocks returns the page of blocks adjacent to the identified block
// and the number of block heights on that side of it: the ones that follow it
// up to the tip when forward is set, else the ones that precede it. Page one
// holds the nearest blocks; within a page heights ascend, as Blockfrost lists
// them, and a descending order reverses the page. A gap in the stored chain
// ends the listing.
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
	var available uint64
	if forward {
		tip, err := a.ledgerState.Database().GetTip(nil)
		if err != nil {
			return nil, 0, fmt.Errorf("get chain tip: %w", err)
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
	// The origin is skipped as well as the pages before the requested one.
	// offset is a non-negative int, so the sum cannot overflow a uint64.
	skip := uint64(offset) + 1 // #nosec G115 -- offset is non-negative
	ret := make([]models.Block, 0, params.Count)
	for i := range params.Count {
		distance := skip + uint64(i) // #nosec G115 -- i is non-negative
		var height uint64
		if forward {
			if origin.Number > math.MaxUint64-database.BlockInitialIndex-distance {
				break
			}
			height = origin.Number + distance
		} else {
			if distance > origin.Number {
				break
			}
			height = origin.Number - distance
		}
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		block, err := a.ledgerState.Database().BlockByIndex(
			height+database.BlockInitialIndex,
			nil,
		)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				break
			}
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
