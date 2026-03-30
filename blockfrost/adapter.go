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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"errors"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var ErrInvalidAddress = errors.New("invalid address")

// NodeAdapter wraps a real dingo Node's LedgerState to
// implement the BlockfrostNode interface.
type NodeAdapter struct {
	ledgerState *ledger.LedgerState
}

// NewNodeAdapter creates a NodeAdapter that queries the
// given LedgerState for blockchain data. Panics if ls is
// nil.
func NewNodeAdapter(
	ls *ledger.LedgerState,
) *NodeAdapter {
	if ls == nil {
		panic("NewNodeAdapter: LedgerState must not be nil")
	}
	return &NodeAdapter{ledgerState: ls}
}

// ChainTip returns the current chain tip from the ledger
// state.
func (a *NodeAdapter) ChainTip() (
	ChainTipInfo, error,
) {
	tip := a.ledgerState.Tip()
	return ChainTipInfo{
		BlockHash: hex.EncodeToString(
			tip.Point.Hash,
		),
		Slot:        tip.Point.Slot,
		BlockNumber: tip.BlockNumber,
	}, nil
}

// LatestBlock returns information about the latest block.
func (a *NodeAdapter) LatestBlock() (
	BlockInfo, error,
) {
	tip := a.ledgerState.Tip()
	// TODO: Retrieve full block details (size, tx count,
	// slot leader, previous block, epoch, epoch slot)
	// from the database once block query methods are
	// available.
	return BlockInfo{
		Hash: hex.EncodeToString(
			tip.Point.Hash,
		),
		Slot:          tip.Point.Slot,
		Height:        tip.BlockNumber,
		Epoch:         0,
		EpochSlot:     0,
		Time:          0,
		Size:          0,
		TxCount:       0,
		SlotLeader:    "",
		PreviousBlock: "",
		Confirmations: 0,
	}, nil
}

// LatestBlockTxHashes returns transaction hashes from the
// latest block.
func (a *NodeAdapter) LatestBlockTxHashes() (
	[]string, error,
) {
	// TODO: Query the database for transaction hashes
	// in the latest block once the appropriate query
	// methods are available.
	return []string{}, nil
}

// CurrentEpoch returns information about the current
// epoch.
func (a *NodeAdapter) CurrentEpoch() (
	EpochInfo, error,
) {
	// TODO: Retrieve full epoch details (epoch number,
	// start/end time, block count, tx count) from the
	// database once epoch query methods are available.
	return EpochInfo{
		Epoch:          0,
		StartTime:      0,
		EndTime:        0,
		FirstBlockTime: 0,
		LastBlockTime:  0,
		BlockCount:     0,
		TxCount:        0,
	}, nil
}

// CurrentProtocolParams returns the current protocol
// parameters.
func (a *NodeAdapter) CurrentProtocolParams() (
	ProtocolParamsInfo, error,
) {
	// TODO: Map real protocol parameter fields from
	// GetCurrentPParams() once the gouroboros
	// ProtocolParameters interface exposes the necessary
	// getters. For now, return placeholder values.
	return ProtocolParamsInfo{
		Epoch:               0,
		MinFeeA:             0,
		MinFeeB:             0,
		MaxBlockSize:        0,
		MaxTxSize:           0,
		MaxBlockHeaderSize:  0,
		KeyDeposit:          "0",
		PoolDeposit:         "0",
		EMax:                0,
		NOpt:                0,
		A0:                  0,
		Rho:                 0,
		Tau:                 0,
		ProtocolMajorVer:    0,
		ProtocolMinorVer:    0,
		MinPoolCost:         "0",
		CoinsPerUtxoSize:    "0",
		PriceMem:            0,
		PriceStep:           0,
		MaxTxExMem:          "0",
		MaxTxExSteps:        "0",
		MaxBlockExMem:       "0",
		MaxBlockExSteps:     "0",
		MaxValSize:          "0",
		CollateralPercent:   0,
		MaxCollateralInputs: 0,
	}, nil
}

// AddressUTXOs returns paginated current UTxOs for the
// requested address.
func (a *NodeAdapter) AddressUTXOs(
	address string,
	params PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	addr, err := lcommon.NewAddress(address)
	if err != nil {
		return nil, 0, ErrInvalidAddress
	}

	utxos, err := a.ledgerState.UtxosByAddressWithOrdering(addr)
	if err != nil {
		return nil, 0, err
	}
	total := len(utxos)
	if params.Order == PaginationOrderDesc {
		for left, right := 0, len(utxos)-1; left < right; left, right = left+1, right-1 {
			utxos[left], utxos[right] = utxos[right], utxos[left]
		}
	}

	paged := paginateUtxos(utxos, params)
	txBlockHashes, err := a.addressUtxoBlockHashes(
		addr,
		paged,
	)
	if err != nil {
		return nil, 0, err
	}

	ret := make([]AddressUTXOInfo, 0, len(paged))
	for _, utxo := range paged {
		txKey := hex.EncodeToString(utxo.TxId)
		ret = append(ret, AddressUTXOInfo{
			Address:             address,
			TxHash:              txKey,
			OutputIndex:         utxo.OutputIdx,
			Amount:              addressAmountsFromUtxo(utxo.Utxo),
			Block:               txBlockHashes[txKey],
			DataHash:            optionalHexString(utxo.DatumHash),
			InlineDatum:         nil,
			ReferenceScriptHash: nil,
		})
	}
	return ret, total, nil
}

// AddressTransactions returns paginated transaction
// history for the requested address.
func (a *NodeAdapter) AddressTransactions(
	address string,
	params PaginationParams,
) ([]AddressTransactionInfo, int, error) {
	addr, err := lcommon.NewAddress(address)
	if err != nil {
		return nil, 0, ErrInvalidAddress
	}

	txs, err := a.ledgerState.GetTransactionsByAddress(
		addr,
		0,
		0,
	)
	if err != nil {
		return nil, 0, err
	}
	total := len(txs)
	if params.Order == PaginationOrderAsc {
		for left, right := 0, len(txs)-1; left < right; left, right = left+1, right-1 {
			txs[left], txs[right] = txs[right], txs[left]
		}
	}

	paged := paginateTransactions(txs, params)
	ret := make([]AddressTransactionInfo, 0, len(paged))
	for _, tx := range paged {
		blockHeight, blockTime, err := a.transactionBlockInfo(tx)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, AddressTransactionInfo{
			TxHash:      hex.EncodeToString(tx.Hash),
			TxIndex:     tx.BlockIndex,
			BlockHeight: blockHeight,
			BlockTime:   blockTime,
		})
	}
	return ret, total, nil
}

func (a *NodeAdapter) addressUtxoBlockHashes(
	addr lcommon.Address,
	utxos []models.UtxoWithOrdering,
) (map[string]string, error) {
	ret := make(map[string]string, len(utxos))
	if len(utxos) == 0 {
		return ret, nil
	}

	txs, err := a.ledgerState.GetTransactionsByAddress(
		addr,
		0,
		0,
	)
	if err != nil {
		return nil, err
	}
	for _, tx := range txs {
		ret[hex.EncodeToString(tx.Hash)] = hex.EncodeToString(tx.BlockHash)
	}
	return ret, nil
}

func (a *NodeAdapter) transactionBlockInfo(
	tx models.Transaction,
) (uint64, int64, error) {
	block, err := a.ledgerState.BlockByHash(tx.BlockHash)
	if err != nil {
		return 0, 0, err
	}
	blockTime, err := a.ledgerState.SlotToTime(tx.Slot)
	if err != nil {
		return 0, 0, err
	}
	return block.Number, blockTime.Unix(), nil
}

func addressAmountsFromUtxo(
	utxo models.Utxo,
) []AddressAmountInfo {
	ret := make([]AddressAmountInfo, 0, len(utxo.Assets)+1)
	ret = append(ret, AddressAmountInfo{
		Unit:     "lovelace",
		Quantity: strconv.FormatUint(uint64(utxo.Amount), 10),
	})
	for _, asset := range utxo.Assets {
		ret = append(ret, AddressAmountInfo{
			Unit: hex.EncodeToString(asset.PolicyId) +
				hex.EncodeToString(asset.Name),
			Quantity: strconv.FormatUint(
				uint64(asset.Amount),
				10,
			),
		})
	}
	return ret
}

func optionalHexString(data []byte) *string {
	if len(data) == 0 {
		return nil
	}
	ret := hex.EncodeToString(data)
	return &ret
}

func paginateUtxos(
	utxos []models.UtxoWithOrdering,
	params PaginationParams,
) []models.UtxoWithOrdering {
	start, end := paginationRange(len(utxos), params)
	if start >= end {
		return []models.UtxoWithOrdering{}
	}
	return utxos[start:end]
}

func paginateTransactions(
	txs []models.Transaction,
	params PaginationParams,
) []models.Transaction {
	start, end := paginationRange(len(txs), params)
	if start >= end {
		return []models.Transaction{}
	}
	return txs[start:end]
}

func paginationRange(
	total int,
	params PaginationParams,
) (int, int) {
	start := (params.Page - 1) * params.Count
	if start >= total {
		return total, total
	}
	end := start + params.Count
	if end > total {
		end = total
	}
	return start, end
}
