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

	"github.com/blinklabs-io/dingo/ledger"
)

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
