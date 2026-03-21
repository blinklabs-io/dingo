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
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
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
	block, decodedBlock, err := a.latestBlockData()
	if err != nil {
		return BlockInfo{}, err
	}
	epoch, err := a.ledgerState.SlotToEpoch(tip.Point.Slot)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"get epoch for tip slot %d: %w",
			tip.Point.Slot,
			err,
		)
	}
	slotTime, err := a.ledgerState.SlotToTime(tip.Point.Slot)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"get time for tip slot %d: %w",
			tip.Point.Slot,
			err,
		)
	}
	return BlockInfo{
		Hash: hex.EncodeToString(
			tip.Point.Hash,
		),
		Slot:      tip.Point.Slot,
		Height:    tip.BlockNumber,
		Epoch:     epoch.EpochId,
		EpochSlot: tip.Point.Slot - epoch.StartSlot,
		Time:      slotTime.Unix(),
		Size:      uint64(len(block.Cbor)),
		TxCount:   len(decodedBlock.Transactions()),
		SlotLeader: blockIssuer(
			decodedBlock.IssuerVkey(),
		),
		PreviousBlock: blockHashString(
			block.PrevHash,
		),
		Confirmations: 0,
	}, nil
}

// LatestBlockTxHashes returns transaction hashes from the
// latest block.
func (a *NodeAdapter) LatestBlockTxHashes() (
	[]string, error,
) {
	_, decodedBlock, err := a.latestBlockData()
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(decodedBlock.Transactions()))
	for _, tx := range decodedBlock.Transactions() {
		ret = append(ret, tx.Hash().String())
	}
	return ret, nil
}

// CurrentEpoch returns information about the current
// epoch.
func (a *NodeAdapter) CurrentEpoch() (
	EpochInfo, error,
) {
	tip := a.ledgerState.Tip()
	epoch := a.ledgerState.CurrentEpoch()
	tipEpoch, err := a.ledgerState.SlotToEpoch(tip.Point.Slot)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"get epoch for tip slot %d: %w",
			tip.Point.Slot,
			err,
		)
	}
	startTime, err := a.ledgerState.SlotToTime(tipEpoch.StartSlot)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"get epoch start time for slot %d: %w",
			tipEpoch.StartSlot,
			err,
		)
	}
	endSlot := tipEpoch.StartSlot + uint64(tipEpoch.LengthInSlots)
	endTime, err := a.ledgerState.SlotToTime(endSlot)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"get epoch end time for slot %d: %w",
			endSlot,
			err,
		)
	}
	blockCount, firstBlockSlot, lastBlockSlot, err := a.ledgerState.CountBlocksInSlotRange(
		tipEpoch.StartSlot,
		tip.Point.Slot,
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"count epoch blocks for slots %d-%d: %w",
			tipEpoch.StartSlot,
			tip.Point.Slot,
			err,
		)
	}
	txCount, err := a.ledgerState.CountTransactionsInSlotRange(
		tipEpoch.StartSlot,
		tip.Point.Slot,
	)
	if err != nil {
		return EpochInfo{}, fmt.Errorf(
			"count epoch transactions for slots %d-%d: %w",
			tipEpoch.StartSlot,
			tip.Point.Slot,
			err,
		)
	}
	firstBlockTime := int64(0)
	lastBlockTime := int64(0)
	if blockCount > 0 {
		firstTime, err := a.ledgerState.SlotToTime(firstBlockSlot)
		if err != nil {
			return EpochInfo{}, fmt.Errorf(
				"get first epoch block time for slot %d: %w",
				firstBlockSlot,
				err,
			)
		}
		lastTime, err := a.ledgerState.SlotToTime(lastBlockSlot)
		if err != nil {
			return EpochInfo{}, fmt.Errorf(
				"get last epoch block time for slot %d: %w",
				lastBlockSlot,
				err,
			)
		}
		firstBlockTime = firstTime.Unix()
		lastBlockTime = lastTime.Unix()
	}
	return EpochInfo{
		Epoch:          epoch,
		StartTime:      startTime.Unix(),
		EndTime:        endTime.Unix(),
		FirstBlockTime: firstBlockTime,
		LastBlockTime:  lastBlockTime,
		BlockCount:     blockCount,
		TxCount:        txCount,
	}, nil
}

// CurrentProtocolParams returns the current protocol
// parameters.
func (a *NodeAdapter) CurrentProtocolParams() (
	ProtocolParamsInfo, error,
) {
	pparams := a.ledgerState.GetCurrentPParams()
	utxorpcParams, err := pparams.Utxorpc()
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"convert current protocol parameters: %w",
			err,
		)
	}
	return ProtocolParamsInfo{
		Epoch:               a.ledgerState.CurrentEpoch(),
		MinFeeA:             uint64ToInt(utxorpcBigIntToUint64(utxorpcParams.GetMinFeeCoefficient())),
		MinFeeB:             uint64ToInt(utxorpcBigIntToUint64(utxorpcParams.GetMinFeeConstant())),
		MaxBlockSize:        uint64ToInt(utxorpcParams.GetMaxBlockBodySize()),
		MaxTxSize:           uint64ToInt(utxorpcParams.GetMaxTxSize()),
		MaxBlockHeaderSize:  uint64ToInt(utxorpcParams.GetMaxBlockHeaderSize()),
		KeyDeposit:          utxorpcBigIntToString(utxorpcParams.GetStakeKeyDeposit()),
		PoolDeposit:         utxorpcBigIntToString(utxorpcParams.GetPoolDeposit()),
		EMax:                uint64ToInt(utxorpcParams.GetPoolRetirementEpochBound()),
		NOpt:                uint64ToInt(utxorpcParams.GetDesiredNumberOfPools()),
		A0:                  rationalToFloat64(utxorpcParams.GetPoolInfluence()),
		Rho:                 rationalToFloat64(utxorpcParams.GetMonetaryExpansion()),
		Tau:                 rationalToFloat64(utxorpcParams.GetTreasuryExpansion()),
		ProtocolMajorVer:    int(utxorpcParams.GetProtocolVersion().GetMajor()),
		ProtocolMinorVer:    int(utxorpcParams.GetProtocolVersion().GetMinor()),
		MinPoolCost:         utxorpcBigIntToString(utxorpcParams.GetMinPoolCost()),
		CoinsPerUtxoSize:    utxorpcBigIntToString(utxorpcParams.GetCoinsPerUtxoByte()),
		PriceMem:            rationalToFloat64(utxorpcParams.GetPrices().GetMemory()),
		PriceStep:           rationalToFloat64(utxorpcParams.GetPrices().GetSteps()),
		MaxTxExMem:          exUnitsMemString(utxorpcParams.GetMaxExecutionUnitsPerTransaction()),
		MaxTxExSteps:        exUnitsStepsString(utxorpcParams.GetMaxExecutionUnitsPerTransaction()),
		MaxBlockExMem:       exUnitsMemString(utxorpcParams.GetMaxExecutionUnitsPerBlock()),
		MaxBlockExSteps:     exUnitsStepsString(utxorpcParams.GetMaxExecutionUnitsPerBlock()),
		MaxValSize:          strconv.FormatUint(utxorpcParams.GetMaxValueSize(), 10),
		CollateralPercent:   uint64ToInt(utxorpcParams.GetCollateralPercentage()),
		MaxCollateralInputs: uint64ToInt(utxorpcParams.GetMaxCollateralInputs()),
	}, nil
}

func (a *NodeAdapter) latestBlockData() (
	models.Block,
	lcommon.Block,
	error,
) {
	tip := a.ledgerState.Tip()
	block, err := a.ledgerState.BlockByHash(tip.Point.Hash)
	if err != nil {
		return models.Block{}, nil, fmt.Errorf(
			"get tip block by hash %x: %w",
			tip.Point.Hash,
			err,
		)
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		return models.Block{}, nil, fmt.Errorf(
			"decode tip block %x: %w",
			tip.Point.Hash,
			err,
		)
	}
	return block, decodedBlock, nil
}

func blockIssuer(issuer lcommon.IssuerVkey) string {
	if bytes.Equal(issuer[:], make([]byte, len(issuer))) {
		return ""
	}
	return issuer.PoolId()
}

func blockHashString(hash []byte) string {
	if len(hash) == 0 || isZeroHash(hash) {
		return ""
	}
	return hex.EncodeToString(hash)
}

func isZeroHash(hash []byte) bool {
	return bytes.Equal(hash, make([]byte, len(hash)))
}

func rationalToFloat64(r *cardano.RationalNumber) float64 {
	if r == nil || r.GetDenominator() == 0 {
		return 0
	}
	return float64(r.GetNumerator()) / float64(r.GetDenominator())
}

func utxorpcBigIntToString(v *cardano.BigInt) string {
	if v == nil {
		return "0"
	}
	switch x := v.GetBigInt().(type) {
	case *cardano.BigInt_Int:
		return strconv.FormatInt(x.Int, 10)
	case *cardano.BigInt_BigUInt:
		if len(x.BigUInt) == 0 {
			return "0"
		}
		return new(big.Int).SetBytes(x.BigUInt).String()
	case *cardano.BigInt_BigNInt:
		if len(x.BigNInt) == 0 {
			return "0"
		}
		return new(big.Int).Neg(new(big.Int).SetBytes(x.BigNInt)).String()
	default:
		return "0"
	}
}

func utxorpcBigIntToUint64(v *cardano.BigInt) uint64 {
	if v == nil {
		return 0
	}
	switch x := v.GetBigInt().(type) {
	case *cardano.BigInt_Int:
		if x.Int < 0 {
			return 0
		}
		return uint64(x.Int)
	case *cardano.BigInt_BigUInt:
		return new(big.Int).SetBytes(x.BigUInt).Uint64()
	default:
		return 0
	}
}

func uint64ToInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

func exUnitsMemString(exUnits *cardano.ExUnits) string {
	if exUnits == nil {
		return "0"
	}
	return strconv.FormatUint(exUnits.GetMemory(), 10)
}

func exUnitsStepsString(exUnits *cardano.ExUnits) string {
	if exUnits == nil {
		return "0"
	}
	return strconv.FormatUint(exUnits.GetSteps(), 10)
}
