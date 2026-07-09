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
	"cmp"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gscript "github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

var (
	ErrInvalidAddress      = errors.New("invalid address")
	ErrInvalidBlockID      = errors.New("invalid block id")
	ErrBlockNotFound       = errors.New("block not found")
	ErrEpochNotFound       = errors.New("epoch not found")
	ErrAssetNotFound       = errors.New("asset not found")
	ErrDRepNotFound        = errors.New("drep not found")
	ErrInvalidTransaction  = errors.New("invalid transaction")
	ErrMempoolUnavailable  = errors.New("mempool unavailable")
	ErrMempoolFull         = errors.New("mempool full")
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrInvalidStakeAddress = errors.New("invalid stake address")
)

// TransactionSubmitter accepts raw transaction CBOR for mempool admission.
type TransactionSubmitter interface {
	AddTransaction(txType uint, txBytes []byte) error
}

// NodeAdapter wraps a real dingo Node's LedgerState to
// implement the BlockfrostNode interface.
type NodeAdapter struct {
	ledgerState *ledger.LedgerState
	submitter   TransactionSubmitter
}

// NewNodeAdapter creates a NodeAdapter that queries the
// given LedgerState for blockchain data.
func NewNodeAdapter(
	ls *ledger.LedgerState,
	submitter TransactionSubmitter,
) (*NodeAdapter, error) {
	if ls == nil {
		return nil, errors.New(
			"new node adapter: ledger state must not be nil",
		)
	}
	return &NodeAdapter{
		ledgerState: ls,
		submitter:   submitter,
	}, nil
}

func uintToUint8(v uint, fieldName string) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf(
			"%s out of range for uint8: %d",
			fieldName,
			v,
		)
	}
	return uint8(v), nil
}

func uint64ToInt64(v uint64, fieldName string) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf(
			"%s out of range for int64: %d",
			fieldName,
			v,
		)
	}
	return int64(v), nil
}

func uint64ToInt32(v uint64, fieldName string) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf(
			"%s out of range for int32: %d",
			fieldName,
			v,
		)
	}
	return int32(v), nil
}

func delegationActivationEpoch(
	ls *ledger.LedgerState,
	slot uint64,
) (int32, error) {
	epoch, err := ls.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	// Stake delegation becomes active after snapshot
	// rotation, two epochs after the certificate epoch.
	return uint64ToInt32(
		epoch.EpochId+2,
		"delegation active epoch",
	)
}

// accountHistoryBlockInfo resolves the tx_slot, block_time (Unix seconds), and
// block_height fields shared by the account delegation and registration
// history endpoints. The block height comes from the block store (keyed by
// block hash, since the metadata SQL schema does not hold block numbers);
// blockNumbers caches lookups across the rows of a single response.
func (a *NodeAdapter) accountHistoryBlockInfo(
	txSlot uint64,
	blockHash []byte,
	blockNumbers map[string]uint64,
) (slot, blockTime, height int64, err error) {
	slot, err = uint64ToInt64(txSlot, "account history tx slot")
	if err != nil {
		return 0, 0, 0, err
	}

	blockHashKey := hex.EncodeToString(blockHash)
	blockHeight, ok := blockNumbers[blockHashKey]
	if !ok {
		block, err := a.ledgerState.BlockByHash(blockHash)
		if err != nil {
			return 0, 0, 0, fmt.Errorf(
				"get block %x for account history: %w",
				blockHash,
				err,
			)
		}
		blockHeight = block.Number
		blockNumbers[blockHashKey] = blockHeight
	}
	height, err = uint64ToInt64(blockHeight, "account history block height")
	if err != nil {
		return 0, 0, 0, err
	}

	slotTime, err := a.ledgerState.SlotToTime(txSlot)
	if err != nil {
		return 0, 0, 0, fmt.Errorf(
			"get block time for slot %d: %w",
			txSlot,
			err,
		)
	}
	return slot, slotTime.Unix(), height, nil
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
	block, err := a.ledgerState.BlockByHash(tip.Point.Hash)
	if err != nil {
		return BlockInfo{}, err
	}
	return a.blockInfoFromBlock(block)
}

// BlockByHashOrNumber returns block information by hash or height.
func (a *NodeAdapter) BlockByHashOrNumber(
	id string,
) (BlockInfo, error) {
	var block models.Block
	if len(id) == 64 {
		hash, err := hex.DecodeString(id)
		if err != nil {
			return BlockInfo{}, fmt.Errorf(
				"decode block hash %q: %w",
				id,
				ErrInvalidBlockID,
			)
		}
		var getErr error
		block, getErr = a.ledgerState.BlockByHash(hash)
		if getErr != nil {
			if errors.Is(getErr, models.ErrBlockNotFound) {
				return BlockInfo{}, fmt.Errorf(
					"block %s: %w",
					id,
					ErrBlockNotFound,
				)
			}
			return BlockInfo{}, getErr
		}
		return a.blockInfoFromBlock(block)
	}

	height, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"parse block height %q: %w",
			id,
			ErrInvalidBlockID,
		)
	}
	if height > math.MaxUint64-database.BlockInitialIndex {
		return BlockInfo{}, fmt.Errorf(
			"block height %q overflows internal index: %w",
			id,
			ErrInvalidBlockID,
		)
	}
	// Blockfrost accepts Cardano block height here. Dingo's blob block index
	// is 1-based while Cardano block heights are 0-based, so translate height
	// to the internal index before lookup.
	block, err = a.ledgerState.Database().BlockByIndex(
		height+database.BlockInitialIndex,
		nil,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return BlockInfo{}, fmt.Errorf(
				"block %s: %w",
				id,
				ErrBlockNotFound,
			)
		}
		return BlockInfo{}, err
	}
	return a.blockInfoFromBlock(block)
}

func (a *NodeAdapter) blockInfoFromBlock(
	block models.Block,
) (BlockInfo, error) {
	decodedBlock, err := block.Decode()
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"decode block %x: %w",
			block.Hash,
			err,
		)
	}
	epoch, err := a.ledgerState.SlotToEpoch(block.Slot)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"get epoch for block slot %d: %w",
			block.Slot,
			err,
		)
	}
	slotTime, err := a.ledgerState.SlotToTime(block.Slot)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"get time for block slot %d: %w",
			block.Slot,
			err,
		)
	}
	tip := a.ledgerState.Tip()
	confirmations := uint64(0)
	if tip.BlockNumber >= block.Number {
		confirmations = tip.BlockNumber - block.Number
	}

	output, fees, err := a.blockOutputAndFees(block.Hash)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"aggregate output and fees for block %x: %w",
			block.Hash,
			err,
		)
	}

	blockVRF, opCert, opCertCounter := praosHeaderFields(
		decodedBlock.Header(),
	)

	nextBlock, err := a.nextBlockHash(block.Number, tip.BlockNumber)
	if err != nil {
		return BlockInfo{}, fmt.Errorf(
			"resolve next block for block %x: %w",
			block.Hash,
			err,
		)
	}

	return BlockInfo{
		Hash:      hex.EncodeToString(block.Hash),
		Slot:      block.Slot,
		Height:    block.Number,
		Epoch:     epoch.EpochId,
		EpochSlot: block.Slot - epoch.StartSlot,
		Time:      slotTime.Unix(),
		Size:      uint64(len(block.Cbor)),
		TxCount:   len(decodedBlock.Transactions()),
		SlotLeader: blockIssuer(
			decodedBlock.IssuerVkey(),
		),
		PreviousBlock: blockHashString(
			block.PrevHash,
		),
		Confirmations: confirmations,
		Output:        output,
		Fees:          fees,
		BlockVRF:      blockVRF,
		OPCert:        opCert,
		OPCertCounter: opCertCounter,
		NextBlock:     nextBlock,
	}, nil
}

// blockOutputAndFees aggregates the total lovelace output and fees across all
// transactions in a block. Fees are summed from each transaction's recorded
// fee. For phase-2 invalid transactions, the collateral return (rather than the
// discarded outputs) counts toward the block output, matching the
// per-transaction endpoint.
func (a *NodeAdapter) blockOutputAndFees(
	blockHash []byte,
) (output string, fees string, err error) {
	txs, err := a.ledgerState.GetTransactionsByBlockHash(blockHash)
	if err != nil {
		return "", "", fmt.Errorf(
			"get transactions for block %x: %w",
			blockHash,
			err,
		)
	}
	var totalOutput, totalFees uint64
	for _, tx := range txs {
		totalFees += uint64(tx.Fee)
		outputs := tx.Outputs
		if !tx.Valid && tx.CollateralReturn != nil {
			outputs = []models.Utxo{*tx.CollateralReturn}
		}
		for _, out := range outputs {
			totalOutput += uint64(out.Amount)
		}
	}
	return strconv.FormatUint(totalOutput, 10),
		strconv.FormatUint(totalFees, 10),
		nil
}

// nextBlockHash returns the hash of the block that directly follows the block
// at the given height, or nil when the block is the chain tip (no successor).
func (a *NodeAdapter) nextBlockHash(
	height uint64,
	tipHeight uint64,
) (*string, error) {
	if height >= tipHeight {
		return nil, nil
	}
	// Dingo's blob block index is 1-based (BlockInitialIndex) while Cardano
	// block heights are 0-based, so the successor's internal index is
	// height + BlockInitialIndex + 1.
	if height > math.MaxUint64-database.BlockInitialIndex-1 {
		return nil, nil
	}
	next, err := a.ledgerState.Database().BlockByIndex(
		height+database.BlockInitialIndex+1,
		nil,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil
		}
		return nil, err
	}
	hash := hex.EncodeToString(next.Hash)
	return &hash, nil
}

// praosHeaderFields extracts the Blockfrost block_vrf, op_cert, and
// op_cert_counter values from a Praos/TPraos block header. Byron and unknown
// headers carry none of these, so all three returns are nil.
func praosHeaderFields(
	header gledger.BlockHeader,
) (blockVRF *string, opCert *string, opCertCounter *string) {
	var vrfKey, opCertHotVkey []byte
	var counter uint64
	switch h := header.(type) {
	case *shelley.ShelleyBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCertHotVkey
		counter = uint64(h.Body.OpCertSequenceNumber)
	case *allegra.AllegraBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCertHotVkey
		counter = uint64(h.Body.OpCertSequenceNumber)
	case *mary.MaryBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCertHotVkey
		counter = uint64(h.Body.OpCertSequenceNumber)
	case *alonzo.AlonzoBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCertHotVkey
		counter = uint64(h.Body.OpCertSequenceNumber)
	case *babbage.BabbageBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCert.HotVkey
		counter = uint64(h.Body.OpCert.SequenceNumber)
	case *conway.ConwayBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCert.HotVkey
		counter = uint64(h.Body.OpCert.SequenceNumber)
	case *dijkstra.DijkstraBlockHeader:
		vrfKey = h.Body.VrfKey
		opCertHotVkey = h.Body.OpCert.HotVkey
		counter = uint64(h.Body.OpCert.SequenceNumber)
	default:
		// Byron and unknown headers have no Praos/TPraos header fields.
		return nil, nil, nil
	}
	if len(vrfKey) > 0 {
		if encoded, err := bech32EncodeData("vrf_vk", vrfKey); err == nil {
			blockVRF = &encoded
		}
	}
	if len(opCertHotVkey) > 0 {
		hexCert := hex.EncodeToString(opCertHotVkey)
		opCert = &hexCert
	}
	counterStr := strconv.FormatUint(counter, 10)
	opCertCounter = &counterStr
	return blockVRF, opCert, opCertCounter
}

// bech32EncodeData bech32-encodes raw 8-bit data under the given human-readable
// prefix, converting to the 5-bit groups bech32 requires.
func bech32EncodeData(hrp string, data []byte) (string, error) {
	conv, err := bech32.ConvertBits(data, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("convert bits: %w", err)
	}
	encoded, err := bech32.Encode(hrp, conv)
	if err != nil {
		return "", fmt.Errorf("bech32 encode: %w", err)
	}
	return encoded, nil
}

// LatestBlockTxHashes returns transaction hashes from the
// latest block.
func (a *NodeAdapter) LatestBlockTxHashes() (
	[]string, error,
) {
	tip := a.ledgerState.Tip()
	_, decodedBlock, err := a.latestBlockData(
		tip.Point.Hash,
	)
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
		Epoch:          tipEpoch.EpochId,
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
	if pparams == nil {
		return ProtocolParamsInfo{}, errors.New(
			"protocol parameters not available",
		)
	}
	info, err := protocolParamsInfoFromNative(
		pparams,
		a.ledgerState.CurrentEpoch(),
	)
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"convert current protocol parameters: %w",
			err,
		)
	}
	return info, nil
}

// EpochProtocolParams returns protocol parameters for the
// requested epoch.
func (a *NodeAdapter) EpochProtocolParams(
	epoch uint64,
) (ProtocolParamsInfo, error) {
	// Resolve the era active at the requested epoch via the epoch
	// table — the metadata-store GetPParams now requires an era so
	// the row's era_id matches the chosen decoder. Without this
	// lookup, picking an era from the row's own era_id would not
	// disambiguate the rollover-boundary case where two rows exist
	// at the same epoch with different shapes.
	//
	// Both reads run under one read transaction so a rollback or
	// epoch-rollover commit landing between them can't surface a
	// row whose era_id disagrees with the era chosen by GetEpoch.
	db := a.ledgerState.Database()
	txn := db.Transaction(false)
	defer txn.Release()
	epochRow, err := db.GetEpoch(epoch, txn)
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get epoch %d: %w", epoch, err,
		)
	}
	if epochRow == nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: %w",
			epoch,
			ErrEpochNotFound,
		)
	}
	era := eras.GetEraById(epochRow.EraId)
	if era == nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: unknown era ID %d",
			epoch,
			epochRow.EraId,
		)
	}
	pparamRows, err := db.Metadata().GetPParams(
		epoch,
		era.Id,
		txn.Metadata(),
	)
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: %w",
			epoch,
			err,
		)
	}
	if len(pparamRows) == 0 {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: %w",
			epoch,
			ErrEpochNotFound,
		)
	}
	pparamRow := pparamRows[0]
	pparams, err := era.DecodePParamsFunc(
		pparamRow.Cbor,
	)
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"decode protocol parameters for epoch %d from row epoch %d: %w",
			epoch,
			pparamRow.Epoch,
			err,
		)
	}
	if pparams == nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: %w",
			epoch,
			ErrEpochNotFound,
		)
	}
	info, err := protocolParamsInfoFromNative(
		pparams,
		epoch,
	)
	if err != nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"convert protocol parameters for epoch %d: %w",
			epoch,
			err,
		)
	}
	return info, nil
}

// Network returns current network supply and stake information.
func (a *NodeAdapter) Network() (NetworkInfo, error) {
	nodeCfg := a.ledgerState.CardanoNodeConfig()
	if nodeCfg == nil || nodeCfg.ShelleyGenesis() == nil {
		return NetworkInfo{}, errors.New("shelley genesis not available")
	}
	genesis := nodeCfg.ShelleyGenesis()
	maxSupply := genesis.MaxLovelaceSupply

	state, err := a.ledgerState.Database().Metadata().GetNetworkState(nil)
	if err != nil {
		return NetworkInfo{}, fmt.Errorf("get network state: %w", err)
	}
	treasury := uint64(0)
	reserves := uint64(0)
	if state != nil {
		treasury = uint64(state.Treasury)
		reserves = uint64(state.Reserves)
	}
	// Script-locked supply: lovelace held in live UTxOs whose payment
	// credential is a script. Subtracted from circulating supply below.
	locked, err := a.ledgerState.Database().Metadata().
		GetScriptLockedSupply(nil)
	if err != nil {
		return NetworkInfo{}, fmt.Errorf(
			"get script-locked supply: %w",
			err,
		)
	}
	total := uint64(0)
	if reserves <= maxSupply {
		total = maxSupply - reserves
	}
	circulating := uint64(0)
	if treasury+locked <= total {
		circulating = total - treasury - locked
	}

	activeStakeEpoch := uint64(0)
	currentEpoch := a.ledgerState.CurrentEpoch()
	if currentEpoch >= 2 {
		activeStakeEpoch = currentEpoch - 2
	}
	activeStake, err := a.ledgerState.Database().Metadata().
		GetTotalActiveStake(activeStakeEpoch, "mark", nil)
	if err != nil {
		return NetworkInfo{}, fmt.Errorf(
			"get active stake for epoch %d: %w",
			activeStakeEpoch,
			err,
		)
	}

	liveStake, err := a.liveStake()
	if err != nil {
		return NetworkInfo{}, err
	}

	return NetworkInfo{
		Supply: NetworkSupplyInfo{
			Max: strconv.FormatUint(maxSupply, 10),
			Total: strconv.FormatUint(
				total,
				10,
			),
			Circulating: strconv.FormatUint(
				circulating,
				10,
			),
			Locked:   strconv.FormatUint(locked, 10),
			Treasury: strconv.FormatUint(treasury, 10),
			Reserves: strconv.FormatUint(reserves, 10),
		},
		Stake: NetworkStakeInfo{
			Live:   strconv.FormatUint(liveStake, 10),
			Active: strconv.FormatUint(activeStake, 10),
		},
	}, nil
}

// NetworkEras returns hard-fork era summaries.
func (a *NodeAdapter) NetworkEras() ([]NetworkEraInfo, error) {
	ret := make([]NetworkEraInfo, 0, len(eras.Eras))
	for _, era := range eras.Eras {
		epochs, err := a.ledgerState.Database().Metadata().
			GetEpochsByEra(era.Id, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"get epochs for era %s: %w",
				era.Name,
				err,
			)
		}
		if len(epochs) == 0 {
			continue
		}
		first := epochs[0]
		last := epochs[len(epochs)-1]
		params, err := eras.BuildEraParams(
			a.ledgerState.CardanoNodeConfig(),
			era,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"build era params for %s: %w",
				era.Name,
				err,
			)
		}
		startTime, err := a.ledgerState.SlotToTime(first.StartSlot)
		if err != nil {
			return nil, fmt.Errorf(
				"get era %s start time: %w",
				era.Name,
				err,
			)
		}
		endSlot := last.StartSlot + uint64(last.LengthInSlots)
		endTime, err := a.ledgerState.SlotToTime(endSlot)
		if err != nil {
			return nil, fmt.Errorf(
				"get era %s end time: %w",
				era.Name,
				err,
			)
		}
		slotLengthMs := uint64(last.SlotLength)
		slotLengthSeconds := (slotLengthMs + 500) / 1000
		if slotLengthMs > 0 && slotLengthSeconds == 0 {
			slotLengthSeconds = 1
		}
		ret = append(ret, NetworkEraInfo{
			Era: era.Name,
			Start: NetworkEraBoundInfo{
				Time:  startTime.Unix(),
				Slot:  first.StartSlot,
				Epoch: first.EpochId,
			},
			End: &NetworkEraBoundInfo{
				Time:  endTime.Unix(),
				Slot:  endSlot,
				Epoch: last.EpochId + 1,
			},
			Params: NetworkEraParamsInfo{
				EpochLength: uint64(last.LengthInSlots),
				SlotLength:  slotLengthSeconds,
				SafeZone:    params.SafeZoneSlots,
			},
		})
	}
	return ret, nil
}

// Genesis returns Shelley genesis configuration values.
func (a *NodeAdapter) Genesis() (GenesisInfo, error) {
	nodeCfg := a.ledgerState.CardanoNodeConfig()
	if nodeCfg == nil || nodeCfg.ShelleyGenesis() == nil {
		return GenesisInfo{}, errors.New("shelley genesis not available")
	}
	genesis := nodeCfg.ShelleyGenesis()
	slotLength, _ := genesis.SlotLength.Float64()
	slotLengthSeconds := int(math.Round(slotLength))
	if slotLength > 0 && slotLengthSeconds == 0 {
		slotLengthSeconds = 1
	}
	return GenesisInfo{
		ActiveSlotsCoefficient: float32(a.ledgerState.ActiveSlotCoeff()),
		UpdateQuorum:           genesis.UpdateQuorum,
		MaxLovelaceSupply: strconv.FormatUint(
			genesis.MaxLovelaceSupply,
			10,
		),
		NetworkMagic:      int(genesis.NetworkMagic),
		EpochLength:       genesis.EpochLength,
		SystemStart:       int(genesis.SystemStart.Unix()),
		SlotsPerKESPeriod: genesis.SlotsPerKESPeriod,
		SlotLength:        slotLengthSeconds,
		MaxKESEvolutions:  genesis.MaxKESEvolutions,
		SecurityParam:     genesis.SecurityParam,
	}, nil
}

// Asset returns native asset information for a policy ID
// and raw asset name bytes.
func (a *NodeAdapter) Asset(
	policyID string,
	assetName []byte,
) (AssetInfo, error) {
	policyIDBytes, err := hex.DecodeString(policyID)
	if err != nil {
		return AssetInfo{}, fmt.Errorf(
			"decode asset policy ID %q: %w",
			policyID,
			err,
		)
	}
	policyHash := lcommon.NewBlake2b224(policyIDBytes)
	asset, err := a.ledgerState.Database().
		Metadata().
		GetAssetByPolicyAndName(policyHash, assetName, nil)
	if err != nil {
		return AssetInfo{}, fmt.Errorf(
			"get asset by policy %s and name %x: %w",
			policyID,
			assetName,
			err,
		)
	}
	if asset.ID == 0 {
		return AssetInfo{}, fmt.Errorf(
			"asset %s%x: %w",
			policyID,
			assetName,
			ErrAssetNotFound,
		)
	}
	quantity, err := a.ledgerState.Database().
		Metadata().
		GetAssetQuantityByPolicyAndName(policyHash, assetName, nil)
	if err != nil {
		return AssetInfo{}, fmt.Errorf(
			"get asset quantity by policy %s and name %x: %w",
			policyID,
			assetName,
			err,
		)
	}

	return AssetInfo{
		Asset:             policyID + hex.EncodeToString(assetName),
		PolicyID:          policyID,
		AssetName:         hex.EncodeToString(assetName),
		AssetNameASCII:    assetNameASCII(assetName),
		Fingerprint:       string(asset.Fingerprint),
		Quantity:          strconv.FormatUint(quantity, 10),
		InitialMintTxHash: "",
		MintOrBurnCount:   0,
		OnchainMetadata:   nil,
	}, nil
}

// AssetAddresses returns paginated addresses currently holding the given asset.
func (a *NodeAdapter) AssetAddresses(
	policyID string,
	assetName []byte,
	params PaginationParams,
) ([]AssetHolderInfo, int, error) {
	policyIDBytes, err := hex.DecodeString(policyID)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"decode asset policy ID %q: %w",
			policyID,
			err,
		)
	}
	utxos, err := a.ledgerState.Database().
		UtxosByAssets(policyIDBytes, assetName, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get asset UTxOs for %s%x: %w",
			policyID,
			assetName,
			err,
		)
	}
	holders, err := assetHoldersFromUtxos(policyIDBytes, assetName, utxos, params)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"build asset holders for %s%x: %w",
			policyID,
			assetName,
			err,
		)
	}
	if len(holders) == 0 {
		return nil, 0, fmt.Errorf(
			"asset %s%x: %w",
			policyID,
			assetName,
			ErrAssetNotFound,
		)
	}
	total := len(holders)
	return paginateAssetHolders(holders, params), total, nil
}

type assetHolderQuantity struct {
	address  string
	quantity uint64
}

func assetHoldersFromUtxos(
	policyID []byte,
	assetName []byte,
	utxos []models.Utxo,
	params PaginationParams,
) ([]AssetHolderInfo, error) {
	quantities := make(map[string]uint64)
	for _, utxo := range utxos {
		var quantity uint64
		for _, asset := range utxo.Assets {
			if bytes.Equal(asset.PolicyId, policyID) &&
				bytes.Equal(asset.Name, assetName) {
				quantity += uint64(asset.Amount)
			}
		}
		if quantity == 0 {
			continue
		}
		output, err := gledger.NewTransactionOutputFromCbor(utxo.Cbor)
		if err != nil {
			return nil, fmt.Errorf(
				"decode UTxO %x#%d: %w",
				utxo.TxId,
				utxo.OutputIdx,
				err,
			)
		}
		quantities[output.Address().String()] += quantity
	}

	rows := make([]assetHolderQuantity, 0, len(quantities))
	for address, quantity := range quantities {
		rows = append(rows, assetHolderQuantity{
			address:  address,
			quantity: quantity,
		})
	}
	slices.SortFunc(rows, func(a, b assetHolderQuantity) int {
		if params.Order == "desc" {
			if n := cmp.Compare(b.quantity, a.quantity); n != 0 {
				return n
			}
			return cmp.Compare(b.address, a.address)
		}
		if n := cmp.Compare(a.quantity, b.quantity); n != 0 {
			return n
		}
		return cmp.Compare(a.address, b.address)
	})

	holders := make([]AssetHolderInfo, 0, len(rows))
	for _, row := range rows {
		holders = append(holders, AssetHolderInfo{
			Address:  row.address,
			Quantity: strconv.FormatUint(row.quantity, 10),
		})
	}
	return holders, nil
}

func paginateAssetHolders(
	holders []AssetHolderInfo,
	params PaginationParams,
) []AssetHolderInfo {
	start := (params.Page - 1) * params.Count
	if start >= len(holders) {
		return []AssetHolderInfo{}
	}
	end := min(start+params.Count, len(holders))
	return holders[start:end]
}

// DRep returns governance DRep information for the requested
// credential.
func (a *NodeAdapter) DRep(
	credential DRepCredential,
) (DRepInfo, error) {
	if credential.CredentialTagKnown {
		var credentialTag uint8
		if credential.HasScript {
			credentialTag = 1
		}
		return a.drepByCredentialTag(credential, credentialTag)
	}

	info, err := a.drepByCredentialTag(credential, 0)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, ErrDRepNotFound) {
		return DRepInfo{}, err
	}
	return a.drepByCredentialTag(credential, 1)
}

func (a *NodeAdapter) drepByCredentialTag(
	credential DRepCredential,
	credentialTag uint8,
) (DRepInfo, error) {
	db := a.ledgerState.Database()
	drep, err := db.GetDrepByCredential(credentialTag, credential.Hash, true, nil)
	if err != nil {
		if errors.Is(err, models.ErrDrepNotFound) {
			return DRepInfo{}, fmt.Errorf(
				"drep %x: %w",
				credential.Hash,
				ErrDRepNotFound,
			)
		}
		return DRepInfo{}, fmt.Errorf(
			"get drep %x: %w",
			credential.Hash,
			err,
		)
	}
	hasScript := credentialTag == 1
	power, err := db.GetDRepVotingPower(credentialTag, credential.Hash, nil)
	if err != nil {
		return DRepInfo{}, fmt.Errorf(
			"get drep voting power %x: %w",
			credential.Hash,
			err,
		)
	}

	registrationEpoch, err := a.ledgerState.SlotToEpoch(drep.AddedSlot)
	if err != nil {
		return DRepInfo{}, fmt.Errorf(
			"get DRep registration epoch for slot %d: %w",
			drep.AddedSlot,
			err,
		)
	}

	currentEpoch := a.ledgerState.CurrentEpoch()
	registered := drep.Active
	active := registered &&
		(drep.ExpiryEpoch == 0 || drep.ExpiryEpoch > currentEpoch)
	amount := strconv.FormatUint(power, 10)

	return DRepInfo{
		DRepID:      credential.ID,
		Hex:         hex.EncodeToString(credential.Hash),
		HasScript:   hasScript,
		Registered:  registered,
		Epoch:       registrationEpoch.EpochId,
		Amount:      amount,
		Active:      active,
		ActiveEpoch: drep.LastActivityEpoch,
		LiveStake:   amount,
	}, nil
}

func (a *NodeAdapter) latestBlockData(
	hash []byte,
) (
	models.Block,
	lcommon.Block,
	error,
) {
	block, err := a.ledgerState.BlockByHash(hash)
	if err != nil {
		return models.Block{}, nil, fmt.Errorf(
			"get block by hash %x: %w",
			hash,
			err,
		)
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		return models.Block{}, nil, fmt.Errorf(
			"decode block %x: %w",
			hash,
			err,
		)
	}
	return block, decodedBlock, nil
}

func (a *NodeAdapter) liveStake() (uint64, error) {
	poolKeyHashes, err := a.ledgerState.Database().Metadata().
		GetActivePoolKeyHashes(nil)
	if err != nil {
		return 0, fmt.Errorf(
			"get active pool key hashes: %w",
			err,
		)
	}
	if len(poolKeyHashes) == 0 {
		return 0, nil
	}
	stakeByPool, _, err := a.ledgerState.Database().Metadata().
		GetStakeByPools(poolKeyHashes, nil)
	if err != nil {
		return 0, fmt.Errorf("get live stake by pools: %w", err)
	}
	total := uint64(0)
	for _, stake := range stakeByPool {
		total += stake
	}
	return total, nil
}

// PoolsExtended returns the current active pools with
// extended details.
func (a *NodeAdapter) PoolsExtended() (
	[]PoolExtendedInfo, error,
) {
	db := a.ledgerState.Database()
	txn := db.Transaction(false)
	defer txn.Release()

	poolKeyHashes, err := db.Metadata().GetActivePoolKeyHashes(txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"get active pool key hashes: %w",
			err,
		)
	}
	if len(poolKeyHashes) == 0 {
		return []PoolExtendedInfo{}, nil
	}

	liveStakeByPool, _, err := db.Metadata().GetStakeByPools(
		poolKeyHashes,
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get live stake by pools: %w",
			err,
		)
	}

	activeStakeByPool := make(map[string]uint64, len(poolKeyHashes))
	currentEpoch := a.ledgerState.CurrentEpoch()
	activeStakeEpoch := uint64(0)
	if currentEpoch >= 2 {
		activeStakeEpoch = currentEpoch - 2
	}
	snapshots, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		activeStakeEpoch,
		"mark",
		txn.Metadata(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get pool stake snapshots for epoch %d: %w",
			activeStakeEpoch,
			err,
		)
	}
	for _, snapshot := range snapshots {
		activeStakeByPool[hex.EncodeToString(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}

	poolHashes := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		poolHashes = append(poolHashes, lcommon.PoolKeyHash(poolKeyHash))
	}
	pools, err := db.GetPools(poolHashes, txn)
	if err != nil {
		return nil, fmt.Errorf("get pools: %w", err)
	}
	poolsByHash := make(map[string]*models.Pool, len(pools))
	for i := range pools {
		pool := &pools[i]
		poolsByHash[string(pool.PoolKeyHash)] = pool
	}

	ret := make([]PoolExtendedInfo, 0, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		pool, ok := poolsByHash[string(poolKeyHash)]
		if !ok {
			return nil, fmt.Errorf("get pool %x: %w", poolKeyHash, models.ErrPoolNotFound)
		}
		poolID := lcommon.PoolId(lcommon.NewBlake2b224(pool.PoolKeyHash))
		poolHex := hex.EncodeToString(pool.PoolKeyHash)

		latestRelays := pool.Relays
		if len(pool.Registration) > 0 {
			latestRelays = pool.Registration[0].Relays
		}

		relays := make([]PoolRelayInfo, 0, len(latestRelays))
		for _, relay := range latestRelays {
			tmpRelay := PoolRelayInfo{
				DNS: relay.Hostname,
			}
			if relay.Port != 0 {
				if relay.Port > uint(math.MaxInt) {
					return nil, fmt.Errorf("relay port out of range for pool %x", pool.PoolKeyHash)
				}
				port := int(relay.Port)
				tmpRelay.Port = &port
			}
			if relay.Ipv4 != nil {
				tmpRelay.IPv4 = relay.Ipv4.String()
			}
			if relay.Ipv6 != nil {
				tmpRelay.IPv6 = relay.Ipv6.String()
			}
			relays = append(relays, tmpRelay)
		}

		marginCost := 0.0
		if pool.Margin != nil && pool.Margin.Rat != nil {
			marginCost, _ = pool.Margin.Float64()
		}

		ret = append(ret, PoolExtendedInfo{
			PoolID:         poolID.String(),
			Hex:            poolHex,
			VrfKey:         hex.EncodeToString(pool.VrfKeyHash),
			ActiveStake:    strconv.FormatUint(activeStakeByPool[poolHex], 10),
			LiveStake:      strconv.FormatUint(liveStakeByPool[string(pool.PoolKeyHash)], 10),
			DeclaredPledge: strconv.FormatUint(uint64(pool.Pledge), 10),
			FixedCost:      strconv.FormatUint(uint64(pool.Cost), 10),
			MarginCost:     marginCost,
			Relays:         relays,
		})
	}

	return ret, nil
}

// Account returns stake-account information for the
// requested stake address.
func (a *NodeAdapter) Account(
	stakeAddress string,
) (AccountInfo, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(
		stakeAddress,
	)
	if err != nil {
		return AccountInfo{}, err
	}

	db := a.ledgerState.Database()
	account, err := db.GetAccountByCredential(
		credentialTag,
		stakeKey,
		true,
		nil,
	)
	if err != nil {
		return AccountInfo{}, err
	}
	controlledAmount, err := a.ledgerState.Database().
		GetControlledAmountByCredential(credentialTag, stakeKey, nil)
	if err != nil {
		return AccountInfo{}, fmt.Errorf(
			"get controlled amount: %w",
			err,
		)
	}

	var activeEpoch *int64
	if account.Active {
		epoch, err := a.ledgerState.SlotToEpoch(account.AddedSlot)
		if err != nil {
			return AccountInfo{}, fmt.Errorf(
				"get account active epoch for slot %d: %w",
				account.AddedSlot,
				err,
			)
		}
		epochID, err := uint64ToInt64(
			epoch.EpochId,
			"account active epoch",
		)
		if err != nil {
			return AccountInfo{}, err
		}
		activeEpoch = &epochID
	}

	// Per Blockfrost OpenAPI (>=0.1.85), `active` is the delegation
	// state (the account is registered and currently delegated to a
	// pool), while `registered` is the registration state on its own.
	// account.Active is Dingo's registration flag, so it backs
	// `registered`; `active` additionally requires a pool delegation.
	delegating := account.Active && len(account.Pool) > 0

	var poolID *string
	if delegating {
		pool := lcommon.PoolId(
			lcommon.NewBlake2b224(account.Pool),
		).String()
		poolID = &pool
	}

	sums, err := db.GetAccountSumsByCredential(
		credentialTag,
		stakeKey,
		nil,
	)
	if err != nil {
		return AccountInfo{}, fmt.Errorf(
			"get account sums: %w",
			err,
		)
	}

	reward := strconv.FormatUint(uint64(account.Reward), 10)
	return AccountInfo{
		StakeAddress:       stakeAddress,
		Active:             delegating,
		ActiveEpoch:        activeEpoch,
		ControlledAmount:   strconv.FormatUint(controlledAmount, 10),
		RewardsSum:         reward,
		WithdrawalsSum:     strconv.FormatUint(sums.WithdrawalsSum, 10),
		ReservesSum:        strconv.FormatUint(sums.ReservesSum, 10),
		TreasurySum:        strconv.FormatUint(sums.TreasurySum, 10),
		WithdrawableAmount: reward,
		PoolID:             poolID,
		DrepID:             accountDrepID(account.Drep, account.DrepType),
		Registered:         account.Active,
	}, nil
}

// accountDrepID renders the Bech32 DRep ID a stake account is delegated to,
// matching the Blockfrost account_content drep_id field. It returns nil when
// the account has no DRep delegation (no credential and the default key-hash
// type), distinguishing that from an explicit always-abstain/no-confidence
// delegation, which carry no credential but a non-default type.
func accountDrepID(credential []byte, drepType uint64) *string {
	if len(credential) == 0 && drepType == models.DrepTypeAddrKeyHash {
		return nil
	}
	// DrepType is a small ledger enum (0-3); guard the narrowing
	// conversion so gosec is satisfied and an out-of-range value
	// degrades to "no DRep" rather than wrapping negative.
	if drepType > uint64(math.MaxInt) {
		return nil
	}
	drep := lcommon.Drep{
		Type:       int(drepType),
		Credential: credential,
	}
	id := drep.String()
	return &id
}

// AccountAssociatedAddresses returns payment addresses
// associated with the requested stake address.
func (a *NodeAdapter) AccountAssociatedAddresses(
	stakeAddress string,
	params PaginationParams,
) ([]AccountAssociatedAddressInfo, int, error) {
	stakeAddr, credentialTag, stakeKey, err := parseStakeAddress(
		stakeAddress,
	)
	if err != nil {
		return nil, 0, err
	}
	networkID, err := uintToUint8(
		stakeAddr.NetworkId(),
		"stake address network id",
	)
	if err != nil {
		return nil, 0, err
	}
	if _, err := a.ledgerState.Database().
		GetAccountByCredential(
			credentialTag,
			stakeKey,
			true,
			nil,
		); err != nil {
		return nil, 0, err
	}
	total, err := a.ledgerState.Database().
		CountAddressesByCredential(credentialTag, stakeKey, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count associated addresses: %w",
			err,
		)
	}
	offset := (params.Page - 1) * params.Count

	rows, err := a.ledgerState.Database().
		GetAddressesByCredential(
			credentialTag,
			stakeKey,
			params.Count,
			offset,
			params.Order,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get associated addresses: %w",
			err,
		)
	}

	ret := make(
		[]AccountAssociatedAddressInfo,
		0,
		len(rows),
	)
	addressType := uint8(lcommon.AddressTypeKeyKey)
	if credentialTag == 1 {
		addressType = lcommon.AddressTypeKeyScript
	}
	for _, row := range rows {
		addr, err := lcommon.NewAddressFromParts(
			addressType,
			networkID,
			row.PaymentKey,
			stakeKey,
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"build associated address: %w",
				err,
			)
		}
		ret = append(ret, AccountAssociatedAddressInfo{
			Address: addr.String(),
		})
	}
	return ret, total, nil
}

// AccountDelegationHistory returns delegation history
// rows for the requested stake address.
func (a *NodeAdapter) AccountDelegationHistory(
	stakeAddress string,
	params PaginationParams,
) ([]AccountDelegationHistoryInfo, int, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}

	if _, err := a.ledgerState.Database().
		GetAccountByCredential(
			credentialTag,
			stakeKey,
			true,
			nil,
		); err != nil {
		return nil, 0, err
	}
	offset := (params.Page - 1) * params.Count
	total, err := a.ledgerState.Database().
		CountAccountDelegationHistoryByCredential(
			credentialTag,
			stakeKey,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count account delegation history: %w",
			err,
		)
	}
	if offset >= total {
		return []AccountDelegationHistoryInfo{}, total, nil
	}
	rows, err := a.ledgerState.Database().
		GetAccountDelegationHistoryByCredential(
			credentialTag,
			stakeKey,
			params.Count,
			offset,
			params.Order,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account delegation history: %w",
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(rows))
	ret := make([]AccountDelegationHistoryInfo, 0, len(rows))
	for _, row := range rows {
		activeEpoch, err := delegationActivationEpoch(
			a.ledgerState,
			row.AddedSlot,
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get activation epoch for delegation slot %d: %w",
				row.AddedSlot,
				err,
			)
		}
		txSlot, blockTime, blockHeight, err := a.accountHistoryBlockInfo(
			row.TxSlot,
			row.BlockHash,
			blockNumbers,
		)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, AccountDelegationHistoryInfo{
			ActiveEpoch: activeEpoch,
			TxHash:      hex.EncodeToString(row.TxHash),
			Amount:      "0",
			PoolID: lcommon.PoolId(
				lcommon.NewBlake2b224(row.PoolKeyHash),
			).String(),
			TxSlot:      txSlot,
			BlockTime:   blockTime,
			BlockHeight: blockHeight,
		})
	}
	return ret, total, nil
}

// AccountRegistrationHistory returns registration
// history rows for the requested stake address.
func (a *NodeAdapter) AccountRegistrationHistory(
	stakeAddress string,
	params PaginationParams,
) ([]AccountRegistrationHistoryInfo, int, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}

	if _, err := a.ledgerState.Database().
		GetAccountByCredential(
			credentialTag,
			stakeKey,
			true,
			nil,
		); err != nil {
		return nil, 0, err
	}
	offset := (params.Page - 1) * params.Count
	total, err := a.ledgerState.Database().
		CountAccountRegistrationHistoryByCredential(
			credentialTag,
			stakeKey,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count account registration history: %w",
			err,
		)
	}
	if offset >= total {
		return []AccountRegistrationHistoryInfo{}, total, nil
	}
	rows, err := a.ledgerState.Database().
		GetAccountRegistrationHistoryByCredential(
			credentialTag,
			stakeKey,
			params.Count,
			offset,
			params.Order,
			nil,
		)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get account registration history: %w",
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(rows))
	ret := make(
		[]AccountRegistrationHistoryInfo,
		0,
		len(rows),
	)
	for _, row := range rows {
		txSlot, blockTime, blockHeight, err := a.accountHistoryBlockInfo(
			row.TxSlot,
			row.BlockHash,
			blockNumbers,
		)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, AccountRegistrationHistoryInfo{
			TxHash:      hex.EncodeToString(row.TxHash),
			Action:      row.Action,
			Deposit:     strconv.FormatUint(row.Deposit, 10),
			TxSlot:      txSlot,
			BlockTime:   blockTime,
			BlockHeight: blockHeight,
		})
	}
	return ret, total, nil
}

// AccountRewardHistory returns reward history rows for
// the requested stake address.
func (a *NodeAdapter) AccountRewardHistory(
	stakeAddress string,
	params PaginationParams,
) ([]AccountRewardHistoryInfo, int, error) {
	_, credentialTag, stakeKey, err := parseStakeAddress(stakeAddress)
	if err != nil {
		return nil, 0, err
	}
	if _, err := a.ledgerState.Database().
		GetAccountByCredential(
			credentialTag,
			stakeKey,
			true,
			nil,
		); err != nil {
		return nil, 0, err
	}
	// TODO(#1875): Implement reward history once Dingo persists
	// per-account, per-epoch reward records. This endpoint remains
	// stubbed in this PR because the backing reward-history storage
	// and rollback-safe ledger/database plumbing do not exist yet.
	return []AccountRewardHistoryInfo{}, 0, nil
}

func blockIssuer(issuer lcommon.IssuerVkey) string {
	if bytes.Equal(issuer[:], make([]byte, len(issuer))) {
		return ""
	}
	return issuer.PoolId()
}

func parseStakeAddress(
	stakeAddress string,
) (lcommon.Address, uint8, []byte, error) {
	addr, err := lcommon.NewAddress(stakeAddress)
	if err != nil {
		return lcommon.Address{}, 0, nil, ErrInvalidStakeAddress
	}
	zeroHash := lcommon.NewBlake2b224(nil)
	if addr.PaymentKeyHash() != zeroHash ||
		addr.StakeKeyHash() == zeroHash {
		return lcommon.Address{}, 0, nil, ErrInvalidStakeAddress
	}
	var credentialTag uint8
	switch addr.StakingPayload().(type) {
	case lcommon.AddressPayloadKeyHash:
		credentialTag = 0
	case lcommon.AddressPayloadScriptHash:
		credentialTag = 1
	default:
		return lcommon.Address{}, 0, nil, ErrInvalidStakeAddress
	}
	stakeKey := addr.StakeKeyHash().Bytes()
	return addr, credentialTag, stakeKey, nil
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

func ratToFloat64(r *cbor.Rat) float64 {
	if r == nil || r.Denom().Sign() == 0 {
		return 0
	}
	f, _ := r.Float64()
	return f
}

func exUnitsMemString(exUnits lcommon.ExUnits) string {
	if exUnits.Memory <= 0 {
		return "0"
	}
	return strconv.FormatInt(exUnits.Memory, 10)
}

func exUnitsStepsString(exUnits lcommon.ExUnits) string {
	if exUnits.Steps <= 0 {
		return "0"
	}
	return strconv.FormatInt(exUnits.Steps, 10)
}

// Blockfrost uses a flattened protocol-parameter view, while Dingo keeps
// era-specific native types. Map directly from the native ledger type here
// instead of routing through the UTxO RPC representation first.
func protocolParamsInfoFromNative(
	pparams lcommon.ProtocolParameters,
	epoch uint64,
) (ProtocolParamsInfo, error) {
	info := ProtocolParamsInfo{
		Epoch:            epoch,
		KeyDeposit:       "0",
		PoolDeposit:      "0",
		MinPoolCost:      "0",
		CoinsPerUtxoSize: "0",
		MaxTxExMem:       "0",
		MaxTxExSteps:     "0",
		MaxBlockExMem:    "0",
		MaxBlockExSteps:  "0",
		MaxValSize:       "0",
	}
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor)
	case *mary.MaryProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor)
		info.MinPoolCost = strconv.FormatUint(pp.MinPoolCost, 10)
	case *alonzo.AlonzoProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor)
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs, pp.CostModels)
	case *babbage.BabbageProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor)
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs, pp.CostModels)
	case *conway.ConwayProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolVersion.Major, pp.ProtocolVersion.Minor)
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs, pp.CostModels)
		fillConwayPParamsInfo(&info, pp)
	default:
		return ProtocolParamsInfo{}, fmt.Errorf(
			"unsupported protocol parameters type: %T",
			pparams,
		)
	}
	return info, nil
}

func fillBasePParamsInfo(
	info *ProtocolParamsInfo,
	minFeeA uint,
	minFeeB uint,
	maxBlockBodySize uint,
	maxTxSize uint,
	maxBlockHeaderSize uint,
	keyDeposit uint,
	poolDeposit uint,
	maxEpoch uint,
	nOpt uint,
	a0 *cbor.Rat,
	rho *cbor.Rat,
	tau *cbor.Rat,
	protocolMajor uint,
	protocolMinor uint,
) {
	// These fields are shared across the Shelley-family protocol parameter
	// types, so they can be filled uniformly regardless of era.
	info.MinFeeA = uintToInt(minFeeA)
	info.MinFeeB = uintToInt(minFeeB)
	info.MaxBlockSize = uintToInt(maxBlockBodySize)
	info.MaxTxSize = uintToInt(maxTxSize)
	info.MaxBlockHeaderSize = uintToInt(maxBlockHeaderSize)
	info.KeyDeposit = strconv.FormatUint(uint64(keyDeposit), 10)
	info.PoolDeposit = strconv.FormatUint(uint64(poolDeposit), 10)
	info.EMax = uintToInt(maxEpoch)
	info.NOpt = uintToInt(nOpt)
	info.A0 = ratToFloat64(a0)
	info.Rho = ratToFloat64(rho)
	info.Tau = ratToFloat64(tau)
	info.ProtocolMajorVer = uintToInt(protocolMajor)
	info.ProtocolMinorVer = uintToInt(protocolMinor)
}

func fillAlonzoPParamsInfo(
	info *ProtocolParamsInfo,
	minPoolCost uint64,
	coinsPerUtxoByte uint64,
	executionCosts lcommon.ExUnitPrice,
	maxTxExUnits lcommon.ExUnits,
	maxBlockExUnits lcommon.ExUnits,
	maxValueSize uint,
	collateralPercentage uint,
	maxCollateralInputs uint,
	costModels map[uint][]int64,
) {
	// Execution pricing, ex-units, collateral, and coins-per-UTxO sizing only
	info.MinPoolCost = strconv.FormatUint(minPoolCost, 10)
	info.CoinsPerUtxoSize = strconv.FormatUint(coinsPerUtxoByte, 10)
	info.PriceMem = ratToFloat64(executionCosts.MemPrice)
	info.PriceStep = ratToFloat64(executionCosts.StepPrice)
	info.MaxTxExMem = exUnitsMemString(maxTxExUnits)
	info.MaxTxExSteps = exUnitsStepsString(maxTxExUnits)
	info.MaxBlockExMem = exUnitsMemString(maxBlockExUnits)
	info.MaxBlockExSteps = exUnitsStepsString(maxBlockExUnits)
	info.MaxValSize = strconv.FormatUint(uint64(maxValueSize), 10)
	info.CollateralPercent = uintToInt(collateralPercentage)
	info.MaxCollateralInputs = uintToInt(maxCollateralInputs)
	info.CostModelsRaw = costModelsRaw(costModels)
}

// fillConwayPParamsInfo maps Conway-era governance and reference-script
// parameters from the native ledger type. These fields are absent in
// pre-Conway eras and therefore left nil there.
func fillConwayPParamsInfo(
	info *ProtocolParamsInfo,
	pp *conway.ConwayProtocolParameters,
) {
	pvt := pp.PoolVotingThresholds
	dvt := pp.DRepVotingThresholds
	info.PvtMotionNoConfidence = ratValuePtr(pvt.MotionNoConfidence)
	info.PvtCommitteeNormal = ratValuePtr(pvt.CommitteeNormal)
	info.PvtCommitteeNoConfidence = ratValuePtr(pvt.CommitteeNoConfidence)
	info.PvtHardForkInitiation = ratValuePtr(pvt.HardForkInitiation)
	info.PvtPPSecurityGroup = ratValuePtr(pvt.PpSecurityGroup)
	info.DvtMotionNoConfidence = ratValuePtr(dvt.MotionNoConfidence)
	info.DvtCommitteeNormal = ratValuePtr(dvt.CommitteeNormal)
	info.DvtCommitteeNoConfidence = ratValuePtr(dvt.CommitteeNoConfidence)
	info.DvtUpdateToConstitution = ratValuePtr(dvt.UpdateToConstitution)
	info.DvtHardForkInitiation = ratValuePtr(dvt.HardForkInitiation)
	info.DvtPPNetworkGroup = ratValuePtr(dvt.PpNetworkGroup)
	info.DvtPPEconomicGroup = ratValuePtr(dvt.PpEconomicGroup)
	info.DvtPPTechnicalGroup = ratValuePtr(dvt.PpTechnicalGroup)
	info.DvtPPGovGroup = ratValuePtr(dvt.PpGovGroup)
	info.DvtTreasuryWithdrawal = ratValuePtr(dvt.TreasuryWithdrawal)
	committeeMinSize := strconv.FormatUint(uint64(pp.MinCommitteeSize), 10)
	info.CommitteeMinSize = &committeeMinSize
	committeeMaxTermLength := strconv.FormatUint(pp.CommitteeTermLimit, 10)
	info.CommitteeMaxTermLength = &committeeMaxTermLength
	govActionLifetime := strconv.FormatUint(pp.GovActionValidityPeriod, 10)
	info.GovActionLifetime = &govActionLifetime
	govActionDeposit := strconv.FormatUint(pp.GovActionDeposit, 10)
	info.GovActionDeposit = &govActionDeposit
	drepDeposit := strconv.FormatUint(pp.DRepDeposit, 10)
	info.DRepDeposit = &drepDeposit
	drepActivity := strconv.FormatUint(pp.DRepInactivityPeriod, 10)
	info.DRepActivity = &drepActivity
	info.MinFeeRefScriptCostPerByte = ratPointerPtr(
		pp.MinFeeRefScriptCostPerByte,
	)
}

// ratValuePtr converts a value cbor.Rat into a *float64 for JSON output. A
// zero-value Rat (nil inner big.Rat) yields a zero threshold rather than
// panicking.
func ratValuePtr(r cbor.Rat) *float64 {
	if r.Rat == nil {
		return new(float64)
	}
	f := ratToFloat64(&r)
	return &f
}

// ratPointerPtr converts an optional *cbor.Rat into a *float64, preserving
// nil so an absent parameter serializes as JSON null.
func ratPointerPtr(r *cbor.Rat) *float64 {
	if r == nil {
		return nil
	}
	f := ratToFloat64(r)
	return &f
}

// costModelsRaw renders cost models keyed by Plutus version name with raw
// integer arrays, matching the Blockfrost cost_models_raw field. It returns
// nil when the era carries no cost models.
func costModelsRaw(models map[uint][]int64) *any {
	if len(models) == 0 {
		return nil
	}
	out := make(map[string][]int64, len(models))
	for version, model := range models {
		out[plutusVersionName(version)] = model
	}
	var result any = out
	return &result
}

func plutusVersionName(version uint) string {
	switch version {
	case 0:
		return "PlutusV1"
	case 1:
		return "PlutusV2"
	case 2:
		return "PlutusV3"
	default:
		return "PlutusV" + strconv.FormatUint(uint64(version)+1, 10)
	}
}

func uintToInt(v uint) int {
	if uint64(v) > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// AddressUTXOs returns paginated current UTxOs for the
// requested address.
func (a *NodeAdapter) AddressUTXOs(
	address string,
	params PaginationParams,
) ([]AddressUTXOInfo, int, error) {
	addr, err := lcommon.NewAddress(address)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"parse address %q: %w",
			address,
			ErrInvalidAddress,
		)
	}

	utxos, err := a.ledgerState.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			Addresses: []lcommon.Address{addr},
		},
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get address UTxOs for %q: %w",
			address,
			err,
		)
	}
	total := len(utxos)
	if params.Order == PaginationOrderDesc {
		for left, right := 0, len(utxos)-1; left < right; left, right = left+1, right-1 {
			utxos[left], utxos[right] = utxos[right], utxos[left]
		}
	}

	paged := paginateUtxos(utxos, params)
	txBlockHashes, err := a.addressUtxoBlockHashes(paged)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get block hashes for address UTxOs %q: %w",
			address,
			err,
		)
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
		return nil, 0, fmt.Errorf(
			"parse address %q: %w",
			address,
			ErrInvalidAddress,
		)
	}

	total, err := a.ledgerState.CountTransactionsByAddress(addr)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count address transactions for %q: %w",
			address,
			err,
		)
	}

	txs, err := a.ledgerState.GetTransactionsByAddressWithOrder(
		addr,
		params.Count,
		(params.Page-1)*params.Count,
		params.Order,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get address transactions for %q: %w",
			address,
			err,
		)
	}

	blockNumbers := make(map[string]uint64, len(txs))
	ret := make([]AddressTransactionInfo, 0, len(txs))
	for _, tx := range txs {
		blockHashKey := hex.EncodeToString(tx.BlockHash)
		blockHeight, ok := blockNumbers[blockHashKey]
		if !ok {
			block, err := a.ledgerState.BlockByHash(tx.BlockHash)
			if err != nil {
				return nil, 0, fmt.Errorf(
					"get block for transaction %x: %w",
					tx.Hash,
					err,
				)
			}
			blockHeight = block.Number
			blockNumbers[blockHashKey] = blockHeight
		}

		blockTime, err := a.transactionBlockTime(tx)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get block time for transaction %x: %w",
				tx.Hash,
				err,
			)
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

// MetadataTransactions returns paginated transaction metadata values for the
// requested label in JSON form.
func (a *NodeAdapter) MetadataTransactions(
	label uint64,
	params PaginationParams,
) ([]MetadataTransactionJSONInfo, int, error) {
	db := a.ledgerState.Database()

	total, err := db.CountTransactionsByMetadataLabel(label, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count transactions by metadata label %d: %w",
			label,
			err,
		)
	}

	txs, err := db.GetTransactionsByMetadataLabel(
		label,
		params.Count,
		(params.Page-1)*params.Count,
		params.Order == PaginationOrderDesc,
		nil,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get transactions by metadata label %d: %w",
			label,
			err,
		)
	}

	ret := make([]MetadataTransactionJSONInfo, 0, len(txs))
	for _, tx := range txs {
		jsonValue, _, err := labelcodec.RawValues(tx.Metadata, label)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"extract json metadata label %d from tx %x: %w",
				label,
				tx.Hash,
				err,
			)
		}
		ret = append(ret, MetadataTransactionJSONInfo{
			TxHash:       hex.EncodeToString(tx.Hash),
			JSONMetadata: jsonValue,
		})
	}
	return ret, total, nil
}

// MetadataTransactionsCBOR returns paginated transaction metadata values for
// the requested label in CBOR-hex form.
func (a *NodeAdapter) MetadataTransactionsCBOR(
	label uint64,
	params PaginationParams,
) ([]MetadataTransactionCBORInfo, int, error) {
	db := a.ledgerState.Database()

	total, err := db.CountTransactionsByMetadataLabel(label, nil)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"count transactions by metadata label %d: %w",
			label,
			err,
		)
	}

	txs, err := db.GetTransactionsByMetadataLabel(
		label,
		params.Count,
		(params.Page-1)*params.Count,
		params.Order == PaginationOrderDesc,
		nil,
	)
	if err != nil {
		return nil, 0, fmt.Errorf(
			"get transactions by metadata label %d: %w",
			label,
			err,
		)
	}

	ret := make([]MetadataTransactionCBORInfo, 0, len(txs))
	for _, tx := range txs {
		_, cborValue, err := labelcodec.RawValues(tx.Metadata, label)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"extract cbor metadata label %d from tx %x: %w",
				label,
				tx.Hash,
				err,
			)
		}
		ret = append(ret, MetadataTransactionCBORInfo{
			TxHash:   hex.EncodeToString(tx.Hash),
			Metadata: hex.EncodeToString(cborValue),
		})
	}
	return ret, total, nil
}

// Transaction returns summary details for the requested transaction.
func (a *NodeAdapter) Transaction(
	hash []byte,
) (TransactionInfo, error) {
	tx, block, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return TransactionInfo{}, err
	}

	blockTime, err := a.transactionBlockTime(*tx)
	if err != nil {
		return TransactionInfo{}, err
	}

	size := len(decodedTx.Cbor())
	withdrawalCount := len(decodedTx.Withdrawals())
	assetMintBurnCount := 0
	if mint := decodedTx.AssetMint(); mint != nil {
		for _, policy := range mint.Policies() {
			assetMintBurnCount += len(mint.Assets(policy))
		}
	}

	var invalidBefore *string
	if v := decodedTx.ValidityIntervalStart(); v != 0 {
		ret := strconv.FormatUint(v, 10)
		invalidBefore = &ret
	}
	var invalidHereafter *string
	if v := decodedTx.TTL(); v != 0 {
		ret := strconv.FormatUint(v, 10)
		invalidHereafter = &ret
	}

	counts := transactionCertificateCounts(tx.Certificates)
	outputs := tx.Outputs
	if !tx.Valid && tx.CollateralReturn != nil {
		outputs = []models.Utxo{*tx.CollateralReturn}
	}
	outputAmount := aggregateTransactionOutputAmount(outputs)
	utxoCount := len(tx.Inputs) +
		len(tx.Collateral) +
		len(tx.ReferenceInputs) +
		len(tx.Outputs)
	if tx.CollateralReturn != nil && !tx.Valid {
		utxoCount++
	}
	deposit, err := a.transactionDeposit(decodedTx, block.Type, tx.Slot)
	if err != nil {
		return TransactionInfo{}, fmt.Errorf(
			"calculate deposit for transaction %x: %w",
			hash,
			err,
		)
	}

	return TransactionInfo{
		Hash:               hex.EncodeToString(tx.Hash),
		Block:              hex.EncodeToString(tx.BlockHash),
		Slot:               tx.Slot,
		BlockHeight:        block.Number,
		BlockTime:          blockTime,
		Index:              tx.BlockIndex,
		OutputAmount:       outputAmount,
		Fees:               strconv.FormatUint(uint64(tx.Fee), 10),
		Deposit:            strconv.FormatUint(deposit, 10),
		TreasuryDonation:   bigIntString(decodedTx.Donation()),
		Size:               size,
		UtxoCount:          utxoCount,
		WithdrawalCount:    withdrawalCount,
		MirCertCount:       counts.mirCerts,
		DelegationCount:    counts.delegations,
		StakeCertCount:     counts.stakeCerts,
		PoolUpdateCount:    counts.poolUpdates,
		PoolRetireCount:    counts.poolRetires,
		AssetMintBurnCount: assetMintBurnCount,
		RedeemerCount:      len(tx.Redeemers),
		ValidContract:      tx.Valid,
		InvalidBefore:      invalidBefore,
		InvalidHereafter:   invalidHereafter,
	}, nil
}

// TransactionSubmit submits raw signed transaction CBOR to the mempool.
func (a *NodeAdapter) TransactionSubmit(
	txCbor []byte,
) (string, error) {
	if a.submitter == nil {
		return "", ErrMempoolUnavailable
	}
	txType, err := gledger.DetermineTransactionType(txCbor)
	if err != nil {
		return "", fmt.Errorf(
			"%w: determine transaction type: %w",
			ErrInvalidTransaction,
			err,
		)
	}
	tx, err := gledger.NewTransactionFromCbor(txType, txCbor)
	if err != nil {
		return "", fmt.Errorf(
			"decode transaction: %w: %w",
			err,
			ErrInvalidTransaction,
		)
	}
	if err := a.submitter.AddTransaction(txType, txCbor); err != nil {
		if _, ok := errors.AsType[*mempool.MempoolFullError](err); ok {
			return "", fmt.Errorf("submit transaction to mempool: %w: %w", err, ErrMempoolFull)
		}
		return "", fmt.Errorf("submit transaction to mempool: %w: %w", err, ErrInvalidTransaction)
	}
	return tx.Hash().String(), nil
}

// TransactionCBOR returns raw signed transaction CBOR bytes for the requested
// transaction hash.
func (a *NodeAdapter) TransactionCBOR(
	hash []byte,
) ([]byte, error) {
	_, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	txCbor := decodedTx.Cbor()
	if len(txCbor) == 0 {
		return nil, fmt.Errorf(
			"transaction %x CBOR unavailable",
			hash,
		)
	}
	return txCbor, nil
}

// TransactionMetadata returns metadata labels for the requested transaction as
// JSON values.
func (a *NodeAdapter) TransactionMetadata(
	hash []byte,
) ([]TransactionMetadataInfo, error) {
	entries, err := a.transactionMetadataEntries(hash)
	if err != nil {
		return nil, err
	}
	ret := make([]TransactionMetadataInfo, 0, len(entries))
	for _, entry := range entries {
		ret = append(ret, TransactionMetadataInfo{
			Label:        strconv.FormatUint(entry.Label, 10),
			JSONMetadata: json.RawMessage(entry.JsonValue),
		})
	}
	return ret, nil
}

// TransactionMetadataCBOR returns metadata labels for the requested transaction
// as CBOR hex values.
func (a *NodeAdapter) TransactionMetadataCBOR(
	hash []byte,
) ([]TransactionMetadataCBORInfo, error) {
	entries, err := a.transactionMetadataEntries(hash)
	if err != nil {
		return nil, err
	}
	ret := make([]TransactionMetadataCBORInfo, 0, len(entries))
	for _, entry := range entries {
		ret = append(ret, TransactionMetadataCBORInfo{
			Label:        strconv.FormatUint(entry.Label, 10),
			CBORMetadata: hex.EncodeToString(entry.CborValue),
		})
	}
	return ret, nil
}

// TransactionUTXOs returns inputs and outputs for the requested transaction.
func (a *NodeAdapter) TransactionUTXOs(
	hash []byte,
) (TransactionUTXOsInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return TransactionUTXOsInfo{}, err
	}

	decodedOutputs := decodedTx.Outputs()
	txOutputs := slices.Clone(tx.Outputs)
	isCollateralReturn := false
	if !tx.Valid && tx.CollateralReturn != nil {
		txOutputs = []models.Utxo{*tx.CollateralReturn}
		isCollateralReturn = true
	}
	slices.SortFunc(txOutputs, func(a, b models.Utxo) int {
		return cmp.Compare(a.OutputIdx, b.OutputIdx)
	})
	outputs := make([]TransactionOutputInfo, 0, len(txOutputs))
	for _, output := range txOutputs {
		address := ""
		outputIndex := int(output.OutputIdx)
		if outputIndex < len(decodedOutputs) {
			address = decodedOutputs[outputIndex].Address().String()
		}
		outputs = append(outputs, TransactionOutputInfo{
			Address:             address,
			Amount:              addressAmountsFromUtxo(output),
			OutputIndex:         output.OutputIdx,
			DataHash:            optionalHexString(output.DatumHash),
			InlineDatum:         optionalHexString(output.Datum),
			ReferenceScriptHash: nil,
			Collateral:          isCollateralReturn,
		})
	}

	txInputs := slices.Clone(tx.Inputs)
	slices.SortFunc(txInputs, compareUtxoRefs)
	txCollateral := slices.Clone(tx.Collateral)
	slices.SortFunc(txCollateral, compareUtxoRefs)
	txReferenceInputs := slices.Clone(tx.ReferenceInputs)
	slices.SortFunc(txReferenceInputs, compareUtxoRefs)

	inputCbor, err := a.transactionInputCbor(
		txInputs,
		txCollateral,
		txReferenceInputs,
	)
	if err != nil {
		return TransactionUTXOsInfo{}, fmt.Errorf(
			"resolve transaction input CBOR for %x: %w",
			hash,
			err,
		)
	}

	inputs := make([]TransactionInputInfo, 0, len(txInputs)+len(txCollateral)+len(txReferenceInputs))
	for _, input := range txInputs {
		input.Cbor = inputCbor[utxoRef(input)]
		info, err := a.transactionInputInfoFromUtxo(input, false, nil)
		if err != nil {
			return TransactionUTXOsInfo{}, fmt.Errorf("resolve input address for %x:%d: %w", input.TxId, input.OutputIdx, err)
		}
		inputs = append(inputs, info)
	}
	for _, input := range txCollateral {
		input.Cbor = inputCbor[utxoRef(input)]
		info, err := a.transactionInputInfoFromUtxo(input, true, nil)
		if err != nil {
			return TransactionUTXOsInfo{}, fmt.Errorf("resolve collateral address for %x:%d: %w", input.TxId, input.OutputIdx, err)
		}
		inputs = append(inputs, info)
	}
	referenceInput := true
	for _, input := range txReferenceInputs {
		input.Cbor = inputCbor[utxoRef(input)]
		info, err := a.transactionInputInfoFromUtxo(input, false, &referenceInput)
		if err != nil {
			return TransactionUTXOsInfo{}, fmt.Errorf("resolve reference input address for %x:%d: %w", input.TxId, input.OutputIdx, err)
		}
		inputs = append(inputs, info)
	}

	return TransactionUTXOsInfo{
		Hash:    hex.EncodeToString(tx.Hash),
		Inputs:  inputs,
		Outputs: outputs,
	}, nil
}

func (a *NodeAdapter) transactionInputCbor(
	inputGroups ...[]models.Utxo,
) (map[database.UtxoRef][]byte, error) {
	seen := map[database.UtxoRef]struct{}{}
	refs := []database.UtxoRef{}
	for _, inputs := range inputGroups {
		for _, input := range inputs {
			ref := utxoRef(input)
			if _, ok := seen[ref]; ok {
				continue
			}
			seen[ref] = struct{}{}
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return map[database.UtxoRef][]byte{}, nil
	}
	return a.ledgerState.Database().CborCache().ResolveUtxoCborBatch(refs)
}

func utxoRef(utxo models.Utxo) database.UtxoRef {
	var txID [32]byte
	copy(txID[:], utxo.TxId)
	return database.UtxoRef{
		TxId:      txID,
		OutputIdx: utxo.OutputIdx,
	}
}

func transactionInputRef(input lcommon.TransactionInput) database.UtxoRef {
	var txID [32]byte
	copy(txID[:], input.Id().Bytes())
	return database.UtxoRef{
		TxId:      txID,
		OutputIdx: input.Index(),
	}
}

// TransactionDelegations returns delegation certificates in the requested
// transaction.
func (a *NodeAdapter) TransactionDelegations(
	hash []byte,
) ([]TransactionDelegationInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	epoch, err := a.ledgerState.SlotToEpoch(tx.Slot)
	if err != nil {
		return nil, fmt.Errorf(
			"get active delegation epoch for transaction %x: %w",
			hash,
			err,
		)
	}
	activeEpoch := epoch.EpochId + 2
	networkID := a.networkID()

	certs, err := transactionCertificatesByType(
		hash,
		tx,
		decodedTx,
		lcommon.CertificateTypeStakeDelegation,
		lcommon.CertificateTypeStakeRegistrationDelegation,
		lcommon.CertificateTypeStakeVoteDelegation,
		lcommon.CertificateTypeStakeVoteRegistrationDelegation,
	)
	if err != nil {
		return nil, err
	}
	ret := []TransactionDelegationInfo{}
	for _, cert := range certs {
		var stakeCredential lcommon.Credential
		var poolKeyHash lcommon.PoolKeyHash
		switch c := cert.Certificate.(type) {
		case *lcommon.StakeDelegationCertificate:
			if c.StakeCredential == nil {
				continue
			}
			stakeCredential = *c.StakeCredential
			poolKeyHash = c.PoolKeyHash
		case *lcommon.StakeRegistrationDelegationCertificate:
			stakeCredential = c.StakeCredential
			poolKeyHash = c.PoolKeyHash
		case *lcommon.StakeVoteDelegationCertificate:
			stakeCredential = c.StakeCredential
			poolKeyHash = c.PoolKeyHash
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			stakeCredential = c.StakeCredential
			poolKeyHash = c.PoolKeyHash
		default:
			continue
		}
		address, err := stakeAddressFromCredential(stakeCredential, networkID)
		if err != nil {
			return nil, fmt.Errorf(
				"convert delegation stake address for transaction %x cert %d: %w",
				hash,
				cert.Index,
				err,
			)
		}
		ret = append(ret, TransactionDelegationInfo{
			Address:     address,
			PoolID:      lcommon.PoolId(poolKeyHash).String(),
			Index:       cert.Index,
			CertIndex:   cert.Index,
			ActiveEpoch: activeEpoch,
		})
	}
	return ret, nil
}

// TransactionStakeAddresses returns stake registration/deregistration
// certificates in the requested transaction.
func (a *NodeAdapter) TransactionStakeAddresses(
	hash []byte,
) ([]TransactionStakeAddressInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	networkID := a.networkID()

	certs, err := transactionCertificatesByType(
		hash,
		tx,
		decodedTx,
		lcommon.CertificateTypeStakeRegistration,
		lcommon.CertificateTypeStakeDeregistration,
		lcommon.CertificateTypeRegistration,
		lcommon.CertificateTypeDeregistration,
		lcommon.CertificateTypeStakeRegistrationDelegation,
		lcommon.CertificateTypeStakeVoteRegistrationDelegation,
		lcommon.CertificateTypeVoteRegistrationDelegation,
	)
	if err != nil {
		return nil, err
	}
	ret := []TransactionStakeAddressInfo{}
	for _, cert := range certs {
		var stakeCredential lcommon.Credential
		var registration bool
		switch c := cert.Certificate.(type) {
		case *lcommon.StakeRegistrationCertificate:
			stakeCredential = c.StakeCredential
			registration = true
		case *lcommon.RegistrationCertificate:
			stakeCredential = c.StakeCredential
			registration = true
		case *lcommon.StakeRegistrationDelegationCertificate:
			stakeCredential = c.StakeCredential
			registration = true
		case *lcommon.StakeVoteRegistrationDelegationCertificate:
			stakeCredential = c.StakeCredential
			registration = true
		case *lcommon.VoteRegistrationDelegationCertificate:
			stakeCredential = c.StakeCredential
			registration = true
		case *lcommon.StakeDeregistrationCertificate:
			stakeCredential = c.StakeCredential
		case *lcommon.DeregistrationCertificate:
			stakeCredential = c.StakeCredential
		default:
			continue
		}
		address, err := stakeAddressFromCredential(stakeCredential, networkID)
		if err != nil {
			return nil, fmt.Errorf(
				"convert stake address for transaction %x cert %d: %w",
				hash,
				cert.Index,
				err,
			)
		}
		ret = append(ret, TransactionStakeAddressInfo{
			Address:      address,
			CertIndex:    cert.Index,
			Registration: registration,
		})
	}
	return ret, nil
}

// TransactionWithdrawals returns reward withdrawals in the requested
// transaction.
func (a *NodeAdapter) TransactionWithdrawals(
	hash []byte,
) ([]TransactionWithdrawalInfo, error) {
	_, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}

	ret := make([]TransactionWithdrawalInfo, 0, len(decodedTx.Withdrawals()))
	for address, amount := range decodedTx.Withdrawals() {
		ret = append(ret, TransactionWithdrawalInfo{
			Address: address.String(),
			Amount:  amount.String(),
		})
	}
	slices.SortFunc(ret, func(a, b TransactionWithdrawalInfo) int {
		return cmp.Compare(a.Address, b.Address)
	})
	return ret, nil
}

// TransactionMIRs returns MIR certificate reward targets in the requested
// transaction.
func (a *NodeAdapter) TransactionMIRs(
	hash []byte,
) ([]TransactionMIRInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	certs, err := transactionCertificatesByType(
		hash,
		tx,
		decodedTx,
		lcommon.CertificateTypeMoveInstantaneousRewards,
	)
	if err != nil {
		return nil, err
	}

	networkID := a.networkID()
	ret := []TransactionMIRInfo{}
	for _, cert := range certs {
		c, ok := cert.Certificate.(*lcommon.MoveInstantaneousRewardsCertificate)
		if !ok {
			continue
		}
		pot := ""
		switch c.Reward.Source {
		case uint(lcommon.MirSourceReserves):
			pot = "reserve"
		case uint(lcommon.MirSourceTreasury):
			pot = "treasury"
		}
		for credential, amount := range c.Reward.Rewards {
			address, err := stakeAddressFromCredential(*credential, networkID)
			if err != nil {
				return nil, fmt.Errorf(
					"convert MIR stake address for transaction %x cert %d: %w",
					hash,
					cert.Index,
					err,
				)
			}
			ret = append(ret, TransactionMIRInfo{
				Address:   address,
				Amount:    strconv.FormatUint(amount, 10),
				CertIndex: cert.Index,
				Pot:       pot,
			})
		}
	}
	slices.SortFunc(ret, func(a, b TransactionMIRInfo) int {
		if c := cmp.Compare(a.CertIndex, b.CertIndex); c != 0 {
			return c
		}
		return cmp.Compare(a.Address, b.Address)
	})
	return ret, nil
}

// TransactionPoolUpdates returns pool registration certificates in the
// requested transaction.
func (a *NodeAdapter) TransactionPoolUpdates(
	hash []byte,
) ([]TransactionPoolUpdateInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	epoch, err := a.ledgerState.SlotToEpoch(tx.Slot)
	if err != nil {
		return nil, fmt.Errorf(
			"get active pool epoch for transaction %x: %w",
			hash,
			err,
		)
	}
	certs, err := transactionCertificatesByType(
		hash,
		tx,
		decodedTx,
		lcommon.CertificateTypePoolRegistration,
	)
	if err != nil {
		return nil, err
	}

	networkID := a.networkID()
	ret := []TransactionPoolUpdateInfo{}
	for _, cert := range certs {
		c, ok := cert.Certificate.(*lcommon.PoolRegistrationCertificate)
		if !ok {
			continue
		}
		owners := make([]string, 0, len(c.PoolOwners))
		for _, owner := range c.PoolOwners {
			address, err := stakeAddressFromCredential(
				lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(owner),
				},
				networkID,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"convert pool owner address for transaction %x cert %d: %w",
					hash,
					cert.Index,
					err,
				)
			}
			owners = append(owners, address)
		}

		rewardAccountCredential := poolRewardAccountCredential(c)
		rewardAccount, err := stakeAddressFromCredential(
			rewardAccountCredential,
			networkID,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"convert pool reward account for transaction %x cert %d: %w",
				hash,
				cert.Index,
				err,
			)
		}

		ret = append(ret, TransactionPoolUpdateInfo{
			ActiveEpoch: epoch.EpochId + 2,
			CertIndex:   cert.Index,
			FixedCost:   strconv.FormatUint(c.Cost, 10),
			MarginCost:  ratToFloat64(&c.Margin),
			Metadata: func() *TransactionPoolMetadataInfo {
				if c.PoolMetadata == nil {
					return nil
				}
				return &TransactionPoolMetadataInfo{
					URL:  c.PoolMetadata.Url,
					Hash: hex.EncodeToString(c.PoolMetadata.Hash.Bytes()),
				}
			}(),
			Owners:        owners,
			Pledge:        strconv.FormatUint(c.Pledge, 10),
			PoolID:        lcommon.PoolId(c.Operator).String(),
			Relays:        transactionPoolRelaysInfo(c.Relays),
			RewardAccount: rewardAccount,
			VrfKey:        hex.EncodeToString(c.VrfKeyHash.Bytes()),
		})
	}
	return ret, nil
}

// TransactionPoolRetires returns pool retirement certificates in the requested
// transaction.
func (a *NodeAdapter) TransactionPoolRetires(
	hash []byte,
) ([]TransactionPoolRetireInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	certs, err := transactionCertificatesByType(
		hash,
		tx,
		decodedTx,
		lcommon.CertificateTypePoolRetirement,
	)
	if err != nil {
		return nil, err
	}

	ret := []TransactionPoolRetireInfo{}
	for _, cert := range certs {
		c, ok := cert.Certificate.(*lcommon.PoolRetirementCertificate)
		if !ok {
			continue
		}
		ret = append(ret, TransactionPoolRetireInfo{
			PoolID:        lcommon.PoolId(c.PoolKeyHash).String(),
			CertIndex:     cert.Index,
			RetiringEpoch: c.Epoch,
		})
	}
	return ret, nil
}

// TransactionRedeemers returns Plutus redeemers stored for the requested
// transaction.
func (a *NodeAdapter) TransactionRedeemers(
	hash []byte,
) ([]TransactionRedeemerInfo, error) {
	tx, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}

	redeemers := slices.Clone(tx.Redeemers)
	slices.SortFunc(redeemers, func(a, b models.Redeemer) int {
		if c := cmp.Compare(a.Tag, b.Tag); c != 0 {
			return c
		}
		return cmp.Compare(a.Index, b.Index)
	})
	if len(redeemers) == 0 {
		return []TransactionRedeemerInfo{}, nil
	}

	metadata, err := a.transactionRedeemerMetadata(hash, tx, decodedTx)
	if err != nil {
		return nil, err
	}
	pparams, err := a.protocolParamsForSlot(tx.Slot)
	if err != nil {
		return nil, fmt.Errorf(
			"get protocol parameters for transaction %x redeemer fees: %w",
			hash,
			err,
		)
	}
	executionCosts, err := executionCostsFromPParams(pparams)
	if err != nil {
		return nil, fmt.Errorf(
			"get execution prices for transaction %x redeemer fees: %w",
			hash,
			err,
		)
	}
	ret := make([]TransactionRedeemerInfo, 0, len(redeemers))
	for _, redeemer := range redeemers {
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTag(redeemer.Tag),
			Index: redeemer.Index,
		}
		dataHash := lcommon.Blake2b256Hash(redeemer.Data)
		redeemerMetadata, ok := metadata[key]
		if !ok {
			return nil, fmt.Errorf(
				"resolve redeemer metadata for transaction %x tag=%d index=%d",
				hash,
				redeemer.Tag,
				redeemer.Index,
			)
		}
		fee := redeemerExecutionFee(
			executionCosts,
			redeemer.ExUnitsMemory,
			redeemer.ExUnitsCPU,
		)
		ret = append(ret, TransactionRedeemerInfo{
			DatumHash:        redeemerMetadata.DatumHash,
			TxIndex:          int(redeemer.Index),
			Purpose:          redeemerPurpose(lcommon.RedeemerTag(redeemer.Tag)),
			ScriptHash:       redeemerMetadata.ScriptHash,
			RedeemerDataHash: hex.EncodeToString(dataHash.Bytes()),
			UnitMem:          strconv.FormatUint(redeemer.ExUnitsMemory, 10),
			UnitSteps:        strconv.FormatUint(redeemer.ExUnitsCPU, 10),
			Fee:              strconv.FormatUint(fee, 10),
		})
	}
	return ret, nil
}

// TransactionRequiredSigners returns the required signers (extra signing key
// hashes) declared in the requested transaction.
func (a *NodeAdapter) TransactionRequiredSigners(
	hash []byte,
) ([]TransactionRequiredSignerInfo, error) {
	_, _, decodedTx, err := a.decodedTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	signers := decodedTx.RequiredSigners()
	ret := make([]TransactionRequiredSignerInfo, 0, len(signers))
	for _, signer := range signers {
		ret = append(ret, TransactionRequiredSignerInfo{
			WitnessHash: hex.EncodeToString(signer.Bytes()),
		})
	}
	return ret, nil
}

func (a *NodeAdapter) addressUtxoBlockHashes(
	utxos []models.UtxoWithOrdering,
) (map[string]string, error) {
	ret := make(map[string]string, len(utxos))
	if len(utxos) == 0 {
		return ret, nil
	}

	hashes := make([][]byte, 0, len(utxos))
	seen := make(map[string]struct{}, len(utxos))
	for _, utxo := range utxos {
		txKey := hex.EncodeToString(utxo.TxId)
		if _, ok := seen[txKey]; ok {
			continue
		}
		seen[txKey] = struct{}{}
		hashes = append(hashes, utxo.TxId)
	}

	txs, err := a.ledgerState.GetTransactionsByHashes(hashes)
	if err != nil {
		return nil, fmt.Errorf(
			"get transactions for address UTxO block mapping: %w",
			err,
		)
	}
	for _, tx := range txs {
		ret[hex.EncodeToString(tx.Hash)] = hex.EncodeToString(tx.BlockHash)
	}
	return ret, nil
}

func findTransactionInBlock(
	block lcommon.Block,
	hash []byte,
	index uint32,
) lcommon.Transaction {
	txs := block.Transactions()
	if int(index) < len(txs) {
		tx := txs[index]
		if bytes.Equal(tx.Hash().Bytes(), hash) {
			return tx
		}
	}
	for _, tx := range txs {
		if bytes.Equal(tx.Hash().Bytes(), hash) {
			return tx
		}
	}
	return nil
}

func (a *NodeAdapter) decodedTransactionByHash(
	hash []byte,
) (*models.Transaction, models.Block, lcommon.Transaction, error) {
	tx, err := a.ledgerState.TransactionByHash(hash)
	if err != nil {
		return nil, models.Block{}, nil, fmt.Errorf(
			"get transaction by hash %x: %w",
			hash,
			err,
		)
	}
	if tx == nil {
		return nil, models.Block{}, nil, fmt.Errorf(
			"transaction %x: %w",
			hash,
			ErrTransactionNotFound,
		)
	}
	block, err := a.ledgerState.BlockByHash(tx.BlockHash)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, models.Block{}, nil, fmt.Errorf(
				"transaction %x block %x: %w",
				hash,
				tx.BlockHash,
				ErrTransactionNotFound,
			)
		}
		return nil, models.Block{}, nil, fmt.Errorf(
			"get block for transaction %x: %w",
			hash,
			err,
		)
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		return nil, models.Block{}, nil, fmt.Errorf(
			"decode block %x for transaction %x: %w",
			tx.BlockHash,
			hash,
			err,
		)
	}
	decodedTx := findTransactionInBlock(decodedBlock, hash, tx.BlockIndex)
	if decodedTx == nil {
		return nil, models.Block{}, nil, fmt.Errorf(
			"transaction %x not found in block %x: %w",
			hash,
			tx.BlockHash,
			ErrTransactionNotFound,
		)
	}
	return tx, block, decodedTx, nil
}

type transactionCertificate struct {
	Certificate lcommon.Certificate
	Index       int
}

type transactionRedeemerMetadata struct {
	DatumHash  *string
	ScriptHash string
}

func (a *NodeAdapter) transactionMetadataEntries(
	hash []byte,
) ([]labelcodec.Entry, error) {
	tx, err := a.ledgerState.TransactionByHash(hash)
	if err != nil {
		return nil, fmt.Errorf(
			"get transaction by hash %x: %w",
			hash,
			err,
		)
	}
	if tx == nil {
		return nil, fmt.Errorf(
			"transaction %x: %w",
			hash,
			ErrTransactionNotFound,
		)
	}
	if len(tx.Metadata) == 0 {
		return []labelcodec.Entry{}, nil
	}

	entries, err := labelcodec.EntriesFromCBOR(tx.Metadata)
	if err != nil {
		return nil, fmt.Errorf(
			"decode transaction metadata %x: %w",
			hash,
			err,
		)
	}
	return entries, nil
}

func transactionCertificatesByType(
	hash []byte,
	tx *models.Transaction,
	decodedTx lcommon.Transaction,
	certTypes ...lcommon.CertificateType,
) ([]transactionCertificate, error) {
	allowed := make(map[uint]struct{}, len(certTypes))
	for _, certType := range certTypes {
		allowed[uint(certType)] = struct{}{}
	}

	decodedCerts := decodedTx.Certificates()
	ret := []transactionCertificate{}
	for _, certRow := range tx.Certificates {
		if _, ok := allowed[certRow.CertType]; !ok {
			continue
		}
		certIndex := int(certRow.CertIndex)
		if certIndex >= len(decodedCerts) {
			return nil, fmt.Errorf(
				"transaction %x cert index %d out of range",
				hash,
				certIndex,
			)
		}
		ret = append(ret, transactionCertificate{
			Certificate: decodedCerts[certIndex],
			Index:       certIndex,
		})
	}
	slices.SortFunc(ret, func(a, b transactionCertificate) int {
		return cmp.Compare(a.Index, b.Index)
	})
	return ret, nil
}

func (a *NodeAdapter) transactionRedeemerMetadata(
	hash []byte,
	tx *models.Transaction,
	decodedTx lcommon.Transaction,
) (map[lcommon.RedeemerKey]transactionRedeemerMetadata, error) {
	ret := make(map[lcommon.RedeemerKey]transactionRedeemerMetadata)
	if len(tx.Redeemers) == 0 {
		return ret, nil
	}

	inputCbor, err := a.transactionInputCbor(tx.Inputs)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve transaction redeemer input CBOR for %x: %w",
			hash,
			err,
		)
	}
	inputsByRef := make(map[database.UtxoRef]models.Utxo, len(tx.Inputs))
	for _, input := range tx.Inputs {
		inputsByRef[utxoRef(input)] = input
	}

	resolvedInputs := make(map[string]lcommon.Utxo, len(decodedTx.Inputs()))
	for _, input := range decodedTx.Inputs() {
		ref := transactionInputRef(input)
		utxo, ok := inputsByRef[ref]
		if !ok {
			continue
		}
		utxo.Cbor = inputCbor[ref]
		if len(utxo.Cbor) == 0 {
			continue
		}
		output, err := gledger.NewTransactionOutputFromCbor(utxo.Cbor)
		if err != nil {
			return nil, fmt.Errorf(
				"decode redeemer input output %s for transaction %x: %w",
				input.String(),
				hash,
				err,
			)
		}
		resolvedInputs[input.String()] = lcommon.Utxo{
			Id:     input,
			Output: output,
		}
	}

	witnessDatums := map[lcommon.Blake2b256]*lcommon.Datum{}
	if witnesses := decodedTx.Witnesses(); witnesses != nil {
		for _, datum := range witnesses.PlutusData() {
			tmpDatum := datum
			witnessDatums[datum.Hash()] = &tmpDatum
		}
	}

	var mint lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
	if decodedTx.AssetMint() != nil {
		mint = *decodedTx.AssetMint()
	}

	for _, redeemer := range tx.Redeemers {
		key := lcommon.RedeemerKey{
			Tag:   lcommon.RedeemerTag(redeemer.Tag),
			Index: redeemer.Index,
		}
		purpose := gscript.BuildScriptPurpose(
			key,
			resolvedInputs,
			decodedTx.Inputs(),
			mint,
			decodedTx.Certificates(),
			decodedTx.Withdrawals(),
			decodedTx.VotingProcedures(),
			decodedTx.ProposalProcedures(),
			witnessDatums,
		)
		if purpose == nil {
			return nil, fmt.Errorf(
				"build script purpose for transaction %x tag=%d index=%d",
				hash,
				redeemer.Tag,
				redeemer.Index,
			)
		}
		metadata := transactionRedeemerMetadata{
			ScriptHash: hex.EncodeToString(purpose.ScriptHash().Bytes()),
		}
		if spending, ok := purpose.(gscript.ScriptPurposeSpending); ok {
			if datum := spending.Input.Output.Datum(); datum != nil {
				hash := datum.Hash()
				hashStr := hex.EncodeToString(hash.Bytes())
				metadata.DatumHash = &hashStr
			} else if datumHash := spending.Input.Output.DatumHash(); datumHash != nil {
				hashStr := hex.EncodeToString(datumHash.Bytes())
				metadata.DatumHash = &hashStr
			}
		}
		ret[key] = metadata
	}
	return ret, nil
}

func (a *NodeAdapter) transactionInputInfoFromUtxo(
	utxo models.Utxo,
	collateral bool,
	reference *bool,
) (TransactionInputInfo, error) {
	addr, err := a.addressFromUtxo(utxo)
	if err != nil {
		return TransactionInputInfo{}, err
	}
	return TransactionInputInfo{
		Address:             addr,
		Amount:              addressAmountsFromUtxo(utxo),
		TxHash:              hex.EncodeToString(utxo.TxId),
		OutputIndex:         utxo.OutputIdx,
		DataHash:            optionalHexString(utxo.DatumHash),
		Collateral:          collateral,
		InlineDatum:         optionalHexString(utxo.Datum),
		ReferenceScriptHash: nil,
		Reference:           reference,
	}, nil
}

func compareUtxoRefs(a, b models.Utxo) int {
	if ret := bytes.Compare(a.TxId, b.TxId); ret != 0 {
		return ret
	}
	return cmp.Compare(a.OutputIdx, b.OutputIdx)
}

func (a *NodeAdapter) addressFromUtxo(
	utxo models.Utxo,
) (string, error) {
	if len(utxo.Cbor) > 0 {
		output, err := utxo.Decode()
		if err == nil {
			return output.Address().String(), nil
		}
	}

	networkID := a.networkID()
	switch {
	case len(utxo.PaymentKey) == lcommon.AddressHashSize &&
		len(utxo.StakingKey) == lcommon.AddressHashSize:
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			networkID,
			utxo.PaymentKey,
			utxo.StakingKey,
		)
		if err == nil {
			return addr.String(), nil
		}
	case len(utxo.PaymentKey) == lcommon.AddressHashSize:
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			networkID,
			utxo.PaymentKey,
			nil,
		)
		if err == nil {
			return addr.String(), nil
		}
	}
	return "", fmt.Errorf("address not resolvable for utxo %x:%d: CBOR unavailable and no payment key hash stored", utxo.TxId, utxo.OutputIdx)
}

func (a *NodeAdapter) networkID() uint8 {
	cfg := a.ledgerState.CardanoNodeConfig()
	if cfg == nil || cfg.ShelleyGenesis() == nil {
		return lcommon.AddressNetworkTestnet
	}
	if cfg.ShelleyGenesis().NetworkId == "Mainnet" {
		return lcommon.AddressNetworkMainnet
	}
	return lcommon.AddressNetworkTestnet
}

func stakeAddressFromCredential(
	credential lcommon.Credential,
	networkID uint8,
) (string, error) {
	addressType := uint8(lcommon.AddressTypeNoneKey)
	if credential.CredType == lcommon.CredentialTypeScriptHash {
		addressType = lcommon.AddressTypeNoneScript
	}
	addr, err := lcommon.NewAddressFromParts(
		addressType,
		networkID,
		nil,
		credential.Credential.Bytes(),
	)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

func poolRewardAccountCredential(
	cert *lcommon.PoolRegistrationCertificate,
) lcommon.Credential {
	credType := uint(lcommon.CredentialTypeAddrKeyHash)
	hash := cert.RewardAccount[:]
	rawCbor := cert.Cbor()
	if len(rawCbor) > 0 {
		var raw []cbor.RawMessage
		if _, err := cbor.Decode(rawCbor, &raw); err == nil && len(raw) > 6 {
			var rewardAddrBytes []byte
			if _, err := cbor.Decode(raw[6], &rewardAddrBytes); err == nil &&
				len(rewardAddrBytes) == 29 {
				if (rewardAddrBytes[0] & 0xF0) == 0xF0 {
					credType = lcommon.CredentialTypeScriptHash
				}
				hash = rewardAddrBytes[1:]
			}
		}
	}
	return lcommon.Credential{
		CredType:   credType,
		Credential: lcommon.CredentialHash(lcommon.NewBlake2b224(hash)),
	}
}

func redeemerPurpose(tag lcommon.RedeemerTag) string {
	switch tag {
	case lcommon.RedeemerTagSpend:
		return "spend"
	case lcommon.RedeemerTagMint:
		return "mint"
	case lcommon.RedeemerTagCert:
		return "cert"
	case lcommon.RedeemerTagReward:
		return "reward"
	case lcommon.RedeemerTagVoting:
		return "voting"
	case lcommon.RedeemerTagProposing:
		return "proposing"
	case lcommon.RedeemerTagGuarding:
		return "guarding"
	default:
		return ""
	}
}

func executionCostsFromPParams(
	pparams lcommon.ProtocolParameters,
) (lcommon.ExUnitPrice, error) {
	switch pp := pparams.(type) {
	case *alonzo.AlonzoProtocolParameters:
		return pp.ExecutionCosts, nil
	case *babbage.BabbageProtocolParameters:
		return pp.ExecutionCosts, nil
	case *conway.ConwayProtocolParameters:
		return pp.ExecutionCosts, nil
	case *dijkstra.DijkstraProtocolParameters:
		return pp.ExecutionCosts, nil
	default:
		return lcommon.ExUnitPrice{}, fmt.Errorf(
			"protocol parameters %T do not include execution prices",
			pparams,
		)
	}
}

func redeemerExecutionFee(
	executionCosts lcommon.ExUnitPrice,
	memory uint64,
	steps uint64,
) uint64 {
	if executionCosts.MemPrice == nil || executionCosts.StepPrice == nil {
		return 0
	}
	memCost := new(big.Rat).Mul(
		executionCosts.MemPrice.ToBigRat(),
		new(big.Rat).SetUint64(memory),
	)
	stepCost := new(big.Rat).Mul(
		executionCosts.StepPrice.ToBigRat(),
		new(big.Rat).SetUint64(steps),
	)
	return ceilRatToUint64(new(big.Rat).Add(memCost, stepCost))
}

func ceilRatToUint64(v *big.Rat) uint64 {
	if v == nil || v.Sign() <= 0 {
		return 0
	}
	num := new(big.Int).Set(v.Num())
	den := new(big.Int).Set(v.Denom())
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsUint64() {
		return math.MaxUint64
	}
	return q.Uint64()
}

func transactionPoolRelaysInfo(
	relays []lcommon.PoolRelay,
) []TransactionPoolRelayInfo {
	ret := make([]TransactionPoolRelayInfo, 0, len(relays))
	for _, relay := range relays {
		info := TransactionPoolRelayInfo{}
		if relay.Port != nil {
			port := int(*relay.Port)
			info.Port = &port
		}
		if relay.Ipv4 != nil {
			info.IPv4 = relay.Ipv4.String()
		}
		if relay.Ipv6 != nil {
			info.IPv6 = relay.Ipv6.String()
		}
		if relay.Hostname != nil {
			if relay.Type == lcommon.PoolRelayTypeMultiHostName {
				info.DNSSrv = *relay.Hostname
			} else {
				info.DNS = *relay.Hostname
			}
		}
		ret = append(ret, info)
	}
	return ret
}

type certificateCounts struct {
	mirCerts    int
	delegations int
	stakeCerts  int
	poolUpdates int
	poolRetires int
}

func transactionCertificateCounts(
	certs []models.Certificate,
) certificateCounts {
	var ret certificateCounts
	for _, cert := range certs {
		certType := cert.CertType
		if certType == uint(lcommon.CertificateTypeMoveInstantaneousRewards) {
			ret.mirCerts++
		}
		switch certType {
		case uint(lcommon.CertificateTypeStakeDelegation),
			uint(lcommon.CertificateTypeStakeRegistrationDelegation),
			uint(lcommon.CertificateTypeStakeVoteDelegation),
			uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation):
			ret.delegations++
		}
		switch certType {
		case uint(lcommon.CertificateTypeStakeRegistration),
			uint(lcommon.CertificateTypeStakeDeregistration),
			uint(lcommon.CertificateTypeRegistration),
			uint(lcommon.CertificateTypeDeregistration),
			uint(lcommon.CertificateTypeStakeRegistrationDelegation),
			uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
			uint(lcommon.CertificateTypeVoteRegistrationDelegation):
			ret.stakeCerts++
		}
		switch certType {
		case uint(lcommon.CertificateTypePoolRegistration):
			ret.poolUpdates++
		case uint(lcommon.CertificateTypePoolRetirement):
			ret.poolRetires++
		}
	}
	return ret
}

func aggregateTransactionOutputAmount(
	outputs []models.Utxo,
) []AddressAmountInfo {
	amounts := map[string]uint64{}
	for _, output := range outputs {
		amounts["lovelace"] += uint64(output.Amount)
		for _, asset := range output.Assets {
			unit := hex.EncodeToString(asset.PolicyId) +
				hex.EncodeToString(asset.Name)
			amounts[unit] += uint64(asset.Amount)
		}
	}
	ret := make([]AddressAmountInfo, 0, len(amounts))
	if lovelace, ok := amounts["lovelace"]; ok {
		ret = append(ret, AddressAmountInfo{
			Unit:     "lovelace",
			Quantity: strconv.FormatUint(lovelace, 10),
		})
		delete(amounts, "lovelace")
	}
	keys := make([]string, 0, len(amounts))
	for unit := range amounts {
		keys = append(keys, unit)
	}
	slices.Sort(keys)
	for _, unit := range keys {
		ret = append(ret, AddressAmountInfo{
			Unit:     unit,
			Quantity: strconv.FormatUint(amounts[unit], 10),
		})
	}
	return ret
}

func (a *NodeAdapter) transactionDeposit(
	tx lcommon.Transaction,
	blockEraID uint,
	slot uint64,
) (uint64, error) {
	blockEra := eras.GetEraById(blockEraID)
	if blockEra == nil || blockEra.CertDepositFunc == nil {
		return 0, nil
	}
	pparams, err := a.protocolParamsForSlot(slot)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, cert := range tx.Certificates() {
		deposit, err := blockEra.CertDepositFunc(cert, pparams)
		if err != nil {
			if errors.Is(err, eras.ErrIncompatibleProtocolParams) {
				return 0, nil
			}
			return 0, err
		}
		total += deposit
	}
	return total, nil
}

func (a *NodeAdapter) protocolParamsForSlot(
	slot uint64,
) (lcommon.ProtocolParameters, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(false)
	defer txn.Release()
	epoch, err := db.GetEpochBySlot(slot, txn)
	if err != nil {
		return nil, err
	}
	if epoch == nil {
		pparams := a.ledgerState.GetCurrentPParams()
		if pparams == nil {
			return nil, errors.New("protocol parameters not available")
		}
		return pparams, nil
	}
	era := eras.GetEraById(epoch.EraId)
	if era == nil {
		return nil, fmt.Errorf("unknown era ID %d", epoch.EraId)
	}
	pparams, err := db.GetPParams(
		epoch.EpochId,
		era.Id,
		era.DecodePParamsFunc,
		txn,
	)
	if err != nil {
		return nil, err
	}
	if pparams == nil {
		return nil, errors.New("decoded protocol parameters are nil")
	}
	return pparams, nil
}

func (a *NodeAdapter) transactionBlockTime(
	tx models.Transaction,
) (int64, error) {
	blockTime, err := a.ledgerState.SlotToTime(tx.Slot)
	if err != nil {
		return 0, fmt.Errorf(
			"convert slot %d to block time for transaction %x: %w",
			tx.Slot,
			tx.Hash,
			err,
		)
	}
	return blockTime.Unix(), nil
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

func bigIntString(v *big.Int) string {
	if v == nil {
		return "0"
	}
	return v.String()
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

func paginationRange(
	total int,
	params PaginationParams,
) (int, int) {
	if total <= 0 || params.Count <= 0 || params.Page <= 0 {
		return total, total
	}
	if params.Page-1 > (math.MaxInt / params.Count) {
		return total, total
	}
	start := (params.Page - 1) * params.Count
	if start >= total {
		return total, total
	}
	end := min(start+params.Count, total)
	return start, end
}
