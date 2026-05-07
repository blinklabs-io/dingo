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
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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
	ErrTransactionNotFound = errors.New("transaction not found")
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
	}, nil
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
	// TODO: Wire locked supply from script-address UTxOs and ledger deposits,
	// then subtract it from circulating supply.
	locked := uint64(0)
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

// DRep returns governance DRep information for the requested
// credential.
func (a *NodeAdapter) DRep(
	credential DRepCredential,
) (DRepInfo, error) {
	db := a.ledgerState.Database()
	drep, err := db.GetDrep(credential.Hash, true, nil)
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
	power, err := db.GetDRepVotingPower(credential.Hash, nil)
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
		HasScript:   credential.HasScript,
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
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs)
	case *babbage.BabbageProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor)
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs)
	case *conway.ConwayProtocolParameters:
		fillBasePParamsInfo(&info, pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize, pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch, pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolVersion.Major, pp.ProtocolVersion.Minor)
		fillAlonzoPParamsInfo(&info, pp.MinPoolCost, pp.AdaPerUtxoByte, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits, pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs)
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
	if tx.CollateralReturn != nil {
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
			"determine transaction type: %w: %w",
			err,
			ErrInvalidTransaction,
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
		return "", fmt.Errorf("submit transaction to mempool: %w", err)
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
	if !tx.Valid && tx.CollateralReturn != nil {
		txOutputs = []models.Utxo{*tx.CollateralReturn}
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
		inputs = append(inputs, a.transactionInputInfoFromUtxo(input, false, nil))
	}
	for _, input := range txCollateral {
		input.Cbor = inputCbor[utxoRef(input)]
		inputs = append(inputs, a.transactionInputInfoFromUtxo(input, true, nil))
	}
	referenceInput := true
	for _, input := range txReferenceInputs {
		input.Cbor = inputCbor[utxoRef(input)]
		inputs = append(inputs, a.transactionInputInfoFromUtxo(input, false, &referenceInput))
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

		rewardAccount, err := stakeAddressFromCredential(
			lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(c.RewardAccount),
			},
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

	redeemers := slices.Clone(tx.Redeemers)
	slices.SortFunc(redeemers, func(a, b models.Redeemer) int {
		if c := cmp.Compare(a.Tag, b.Tag); c != 0 {
			return c
		}
		return cmp.Compare(a.Index, b.Index)
	})

	ret := make([]TransactionRedeemerInfo, 0, len(redeemers))
	for _, redeemer := range redeemers {
		dataHash := lcommon.Blake2b256Hash(redeemer.Data)
		ret = append(ret, TransactionRedeemerInfo{
			TxIndex: int(redeemer.Index),
			Purpose: redeemerPurpose(lcommon.RedeemerTag(redeemer.Tag)),
			// TODO: Populate script_hash once redeemer-to-script mapping is stored.
			ScriptHash:       "",
			RedeemerDataHash: hex.EncodeToString(dataHash.Bytes()),
			UnitMem:          strconv.FormatUint(redeemer.ExUnitsMemory, 10),
			UnitSteps:        strconv.FormatUint(redeemer.ExUnitsCPU, 10),
			Fee:              "0",
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

func (a *NodeAdapter) transactionInputInfoFromUtxo(
	utxo models.Utxo,
	collateral bool,
	reference *bool,
) TransactionInputInfo {
	return TransactionInputInfo{
		Address:             a.addressFromUtxo(utxo),
		Amount:              addressAmountsFromUtxo(utxo),
		TxHash:              hex.EncodeToString(utxo.TxId),
		OutputIndex:         utxo.OutputIdx,
		DataHash:            optionalHexString(utxo.DatumHash),
		Collateral:          collateral,
		InlineDatum:         optionalHexString(utxo.Datum),
		ReferenceScriptHash: nil,
		Reference:           reference,
	}
}

func compareUtxoRefs(a, b models.Utxo) int {
	if ret := bytes.Compare(a.TxId, b.TxId); ret != 0 {
		return ret
	}
	return cmp.Compare(a.OutputIdx, b.OutputIdx)
}

func (a *NodeAdapter) addressFromUtxo(
	utxo models.Utxo,
) string {
	if len(utxo.Cbor) > 0 {
		output, err := utxo.Decode()
		if err == nil {
			return output.Address().String()
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
			return addr.String()
		}
	case len(utxo.PaymentKey) == lcommon.AddressHashSize:
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			networkID,
			utxo.PaymentKey,
			nil,
		)
		if err == nil {
			return addr.String()
		}
	}
	// Script payment addresses are not stored in the DB model; returns "".
	return ""
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
	default:
		return ""
	}
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
