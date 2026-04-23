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
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

var (
	ErrInvalidAddress = errors.New("invalid address")
	ErrEpochNotFound  = errors.New("epoch not found")
	ErrAssetNotFound  = errors.New("asset not found")
)

// NodeAdapter wraps a real dingo Node's LedgerState to
// implement the BlockfrostNode interface.
type NodeAdapter struct {
	ledgerState *ledger.LedgerState
}

// NewNodeAdapter creates a NodeAdapter that queries the
// given LedgerState for blockchain data.
func NewNodeAdapter(
	ls *ledger.LedgerState,
) (*NodeAdapter, error) {
	if ls == nil {
		return nil, errors.New(
			"new node adapter: ledger state must not be nil",
		)
	}
	return &NodeAdapter{ledgerState: ls}, nil
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
	block, decodedBlock, err := a.latestBlockData(
		tip.Point.Hash,
	)
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
	pparamRows, err := a.ledgerState.Database().Metadata().GetPParams(
		epoch,
		nil,
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
	era := eras.GetEraById(pparamRow.EraId)
	if era == nil {
		return ProtocolParamsInfo{}, fmt.Errorf(
			"get protocol parameters for epoch %d: unknown era ID %d",
			epoch,
			pparamRow.EraId,
		)
	}
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
