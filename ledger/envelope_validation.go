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

package ledger

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

type envelopeParent struct {
	slot        uint64
	blockNumber uint64
	origin      bool
	byronEbb    bool
	// eraId is meaningful only when eraKnown: a persisted tip whose block type
	// was not loaded, or is not recognised, leaves the era rule unchecked.
	eraId    uint8
	eraKnown bool
	// byronEpoch and byronSlot are a Byron parent's raw epoch and slot within
	// it, set when the parent is a decoded Byron block or a persisted tip
	// positioned by withStoredByronPosition. Otherwise only the stored slot is
	// known, numbered with gouroboros' fixed epoch length; see
	// byronParentPosition.
	byronEpoch      uint64
	byronSlot       uint64
	byronPositioned bool
}

// envelopeParentFromTip reconstructs the envelope metadata for a persisted
// chain tip. A Tip contains only the point and block number, so callers must
// provide the stored block type to preserve the Byron EBB exception across
// ledger-processing batches.
func envelopeParentFromTip(
	slot uint64,
	blockNumber uint64,
	hash []byte,
	blockType uint,
	blockTypeLoaded bool,
) envelopeParent {
	origin := len(hash) == 0
	parent := envelopeParent{
		slot:        slot,
		blockNumber: blockNumber,
		origin:      origin,
		byronEbb: !origin && blockTypeLoaded &&
			blockType == uint(gledger.BlockTypeByronEbb),
	}
	if !origin && blockTypeLoaded {
		parent.eraId, parent.eraKnown = chain.EraIdForBlockType(blockType)
	}
	return parent
}

// withStoredByronPosition sets a persisted Byron tip's epoch and slot within
// it from the stored block. The stored slot is numbered with gouroboros'
// fixed epoch length and cannot be split back once the configured epoch is
// longer, so the header's own fields are the only exact source. A parent that
// is not a Byron block, or is already positioned, is returned unchanged.
func (p envelopeParent) withStoredByronPosition(
	stored models.Block,
) (envelopeParent, error) {
	if p.byronPositioned || !p.eraKnown || p.eraId != byron.EraIdByron {
		return p, nil
	}
	decoded, err := stored.Decode()
	if err != nil {
		return p, fmt.Errorf("decode stored Byron parent: %w", err)
	}
	if decoded.SlotNumber() != p.slot {
		return p, fmt.Errorf(
			"stored Byron parent decodes to slot %d, not tip slot %d",
			decoded.SlotNumber(),
			p.slot,
		)
	}
	p.byronEpoch, p.byronSlot, p.byronPositioned = byronBlockPosition(decoded)
	return p, nil
}

func envelopeParentFromBlock(block gledger.Block) envelopeParent {
	_, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	parent := envelopeParent{
		slot:        block.SlotNumber(),
		blockNumber: block.BlockNumber(),
		byronEbb:    isEbb,
		eraId:       block.Era().Id,
		eraKnown:    true,
	}
	epoch, slot, positioned := byronBlockPosition(block)
	parent.byronEpoch = epoch
	parent.byronSlot = slot
	parent.byronPositioned = positioned
	return parent
}

// byronBlockPosition returns a Byron block's epoch and its slot within that
// epoch, which are the header's own fields and do not depend on the epoch
// length. An epoch boundary block sits at slot 0 of its epoch.
func byronBlockPosition(block gledger.Block) (epoch, slot uint64, ok bool) {
	switch header := block.Header().(type) {
	case *byron.ByronMainBlockHeader:
		if header != nil {
			return header.ConsensusData.SlotId.Epoch,
				header.ConsensusData.SlotId.Slot,
				true
		}
	case *byron.ByronEpochBoundaryBlockHeader:
		if header != nil {
			return header.ConsensusData.Epoch, 0, true
		}
	}
	return 0, 0, false
}

// byronEpochSlots returns the configured Byron epoch length, 10 * k. Without
// a Byron genesis it returns gouroboros' fixed mainnet length.
func byronEpochSlots(config *cardano.CardanoNodeConfig) uint64 {
	if config == nil || config.ByronGenesis() == nil {
		return byron.ByronSlotsPerEpoch
	}
	k := config.ByronGenesis().ProtocolConsts.K
	if k <= 0 {
		return byron.ByronSlotsPerEpoch
	}
	return uint64(k) * 10
}

// byronParentPosition returns the parent's epoch and slot within it. A parent
// rebuilt from a persisted tip only has the slot the chain stored, which is
// epoch * ByronSlotsPerEpoch + slot; that splits back into the two fields
// exactly when the slot within the epoch fits both lengths, and not at all
// once the configured epoch is longer than the stored one.
func byronParentPosition(
	parent envelopeParent,
	epochSlots uint64,
) (epoch, slot uint64, ok bool) {
	if parent.byronPositioned {
		return parent.byronEpoch, parent.byronSlot, true
	}
	if !parent.eraKnown || parent.eraId != byron.EraIdByron ||
		epochSlots > byron.ByronSlotsPerEpoch {
		return 0, 0, false
	}
	epoch = parent.slot / byron.ByronSlotsPerEpoch
	slot = parent.slot % byron.ByronSlotsPerEpoch
	if slot >= epochSlots {
		return 0, 0, false
	}
	return epoch, slot, true
}

// byronOrderSlots numbers a Byron block and its Byron parent with the
// configured epoch length. ok is false when either cannot be numbered that
// way, and the caller then compares the slots the chain stored.
func byronOrderSlots(
	block gledger.Block,
	parent envelopeParent,
	epochSlots uint64,
) (blockSlot, parentSlot uint64, ok bool, err error) {
	blockEpoch, blockInEpoch, blockOk := byronBlockPosition(block)
	if !blockOk {
		return 0, 0, false, nil
	}
	parentEpoch, parentInEpoch, parentOk := byronParentPosition(
		parent,
		epochSlots,
	)
	if !parentOk {
		return 0, 0, false, nil
	}
	blockSlot, err = byron.SlotNumberFromEpochAndSlot(
		blockEpoch, blockInEpoch, epochSlots,
	)
	if err != nil {
		return 0, 0, false, fmt.Errorf(
			"byron block epoch %d slot %d: %w",
			blockEpoch, blockInEpoch, err,
		)
	}
	parentSlot, err = byron.SlotNumberFromEpochAndSlot(
		parentEpoch, parentInEpoch, epochSlots,
	)
	if err != nil {
		return 0, 0, false, fmt.Errorf(
			"byron parent epoch %d slot %d: %w",
			parentEpoch, parentInEpoch, err,
		)
	}
	return blockSlot, parentSlot, true, nil
}

// validateInboundBlockEnvelope runs the consensus envelope checks that must
// pass before header crypto and body validation accept an inbound block.
func validateInboundBlockEnvelope(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
	nodeConfig *cardano.CardanoNodeConfig,
	parent envelopeParent,
) error {
	if block == nil {
		return errors.New("validate inbound block envelope: nil block")
	}
	epochSlots := byronEpochSlots(nodeConfig)
	if err := validateByronEbbPlacement(block, epochSlots); err != nil {
		return err
	}
	if isNilBlockHeader(block.Header()) {
		return errors.New("validate inbound block envelope: nil block header")
	}
	if err := validateBlockOrder(block, parent, epochSlots); err != nil {
		return err
	}
	if block.Era().Id == byron.EraIdByron {
		// Byron does not carry the Shelley-style body-size field, but a main
		// block's header carries a separate proof over every body payload.
		// Verify it before admitting the block so a genuine header cannot be
		// paired with a substituted body. The reference decodes an EBB's
		// body proof as a byte string and discards it, so an EBB body is not
		// bound to its header.
		// Decoded inbound blocks preserve their complete CBOR. Synthetic
		// blocks used by callers that do not carry wire bytes cannot provide a
		// body proof to verify and are handled by the normal structural path.
		if len(block.Cbor()) == 0 {
			return nil
		}
		switch byronBlock := block.(type) {
		case *byron.ByronMainBlock:
			if err := byronBlock.ValidateBodyProof(); err != nil {
				return fmt.Errorf("validate Byron main block body proof: %w", err)
			}
		case *byron.ByronEpochBoundaryBlock:
			if err := byronBlock.ValidateBodyProof(); err != nil {
				return fmt.Errorf("validate Byron epoch boundary body proof: %w", err)
			}
			return validateByronEbbSize(byronBlock)
		default:
			return nil
		}
		return validateByronBlockSizes(block, pparams, nodeConfig)
	}
	if err := validateBlockSizes(block, pparams); err != nil {
		return err
	}
	return validateBlockExUnits(block, pparams)
}

// validateBlockExUnits enforces the aggregate execution-unit budget for all
// transactions in an inbound block. Per-transaction validation checks
// MaxTxExUnits, but the protocol also bounds the sum at MaxBlockExUnits.
// This runs before ledger deltas are created or applied.
func validateBlockExUnits(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
) error {
	limits, ok := protocolBlockLimits(pparams)
	if !ok {
		// Byron through Mary have no Plutus execution-unit budget.
		return nil
	}
	if !limits.hasMaxBlockExUnits {
		return nil
	}
	var total lcommon.ExUnits
	for index, tx := range block.Transactions() {
		declared, err := eras.DeclaredExUnits(tx)
		if err != nil {
			return fmt.Errorf(
				"transaction %d declared execution units: %w",
				index,
				err,
			)
		}
		total, err = eras.SafeAddExUnits(total, declared)
		if err != nil {
			return fmt.Errorf("block declared execution units: %w", err)
		}
		if total.Memory > limits.maxBlockExUnits.Memory ||
			total.Steps > limits.maxBlockExUnits.Steps {
			return fmt.Errorf(
				"block declared execution units %d memory/%d steps exceed maxBlockExUnits %d memory/%d steps",
				total.Memory,
				total.Steps,
				limits.maxBlockExUnits.Memory,
				limits.maxBlockExUnits.Steps,
			)
		}
	}
	return nil
}

func isNilBlockHeader(header lcommon.BlockHeader) bool {
	if header == nil {
		return true
	}
	value := reflect.ValueOf(header)
	kind := value.Kind()
	if kind == reflect.Chan ||
		kind == reflect.Func ||
		kind == reflect.Interface ||
		kind == reflect.Map ||
		kind == reflect.Pointer ||
		kind == reflect.Slice {
		return value.IsNil()
	}
	return false
}

// validateBlockOrder checks that a block follows its parent by block number
// and slot. Byron's envelope rules are asymmetric around epoch boundary blocks
// (the expectedNextBlockNo and minimumNextSlotNo tables of the Byron
// consensus envelope):
//
//	parent   block    block number   slot
//	regular  regular  parent + 1     later
//	regular  EBB      parent         later
//	EBB      regular  parent + 1     same or later
//	EBB      EBB      parent + 1     later
//
// A regular block may share an EBB parent's slot only within Byron. Between
// two Byron blocks the slots are compared as the configured epoch length
// numbers them, so epochSlots is the Byron epoch length, 10 * k.
func validateBlockOrder(
	block gledger.Block,
	parent envelopeParent,
	epochSlots uint64,
) error {
	if parent.origin {
		return nil
	}
	if parent.eraKnown {
		if err := chain.CheckEraOrder(
			block.Era().Id,
			parent.eraId,
		); err != nil {
			return fmt.Errorf(
				"block at slot %d: %w",
				block.SlotNumber(),
				err,
			)
		}
	}
	_, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	expectedBlockNumber := parent.blockNumber + 1
	if isEbb && !parent.byronEbb {
		expectedBlockNumber = parent.blockNumber
	}
	if block.BlockNumber() != expectedBlockNumber {
		if isEbb {
			return fmt.Errorf(
				"byron EBB block number %d does not match expected block number %d",
				block.BlockNumber(),
				expectedBlockNumber,
			)
		}
		return fmt.Errorf(
			"block number %d does not follow parent block number %d",
			block.BlockNumber(),
			parent.blockNumber,
		)
	}
	blockSlot, parentSlot := block.SlotNumber(), parent.slot
	if block.Era().Id == byron.EraIdByron {
		byronBlockSlot, byronParentSlot, ok, err := byronOrderSlots(
			block, parent, epochSlots,
		)
		if err != nil {
			return err
		}
		if ok {
			blockSlot, parentSlot = byronBlockSlot, byronParentSlot
		}
	}
	if !isEbb && parent.byronEbb &&
		block.Era().Id == byron.EraIdByron &&
		blockSlot == parentSlot {
		return nil
	}
	if blockSlot <= parentSlot {
		if isEbb {
			return fmt.Errorf(
				"byron EBB slot %d does not follow parent slot %d",
				blockSlot,
				parentSlot,
			)
		}
		return fmt.Errorf(
			"block slot %d does not follow parent slot %d",
			blockSlot,
			parentSlot,
		)
	}
	return nil
}

// validateByronEbbPlacement rejects Byron epoch boundary blocks outside their
// declared epoch boundary slot, while ignoring non-EBB blocks. The boundary
// slot is the EBB's epoch times the configured epoch length, so epochSlots is
// the Byron epoch length, 10 * k.
func validateByronEbbPlacement(
	block gledger.Block,
	epochSlots uint64,
) error {
	ebb, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	if !isEbb {
		return nil
	}
	if ebb.BlockHeader == nil {
		return errors.New("byron EBB has nil header")
	}
	slot, err := ebb.BlockHeader.SlotNumberWithEpochLength(epochSlots)
	if err != nil {
		return fmt.Errorf(
			"byron EBB epoch %d: %w",
			ebb.BlockHeader.ConsensusData.Epoch,
			err,
		)
	}
	if slot%epochSlots != 0 {
		return fmt.Errorf(
			"byron EBB slot %d is not an epoch boundary slot",
			slot,
		)
	}
	if slot/epochSlots != ebb.BlockHeader.ConsensusData.Epoch {
		return fmt.Errorf(
			"byron EBB slot %d does not match epoch %d boundary slot",
			slot,
			ebb.BlockHeader.ConsensusData.Epoch,
		)
	}
	return nil
}

// byronMaxEbbSize is the reference updateChainBoundary bound on an epoch
// boundary block's whole encoding. It replaces maxBlockSize and
// maxHeaderSize for EBBs rather than adding to them.
const byronMaxEbbSize = 2_000_000

func validateByronEbbSize(block *byron.ByronEpochBoundaryBlock) error {
	if size := len(block.Cbor()); size > byronMaxEbbSize {
		return fmt.Errorf(
			"byron epoch boundary block size %d exceeds fixed limit %d",
			size,
			byronMaxEbbSize,
		)
	}
	return nil
}

// validateBlockSizes enforces maxBlockHeaderSize and maxBlockBodySize for
// Shelley-and-later inbound blocks using protocol parameter limits.
func validateBlockSizes(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
) error {
	limits, ok := protocolBlockLimits(pparams)
	if !ok {
		return fmt.Errorf(
			"block size validation unsupported for protocol parameters %T",
			pparams,
		)
	}
	headerCbor := block.Header().Cbor()
	if uint64(len(headerCbor)) > limits.maxHeaderSize {
		return fmt.Errorf(
			"block header size %d exceeds maxBlockHeaderSize %d",
			len(headerCbor),
			limits.maxHeaderSize,
		)
	}
	actualBodySize, err := serializedBlockBodySize(block)
	if err != nil {
		return err
	}
	declaredBodySize := block.BlockBodySize()
	if declaredBodySize != actualBodySize {
		return fmt.Errorf(
			"block body size mismatch: header declares %d, actual size is %d",
			declaredBodySize,
			actualBodySize,
		)
	}
	if actualBodySize > limits.maxBodySize {
		return fmt.Errorf(
			"block body size %d exceeds maxBlockBodySize %d",
			actualBodySize,
			limits.maxBodySize,
		)
	}
	return nil
}

// validateByronBlockSizes enforces the Byron block and header size limits.
// A main block is measured against ppMaxBlockSize and ppMaxHeaderSize as
// adopted for its epoch when pparams carries them; genesis only initializes
// those parameters. Epoch boundary blocks, and callers with no adopted
// parameters, use the genesis limits. A main block whose adopted parameters
// are unknown (eras.ByronProtocolParameters.AdoptionUnknown) is not measured.
func validateByronBlockSizes(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
	config *cardano.CardanoNodeConfig,
) error {
	if _, isMain := block.(*byron.ByronMainBlock); isMain {
		if adopted, ok := pparams.(*eras.ByronProtocolParameters); ok &&
			adopted != nil && adopted.AdoptionUnknown {
			return nil
		}
	}
	maxBlockSize, maxHeaderSize, err := byronBlockSizeLimits(
		block, pparams, config,
	)
	if err != nil {
		return err
	}
	headerSize := new(big.Int).SetInt64(int64(len(block.Header().Cbor())))
	if headerSize.Cmp(maxHeaderSize) > 0 {
		return fmt.Errorf(
			"byron block header size %s exceeds maxHeaderSize %s",
			headerSize, maxHeaderSize,
		)
	}
	blockSize := new(big.Int).SetInt64(int64(len(block.Cbor())))
	if blockSize.Cmp(maxBlockSize) > 0 {
		return fmt.Errorf(
			"byron block size %s exceeds maxBlockSize %s",
			blockSize, maxBlockSize,
		)
	}
	return nil
}

func byronBlockSizeLimits(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
	config *cardano.CardanoNodeConfig,
) (maxBlockSize, maxHeaderSize *big.Int, err error) {
	if _, isMain := block.(*byron.ByronMainBlock); isMain {
		if adopted, ok := pparams.(*eras.ByronProtocolParameters); ok &&
			adopted != nil && adopted.MaxBlockSize != nil &&
			adopted.MaxHeaderSize != nil {
			return adopted.MaxBlockSize, adopted.MaxHeaderSize, nil
		}
	}
	if config == nil || config.ByronGenesis() == nil {
		return nil, nil, errors.New(
			"byron genesis is required for block size validation",
		)
	}
	version := config.ByronGenesis().BlockVersionData
	if version.MaxBlockSize <= 0 || version.MaxHeaderSize <= 0 {
		return nil, nil, errors.New(
			"byron genesis has invalid block size limits",
		)
	}
	return big.NewInt(int64(version.MaxBlockSize)),
		big.NewInt(int64(version.MaxHeaderSize)),
		nil
}

// serializedBlockBodySize measures the serialized body portion of block CBOR
// using the same era-specific field layout that the header declaration covers.
func serializedBlockBodySize(block gledger.Block) (uint64, error) {
	blockCbor := block.Cbor()
	if len(blockCbor) == 0 {
		return 0, fmt.Errorf(
			"block at slot %d has no CBOR for body size validation",
			block.SlotNumber(),
		)
	}
	var fields []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &fields); err != nil {
		return 0, fmt.Errorf(
			"decode block CBOR for body size validation: %w",
			err,
		)
	}
	if len(fields) < 2 {
		return 0, fmt.Errorf(
			"block CBOR has %d fields, expected at least header and body",
			len(fields),
		)
	}
	if block.Era().Id == dijkstra.EraIdDijkstra {
		return uint64(len(fields[1])), nil
	}
	var size uint64
	for _, field := range fields[1:] {
		size += uint64(len(field))
	}
	return size, nil
}

type blockProtocolLimits struct {
	maxBodySize        uint64
	maxHeaderSize      uint64
	maxBlockExUnits    lcommon.ExUnits
	hasMaxBlockExUnits bool
}

// protocolBlockLimits extracts the inbound block limits for a protocol era.
// Keeping the era mapping here ensures size and execution-unit validation use
// the same protocol-parameter type coverage.
func protocolBlockLimits(
	pparams lcommon.ProtocolParameters,
) (blockProtocolLimits, bool) {
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:   uint64(pp.MaxBlockBodySize),
			maxHeaderSize: uint64(pp.MaxBlockHeaderSize),
		}, true
	case *mary.MaryProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:   uint64(pp.MaxBlockBodySize),
			maxHeaderSize: uint64(pp.MaxBlockHeaderSize),
		}, true
	case *alonzo.AlonzoProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:        uint64(pp.MaxBlockBodySize),
			maxHeaderSize:      uint64(pp.MaxBlockHeaderSize),
			maxBlockExUnits:    pp.MaxBlockExUnits,
			hasMaxBlockExUnits: true,
		}, true
	case *babbage.BabbageProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:        uint64(pp.MaxBlockBodySize),
			maxHeaderSize:      uint64(pp.MaxBlockHeaderSize),
			maxBlockExUnits:    pp.MaxBlockExUnits,
			hasMaxBlockExUnits: true,
		}, true
	case *conway.ConwayProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:        uint64(pp.MaxBlockBodySize),
			maxHeaderSize:      uint64(pp.MaxBlockHeaderSize),
			maxBlockExUnits:    pp.MaxBlockExUnits,
			hasMaxBlockExUnits: true,
		}, true
	case *dijkstra.DijkstraProtocolParameters:
		if pp == nil {
			return blockProtocolLimits{}, false
		}
		return blockProtocolLimits{
			maxBodySize:        uint64(pp.MaxBlockBodySize),
			maxHeaderSize:      uint64(pp.MaxBlockHeaderSize),
			maxBlockExUnits:    pp.MaxBlockExUnits,
			hasMaxBlockExUnits: true,
		}, true
	default:
		return blockProtocolLimits{}, false
	}
}
