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
	"reflect"

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
}

func envelopeParentFromBlock(block gledger.Block) envelopeParent {
	_, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	return envelopeParent{
		slot:        block.SlotNumber(),
		blockNumber: block.BlockNumber(),
		byronEbb:    isEbb,
	}
}

// validateInboundBlockEnvelope runs the consensus envelope checks that must
// pass before header crypto and body validation accept an inbound block.
func validateInboundBlockEnvelope(
	block gledger.Block,
	pparams lcommon.ProtocolParameters,
	parent envelopeParent,
) error {
	if block == nil {
		return errors.New("validate inbound block envelope: nil block")
	}
	if err := validateByronEbbPlacement(block); err != nil {
		return err
	}
	if isNilBlockHeader(block.Header()) {
		return errors.New("validate inbound block envelope: nil block header")
	}
	if err := validateBlockOrder(block, parent); err != nil {
		return err
	}
	if block.Era().Id == byron.EraIdByron {
		return nil
	}
	return validateBlockSizes(block, pparams)
}

func isNilBlockHeader(header lcommon.BlockHeader) bool {
	if header == nil {
		return true
	}
	value := reflect.ValueOf(header)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface,
		reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// validateBlockOrder checks that a block follows its parent by block number
// and slot, including the Byron EBB exception for shared number/slot.
func validateBlockOrder(block gledger.Block, parent envelopeParent) error {
	if parent.origin {
		return nil
	}
	_, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	if isEbb {
		if block.BlockNumber() != parent.blockNumber {
			return fmt.Errorf(
				"byron EBB block number %d does not match parent block number %d",
				block.BlockNumber(),
				parent.blockNumber,
			)
		}
		if block.SlotNumber() < parent.slot {
			return fmt.Errorf(
				"byron EBB slot %d precedes parent slot %d",
				block.SlotNumber(),
				parent.slot,
			)
		}
		return nil
	}
	if block.BlockNumber() != parent.blockNumber+1 {
		return fmt.Errorf(
			"block number %d does not follow parent block number %d",
			block.BlockNumber(),
			parent.blockNumber,
		)
	}
	if block.SlotNumber() == parent.slot &&
		(parent.byronEbb && block.Era().Id == byron.EraIdByron) {
		return nil
	}
	if block.SlotNumber() <= parent.slot {
		return fmt.Errorf(
			"block slot %d does not follow parent slot %d",
			block.SlotNumber(),
			parent.slot,
		)
	}
	return nil
}

// validateByronEbbPlacement rejects Byron epoch boundary blocks outside their
// declared epoch boundary slot, while ignoring non-EBB blocks.
func validateByronEbbPlacement(block gledger.Block) error {
	ebb, isEbb := block.(*byron.ByronEpochBoundaryBlock)
	if !isEbb {
		return nil
	}
	if ebb.BlockHeader == nil {
		return errors.New("byron EBB has nil header")
	}
	slot := ebb.SlotNumber()
	if slot%byron.ByronSlotsPerEpoch != 0 {
		return fmt.Errorf(
			"byron EBB slot %d is not an epoch boundary slot",
			slot,
		)
	}
	expectedSlot := ebb.BlockHeader.ConsensusData.Epoch * byron.ByronSlotsPerEpoch
	if slot != expectedSlot {
		return fmt.Errorf(
			"byron EBB slot %d does not match epoch %d boundary slot %d",
			slot,
			ebb.BlockHeader.ConsensusData.Epoch,
			expectedSlot,
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
	maxBodySize, maxHeaderSize, ok := protocolBlockSizeLimits(pparams)
	if !ok {
		return fmt.Errorf(
			"block size validation unsupported for protocol parameters %T",
			pparams,
		)
	}
	headerCbor := block.Header().Cbor()
	if uint64(len(headerCbor)) > maxHeaderSize {
		return fmt.Errorf(
			"block header size %d exceeds maxBlockHeaderSize %d",
			len(headerCbor),
			maxHeaderSize,
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
	if actualBodySize > maxBodySize {
		return fmt.Errorf(
			"block body size %d exceeds maxBlockBodySize %d",
			actualBodySize,
			maxBodySize,
		)
	}
	return nil
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

// protocolBlockSizeLimits extracts max block body/header size protocol
// parameters from eras whose inbound block sizes can be validated here.
func protocolBlockSizeLimits(
	pparams lcommon.ProtocolParameters,
) (maxBodySize, maxHeaderSize uint64, ok bool) {
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	case *mary.MaryProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	case *alonzo.AlonzoProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	case *babbage.BabbageProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	case *conway.ConwayProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	case *dijkstra.DijkstraProtocolParameters:
		return uint64(pp.MaxBlockBodySize), uint64(pp.MaxBlockHeaderSize), true
	default:
		return 0, 0, false
	}
}
