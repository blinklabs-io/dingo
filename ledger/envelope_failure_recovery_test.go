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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func envelopeRecoveryPParams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
		MaxBlockBodySize:   100_000,
		MaxBlockHeaderSize: 100_000,
		MaxBlockExUnits:    lcommon.ExUnits{Memory: 1_000, Steps: 2_000},
	}
}

// decodeConwayBlock fills the header fields a wire decode requires, fixes the
// declared body size and round-trips the block through its bytes. Only a
// decoded block carries its header CBOR, which is what its hash covers; a
// constructed one hashes the empty string whatever its slot.
func decodeConwayBlock(t *testing.T, block *conway.ConwayBlock) gledger.Block {
	t.Helper()
	body := &block.BlockHeader.Body
	body.VrfKey = make([]byte, 32)
	body.VrfResult = lcommon.VrfResult{
		Output: make([]byte, 64),
		Proof:  make([]byte, 80),
	}
	body.OpCert = babbage.BabbageOpCert{
		HotVkey:   make([]byte, 32),
		Signature: make([]byte, 64),
	}
	block.BlockHeader.Signature = make([]byte, 448)
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	// The block body hash covers the four body components, in order.
	var components []cbor.RawMessage
	_, err = cbor.Decode(encoded, &components)
	require.NoError(t, err)
	require.Len(t, components, 5)
	var componentHashes []byte
	for _, component := range components[1:] {
		componentHashes = append(
			componentHashes,
			lcommon.Blake2b256Hash(component).Bytes()...,
		)
	}
	body.BlockBodyHash = lcommon.Blake2b256Hash(componentHashes)
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	decoded, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, encoded)
	require.NoError(t, err)
	return decoded
}

// conwayEmptyBlock is a transaction-free Conway block, which always meets the
// inbound envelope.
func conwayEmptyBlock(t *testing.T, slot uint64) gledger.Block {
	t.Helper()
	block := &conway.ConwayBlock{
		BlockHeader:            &conway.ConwayBlockHeader{},
		TransactionBodies:      []conway.ConwayTransactionBody{},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{},
		TransactionMetadataSet: lcommon.TransactionMetadataSet{},
		InvalidTransactions:    []uint{},
	}
	// Block number 0 is the only first block a chain rewound to origin takes.
	block.BlockHeader.Body.BlockNumber = 0
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.ProtoVersion.Major = uint64(
		lcommon.ProtocolVersionPlomin,
	)
	return decodeConwayBlock(t, block)
}

func processEnvelopeBlock(
	t *testing.T,
	ls *LedgerState,
	slot uint64,
	validate bool,
	block gledger.Block,
) error {
	t.Helper()
	return ls.db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(
				context.Background(),
				txn,
				ocommon.NewPoint(slot, block.Hash().Bytes()),
				block,
				validate,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				nil,
				eras.ConwayEraDesc,
				envelopeRecoveryPParams(),
				nil,
				0,
				0,
				false,
			)
			return err
		},
	)
}

type envelopeForgedChecker struct{ hash []byte }

func (c envelopeForgedChecker) WasForgedByUs(uint64) ([]byte, bool) {
	return c.hash, c.hash != nil
}

// A peer block that fails the inbound envelope is a deterministic verdict on a
// block already persisted, so ledgerProcessBlock must return it as a rejected
// block that the pipeline's reject-and-rewind recovery recognizes. Trusted
// input and locally forged blocks keep their existing handling.
func TestLedgerProcessBlockClassifiesEnvelopeFailure(t *testing.T) {
	t.Parallel()

	const slot = uint64(10)
	for _, tc := range []struct {
		name          string
		validate      bool
		forged        bool
		trustedReplay bool
		wantRejected  bool
		wantExceeds   bool
	}{
		{name: "validated peer block", validate: true, wantRejected: true, wantExceeds: true},
		{name: "unvalidated peer block", validate: false, wantRejected: true, wantExceeds: true},
		{name: "locally forged block", validate: true, forged: true, wantExceeds: true},
		{name: "trusted replay skips the check", validate: false, trustedReplay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			constructed, _ := conwayRedeemerBlock(t, slot, 1_001, 1)
			block := decodeConwayBlock(t, constructed)
			ls := newRequiredDatumLedger(t, newTestDB(t), eras.ConwayEraDesc)
			ls.config.TrustedReplay = tc.trustedReplay
			if tc.forged {
				ls.config.ForgedBlockChecker = envelopeForgedChecker{
					hash: block.Hash().Bytes(),
				}
			}
			err := processEnvelopeBlock(t, ls, slot, tc.validate, block)
			if !tc.wantExceeds {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "exceed maxBlockExUnits")
			var rejected *headerValidationError
			if !tc.wantRejected {
				require.NotErrorAs(t, err, &rejected)
				return
			}
			require.ErrorAs(t, err, &rejected)
			require.Equal(
				t,
				ocommon.NewPoint(slot, block.Hash().Bytes()),
				rejected.BlockPoint,
			)
		})
	}
}

// A peer block whose own body exceeds maxBlockExUnits is rejected without
// stopping the pipeline: recovery rewinds the primary chain past it, and a
// replacement block appended at the rewind point then applies.
func TestLedgerRejectsOverBudgetPeerBlockAndAppliesReplacement(t *testing.T) {
	t.Parallel()

	const badSlot = uint64(10)
	constructed, _ := conwayRedeemerBlock(t, badSlot, 1_001, 1)
	bad := decodeConwayBlock(t, constructed)
	fork := conwayEmptyBlock(t, badSlot+1)
	require.NotEqual(t, bad.Hash().Bytes(), fork.Hash().Bytes())

	ls := newReplayTestLedger(
		t,
		newTestDB(t),
		bad,
		conway.BlockTypeConway,
		eras.ConwayEraDesc,
		envelopeRecoveryPParams(),
	)
	cm := ls.config.ChainManager
	require.Equal(t, badSlot, cm.PrimaryChain().Tip().Point.Slot)

	err := replayTestBlock(ls, bad)
	require.ErrorIs(t, err, errRestartLedgerPipeline)
	var rejected *headerValidationError
	require.ErrorAs(t, err, &rejected)
	require.Equal(
		t,
		ocommon.NewPoint(badSlot, bad.Hash().Bytes()),
		rejected.BlockPoint,
	)
	require.Zero(
		t,
		cm.PrimaryChain().Tip().Point.Slot,
		"the rejected block must be dropped from the primary chain",
	)
	ls.RLock()
	ledgerTip := ls.currentTip.Point
	ls.RUnlock()
	require.Zero(
		t,
		ledgerTip.Slot,
		"the rejected block must not be applied to the ledger",
	)

	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		context.Background(),
		[]chain.RawBlock{{
			Slot:        fork.SlotNumber(),
			Hash:        fork.Hash().Bytes(),
			BlockNumber: fork.BlockNumber(),
			Type:        conway.BlockTypeConway,
			Cbor:        fork.Cbor(),
		}},
	))
	// The rollback to origin returned the ledger to its pre-Shelley state; a
	// network's own era bootstrap is out of scope, so place it back in the
	// fixture's era as the harness does at construction.
	ls.Lock()
	setReplayTestLedgerOrigin(ls, eras.ConwayEraDesc, envelopeRecoveryPParams())
	ls.Unlock()
	require.NoError(t, replayTestBlock(ls, fork))
	ls.RLock()
	tip := ls.currentTip.Point
	ls.RUnlock()
	require.Equal(t, ocommon.NewPoint(fork.SlotNumber(), fork.Hash().Bytes()), tip)
}
