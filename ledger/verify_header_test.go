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
	"crypto/ed25519"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// tamperOption controls which part of a test block to corrupt.
type tamperOption int

const (
	tamperNone      tamperOption = iota
	tamperKESSig                 // Flip bits in the KES signature
	tamperVRFProof               // Flip bits in the VRF proof
	tamperOpCertSig              // Flip bits in the OpCert cold-key signature
)

// testBlockResult holds a constructed test block and the parameters needed
// to verify it.
type testBlockResult struct {
	block             *realBabbageBlock
	epochNonce        []byte
	slotsPerKesPeriod uint64
}

// createTestBlock generates real VRF, KES, and cold keys, builds a valid
// Babbage block header at an eligible slot, and optionally tampers with
// one component. The seed parameter must be exactly 32 bytes and should
// differ between tests to avoid key collisions.
func createTestBlock(
	t *testing.T,
	seed [32]byte,
	nonceSeed byte,
	tamper tamperOption,
) *testBlockResult {
	t.Helper()

	// Generate VRF key pair
	vrfPk, vrfSk, err := vrf.KeyGen(seed[:])
	require.NoError(t, err, "VRF key generation should succeed")

	// Generate KES key pair at production depth (use rotated seed)
	kesSeed := seed
	kesSeed[0] ^= 0xAA
	kesSk, kesPk, err := kes.KeyGen(kes.CardanoKesDepth, kesSeed[:])
	require.NoError(t, err, "KES key generation should succeed")

	// Generate cold key for OpCert signing (use rotated seed)
	coldSeed := seed
	coldSeed[0] ^= 0xBB
	coldPrivKey := ed25519.NewKeyFromSeed(coldSeed[:])
	coldPubKey := coldPrivKey.Public().(ed25519.PublicKey)

	slotsPerKesPeriod := uint64(129600)
	epochNonce := make([]byte, 32)
	for i := range epochNonce {
		epochNonce[i] = nonceSeed + byte(i) //nolint:gosec
	}

	// Create OpCert: cold key signs [hot_vkey, seq_number, kes_period]
	opCertSeqNum := uint32(0)
	opCertKesPeriod := uint32(0)
	opCertBody := []any{kesPk, opCertSeqNum, opCertKesPeriod}
	opCertBodyBytes, err := cbor.Encode(opCertBody)
	require.NoError(t, err, "CBOR encode OpCert body should succeed")
	opCertSig := ed25519.Sign(coldPrivKey, opCertBodyBytes)

	if tamper == tamperOpCertSig {
		opCertSig[0] ^= 0xFF
		opCertSig[1] ^= 0xFF
	}

	// Try multiple slots to find one where VRF proves leadership
	activeSlotCoeff := big.NewRat(99, 100) // 99% active slots

	var result *realBabbageBlock
	for slot := uint64(1); slot <= 200; slot++ {
		vrfInput := vrf.MkInputVrf(int64(slot), epochNonce) //nolint:gosec
		vrfProof, vrfOutput, proveErr := vrf.Prove(vrfSk, vrfInput)
		if proveErr != nil {
			continue
		}

		threshold := consensus.CertifiedNatThreshold(
			1000000000, // pool stake
			1000000000, // total stake = 100%
			activeSlotCoeff,
		)
		if !consensus.IsVRFOutputBelowThreshold(vrfOutput, threshold) {
			continue
		}

		if tamper == tamperVRFProof {
			vrfProof[0] ^= 0xFF
		}

		prevHash := make([]byte, 32)
		bodyHash := make([]byte, 32)
		headerBody := babbage.BabbageBlockHeaderBody{
			BlockNumber: slot,
			Slot:        slot,
			PrevHash: func() lcommon.Blake2b256 {
				var h lcommon.Blake2b256
				copy(h[:], prevHash)
				return h
			}(),
			IssuerVkey: func() lcommon.IssuerVkey {
				var k lcommon.IssuerVkey
				copy(k[:], coldPubKey)
				return k
			}(),
			VrfKey: vrfPk,
			VrfResult: lcommon.VrfResult{
				Output: vrfOutput,
				Proof:  vrfProof,
			},
			BlockBodySize: 1024,
			BlockBodyHash: func() lcommon.Blake2b256 {
				var h lcommon.Blake2b256
				copy(h[:], bodyHash)
				return h
			}(),
			OpCert: babbage.BabbageOpCert{
				HotVkey:        kesPk,
				SequenceNumber: opCertSeqNum,
				KesPeriod:      opCertKesPeriod,
				Signature:      opCertSig,
			},
			ProtoVersion: babbage.BabbageProtoVersion{
				Major: 7,
				Minor: 0,
			},
		}

		headerBodyCbor, encErr := cbor.Encode(headerBody)
		if encErr != nil {
			continue
		}

		kesSig, signErr := kes.Sign(kesSk, 0, headerBodyCbor)
		if signErr != nil {
			continue
		}

		if tamper == tamperKESSig {
			kesSig[0] ^= 0xFF
			kesSig[1] ^= 0xFF
		}

		header := &babbage.BabbageBlockHeader{
			Body:      headerBody,
			Signature: kesSig,
		}

		result = &realBabbageBlock{
			header: header,
			era:    babbage.EraBabbage,
			slot:   slot,
		}
		break
	}

	require.NotNil(t, result, "should find an eligible slot for the test block")

	return &testBlockResult{
		block:             result,
		epochNonce:        epochNonce,
		slotsPerKesPeriod: slotsPerKesPeriod,
	}
}

// mockByronBlock implements ledger.Block for Byron-era testing.
// Byron blocks use PBFT consensus and should be skipped by header
// verification.
type mockByronBlock struct {
	byron.ByronMainBlock
}

func (m *mockByronBlock) Era() lcommon.Era {
	return byron.EraByron
}

func (m *mockByronBlock) SlotNumber() uint64 {
	return 100
}

// TestVerifyBlockHeader_ByronBlockSkipped verifies that Byron-era blocks
// are gracefully skipped during header verification since Byron uses PBFT
// consensus instead of Praos (no VRF/KES fields).
func TestVerifyBlockHeader_ByronBlockSkipped(t *testing.T) {
	block := &mockByronBlock{}
	err := verifyBlockHeader(block, nil, 129600)
	assert.NoError(t, err, "Byron blocks should be skipped")
}

// TestVerifyBlockHeader_MissingEpochNonce verifies that post-Byron blocks
// fail verification when no epoch nonce is available.
func TestVerifyBlockHeader_MissingEpochNonce(t *testing.T) {
	block := &mockBabbageBlock{slot: 1000}
	err := verifyBlockHeader(block, nil, 129600)
	assert.Error(t, err, "should fail with missing epoch nonce")
	assert.Contains(t, err.Error(), "epoch nonce not available")
}

// TestVerifyBlockHeader_EmptyEpochNonce verifies that an empty epoch
// nonce also fails.
func TestVerifyBlockHeader_EmptyEpochNonce(t *testing.T) {
	block := &mockBabbageBlock{slot: 1000}
	err := verifyBlockHeader(block, []byte{}, 129600)
	assert.Error(t, err, "should fail with empty epoch nonce")
	assert.Contains(t, err.Error(), "epoch nonce not available")
}

// TestVerifyBlockHeader_ValidBlock tests that a block with valid
// cryptographic proofs passes header verification.
func TestVerifyBlockHeader_ValidBlock(t *testing.T) {
	tb := createTestBlock(t, [32]byte{1}, 0, tamperNone)
	err := verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	assert.NoError(t, err, "valid block should pass verification")
}

// TestVerifyBlockHeader_TamperedKESSignature tests that a block with a
// tampered KES signature is rejected.
func TestVerifyBlockHeader_TamperedKESSignature(t *testing.T) {
	tb := createTestBlock(t, [32]byte{2}, 42, tamperKESSig)
	err := verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	assert.Error(
		t,
		err,
		"block with tampered KES signature should fail verification",
	)
}

// TestVerifyBlockHeader_TamperedVRFProof tests that a block with a
// tampered VRF proof is rejected.
func TestVerifyBlockHeader_TamperedVRFProof(t *testing.T) {
	tb := createTestBlock(t, [32]byte{3}, 99, tamperVRFProof)
	err := verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	assert.Error(
		t,
		err,
		"block with tampered VRF proof should fail verification",
	)
}

// TestVerifyBlockHeader_TamperedOpCertSignature verifies the current
// behavior for blocks with tampered OpCert signatures. Because
// verifyBlockHeader uses SkipStakePoolValidation (OpCert cold-key
// signature verification requires pool registration data from ledger
// state), the tampered signature is not caught at this layer. Full
// OpCert validation happens during stake pool verification in the
// ledger processing pipeline.
func TestVerifyBlockHeader_TamperedOpCertSignature(t *testing.T) {
	tb := createTestBlock(t, [32]byte{4}, 77, tamperOpCertSig)
	err := verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	// OpCert signature is NOT verified at this layer (requires ledger
	// state for pool registration lookup), so tampering does not cause
	// an error here.
	assert.NoError(
		t,
		err,
		"OpCert signature not validated at header-only layer",
	)
}

// mockBabbageBlock is a minimal mock implementing ledger.Block for
// non-Byron blocks that should trigger the verification path.
type mockBabbageBlock struct {
	slot uint64
}

func (m *mockBabbageBlock) Era() lcommon.Era {
	return babbage.EraBabbage
}

func (m *mockBabbageBlock) SlotNumber() uint64 {
	return m.slot
}

func (m *mockBabbageBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *mockBabbageBlock) PrevHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *mockBabbageBlock) BlockNumber() uint64 {
	return 1
}

func (m *mockBabbageBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}

func (m *mockBabbageBlock) BlockBodySize() uint64 {
	return 0
}

func (m *mockBabbageBlock) Cbor() []byte {
	return nil
}

func (m *mockBabbageBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *mockBabbageBlock) Header() lcommon.BlockHeader {
	return nil
}

func (m *mockBabbageBlock) Type() int {
	return int(babbage.BlockTypeBabbage)
}

func (m *mockBabbageBlock) Transactions() []lcommon.Transaction {
	return nil
}

func (m *mockBabbageBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}

// realBabbageBlock wraps a real BabbageBlockHeader for verification tests.
type realBabbageBlock struct {
	header *babbage.BabbageBlockHeader
	era    lcommon.Era
	slot   uint64
}

func (b *realBabbageBlock) Era() lcommon.Era {
	return b.era
}

func (b *realBabbageBlock) SlotNumber() uint64 {
	return b.slot
}

func (b *realBabbageBlock) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (b *realBabbageBlock) PrevHash() lcommon.Blake2b256 {
	return b.header.Body.PrevHash
}

func (b *realBabbageBlock) BlockNumber() uint64 {
	return b.header.Body.BlockNumber
}

func (b *realBabbageBlock) IssuerVkey() lcommon.IssuerVkey {
	return b.header.Body.IssuerVkey
}

func (b *realBabbageBlock) BlockBodySize() uint64 {
	return b.header.Body.BlockBodySize
}

func (b *realBabbageBlock) Cbor() []byte {
	return nil
}

func (b *realBabbageBlock) BlockBodyHash() lcommon.Blake2b256 {
	return b.header.Body.BlockBodyHash
}

func (b *realBabbageBlock) Header() lcommon.BlockHeader {
	return b.header
}

func (b *realBabbageBlock) Type() int {
	return int(babbage.BlockTypeBabbage)
}

func (b *realBabbageBlock) Transactions() []lcommon.Transaction {
	return nil
}

func (b *realBabbageBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
}

// --- epochForSlot tests ---

// TestEpochForSlot_EmptyCache verifies that epochForSlot returns an error
// when the epoch cache is empty.
func TestEpochForSlot_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		epochCache: nil,
	}
	_, err := ls.epochForSlot(100)
	assert.Error(t, err, "should fail with empty epoch cache")
	assert.Contains(t, err.Error(), "epoch cache is empty")
}

// TestEpochForSlot_SlotInFirstEpoch verifies that epochForSlot returns
// the correct epoch when the slot falls within the first epoch.
func TestEpochForSlot_SlotInFirstEpoch(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         []byte{0x01, 0x02},
			},
		},
	}
	ep, err := ls.epochForSlot(1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ep.EpochId)
	assert.Equal(t, []byte{0x01, 0x02}, ep.Nonce)
}

// TestEpochForSlot_SlotInSecondEpoch verifies that epochForSlot returns
// the correct epoch when the slot falls in the second epoch, ensuring
// epoch-aware lookup works across epoch boundaries.
func TestEpochForSlot_SlotInSecondEpoch(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         []byte{0x01},
			},
			{
				EpochId:       1,
				StartSlot:     432000,
				LengthInSlots: 432000,
				Nonce:         []byte{0x02},
			},
		},
	}
	// Slot at the very start of epoch 1
	ep, err := ls.epochForSlot(432000)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ep.EpochId)
	assert.Equal(t, []byte{0x02}, ep.Nonce)

	// Slot in the middle of epoch 1
	ep, err = ls.epochForSlot(500000)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ep.EpochId)
}

// TestEpochForSlot_SlotBeyondKnownEpochs verifies that epochForSlot
// returns an error when the slot is beyond all known epochs. This is
// critical for the security fix: blocks from unknown future epochs
// must be rejected rather than silently skipped.
func TestEpochForSlot_SlotBeyondKnownEpochs(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         []byte{0x01},
			},
		},
	}
	_, err := ls.epochForSlot(432001)
	assert.Error(t, err, "should fail for slot beyond known epochs")
	assert.Contains(t, err.Error(), "not covered by any known epoch")
}

// TestEpochForSlot_SlotAtEpochBoundary verifies correct behavior at
// the exact boundary between two epochs. Slot N (last slot of epoch 0)
// should belong to epoch 0, and slot N+1 (first slot of epoch 1)
// should belong to epoch 1.
func TestEpochForSlot_SlotAtEpochBoundary(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1000,
				Nonce:         []byte{0xAA},
			},
			{
				EpochId:       1,
				StartSlot:     1000,
				LengthInSlots: 1000,
				Nonce:         []byte{0xBB},
			},
		},
	}
	// Last slot of epoch 0
	ep, err := ls.epochForSlot(999)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ep.EpochId)
	assert.Equal(t, []byte{0xAA}, ep.Nonce)

	// First slot of epoch 1
	ep, err = ls.epochForSlot(1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ep.EpochId)
	assert.Equal(t, []byte{0xBB}, ep.Nonce)
}

// TestEpochForSlot_SkipsZeroLengthEpochs verifies that epochs with
// LengthInSlots == 0 are skipped during lookup.
func TestEpochForSlot_SkipsZeroLengthEpochs(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 0, // zero-length, should be skipped
				Nonce:         []byte{0x01},
			},
			{
				EpochId:       1,
				StartSlot:     0,
				LengthInSlots: 1000,
				Nonce:         []byte{0x02},
			},
		},
	}
	ep, err := ls.epochForSlot(500)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ep.EpochId)
}

// --- verifyBlockHeaderCrypto tests ---

// newTestShelleyGenesisCfg creates a CardanoNodeConfig with Shelley genesis
// loaded for use in verifyBlockHeaderCrypto tests.
func newTestShelleyGenesisCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	)
	require.NoError(t, err)
	return cfg
}

// TestVerifyBlockHeaderCrypto_ByronSkipped verifies that Byron-era blocks
// are skipped by the LedgerState-level verification method.
func TestVerifyBlockHeaderCrypto_ByronSkipped(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         []byte{0x01},
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	block := &mockByronBlock{}
	err := ls.verifyBlockHeaderCrypto(block)
	assert.NoError(t, err, "Byron blocks should be skipped")
}

// TestVerifyBlockHeaderCrypto_RejectsBlockOutsideKnownEpochs verifies that
// a block whose slot is beyond all known epochs is REJECTED rather than
// silently skipped. This is the core of the LDG-08 security fix.
func TestVerifyBlockHeaderCrypto_RejectsBlockOutsideKnownEpochs(
	t *testing.T,
) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1000,
				Nonce:         []byte{0x01, 0x02, 0x03},
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	// Block at slot 2000, which is beyond epoch 0 (ends at slot 1000)
	block := &mockBabbageBlock{slot: 2000}
	err := ls.verifyBlockHeaderCrypto(block)
	assert.Error(
		t,
		err,
		"block outside known epochs must be rejected, not skipped",
	)
	assert.Contains(t, err.Error(), "no epoch data for slot")
}

// TestVerifyBlockHeaderCrypto_RejectsBlockWithNoNonce verifies that a block
// in an epoch that has no nonce (e.g., epoch rollover not yet processed)
// is rejected.
func TestVerifyBlockHeaderCrypto_RejectsBlockWithNoNonce(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1000,
				Nonce:         nil, // no nonce
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	block := &mockBabbageBlock{slot: 500}
	err := ls.verifyBlockHeaderCrypto(block)
	assert.Error(t, err, "block with missing nonce must be rejected")
	assert.Contains(t, err.Error(), "has no nonce")
}

// TestVerifyBlockHeaderCrypto_EpochBoundaryUsesCorrectNonce verifies that
// when blocks span an epoch boundary, each block is verified against
// the nonce of its own epoch, not the "current" epoch. This is the
// epoch-aware lookup that prevents the LDG-08 bypass.
func TestVerifyBlockHeaderCrypto_EpochBoundaryUsesCorrectNonce(
	t *testing.T,
) {
	epoch0Nonce := make([]byte, 32)
	epoch1Nonce := make([]byte, 32)
	for i := range epoch0Nonce {
		epoch0Nonce[i] = byte(i)     //nolint:gosec
		epoch1Nonce[i] = byte(i + 1) //nolint:gosec
	}

	ls := &LedgerState{
		currentEpoch: models.Epoch{
			EpochId:       1,
			StartSlot:     1000,
			LengthInSlots: 1000,
			Nonce:         epoch1Nonce,
		},
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1000,
				Nonce:         epoch0Nonce,
			},
			{
				EpochId:       1,
				StartSlot:     1000,
				LengthInSlots: 1000,
				Nonce:         epoch1Nonce,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}

	// Create a valid block in epoch 0 (slot < 1000).
	// The block's VRF proof was generated with epoch0Nonce.
	tb := createTestBlock(t, [32]byte{10}, 0, tamperNone)
	// The test block's slot is in [1, 200]. Ensure epoch 0 covers it.
	require.Less(
		t,
		tb.block.slot,
		uint64(1000),
		"test block slot must be in epoch 0",
	)

	// Verify: the epoch-aware lookup should find epoch 0 for this block
	// and use epoch0Nonce (which matches the block's VRF proof).
	// tb.epochNonce == epoch0Nonce by construction (nonceSeed=0).
	err := ls.verifyBlockHeaderCrypto(tb.block)
	assert.NoError(
		t,
		err,
		"block in epoch 0 should verify with epoch 0 nonce "+
			"even when currentEpoch is epoch 1",
	)
}

// TestVerifyBlockHeaderCrypto_RejectsEmptyEpochCache verifies that
// verification rejects blocks when the epoch cache is completely empty.
func TestVerifyBlockHeaderCrypto_RejectsEmptyEpochCache(t *testing.T) {
	ls := &LedgerState{
		epochCache: nil,
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	block := &mockBabbageBlock{slot: 100}
	err := ls.verifyBlockHeaderCrypto(block)
	assert.Error(t, err, "should reject with empty epoch cache")
	assert.Contains(t, err.Error(), "epoch cache is empty")
}

// TestVerifyBlockHeaderCrypto_WrongNonceFails verifies that a block
// verified against the wrong epoch's nonce fails cryptographic checks.
// This demonstrates the attack scenario: an attacker sends a block
// crafted for epoch 0's nonce, but it arrives during epoch 1. With the
// fix, the epoch-aware lookup correctly identifies the block's epoch
// and rejects the mismatched nonce.
func TestVerifyBlockHeaderCrypto_WrongNonceFails(t *testing.T) {
	// Create a valid block with nonceSeed=0 (epoch 0 nonce)
	tb := createTestBlock(t, [32]byte{20}, 0, tamperNone)

	// Set up ledger state where only epoch 1 exists (epoch 0 is gone)
	// and epoch 1 has a DIFFERENT nonce
	wrongNonce := make([]byte, 32)
	for i := range wrongNonce {
		wrongNonce[i] = 0xFF
	}

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 1000,
				// Different nonce than what the block was built with
				Nonce: wrongNonce,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}

	err := ls.verifyBlockHeaderCrypto(tb.block)
	assert.Error(
		t,
		err,
		"block verified against wrong epoch nonce should fail",
	)
}
