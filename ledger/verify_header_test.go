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
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ledgersnapshot "github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/kes"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// verifyBlockHeader is a test helper that wraps verifyBlockHeaderHex,
// accepting raw epoch nonce bytes for convenience.
func verifyBlockHeader(
	block gledger.Block,
	epochNonce []byte,
	slotsPerKesPeriod uint64,
) error {
	return verifyBlockHeaderHex(
		block,
		hex.EncodeToString(epochNonce),
		slotsPerKesPeriod,
	)
}

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
	t testing.TB,
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

	// Create OpCert: the cold key signs the cardano-ledger OCertSignable
	// representation — KES vkey (32) || issue number (8 BE) || KES period
	// (8 BE), the raw concatenation real cardano-cli opcerts use, NOT a CBOR
	// array. See ledger/forging/keys.go ValidateOpCert and
	// verifyOpCertColdSignature.
	opCertSeqNum := uint32(0)
	opCertKesPeriod := uint32(0)
	var opCertBody [48]byte
	copy(opCertBody[:32], kesPk)
	binary.BigEndian.PutUint64(opCertBody[32:40], uint64(opCertSeqNum))
	binary.BigEndian.PutUint64(opCertBody[40:48], uint64(opCertKesPeriod))
	opCertSig := ed25519.Sign(coldPrivKey, opCertBody[:])

	if tamper == tamperOpCertSig {
		opCertSig[0] ^= 0xFF
		opCertSig[1] ^= 0xFF
	}

	// Try multiple slots to find one where VRF proves leadership
	activeSlotCoeff := big.NewRat(99, 100) // 99% active slots

	var result *realBabbageBlock
	for slot := uint64(1); slot <= 200; slot++ {
		vrfInput, vrfInputErr := vrf.MkInputVrf(int64(slot), epochNonce) //nolint:gosec
		if vrfInputErr != nil {
			continue
		}
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
		// Store the CBOR on the header body so that
		// VerifyBlock's extractOriginalBodyCbor can retrieve it
		// for KES signature verification.
		headerBody.SetCbor(headerBodyCbor)

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

// TestVerifyBlockHeader_UsesBodyCBORVRFFields verifies that header crypto
// verification is driven by the original header-body CBOR, not by stale typed
// VRF fields on the decoded header object.
func TestVerifyBlockHeader_UsesBodyCBORVRFFields(t *testing.T) {
	tb := createTestBlock(t, [32]byte{5}, 11, tamperNone)
	header := tb.block.header
	require.NotEmpty(t, header.Body.Cbor())

	originalKey := cloneBytes(header.Body.VrfKey)
	originalOutput := cloneBytes(header.Body.VrfResult.Output)
	originalProof := cloneBytes(header.Body.VrfResult.Proof)
	staleKey := bytes.Repeat([]byte{0x33}, len(originalKey))
	staleOutput := bytes.Repeat([]byte{0x44}, len(originalOutput))
	staleProof := bytes.Repeat([]byte{0x55}, len(originalProof))
	require.False(t, bytes.Equal(originalKey, staleKey))
	require.False(t, bytes.Equal(originalOutput, staleOutput))
	require.False(t, bytes.Equal(originalProof, staleProof))

	header.Body.VrfKey = staleKey
	header.Body.VrfResult.Output = staleOutput
	header.Body.VrfResult.Proof = staleProof

	normalized, err := normalizeHeaderVrfFieldsFromBodyCbor(header)
	require.NoError(t, err)
	normalizedHeader, ok := normalized.(*babbage.BabbageBlockHeader)
	require.True(t, ok)
	assert.Equal(t, originalKey, normalizedHeader.Body.VrfKey)
	assert.Equal(t, originalOutput, normalizedHeader.Body.VrfResult.Output)
	assert.Equal(t, originalProof, normalizedHeader.Body.VrfResult.Proof)

	err = verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	assert.NoError(
		t,
		err,
		"valid header should pass even when decoded VRF fields are stale",
	)
	assert.Equal(t, staleKey, header.Body.VrfKey)
	assert.Equal(t, staleOutput, header.Body.VrfResult.Output)
	assert.Equal(t, staleProof, header.Body.VrfResult.Proof)
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

// TestVerifyBlockHeader_TamperedOpCertSignature verifies that the
// VerifyBlock-based crypto path (verifyBlockHeaderHex) does not, by itself,
// validate the OpCert cold-key signature: it runs with SkipStakePoolValidation.
// Inbound OpCert validation lives in the sibling verifyOpCertHeaderCrypto
// (exercised by verify_opcert_test.go), which verifyBlockHeaderCrypto invokes
// alongside this path. This test pins the boundary so the two layers stay
// distinct.
func TestVerifyBlockHeader_TamperedOpCertSignature(t *testing.T) {
	tb := createTestBlock(t, [32]byte{4}, 77, tamperOpCertSig)
	err := verifyBlockHeader(tb.block, tb.epochNonce, tb.slotsPerKesPeriod)
	// The hex/VerifyBlock layer does not verify the OpCert signature, so
	// tampering does not cause an error here; verifyOpCertHeaderCrypto does.
	assert.NoError(
		t,
		err,
		"OpCert signature not validated by the VerifyBlock crypto layer",
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
func newTestShelleyGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
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
//
// The test also exercises the full pipeline including leader-eligibility:
// epoch 0 blocks query the genesis snapshot (epoch 0, "mark"), so the
// database is seeded with the pool's stake before calling verifyBlockHeaderCrypto.
func TestVerifyBlockHeaderCrypto_EpochBoundaryUsesCorrectNonce(
	t *testing.T,
) {
	// createTestBlock uses f=0.99 to find eligible slots; use the same
	// coefficient in the Shelley genesis so the eligibility check matches.
	tb := createTestBlock(t, [32]byte{10}, 0, tamperNone)

	epoch0Nonce := tb.epochNonce // nonceSeed=0 → epoch0Nonce
	epoch1Nonce := make([]byte, 32)
	for i := range epoch1Nonce {
		epoch1Nonce[i] = byte(i + 1) //nolint:gosec
	}

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	// Seed genesis snapshot (epoch 0, "mark") for the pool so leader
	// eligibility succeeds for blocks in epoch 0.
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 0, poolKeyHash[:], 1_000_000_000)

	// Register the pool with the block's actual VRF key hash so the
	// registered-VRF-key binding check accepts the block. Without this the
	// block is rejected before the nonce-selection logic under test runs.
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	seedPoolRegistration(
		t,
		db,
		poolKeyHash[:],
		lcommon.Blake2b256Hash(vrfKey).Bytes(),
	)

	ls := &LedgerState{
		db: db,
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
			CardanoNodeConfig: newHighFreqShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}

	// The test block's slot is in [1, 200]. Ensure epoch 0 covers it.
	require.Less(
		t,
		tb.block.slot,
		uint64(1000),
		"test block slot must be in epoch 0",
	)

	// Verify: the epoch-aware lookup should find epoch 0 for this block
	// and use epoch0Nonce (which matches the block's VRF proof).
	err = ls.verifyBlockHeaderCrypto(tb.block)
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

// --- verifyBlockLeaderEligibility tests ---

// newHighFreqShelleyGenesisCfg returns a CardanoNodeConfig with
// activeSlotsCoeff=0.99, matching the coefficient used in createTestBlock
// so that VRF outputs found eligible there are also eligible here.
func newHighFreqShelleyGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.99,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON))
	require.NoError(t, err)
	return cfg
}

// newEligibilityTestLedger builds a LedgerState backed by in-memory SQLite,
// with an epoch cache that places any slot in [0, 1_000_000) at epoch 5
// (so snapshotEpoch = 3). The Shelley genesis uses activeSlotsCoeff=0.99
// to match createTestBlock's VRF eligibility threshold.
func newEligibilityTestLedger(
	t *testing.T,
	epochNonce []byte,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:       5,
				StartSlot:     0,
				LengthInSlots: 1_000_000,
				Nonce:         epochNonce,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newHighFreqShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// seedPoolStakeSnapshot inserts a pool stake snapshot using the store interface.
func seedPoolStakeSnapshot(
	t *testing.T,
	db *database.Database,
	epoch uint64,
	poolKeyHash []byte,
	totalStake uint64,
) {
	t.Helper()
	seedPoolStakeSnapshotOfType(
		t,
		db,
		epoch,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash,
		totalStake,
		0,
	)
}

func seedPoolStakeSnapshotOfType(
	t *testing.T,
	db *database.Database,
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	totalStake uint64,
	stakeDenominator uint64,
) {
	t.Helper()
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		epoch,
		snapshotType,
		poolKeyHash,
		totalStake,
		stakeDenominator,
		0,
	)
}

func seedPoolStakeSnapshotOfTypeAtSlot(
	t *testing.T,
	db *database.Database,
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	totalStake uint64,
	stakeDenominator uint64,
	capturedSlot uint64,
) {
	t.Helper()
	err := db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:            epoch,
			SnapshotType:     snapshotType,
			PoolKeyHash:      poolKeyHash,
			TotalStake:       types.Uint64(totalStake),
			StakeDenominator: types.Uint64(stakeDenominator),
			CapturedSlot:     capturedSlot,
		},
		nil,
	)
	require.NoError(t, err)
}

func seedEligibilityEpochs(
	t *testing.T,
	db *database.Database,
	epochs []models.Epoch,
) {
	t.Helper()
	for _, epoch := range epochs {
		require.NoError(t, db.SetEpoch(
			epoch.StartSlot,
			epoch.EpochId,
			epoch.Nonce,
			epoch.EvolvingNonce,
			epoch.CandidateNonce,
			epoch.LastEpochBlockNonce,
			epoch.EraId,
			epoch.SlotLength,
			epoch.LengthInSlots,
			nil,
		))
	}
}

func seedLiveDelegatedPoolStake(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	totalStake uint64,
	slot uint64,
	discriminator byte,
) {
	t.Helper()
	margin := &types.Rat{Rat: big.NewRat(1, 100)}
	err := db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   poolKeyHash,
			VrfKeyHash:    make([]byte, 32),
			Pledge:        types.Uint64(1_000_000),
			Cost:          types.Uint64(340_000_000),
			Margin:        margin,
			RewardAccount: make([]byte, 28),
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			VrfKeyHash:    make([]byte, 32),
			AddedSlot:     slot,
			Pledge:        types.Uint64(1_000_000),
			Cost:          types.Uint64(340_000_000),
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: make([]byte, 28),
		},
		nil,
	)
	require.NoError(t, err)

	stakingKey := make([]byte, 28)
	copy(stakingKey, poolKeyHash)
	stakingKey[27] ^= discriminator
	err = db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: stakingKey,
		Pool:       poolKeyHash,
		AddedSlot:  slot,
		Active:     true,
	})
	require.NoError(t, err)

	txId := make([]byte, 32)
	copy(txId, poolKeyHash)
	txId[28] = discriminator
	txId[31] = discriminator + 1
	err = db.Metadata().CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  uint32(discriminator),
		StakingKey: stakingKey,
		Amount:     types.Uint64(totalStake),
		AddedSlot:  slot,
	})
	require.NoError(t, err)
}

func captureLiveMarkSnapshot(
	t *testing.T,
	db *database.Database,
	newEpoch uint64,
	boundarySlot uint64,
	snapshotSlot uint64,
	poolKeyHash []byte,
) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	t.Cleanup(eventBus.Close)

	ctx, cancel := context.WithCancel(context.Background())
	mgr := ledgersnapshot.NewManager(db, eventBus, logger)
	require.NoError(t, mgr.Start(ctx))
	t.Cleanup(func() {
		cancel()
		require.NoError(t, mgr.Stop())
	})

	epochEvent := event.EpochTransitionEvent{
		PreviousEpoch: newEpoch - 1,
		NewEpoch:      newEpoch,
		BoundarySlot:  boundarySlot,
		SnapshotSlot:  snapshotSlot,
	}
	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, epochEvent),
	)

	testutil.WaitForCondition(t, func() bool {
		snapshot, err := db.Metadata().GetPoolStakeSnapshot(
			newEpoch,
			models.PoolStakeSnapshotTypeMark,
			poolKeyHash,
			nil,
		)
		return err == nil &&
			snapshot != nil &&
			snapshot.CapturedSlot == snapshotSlot
	}, 2*time.Second, "live mark snapshot should be captured")
}

// seedPoolRegistration registers a pool so that db.GetPool(poolKeyHash)
// returns a *models.Pool whose VrfKeyHash equals vrfKeyHash. It mirrors
// seedPoolStakeSnapshot by persisting through the metadata store interface:
// ImportPool upserts the Pool row (whose denormalized VrfKeyHash is what
// verifyRegisteredVrfKey compares against) and creates a linked
// PoolRegistration record, which GetPool requires before it treats the pool
// as active. vrfKeyHash must be the 32-byte Blake2b256 of the block header's
// VRF key bytes for verifyRegisteredVrfKey to accept the block.
func seedPoolRegistration(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	vrfKeyHash []byte,
) {
	t.Helper()
	err := db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolKeyHash,
			VrfKeyHash:  vrfKeyHash,
		},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash,
			VrfKeyHash:  vrfKeyHash,
			AddedSlot:   1,
		},
		nil,
	)
	require.NoError(t, err)
}

// TestVerifyBlockLeaderEligibility_ByronSkipped verifies that Byron blocks
// bypass eligibility checking entirely (Byron uses PBFT, not Praos).
func TestVerifyBlockLeaderEligibility_ByronSkipped(t *testing.T) {
	ls := &LedgerState{} // no db needed
	block := &mockByronBlock{}
	err := ls.verifyBlockLeaderEligibility(block, 5)
	assert.NoError(t, err, "Byron blocks must be skipped")
}

// TestVerifyBlockLeaderEligibility_EarlyEpochUsesGenesisSnapshot verifies that
// epochs 0 and 1 query the genesis snapshot (epoch 0, "mark") rather than
// skipping eligibility checks. A pool absent from that snapshot is rejected.
func TestVerifyBlockLeaderEligibility_EarlyEpochUsesGenesisSnapshot(t *testing.T) {
	tb := createTestBlock(t, [32]byte{35}, 0, tamperNone)
	// Use epoch 5 nonce for the genesis epoch cache entry; the actual nonce
	// is not used by verifyBlockLeaderEligibility itself.
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// Override epoch cache to place the block in epoch 1 (snapshotEpoch = 0).
	ls.epochCache = []models.Epoch{
		{EpochId: 1, StartSlot: 0, LengthInSlots: 1_000_000, Nonce: tb.epochNonce},
	}

	// No genesis snapshot seeded — pool has no stake at epoch 0.
	err := ls.verifyBlockLeaderEligibility(tb.block, 1)
	require.Error(t, err, "epoch 1 without genesis snapshot must be rejected")
	assert.Contains(t, err.Error(), "has no stake in epoch")

	// Now seed the genesis snapshot at epoch 0.
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 0, poolKeyHash[:], 1_000_000_000)

	err = ls.verifyBlockLeaderEligibility(tb.block, 1)
	assert.NoError(t, err, "epoch 1 with valid genesis snapshot should pass")
}

// TestVerifyBlockLeaderEligibility_EligiblePoolPasses verifies that a block
// from a pool with sufficient stake and an eligible VRF output passes the check.
func TestVerifyBlockLeaderEligibility_EligiblePoolPasses(t *testing.T) {
	tb := createTestBlock(t, [32]byte{30}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	// Pool owns 100% of stake — matches createTestBlock's threshold assumption.
	const totalStake = uint64(1_000_000_000)
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], totalStake)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "eligible pool with full stake should pass")
}

func TestVerifyBlockLeaderEligibility_MithrilEpochRequiresActiveDistribution(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{37}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	if tb.block.slot <= 1 {
		// This test exercises stake-source selection, not VRF proof input.
		// Slot 0 disables the Mithril boundary sentinel, so move the mock
		// block's reported slot past it.
		tb.block.slot = 2
	}
	ls.currentEpoch = models.Epoch{
		EpochId:       5,
		StartSlot:     0,
		LengthInSlots: 1_000_000,
		Nonce:         tb.epochNonce,
	}
	ls.mithrilLedgerSlot = tb.block.slot - 1

	poolKeyHash := tb.block.IssuerVkey().Hash()
	// Seed the normal rotated mark snapshot with full stake. In the imported
	// Mithril epoch this must not substitute for NewEpochState.pool-distr.
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from active pool distribution")

	seedPoolStakeSnapshotOfType(
		t,
		db,
		5,
		models.PoolStakeSnapshotTypeActive,
		poolKeyHash[:],
		1,
		1,
	)
	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err)
}

func TestVerifyBlockLeaderEligibility_ActiveDistributionVRFAboveThresholdFails(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{40}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	if tb.block.slot <= 1 {
		tb.block.slot = 2
	}
	ls.currentEpoch = models.Epoch{
		EpochId:       5,
		StartSlot:     0,
		LengthInSlots: 1_000_000,
		Nonce:         tb.epochNonce,
	}
	ls.mithrilLedgerSlot = tb.block.slot - 1

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshotOfType(
		t,
		db,
		5,
		models.PoolStakeSnapshotTypeActive,
		poolKeyHash[:],
		1,
		1_000_000_000_000_000_000,
	)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VRF leader value exceeds stake-derived threshold")
}

// TestVerifyBlockLeaderEligibility_PoolNotInSnapshotFails verifies that a block
// from a pool absent from the epoch-2 mark snapshot is rejected.
func TestVerifyBlockLeaderEligibility_PoolNotInSnapshotFails(t *testing.T) {
	tb := createTestBlock(t, [32]byte{31}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// No snapshot seeded — pool is unknown.

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no stake in epoch")
}

// TestVerifyBlockLeaderEligibility_ZeroStakeFails verifies that a block
// from a pool with a zero-stake snapshot entry is rejected.
func TestVerifyBlockLeaderEligibility_ZeroStakeFails(t *testing.T) {
	tb := createTestBlock(t, [32]byte{32}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 0)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no stake in epoch")
}

// TestVerifyBlockLeaderEligibility_VRFAboveThresholdFails verifies that a block
// whose VRF leader value is above the stake-derived threshold is rejected.
// The block was produced eligible at 100% stake (f=0.99 threshold ≈ 2^256*0.99),
// but the pool only holds 1 lovelace out of 10^18 — making its threshold
// near zero and ensuring the VRF output exceeds it.
func TestVerifyBlockLeaderEligibility_VRFAboveThresholdFails(t *testing.T) {
	tb := createTestBlock(t, [32]byte{33}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	// Actual pool: tiny stake (1 lovelace).
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1)
	// Dummy pool: huge stake to make the total far exceed the pool's share,
	// pushing sigma ≈ 1/10^18 and the threshold to essentially zero.
	dummyHash := make([]byte, 28)
	dummyHash[0] = 0xFF
	seedPoolStakeSnapshot(t, db, 4, dummyHash, 1_000_000_000_000_000_000)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VRF leader value exceeds stake-derived threshold")
}

func TestVerifyBlockLeaderEligibility_MithrilImportedHistoricalMarkSkips(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{38}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	var logBuf bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	ls.epochCache = []models.Epoch{
		{EpochId: 3, StartSlot: 300, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 4, StartSlot: 400, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 5, StartSlot: 500, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	importedCaptureSlot := ls.epochCache[1].StartSlot + 50
	ls.mithrilLedgerSlot = importedCaptureSlot
	tb.block.slot = ls.epochCache[2].StartSlot + 50

	// Leader election in epoch 5 uses mark[StakeSnapshotEpoch(5)] = mark[4]
	// (the end-of-epoch-3 "set" distribution), so the imported mark is seeded
	// at epoch 4. importedCaptureSlot (epoch4-start+50) is past epoch 4's start,
	// which is what marks it as Mithril-imported.
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		1,
		0,
		importedCaptureSlot,
	)
	dummyHash := make([]byte, 28)
	dummyHash[0] = 0xFF
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		4,
		models.PoolStakeSnapshotTypeMark,
		dummyHash,
		1_000_000_000_000_000_000,
		0,
		importedCaptureSlot,
	)
	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		nil,
	)
	require.NoError(t, err)
	require.True(t, ls.isMithrilImportedMarkSnapshot(snapshot, 4))

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err)
	assert.Contains(t, logBuf.String(), "mark snapshot captured after target epoch start")
	assert.NotContains(t, logBuf.String(), "total active stake is zero")
}

func TestVerifyBlockLeaderEligibility_LiveComputedHistoricalMarkStillChecks(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{39}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = []models.Epoch{
		{EpochId: 3, StartSlot: 300, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 4, StartSlot: 400, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 5, StartSlot: 500, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.mithrilLedgerSlot = ls.epochCache[1].StartSlot + 50
	tb.block.slot = ls.epochCache[2].StartSlot + 50
	seedEligibilityEpochs(t, db, append([]models.Epoch{
		{EpochId: 2, StartSlot: 200, LengthInSlots: 100},
	}, ls.epochCache...))

	poolKeyHash := tb.block.IssuerVkey().Hash()
	dummyHash := make([]byte, 28)
	dummyHash[0] = 0xFF
	// Leader election in epoch 5 uses mark[StakeSnapshotEpoch(5)] = mark[4].
	// Capture it live at epoch 4's boundary-1 (end of epoch 3); that slot is
	// before epoch 4's start, so it reads as a genuinely live-computed mark
	// (not Mithril-imported) and the threshold check must still run.
	snapshotEpoch := uint64(4)
	boundarySlot := ls.epochCache[1].StartSlot
	snapshotSlot := boundarySlot - 1
	seedLiveDelegatedPoolStake(
		t, db, poolKeyHash[:], 1, snapshotSlot, 1,
	)
	seedLiveDelegatedPoolStake(
		t, db, dummyHash, 1_000_000_000_000_000_000, snapshotSlot, 2,
	)
	captureLiveMarkSnapshot(
		t, db, snapshotEpoch, boundarySlot, snapshotSlot, poolKeyHash[:],
	)
	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		snapshotEpoch,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		nil,
	)
	require.NoError(t, err)
	require.False(t, ls.isMithrilImportedMarkSnapshot(snapshot, snapshotEpoch))

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VRF leader value exceeds stake-derived threshold")
}

// TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffSkips verifies that
// when the active slot coefficient is unavailable (Shelley genesis not loaded),
// the eligibility check is skipped rather than rejecting the block.
func TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffSkips(t *testing.T) {
	tb := createTestBlock(t, [32]byte{34}, 0, tamperNone)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	// Seed a pool stake snapshot so the check reaches the coeff lookup.
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{EpochId: 5, StartSlot: 0, LengthInSlots: 1_000_000, Nonce: tb.epochNonce},
		},
		config: LedgerStateConfig{
			// No CardanoNodeConfig → ActiveSlotCoeff() returns 0 → skip.
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "missing active slot coeff should skip, not reject")
}

// TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffSkips_ExplicitZero
// verifies that a genesis with activeSlotsCoeff=0 also triggers the skip path.
// A zero coefficient produces a zero threshold and would otherwise reject every
// non-Byron block.
func TestVerifyBlockLeaderEligibility_ZeroCoeffSkips(t *testing.T) {
	tb := createTestBlock(t, [32]byte{36}, 0, tamperNone)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	// Build a genesis config with activeSlotsCoeff explicitly set to 0.
	// big.Rat.SetString("0") gives Sign()==0, which the guard must catch.
	zeroCoeffJSON := `{
		"activeSlotsCoeff": 0,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	zeroCfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		zeroCfg.LoadShelleyGenesisFromReader(strings.NewReader(zeroCoeffJSON)),
	)

	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{EpochId: 5, StartSlot: 0, LengthInSlots: 1_000_000, Nonce: tb.epochNonce},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: zeroCfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "zero active slot coeff should skip, not reject all blocks")
}
