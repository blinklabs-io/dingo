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
	"math/big"
	"testing"

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
