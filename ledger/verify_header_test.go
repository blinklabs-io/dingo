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
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ledgersnapshot "github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/kes"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

type tpraosNonceTamper int

const (
	tpraosNonceValid tpraosNonceTamper = iota
	tpraosNonceMismatchedKey
	tpraosNonceAlteredProof
)

type tpraosTestBlockResult struct {
	block             *realTPraosBlock
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
		vrfInput, vrfInputErr := vrf.MkInputVrf(
			int64(slot),
			epochNonce,
		) //nolint:gosec
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

// createTestTPraosBlock constructs a Shelley-through-Alonzo header with
// independently valid leader and nonce VRF certificates. The nonce
// certificate can be signed by another key or altered before the header body
// receives its valid KES signature, isolating nonce-VRF verification from the
// existing leader-VRF and KES checks.
func createTestTPraosBlock(
	t testing.TB,
	era lcommon.Era,
	blockType int,
	seed [32]byte,
	nonceSeed byte,
	tamper tpraosNonceTamper,
) *tpraosTestBlockResult {
	t.Helper()

	vrfPk, vrfSk, err := vrf.KeyGen(seed[:])
	require.NoError(t, err)

	nonceVrfSk := vrfSk
	if tamper == tpraosNonceMismatchedKey {
		otherSeed := seed
		otherSeed[0] ^= 0xCC
		_, nonceVrfSk, err = vrf.KeyGen(otherSeed[:])
		require.NoError(t, err)
	}

	kesSeed := seed
	kesSeed[0] ^= 0xAA
	kesSk, kesPk, err := kes.KeyGen(kes.CardanoKesDepth, kesSeed[:])
	require.NoError(t, err)

	coldSeed := seed
	coldSeed[0] ^= 0xBB
	coldPrivKey := ed25519.NewKeyFromSeed(coldSeed[:])
	coldPubKey := coldPrivKey.Public().(ed25519.PublicKey)

	const slotsPerKesPeriod = uint64(129600)
	const slot = uint64(1)
	epochNonce := make([]byte, 32)
	for i := range epochNonce {
		epochNonce[i] = nonceSeed + byte(i) //nolint:gosec
	}

	nonceInput, err := vrf.MkSeedTPraos(
		int64(slot),
		epochNonce,
		vrf.SeedEta(),
	)
	require.NoError(t, err)
	nonceProof, nonceOutput, err := vrf.Prove(nonceVrfSk, nonceInput)
	require.NoError(t, err)
	if tamper == tpraosNonceAlteredProof {
		nonceProof[0] ^= 0xFF
	}

	leaderInput, err := vrf.MkSeedTPraos(
		int64(slot),
		epochNonce,
		vrf.SeedL(),
	)
	require.NoError(t, err)
	leaderProof, leaderOutput, err := vrf.Prove(vrfSk, leaderInput)
	require.NoError(t, err)

	const opCertSeqNum = uint32(0)
	const opCertKesPeriod = uint32(0)
	var opCertBody [48]byte
	copy(opCertBody[:32], kesPk)
	binary.BigEndian.PutUint64(opCertBody[32:40], uint64(opCertSeqNum))
	binary.BigEndian.PutUint64(opCertBody[40:48], uint64(opCertKesPeriod))
	opCertSig := ed25519.Sign(coldPrivKey, opCertBody[:])

	headerBody := shelley.ShelleyBlockHeaderBody{
		BlockNumber: 1,
		Slot:        slot,
		IssuerVkey: func() lcommon.IssuerVkey {
			var key lcommon.IssuerVkey
			copy(key[:], coldPubKey)
			return key
		}(),
		VrfKey: vrfPk,
		NonceVrf: lcommon.VrfResult{
			Output: nonceOutput,
			Proof:  nonceProof,
		},
		LeaderVrf: lcommon.VrfResult{
			Output: leaderOutput,
			Proof:  leaderProof,
		},
		BlockBodySize:        1024,
		OpCertHotVkey:        kesPk,
		OpCertSequenceNumber: opCertSeqNum,
		OpCertKesPeriod:      opCertKesPeriod,
		OpCertSignature:      opCertSig,
		ProtoMajorVersion:    uint64(era.Id + 1), //nolint:gosec
	}
	headerBodyCbor, err := cbor.Encode(headerBody)
	require.NoError(t, err)
	headerBody.SetCbor(headerBodyCbor)
	kesSig, err := kes.Sign(kesSk, 0, headerBodyCbor)
	require.NoError(t, err)

	shelleyHeader := shelley.ShelleyBlockHeader{
		Body:      headerBody,
		Signature: kesSig,
	}
	var header gledger.BlockHeader
	switch era.Id {
	case shelley.EraIdShelley:
		header = &shelleyHeader
	case allegra.EraIdAllegra:
		header = &allegra.AllegraBlockHeader{
			ShelleyBlockHeader: shelleyHeader,
		}
	case mary.EraIdMary:
		header = &mary.MaryBlockHeader{
			ShelleyBlockHeader: shelleyHeader,
		}
	case alonzo.EraIdAlonzo:
		header = &alonzo.AlonzoBlockHeader{
			ShelleyBlockHeader: shelleyHeader,
		}
	default:
		t.Fatalf("unsupported TPraos test era %d", era.Id)
	}

	return &tpraosTestBlockResult{
		block: &realTPraosBlock{
			header:    header,
			era:       era,
			blockType: blockType,
		},
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

func TestVerifyBlockHeaderTPraosNonceVRF(t *testing.T) {
	eras := []struct {
		name      string
		era       lcommon.Era
		blockType int
	}{
		{name: "Shelley", era: shelley.EraShelley, blockType: shelley.BlockTypeShelley},
		{name: "Allegra", era: allegra.EraAllegra, blockType: allegra.BlockTypeAllegra},
		{name: "Mary", era: mary.EraMary, blockType: mary.BlockTypeMary},
		{name: "Alonzo", era: alonzo.EraAlonzo, blockType: alonzo.BlockTypeAlonzo},
	}
	tests := []struct {
		name    string
		tamper  tpraosNonceTamper
		wantErr bool
	}{
		{name: "valid", tamper: tpraosNonceValid},
		{
			name:    "mismatched_key",
			tamper:  tpraosNonceMismatchedKey,
			wantErr: true,
		},
		{
			name:    "altered_proof",
			tamper:  tpraosNonceAlteredProof,
			wantErr: true,
		},
	}
	for eraIdx, era := range eras {
		for testIdx, test := range tests {
			t.Run(era.name+"/"+test.name, func(t *testing.T) {
				var seed [32]byte
				seed[0] = byte(20 + eraIdx*len(tests) + testIdx)
				tb := createTestTPraosBlock(
					t,
					era.era,
					era.blockType,
					seed,
					byte(40+eraIdx),
					test.tamper,
				)
				err := verifyBlockHeader(
					tb.block,
					tb.epochNonce,
					tb.slotsPerKesPeriod,
				)
				if test.wantErr {
					require.ErrorContains(t, err, "nonce VRF")
					return
				}
				require.NoError(t, err)
			})
		}
	}
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

type mockBoundaryAlonzoBlock struct {
	gledger.Block
	slot uint64
}

func (m *mockBoundaryAlonzoBlock) Era() lcommon.Era {
	return alonzo.EraAlonzo
}

func (m *mockBoundaryAlonzoBlock) SlotNumber() uint64 {
	return m.slot
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

type realTPraosBlock struct {
	header    gledger.BlockHeader
	era       lcommon.Era
	blockType int
}

func (b *realTPraosBlock) Era() lcommon.Era { return b.era }

func (b *realTPraosBlock) SlotNumber() uint64 { return b.header.SlotNumber() }

func (b *realTPraosBlock) Hash() lcommon.Blake2b256 { return b.header.Hash() }

func (b *realTPraosBlock) PrevHash() lcommon.Blake2b256 {
	return b.header.PrevHash()
}

func (b *realTPraosBlock) BlockNumber() uint64 {
	return b.header.BlockNumber()
}

func (b *realTPraosBlock) IssuerVkey() lcommon.IssuerVkey {
	return b.header.IssuerVkey()
}

func (b *realTPraosBlock) BlockBodySize() uint64 {
	return b.header.BlockBodySize()
}

func (b *realTPraosBlock) Cbor() []byte { return nil }

func (b *realTPraosBlock) BlockBodyHash() lcommon.Blake2b256 {
	return b.header.BlockBodyHash()
}

func (b *realTPraosBlock) Header() lcommon.BlockHeader { return b.header }

func (b *realTPraosBlock) Type() int { return b.blockType }

func (b *realTPraosBlock) Transactions() []lcommon.Transaction { return nil }

func (b *realTPraosBlock) Utxorpc() (*utxorpc_cardano.Block, error) {
	return nil, nil
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()
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

// TestVerifyBlockHeaderCrypto_ByronValidated verifies that the
// LedgerState-level method applies the Byron PBFT path before skipping the
// Praos-only epoch and nonce lookups.
func TestVerifyBlockHeaderCrypto_ByronValidated(t *testing.T) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(t, block, 10),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	err = ls.verifyBlockHeaderCrypto(block)
	assert.NoError(t, err, "valid Byron PBFT headers should pass")
}

func TestVerifyBlockHeaderOnlyCryptoRejectsTamperedByronSignature(
	t *testing.T,
) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	header, ok := block.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	require.Len(t, header.ConsensusData.BlockSig, 2)
	proxySignature, ok := header.ConsensusData.BlockSig[1].([]any)
	require.True(t, ok)
	require.Len(t, proxySignature, 2)
	signature, ok := proxySignature[1].([]byte)
	require.True(t, ok)
	require.NotEmpty(t, signature)
	signature[0] ^= 0xff

	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(t, block, 10),
		},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	err = ls.verifyBlockHeaderOnlyCrypto(header)
	require.ErrorContains(t, err, "byron PBFT header verification failed")
	require.ErrorContains(t, err, "signature")
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
	ls.publishSnapshotsLocked()
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

func TestHeaderVerificationEpochRejectsPastForecastBeforeCacheAdvance(
	t *testing.T,
) {
	const horizonSlot = uint64(532_000)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		epochCache: []models.Epoch{
			{
				EpochId:       500,
				StartSlot:     100_000,
				SlotLength:    1_000,
				LengthInSlots: 432_000,
				EraId:         eras.ConwayEraDesc.Id,
				Nonce:         []byte{0x01},
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	}
	ls.publishSnapshotsLocked()

	epoch, err := ls.headerVerificationEpoch(horizonSlot-1, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), epoch.EpochId)

	_, err = ls.headerVerificationEpoch(horizonSlot, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
	assert.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
		"past-horizon rejection must happen before forecast cache mutation")
}

func TestValidateBlockHeaderCryptoDoesNotAdvanceEpochCache(t *testing.T) {
	const futureSlot = uint64(1001)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte{0x01},
		}},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.ValidateBlockHeaderCrypto(&mockBabbageBlock{slot: futureSlot})
	require.Error(t, err)
	assert.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
		"header-only validation must not advance the shared epoch cache")
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
	ls.publishSnapshotsLocked()
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

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

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
	ls.publishSnapshotsLocked()

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

func TestVerifyBlockHeaderOnlyCryptoSkipsStatefulPoolChecks(t *testing.T) {
	tb := createTestBlock(t, [32]byte{43}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)

	err := ls.verifyBlockHeaderOnlyCrypto(tb.block.Header())
	require.NoError(t, err)

	err = ls.verifyBlockHeaderCrypto(tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, models.ErrPoolNotFound)
}

func TestVerifyBlockHeaderCryptoBeforeApplyDefersMissingPoolState(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{44}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.verifyBlockHeaderCryptoBeforeApply(tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, errHeaderVerificationDeferred)
	assert.True(t, IsHeaderVerificationDeferred(err))

	err = ls.verifyBlockHeaderCrypto(tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, models.ErrPoolNotFound)
}

func TestVerifyBlockHeaderCryptoBeforeApplyDefersEmptyMarkSnapshot(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{45}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()
	seedBlockPoolRegistration(t, db, tb.block)

	err := ls.verifyBlockHeaderCryptoBeforeApply(tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, errHeaderVerificationDeferred)
	assert.Contains(t, err.Error(), "leader stake snapshot state")

	err = ls.verifyBlockHeaderCrypto(tb.block)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errHeaderVerificationDeferred)
	assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

func TestVerifyDeferredBlockHeaderStateRunsStrictlyAtApply(t *testing.T) {
	tb := createTestBlock(t, [32]byte{46}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())

	ls.markDeferredHeaderValidation(point)
	err := ls.verifyDeferredBlockHeaderState(nil, point, tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, models.ErrPoolNotFound)
	assert.False(t, ls.consumeDeferredHeaderValidation(point))

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedBlockPoolRegistration(t, db, tb.block)
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	ls.markDeferredHeaderValidation(point)
	err = ls.verifyDeferredBlockHeaderState(nil, point, tb.block)
	require.NoError(t, err)
	assert.False(t, ls.consumeDeferredHeaderValidation(point))
}

func TestVerifyDeferredBlockHeaderStateSurvivesRestartMarker(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{48}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedBlockPoolRegistration(t, db, tb.block)
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	require.NoError(t, ls.persistDeferredHeaderValidation(point, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.verifyDeferredBlockHeaderState(txn, point, tb.block)
	}))

	value, err := db.GetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, value)
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
	ls.publishSnapshotsLocked()
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
	ls.publishSnapshotsLocked()

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
	err := cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	)
	require.NoError(t, err)
	return cfg
}

func newGenesisDelegateShelleyGenesisCfg(
	t testing.TB,
	delegateHashHex string,
	vrfHashHex string,
) *cardano.CardanoNodeConfig {
	t.Helper()
	return newGenesisDelegateShelleyGenesisCfgWithActiveSlots(
		t,
		delegateHashHex,
		vrfHashHex,
		"0.99",
	)
}

func newGenesisDelegateShelleyGenesisCfgWithActiveSlots(
	t testing.TB,
	delegateHashHex string,
	vrfHashHex string,
	activeSlotsCoeff string,
) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": ` + activeSlotsCoeff + `,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z",
		"protocolParams": {
			"decentralisationParam": 1
		},
		"genDelegs": {
			"` + strings.Repeat("11", 28) + `": {
				"delegate": "` + delegateHashHex + `",
				"vrf": "` + vrfHashHex + `"
			}
		}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	)
	require.NoError(t, err)
	return cfg
}

func seedGenesisDelegation(
	t testing.TB,
	db *database.Database,
	row models.GenesisDelegation,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO genesis_delegation (
    genesis_hash, genesis_delegate_hash, vrf_key_hash,
    added_slot, block_index, cert_index, certificate_id
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		row.GenesisHash,
		row.GenesisDelegateHash,
		row.VrfKeyHash,
		row.AddedSlot,
		row.BlockIndex,
		row.CertIndex,
		row.CertificateID,
	)
	require.NoError(t, err)
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
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

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
	ls.publishSnapshotsLocked()
	return ls, db
}

func TestVerifyBlockHeaderState_GenesisDelegateSkipsPoolChecks(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{49}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfg(
		t,
		hex.EncodeToString(delegateHash.Bytes()),
		hex.EncodeToString(vrfHash.Bytes()),
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.NoError(t, err)
}

func TestVerifyBlockHeaderState_GenesisDelegateVRFMismatchFails(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{50}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfg(
		t,
		hex.EncodeToString(delegateHash.Bytes()),
		strings.Repeat("00", lcommon.Blake2b256Size),
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	ls.publishSnapshotsLocked()

	err := ls.verifyBlockHeaderState(tb.block, 5, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "genesis delegate")
	assert.Contains(t, err.Error(), "VRF key does not match")
}

func TestVerifyBlockHeaderState_GenesisDelegateInactiveAtDZero(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{51}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfg(
		t,
		hex.EncodeToString(delegateHash.Bytes()),
		hex.EncodeToString(vrfHash.Bytes()),
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(0, 1)},
	}
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, models.ErrPoolNotFound)
}

func TestGenesisOverlayUsesEffectiveEpochPParamsAtBoundary(t *testing.T) {
	genesisCfg := newGenesisDelegateShelleyGenesisCfgWithActiveSlots(
		t,
		strings.Repeat("00", lcommon.Blake2b224Size),
		strings.Repeat("00", lcommon.Blake2b256Size),
		"0.05",
	)
	initialPParams := &alonzo.AlonzoProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId:       1,
			StartSlot:     86_400,
			LengthInSlots: 86_400,
			SlotLength:    1,
			EraId:         eras.AlonzoEraDesc.Id,
		},
		currentEra:     eras.AlonzoEraDesc,
		currentPParams: initialPParams,
		epochCache: []models.Epoch{
			{
				EpochId:       1,
				StartSlot:     86_400,
				LengthInSlots: 86_400,
				SlotLength:    1,
				EraId:         eras.AlonzoEraDesc.Id,
			},
			{
				EpochId:       2,
				StartSlot:     172_800,
				LengthInSlots: 86_400,
				SlotLength:    1,
				EraId:         eras.AlonzoEraDesc.Id,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: genesisCfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// The epoch-2 update is already part of historical metadata even though
	// the in-memory current epoch is still epoch 1. This is the state observed
	// while a boundary block is being checked.
	nextPParams := &alonzo.AlonzoProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(0, 1)},
	}
	nextPParamsCbor, err := cbor.Encode(nextPParams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		nextPParamsCbor,
		172_800,
		2,
		eras.AlonzoEraDesc.Id,
		nil,
	))

	// The preceding epoch has an active overlay slot, while canonical epoch-2
	// slots use decentralisationParam=0 and must fall through to the normal
	// pool path. A current-epoch-only lookup regresses here by reading epoch-1's
	// d=1 and returning genesisOverlayNonActive for the epoch-2 block.
	precedingBlock := &mockBoundaryAlonzoBlock{
		Block: &mockBabbageBlock{slot: 172_780},
		slot:  172_780,
	}
	_, status, err := ls.genesisOverlayDelegationForBlock(
		precedingBlock,
		genesisCfg.ShelleyGenesis(),
	)
	require.NoError(t, err)
	require.Equal(t, genesisOverlayActive, status)

	block := &mockBoundaryAlonzoBlock{
		Block: &mockBabbageBlock{slot: 172_836},
		slot:  172_836,
	}
	_, status, err = ls.genesisOverlayDelegationForBlock(
		block,
		genesisCfg.ShelleyGenesis(),
	)
	require.NoError(t, err)
	assert.Equal(t, genesisOverlayNone, status)
}

func TestGenesisOverlayBoundaryBlockUsesBodyEraPParams(t *testing.T) {
	tb := createTestBlock(t, [32]byte{52}, 0, tamperNone)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	previousPParams := &alonzo.AlonzoProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	previousPParamsCbor, err := cbor.Encode(previousPParams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		previousPParamsCbor,
		0,
		0,
		eras.AlonzoEraDesc.Id,
		nil,
	))

	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId:       1,
			StartSlot:     86_400,
			LengthInSlots: 86_400,
			SlotLength:    1,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentEra: eras.BabbageEraDesc,
		currentPParams: &babbage.BabbageProtocolParameters{
			ProtocolMajor: eras.BabbageEraDesc.MinMajorVersion,
		},
		epochCache: []models.Epoch{
			{
				EpochId:       1,
				StartSlot:     86_400,
				LengthInSlots: 86_400,
				SlotLength:    1,
				EraId:         eras.BabbageEraDesc.Id,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newGenesisDelegateShelleyGenesisCfg(
				t,
				hex.EncodeToString(delegateHash.Bytes()),
				hex.EncodeToString(vrfHash.Bytes()),
			),
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	block := &mockBoundaryAlonzoBlock{
		Block: tb.block,
		slot:  86_400,
	}
	pparams := ls.genesisOverlayProtocolParamsForBlock(block)
	require.NotNil(t, pparams)
	assert.Equal(t, big.NewRat(1, 1), decentralizationParamRat(pparams))
	handled, err := ls.verifyGenesisDelegateHeader(block, false)
	require.NoError(t, err)
	assert.True(t, handled)
}

func TestGenesisOverlayBoundaryBlockUsesBoundaryEpochPredecessorPParams(
	t *testing.T,
) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	for _, tc := range []struct {
		epoch uint64
		d     *big.Rat
	}{
		{epoch: 0, d: big.NewRat(1, 2)},
		{epoch: 1, d: big.NewRat(1, 1)},
	} {
		encoded, encodeErr := cbor.Encode(&alonzo.AlonzoProtocolParameters{
			Decentralization: &cbor.Rat{Rat: tc.d},
		})
		require.NoError(t, encodeErr)
		require.NoError(t, db.SetPParams(
			encoded,
			86_400*tc.epoch,
			tc.epoch,
			eras.AlonzoEraDesc.Id,
			nil,
		))
	}

	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId:       1,
			StartSlot:     86_400,
			LengthInSlots: 86_400,
			SlotLength:    1,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentEra: eras.BabbageEraDesc,
		currentPParams: &babbage.BabbageProtocolParameters{
			ProtocolMajor: eras.BabbageEraDesc.MinMajorVersion,
		},
		epochCache: []models.Epoch{{
			EpochId:       1,
			StartSlot:     86_400,
			LengthInSlots: 86_400,
			SlotLength:    1,
			EraId:         eras.BabbageEraDesc.Id,
		}},
	}
	ls.publishSnapshotsLocked()

	block := &mockBoundaryAlonzoBlock{
		Block: &mockBabbageBlock{slot: 86_400},
		slot:  86_400,
	}
	pparams := ls.genesisOverlayProtocolParamsForBlock(block)
	require.NotNil(t, pparams)
	require.Equal(t, big.NewRat(1, 1), decentralizationParamRat(pparams))
}

func TestVerifyBlockHeaderState_GenesisDelegateInactiveOverlaySlotFails(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{52}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfgWithActiveSlots(
		t,
		hex.EncodeToString(delegateHash.Bytes()),
		hex.EncodeToString(vrfHash.Bytes()),
		"0.001",
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved for the genesis overlay schedule")
	assert.Contains(t, err.Error(), "not active")

	// Header verification can run ahead of ledger apply. In that path the
	// current epoch's protocol parameters may still be in memory, so defer a
	// state-dependent overlay rejection until the epoch rollover is committed.
	err = ls.verifyBlockHeaderState(tb.block, 5, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, errHeaderVerificationDeferred)
}

func TestVerifyBlockHeaderState_GenesisDelegateNonOverlaySlotUsesPoolThreshold(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{54}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfg(
		t,
		hex.EncodeToString(delegateHash.Bytes()),
		hex.EncodeToString(vrfHash.Bytes()),
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1000)},
	}
	ls.publishSnapshotsLocked()
	seedBlockPoolRegistration(t, db, tb.block)
	seedPoolStakeSnapshot(t, db, 4, delegateHash.Bytes(), 1)
	dummyPool := make([]byte, lcommon.Blake2b224Size)
	dummyPool[0] = 0xFF
	seedPoolStakeSnapshot(t, db, 4, dummyPool, 1_000_000_000_000_000_000)

	_, status, err := ls.genesisOverlayDelegationForBlock(
		tb.block,
		ls.config.CardanoNodeConfig.ShelleyGenesis(),
	)
	require.NoError(t, err)
	require.Equal(t, genesisOverlayNone, status)

	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.Error(
		t,
		err,
		"a non-overlay slot must apply the Praos leader threshold",
	)
	assert.Contains(t, err.Error(), "VRF leader value exceeds stake-derived threshold")

	// A stale decentralized parameter set can classify this future slot as
	// genesisOverlayNone. Header verification must defer before that
	// classification can bypass the apply-time state recheck.
	err = ls.verifyBlockHeaderState(tb.block, 5, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, errHeaderVerificationDeferred)
}

// TestVerifyBlockHeaderState_UnavailableSnapshotRecoverableVsGenuine pins the
// consensus-sensitive scoping of the deferral (issue #3727, finding 4). A
// leader-stake snapshot reported unavailable is only recoverable while the
// apply cursor is still BEHIND the header's slot (the mark snapshot has not
// been produced yet) -> defer. Once the cursor has caught up, a still-empty
// distribution is a genuine, permanent gap for that epoch and MUST stay a hard
// rejection -- deferring it forever would adopt a block whose leader
// eligibility is never checked, or loop. A producer absent from a POPULATED
// snapshot is authoritative ineligibility (VRFKeyUnknown) and hard-rejects
// regardless of the cursor.
func TestVerifyBlockHeaderState_UnavailableSnapshotRecoverableVsGenuine(
	t *testing.T,
) {
	dummyPool := func() []byte {
		p := make([]byte, lcommon.Blake2b224Size)
		p[0] = 0xEE
		return p
	}

	t.Run(
		"unavailable snapshot with tip BEHIND defers (recoverable)",
		func(t *testing.T) {
			tb := createTestBlock(t, [32]byte{60}, 0, tamperNone)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			// Tip behind the block slot: the mark snapshot for this slot may
			// not be computed yet, so the empty distribution is recoverable.
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.Point{Slot: tb.block.SlotNumber() - 1},
			}
			ls.publishSnapshotsLocked()
			require.True(
				t,
				ls.ledgerTipBehindSlot(tb.block.SlotNumber()),
				"tip must be behind so the recoverable branch can defer",
			)
			seedBlockPoolRegistration(t, db, tb.block)

			err := ls.verifyBlockHeaderState(tb.block, 5, true)
			require.Error(t, err)
			assert.True(
				t,
				IsHeaderVerificationDeferred(err),
				"tip-behind unavailable snapshot must defer: %v",
				err,
			)
			assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
		},
	)

	t.Run(
		"genuinely empty snapshot with tip AHEAD hard-rejects",
		func(t *testing.T) {
			tb := createTestBlock(t, [32]byte{60}, 0, tamperNone)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			// Tip advanced PAST the slot: an epoch whose distribution is still
			// empty here is a genuine, permanent gap -- the original hard
			// rejection must be preserved (finding 4), NOT deferred forever.
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.Point{Slot: tb.block.SlotNumber() + 1_000},
			}
			ls.publishSnapshotsLocked()
			require.False(
				t,
				ls.ledgerTipBehindSlot(tb.block.SlotNumber()),
				"tip must be ahead so the genuine-gap rejection applies",
			)
			seedBlockPoolRegistration(t, db, tb.block)
			// No rows for the required mark epoch: empty/unavailable.

			err := ls.verifyBlockHeaderState(tb.block, 5, true)
			require.Error(t, err)
			assert.False(
				t,
				IsHeaderVerificationDeferred(err),
				"tip-ahead genuinely-empty snapshot must hard-reject, not defer: %v",
				err,
			)
			assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
		},
	)

	t.Run(
		"populated snapshot absent pool hard-rejects",
		func(t *testing.T) {
			tb := createTestBlock(t, [32]byte{61}, 0, tamperNone)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			ls.currentTip = ochainsync.Tip{
				Point: ocommon.Point{Slot: tb.block.SlotNumber() + 1_000},
			}
			ls.publishSnapshotsLocked()

			seedBlockPoolRegistration(t, db, tb.block)
			// Populate the epoch-4 mark distribution with a DIFFERENT pool so
			// the snapshot is present (total > 0) and the producer is
			// genuinely absent from it -- authoritative ineligibility.
			seedPoolStakeSnapshot(t, db, 4, dummyPool(), 1_000_000_000)

			err := ls.verifyBlockHeaderState(tb.block, 5, true)
			require.Error(t, err)
			assert.False(
				t,
				IsHeaderVerificationDeferred(err),
				"populated absent-pool header must hard-reject: %v",
				err,
			)
			assert.NotErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
			assert.Contains(t, err.Error(), "has no stake in epoch")
		},
	)
}

// TestOldestRequiredSnapshotEpoch covers the retention-floor provider the
// snapshot manager consults to keep a deferred header's required snapshot from
// being pruned (issue #3727). The floor is the minimum over all outstanding
// deferred headers of StakeSnapshotEpoch(epochOf(slot)); with none deferred it
// reports no pin.
func TestOldestRequiredSnapshotEpoch(t *testing.T) {
	tb := createTestBlock(t, [32]byte{62}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Build an epoch cache mapping distinct slot ranges to epochs 11, 14, 22
	// so the required mark epochs are 10, 13, 21 (StakeSnapshotEpoch = E-1).
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 14, StartSlot: 1_400, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 22, StartSlot: 2_200, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	// No deferred headers => no pin.
	if _, ok := ls.OldestRequiredSnapshotEpoch(); ok {
		t.Fatalf("expected no retention pin when nothing is deferred")
	}

	// Defer three headers in epochs 22, 14, 11. The oldest required snapshot
	// epoch is StakeSnapshotEpoch(11) == 10.
	ls.markDeferredHeaderValidation(ocommon.Point{Slot: 2_250, Hash: []byte{0x22}})
	ls.markDeferredHeaderValidation(ocommon.Point{Slot: 1_450, Hash: []byte{0x14}})
	ls.markDeferredHeaderValidation(ocommon.Point{Slot: 1_150, Hash: []byte{0x11}})

	floor, ok := ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok, "a deferred header must produce a retention pin")
	assert.Equal(t, uint64(10), floor)

	// Resolving the epoch-11 header releases the pin up to the next-oldest
	// required snapshot epoch, StakeSnapshotEpoch(14) == 13.
	ls.clearDeferredHeaderValidation(ocommon.Point{Slot: 1_150, Hash: []byte{0x11}})
	floor, ok = ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(13), floor)

	// A deferred slot outside the published epoch cache cannot be mapped to a
	// snapshot epoch yet. While ANY deferred slot is unmappable the provider
	// must signal retain-all (floor 0, ok true) so cleanup prunes nothing --
	// otherwise the snapshot this header will need once the cache advances
	// could be pruned now, looping the header on defer (issue #3727, gap 2).
	ls.markDeferredHeaderValidation(ocommon.Point{Slot: 9_999_999, Hash: []byte{0xFF}})
	floor, ok = ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok, "an unmappable deferred slot must still pin (retain-all)")
	assert.Equal(
		t,
		uint64(0),
		floor,
		"unmappable deferred slot must force retain-all (floor 0)",
	)

	// Once the unmappable slot is dropped, the floor returns to the real
	// minimum over the remaining mappable headers.
	ls.clearDeferredHeaderValidation(ocommon.Point{Slot: 9_999_999, Hash: []byte{0xFF}})
	floor, ok = ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(13), floor)
}

func TestVerifyBlockHeaderState_GenesisDelegateUsesActiveDelegation(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{55}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	delegateHash := tb.block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	vrfHash := lcommon.Blake2b256Hash(vrfKey)
	ls.config.CardanoNodeConfig = newGenesisDelegateShelleyGenesisCfg(
		t,
		strings.Repeat("aa", lcommon.Blake2b224Size),
		strings.Repeat("bb", lcommon.Blake2b256Size),
	)
	ls.currentPParams = &shelley.ShelleyProtocolParameters{
		Decentralization: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	ls.publishSnapshotsLocked()
	seedGenesisDelegation(t, db, models.GenesisDelegation{
		GenesisHash:         bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size),
		GenesisDelegateHash: delegateHash.Bytes(),
		VrfKeyHash:          vrfHash.Bytes(),
		AddedSlot:           0,
		BlockIndex:          0,
		CertIndex:           0,
	})

	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.NoError(t, err)
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

func seedBlockPoolRegistration(
	t *testing.T,
	db *database.Database,
	block gledger.Block,
) {
	t.Helper()
	poolKeyHash := block.IssuerVkey().Hash()
	vrfKey, ok, err := headerVrfKeyFromBodyCbor(block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	seedPoolRegistration(
		t,
		db,
		poolKeyHash[:],
		lcommon.Blake2b256Hash(vrfKey).Bytes(),
	)
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
func TestVerifyBlockLeaderEligibility_EarlyEpochUsesGenesisSnapshot(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{35}, 0, tamperNone)
	// Use epoch 5 nonce for the genesis epoch cache entry; the actual nonce
	// is not used by verifyBlockLeaderEligibility itself.
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// Override epoch cache to place the block in epoch 1 (snapshotEpoch = 0).
	ls.epochCache = []models.Epoch{
		{
			EpochId:       1,
			StartSlot:     0,
			LengthInSlots: 1_000_000,
			Nonce:         tb.epochNonce,
		},
	}
	ls.publishSnapshotsLocked()

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

// TestVerifyBlockLeaderEligibility_Issue2876RewardInclusiveStake is a golden
// regression for Preview block 5ac88b23fb3060edbc1e478976b75033b82b64941d6b9830559f938b31e915a8
// at slot 117744396. The block is a deliberately narrow CPraos winner: the
// reward-stale distribution reported by dingo v0.65.1 rejects it, while the
// reward-inclusive active distribution accepts it. This pins the fact that a
// seemingly small stake discrepancy cannot be tolerated in consensus.
func TestVerifyBlockLeaderEligibility_Issue2876RewardInclusiveStake(
	t *testing.T,
) {
	const (
		epoch               = uint64(1362)
		snapshotEpoch       = uint64(1361)
		slot                = uint64(117744396)
		stalePoolStake      = uint64(21_256_151_898_192)
		staleTotalStake     = uint64(3_267_403_053_048_802)
		referencePoolStake  = uint64(21_281_013_692_685)
		referenceTotalStake = uint64(3_268_194_725_993_512)
	)

	issuerVkey, err := hex.DecodeString(
		"959dc65b2759195a259bce816606dfc6b4772c0544a2668595cb2629be0e48f7",
	)
	require.NoError(t, err)
	vrfOutput, err := hex.DecodeString(
		"ee98d65a916622d87cc9661dd9440884cf7fbca9bf6e183b99dfcf2bd915b7b" +
			"e66ad2e184fdfa0bf7e4c5e5c47733b83b98fbe1e336e00c7ceca3a7832f04b64",
	)
	require.NoError(t, err)

	var issuer lcommon.IssuerVkey
	copy(issuer[:], issuerVkey)
	headerBody := babbage.BabbageBlockHeaderBody{
		Slot:       slot,
		IssuerVkey: issuer,
		VrfResult: lcommon.VrfResult{
			Output: vrfOutput,
		},
	}
	headerBodyCbor, err := cbor.Encode(headerBody)
	require.NoError(t, err)
	headerBody.SetCbor(headerBodyCbor)
	block := &realBabbageBlock{
		header: &babbage.BabbageBlockHeader{Body: headerBody},
		era:    babbage.EraBabbage,
		slot:   slot,
	}

	for _, test := range []struct {
		name       string
		poolStake  uint64
		totalStake uint64
		wantErr    bool
	}{
		{
			name:       "v0.65.1 reward-stale snapshot rejects",
			poolStake:  stalePoolStake,
			totalStake: staleTotalStake,
			wantErr:    true,
		},
		{
			name:       "reward-inclusive snapshot accepts",
			poolStake:  referencePoolStake,
			totalStake: referenceTotalStake,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ls, db := newEligibilityTestLedger(t, nil)
			ls.config.CardanoNodeConfig = newTestShelleyGenesisCfg(t)
			ls.epochCache = []models.Epoch{{
				EpochId:       epoch,
				StartSlot:     slot - 1,
				LengthInSlots: 2,
				EraId:         eras.ConwayEraDesc.Id,
			}}
			ls.publishSnapshotsLocked()

			poolKeyHash := block.IssuerVkey().Hash()
			seedPoolStakeSnapshot(
				t, db, snapshotEpoch, poolKeyHash[:], test.poolStake,
			)
			dummyPool := bytes.Repeat([]byte{0xff}, lcommon.Blake2b224Size)
			seedPoolStakeSnapshot(
				t,
				db,
				snapshotEpoch,
				dummyPool,
				test.totalStake-test.poolStake,
			)

			err := ls.verifyBlockLeaderEligibility(block, epoch)
			if test.wantErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"VRF leader value exceeds stake-derived threshold",
				)
				return
			}
			require.NoError(t, err)
		})
	}
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
	ls.publishSnapshotsLocked()

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
	ls.publishSnapshotsLocked()

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
	assert.Contains(
		t,
		err.Error(),
		"VRF leader value exceeds stake-derived threshold",
	)
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
	assert.Contains(
		t,
		err.Error(),
		"VRF leader value exceeds stake-derived threshold",
	)
}

func TestVerifyBlockHeaderCrypto_SkipLeaderStakeThresholdCheckWarnsAndAccepts(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{41}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	var logBuf bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(
		&logBuf,
		&slog.HandlerOptions{Level: slog.LevelWarn},
	))
	ls.config.SkipLeaderStakeThresholdCheck = true

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1)
	dummyHash := make([]byte, 28)
	dummyHash[0] = 0xFF
	seedPoolStakeSnapshot(t, db, 4, dummyHash, 1_000_000_000_000_000_000)
	seedBlockPoolRegistration(t, db, tb.block)

	err := ls.verifyBlockHeaderCrypto(tb.block)
	require.NoError(t, err)
	logs := logBuf.String()
	assert.Contains(
		t,
		logs,
		"leader eligibility below stake-derived threshold; trusting block",
	)
	assert.Contains(t, logs, "leadership stake omits reward balances")
}

func TestVerifyBlockHeaderCrypto_EmptyMarkSnapshotDiagnostic(t *testing.T) {
	tb := createTestBlock(t, [32]byte{42}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	seedBlockPoolRegistration(t, db, tb.block)

	err := ls.verifyBlockHeaderCrypto(tb.block)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "epoch mark snapshot is empty")
	assert.Contains(t, err.Error(), "has no stake in epoch")
}

func TestVerifyBlockLeaderEligibility_MithrilImportedHistoricalMarkChecks(
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

	// Leader election in epoch 5 uses mark[StakeSnapshotEpoch(5)] = mark[4].
	// Older Dingo imports stamped the certified NewEpochState SnapShots.Mark
	// row with the mid-epoch Mithril anchor. That provenance is still
	// authoritative even though the stored capture slot is after epoch 4's
	// start, so existing databases must run the threshold check.
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
	require.False(t, ls.shouldSkipPostMithrilMarkEligibility(snapshot, 4))
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"VRF leader value exceeds stake-derived threshold",
	)
	assert.NotContains(t, logBuf.String(), "skipping leader eligibility check")
}

func TestVerifyBlockLeaderEligibility_ReconstructedHistoricalMarkSkips(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{40}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	var logBuf bytes.Buffer
	ls.config.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))
	ls.epochCache = []models.Epoch{
		{EpochId: 3, StartSlot: 300, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 4, StartSlot: 400, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 5, StartSlot: 500, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.mithrilLedgerSlot = ls.epochCache[1].StartSlot + 50
	tb.block.slot = ls.epochCache[2].StartSlot + 50

	// The startup fallback derives historical rows from current live state and
	// stamps them with the current epoch start. Unlike a certified imported
	// row, this capture is neither the target boundary nor the Mithril anchor,
	// so hard threshold rejection remains unsafe.
	reconstructedCaptureSlot := ls.epochCache[1].StartSlot
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		1,
		0,
		reconstructedCaptureSlot,
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
		reconstructedCaptureSlot,
	)
	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		nil,
	)
	require.NoError(t, err)
	require.True(t, ls.shouldSkipPostMithrilMarkEligibility(snapshot, 4))
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.NoError(t, err)
	assert.Contains(t, logBuf.String(), "skipping leader eligibility check")
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
	require.False(t, ls.shouldSkipPostMithrilMarkEligibility(
		snapshot,
		snapshotEpoch,
	))
	ls.publishSnapshotsLocked()

	err = ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"VRF leader value exceeds stake-derived threshold",
	)
}

// newCoeffGuardLedger builds a ledger whose pool stake is already seeded, so
// verifyBlockLeaderEligibility reaches the active-slot-coefficient guard. cfg
// is the Shelley genesis under test (nil for "genesis never loaded"), and
// prototypeProfile selects the Musashi prototype bypass.
func newCoeffGuardLedger(
	t *testing.T,
	tb *testBlockResult,
	cfg *cardano.CardanoNodeConfig,
	prototypeProfile bool,
) *LedgerState {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)

	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:       5,
				StartSlot:     0,
				LengthInSlots: 1_000_000,
				Nonce:         tb.epochNonce,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig:             cfg,
			SkipLeaderStakeThresholdCheck: prototypeProfile,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// newZeroCoeffGenesisCfg returns a Shelley genesis with activeSlotsCoeff
// explicitly 0. big.Rat.SetString("0") gives Sign()==0, which the guard must
// catch: a zero coefficient produces a zero threshold, under which no VRF
// output is ever below the threshold.
func newZeroCoeffGenesisCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	zeroCoeffJSON := `{
		"activeSlotsCoeff": 0,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(zeroCoeffJSON)),
	)
	return cfg
}

// TestVerifyBlockLeaderEligibility_MissingActiveSlotsCoeffRejects verifies that
// an unavailable active slot coefficient (Shelley genesis not loaded) rejects
// the block on a standard profile. The coefficient is an input to the
// leadership threshold, so without it eligibility cannot be evaluated at all;
// accepting the block anyway admits an unverified producer.
func TestVerifyBlockLeaderEligibility_MissingActiveSlotsCoeffRejects(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{34}, 0, tamperNone)
	ls := newCoeffGuardLedger(t, tb, nil, false)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err, "unevaluable eligibility must not be accepted")
	assert.Contains(t, err.Error(), "active slot coefficient")
}

// TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffRejects covers the same
// guard for a genesis that loads but carries activeSlotsCoeff=0.
func TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffRejects(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{36}, 0, tamperNone)
	ls := newCoeffGuardLedger(t, tb, newZeroCoeffGenesisCfg(t), false)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err, "a zero coefficient must not be accepted")
	assert.Contains(t, err.Error(), "active slot coefficient")
}

// TestVerifyBlockLeaderEligibility_MissingActiveSlotsCoeffPrototypeAccepts
// pins the one profile that may still bypass the check. The Musashi prototype
// already trusts stake-derived threshold failures
// (SkipLeaderStakeThresholdCheck); an unevaluable threshold is bypassed under
// the same explicitly selected profile and nowhere else.
func TestVerifyBlockLeaderEligibility_MissingActiveSlotsCoeffPrototypeAccepts(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{34}, 0, tamperNone)
	ls := newCoeffGuardLedger(t, tb, nil, true)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "prototype profile keeps its documented bypass")
}

// TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffPrototypeAccepts is the
// zero-coefficient half of the prototype bypass.
func TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffPrototypeAccepts(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{36}, 0, tamperNone)
	ls := newCoeffGuardLedger(t, tb, newZeroCoeffGenesisCfg(t), true)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "prototype profile keeps its documented bypass")
}

// seedZeroTotalActiveStakeSummary marks the epoch's mark aggregate ready at
// zero. GetTotalActiveStake prefers a ready epoch_summary over summing the
// pool rows, so this reproduces the inconsistency the guard is about: the
// producing pool holds stake, yet the network-wide denominator reads zero.
func seedZeroTotalActiveStakeSummary(
	t *testing.T,
	db *database.Database,
	epoch uint64,
) {
	t.Helper()
	require.NoError(
		t,
		db.Metadata().SaveEpochSummary(epochSummary(epoch, 0), nil),
	)
}

// TestVerifyBlockLeaderEligibility_ZeroTotalActiveStakeRejects verifies that a
// zero total active stake rejects rather than accepting the block. The pool
// row carries stake, so this is a storage or computation gap in dingo's own
// aggregate, not a genuinely empty network — and the threshold's denominator
// is unusable either way.
func TestVerifyBlockLeaderEligibility_ZeroTotalActiveStakeRejects(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{37}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
	seedZeroTotalActiveStakeSummary(t, db, 4)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err, "a zero stake denominator must not be accepted")
	assert.Contains(t, err.Error(), "total active stake")
	// Classified as an unavailable snapshot so header verification running
	// ahead of the ledger apply cursor can defer instead of rejecting.
	assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

// TestVerifyBlockLeaderEligibility_ZeroTotalActiveStakePrototypeAccepts pins
// the prototype bypass for the stake half of the guard.
func TestVerifyBlockLeaderEligibility_ZeroTotalActiveStakePrototypeAccepts(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{37}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.SkipLeaderStakeThresholdCheck = true
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
	seedZeroTotalActiveStakeSummary(t, db, 4)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	assert.NoError(t, err, "prototype profile keeps its documented bypass")
}

// TestVerifyBlockHeaderCryptoBeforeApplyDefersZeroTotalActiveStake verifies
// that the new rejection does not break blockfetch header verification during
// catch-up: while the ledger apply cursor is behind the block, a missing
// aggregate defers, and it only becomes a rejection once the cursor has
// caught up.
func TestVerifyBlockHeaderCryptoBeforeApplyDefersZeroTotalActiveStake(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{38}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	seedBlockPoolRegistration(t, db, tb.block)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
	seedZeroTotalActiveStakeSummary(t, db, 4)

	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.verifyBlockHeaderCryptoBeforeApply(tb.block)
	require.Error(t, err)
	assert.ErrorIs(t, err, errHeaderVerificationDeferred)
	assert.Contains(t, err.Error(), "leader stake snapshot state")

	err = ls.verifyBlockHeaderCrypto(tb.block)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errHeaderVerificationDeferred)
	assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

// newImportedActiveLedger builds a ledger positioned inside a Mithril-imported
// epoch, so leaderEligibilityStake takes the imported active-distribution
// branch rather than the rotated mark snapshot.
func newImportedActiveLedger(
	t *testing.T,
	tb *testBlockResult,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	if tb.block.slot <= 1 {
		// Slot 0 disables the Mithril boundary sentinel; move the mock
		// block past it. This test exercises stake-source selection, not
		// VRF proof input.
		tb.block.slot = 2
	}
	ls.currentEpoch = models.Epoch{
		EpochId:       5,
		StartSlot:     0,
		LengthInSlots: 1_000_000,
		Nonce:         tb.epochNonce,
	}
	ls.mithrilLedgerSlot = tb.block.slot - 1
	ls.publishSnapshotsLocked()
	return ls, db
}

// TestVerifyBlockLeaderEligibility_ImportedActiveZeroDenominatorIsUnavailable
// verifies that a pool row carrying stake with a zero stake denominator is
// classified as an unavailable snapshot. The denominator is the threshold's
// divisor, so its absence means eligibility cannot be evaluated — a storage
// gap in the import, not a statement that the pool is ineligible.
func TestVerifyBlockLeaderEligibility_ImportedActiveZeroDenominatorIsUnavailable(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{39}, 0, tamperNone)
	ls, db := newImportedActiveLedger(t, tb)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshotOfType(
		t,
		db,
		5,
		models.PoolStakeSnapshotTypeActive,
		poolKeyHash[:],
		1_000_000_000,
		0,
	)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

// TestVerifyBlockLeaderEligibility_ImportedActiveEmptyDistributionIsUnavailable
// covers an imported epoch with no active rows at all. cardano-ledger's
// nesPd is always populated, so an empty one is dingo-side incompleteness.
func TestVerifyBlockLeaderEligibility_ImportedActiveEmptyDistributionIsUnavailable(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{40}, 0, tamperNone)
	ls, _ := newImportedActiveLedger(t, tb)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from active pool distribution")
	assert.ErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

// TestVerifyBlockLeaderEligibility_ImportedActivePoolAbsentStaysHardRejection
// is the negative case that keeps the classification honest: when the imported
// distribution is populated and this pool is simply not in it, the answer is
// authoritative. That is cardano-ledger's VRFKeyUnknown and must stay a
// rejection, not become a deferrable "snapshot unavailable".
func TestVerifyBlockLeaderEligibility_ImportedActivePoolAbsentStaysHardRejection(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{41}, 0, tamperNone)
	ls, db := newImportedActiveLedger(t, tb)
	otherPool := make([]byte, 28)
	otherPool[0] = 0xAB
	seedPoolStakeSnapshotOfType(
		t,
		db,
		5,
		models.PoolStakeSnapshotTypeActive,
		otherPool,
		1_000_000_000,
		1_000_000_000,
	)

	err := ls.verifyBlockLeaderEligibility(tb.block, 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing from active pool distribution")
	assert.NotErrorIs(t, err, errLeaderStakeSnapshotUnavailable)
}

// TestPrunePoolSnapshotsWithRetentionFloor_SerializesAdmission is the
// regression guard for the deferred-header admission <-> retention-floor race
// (issue #3727, gap 1). PrunePoolSnapshotsWithRetentionFloor must hold the
// deferred-header set stable across BOTH the floor computation and the prune,
// so a header admitted concurrently cannot slip in between the floor read and
// the prune and have its still-needed snapshot deleted under a stale boundary.
// The prune sees a floor computed from the set as it was when the guard took
// the lock, and a concurrent markDeferredHeaderValidation blocks until release.
// TestPrunePoolSnapshotsWithRetentionFloor_FloorReadIsAtomic replaces the former
// _SerializesAdmission test, whose contract (hold deferredHeaderValidationMu
// across prune) is exactly the lock-order inversion that deadlocks the node
// (issue #3717). The invariant that survives the fix is narrower but sufficient:
// the eviction + floor read happen under ONE lock hold, so the boundary handed
// to prune is a coherent read of the deferred set as it stood when the guard
// took the lock -- never a mix. prune then runs with the lock RELEASED. A header
// admitted during prune is NOT pinned by this pass; it is picked up by the next
// cleanup pass, because the retention floor is a lower-watermark recomputed every
// pass. That next-pass recovery is the deliberate trade for never inverting the
// lock order.
func TestPrunePoolSnapshotsWithRetentionFloor_FloorReadIsAtomic(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{63}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Map slots to epochs 11 (snapshot 10) and 14 (snapshot 13).
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 14, StartSlot: 1_400, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	// One header is already deferred at epoch 14 (needs snapshot 13) when the
	// guard runs.
	ls.markDeferredHeaderValidation(
		ocommon.Point{Slot: 1_450, Hash: []byte{0x14}},
	)

	pruneStarted := make(chan struct{})
	admitted := make(chan struct{})
	// Admit an epoch-11 header (needs snapshot 10) DURING prune. Because the
	// guard has already released the lock before calling prune, this admission
	// proceeds concurrently -- it does not (and must not) deadlock against the
	// guard, and it does not retroactively lower this pass's boundary.
	go func() {
		<-pruneStarted
		ls.markDeferredHeaderValidation(
			ocommon.Point{Slot: 1_150, Hash: []byte{0x11}},
		)
		close(admitted)
	}()

	var seenBefore uint64
	err := ls.PrunePoolSnapshotsWithRetentionFloor(
		25,
		0,
		func(before uint64) error {
			seenBefore = before
			close(pruneStarted)
			// Let the concurrent admission land while prune runs. On the old
			// (buggy) code the admission would block on the still-held mutex and
			// this receive would deadlock; on the fixed code it completes.
			testutil.RequireReceive(
				t,
				admitted,
				15*time.Second,
				"concurrent admission blocked while the guard was pruning (lock-order inversion, issue #3717)",
			)
			return nil
		},
	)
	require.NoError(t, err)

	// The floor this invocation pruned to reflects only the epoch-14 header
	// present when the guard took the lock (snapshot 13), NOT the epoch-11
	// header admitted during prune: the boundary is a coherent read, never a
	// mix of pre- and post-lock state.
	assert.Equal(t, uint64(13), seenBefore)

	// The concurrently-admitted header is now visible; the next cleanup pass
	// pins the lower floor (snapshot 10), so no needed snapshot is lost for good.
	floor, ok := ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(10), floor)

	var nextBefore uint64
	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(
		25,
		0,
		func(before uint64) error {
			nextBefore = before
			return nil
		},
	))
	assert.Equal(
		t,
		uint64(10),
		nextBefore,
		"next pass pins the floor of the concurrently-admitted header",
	)
}

// TestPrunePoolSnapshotsWithRetentionFloor_RealPruneNoDeadlock is the lock-order
// inversion regression guard for wolf31o2's blocking review on PR #3717. Unlike
// the other guard tests -- which pass a stub prune that opens no transaction and
// so cannot catch the bug -- this passes a REAL prune that opens the single
// sqlite write connection via db.Transaction(true), exactly as
// cleanupOldSnapshots' prunePoolSnapshots does. A second goroutine reproduces
// the block-apply order: hold that write connection, THEN take
// deferredHeaderValidationMu (as ledgerProcessBlock -> verifyDeferredBlockHeaderState
// -> consumeDeferredHeaderValidation does inside its write txn).
//
// The two lock orders:
//   - guard:  deferredHeaderValidationMu  THEN  write connection (prune)
//   - apply:  write connection            THEN  deferredHeaderValidationMu
//
// If the guard holds the mutex across prune (the pre-fix code), prune blocks
// acquiring the single write connection the apply goroutine holds, while the
// apply goroutine blocks acquiring the mutex the guard holds: deadlock. WITHOUT
// the fix this test times out (go test -timeout dumps the two inverted stacks);
// WITH the fix the guard releases the mutex before prune, the apply goroutine
// takes the mutex and releases the connection, and prune completes.
func TestPrunePoolSnapshotsWithRetentionFloor_RealPruneNoDeadlock(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 14, StartSlot: 1_400, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	// A deferred header so the guard does a real floor computation under the
	// lock, and so consume has a marker to clear on the apply side.
	deferred := ocommon.Point{Slot: 1_450, Hash: []byte{0x14}}
	ls.markDeferredHeaderValidation(deferred)
	require.NoError(t, ls.persistDeferredHeaderValidation(deferred, nil))

	connHeld := make(chan struct{})
	pruneReached := make(chan struct{})
	applyDone := make(chan struct{})

	// Apply-side order: hold the single write connection, THEN take the
	// deferred-header mutex via consumeDeferredHeaderValidation.
	go func() {
		applyTxn := ls.db.Transaction(true) // acquires the single write conn
		close(connHeld)
		// Wait until the guard is inside prune (mutex-then-conn on the buggy
		// path) before contending for the mutex, so the inversion is forced.
		<-pruneReached
		ls.consumeDeferredHeaderValidation(deferred) // needs the mutex
		_ = applyTxn.Rollback()                      // release the write conn
		close(applyDone)
	}()

	<-connHeld // apply goroutine now holds the single write connection

	guardDone := make(chan error, 1)
	go func() {
		guardDone <- ls.PrunePoolSnapshotsWithRetentionFloor(
			25,
			0,
			func(before uint64) error {
				// REAL prune: open the single write connection like production
				// (cleanupOldSnapshots' prunePoolSnapshots).
				close(pruneReached)
				poolTxn := ls.db.Transaction(true) // blocks until apply releases it
				defer func() { _ = poolTxn.Rollback() }()
				return db.Metadata().DeletePoolStakeSnapshotsBeforeEpoch(
					before,
					poolTxn.Metadata(),
				)
			},
		)
	}()

	// With the fix both goroutines complete promptly; without it they deadlock
	// on the single write connection and these receives time out.
	testutil.RequireReceive(
		t,
		applyDone,
		15*time.Second,
		"apply goroutine blocked taking the mutex while holding the write connection (lock-order inversion, issue #3717)",
	)
	err := testutil.RequireReceive(
		t,
		guardDone,
		15*time.Second,
		"retention guard blocked opening the write connection while holding the mutex (lock-order inversion, issue #3717)",
	)
	require.NoError(t, err)
}

// TestPrunePoolSnapshotsWithRetentionFloor_UnmappableRetainsAll is the
// regression guard for the unmappable-deferred-slot case (issue #3727, gap 2).
// While a deferred header's slot cannot yet be mapped to an epoch, the guard
// must retain ALL pool snapshots (prune boundary 0) so the snapshot the header
// will need once the epoch cache advances is not pruned in the meantime and the
// header driven into a defer loop. Once the mapping is published, normal floor
// pruning resumes.
func TestPrunePoolSnapshotsWithRetentionFloor_UnmappableRetainsAll(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{64}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	// Default cache (epoch 5, slots [0, 1_000_000)) does NOT cover the deferred
	// slot, so it is initially unmappable.
	const deferredSlot = uint64(50_000_000)
	ls.markDeferredHeaderValidation(
		ocommon.Point{Slot: deferredSlot, Hash: []byte{0xAB}},
	)

	// Seed mark snapshots for epochs 0..28.
	dummyPool := bytes.Repeat([]byte{0xEE}, lcommon.Blake2b224Size)
	for epoch := uint64(0); epoch <= 28; epoch++ {
		seedPoolStakeSnapshot(t, db, epoch, dummyPool, 1_000_000_000)
	}
	deleteBelow := func(before uint64) error {
		return db.Metadata().DeletePoolStakeSnapshotsBeforeEpoch(before, nil)
	}

	// Unmappable: the guard must hand prune boundary 0 (retain everything).
	var seenBefore uint64
	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(
		25,
		0,
		func(before uint64) error {
			seenBefore = before
			return deleteBelow(before)
		},
	))
	assert.Equal(
		t,
		uint64(0),
		seenBefore,
		"unmappable deferred slot must force retain-all",
	)
	for epoch := uint64(0); epoch <= 28; epoch++ {
		snaps, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err)
		require.Len(
			t,
			snaps,
			1,
			"epoch %d must be retained while the deferred slot is unmappable",
			epoch,
		)
	}

	// Publish an epoch mapping so the deferred slot resolves to epoch 22
	// (snapshot 21). Normal floor pruning resumes.
	ls.epochCache = []models.Epoch{
		{EpochId: 5, StartSlot: 0, LengthInSlots: 1_000_000, Nonce: tb.epochNonce},
		{
			EpochId:       22,
			StartSlot:     49_000_000,
			LengthInSlots: 2_000_000,
			Nonce:         tb.epochNonce,
		},
	}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(
		25,
		0,
		func(before uint64) error {
			seenBefore = before
			return deleteBelow(before)
		},
	))
	assert.Equal(
		t,
		uint64(21),
		seenBefore,
		"mappable deferred slot pins at StakeSnapshotEpoch(22)=21",
	)
	for epoch := uint64(0); epoch < 21; epoch++ {
		snaps, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err)
		require.Empty(
			t,
			snaps,
			"epoch %d below the pinned floor must be pruned",
			epoch,
		)
	}
	for epoch := uint64(21); epoch <= 28; epoch++ {
		snaps, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err)
		require.Len(
			t,
			snaps,
			1,
			"epoch %d at/above the pinned floor must be retained",
			epoch,
		)
	}
}

// TestRepopulateDeferredHeaderValidation is the restart-durability regression
// guard (issue #3727, finding 3). Deferred-header markers persisted before a
// restart must be reloaded into the in-memory set so the retention floor
// covers them on the first post-restart cleanup, instead of the set starting
// empty and the needed snapshot being pruned.
func TestRepopulateDeferredHeaderValidation(t *testing.T) {
	tb := createTestBlock(t, [32]byte{70}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 14, StartSlot: 1_400, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	// Persist two markers as a pre-restart node would, WITHOUT touching the
	// in-memory map (simulating the post-restart empty set).
	p11 := ocommon.Point{Slot: 1_150, Hash: []byte{0x11}}
	p14 := ocommon.Point{Slot: 1_450, Hash: []byte{0x14}}
	require.NoError(t, ls.persistDeferredHeaderValidation(p11, nil))
	require.NoError(t, ls.persistDeferredHeaderValidation(p14, nil))

	// Empty in-memory set: no pin yet.
	if _, ok := ls.OldestRequiredSnapshotEpoch(); ok {
		t.Fatalf("expected no pin before repopulation")
	}

	// Repopulate from persisted markers (as LedgerState.Start does).
	require.NoError(t, ls.repopulateDeferredHeaderValidation())

	floor, ok := ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok, "repopulated markers must produce a retention pin")
	assert.Equal(t, uint64(10), floor, "floor = StakeSnapshotEpoch(11)")
}

// TestRepopulateDeferredHeaderValidation_FailsClosedOnScanError is the
// regression guard for the swallowed marker-scan failure (issue #3727, P1
// re-review). If the persisted-marker scan fails at startup and the error is
// swallowed, the in-memory deferred set stays empty, the retention floor does
// not cover pre-restart deferred headers, and the first post-restart cleanup
// can prune a snapshot one of them needs -- after which stateful verification
// hard-rejects the missing snapshot instead of deferring. repopulate MUST
// surface the error so LedgerState.Start aborts rather than run unpinned.
func TestRepopulateDeferredHeaderValidation_FailsClosedOnScanError(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{71}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// Persist a marker, then close the database so the marker scan errors.
	require.NoError(t, ls.persistDeferredHeaderValidation(
		ocommon.Point{Slot: 1_150, Hash: []byte{0x11}}, nil,
	))
	dbtest.CloseDatabase(db) //nolint:errcheck

	err := ls.repopulateDeferredHeaderValidation()
	require.Error(
		t,
		err,
		"a marker-scan failure must be surfaced (fail closed), not swallowed",
	)
}

// TestDeletePersistedDeferredMarkers_SkipsReAdmitted is the regression guard
// for the evicted-marker delete racing a re-defer (issue #3727, P2 re-review).
// deletePersistedDeferredMarkers runs after eviction released the in-memory
// pin, so between eviction and the delete the same point can be re-deferred and
// re-persisted (it is still ahead of the lagging apply cursor). Deleting the
// marker for a point that is live in the in-memory set again would drop the
// durable pin, so the delete must skip any key present in the set; only a key
// that is genuinely absent (still evicted) may have its marker removed.
func TestDeletePersistedDeferredMarkers_SkipsReAdmitted(t *testing.T) {
	tb := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)

	reAdmitted := ocommon.Point{Slot: 1_150, Hash: []byte{0x11}}
	staleGone := ocommon.Point{Slot: 1_160, Hash: []byte{0x12}}
	reAdmittedKey := headerValidationPointKey(reAdmitted)
	staleKey := headerValidationPointKey(staleGone)

	// Both points have a persisted marker (as any deferred header would).
	require.NoError(t, ls.persistDeferredHeaderValidation(reAdmitted, nil))
	require.NoError(t, ls.persistDeferredHeaderValidation(staleGone, nil))

	// The re-admitted point is back in the in-memory set (re-deferred after the
	// eviction that produced the delete list); the stale one is not.
	ls.markDeferredHeaderValidation(reAdmitted)

	// Cleanup runs for BOTH evicted keys.
	ls.deletePersistedDeferredMarkers([]string{reAdmittedKey, staleKey})

	remaining, err := ls.db.ListSyncStateKeysByPrefix(
		deferredHeaderValidationSyncStatePrefix, nil,
	)
	require.NoError(t, err)
	// The re-admitted point's marker MUST survive (it now backs a live pin);
	// the genuinely-stale marker MUST be gone.
	assert.Contains(
		t,
		remaining,
		deferredHeaderValidationSyncStatePrefix+reAdmittedKey,
		"re-deferred point's marker must not be deleted",
	)
	assert.NotContains(
		t,
		remaining,
		deferredHeaderValidationSyncStatePrefix+staleKey,
		"genuinely stale marker must be deleted",
	)
}

// TestPrunePoolSnapshotsWithRetentionFloor_EvictsStaleBehindCursor is the
// unbounded-retention-leak guard (issue #3727, finding 5). A deferred header
// the apply cursor has already passed is abandoned (a canonical one would have
// been consumed at apply); the retention guard must evict it so it stops
// pinning its snapshot, and delete its persisted marker.
func TestPrunePoolSnapshotsWithRetentionFloor_EvictsStaleBehindCursor(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{71}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	// Apply cursor is WELL AHEAD of the deferred header's slot.
	ls.currentTip = ochainsync.Tip{Point: ocommon.Point{Slot: 500_000}}
	ls.publishSnapshotsLocked()

	stale := ocommon.Point{Slot: 1_150, Hash: []byte{0x11}}
	ls.markDeferredHeaderValidation(stale)
	require.NoError(t, ls.persistDeferredHeaderValidation(stale, nil))

	// Before: the abandoned header pins epoch 10.
	floor, ok := ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(10), floor)

	var seenBefore uint64
	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(
		25, 0,
		func(before uint64) error { seenBefore = before; return nil },
	))

	// The stale header is evicted: no pin remains, so the boundary is the
	// default (not lowered to 10), and the persisted marker is deleted.
	assert.Equal(t, uint64(25), seenBefore, "evicted header must not pin")
	_, ok = ls.OldestRequiredSnapshotEpoch()
	assert.False(t, ok, "abandoned header must be evicted from the set")
	marker, err := db.GetSyncState(deferredHeaderValidationSyncStateKey(stale), nil)
	require.NoError(t, err)
	assert.Empty(t, marker, "evicted header's persisted marker must be deleted")
}

// TestPrunePoolSnapshotsWithRetentionFloor_ResolveReleasesPin proves a deferred
// header that RESOLVES releases its pool-snapshot pin so the floor rises (issue
// #3727, finding 5).
func TestPrunePoolSnapshotsWithRetentionFloor_ResolveReleasesPin(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Both headers are AHEAD of the cursor (tip 0) so eviction does not fire.
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 14, StartSlot: 1_400, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	p11 := ocommon.Point{Slot: 1_150, Hash: []byte{0x11}}
	p14 := ocommon.Point{Slot: 1_450, Hash: []byte{0x14}}
	ls.markDeferredHeaderValidation(p11)
	ls.markDeferredHeaderValidation(p14)

	var seenBefore uint64
	record := func(before uint64) error { seenBefore = before; return nil }

	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(25, 0, record))
	assert.Equal(t, uint64(10), seenBefore, "pinned to oldest (epoch 10)")

	// Resolve the epoch-11 header, as apply-time consumption does.
	require.True(t, ls.consumeDeferredHeaderValidation(p11))

	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(25, 0, record))
	assert.Equal(
		t,
		uint64(13),
		seenBefore,
		"resolving the epoch-11 header must raise the pin to epoch 13",
	)
}

// TestPrunePoolSnapshotsWithRetentionFloor_DepthCapBoundsRetention proves the
// hard backstop: even a live (ahead-of-cursor) deferred header needing a very
// old snapshot cannot lower pruning past minBefore, so retention is bounded
// (issue #3727, finding 5).
func TestPrunePoolSnapshotsWithRetentionFloor_DepthCapBoundsRetention(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Header maps to epoch 11 (needs snapshot 10) and is ahead of the cursor
	// (tip 0) so it is not evicted; only the cap bounds it.
	ls.epochCache = []models.Epoch{
		{EpochId: 11, StartSlot: 1_100, LengthInSlots: 100, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()
	ls.markDeferredHeaderValidation(ocommon.Point{Slot: 1_150, Hash: []byte{0x11}})

	floor, ok := ls.OldestRequiredSnapshotEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(10), floor)

	var seenBefore uint64
	require.NoError(t, ls.PrunePoolSnapshotsWithRetentionFloor(
		25, 16, // minBefore = 16: retain at most down to epoch 16
		func(before uint64) error { seenBefore = before; return nil },
	))
	assert.Equal(
		t,
		uint64(16),
		seenBefore,
		"floor 10 must be clamped up to the depth-cap minBefore 16",
	)
}
