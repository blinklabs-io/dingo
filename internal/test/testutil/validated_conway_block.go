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

package testutil

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/stretchr/testify/require"
)

// ValidatedConwayBlock holds a genuinely VRF/KES-valid Conway block's raw
// CBOR alongside the parameters a caller needs to independently re-verify
// it (e.g. via gouroboros' ledger.VerifyBlock or pipeline.ValidateStage).
type ValidatedConwayBlock struct {
	Cbor              []byte
	Hash              []byte
	Slot              uint64
	BlockNumber       uint64
	EpochNonceHex     string
	SlotsPerKesPeriod uint64
}

// conwayEmptyBodyHash returns the block body hash for a Conway block with
// empty transaction components (bodies, witness sets, metadata set, invalid
// transactions), which is independent of the header. Computed once via the
// same technique as BuildDecodableConwayBlockBytes (blake2b256 over the
// concatenation of the per-component blake2b256 hashes).
func conwayEmptyBodyHash(t testing.TB) lcommon.Blake2b256 {
	t.Helper()
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{},
	}
	tmp, err := cbor.Encode(block)
	require.NoError(t, err)
	var comps []cbor.RawMessage
	_, err = cbor.Decode(tmp, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var concat []byte
	for i := 1; i < 5; i++ {
		h := lcommon.Blake2b256Hash(comps[i])
		concat = append(concat, h.Bytes()...)
	}
	return lcommon.Blake2b256Hash(concat)
}

// BuildValidatedConwayBlockBytes generates real VRF, KES, and cold keys,
// searches slots in [slotRangeStart, slotRangeStart+199] for one where the
// generated VRF key wins leadership (99% active slot coefficient, pool
// stake == total stake), and returns a genuinely decodable, VRF/KES-valid
// Conway block at that slot with an empty transaction body.
//
// Unlike BuildDecodableConwayBlockBytes (which zeroes out the VRF/KES/OpCert
// fields for tests that only need a decodable block), this is for tests
// that need actual cryptographic validation -- gouroboros'
// pipeline.ValidateStage / ledger.VerifyBlock -- to genuinely pass, not
// merely decode. seed must be 32 bytes and should differ between blocks
// needing distinct producer keys; nonceSeed derives the epoch nonce the
// block is proven against (use the same nonceSeed for blocks meant to share
// an epoch).
func BuildValidatedConwayBlockBytes(
	t *testing.T,
	seed [32]byte,
	nonceSeed byte,
	slotRangeStart uint64,
	blockNumber uint64,
) ValidatedConwayBlock {
	t.Helper()

	vrfPk, vrfSk, err := vrf.KeyGen(seed[:])
	require.NoError(t, err)

	kesSeed := seed
	kesSeed[0] ^= 0xAA
	kesSk, kesPk, err := kes.KeyGen(kes.CardanoKesDepth, kesSeed[:])
	require.NoError(t, err)

	coldSeed := seed
	coldSeed[0] ^= 0xBB
	coldPrivKey := ed25519.NewKeyFromSeed(coldSeed[:])
	coldPubKey := coldPrivKey.Public().(ed25519.PublicKey)

	const slotsPerKesPeriod = uint64(129600)
	epochNonce := make([]byte, 32)
	for i := range epochNonce {
		epochNonce[i] = nonceSeed + byte(i) //nolint:gosec
	}

	const opCertSeqNum = uint32(0)
	const opCertKesPeriod = uint32(0)
	var opCertBody [48]byte
	copy(opCertBody[:32], kesPk)
	binary.BigEndian.PutUint64(opCertBody[32:40], uint64(opCertSeqNum))
	binary.BigEndian.PutUint64(opCertBody[40:48], uint64(opCertKesPeriod))
	opCertSig := ed25519.Sign(coldPrivKey, opCertBody[:])

	bodyHash := conwayEmptyBodyHash(t)
	activeSlotCoeff := big.NewRat(99, 100)

	for slot := slotRangeStart; slot < slotRangeStart+200; slot++ {
		slotInt64 := int64(slot) //nolint:gosec // test slots are small
		vrfInput, vrfInputErr := vrf.MkInputVrf(slotInt64, epochNonce)
		if vrfInputErr != nil {
			continue
		}
		vrfProof, vrfOutput, proveErr := vrf.Prove(vrfSk, vrfInput)
		if proveErr != nil {
			continue
		}
		threshold := consensus.CertifiedNatThreshold(
			1_000_000_000,
			1_000_000_000,
			activeSlotCoeff,
		)
		if !consensus.IsVRFOutputBelowThreshold(vrfOutput, threshold) {
			continue
		}

		headerBody := babbage.BabbageBlockHeaderBody{
			BlockNumber: blockNumber,
			Slot:        slot,
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
			BlockBodySize: 0,
			BlockBodyHash: bodyHash,
			OpCert: babbage.BabbageOpCert{
				HotVkey:        kesPk,
				SequenceNumber: opCertSeqNum,
				KesPeriod:      opCertKesPeriod,
				Signature:      opCertSig,
			},
			ProtoVersion: babbage.BabbageProtoVersion{Major: 10},
		}

		headerBodyCbor, encErr := cbor.Encode(headerBody)
		if encErr != nil {
			continue
		}
		// Store the CBOR on the header body so VerifyBlock's
		// extractOriginalBodyCbor can retrieve it for KES verification.
		headerBody.SetCbor(headerBodyCbor)

		kesSig, signErr := kes.Sign(kesSk, 0, headerBodyCbor)
		if signErr != nil {
			continue
		}

		block := &conway.ConwayBlock{
			BlockHeader: &conway.ConwayBlockHeader{
				BabbageBlockHeader: babbage.BabbageBlockHeader{
					Body:      headerBody,
					Signature: kesSig,
				},
			},
		}
		raw, encErr := cbor.Encode(block)
		if encErr != nil {
			continue
		}

		decoded, decErr := conway.NewConwayBlockFromCbor(raw)
		require.NoError(t, decErr)

		return ValidatedConwayBlock{
			Cbor:              raw,
			Hash:              decoded.Hash().Bytes(),
			Slot:              slot,
			BlockNumber:       blockNumber,
			EpochNonceHex:     hex.EncodeToString(epochNonce),
			SlotsPerKesPeriod: slotsPerKesPeriod,
		}
	}

	require.FailNow(t, "should find an eligible slot for the test block")
	return ValidatedConwayBlock{}
}
