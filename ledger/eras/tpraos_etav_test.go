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

package eras_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedTPraosEtaV computes one step of the evolving-nonce update under
// the Shelley protocol's rules.
//
// Per the Shelley formal spec (chain.tex / crypto-details.tex):
//
//   - the per-block nonce contribution is bnonce(bhbody) ∈ Seed, where Seed
//     is the BLAKE2b-256 output type (32 bytes);
//   - the seed combination operator x ⊕ y is implemented as
//     BLAKE2b-256(x || y) where both inputs are Seeds (32 bytes each);
//   - the Update Nonce rule replaces η_v by η_v ⊕ η, where η is the block's
//     Seed contribution.
//
// The VRF certificate output for the curve used in Shelley is wider than a
// Seed, so converting it to bnonce ∈ Seed requires hashing it down. The
// expected per-block update is therefore:
//
//	η_v' = BLAKE2b-256( η_v || BLAKE2b-256(rawVrfCertOutput) )
//
// Concatenating the raw certificate bytes with η_v and hashing in one step
// instead of pre-hashing produces a different result that does not match
// what other nodes on the network compute, which manifests as VRF
// verification failures on every header at the next epoch boundary
// (issue #2125 Shelley→Allegra symptom).
func expectedTPraosEtaV(prev, rawVrfOutput []byte) []byte {
	contribution := lcommon.Blake2b256Hash(rawVrfOutput).Bytes()
	concat := make([]byte, 0, len(prev)+len(contribution))
	concat = append(concat, prev...)
	concat = append(concat, contribution...)
	return lcommon.Blake2b256Hash(concat).Bytes()
}

// tpraosTestVectors returns deterministic inputs the TPraos eras share.
// Non-trivial fixed bytes (no all-zeros, no neutral-nonce path) so the
// divergence between the buggy and correct formulas is unambiguous.
func tpraosTestVectors(t *testing.T) (cfg *cardano.CardanoNodeConfig, prev, rawVrf []byte) {
	t.Helper()
	prev = bytes.Repeat([]byte{0x33}, 32)
	rawVrf = bytes.Repeat([]byte{0x42}, 64)
	cfg = &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
	}
	return cfg, prev, rawVrf
}

// TestCalculateEtaV_TPraos_RawVrfMustBePreHashed asserts that every TPraos
// era's CalculateEtaVFunc applies BLAKE2b-256 to the raw VRF certificate
// output before folding it into the rolling nonce. Without that pre-hash,
// the resulting evolving and candidate nonces diverge from peers, the
// derived epoch nonce no longer matches, and VRF verification of every
// post-fork-boundary header fails — the symptom of issue #2125 at the
// Shelley→Allegra boundary in the eras DevNet.
func TestCalculateEtaV_TPraos_RawVrfMustBePreHashed(t *testing.T) {
	cfg, prev, rawVrf := tpraosTestVectors(t)
	expected := expectedTPraosEtaV(prev, rawVrf)

	cases := []struct {
		name  string
		block ledger.Block
		fn    func(*cardano.CardanoNodeConfig, []byte, ledger.Block) ([]byte, error)
	}{
		{
			name: "shelley",
			block: &shelley.ShelleyBlock{
				BlockHeader: &shelley.ShelleyBlockHeader{
					Body: shelley.ShelleyBlockHeaderBody{
						NonceVrf: lcommon.VrfResult{Output: rawVrf},
					},
				},
			},
			fn: eras.CalculateEtaVShelley,
		},
		{
			name: "allegra",
			block: &allegra.AllegraBlock{
				BlockHeader: &allegra.AllegraBlockHeader{
					ShelleyBlockHeader: shelley.ShelleyBlockHeader{
						Body: shelley.ShelleyBlockHeaderBody{
							NonceVrf: lcommon.VrfResult{Output: rawVrf},
						},
					},
				},
			},
			fn: eras.CalculateEtaVAllegra,
		},
		{
			name: "mary",
			block: &mary.MaryBlock{
				BlockHeader: &mary.MaryBlockHeader{
					ShelleyBlockHeader: shelley.ShelleyBlockHeader{
						Body: shelley.ShelleyBlockHeaderBody{
							NonceVrf: lcommon.VrfResult{Output: rawVrf},
						},
					},
				},
			},
			fn: eras.CalculateEtaVMary,
		},
		{
			name: "alonzo",
			block: &alonzo.AlonzoBlock{
				BlockHeader: &alonzo.AlonzoBlockHeader{
					ShelleyBlockHeader: shelley.ShelleyBlockHeader{
						Body: shelley.ShelleyBlockHeaderBody{
							NonceVrf: lcommon.VrfResult{Output: rawVrf},
						},
					},
				},
			},
			fn: eras.CalculateEtaVAlonzo,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(cfg, prev, tc.block)
			require.NoError(t, err)
			assert.Equal(t,
				hex.EncodeToString(expected),
				hex.EncodeToString(got),
				"TPraos %s: per-block etaV must hash the raw VRF "+
					"certificate output to a Seed before folding "+
					"(issue #2125)",
				tc.name,
			)
		})
	}
}

// TestCalculateEtaV_TPraos_GenesisFallback exercises the path where the
// previous block nonce is empty (chain start). The Shelley genesis hash
// is substituted as the initial accumulator, after which the same
// pre-hashed update applies. Guards against a regression where the
// genesis fallback skips the pre-hash step.
func TestCalculateEtaV_TPraos_GenesisFallback(t *testing.T) {
	cfg, _, rawVrf := tpraosTestVectors(t)
	genesis, err := hex.DecodeString(cfg.ShelleyGenesisHash)
	require.NoError(t, err)
	expected := expectedTPraosEtaV(genesis, rawVrf)

	block := &shelley.ShelleyBlock{
		BlockHeader: &shelley.ShelleyBlockHeader{
			Body: shelley.ShelleyBlockHeaderBody{
				NonceVrf: lcommon.VrfResult{Output: rawVrf},
			},
		},
	}
	got, err := eras.CalculateEtaVShelley(cfg, nil, block)
	require.NoError(t, err)
	assert.Equal(t,
		hex.EncodeToString(expected),
		hex.EncodeToString(got),
		"empty prev nonce should fall back to genesis hash, then apply "+
			"the TPraos pre-hash formula (issue #2125)",
	)
}

// TestCalculateEtaV_Praos_NotAffected guards the Praos (Babbage+) path
// against an over-broad fix. Praos applies a domain-separated double
// hash of the VRF output (range-extension prefix "N" then a second
// BLAKE2b-256), producing a 32-byte contribution that is then folded by
// the same seed-combination operator. The result is therefore distinct
// from the TPraos pre-hash formula, which serves as a sanity check that
// the two era families remain on separate code paths after the issue
// #2125 fix.
func TestCalculateEtaV_Praos_NotAffected(t *testing.T) {
	cfg, prev, rawVrf := tpraosTestVectors(t)

	// Independent recomputation of the Praos contribution.
	tagged := make([]byte, 0, 1+len(rawVrf))
	tagged = append(tagged, 'N')
	tagged = append(tagged, rawVrf...)
	rangeExtended := lcommon.Blake2b256Hash(tagged).Bytes()
	contribution := lcommon.Blake2b256Hash(rangeExtended).Bytes()
	concat := make([]byte, 0, len(prev)+len(contribution))
	concat = append(concat, prev...)
	concat = append(concat, contribution...)
	expected := lcommon.Blake2b256Hash(concat).Bytes()

	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				VrfResult: lcommon.VrfResult{Output: rawVrf},
			},
		},
	}
	got, err := eras.CalculateEtaVBabbage(cfg, prev, block)
	require.NoError(t, err)
	assert.Equal(t,
		hex.EncodeToString(expected),
		hex.EncodeToString(got),
		"Babbage etaV should follow the Praos domain-separated formula",
	)
	assert.NotEqual(t,
		hex.EncodeToString(expectedTPraosEtaV(prev, rawVrf)),
		hex.EncodeToString(got),
		"Praos and TPraos formulas should not collapse to the same result",
	)
}
