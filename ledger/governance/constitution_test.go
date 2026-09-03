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

package governance

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestConstitutionFromModelMapsAnchorAndPolicyHash proves the stored anchor
// URL, anchor hash, and guardrails policy hash all reach the shared
// ledger-state contract. Before the mapping existed the state providers
// returned an empty common.Constitution, so every one of these assertions
// read a zero value.
func TestConstitutionFromModelMapsAnchorAndPolicyHash(t *testing.T) {
	anchorHash := bytes.Repeat([]byte{0x11}, lcommon.Blake2b256Size)
	policyHash := bytes.Repeat([]byte{0x22}, lcommon.Blake2b224Size)

	got, err := ConstitutionFromModel(&models.Constitution{
		AnchorURL:  "https://example.invalid/constitution",
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  42,
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/constitution", got.Anchor.Url)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)
}

// TestConstitutionFromModelWithoutPolicyHash proves a constitution with no
// guardrails script maps to a nil ScriptHash. gouroboros' guardrails rule
// compares the proposal's policy hash against this by nil-ness as well as
// by value, so an empty-but-non-nil slice would be read as "a guardrails
// script is required" and reject every proposal.
func TestConstitutionFromModelWithoutPolicyHash(t *testing.T) {
	anchorHash := bytes.Repeat([]byte{0x33}, lcommon.Blake2b256Size)
	for name, stored := range map[string][]byte{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ConstitutionFromModel(&models.Constitution{
				AnchorURL:  "https://example.invalid/no-guardrails",
				AnchorHash: anchorHash,
				PolicyHash: stored,
			})
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, anchorHash, got.Anchor.DataHash[:])
			require.Nil(t, got.ScriptHash)
		})
	}
}

// TestConstitutionFromModelDoesNotAliasStoredPolicyHash proves the mapped
// ScriptHash does not share backing memory with the stored row, so a caller
// mutating the returned value cannot corrupt the store's buffer.
func TestConstitutionFromModelDoesNotAliasStoredPolicyHash(t *testing.T) {
	policyHash := bytes.Repeat([]byte{0x44}, lcommon.Blake2b224Size)
	stored := &models.Constitution{
		AnchorHash: bytes.Repeat([]byte{0x45}, lcommon.Blake2b256Size),
		PolicyHash: policyHash,
	}
	got, err := ConstitutionFromModel(stored)
	require.NoError(t, err)
	require.Len(t, got.ScriptHash, lcommon.Blake2b224Size)
	got.ScriptHash[0] = 0xff
	require.Equal(t, byte(0x44), stored.PolicyHash[0])
}

// TestConstitutionFromModelMissingFailsClosed proves an absent constitution
// row is reported as unavailable rather than as a valid constitution with no
// guardrails script. Reporting the latter would let guardrails validation
// accept a parameter-change or treasury-withdrawal proposal carrying no
// policy hash on a chain whose constitution requires one.
func TestConstitutionFromModelMissingFailsClosed(t *testing.T) {
	got, err := ConstitutionFromModel(nil)
	require.ErrorIs(t, err, ErrConstitutionUnavailable)
	require.Nil(t, got)
}

// TestConstitutionFromModelMalformedAnchorFailsClosed proves a stored row
// whose anchor hash is not a full blake2b-256 digest fails closed instead of
// being zero-padded or truncated into the contract's fixed-size array.
func TestConstitutionFromModelMalformedAnchorFailsClosed(t *testing.T) {
	for name, anchorHash := range map[string][]byte{
		"absent": nil,
		"short":  bytes.Repeat([]byte{0x55}, lcommon.Blake2b256Size-1),
		"long":   bytes.Repeat([]byte{0x55}, lcommon.Blake2b256Size+1),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ConstitutionFromModel(&models.Constitution{
				AnchorURL:  "https://example.invalid/malformed",
				AnchorHash: anchorHash,
				PolicyHash: bytes.Repeat(
					[]byte{0x56},
					lcommon.Blake2b224Size,
				),
			})
			require.ErrorIs(t, err, ErrConstitutionUnavailable)
			require.Nil(t, got)
		})
	}
}

// TestConstitutionFromGenesisMapsAnchorAndScript proves the Conway genesis
// anchor and guardrails script hash are decoded onto a slot-0 row, which is
// what makes the genesis constitution the enacted one on a chain that has
// never enacted a NewConstitution action.
func TestConstitutionFromGenesisMapsAnchorAndScript(t *testing.T) {
	anchorHash := hex.EncodeToString(
		bytes.Repeat([]byte{0x66}, lcommon.Blake2b256Size),
	)
	script := hex.EncodeToString(
		bytes.Repeat([]byte{0x67}, lcommon.Blake2b224Size),
	)
	got, err := ConstitutionFromGenesis(&conway.ConwayGenesis{
		Constitution: conway.ConwayGenesisConstitution{
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: anchorHash,
				Url:      "ipfs://example",
			},
			Script: script,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "ipfs://example", got.AnchorURL)
	require.Equal(t, anchorHash, hex.EncodeToString(got.AnchorHash))
	require.Equal(t, script, hex.EncodeToString(got.PolicyHash))
	require.Equal(t, uint64(0), got.AddedSlot)
}

// TestConstitutionFromGenesisWithoutScript proves a genesis constitution
// that declares no guardrails script maps to a nil policy hash, which
// ConstitutionFromModel then reports as "no guardrails script required"
// rather than as an empty-but-present hash.
func TestConstitutionFromGenesisWithoutScript(t *testing.T) {
	got, err := ConstitutionFromGenesis(&conway.ConwayGenesis{
		Constitution: conway.ConwayGenesisConstitution{
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: hex.EncodeToString(
					bytes.Repeat([]byte{0x68}, lcommon.Blake2b256Size),
				),
				Url: "ipfs://no-guardrails",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Nil(t, got.PolicyHash)

	mapped, err := ConstitutionFromModel(got)
	require.NoError(t, err)
	require.Nil(t, mapped.ScriptHash)
}

// TestConstitutionFromGenesisAbsent proves a nil genesis config, and one
// that declares no constitution at all, both map to no row and no error, so
// genesis initialization seeds nothing instead of failing.
func TestConstitutionFromGenesisAbsent(t *testing.T) {
	got, err := ConstitutionFromGenesis(nil)
	require.NoError(t, err)
	require.Nil(t, got)

	got, err = ConstitutionFromGenesis(&conway.ConwayGenesis{})
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestConstitutionFromGenesisMalformed proves a declared constitution whose
// anchor hash or guardrails script hash is not hex of the required length is
// rejected rather than seeded. A wrong guardrails hash would reject every
// parameter-change and treasury-withdrawal proposal on the chain.
func TestConstitutionFromGenesisMalformed(t *testing.T) {
	validAnchor := hex.EncodeToString(
		bytes.Repeat([]byte{0x69}, lcommon.Blake2b256Size),
	)
	validScript := hex.EncodeToString(
		bytes.Repeat([]byte{0x6a}, lcommon.Blake2b224Size),
	)
	for name, constitution := range map[string]conway.ConwayGenesisConstitution{
		"anchor not hex": {
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: "not-hex",
			},
			Script: validScript,
		},
		"anchor short": {
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: validAnchor[2:],
			},
			Script: validScript,
		},
		"anchor absent with url": {
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				Url: "ipfs://anchorless",
			},
			Script: validScript,
		},
		"script not hex": {
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: validAnchor,
			},
			Script: "not-hex",
		},
		"script wrong length": {
			Anchor: conway.ConwayGenesisConstitutionAnchor{
				DataHash: validAnchor,
			},
			Script: validAnchor,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ConstitutionFromGenesis(&conway.ConwayGenesis{
				Constitution: constitution,
			})
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
