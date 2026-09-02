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

package conformance

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// TestStateProviderConstitutionExposesStoredShape proves the conformance
// provider reports the same anchor and guardrails policy hash the backend
// holds, in the same shape production's ledger.LedgerView.Constitution
// reports. It previously returned an empty common.Constitution regardless
// of what the backend held.
func TestStateProviderConstitutionExposesStoredShape(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xc1)
	policyHash := bytes.Repeat([]byte{0xc2}, 28)
	require.NoError(t, m.db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/conformance",
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  0,
	}, nil))

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/conformance", got.Anchor.Url)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)
}

// TestStateProviderConstitutionWithoutPolicyHash proves a constitution with
// no guardrails script is reported with a nil ScriptHash, which is what
// gouroboros' guardrails rule reads as "proposals must carry no policy
// hash".
func TestStateProviderConstitutionWithoutPolicyHash(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xd1)
	require.NoError(t, m.db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/conformance-plain",
		AnchorHash: anchorHash,
		AddedSlot:  0,
	}, nil))

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)
}

// TestStateProviderConstitutionMissingFailsClosed proves a backend with no
// constitution row is reported as unavailable rather than as a valid
// constitution with no guardrails script, so a vector whose constitution
// never reached the backend fails instead of silently passing guardrails
// validation.
func TestStateProviderConstitutionMissingFailsClosed(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	got, err := m.GetStateProvider().Constitution()
	require.ErrorIs(t, err, governance.ErrConstitutionUnavailable)
	require.Nil(t, got)
}

// TestLoadInitialStateSeedsConstitution proves a vector's initial
// constitution is written to the real backend, so the read side above has
// something to read without consulting the pre-validation govState mirror.
func TestLoadInitialStateSeedsConstitution(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xe1)
	policyHash := bytes.Repeat([]byte{0xe2}, 28)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			Constitution: &conformance.ConstitutionInfo{
				AnchorURL:  "https://example.invalid/vector",
				AnchorHash: anchorHash,
				PolicyHash: policyHash,
			},
		},
		&conway.ConwayProtocolParameters{},
	))

	stored, err := m.db.GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, "https://example.invalid/vector", stored.AnchorURL)
	require.Equal(t, anchorHash, stored.AnchorHash)
	require.Equal(t, policyHash, stored.PolicyHash)

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)
}
