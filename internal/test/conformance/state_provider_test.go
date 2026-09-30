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
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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

func TestDingoStateProviderEpochForSlotUsesReplayEpoch(t *testing.T) {
	t.Parallel()

	provider := NewDingoStateProvider(&DingoStateManager{
		currentEpoch: 42,
	})

	epoch, err := provider.EpochForSlot(123)
	require.NoError(t, err)
	require.Equal(t, uint64(42), epoch)
}

// The corpus documents keyDeposit=2000000 for its stake vectors, and every
// registration it declares was made at that value, so a corpus vector alone
// cannot tell a recorded refund from the KeyDeposit fallback -- the two
// numbers coincide. These constants deliberately separate them: the state is
// seeded at a recorded deposit that is not the KeyDeposit in effect during
// validation, so the two candidate refunds give opposite value-conservation
// outcomes.
const (
	stakeDepositVectorRecorded   = uint64(5_000_000)
	stakeDepositVectorKeyDeposit = uint64(2_000_000)
)

func stakeDepositVectorPparams(
	keyDeposit uint64,
) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		//nolint:gosec // G115: test-scoped constants do not overflow
		KeyDeposit:           uint(keyDeposit),
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func stakeDepositVectorCredential() common.Credential {
	return common.Credential{
		CredType: common.CredentialTypeAddrKeyHash,
		Credential: common.NewBlake2b224(
			bytes.Repeat([]byte{0xd1}, common.AddressHashSize),
		),
	}
}

// loadStakeDepositVector seeds a vector-shaped initial state declaring the
// credential already registered, with the given KeyDeposit in force. The
// deposit recorded for the credential is derived from these protocol
// parameters, matching how the harness seeds every corpus vector.
func loadStakeDepositVector(
	t *testing.T,
	m *DingoStateManager,
	cred common.Credential,
	keyDeposit uint64,
) {
	t.Helper()
	key := mockledger.NewRewardAccountKey(cred)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch: 1,
			StakeRegistrationsByCredential: map[mockledger.RewardAccountKey]bool{
				key: true,
			},
			RewardAccountBalances: map[mockledger.RewardAccountKey]uint64{
				key: 0,
			},
		},
		stakeDepositVectorPparams(keyDeposit),
	))
}

// stakeDepositVectorTx is a legacy stake deregistration with no inputs and no
// outputs, so value conservation reduces to "refund must equal fee" and
// isolates the recorded-deposit lookup from every other term.
func stakeDepositVectorTx(
	cred common.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []common.CertificateWrapper{
				{
					Type: uint(
						common.CertificateTypeStakeDeregistration,
					),
					Certificate: &common.StakeDeregistrationCertificate{
						CertType: uint(
							common.CertificateTypeStakeDeregistration,
						),
						StakeCredential: cred,
					},
				},
			},
		},
	}
}

// TestConformanceProviderRefundsRecordedStakeDepositNotKeyDeposit is the
// regression test for #3831. It runs gouroboros'
// UtxoValidateValueNotConservedUtxo against the conformance state provider
// with a recorded deposit of 5 ADA while the KeyDeposit in force during
// validation is 2 ADA.
//
// Without DingoStateProvider.StakeCredentialDeposit the rule's optional type
// assertion misses, the refund silently becomes the 2 ADA KeyDeposit, and the
// 5 ADA transaction fails value conservation. That is the gap the issue
// describes: the corpus could not distinguish a correct recorded refund from
// the fallback.
func TestConformanceProviderRefundsRecordedStakeDepositNotKeyDeposit(
	t *testing.T,
) {
	require.NotEqual(
		t,
		stakeDepositVectorKeyDeposit,
		stakeDepositVectorRecorded,
		"the recorded deposit must differ from KeyDeposit for this vector to discriminate",
	)

	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := stakeDepositVectorCredential()
	// Seed the registration at the recorded deposit.
	loadStakeDepositVector(t, m, cred, stakeDepositVectorRecorded)
	provider := m.GetStateProvider()
	require.True(t, provider.IsStakeCredentialRegistered(cred))

	// Validate under a *different* KeyDeposit, so the recorded value and the
	// fallback disagree.
	validationPparams := stakeDepositVectorPparams(
		stakeDepositVectorKeyDeposit,
	)

	// Refunded at the recorded 5 ADA: a 5 ADA fee conserves value.
	require.NoError(t, conway.UtxoValidateValueNotConservedUtxo(
		stakeDepositVectorTx(cred, stakeDepositVectorRecorded),
		200,
		provider,
		validationPparams,
	))

	// Refunded at the 2 ADA KeyDeposit instead: rejected. This is the
	// assertion that fails when the provider lacks the capability, because
	// the fallback would make this the accepted case and the one above the
	// rejected one.
	require.ErrorContains(t, conway.UtxoValidateValueNotConservedUtxo(
		stakeDepositVectorTx(cred, stakeDepositVectorKeyDeposit),
		200,
		provider,
		validationPparams,
	), "value not conserved")

	// Supporting evidence, discovered exactly the way the rule discovers it:
	// a runtime type assertion on the value the harness passes as the ledger
	// state. If this assertion misses, the rule takes its silent fallback.
	depositState, ok := provider.(common.StakeCredentialDepositState)
	require.True(
		t,
		ok,
		"the conformance provider must satisfy StakeCredentialDepositState, or the corpus never exercises the recorded refund",
	)
	recorded, err := depositState.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(t, recorded)
	require.Equal(t, stakeDepositVectorRecorded, *recorded)
}

// TestConformanceProviderStakeDepositAbsentForUnregisteredCredential pins the
// nil contract the rule depends on: an unregistered credential reports
// absence, which is what sends value conservation to its KeyDeposit fallback
// rather than to a refund of zero.
func TestConformanceProviderStakeDepositAbsentForUnregisteredCredential(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := stakeDepositVectorCredential()
	provider := m.GetStateProvider()
	require.False(t, provider.IsStakeCredentialRegistered(cred))

	depositState, ok := provider.(common.StakeCredentialDepositState)
	require.True(t, ok)
	recorded, err := depositState.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.Nil(t, recorded)
}

// TestStateProviderTreasuryValueReportsBackendState proves the conformance
// provider reports the treasury the backend holds, in the same shape
// production's ledger.LedgerView.TreasuryValue reports. It previously
// returned a hardcoded zero regardless of backend state.
func TestStateProviderTreasuryValueReportsBackendState(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	const treasury = uint64(87_920_693_660_807)
	require.NoError(
		t,
		m.db.Metadata().SetNetworkState(treasury, 1_000, 42, nil),
	)

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, treasury, got)
}

// TestStateProviderTreasuryValueMissingFailsClosed proves a backend with no
// network-state row is reported as unavailable rather than as a treasury of
// zero. The upstream current-treasury-value rule compares for equality once a
// transaction body carries key 21, so a synthetic zero would silently reject
// every vector declaring a non-zero treasury and silently accept one
// declaring zero.
func TestStateProviderTreasuryValueMissingFailsClosed(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	got, err := m.GetStateProvider().TreasuryValue()
	require.ErrorContains(t, err, "treasury network state is unavailable")
	require.Zero(t, got)
}

// TestStateProviderTreasuryValueTracksLatestSlot proves the provider reads
// the newest network-state row rather than the first one written, so a
// vector that advances the treasury is validated against the value the
// backend currently holds.
func TestStateProviderTreasuryValueTracksLatestSlot(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	require.NoError(t, m.db.Metadata().SetNetworkState(100, 900, 10, nil))
	require.NoError(t, m.db.Metadata().SetNetworkState(250, 750, 20, nil))

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, uint64(250), got)
}
