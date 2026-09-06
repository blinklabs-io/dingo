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

	common "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

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
