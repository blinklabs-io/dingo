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

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const (
	recordedStakeDeposit = uint64(5_000_000)
	currentKeyDeposit    = uint64(2_000_000)
)

func recordedDepositVectorParameters(keyDeposit uint64) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{Major: 9},
		//nolint:gosec // G115: test-scoped constants do not overflow
		KeyDeposit:           uint(keyDeposit),
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func recordedDepositVectorCredential() common.Credential {
	return common.Credential{
		CredType: common.CredentialTypeAddrKeyHash,
		Credential: common.NewBlake2b224(
			bytes.Repeat([]byte{0xd1}, common.AddressHashSize),
		),
	}
}

func loadRecordedDepositVector(
	t *testing.T,
	m *DingoStateManager,
	credential common.Credential,
) {
	t.Helper()
	key := mockledger.NewRewardAccountKey(credential)
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
		recordedDepositVectorParameters(recordedStakeDeposit),
	))
}

func recordedDepositVectorTransaction(
	credential common.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []common.CertificateWrapper{
				{
					Type: uint(common.CertificateTypeStakeDeregistration),
					Certificate: &common.StakeDeregistrationCertificate{
						CertType:        uint(common.CertificateTypeStakeDeregistration),
						StakeCredential: credential,
					},
				},
			},
		},
	}
}

// TestConformanceProviderRefundsRecordedStakeDeposit covers the legacy stake
// deregistration refund path with a real Dingo backend and gouroboros rule.
// The seeded deposit is 5 ADA, while validation uses a 2 ADA KeyDeposit, so
// the result distinguishes the recorded refund from the fallback.
func TestConformanceProviderRefundsRecordedStakeDeposit(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	credential := recordedDepositVectorCredential()
	loadRecordedDepositVector(t, m, credential)
	provider := m.GetStateProvider()
	require.True(t, provider.IsStakeCredentialRegistered(credential))

	validationParameters := recordedDepositVectorParameters(currentKeyDeposit)
	require.NoError(t, conway.UtxoValidateValueNotConservedUtxo(
		recordedDepositVectorTransaction(credential, recordedStakeDeposit),
		200,
		provider,
		validationParameters,
	))

	// If the rule uses the current KeyDeposit instead of the recorded amount,
	// this transaction is incorrectly accepted because its fee is 2 ADA.
	require.ErrorContains(t, conway.UtxoValidateValueNotConservedUtxo(
		recordedDepositVectorTransaction(credential, currentKeyDeposit),
		200,
		provider,
		validationParameters,
	), "value not conserved")
}
