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
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestLedgerViewDijkstraBlsProofDomainSurvivesPendingState(t *testing.T) {
	t.Parallel()
	pending := utxoref.NewStateOverlay()
	pending.Apply(
		mockledger.NewTransactionBuilder().
			WithCertificates(&lcommon.RegistrationCertificate{
				CertType: uint(lcommon.CertificateTypeRegistration), Amount: 1,
				StakeCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash{1},
				},
			}),
	)
	require.Equal(t, 1, pending.Len())
	for _, domain := range []lcommon.DijkstraBlsProofDomain{lcommon.DijkstraBlsProofDomainLegacySig, lcommon.DijkstraBlsProofDomainPop, lcommon.DijkstraBlsProofDomainLegacySig} {
		view := &LedgerView{
			ls: &LedgerState{
				config: LedgerStateConfig{DijkstraBlsProofDomain: domain},
			},
			pendingState: pending,
		}
		require.Equal(t, domain, lcommon.DijkstraBlsProofDomainFor(view))
		state, err := view.validationState(nil, 1)
		require.NoError(t, err)
		require.NotSame(t, view, state)
		require.Equal(
			t,
			domain,
			lcommon.DijkstraBlsProofDomainFor(state),
			"reusing a folded overlay must use its current ledger view",
		)
		require.NoError(
			t,
			lcommon.VerifyTransaction(
				nil,
				0,
				state,
				nil,
				[]lcommon.UtxoValidationRuleFunc{
					func(_ lcommon.Transaction, _ uint64, cached lcommon.LedgerState, _ lcommon.ProtocolParameters) error {
						require.Equal(
							t,
							domain,
							lcommon.DijkstraBlsProofDomainFor(cached),
						)
						return nil
					},
				},
			),
		)
	}
}
