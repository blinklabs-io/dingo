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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
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

type poolRuleState struct {
	lcommon.LedgerState
	view *LedgerView
}

func (s poolRuleState) DijkstraBlsProofDomain() lcommon.DijkstraBlsProofDomain {
	return s.view.DijkstraBlsProofDomain()
}

func (poolRuleState) IsFutureVrfKeyInUse(
	lcommon.Blake2b256,
) (bool, lcommon.PoolKeyHash, error) {
	return false, lcommon.PoolKeyHash{}, nil
}

func poolRegistrationWithProof(
	t *testing.T,
	dst string,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	scalar := big.NewInt(21)
	var public bls12381.G2Affine
	public.ScalarMultiplicationBase(scalar)
	encodedPublic := public.Bytes()
	hash, err := bls12381.HashToG1(encodedPublic[:], []byte(dst))
	require.NoError(t, err)
	var proof bls12381.G1Affine
	proof.ScalarMultiplication(&hash, scalar)
	encodedProof := proof.Bytes()
	cert := &lcommon.PoolRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypePoolRegistration),
		Operator: lcommon.PoolKeyHash{0x01},
		LeiosKey: &lcommon.LeiosKey{
			PublicKey:       encodedPublic[:],
			PossessionProof: encodedProof[:],
		},
		Cost:   340_000_000,
		Margin: cbor.Rat{Rat: big.NewRat(0, 1)},
	}
	require.NoError(t, cert.SetRewardAccountCredential(
		lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash},
		lcommon.AddressNetworkTestnet,
	))
	tx := &dijkstra.DijkstraTransaction{TxIsValid: true}
	tx.Body.TxCertificates = []lcommon.CertificateWrapper{{
		Type:        uint(lcommon.CertificateTypePoolRegistration),
		Certificate: cert,
	}}
	return tx
}

// TestPoolRegistrationProofDomainFollowsLedgerView runs gouroboros's POOL
// rule against the domain the LedgerView reports.
func TestPoolRegistrationProofDomainFollowsLedgerView(t *testing.T) {
	t.Parallel()
	signed := map[lcommon.DijkstraBlsProofDomain]string{
		lcommon.DijkstraBlsProofDomainPop:       leios.LeiosPoPDST,
		lcommon.DijkstraBlsProofDomainLegacySig: leios.LeiosVoteDST,
	}
	pp := &dijkstra.DijkstraProtocolParameters{}
	pp.ProtocolVersion.Major = 12
	pp.MinPoolCost = 340_000_000
	for signedDomain, dst := range signed {
		for viewDomain := range signed {
			view := &LedgerView{ls: &LedgerState{
				config: LedgerStateConfig{DijkstraBlsProofDomain: viewDomain},
			}}
			ls := poolRuleState{
				LedgerState: mockledger.NewLedgerStateBuilder().
					WithNetworkId(0).
					Build(),
				view: view,
			}
			err := dijkstra.UtxoValidatePoolCertificates(
				poolRegistrationWithProof(t, dst), 0, ls, pp,
			)
			if signedDomain == viewDomain {
				require.NoError(t, err)
				continue
			}
			var popErr *dijkstra.BlsKeyInvalidProofOfPossessionError
			require.ErrorAs(t, err, &popErr)
		}
	}
}
