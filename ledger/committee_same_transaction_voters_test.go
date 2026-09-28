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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func authorizeHotCertificate(
	cold, hot lcommon.Credential,
) lcommon.Certificate {
	return &lcommon.AuthCommitteeHotCertificate{
		CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
		ColdCredential: cold,
		HotCredential:  hot,
	}
}

func resignColdCertificate(cold lcommon.Credential) lcommon.Certificate {
	return &lcommon.ResignCommitteeColdCertificate{
		CertType:       uint(lcommon.CertificateTypeResignCommitteeCold),
		ColdCredential: cold,
	}
}

// GOV resolves committee voters against the certificate state after the
// transaction's own certificates (Conway LEDGER passes certStateAfterCERTS to
// GOV). A hot credential stays known while any cold credential authorizes it
// (authorizedHotCommitteeCredentials: there is no unique mapping from hot to
// cold credential), and GOVCERT rewrites only the certifying cold
// credential's own entry.
func TestValidateTxCommitteeVotersSeeSameTransactionCertificates(
	t *testing.T,
) {
	t.Parallel()

	type fixture struct {
		coldA, coldB    lcommon.Credential
		coldAKey        ed25519.PrivateKey
		coldBKey        ed25519.PrivateKey
		hot, otherHot   lcommon.Credential
		hotKey          ed25519.PrivateKey
		otherHotKey     ed25519.PrivateKey
		scriptTwinOfHot lcommon.Credential
	}
	tests := []struct {
		name string
		// shared seats coldB with the same hot key as coldA.
		shared bool
		certs  func(f fixture) []lcommon.Certificate
		// voter returns the voting hot credential and its signing key.
		voter  func(f fixture) (lcommon.Credential, ed25519.PrivateKey)
		signed func(f fixture) []ed25519.PrivateKey
		known  bool
	}{
		{
			name: "hot key authorized in the same transaction",
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{
					authorizeHotCertificate(f.coldA, f.otherHot),
				}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.otherHot, f.otherHotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
			known: true,
		},
		{
			name: "hot key replaced in the same transaction",
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{
					authorizeHotCertificate(f.coldA, f.otherHot),
				}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
		},
		{
			name: "cold key resigned in the same transaction",
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{resignColdCertificate(f.coldA)}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
		},
		{
			name:   "shared hot key survives the other cold key resigning",
			shared: true,
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{resignColdCertificate(f.coldA)}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
			known: true,
		},
		{
			name:   "shared hot key survives the other cold key re-authorizing",
			shared: true,
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{
					authorizeHotCertificate(f.coldA, f.otherHot),
				}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
			known: true,
		},
		{
			name:   "shared hot key is gone once both cold keys move away",
			shared: true,
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{
					resignColdCertificate(f.coldA),
					authorizeHotCertificate(f.coldB, f.otherHot),
				}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey, f.coldBKey}
			},
		},
		{
			name: "authorization keeps the hot credential type",
			certs: func(f fixture) []lcommon.Certificate {
				return []lcommon.Certificate{
					authorizeHotCertificate(f.coldA, f.scriptTwinOfHot),
				}
			},
			voter: func(f fixture) (lcommon.Credential, ed25519.PrivateKey) {
				return f.hot, f.hotKey
			},
			signed: func(f fixture) []ed25519.PrivateKey {
				return []ed25519.PrivateKey{f.coldAKey}
			},
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, test := range tests {
			t.Run(era.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				var f fixture
				f.coldA, f.coldAKey = committeeTestVotingKey(0x31)
				f.coldB, f.coldBKey = committeeTestVotingKey(0x32)
				f.hot, f.hotKey = committeeTestVotingKey(0x33)
				f.otherHot, f.otherHotKey = committeeTestVotingKey(0x34)
				f.scriptTwinOfHot = lcommon.Credential{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: f.hot.Credential,
				}
				seatCommitteeMembers(t, db, f.coldA, f.coldB)
				if !test.shared {
					// coldA holds the voting hot key on its own; coldB
					// authorizes an unrelated hot key.
					seedCommitteeCredentialAuthorization(
						t,
						db,
						f.coldA,
						f.hot,
						1,
						1,
					)
					seedCommitteeCredentialAuthorization(
						t, db, f.coldB, committeeTestCredential(0x35), 2, 1,
					)
				} else {
					seedCommitteeCredentialAuthorization(t, db, f.coldA, f.hot, 1, 1)
					seedCommitteeCredentialAuthorization(t, db, f.coldB, f.hot, 2, 1)
				}
				voter, voterKey := test.voter(f)
				err := committeeVotingValidate(
					t, era, lv, pparams, voterKey,
					lcommon.VotingProcedures{committeeVoter(voter): {}},
					test.certs(f),
					test.signed(f)...,
				)
				if test.known {
					require.NoError(t, err)
				} else {
					requireUnknownCommitteeVoter(t, err)
				}
			})
		}
	}
}

// DRep and stake-pool voters resolve against the same post-CERTS state:
// vsDReps and psStakePools after this transaction's certificates.
func TestValidateTxDRepAndPoolVotersSeeSameTransactionCertificates(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		t.Run(
			era.name+"/DRep registered in the same transaction",
			func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				seatCommitteeMembers(t, db, committeeTestCredential(0x41))
				drep, drepKey := committeeTestVotingKey(0x42)
				err := committeeVotingValidate(
					t, era, lv, pparams, drepKey,
					lcommon.VotingProcedures{
						{Type: lcommon.VoterTypeDRepKeyHash, Hash: [28]byte(drep.Credential)}: {},
					},
					[]lcommon.Certificate{&lcommon.RegistrationDrepCertificate{
						CertType: uint(
							lcommon.CertificateTypeRegistrationDrep,
						),
						DrepCredential: drep,
					}},
				)
				require.NoError(t, err)
			},
		)
		t.Run(
			era.name+"/DRep deregistered in the same transaction",
			func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				seatCommitteeMembers(t, db, committeeTestCredential(0x43))
				drep, drepKey := committeeTestVotingKey(0x44)
				seedImportedDrep(t, db, drep, 0, 1, true)
				registration, err := lv.DRepRegistration(drep)
				require.NoError(t, err)
				require.NotNil(t, registration)
				err = committeeVotingValidate(
					t,
					era,
					lv,
					pparams,
					drepKey,
					lcommon.VotingProcedures{
						{Type: lcommon.VoterTypeDRepKeyHash, Hash: [28]byte(drep.Credential)}: {},
					},
					[]lcommon.Certificate{
						&lcommon.DeregistrationDrepCertificate{
							CertType: uint(
								lcommon.CertificateTypeDeregistrationDrep,
							),
							DrepCredential: drep,
						},
					},
				)
				var unknown conway.UnknownVoterError
				require.ErrorAs(t, err, &unknown)
			},
		)
		t.Run(
			era.name+"/pool registered in the same transaction",
			func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, _ := committeeTestView(t, pparams)
				operator := lcommon.PoolKeyHash(
					committeeTestCredential(0x45).Credential,
				)
				_, paymentKey := committeeTestVotingKey(0x46)
				// Only the voter rule is under test: the pool certificate is not
				// otherwise complete, so other rules may reject the transaction.
				err := committeeVotingValidate(
					t, era, lv, pparams, paymentKey,
					lcommon.VotingProcedures{
						{Type: lcommon.VoterTypeStakingPoolKeyHash, Hash: [28]byte(operator)}: {},
					},
					[]lcommon.Certificate{&lcommon.PoolRegistrationCertificate{
						CertType: uint(lcommon.CertificateTypePoolRegistration),
						Operator: operator,
					}},
				)
				var unknown conway.UnknownVoterError
				require.False(t, errors.As(err, &unknown), "%v", err)
			},
		)
	}
}

// Dijkstra sub-transactions apply before the top-level transaction, so a
// top-level vote sees a hot credential a sub-transaction authorized or
// resigned.
func TestValidateTxDijkstraTopLevelVoterSeesSubTransactionCertificates(
	t *testing.T,
) {
	t.Parallel()

	for _, resign := range []bool{false, true} {
		name := "sub-transaction authorization"
		if resign {
			name = "sub-transaction resignation"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pparams := committeeVotingDijkstra.pparams(
				lcommon.ProtocolVersionPlomin,
			)
			lv, db := committeeTestView(t, pparams)
			cold := committeeTestCredential(0x51)
			oldHot := committeeTestCredential(0x52)
			newHot := committeeTestCredential(0x53)
			seatCommitteeMembers(t, db, cold)
			seedCommitteeCredentialAuthorization(t, db, cold, oldHot, 1, 1)
			cert := authorizeHotCertificate(cold, newHot)
			voter := newHot
			if resign {
				cert = resignColdCertificate(cold)
				voter = oldHot
			}
			tx := &gdijkstra.DijkstraTransaction{
				TxIsValid: true,
				Body: gdijkstra.DijkstraTransactionBody{
					TxSubTransactions: cbor.NewSetType(
						[]gdijkstra.DijkstraSubTransaction{{
							Body: gdijkstra.DijkstraSubTransactionBody{
								TxCertificates: committeeVotingCertificates(
									[]lcommon.Certificate{cert},
								),
							},
						}},
						true,
					),
					TxVotingProcedures: lcommon.VotingProcedures{
						committeeVoter(voter): {},
					},
				},
			}
			err := eras.ValidateTxDijkstra(tx, 0, lv, pparams)
			var unknown conway.UnknownVoterError
			require.Equal(t, resign, errors.As(err, &unknown), "%v", err)
		})
	}
}

func TestValidateTxDijkstraRejectsUnelectedSubTransactionCommitteeVoter(
	t *testing.T,
) {
	t.Parallel()

	pparams := committeeVotingDijkstra.pparams(
		lcommon.ProtocolVersionVanRossem + 1,
	)
	lv, db := committeeTestView(t, pparams)
	seatedCold := committeeTestCredential(0x61)
	unelectedCold := committeeTestCredential(0x62)
	unelectedHot := committeeTestCredential(0x63)
	seatCommitteeMembers(t, db, seatedCold)
	seedCommitteeCredentialAuthorization(t, db, unelectedCold, unelectedHot, 1, 1)
	actionID := storeCommitteeVotingTarget(
		t, db, 0x64, lcommon.GovActionTypeInfo,
	)
	tx := &gdijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{
						TxVotingProcedures: lcommon.VotingProcedures{
							committeeVoter(unelectedHot): {
								actionID: {Vote: lcommon.GovVoteYes},
							},
						},
					},
				}},
				true,
			),
		},
	}
	err := eras.ValidateTxDijkstra(tx, 0, lv, pparams)
	var unknown conway.UnknownVoterError
	require.ErrorAs(t, err, &unknown, "%v", err)
}
