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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// committeeVotingEra drives a fully witnessed transaction through an era's
// production validation entry point, so an accept case is a real acceptance
// and a reject case fails only on the rule under test.
type committeeVotingEra struct {
	name     string
	pparams  func(major uint) lcommon.ProtocolParameters
	validate func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error
	build func(
		input shelley.ShelleyTransactionInput,
		output *shelley.ShelleyTransactionOutput,
		votes lcommon.VotingProcedures,
		certs []lcommon.Certificate,
	) lcommon.Transaction
	witness func(lcommon.Transaction, []lcommon.VkeyWitness)
}

func committeeVotingCertificates(
	certs []lcommon.Certificate,
) []lcommon.CertificateWrapper {
	ret := make([]lcommon.CertificateWrapper, 0, len(certs))
	for _, cert := range certs {
		ret = append(ret, lcommon.CertificateWrapper{
			Type:        cert.Type(),
			Certificate: cert,
		})
	}
	return ret
}

var committeeVotingConway = committeeVotingEra{
	name: "Conway",
	pparams: func(major uint) lcommon.ProtocolParameters {
		pparams := &conway.ConwayProtocolParameters{}
		pparams.ProtocolVersion.Major = major
		pparams.MaxTxSize = 16_384
		pparams.MaxValueSize = 5_000
		return pparams
	},
	validate: eras.ValidateTxConway,
	build: func(
		input shelley.ShelleyTransactionInput,
		output *shelley.ShelleyTransactionOutput,
		votes lcommon.VotingProcedures,
		certs []lcommon.Certificate,
	) lcommon.Transaction {
		return &conway.ConwayTransaction{
			Body: conway.ConwayTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{input},
				),
				TxOutputs: []babbage.BabbageTransactionOutput{{
					OutputAddress: output.OutputAddress,
					OutputAmount: mary.MaryTransactionOutputValue{
						Amount: output.OutputAmount,
					},
				}},
				TxCertificates:     committeeVotingCertificates(certs),
				TxVotingProcedures: votes,
			},
			TxIsValid: true,
		}
	},
	witness: func(tx lcommon.Transaction, witnesses []lcommon.VkeyWitness) {
		tx.(*conway.ConwayTransaction).WitnessSet.VkeyWitnesses =
			cbor.NewSetType(witnesses, true)
	},
}

var committeeVotingDijkstra = committeeVotingEra{
	name: "Dijkstra",
	pparams: func(major uint) lcommon.ProtocolParameters {
		pparams := dijkstraTestProtocolParameters()
		pparams.ConwayProtocolParameters.ProtocolVersion.Major = major
		return pparams
	},
	validate: eras.ValidateTxDijkstra,
	build: func(
		input shelley.ShelleyTransactionInput,
		output *shelley.ShelleyTransactionOutput,
		votes lcommon.VotingProcedures,
		certs []lcommon.Certificate,
	) lcommon.Transaction {
		return &gdijkstra.DijkstraTransaction{
			Body: gdijkstra.DijkstraTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{input},
				),
				TxOutputs: []gdijkstra.DijkstraTransactionOutput{
					{Output: output},
				},
				TxCertificates:     committeeVotingCertificates(certs),
				TxVotingProcedures: votes,
			},
			TxIsValid: true,
		}
	},
	witness: func(tx lcommon.Transaction, witnesses []lcommon.VkeyWitness) {
		tx.(*gdijkstra.DijkstraTransaction).WitnessSet.VkeyWitnesses =
			cbor.NewSetType(witnesses, true)
	},
}

// committeeVotingValidate builds a witnessed spend carrying votes and
// certificates, signed by the payment key and every extra key, and returns
// the era's validation result.
func committeeVotingValidate(
	t *testing.T,
	era committeeVotingEra,
	lv *LedgerView,
	pparams lcommon.ProtocolParameters,
	payment ed25519.PrivateKey,
	votes lcommon.VotingProcedures,
	certs []lcommon.Certificate,
	signers ...ed25519.PrivateKey,
) error {
	t.Helper()
	paymentHash := lcommon.Blake2b224Hash(payment.Public().(ed25519.PublicKey))
	input, address := committeeTestAddSpend(t, lv, paymentHash[:])
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	tx := era.build(input, output, votes, certs)
	witnesses := []lcommon.VkeyWitness{committeeTestVKeyWitness(tx, payment)}
	for _, signer := range signers {
		witnesses = append(witnesses, committeeTestVKeyWitness(tx, signer))
	}
	era.witness(tx, witnesses)
	return era.validate(tx, 0, lv, pparams)
}

func committeeVoter(hot lcommon.Credential) *lcommon.Voter {
	voterType := uint8(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash)
	if hot.CredType == lcommon.CredentialTypeScriptHash {
		voterType = lcommon.VoterTypeConstitutionalCommitteeHotScriptHash
	}
	return &lcommon.Voter{Type: voterType, Hash: [28]byte(hot.Credential)}
}

func seatCommitteeMembers(
	t *testing.T,
	db *database.Database,
	colds ...lcommon.Credential,
) {
	t.Helper()
	members := make([]*models.CommitteeMember, 0, len(colds))
	for _, cold := range colds {
		members = append(members, &models.CommitteeMember{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      10,
		})
	}
	require.NoError(t, db.SetCommitteeMembers(members, nil))
}

// requireOnlyRuleError requires that validation failed and that no rule other
// than the one under test contributed to the joined error.
func requireOnlyRuleError(t *testing.T, err error, target any) {
	t.Helper()
	require.ErrorAs(t, err, target)
	require.NotContains(t, err.Error(), "\n", "more than one rule failed")
}

// requireUnelectedCommitteeVoter requires the elected-voter rejection. For
// Dijkstra it tolerates a second rejection of the same voter: gouroboros'
// Dijkstra unknown-voter rule, which Dingo runs before its own, also rejects
// an unseated committee voter from PV11. Both reject the same transaction.
func requireUnelectedCommitteeVoter(
	t *testing.T,
	era committeeVotingEra,
	err error,
) {
	t.Helper()
	var unelected conway.UnelectedCommitteeVoterError
	if era.name == committeeVotingDijkstra.name {
		require.ErrorAs(t, err, &unelected)
		return
	}
	requireOnlyRuleError(t, err, &unelected)
}

func requireUnknownCommitteeVoter(t *testing.T, err error) {
	t.Helper()
	var unknown conway.UnknownVoterError
	requireOnlyRuleError(t, err, &unknown)
}

// A hot credential authorized by both an elected cold credential and a cold
// credential that is only in a pending UpdateCommittee proposal is an
// authorized elected hot credential: cardano-ledger's
// authorizedElectedHotCommitteeCredentials intersects the whole
// csCommitteeCreds map with the enacted committee, so the pending entry does
// not hide the elected one (Conway GOV, unelectedCommitteeVoters). The pending
// cold credential is ordered on both sides of the elected one, because the
// stored authorizations are read in cold-credential order.
func TestValidateTxAcceptsElectedVoterSharingHotKeyWithPendingMember(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, order := range []struct {
			name        string
			electedSeed byte
			pendingSeed byte
		}{
			{name: "pending sorts first", electedSeed: 0xf1, pendingSeed: 0x11},
			{name: "elected sorts first", electedSeed: 0x11, pendingSeed: 0xf1},
		} {
			for _, major := range []uint{
				lcommon.ProtocolVersionVanRossem,
				lcommon.ProtocolVersionVanRossem + 1,
			} {
				name := fmt.Sprintf("%s/%s/PV%d", era.name, order.name, major)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(major)
					lv, db := committeeTestView(t, pparams)
					elected := committeeTestCredential(order.electedSeed)
					pending := committeeTestCredential(order.pendingSeed)
					hot, hotKey := committeeTestVotingKey(0x5a)
					seatCommitteeMembers(t, db, elected)
					seedCommitteeCredentialAuthorization(t, db, elected, hot, 1, 1)
					storeCommitteeUpdateProposal(t, db, 0x5b, pending, 10)
					seedCommitteeCredentialAuthorization(t, db, pending, hot, 2, 2)

					err := committeeVotingValidate(
						t, era, lv, pparams, hotKey,
						lcommon.VotingProcedures{committeeVoter(hot): {}},
						nil,
					)
					require.NoError(t, err)
					coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(hot)
					require.NoError(t, err)
					require.Contains(t, coldCredentials, elected)
				})
			}
		}
	}
}

// cardano-ledger's EPOCH rule rebuilds csCommitteeCreds as its intersection
// with the enacted committee at every boundary (updateCommitteeState), so a
// hot credential authorized by a cold credential that is not seated stays
// known only for the rest of the epoch it was authorized in. After the
// boundary the vote is VotersDoNotExist at every protocol version.
func TestValidateTxPendingCommitteeAuthorizationLastsOneEpoch(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, tc := range []struct {
			name     string
			authSlot uint64
			known    bool
		}{
			{name: "authorized in a previous epoch", authSlot: epochStartSlot - 1},
			{name: "authorized at the epoch start", authSlot: epochStartSlot, known: true},
			{name: "authorized later in the epoch", authSlot: epochStartSlot + 1, known: true},
		} {
			t.Run(era.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				lv.epochStartSlot = epochStartSlot
				seatCommitteeMembers(t, db, committeeTestCredential(0x61))
				pending := committeeTestCredential(0x62)
				hot, hotKey := committeeTestVotingKey(0x63)
				storeCommitteeUpdateProposal(t, db, 0x64, pending, 10)
				seedCommitteeCredentialAuthorization(
					t, db, pending, hot, 1, tc.authSlot,
				)

				err := committeeVotingValidate(
					t, era, lv, pparams, hotKey,
					lcommon.VotingProcedures{committeeVoter(hot): {}},
					nil,
				)
				if tc.known {
					require.NoError(t, err)
				} else {
					requireUnknownCommitteeVoter(t, err)
				}
			})
		}
	}
}

// A resignation replaces a cold credential's csCommitteeCreds entry with
// CommitteeMemberResigned (GOVCERT), so the hot credential it had authorized
// is no longer an authorized hot credential, whether or not the cold
// credential is seated.
func TestValidateTxRejectsHotKeyOfResignedPendingCommitteeMember(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			pparams := era.pparams(lcommon.ProtocolVersionPlomin)
			lv, db := committeeTestView(t, pparams)
			seatCommitteeMembers(t, db, committeeTestCredential(0x71))
			pending := committeeTestCredential(0x72)
			hot, hotKey := committeeTestVotingKey(0x73)
			storeCommitteeUpdateProposal(t, db, 0x74, pending, 10)
			seedCommitteeCredentialAuthorization(t, db, pending, hot, 1, 1)
			seedCommitteeCredentialResignation(t, db, pending, 2, 2)

			err := committeeVotingValidate(
				t, era, lv, pparams, hotKey,
				lcommon.VotingProcedures{committeeVoter(hot): {}},
				nil,
			)
			requireUnknownCommitteeVoter(t, err)
		})
	}
}

// GOV sees the certificate state after CERTS (Conway LEDGER passes
// certStateAfterCERTS to GOV), so an elected member authorizing a hot
// credential in the same transaction makes that credential an authorized
// elected voter. The hot credential is already known through a pending
// member, so only the election gate distinguishes the two transactions.
func TestValidateTxSameTransactionAuthorizationElectsCommitteeVoter(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, withCert := range []bool{false, true} {
			name := era.name + "/without authorization"
			if withCert {
				name = era.name + "/with authorization"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionVanRossem)
				lv, db := committeeTestView(t, pparams)
				elected, electedKey := committeeTestVotingKey(0x81)
				seatCommitteeMembers(t, db, elected)
				seedCommitteeCredentialAuthorization(
					t, db, elected, committeeTestCredential(0x82), 1, 1,
				)
				pending := committeeTestCredential(0x83)
				hot, hotKey := committeeTestVotingKey(0x84)
				storeCommitteeUpdateProposal(t, db, 0x85, pending, 10)
				seedCommitteeCredentialAuthorization(t, db, pending, hot, 2, 2)

				var certs []lcommon.Certificate
				var signers []ed25519.PrivateKey
				if withCert {
					certs = []lcommon.Certificate{
						&lcommon.AuthCommitteeHotCertificate{
							CertType: uint(
								lcommon.CertificateTypeAuthCommitteeHot,
							),
							ColdCredential: elected,
							HotCredential:  hot,
						},
					}
					signers = []ed25519.PrivateKey{electedKey}
				}
				err := committeeVotingValidate(
					t, era, lv, pparams, hotKey,
					lcommon.VotingProcedures{committeeVoter(hot): {}},
					certs,
					signers...,
				)
				if withCert {
					require.NoError(t, err)
				} else {
					requireUnelectedCommitteeVoter(t, era, err)
				}
			})
		}
	}
}

// An in-transaction authorization adds a hot credential to the authorized
// elected set only when its cold credential is itself elected: the
// intersection in authorizedElectedHotCommitteeCredentials is by full cold
// credential, so neither a pending member nor a key credential sharing its
// hash with a seated script credential qualifies. The voter stays known
// through another pending member, so only the election gate decides.
func TestValidateTxSameTransactionAuthorizationByUnelectedColdCredential(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, sharesSeatedHash := range []bool{false, true} {
			name := era.name + "/pending member"
			if sharesSeatedHash {
				name = era.name + "/key credential sharing a seated script hash"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionVanRossem)
				lv, db := committeeTestView(t, pparams)
				authorizing, authorizingKey := committeeTestVotingKey(0xc1)
				seated := committeeTestCredential(0xc2)
				if sharesSeatedHash {
					seated = lcommon.Credential{
						CredType:   lcommon.CredentialTypeScriptHash,
						Credential: authorizing.Credential,
					}
				}
				seatCommitteeMembers(t, db, seated)
				storeCommitteeUpdateProposal(t, db, 0xc3, authorizing, 10)
				known := committeeTestCredential(0xc4)
				hot, hotKey := committeeTestVotingKey(0xc5)
				storeCommitteeUpdateProposal(t, db, 0xc6, known, 10)
				seedCommitteeCredentialAuthorization(t, db, known, hot, 1, 1)

				err := committeeVotingValidate(
					t, era, lv, pparams, hotKey,
					lcommon.VotingProcedures{committeeVoter(hot): {}},
					[]lcommon.Certificate{
						&lcommon.AuthCommitteeHotCertificate{
							CertType: uint(
								lcommon.CertificateTypeAuthCommitteeHot,
							),
							ColdCredential: authorizing,
							HotCredential:  hot,
						},
					},
					authorizingKey,
				)
				requireUnelectedCommitteeVoter(t, era, err)
			})
		}
	}
}

// Resigning one elected cold credential removes only that credential's
// entry; another elected cold credential authorizing the same hot credential
// keeps it an authorized elected voter.
func TestValidateTxConwaySameTransactionResignationKeepsOtherElectedMember(
	t *testing.T,
) {
	t.Parallel()

	for _, shared := range []bool{false, true} {
		name := "sole elected authorization"
		if shared {
			name = "second elected authorization"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			era := committeeVotingConway
			pparams := era.pparams(lcommon.ProtocolVersionVanRossem)
			lv, db := committeeTestView(t, pparams)
			resigning, resigningKey := committeeTestVotingKey(0x91)
			remaining := committeeTestCredential(0x92)
			hot, hotKey := committeeTestVotingKey(0x93)
			seatCommitteeMembers(t, db, resigning, remaining)
			seedCommitteeCredentialAuthorization(t, db, resigning, hot, 1, 1)
			if shared {
				seedCommitteeCredentialAuthorization(t, db, remaining, hot, 2, 1)
			}

			err := committeeVotingValidate(
				t, era, lv, pparams, hotKey,
				lcommon.VotingProcedures{committeeVoter(hot): {}},
				[]lcommon.Certificate{
					&lcommon.ResignCommitteeColdCertificate{
						CertType: uint(
							lcommon.CertificateTypeResignCommitteeCold,
						),
						ColdCredential: resigning,
					},
				},
				resigningKey,
			)
			if shared {
				require.NoError(t, err)
			} else {
				requireUnelectedCommitteeVoter(t, era, err)
			}
		})
	}
}

// The elected-voter gate is hardforkConwayDisallowUnelectedCommitteeFromVoting,
// pvMajor > 10, so it holds from PV11 and every later version, and PV10 still
// accepts an authorized member that is not elected.
func TestValidateTxUnelectedCommitteeVoterByProtocolVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		era    committeeVotingEra
		major  uint
		reject bool
	}{
		{era: committeeVotingConway, major: lcommon.ProtocolVersionPlomin},
		{
			era:    committeeVotingConway,
			major:  lcommon.ProtocolVersionVanRossem,
			reject: true,
		},
		{
			era:    committeeVotingConway,
			major:  lcommon.ProtocolVersionVanRossem + 1,
			reject: true,
		},
		{
			era:    committeeVotingDijkstra,
			major:  lcommon.ProtocolVersionVanRossem + 1,
			reject: true,
		},
	} {
		t.Run(fmt.Sprintf("%s/PV%d", tc.era.name, tc.major), func(t *testing.T) {
			t.Parallel()
			pparams := tc.era.pparams(tc.major)
			lv, db := committeeTestView(t, pparams)
			seatCommitteeMembers(t, db, committeeTestCredential(0xa1))
			pending := committeeTestCredential(0xa2)
			hot, hotKey := committeeTestVotingKey(0xa3)
			storeCommitteeUpdateProposal(t, db, 0xa4, pending, 10)
			seedCommitteeCredentialAuthorization(t, db, pending, hot, 1, 1)

			err := committeeVotingValidate(
				t, tc.era, lv, pparams, hotKey,
				lcommon.VotingProcedures{committeeVoter(hot): {}},
				nil,
			)
			if tc.reject {
				requireUnelectedCommitteeVoter(t, tc.era, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func storeCommitteeVotingTarget(
	t *testing.T,
	db *database.Database,
	seed byte,
	actionType lcommon.GovActionType,
) *lcommon.GovActionId {
	t.Helper()
	var action []any
	switch actionType {
	case lcommon.GovActionTypeInfo:
		action = []any{uint64(actionType)}
	case lcommon.GovActionTypeNoConfidence:
		action = []any{uint64(actionType), nil}
	default:
		t.Fatalf("unsupported voting target %d", actionType)
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        governanceTestHash(seed),
		ActionType:    uint8(actionType),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		AnchorHash:    make([]byte, 32),
		ReturnAddress: make([]byte, 29),
		GovActionCbor: encoded,
	}, nil))
	var txID [32]byte
	copy(txID[:], governanceTestHash(seed))
	return &lcommon.GovActionId{TransactionId: txID}
}

// isCommitteeVotingAllowed is NoVotingAllowed for NoConfidence and
// UpdateCommittee and has no protocol-version gate, so the DisallowedVoters
// check (checkVotersAreValid) rejects those votes at every Conway protocol
// version, while an elected member's vote on an allowed action is accepted.
func TestValidateTxCommitteeActionRestrictionAtEveryProtocolVersion(
	t *testing.T,
) {
	t.Parallel()

	type target struct {
		name       string
		actionType lcommon.GovActionType
		allowed    bool
	}
	targets := []target{
		{name: "InfoAction", actionType: lcommon.GovActionTypeInfo, allowed: true},
		{name: "NoConfidence", actionType: lcommon.GovActionTypeNoConfidence},
		{name: "UpdateCommittee", actionType: lcommon.GovActionTypeUpdateCommittee},
	}
	versions := []struct {
		era   committeeVotingEra
		major uint
		label string
	}{
		{era: committeeVotingConway, major: lcommon.ProtocolVersionPlomin - 1, label: "PV9"},
		{era: committeeVotingConway, major: lcommon.ProtocolVersionPlomin, label: "PV10"},
		{era: committeeVotingConway, major: lcommon.ProtocolVersionVanRossem, label: "PV11"},
		{era: committeeVotingConway, major: lcommon.ProtocolVersionVanRossem + 1, label: "PV12"},
		{era: committeeVotingDijkstra, major: lcommon.ProtocolVersionVanRossem + 1, label: "PV12"},
	}
	for _, version := range versions {
		for _, tgt := range targets {
			t.Run(version.era.name+"/"+version.label+"/"+tgt.name, func(t *testing.T) {
				t.Parallel()
				pparams := version.era.pparams(version.major)
				lv, db := committeeTestView(t, pparams)
				require.NoError(t, db.SetEpoch(0, 0, nil, nil, nil, nil, 0, 1, 100, nil))
				cold := committeeTestCredential(0xb1)
				hot, hotKey := committeeTestVotingKey(0xb2)
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
				var actionID *lcommon.GovActionId
				if tgt.actionType == lcommon.GovActionTypeUpdateCommittee {
					storeCommitteeUpdateProposal(
						t, db, 0xb3, committeeTestCredential(0xb4), 10,
					)
					var txID [32]byte
					copy(txID[:], governanceTestHash(0xb3))
					actionID = &lcommon.GovActionId{TransactionId: txID}
				} else {
					actionID = storeCommitteeVotingTarget(
						t, db, 0xb3, tgt.actionType,
					)
				}
				resolved, err := lv.GovActionById(*actionID)
				require.NoError(t, err)
				require.NotNil(t, resolved)

				err = committeeVotingValidate(
					t, version.era, lv, pparams, hotKey,
					lcommon.VotingProcedures{
						committeeVoter(hot): {
							actionID: {Vote: lcommon.GovVoteYes},
						},
					},
					nil,
				)
				if tgt.allowed {
					require.NoError(t, err)
					return
				}
				var restriction conway.CCVotingRestrictionError
				if version.major < lcommon.ProtocolVersionPlomin {
					// The bootstrap-phase vote restriction also rejects a
					// committee vote on a non-bootstrap action at PV9, and
					// cardano-ledger reports both failures.
					require.ErrorAs(t, err, &restriction)
					return
				}
				requireOnlyRuleError(t, err, &restriction)
			})
		}
	}
}
