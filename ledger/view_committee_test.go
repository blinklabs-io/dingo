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
	"bytes"
	"errors"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func committeeTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, len(lcommon.Blake2b224{})),
		),
	}
}

func committeeTestView(
	t *testing.T,
	pparams lcommon.ProtocolParameters,
) (*LedgerView, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
		},
	}
	ls.publishSnapshotsLocked()
	return ls.NewView(nil), db
}

func storeCommitteeUpdateProposal(
	t *testing.T,
	db *database.Database,
	seed byte,
	credential lcommon.Credential,
	expiry uint64,
) {
	storeCommitteeUpdateProposalInTxn(
		t,
		db,
		seed,
		credential,
		expiry,
		nil,
	)
}

func storeCommitteeUpdateProposalInTxn(
	t *testing.T,
	db *database.Database,
	seed byte,
	credential lcommon.Credential,
	expiry uint64,
	txn *database.Txn,
) {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint{&credential: uint(expiry)},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        governanceTestHash(seed),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		AnchorHash:    make([]byte, 32),
		ReturnAddress: make([]byte, 29),
		GovActionCbor: encoded,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, txn))
}

func seedCommitteeAuthorization(
	t *testing.T,
	db *database.Database,
	coldKey lcommon.Blake2b224,
	hotKey lcommon.Blake2b224,
	certificateID uint64,
	slot uint64,
) {
	seedCommitteeCredentialAuthorization(
		t,
		db,
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: coldKey,
		},
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: hotKey,
		},
		certificateID,
		slot,
	)
}

func seedCommitteeCredentialAuthorization(
	t *testing.T,
	db *database.Database,
	coldCredential lcommon.Credential,
	hotCredential lcommon.Credential,
	certificateID uint64,
	slot uint64,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)`,
		coldCredential.CredType,
		coldCredential.Credential[:],
		hotCredential.CredType,
		hotCredential.Credential[:],
		certificateID,
		slot,
	)
	require.NoError(t, err)
}

func seedCommitteeResignation(
	t *testing.T,
	db *database.Database,
	coldKey lcommon.Blake2b224,
	certificateID uint64,
	slot uint64,
) {
	seedCommitteeCredentialResignation(
		t,
		db,
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: coldKey,
		},
		certificateID,
		slot,
	)
}

func seedCommitteeCredentialResignation(
	t *testing.T,
	db *database.Database,
	coldCredential lcommon.Credential,
	certificateID uint64,
	slot uint64,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO resign_committee_cold (
    cold_credential_tag, cold_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`,
		coldCredential.CredType,
		coldCredential.Credential[:],
		certificateID,
		slot,
	)
	require.NoError(t, err)
}

func TestLedgerViewProposedCommitteeMemberPreservesCertificateState(t *testing.T) {
	tests := []struct {
		name         string
		seed         func(*testing.T, *database.Database, lcommon.Credential)
		wantHot      bool
		wantResigned bool
	}{
		{
			name: "authorization",
			seed: func(t *testing.T, db *database.Database, cold lcommon.Credential) {
				seedCommitteeCredentialAuthorization(
					t,
					db,
					cold,
					committeeTestCredential(0x72),
					1,
					1,
				)
			},
			wantHot: true,
		},
		{
			// A resignation recorded after the replacement proposal belongs to
			// the term it happened in. Carrying it into the pending term would
			// reject the re-elected member's authorization as resigned. The
			// conformance provider already applies this rule.
			name: "resignation does not carry into the pending term",
			seed: func(t *testing.T, db *database.Database, cold lcommon.Credential) {
				seedCommitteeCredentialResignation(t, db, cold, 1, 1)
			},
			wantResigned: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
			cold := committeeTestCredential(0x71)
			storeCommitteeUpdateProposal(t, db, 0x73, cold, 90)
			test.seed(t, db, cold)

			member, err := lv.CommitteeCredentialMember(cold)
			require.NoError(t, err)
			require.NotNil(t, member)
			require.Equal(t, test.wantResigned, member.Resigned)
			if test.wantHot {
				require.NotNil(t, member.HotKey)
				require.Equal(t, committeeTestCredential(0x72).Credential, *member.HotKey)
			} else {
				require.Nil(t, member.HotKey)
			}
		})
	}
}

func TestLedgerViewCommitteeCredentialsDoNotAliasByHash(t *testing.T) {
	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	hash := committeeTestCredential(0x81).Credential
	keyCold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	scriptCold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hash,
	}
	hotHash := committeeTestCredential(0x82).Credential
	keyHot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hotHash,
	}
	scriptHot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hotHash,
	}
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{ColdCredentialTag: 0, ColdCredHash: hash[:], ExpiresEpoch: 41},
		{ColdCredentialTag: 1, ColdCredHash: hash[:], ExpiresEpoch: 42},
	}, nil))
	seedCommitteeCredentialAuthorization(t, db, keyCold, keyHot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, scriptCold, scriptHot, 2, 1)

	keyMember, err := lv.CommitteeCredentialMember(keyCold)
	require.NoError(t, err)
	require.NotNil(t, keyMember)
	require.Equal(t, uint64(41), keyMember.ExpiryEpoch)
	scriptMember, err := lv.CommitteeCredentialMember(scriptCold)
	require.NoError(t, err)
	require.NotNil(t, scriptMember)
	require.Equal(t, uint64(42), scriptMember.ExpiryEpoch)

	legacy, err := lv.CommitteeMember(hash)
	require.NoError(t, err)
	require.Nil(t, legacy)
	keyVoter, err := lv.CommitteeHotCredentialMember(keyHot)
	require.NoError(t, err)
	require.NotNil(t, keyVoter)
	require.Equal(t, uint64(41), keyVoter.ExpiryEpoch)
	scriptVoter, err := lv.CommitteeHotCredentialMember(scriptHot)
	require.NoError(t, err)
	require.NotNil(t, scriptVoter)
	require.Equal(t, uint64(42), scriptVoter.ExpiryEpoch)
}

func TestLedgerViewCommitteeHotCredentialSelection(t *testing.T) {
	tests := []struct {
		name       string
		expiries   []uint64
		wantMember bool
	}{
		{
			name:       "shared credential with boundary-active member",
			expiries:   []uint64{5, 6},
			wantMember: true,
		},
		{
			name:       "expired member",
			expiries:   []uint64{4},
			wantMember: false,
		},
	}
	eras := []struct {
		name     string
		pparams  lcommon.ProtocolParameters
		buildTx  func(lcommon.VotingProcedures) lcommon.Transaction
		validate func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error
	}{
		{
			name:    "conway",
			pparams: &conway.ConwayProtocolParameters{},
			buildTx: func(votes lcommon.VotingProcedures) lcommon.Transaction {
				return &conway.ConwayTransaction{
					Body: conway.ConwayTransactionBody{
						TxVotingProcedures: votes,
					},
					TxIsValid: true,
				}
			},
			validate: eras.ValidateTxConway,
		},
		{
			name:    "dijkstra",
			pparams: &gdijkstra.DijkstraProtocolParameters{},
			buildTx: func(votes lcommon.VotingProcedures) lcommon.Transaction {
				return &gdijkstra.DijkstraTransaction{
					Body: gdijkstra.DijkstraTransactionBody{
						TxVotingProcedures: votes,
					},
					TxIsValid: true,
				}
			},
			validate: eras.ValidateTxDijkstra,
		},
	}

	for _, test := range tests {
		for _, era := range eras {
			t.Run(test.name+"/"+era.name, func(t *testing.T) {
				lv, db := committeeTestView(t, era.pparams)
				lv.pinCommitteeState(5, era.pparams)
				lv.skipPhase2Validation = true
				hot := committeeTestCredential(0xb0)
				members := make(
					[]*models.CommitteeMember,
					0,
					len(test.expiries),
				)
				for i, expiry := range test.expiries {
					cold := committeeTestCredential(byte(0xb1 + i))
					members = append(members, &models.CommitteeMember{
						ColdCredentialTag: uint8(cold.CredType),
						ColdCredHash:      cold.Credential[:],
						ExpiresEpoch:      expiry,
					})
					seedCommitteeCredentialAuthorization(
						t,
						db,
						cold,
						hot,
						uint64(i+1),
						1,
					)
				}
				require.NoError(t, db.SetCommitteeMembers(members, nil))

				member, err := lv.CommitteeHotCredentialMember(hot)
				require.NoError(t, err)
				if test.wantMember {
					require.NotNil(
						t,
						member,
						"at least one active matching member must authorize the hot credential",
					)
				} else {
					require.Nil(
						t,
						member,
						"a member expired before the pinned epoch must not authorize the hot credential",
					)
				}

				voter := &lcommon.Voter{
					Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
					Hash: [28]byte(hot.Credential),
				}
				err = era.validate(
					era.buildTx(lcommon.VotingProcedures{voter: {}}),
					0,
					lv,
					era.pparams,
				)
				var unknown conway.UnknownVoterError
				if test.wantMember {
					require.False(t, errors.As(err, &unknown), "%v", err)
				} else {
					require.ErrorAs(t, err, &unknown)
				}
			})
		}
	}
}

func TestLedgerViewCommitteeProposalUsesPinnedSnapshot(t *testing.T) {
	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0x91)
	storeCommitteeUpdateProposal(t, db, 0x92, cold, 90)

	lv.ls.currentEpoch = models.Epoch{EpochId: 101}
	lv.ls.publishSnapshotsLocked()
	member, err := lv.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(t, member, "pinned validation view must keep epoch zero")

	fresh := lv.ls.NewView(nil)
	member, err = fresh.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.Nil(t, member, "fresh view must observe the later epoch")
}

func TestCommitteeCredentialStorageRollbackPreservesTags(t *testing.T) {
	_, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	hash := committeeTestCredential(0xa1).Credential
	members := []*models.CommitteeMember{
		{
			ColdCredentialTag: 0,
			ColdCredHash:      hash[:],
			ExpiresEpoch:      41,
			AddedSlot:         10,
		},
		{
			ColdCredentialTag: 1,
			ColdCredHash:      hash[:],
			ExpiresEpoch:      42,
			AddedSlot:         10,
		},
	}
	txn := db.MetadataTxn(true)
	require.NoError(t, db.SetCommitteeMembers(members, txn))
	require.NoError(t, txn.Rollback())
	txn.Release()

	stored, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Empty(t, stored)

	require.NoError(t, db.SetCommitteeMembers(members, nil))
	require.NoError(t, db.SoftDeleteCommitteeMembers(
		[]models.CommitteeCredential{{
			CredentialTag: 1,
			Credential:    hash[:],
		}},
		50,
		nil,
	))
	stored, err = db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, uint8(0), stored[0].ColdCredentialTag)

	require.NoError(t, db.DeleteCommitteeMembersAfterSlot(49, nil))
	stored, err = db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, stored, 2)
}

func TestCommitteeTermStartPresenceSurvivesStorageRollback(t *testing.T) {
	_, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0xa2)
	require.NoError(t, db.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      20,
			TermStartSlot:     0,
			TermStartSlotSet:  true,
			AddedSlot:         10,
		}},
		nil,
	))

	assertTermStart := func(wantStart uint64) {
		t.Helper()
		members, err := db.GetCommitteeMembers(nil)
		require.NoError(t, err)
		require.Len(t, members, 1)
		require.Equal(t, wantStart, members[0].TermStartSlot)
		require.True(t, members[0].TermStartSlotSet)
	}
	assertTermStart(0)

	require.NoError(t, db.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      30,
			TermStartSlot:     15,
			TermStartSlotSet:  true,
			AddedSlot:         20,
		}},
		nil,
	))
	assertTermStart(15)

	require.NoError(t, db.DeleteCommitteeMembersAfterSlot(15, nil))
	assertTermStart(0)
}

func TestLedgerViewCommitteeMember(t *testing.T) {
	t.Run("seated", func(t *testing.T) {
		lv, db := committeeTestView(
			t,
			&conway.ConwayProtocolParameters{},
		)
		cold := committeeTestCredential(0x11).Credential
		hot := committeeTestCredential(0x12).Credential
		require.NoError(t, db.SetCommitteeMembers(
			[]*models.CommitteeMember{{
				ColdCredHash: cold[:],
				ExpiresEpoch: 42,
			}},
			nil,
		))
		seedCommitteeAuthorization(t, db, cold, hot, 1, 1)

		member, err := lv.CommitteeMember(cold)
		require.NoError(t, err)
		require.NotNil(t, member)
		require.Equal(t, cold, member.ColdKey)
		require.Equal(t, uint64(42), member.ExpiryEpoch)
		require.Equal(t, &hot, member.HotKey)
		require.False(t, member.Resigned)
	})

	t.Run("pending proposal", func(t *testing.T) {
		lv, db := committeeTestView(
			t,
			&conway.ConwayProtocolParameters{},
		)
		credential := committeeTestCredential(0x21)
		storeCommitteeUpdateProposal(t, db, 0x22, credential, 75)

		member, err := lv.CommitteeMember(credential.Credential)
		require.NoError(t, err)
		require.NotNil(t, member)
		require.Equal(t, credential.Credential, member.ColdKey)
		require.Equal(t, uint64(75), member.ExpiryEpoch)
		require.Nil(t, member.HotKey)
		require.False(t, member.Resigned)
	})

	t.Run("seated resignation takes precedence over proposal", func(t *testing.T) {
		lv, db := committeeTestView(
			t,
			&conway.ConwayProtocolParameters{},
		)
		credential := committeeTestCredential(0x31)
		hot := committeeTestCredential(0x32).Credential
		require.NoError(t, db.SetCommitteeMembers(
			[]*models.CommitteeMember{{
				ColdCredHash: credential.Credential[:],
				ExpiresEpoch: 50,
			}},
			nil,
		))
		seedCommitteeAuthorization(
			t,
			db,
			credential.Credential,
			hot,
			1,
			1,
		)
		seedCommitteeResignation(t, db, credential.Credential, 2, 2)
		storeCommitteeUpdateProposal(t, db, 0x33, credential, 90)

		member, err := lv.CommitteeMember(credential.Credential)
		require.NoError(t, err)
		require.NotNil(t, member)
		require.Equal(t, uint64(50), member.ExpiryEpoch)
		require.Nil(t, member.HotKey)
		require.True(t, member.Resigned)
	})

	t.Run("unknown", func(t *testing.T) {
		lv, db := committeeTestView(
			t,
			&conway.ConwayProtocolParameters{},
		)
		storeCommitteeUpdateProposal(
			t,
			db,
			0x42,
			committeeTestCredential(0x43),
			85,
		)

		member, err := lv.CommitteeMember(
			committeeTestCredential(0x41).Credential,
		)
		require.NoError(t, err)
		require.Nil(t, member)
	})

	t.Run("proposal storage error", func(t *testing.T) {
		lv, db := committeeTestView(
			t,
			&conway.ConwayProtocolParameters{},
		)
		credential := committeeTestCredential(0x51)
		storeCommitteeUpdateProposal(t, db, 0x52, credential, 80)
		raw, err := dbtest.RawSQLiteMetadata(t, db)
		require.NoError(t, err)
		_, err = raw.Exec(
			"UPDATE governance_proposal SET deposit = ?",
			"not-an-amount",
		)
		require.NoError(t, err)

		member, err := lv.CommitteeMember(credential.Credential)
		require.ErrorContains(t, err, "get active governance proposals")
		require.Nil(t, member)
	})
}

func TestLedgerViewPendingCommitteeCertificateValidationSameTransaction(
	t *testing.T,
) {
	pparams := &conway.ConwayProtocolParameters{}
	initialView, db := committeeTestView(t, pparams)
	seated := committeeTestCredential(0x61)
	require.NoError(t, db.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredHash: seated.Credential[:],
			ExpiresEpoch: 60,
		}},
		nil,
	))
	proposed := committeeTestCredential(0x62)
	txn := db.MetadataTxn(true)
	t.Cleanup(func() {
		require.NoError(t, txn.Rollback())
		txn.Release()
	})
	storeCommitteeUpdateProposalInTxn(t, db, 0x63, proposed, 90, txn)
	lv := initialView.ls.NewView(txn)

	certificates := []lcommon.Certificate{
		&lcommon.AuthCommitteeHotCertificate{
			CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
			ColdCredential: proposed,
			HotCredential:  committeeTestCredential(0x64),
		},
		&lcommon.ResignCommitteeColdCertificate{
			CertType:       uint(lcommon.CertificateTypeResignCommitteeCold),
			ColdCredential: proposed,
		},
	}
	credentials := []struct {
		name          string
		credential    lcommon.Credential
		wantNotMember bool
	}{
		{name: "matching key credential", credential: proposed},
		{
			name: "opposite script credential",
			credential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeScriptHash,
				Credential: proposed.Credential,
			},
			wantNotMember: true,
		},
	}
	for _, certificate := range certificates {
		for _, credential := range credentials {
			t.Run(certificateName(certificate)+"/"+credential.name, func(t *testing.T) {
				switch cert := certificate.(type) {
				case *lcommon.AuthCommitteeHotCertificate:
					cert.ColdCredential = credential.credential
				case *lcommon.ResignCommitteeColdCertificate:
					cert.ColdCredential = credential.credential
				}
				tx := &conway.ConwayTransaction{
					// Committee certificates are only inspected for a
					// phase-2-valid transaction, so the fixture must declare
					// validity or the rule under test never runs.
					TxIsValid: true,
					Body: conway.ConwayTransactionBody{
						TxCertificates: []lcommon.CertificateWrapper{{
							Type:        certificate.Type(),
							Certificate: certificate,
						}},
					},
				}
				err := eras.ValidateTxConway(tx, 0, lv, pparams)
				var notMember conway.NotCommitteeMemberError
				if credential.wantNotMember {
					require.ErrorAs(t, err, &notMember)
				} else {
					require.False(
						t,
						errors.As(err, &notMember),
						"matching uncommitted proposal was rejected: %v",
						err,
					)
				}
			})
		}
	}
}

func certificateName(certificate lcommon.Certificate) string {
	switch certificate.(type) {
	case *lcommon.AuthCommitteeHotCertificate:
		return "authorize hot key"
	case *lcommon.ResignCommitteeColdCertificate:
		return "resign"
	default:
		return "unknown certificate"
	}
}

// An unsupported hot credential tag must be reported as invalid regardless of
// whether any authorizations exist. The tag check used to sit inside the loop
// over authorizations, so an empty committee returned no member and no error.
func TestLedgerViewCommitteeHotCredentialMemberRejectsUnsupportedTag(
	t *testing.T,
) {
	lv, _ := committeeTestView(t, &conway.ConwayProtocolParameters{})
	unsupported := lcommon.Credential{
		CredType:   99,
		Credential: committeeTestCredential(0x81).Credential,
	}

	member, err := lv.CommitteeHotCredentialMember(unsupported)
	require.Error(
		t,
		err,
		"an unsupported hot credential tag must not be reported as absent",
	)
	require.Nil(t, member)
	require.ErrorContains(t, err, "invalid committee hot credential")
}

// A re-elected member has several committee_member rows for one credential.
// Counting hashes alone dropped it from the seated list entirely.
func TestLedgerViewCommitteeMembersIncludesReelectedMember(t *testing.T) {
	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0x91)
	seedCommitteeMemberTerm(t, db, cold, 100, 10)
	seedCommitteeMemberTerm(t, db, cold, 200, 20)

	members, err := lv.CommitteeMembers()
	require.NoError(t, err)
	require.Len(
		t,
		members,
		1,
		"a re-elected credential is one seated member, not an ambiguous hash",
	)
	require.Equal(t, cold.Credential, members[0].ColdKey)
	require.Equal(
		t,
		uint64(200),
		members[0].ExpiryEpoch,
		"the latest term is the seated one",
	)
}

func seedCommitteeMemberTerm(
	t *testing.T,
	db *database.Database,
	coldCredential lcommon.Credential,
	expiresEpoch uint64,
	termStartSlot uint64,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO committee_member (
    cold_cred_hash, cold_credential_tag, expires_epoch,
    term_start_slot, term_start_slot_set, added_slot
) VALUES (?, ?, ?, ?, TRUE, ?)`,
		coldCredential.Credential[:],
		coldCredential.CredType,
		expiresEpoch,
		termStartSlot,
		termStartSlot,
	)
	require.NoError(t, err)
}

// TestLedgerViewProposedCommitteeMemberChainsFromNoConfidenceRoot proves the
// committee root is the latest enacted NoConfidence *or* UpdateCommittee.
//
// NoConfidence and UpdateCommittee chain off the same committee root. Querying
// only UpdateCommittee returns a stale root once a NoConfidence is enacted
// after it, and every pending proposal chained off the NoConfidence then falls
// outside the resolved lineage, so a re-elected member is dropped and its
// authorization is rejected as a non-member.
func TestLedgerViewProposedCommitteeMemberChainsFromNoConfidenceRoot(
	t *testing.T,
) {
	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0x81)

	enactedEpoch := uint64(10)
	// An older enacted UpdateCommittee. Querying UpdateCommittee alone
	// resolves this as the root.
	oldSlot := uint64(100)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:       governanceTestHash(0x82),
		ActionIndex:  1,
		ActionType:   uint8(lcommon.GovActionTypeUpdateCommittee),
		EnactedEpoch: &enactedEpoch,
		EnactedSlot:  &oldSlot,
	}, nil)

	// A NoConfidence enacted afterwards is the true current committee root.
	rootSlot := uint64(200)
	rootIdx := uint32(2)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:       governanceTestHash(0x83),
		ActionIndex:  rootIdx,
		ActionType:   uint8(lcommon.GovActionTypeNoConfidence),
		EnactedEpoch: &enactedEpoch,
		EnactedSlot:  &rootSlot,
	}, nil)

	// A pending UpdateCommittee re-electing the member, chained off the
	// NoConfidence root.
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint{&cold: uint(90)},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          governanceTestHash(0x84),
		ActionIndex:     3,
		ActionType:      uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch:   0,
		ExpiresEpoch:    100,
		ParentTxHash:    governanceTestHash(0x83),
		ParentActionIdx: &rootIdx,
		AnchorHash:      make([]byte, 32),
		ReturnAddress:   make([]byte, 29),
		GovActionCbor:   encoded,
	}, nil)

	member, err := lv.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(
		t,
		member,
		"pending member chained off the enacted NoConfidence root must resolve",
	)
	require.Equal(t, uint64(90), member.ExpiryEpoch)
}

// TestLedgerViewCommitteeStateAvailableTracksSeatedMembers proves availability
// is derived from seated committee members, not from the store being
// reachable.
//
// A view with a live database but no committee rows cannot answer committee
// queries: Dingo does not seed the Conway genesis committee
// (blinklabs-io/dingo#3785), so that state means "never populated" on a
// genesis-synced node, not "authoritatively empty". Reporting true there makes
// the validation rules reject an authorization from a real genesis committee
// member.
func TestLedgerViewCommitteeStateAvailableTracksSeatedMembers(t *testing.T) {
	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})

	// A reachable store with no seated member is not authoritative.
	available, err := lv.CommitteeStateAvailable()
	require.NoError(t, err)
	require.False(
		t,
		available,
		"a reachable store with no committee rows must not claim authority",
	)

	seated := committeeTestCredential(0x91)
	require.NoError(t, db.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredentialTag: uint8(seated.CredType),
			ColdCredHash:      seated.Credential[:],
			ExpiresEpoch:      60,
		}},
		nil,
	))

	available, err = lv.CommitteeStateAvailable()
	require.NoError(t, err)
	require.True(
		t,
		available,
		"a seated committee member makes committee state authoritative",
	)
}
