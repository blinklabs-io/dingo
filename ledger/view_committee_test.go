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
			name: "resignation without prior authorization",
			seed: func(t *testing.T, db *database.Database, cold lcommon.Credential) {
				seedCommitteeCredentialResignation(t, db, cold, 1, 1)
			},
			wantResigned: true,
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
