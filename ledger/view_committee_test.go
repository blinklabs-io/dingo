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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
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
	ls := &LedgerState{db: db, currentPParams: pparams}
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

func storeCommitteeUpdateProposal(
	t *testing.T,
	db *database.Database,
	seed byte,
	credential lcommon.Credential,
	expiry uint64,
) {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint{&credential: uint(expiry)},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        governanceTestHash(seed),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
	}, action)
}

func seedCommitteeAuthorization(
	t *testing.T,
	db *database.Database,
	coldKey lcommon.Blake2b224,
	hotKey lcommon.Blake2b224,
	certificateID uint64,
	slot uint64,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`,
		coldKey[:], hotKey[:], certificateID, slot,
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
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO resign_committee_cold (
    cold_credential, certificate_id, added_slot
) VALUES (?, ?, ?)`,
		coldKey[:], certificateID, slot,
	)
	require.NoError(t, err)
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

func TestLedgerViewPendingCommitteeCertificateValidation(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	lv, db := committeeTestView(t, pparams)
	seated := committeeTestCredential(0x61)
	require.NoError(t, db.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredHash: seated.Credential[:],
			ExpiresEpoch: 60,
		}},
		nil,
	))
	proposed := committeeTestCredential(0x62)
	storeCommitteeUpdateProposal(t, db, 0x63, proposed, 90)

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
	for _, certificate := range certificates {
		t.Run(certificateName(certificate), func(t *testing.T) {
			tx := &conway.ConwayTransaction{
				Body: conway.ConwayTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type:        certificate.Type(),
						Certificate: certificate,
					}},
				},
			}
			require.NoError(t, conway.UtxoValidateCommitteeCertificates(
				tx,
				0,
				lv,
				pparams,
			))
		})
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
