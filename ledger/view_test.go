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
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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

func committeeTestVotingKey(
	seed byte,
) (lcommon.Credential, ed25519.PrivateKey) {
	privateKey := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{seed}, ed25519.SeedSize),
	)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.Blake2b224Hash(publicKey),
	}, privateKey
}

func committeeTestAddSpend(
	t *testing.T,
	lv *LedgerView,
	paymentKeyHash []byte,
) (shelley.ShelleyTransactionInput, lcommon.Address) {
	t.Helper()
	input := shelley.NewShelleyTransactionInput(strings.Repeat("a7", 32), 0)
	address := mustCommitteeTestAddress(t, paymentKeyHash)
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	lv.intraBlockUtxos = map[utxoref.Key]lcommon.Utxo{
		utxoref.ForInput(input): {Id: input, Output: output},
	}
	return input, address
}

func mustCommitteeTestAddress(
	t *testing.T,
	paymentKeyHash []byte,
) lcommon.Address {
	t.Helper()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentKeyHash,
		bytes.Repeat([]byte{0x7a}, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	return address
}

func committeeTestVKeyWitness(
	tx lcommon.Transaction,
	key ed25519.PrivateKey,
) lcommon.VkeyWitness {
	hash := tx.Hash()
	return lcommon.VkeyWitness{
		Vkey:      key.Public().(ed25519.PublicKey),
		Signature: ed25519.Sign(key, hash[:]),
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
		map[*lcommon.Credential]uint64{&credential: uint64(expiry)},
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

func TestLedgerViewProposedCommitteeMemberPreservesCertificateState(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name           string
		seed           func(*testing.T, *database.Database, lcommon.Credential)
		epochStartSlot uint64
		wantHot        bool
		wantResigned   bool
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
			// cardano-ledger drops an unseated credential's committee state at
			// every epoch boundary (Conway EPOCH, updateCommitteeState), so a
			// resignation from before the current epoch does not carry into
			// the pending term, and the conformance provider applies the same
			// window.
			name: "resignation does not carry into the pending term",
			seed: func(t *testing.T, db *database.Database, cold lcommon.Credential) {
				seedCommitteeCredentialResignation(t, db, cold, 1, 1)
			},
			epochStartSlot: 2,
			wantResigned:   false,
		},
		{
			// Within the epoch it was recorded in, a pending credential's
			// resignation stands, and GOVCERT rejects any later certificate
			// from it (ConwayCommitteeHasPreviouslyResigned).
			name: "resignation in the current epoch holds",
			seed: func(t *testing.T, db *database.Database, cold lcommon.Credential) {
				seedCommitteeCredentialResignation(t, db, cold, 1, 1)
			},
			wantResigned: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
			lv.epochStartSlot = test.epochStartSlot
			cold := committeeTestCredential(0x71)
			storeCommitteeUpdateProposal(t, db, 0x73, cold, 90)
			test.seed(t, db, cold)

			member, err := lv.CommitteeCredentialMember(cold)
			require.NoError(t, err)
			require.NotNil(t, member)
			require.Equal(t, test.wantResigned, member.Resigned)
			if test.wantHot {
				require.NotNil(t, member.HotKey)
				require.Equal(
					t,
					committeeTestCredential(0x72).Credential,
					*member.HotKey,
				)
				hot := committeeTestCredential(0x72)
				voterMember, err := lv.CommitteeHotCredentialMember(hot)
				require.NoError(t, err)
				require.NotNil(
					t,
					voterMember,
					"pending committee proposals still contribute authorization state",
				)
				elected, err := lv.CommitteeCredentialIsElected(cold)
				require.NoError(t, err)
				require.False(
					t,
					elected,
					"a pending member must remain distinct from an elected member",
				)
				voter := &lcommon.Voter{
					Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
					Hash: [28]byte(hot.Credential),
				}
				tx := &conway.ConwayTransaction{
					Body: conway.ConwayTransactionBody{
						TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
					},
					TxIsValid: true,
				}
				err = eras.ValidateTxConway(
					tx,
					0,
					lv,
					&conway.ConwayProtocolParameters{},
				)
				var unknownVoter conway.UnknownVoterError
				require.False(
					t,
					errors.As(err, &unknownVoter),
					"authorized pending voter must pass the unknown-voter rule: %v",
					err,
				)
			} else {
				require.Nil(t, member.HotKey)
			}
		})
	}
}

func TestLedgerViewCommitteeCredentialsDoNotAliasByHash(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	tests := []struct {
		name       string
		expiries   []uint64
		resign     bool
		wantMember bool
	}{
		{
			name:       "shared credential with seated member",
			expiries:   []uint64{5, 6},
			wantMember: true,
		},
		{
			// Term expiry is deliberately not applied on this path. The
			// Conway GOV rule resolves a committee voter against the
			// authorization map, which excludes only resigned members, and
			// applies expiry later in the RATIFY tally and the
			// committeeMinSize active count. Rejecting an expired member's
			// vote here would diverge from cardano-ledger, which accepts it.
			name:       "expired member still authorizes and votes",
			expiries:   []uint64{4},
			wantMember: true,
		},
		{
			// Resignation is the exclusion the upstream authorization set
			// does apply.
			name:       "resigned member does not authorize",
			expiries:   []uint64{6},
			resign:     true,
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
					if test.resign {
						seedCommitteeCredentialResignation(
							t,
							db,
							cold,
							uint64(100+i),
							2,
						)
					}
				}
				require.NoError(t, db.SetCommitteeMembers(members, nil))

				member, err := lv.CommitteeHotCredentialMember(hot)
				require.NoError(t, err)
				if test.wantMember {
					require.NotNil(
						t,
						member,
						"a seated matching member must authorize the hot credential",
					)
				} else {
					require.Nil(
						t,
						member,
						"a resigned member must not authorize the hot credential",
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

// GOVCERT's isPotentialFutureMember reads the whole proposals set (Conway
// Rules/Ledger.hs committeeProposals = proposalsWithPurpose grCommitteeL
// proposals), so an UpdateCommittee past its last voting epoch still
// authorizes until the boundary that drops it, in every view.
func TestLedgerViewCommitteeProposalFollowsProposalSet(t *testing.T) {
	t.Parallel()

	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0x91)
	storeCommitteeUpdateProposal(t, db, 0x92, cold, 90)

	lv.ls.currentEpoch = models.Epoch{EpochId: 101}
	lv.ls.publishSnapshotsLocked()
	member, err := lv.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(t, member, "pinned view dropped a proposals-set member")

	fresh := lv.ls.NewView(nil)
	member, err = fresh.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(t, member, "expired proposal left the set before its drop")

	proposal, err := db.GetGovernanceProposal(governanceTestHash(0x92), 0, nil)
	require.NoError(t, err)
	expired, dropped := uint64(101), uint64(102)
	proposal.ExpiredEpoch = &expired
	proposal.DroppedEpoch = &dropped
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	member, err = lv.ls.NewView(nil).CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.Nil(t, member, "dropped proposal still authorizes")
}

func TestCommitteeCredentialStorageRollbackPreservesTags(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

	t.Run(
		"seated resignation takes precedence over proposal",
		func(t *testing.T) {
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
		},
	)

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
		require.ErrorContains(t, err, "get governance proposal set")
		require.Nil(t, member)
	})
}

func TestLedgerViewPendingCommitteeCertificateValidationSameTransaction(
	t *testing.T,
) {
	t.Parallel()

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
			t.Run(
				certificateName(certificate)+"/"+credential.name,
				func(t *testing.T) {
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
				},
			)
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
		map[*lcommon.Credential]uint64{&cold: uint64(90)},
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
// separates the two empty committee states, which decides whether committee
// validation rejects.
//
// Without a genesis declaration, no rows at all is ambiguous. Rows that are
// all soft-deleted mean the committee was seated and is now authoritatively
// empty, as after a
// NoConfidence enactment, which must still reject a former member.
func TestLedgerViewCommitteeStateAvailableTracksSeatedMembers(t *testing.T) {
	t.Parallel()

	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})

	// A reachable store with no rows or genesis declaration is not authoritative.
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

	// NoConfidence soft-deletes every member. The committee is now
	// authoritatively empty, not unknown, so authority must survive.
	require.NoError(t, db.SoftDeleteAllCommitteeMembers(10, nil))
	seatedNow, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Empty(t, seatedNow, "no member may remain seated")

	available, err = lv.CommitteeStateAvailable()
	require.NoError(t, err)
	require.True(
		t,
		available,
		"an authoritatively empty committee after NoConfidence must stay authoritative",
	)
}

// enactTestUpdateCommittee drives a real UpdateCommittee enactment through
// governance.EnactProposal -- the same production entry point epoch-boundary
// processing calls -- rather than seeding committee_member/auth rows
// directly, so this test exercises applyUpdateCommittee's actual
// TermStartSlot-stamping decision (blinklabs-io/dingo#4584).
func enactTestUpdateCommittee(
	t *testing.T,
	db *database.Database,
	pparams lcommon.ProtocolParameters,
	slot uint64,
	credEpochs map[*lcommon.Credential]uint64,
) {
	t.Helper()
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		credEpochs,
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        governanceTestHash(byte(slot)),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		AnchorHash:    make([]byte, 32),
		ReturnAddress: make([]byte, 29),
		GovActionCbor: encoded,
		AddedSlot:     slot,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	_, err = governance.EnactProposal(&governance.EnactmentContext{
		DB:      db,
		Epoch:   0,
		Slot:    slot,
		PParams: pparams,
	}, proposal)
	require.NoError(t, err)
}

// TestLedgerViewCommitteeHotCredentialSurvivesTermRenewal reproduces the
// blinklabs-io/dingo#4584 live Preview halt end-to-end, driving the real
// enactment path (governance.EnactProposal -> applyUpdateCommittee) rather
// than seeding raw rows: a continuing committee member's one-time hot-key
// authorization must survive a later UpdateCommittee action that renews the
// committee's term, and the vote it authorizes must not be rejected as cast
// by an unknown voter (conway UTXO validation rule 52 in the observed
// incident).
func TestLedgerViewCommitteeHotCredentialSurvivesTermRenewal(t *testing.T) {
	t.Parallel()

	pparams := &conway.ConwayProtocolParameters{}
	lv, db := committeeTestView(t, pparams)
	lv.pinCommitteeState(5, pparams)
	lv.skipPhase2Validation = true

	cold := committeeTestCredential(0xd1)
	hot := committeeTestCredential(0xd2)

	// First election at slot 10; the member authorizes its hot key once, at
	// slot 20.
	enactTestUpdateCommittee(
		t, db, pparams, 10,
		map[*lcommon.Credential]uint64{&cold: 50},
	)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 20)

	// A renewal: a later UpdateCommittee action re-elects the same
	// credential (continuing membership, not removal-then-rejoin), with no
	// new AuthCommitteeHot certificate.
	enactTestUpdateCommittee(
		t, db, pparams, 30,
		map[*lcommon.Credential]uint64{&cold: 100},
	)

	member, err := lv.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(t, member)
	require.False(t, member.Resigned)
	require.NotNil(
		t,
		member.HotKey,
		"a continuing member's authorization must survive a term renewal",
	)
	require.Equal(t, hot.Credential, *member.HotKey)

	voterMember, err := lv.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.NotNil(
		t,
		voterMember,
		"the renewed committee's authorization map must still resolve the voter",
	)

	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
		TxIsValid: true,
	}
	err = eras.ValidateTxConway(tx, 0, lv, pparams)
	var unknown conway.UnknownVoterError
	require.False(
		t,
		errors.As(err, &unknown),
		"a term renewal must not reject a still-valid hot key as unknown: %v",
		err,
	)
}

func TestValidateTxConwayRejectsUnelectedCommitteeVoterAtPV11(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = lcommon.ProtocolVersionVanRossem
	lv, db := committeeTestView(t, pparams)
	lv.skipPhase2Validation = true
	cold := committeeTestCredential(0xd1)
	hot := committeeTestCredential(0xd2)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
	storeCommitteeUpdateProposal(t, db, 0xd4, cold, 10)
	elected := committeeTestCredential(0xd3)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(elected.CredType),
		ColdCredHash:      elected.Credential[:],
		ExpiresEpoch:      10,
	}}, nil))
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
		TxIsValid: true,
	}

	err := eras.ValidateTxConway(tx, 0, lv, pparams)
	require.ErrorContains(t, err, "committee voter is not elected")
}

func TestValidateTxDijkstraAcceptsElectedCommitteeVoter(t *testing.T) {
	pparams := dijkstraTestProtocolParameters()
	pparams.ConwayProtocolParameters.ProtocolVersion.Major =
		lcommon.ProtocolVersionVanRossem
	pparams.ConwayProtocolParameters.MaxTxSize = 16_384
	pparams.ConwayProtocolParameters.MaxValueSize = 5_000
	lv, db := committeeTestView(t, pparams)
	cold := committeeTestCredential(0xe1)
	hot, votingKey := committeeTestVotingKey(0xe2)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      10,
	}}, nil))
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	input, address := committeeTestAddSpend(t, lv, hot.Credential[:])
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{
				{Output: output},
			},
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
		TxIsValid: true,
	}
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{committeeTestVKeyWitness(tx, votingKey)}, true,
	)

	require.NoError(t, eras.ValidateTxDijkstra(tx, 0, lv, pparams))
}

func TestValidateTxConwayAcceptsElectedCommitteeVoterAtPV11(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = lcommon.ProtocolVersionVanRossem
	pparams.MaxTxSize = 16_384
	pparams.MaxValueSize = 5_000
	lv, db := committeeTestView(t, pparams)
	cold := committeeTestCredential(0xd1)
	hot, votingKey := committeeTestVotingKey(0xd2)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      10,
	}}, nil))
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	input, address := committeeTestAddSpend(t, lv, hot.Credential[:])
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	tx := &conway.ConwayTransaction{
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
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
		TxIsValid: true,
	}
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{committeeTestVKeyWitness(tx, votingKey)}, true,
	)

	require.NoError(t, eras.ValidateTxConway(tx, 0, lv, pparams))
}

func TestValidateTxConwayAcceptsAuthorizedPendingCommitteeVoterAtPV10(
	t *testing.T,
) {
	t.Parallel()

	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = lcommon.ProtocolVersionPlomin
	pparams.MaxTxSize = 16_384
	pparams.MaxValueSize = 5_000
	lv, db := committeeTestView(t, pparams)
	cold := committeeTestCredential(0xd8)
	hot, votingKey := committeeTestVotingKey(0xd9)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
	elected := committeeTestCredential(0xda)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(elected.CredType),
		ColdCredHash:      elected.Credential[:],
		ExpiresEpoch:      10,
	}}, nil))
	storeCommitteeUpdateProposal(t, db, 0xdb, cold, 10)
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	input, address := committeeTestAddSpend(t, lv, hot.Credential[:])
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	tx := &conway.ConwayTransaction{
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
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
		TxIsValid: true,
	}
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{committeeTestVKeyWitness(tx, votingKey)}, true,
	)

	require.NoError(t, eras.ValidateTxConway(tx, 0, lv, pparams))
}

func TestValidateTxCommitteeCertsAffectSameTransactionVoterElection(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name        string
		certificate func(
			cold, oldHot, newHot lcommon.Credential,
		) lcommon.Certificate
	}{
		{
			name: "hot key replacement",
			certificate: func(
				cold, _ lcommon.Credential,
				newHot lcommon.Credential,
			) lcommon.Certificate {
				return &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: cold,
					HotCredential:  newHot,
				}
			},
		},
		{
			name: "cold key resignation",
			certificate: func(
				cold, _, _ lcommon.Credential,
			) lcommon.Certificate {
				return &lcommon.ResignCommitteeColdCertificate{
					CertType: uint(
						lcommon.CertificateTypeResignCommitteeCold,
					),
					ColdCredential: cold,
				}
			},
		},
	}

	for _, era := range []string{"Conway", "Dijkstra"} {
		for _, test := range tests {
			t.Run(era+"/"+test.name, func(t *testing.T) {
				pparams := &conway.ConwayProtocolParameters{}
				pparams.ProtocolVersion.Major = lcommon.ProtocolVersionVanRossem
				lv, db := committeeTestView(t, pparams)
				lv.skipPhase2Validation = true
				cold := committeeTestCredential(0xdc)
				oldHot := committeeTestCredential(0xdd)
				newHot := committeeTestCredential(0xde)
				seedCommitteeCredentialAuthorization(t, db, cold, oldHot, 1, 1)
				require.NoError(
					t,
					db.SetCommitteeMembers([]*models.CommitteeMember{{
						ColdCredentialTag: uint8(cold.CredType),
						ColdCredHash:      cold.Credential[:],
						ExpiresEpoch:      10,
					}}, nil),
				)
				cert := test.certificate(cold, oldHot, newHot)
				voter := &lcommon.Voter{
					Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
					Hash: [28]byte(oldHot.Credential),
				}
				var err error
				if era == "Conway" {
					tx := &conway.ConwayTransaction{
						Body: conway.ConwayTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type: cert.Type(), Certificate: cert,
							}},
							TxVotingProcedures: lcommon.VotingProcedures{
								voter: {},
							},
						},
						TxIsValid: true,
					}
					err = eras.ValidateTxConway(tx, 0, lv, pparams)
				} else {
					dijkstraPParams := &gdijkstra.DijkstraProtocolParameters{
						ConwayProtocolParameters: *pparams,
					}
					tx := &gdijkstra.DijkstraTransaction{
						Body: gdijkstra.DijkstraTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type: cert.Type(), Certificate: cert,
							}},
							TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
						},
						TxIsValid: true,
					}
					err = eras.ValidateTxDijkstra(tx, 0, lv, dijkstraPParams)
				}
				require.ErrorContains(t, err, "committee voter is not elected")
			})
		}
	}
}

func TestValidateTxConwayRejectsCommitteeUpdateVoteBeforePV11(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		version uint
	}{
		{name: "PV9", version: lcommon.ProtocolVersionPlomin - 1},
		{name: "PV10", version: lcommon.ProtocolVersionPlomin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			validateCommitteeUpdateVoteRejectedAtVersion(t, tc.version)
		})
	}
}

func validateCommitteeUpdateVoteRejectedAtVersion(t *testing.T, version uint) {
	t.Helper()
	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = version
	lv, db := committeeTestView(t, pparams)
	lv.skipPhase2Validation = true
	cold := committeeTestCredential(0xd5)
	hot := committeeTestCredential(0xd6)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      10,
	}}, nil))
	storeCommitteeUpdateProposal(t, db, 0xd7, cold, 10)
	require.NoError(t, db.SetEpoch(0, 0, nil, nil, nil, nil, 0, 1, 100, nil))
	var actionTxID [32]byte
	copy(actionTxID[:], governanceTestHash(0xd7))
	actionID := &lcommon.GovActionId{TransactionId: actionTxID}
	resolvedAction, err := lv.GovActionById(*actionID)
	require.NoError(t, err)
	require.NotNil(t, resolvedAction)
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{
				voter: {actionID: {}},
			},
		},
		TxIsValid: true,
	}

	err = eras.ValidateTxConway(tx, 0, lv, pparams)
	require.ErrorContains(t, err, "CC cannot vote on UpdateCommittee")
}

// TestLedgerViewCommitteeResignationSurvivesTermRenewal covers the opposite
// direction of the same TermStartSlot-stamping defect. cardano-ledger keeps
// the CommitteeState entry of every cold credential still present in the
// enacted committee (Map.intersection in the Conway EPOCH rule), so a
// CommitteeMemberResigned marker survives a term renewal and a later
// AuthCommitteeHot certificate from that credential is rejected
// (ConwayCommitteeHasPreviouslyResigned). Stamping a fresh TermStartSlot on a
// continuing member moved the term past the resignation certificate, silently
// un-resigning the member and accepting a certificate cardano-node rejects.
func TestLedgerViewCommitteeResignationSurvivesTermRenewal(t *testing.T) {
	t.Parallel()

	pparams := &conway.ConwayProtocolParameters{}
	lv, db := committeeTestView(t, pparams)
	lv.pinCommitteeState(5, pparams)
	lv.skipPhase2Validation = true

	cold := committeeTestCredential(0xd7)
	hot := committeeTestCredential(0xd8)

	enactTestUpdateCommittee(
		t, db, pparams, 10,
		map[*lcommon.Credential]uint64{&cold: 50},
	)
	seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 20)
	seedCommitteeCredentialResignation(t, db, cold, 2, 25)

	enactTestUpdateCommittee(
		t, db, pparams, 30,
		map[*lcommon.Credential]uint64{&cold: 100},
	)

	member, err := lv.CommitteeCredentialMember(cold)
	require.NoError(t, err)
	require.NotNil(t, member)
	require.True(
		t,
		member.Resigned,
		"a term renewal must not un-resign a continuing member",
	)
	require.Nil(t, member.HotKey)

	voterMember, err := lv.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.Nil(
		t,
		voterMember,
		"a resigned member's hot key must not resolve after a term renewal",
	)

	certificate := &lcommon.AuthCommitteeHotCertificate{
		CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
		ColdCredential: cold,
		HotCredential:  hot,
	}
	authTx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type:        certificate.Type(),
				Certificate: certificate,
			}},
		},
	}
	var resigned conway.ResignedCommitteeMemberHotKeyError
	require.ErrorAs(
		t,
		eras.ValidateTxConway(authTx, 0, lv, pparams),
		&resigned,
		"a resigned member must not be able to re-authorize after a renewal",
	)
}

// TestLedgerViewCommitteeHotCredentialMembersReturnsEveryActiveAuthorization
// is the direct proof for the gouroboros#2574 plural capability: when two
// cold credentials both currently authorize the same hot credential,
// CommitteeHotCredentialMembers must return both, not just whichever one the
// singular CommitteeHotCredentialMember happens to find first (GOVCERT keeps
// one authorization entry per cold credential, and cardano-ledger's own
// authorizedHotCommitteeCredentials folds every entry into a set for exactly
// this reason -- see the CommitteeHotCredentialMembers doc comment in
// gouroboros ledger/common/state.go). Resigning one cold credential must
// leave only the other in the result.
func TestLedgerViewCommitteeHotCredentialMembersReturnsEveryActiveAuthorization(
	t *testing.T,
) {
	t.Parallel()

	lv, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	hot := committeeTestCredential(0xc0)
	coldA := committeeTestCredential(0xc1)
	coldB := committeeTestCredential(0xc2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredentialTag: uint8(coldA.CredType),
			ColdCredHash:      coldA.Credential[:],
			ExpiresEpoch:      10,
		},
		{
			ColdCredentialTag: uint8(coldB.CredType),
			ColdCredHash:      coldB.Credential[:],
			ExpiresEpoch:      10,
		},
	}, nil))
	seedCommitteeCredentialAuthorization(t, db, coldA, hot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, coldB, hot, 2, 1)

	members, err := lv.CommitteeHotCredentialMembers(hot)
	require.NoError(t, err)
	require.Len(
		t,
		members,
		2,
		"both cold credentials currently authorize the shared hot credential",
	)
	gotColdKeys := make([]lcommon.Blake2b224, 0, len(members))
	for _, member := range members {
		gotColdKeys = append(gotColdKeys, member.ColdKey)
	}
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b224{coldA.Credential, coldB.Credential},
		gotColdKeys,
	)

	seedCommitteeCredentialResignation(t, db, coldA, 3, 2)

	members, err = lv.CommitteeHotCredentialMembers(hot)
	require.NoError(t, err)
	require.Len(
		t,
		members,
		1,
		"only the un-resigned cold credential still authorizes the hot credential",
	)
	require.Equal(t, coldB.Credential, members[0].ColdKey)

	single, err := lv.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.NotNil(t, single)
	require.Equal(
		t,
		coldB.Credential,
		single.ColdKey,
		"the singular accessor must still resolve the remaining authorizer",
	)
}

// TestValidateTxDijkstraAcceptsVoteWhenSharedHotCredentialColdKeyResignsInTx
// is the end-to-end regression for gouroboros#2574 through dingo's real
// Dijkstra validation path (eras.ValidateTxDijkstra -> a real *LedgerView).
//
// Cold A and cold B both currently authorize hot H (persisted, before this
// transaction). This transaction's first level (a Dijkstra sub-transaction)
// resigns cold A; its last level (the outer transaction body) casts a
// committee vote under hot H. Cold A's resignation must not take voting
// rights away from cold B, which still authorizes H and was never touched by
// this transaction.
//
// gouroboros's per-transaction committee bookkeeping
// (dijkstraGovernanceStateView) tracks only cold credentials this
// transaction's own certificates touched (cold A here); for every other cold
// credential it falls back to what the ledger state reports for hot H. Before
// gouroboros#2574 that fallback was the singular CommitteeHotCredentialMember,
// which returns at most one witness and could return cold A -- correctly
// excluded as touched, but leaving cold B's authorization undiscovered and the
// vote wrongly rejected as unknown. LedgerView.CommitteeHotCredentialMembers
// (added by this change) reports both, so gouroboros can exclude the touched
// one and still find cold B.
func TestValidateTxDijkstraAcceptsVoteWhenSharedHotCredentialColdKeyResignsInTx(
	t *testing.T,
) {
	t.Parallel()

	pparams := &gdijkstra.DijkstraProtocolParameters{}
	lv, db := committeeTestView(t, pparams)
	hot := committeeTestCredential(0xd0)
	coldA := committeeTestCredential(0xd1)
	coldB := committeeTestCredential(0xd2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredentialTag: uint8(coldA.CredType),
			ColdCredHash:      coldA.Credential[:],
			ExpiresEpoch:      10,
		},
		{
			ColdCredentialTag: uint8(coldB.CredType),
			ColdCredHash:      coldB.Credential[:],
			ExpiresEpoch:      10,
		},
	}, nil))
	seedCommitteeCredentialAuthorization(t, db, coldA, hot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, coldB, hot, 2, 1)

	resign := &lcommon.ResignCommitteeColdCertificate{
		CertType:       uint(lcommon.CertificateTypeResignCommitteeCold),
		ColdCredential: coldA,
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hot.Credential),
	}
	tx := &gdijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body: gdijkstra.DijkstraTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{
					{
						Body: gdijkstra.DijkstraSubTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type:        resign.Type(),
								Certificate: resign,
							}},
						},
					},
				},
				false,
			),
		},
	}

	err := eras.ValidateTxDijkstra(tx, 0, lv, pparams)
	var unknown conway.UnknownVoterError
	require.False(
		t,
		errors.As(err, &unknown),
		"cold B still authorizes hot after cold A resigns within this "+
			"transaction, so the vote must not be rejected as unknown: %v",
		err,
	)
}

// TestConwayKnownVoterRuleResolvesEveryAuthorizerOfSharedHotCredential runs
// the upstream Conway known-voter rule against a real LedgerView at protocol
// versions 9, 10 and 11. The transaction resigns cold credential A, which
// authorizes hot credential H, and votes as H.
//
// Reference: cardano-ledger-core's authorizedHotCommitteeCredentials is the
// set of hot credentials that any non-resigned csCommitteeCreds entry maps to,
// so H stays a known voter while another cold credential still authorizes it,
// and becomes unknown once its only authorizer resigns; from protocol version
// 11 the voter must also belong to the enacted committee.
func TestConwayKnownVoterRuleResolvesEveryAuthorizerOfSharedHotCredential(
	t *testing.T,
) {
	t.Parallel()

	const (
		sharerSeated = "seated"
		sharerNone   = "none"
	)
	for _, tc := range []struct {
		sharer      string
		major       uint
		wantUnknown bool
	}{
		{sharerSeated, 9, false},
		{sharerSeated, 10, false},
		{sharerSeated, 11, false},
		{sharerNone, 9, true},
		{sharerNone, 10, true},
		{sharerNone, 11, true},
	} {
		t.Run(
			fmt.Sprintf("%s sharer pv%d", tc.sharer, tc.major),
			func(t *testing.T) {
				t.Parallel()

				pparams := &conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: tc.major,
					},
				}
				lv, db := committeeTestView(t, pparams)
				hot := committeeTestCredential(0xe0)
				coldA := committeeTestCredential(0xe1)
				coldB := committeeTestCredential(0xe2)
				seated := []*models.CommitteeMember{{
					ColdCredentialTag: uint8(coldA.CredType),
					ColdCredHash:      coldA.Credential[:],
					ExpiresEpoch:      10,
				}}
				if tc.sharer == sharerSeated {
					seated = append(seated, &models.CommitteeMember{
						ColdCredentialTag: uint8(coldB.CredType),
						ColdCredHash:      coldB.Credential[:],
						ExpiresEpoch:      10,
					})
				}
				require.NoError(t, db.SetCommitteeMembers(seated, nil))
				seedCommitteeCredentialAuthorization(t, db, coldA, hot, 1, 1)
				if tc.sharer == sharerSeated {
					seedCommitteeCredentialAuthorization(
						t,
						db,
						coldB,
						hot,
						2,
						1,
					)
				}

				resign := &lcommon.ResignCommitteeColdCertificate{
					CertType: uint(
						lcommon.CertificateTypeResignCommitteeCold,
					),
					ColdCredential: coldA,
				}
				voter := &lcommon.Voter{
					Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
					Hash: [28]byte(hot.Credential),
				}
				tx := &conway.ConwayTransaction{
					TxIsValid: true,
					Body: conway.ConwayTransactionBody{
						TxCertificates: []lcommon.CertificateWrapper{{
							Type:        resign.Type(),
							Certificate: resign,
						}},
						TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
					},
				}

				err := conway.UtxoValidateUnknownVoters(tx, 0, lv, pparams)
				if tc.wantUnknown {
					var unknown conway.UnknownVoterError
					require.ErrorAs(t, err, &unknown)
					return
				}
				require.NoError(t, err)
			},
		)
	}
}

// The guardrails rule reaches the enacted constitution through
// common.LedgerState, so drift that stopped *LedgerView from satisfying that
// interface would silently remove the constitution from validation instead
// of failing to build.
var _ lcommon.LedgerState = (*LedgerView)(nil)

// constitutionTestView builds a LedgerView over a file-backed test database,
// so tests that must make the constitution row unreadable can reach the
// underlying SQLite file.
func constitutionTestView(t *testing.T) (*LedgerView, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentPParams: &conway.ConwayProtocolParameters{},
	}
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

// constitutionTestWithdrawalTx builds a valid transaction carrying a single
// treasury-withdrawal proposal with the given optional guardrails policy
// hash. Treasury withdrawal is one of the two action types the guardrails
// rule checks, and it needs no ledger state of its own to construct.
func constitutionTestWithdrawalTx(
	t *testing.T,
	policyHash []byte,
) *conway.ConwayTransaction {
	t.Helper()
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		bytes.Repeat([]byte{0x77}, 28),
	)
	require.NoError(t, err)
	action, err := lcommon.NewTreasuryWithdrawalGovAction(
		map[*lcommon.Address]uint64{&address: 1_000_000},
		policyHash,
	)
	require.NoError(t, err)
	wrapper, err := conway.NewConwayGovAction(action)
	require.NoError(t, err)
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPGovAction: wrapper,
			}},
		},
		TxIsValid: true,
	}
}

func constitutionTestGuardrails(
	t *testing.T,
	lv *LedgerView,
	policyHash []byte,
) error {
	t.Helper()
	return conway.UtxoValidateGuardrailsScriptHash(
		constitutionTestWithdrawalTx(t, policyHash),
		1,
		lv,
		&conway.ConwayProtocolParameters{},
	)
}

// TestLedgerViewConstitutionWithPolicyHash proves the stored anchor and
// guardrails policy hash reach the shared ledger-state contract, and that
// guardrails validation consumes them: a proposal carrying the enacted
// policy hash is accepted and one carrying none is rejected. Before the
// mapping existed the view returned an empty common.Constitution, which
// inverted both verdicts.
func TestLedgerViewConstitutionWithPolicyHash(t *testing.T) {
	t.Parallel()

	lv, db := constitutionTestView(t)
	anchorHash := bytes.Repeat([]byte{0xa1}, lcommon.Blake2b256Size)
	policyHash := bytes.Repeat([]byte{0xa2}, lcommon.Blake2b224Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/with-guardrails",
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  10,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(
		t,
		"https://example.invalid/with-guardrails",
		got.Anchor.Url,
	)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, policyHash))

	err = constitutionTestGuardrails(t, lv, nil)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, policyHash, mismatch.Expected)
}

// TestLedgerViewConstitutionWithoutPolicyHash proves a constitution with no
// guardrails script maps to a nil ScriptHash, so guardrails validation
// accepts a proposal that carries no policy hash and rejects one that does.
func TestLedgerViewConstitutionWithoutPolicyHash(t *testing.T) {
	t.Parallel()

	lv, db := constitutionTestView(t)
	anchorHash := bytes.Repeat([]byte{0xb1}, lcommon.Blake2b256Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/no-guardrails",
		AnchorHash: anchorHash,
		AddedSlot:  10,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(
		t,
		"https://example.invalid/no-guardrails",
		got.Anchor.Url,
	)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, nil))

	err = constitutionTestGuardrails(
		t,
		lv,
		bytes.Repeat([]byte{0xb2}, lcommon.Blake2b224Size),
	)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Nil(t, mismatch.Expected)
}

// TestLedgerViewConstitutionLatestEnactedWins proves the view reports the
// most recently enacted constitution, including one that drops the
// guardrails script a previous constitution carried.
func TestLedgerViewConstitutionLatestEnactedWins(t *testing.T) {
	t.Parallel()

	lv, db := constitutionTestView(t)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/first",
		AnchorHash: bytes.Repeat([]byte{0xc1}, lcommon.Blake2b256Size),
		PolicyHash: bytes.Repeat([]byte{0xc2}, lcommon.Blake2b224Size),
		AddedSlot:  10,
	}, nil))
	secondAnchor := bytes.Repeat([]byte{0xc3}, lcommon.Blake2b256Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/second",
		AnchorHash: secondAnchor,
		AddedSlot:  20,
	}, nil))

	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/second", got.Anchor.Url)
	require.Equal(t, secondAnchor, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)
}

// TestLedgerViewConstitutionMissingFailsClosed proves an unpopulated
// constitution store is reported as unavailable, and that guardrails
// validation therefore rejects the proposal with a ConstitutionLookupError
// instead of treating "no constitution recorded" as "no guardrails script
// required".
func TestLedgerViewConstitutionMissingFailsClosed(t *testing.T) {
	t.Parallel()

	lv, _ := constitutionTestView(t)

	got, err := lv.Constitution()
	require.ErrorIs(t, err, governance.ErrConstitutionUnavailable)
	require.Nil(t, got)

	guardrailsErr := constitutionTestGuardrails(t, lv, nil)
	require.Error(t, guardrailsErr)
	var lookup conway.ConstitutionLookupError
	require.ErrorAs(t, guardrailsErr, &lookup)
	require.ErrorIs(
		t,
		guardrailsErr,
		governance.ErrConstitutionUnavailable,
	)
}

// TestLedgerViewConstitutionUnreadableFailsClosed proves a constitution
// store that cannot be read at all fails closed the same way a missing one
// does: the read error is propagated, never flattened into a valid-looking
// constitution with no guardrails script. The propagated error is the
// wrapped store error and not ErrConstitutionUnavailable, which is reserved
// for state that was read and found missing or malformed.
func TestLedgerViewConstitutionUnreadableFailsClosed(t *testing.T) {
	t.Parallel()

	lv, db := constitutionTestView(t)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/unreadable",
		AnchorHash: bytes.Repeat([]byte{0xd1}, lcommon.Blake2b256Size),
		PolicyHash: bytes.Repeat([]byte{0xd2}, lcommon.Blake2b224Size),
		AddedSlot:  10,
	}, nil))

	// Confirm the row is readable first, so the assertions below cannot
	// pass because the fixture never had a constitution to lose.
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP TABLE constitution")
	require.NoError(t, err)

	got, err = lv.Constitution()
	require.Error(t, err)
	require.Nil(t, got)
	require.NotErrorIs(t, err, governance.ErrConstitutionUnavailable)

	guardrailsErr := constitutionTestGuardrails(t, lv, nil)
	require.Error(t, guardrailsErr)
	var lookup conway.ConstitutionLookupError
	require.ErrorAs(t, guardrailsErr, &lookup)
}

// TestLedgerViewGetDRepVotingPowerIncludesActiveProposalDeposit proves the
// local-state-query GetDRepVotingPower path (LedgerView.GetDRepVotingPower,
// which currently has no wired caller but is exported for a future
// GetDRepState handler) reports the same CIP-1694 deposit-inclusive voting
// power ledger/governance.LoadDRepVotingState uses for real ratification and
// the Blockfrost adapter's DRep reads (blinklabs-io/dingo#4355), not the
// plain UTxO+reward figure GetDRepVotingPower alone returns.
func TestLedgerViewGetDRepVotingPowerIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })

	drepCred := bytes.Repeat([]byte{0x11}, 28)
	drepStakeCred := bytes.Repeat([]byte{0x22}, 28)
	returnStakeCred := bytes.Repeat([]byte{0x33}, 28)

	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: drepStakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x44}, 32),
		OutputIdx:  0,
		StakingKey: drepStakeCred,
		AddedSlot:  1,
		Amount:     100,
	}))

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: returnStakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		returnStakeCred,
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x55}, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    bytes.Repeat([]byte{0x66}, 32),
		AddedSlot:     1,
	}, nil))

	lv := &LedgerView{ls: ls}
	power, err := lv.GetDRepVotingPower(0, drepCred)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), power)
}

// TestLedgerViewSatisfiesEpochState pins that LedgerView provides gouroboros'
// optional EpochState capability.
//
// Rules that are expressed relative to the current epoch degrade to a weaker
// check when the ledger state cannot supply one, and they degrade silently. The
// pool-deposit decision is the case that found this: without an epoch it cannot
// tell a retired pool from a registered one, charges no deposit for a
// registration that needs one, and the transaction then fails value
// conservation by exactly the deposit (issue #3908).
func TestLedgerViewSatisfiesEpochState(t *testing.T) {
	t.Parallel()

	var lv any = &LedgerView{}
	_, ok := lv.(lcommon.EpochState)
	require.True(t, ok,
		"LedgerView must satisfy common.EpochState, or every rule that needs "+
			"the current epoch silently takes its degraded path")
}

// TestLedgerViewEpochForSlot checks the mapping itself against a known cache.
func TestLedgerViewEpochForSlot(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 196, StartSlot: 16_934_400, LengthInSlots: 86_400},
			{EpochId: 197, StartSlot: 17_020_800, LengthInSlots: 86_400},
		},
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	for _, tc := range []struct {
		name string
		slot uint64
		want uint64
	}{
		{"first slot of an epoch", 17_020_800, 197},
		// The slot that wedged the replay, ninety slots into epoch 197.
		{"the slot from issue 3908", 17_020_890, 197},
		{"last slot of the previous epoch", 17_020_799, 196},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.EpochForSlot(tc.slot)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run(
		"a slot outside the cache is an error, not a guess",
		func(t *testing.T) {
			_, err := lv.EpochForSlot(99_000_000)
			require.Error(t, err)
		},
	)
}

func governanceTestView(
	t *testing.T,
	pparams lcommon.ProtocolParameters,
) (*LedgerView, *database.Database) {
	t.Helper()
	db := newTestDB(t)
	ls := &LedgerState{db: db, currentPParams: pparams}
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

func governanceTestHash(seed byte) []byte {
	ret := make([]byte, len(lcommon.Blake2b256{}))
	ret[0] = seed
	return ret
}

func governanceTestID(seed byte, idx uint32) lcommon.GovActionId {
	var txID lcommon.Blake2b256
	copy(txID[:], governanceTestHash(seed))
	return lcommon.GovActionId{TransactionId: txID, GovActionIdx: idx}
}

func storeGovernanceTestProposal(
	t *testing.T,
	db *database.Database,
	proposal *models.GovernanceProposal,
	action lcommon.GovAction,
) {
	t.Helper()
	if proposal.AnchorHash == nil {
		proposal.AnchorHash = make([]byte, 32)
	}
	if proposal.ReturnAddress == nil {
		proposal.ReturnAddress = make([]byte, 29)
	}
	if action != nil {
		encoded, err := cbor.Encode(action)
		require.NoError(t, err)
		proposal.GovActionCbor = encoded
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
}

func hardForkGovernanceTestAction(
	t *testing.T,
	ancestor *lcommon.GovActionId,
	major uint,
	minor uint,
) *lcommon.HardForkInitiationGovAction {
	t.Helper()
	action, err := lcommon.NewHardForkInitiationGovAction(
		ancestor,
		major,
		minor,
	)
	require.NoError(t, err)
	return action
}

func governanceProposalTestTx(
	t *testing.T,
	action lcommon.GovAction,
) *conway.ConwayTransaction {
	t.Helper()
	wrapper, err := conway.NewConwayGovAction(action)
	require.NoError(t, err)
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPGovAction: wrapper,
			}},
		},
	}
}

func governanceVoteTestTx(
	id lcommon.GovActionId,
	voterType uint8,
) *conway.ConwayTransaction {
	voter := lcommon.Voter{Type: voterType}
	actionID := id
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{
				&voter: {
					&actionID: lcommon.VotingProcedure{
						Vote: lcommon.GovVoteYes,
					},
				},
			},
		},
	}
}

func TestLedgerViewGovPurposeRoots(t *testing.T) {
	t.Parallel()

	lv, db := governanceTestView(t, &conway.ConwayProtocolParameters{})

	// A non-nil empty set is authoritative. Returning nil would make
	// gouroboros silently fall back to the weaker existence-only rule.
	roots, err := lv.GovPurposeRoots()
	require.NoError(t, err)
	require.NotNil(t, roots)
	require.Nil(t, roots.PParamUpdate)
	require.Nil(t, roots.HardFork)
	require.Nil(t, roots.Committee)
	require.Nil(t, roots.Constitution)

	enactedEpoch := uint64(20)
	storeRoot := func(
		seed byte,
		actionType lcommon.GovActionType,
		actionIndex uint32,
		enactedSlot uint64,
	) {
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       governanceTestHash(seed),
			ActionIndex:  actionIndex,
			ActionType:   uint8(actionType),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &enactedSlot,
		}, nil)
	}
	storeRoot(0x11, lcommon.GovActionTypeParameterChange, 1, 100)
	storeRoot(0x12, lcommon.GovActionTypeHardForkInitiation, 2, 101)
	storeRoot(0x13, lcommon.GovActionTypeNoConfidence, 3, 102)
	storeRoot(0x14, lcommon.GovActionTypeUpdateCommittee, 4, 103)
	storeRoot(0x15, lcommon.GovActionTypeNewConstitution, 5, 104)

	roots, err = lv.GovPurposeRoots()
	require.NoError(t, err)
	require.NotNil(t, roots.PParamUpdate)
	require.NotNil(t, roots.HardFork)
	require.NotNil(t, roots.Committee)
	require.NotNil(t, roots.Constitution)
	require.Equal(t, governanceTestID(0x11, 1), *roots.PParamUpdate)
	require.Equal(t, governanceTestID(0x12, 2), *roots.HardFork)
	// NoConfidence and UpdateCommittee share a purpose; the latest enacted
	// member of the pair is the single committee root.
	require.Equal(t, governanceTestID(0x14, 4), *roots.Committee)
	require.Equal(t, governanceTestID(0x15, 5), *roots.Constitution)
}

func TestLedgerViewGovPurposeRootsPropagatesDatabaseError(t *testing.T) {
	t.Parallel()

	lv, db := governanceTestView(t, &conway.ConwayProtocolParameters{})
	require.NoError(t, dbtest.CloseDatabase(db))

	roots, err := lv.GovPurposeRoots()
	require.Error(t, err)
	require.Nil(t, roots)
}

func TestLedgerViewGovernanceActionExpiryIsInclusive(t *testing.T) {
	t.Parallel()

	pparams := &conway.ConwayProtocolParameters{}
	lv, db := governanceTestView(t, pparams)
	require.NoError(t, db.SetEpoch(
		1_000, 10, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	id := governanceTestID(0x21, 0)
	action := hardForkGovernanceTestAction(t, nil, 10, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        id.TransactionId[:],
		ActionIndex:   id.GovActionIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 10,
		ExpiresEpoch:  12,
	}, action)

	state, err := lv.GovActionById(id)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.True(t, lv.GovActionExists(id))
	// ExpiresEpoch is inclusive: epoch 12 ends at slot 1299.
	require.Equal(t, uint64(1_299), state.ExpirySlot)
	require.IsType(t, &lcommon.HardForkInitiationGovAction{}, state.Action)

	vote := governanceVoteTestTx(id, lcommon.VoterTypeDRepKeyHash)
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		vote, 1_299, lv, pparams,
	))
	err = conway.UtxoValidateVotingOnExpiredGovAction(
		vote, 1_300, lv, pparams,
	)
	var expiryErr conway.VotingOnExpiredGovActionError
	require.ErrorAs(t, err, &expiryErr)
}

func TestLedgerViewGovernanceProposalAncestry(t *testing.T) {
	t.Parallel()

	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionConway,
		},
	}
	lv, db := governanceTestView(t, pparams)
	require.NoError(t, db.SetEpoch(
		1_000, 10, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	enactedEpoch := uint64(10)
	olderEnactedEpoch := uint64(9)
	rootID := governanceTestID(0x31, 0)
	oldRootID := governanceTestID(0x32, 0)
	pendingID := governanceTestID(0x33, 0)
	expiredID := governanceTestID(0x34, 0)
	for _, test := range []struct {
		id      lcommon.GovActionId
		expires uint64
		enacted *uint64
		slot    uint64
	}{
		{rootID, 12, &enactedEpoch, 200},
		{oldRootID, 12, &olderEnactedEpoch, 100},
		{pendingID, 12, nil, 0},
		{expiredID, 10, nil, 0},
	} {
		var enactedSlot *uint64
		if test.enacted != nil {
			enactedSlot = &test.slot
		}
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:        test.id.TransactionId[:],
			ActionIndex:   test.id.GovActionIdx,
			ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
			ProposedEpoch: 10,
			ExpiresEpoch:  test.expires,
			EnactedEpoch:  test.enacted,
			EnactedSlot:   enactedSlot,
		}, hardForkGovernanceTestAction(t, nil, 10, 0))
	}

	for _, id := range []lcommon.GovActionId{rootID, pendingID, expiredID} {
		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &id, 10, 1),
		)
		require.NoError(t, conway.UtxoValidateProposalAncestry(
			tx, 1_250, lv, pparams,
		))
	}
	for _, id := range []lcommon.GovActionId{oldRootID} {
		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &id, 10, 1),
		)
		err := conway.UtxoValidateProposalAncestry(
			tx, 1_250, lv, pparams,
		)
		var ancestryErr conway.InvalidGovActionAncestorError
		require.ErrorAs(t, err, &ancestryErr)
	}
	// Expiry is applied by EPOCH. Until then, the proposal remains in its
	// purpose tree and may still be named as an ancestor.
	tx := governanceProposalTestTx(
		t,
		hardForkGovernanceTestAction(t, &expiredID, 10, 1),
	)
	require.NoError(t, conway.UtxoValidateProposalAncestry(
		tx, 1_250, lv, pparams,
	))
}

func TestLedgerViewGovernanceActionContentDrivesRules(t *testing.T) {
	t.Parallel()

	t.Run("hard fork succession", func(t *testing.T) {
		pparams := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
		}
		lv, db := governanceTestView(t, pparams)
		require.NoError(t, db.SetEpoch(
			0, 0, nil, nil, nil, nil, 0, 1, 1_000, nil,
		))
		ancestorID := governanceTestID(0x41, 0)
		oldRootID := governanceTestID(0x40, 0)
		enactedEpoch := uint64(0)
		oldRootSlot := uint64(1)
		rootSlot := uint64(2)
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       oldRootID.TransactionId[:],
			ActionType:   uint8(lcommon.GovActionTypeHardForkInitiation),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &oldRootSlot,
		}, hardForkGovernanceTestAction(t, nil, 9, 0))
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       ancestorID.TransactionId[:],
			ActionType:   uint8(lcommon.GovActionTypeHardForkInitiation),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &rootSlot,
		}, hardForkGovernanceTestAction(t, nil, 10, 0))
		oldRoot, err := lv.GovActionById(oldRootID)
		require.NoError(t, err)
		require.Nil(t, oldRoot)
		root, err := lv.GovActionById(ancestorID)
		require.NoError(t, err)
		require.IsType(t, &lcommon.HardForkInitiationGovAction{}, root.Action)
		require.False(t, lv.GovActionExists(ancestorID))
		rootVote := governanceVoteTestTx(
			ancestorID,
			lcommon.VoterTypeDRepKeyHash,
		)
		err = conway.UtxoValidateUnknownGovActionIds(
			rootVote,
			1,
			lv,
			pparams,
		)
		var unknownActionErr conway.UnknownGovActionIdError
		require.ErrorAs(t, err, &unknownActionErr)

		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &ancestorID, 10, 1),
		)
		require.NoError(t, conway.UtxoValidateHardForkCanFollow(
			tx, 1, lv, pparams,
		))

		tx = governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &ancestorID, 10, 2),
		)
		err = conway.UtxoValidateHardForkCanFollow(tx, 1, lv, pparams)
		var hardForkErr conway.BadHardForkProtocolVersionError
		require.ErrorAs(t, err, &hardForkErr)
	})

	t.Run("security group voting", func(t *testing.T) {
		pparams := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionPlomin,
			},
		}
		lv, db := governanceTestView(t, pparams)
		require.NoError(t, db.SetEpoch(
			0, 0, nil, nil, nil, nil, 0, 1, 1_000, nil,
		))
		maxBlockBodySize := uint(90_112)
		keyDeposit := uint(2_000_000)
		for _, test := range []struct {
			id     lcommon.GovActionId
			update conway.ConwayProtocolParameterUpdate
			valid  bool
		}{
			{governanceTestID(0x42, 0), conway.ConwayProtocolParameterUpdate{
				MaxBlockBodySize: &maxBlockBodySize,
			}, true},
			{governanceTestID(0x43, 0), conway.ConwayProtocolParameterUpdate{
				KeyDeposit: &keyDeposit,
			}, false},
		} {
			action, err := conway.NewConwayParameterChangeGovAction(
				nil, test.update, nil,
			)
			require.NoError(t, err)
			storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
				TxHash:       test.id.TransactionId[:],
				ActionType:   uint8(lcommon.GovActionTypeParameterChange),
				ExpiresEpoch: 1,
			}, action)
			err = conway.UtxoValidateStakePoolVotingRestrictions(
				governanceVoteTestTx(
					test.id,
					lcommon.VoterTypeStakingPoolKeyHash,
				),
				1,
				lv,
				pparams,
			)
			if test.valid {
				require.NoError(t, err)
			} else {
				var restrictionErr conway.StakePoolVotingRestrictionError
				require.ErrorAs(t, err, &restrictionErr)
			}
		}
	})
}

func TestLedgerViewDecodesHistoricalParameterActionForCurrentEra(t *testing.T) {
	t.Parallel()

	maxBlockBodySize := uint(90_112)
	action, err := conway.NewConwayParameterChangeGovAction(
		nil,
		conway.ConwayProtocolParameterUpdate{
			MaxBlockBodySize: &maxBlockBodySize,
		},
		nil,
	)
	require.NoError(t, err)

	for _, test := range []struct {
		name    string
		pparams lcommon.ProtocolParameters
		want    any
	}{
		{
			name:    "Conway",
			pparams: &conway.ConwayProtocolParameters{},
			want:    &conway.ConwayParameterChangeGovAction{},
		},
		{
			name: "Dijkstra",
			pparams: &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{},
			},
			want: &gdijkstra.DijkstraParameterChangeGovAction{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lv, db := governanceTestView(t, test.pparams)
			require.NoError(t, db.SetEpoch(
				0, 0, nil, nil, nil, nil, 0, 1, 100, nil,
			))
			id := governanceTestID(0x51, 0)
			storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
				TxHash:       id.TransactionId[:],
				ActionType:   uint8(lcommon.GovActionTypeParameterChange),
				ExpiresEpoch: 1,
			}, action)
			state, err := lv.GovActionById(id)
			require.NoError(t, err)
			require.IsType(t, test.want, state.Action)
		})
	}
}

// mirQuorumTestView returns a LedgerView whose Shelley genesis delegates
// three genesis keys to the given delegate keys with an update quorum of two.
func mirQuorumTestView(
	t *testing.T,
	delegates [3]ed25519.PublicKey,
) *LedgerView {
	t.Helper()
	var genDelegs []string
	for i, delegate := range delegates {
		genDelegs = append(genDelegs, `"`+
			strings.Repeat(hex.EncodeToString([]byte{byte(0x11 * (i + 1))}), 28)+
			`": {"delegate": "`+
			hex.EncodeToString(lcommon.Blake2b224Hash(delegate).Bytes())+
			`", "vrf": "`+strings.Repeat("bb", 32)+`"}`)
	}
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"updateQuorum": 2,
		"systemStart": "2022-10-25T00:00:00Z",
		"protocolParams": {"decentralisationParam": 1},
		"genDelegs": {` + strings.Join(genDelegs, ",") + `}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(
		t,
		cfg,
		strings.NewReader(
			`{"blockVersionData":{"slotDuration":"20000"},"protocolConsts":{"k":432}}`,
		),
	))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(
		strings.NewReader(shelleyGenesisJSON),
	))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db:             db,
		currentPParams: &shelley.ShelleyProtocolParameters{},
		config:         LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	ls.publishSnapshotsLocked()
	return ls.NewView(nil)
}

// TestMIRGenesisQuorumThroughEraValidation drives the genesis-delegate quorum
// for move instantaneous rewards certificates through every era's validation
// entry point that admits MIR, against a real LedgerView.
//
// Reference: Shelley UTXOW validateMIRInsufficientGenesisSigs, inherited
// unchanged through Babbage: genSig is the set of current genesis delegate key
// hashes intersected with the transaction's witness key hashes, and a
// transaction carrying a MIR certificate needs |genSig| >= Quorum. A signer
// that is not a genesis delegate contributes nothing, and a delegate that
// signs twice counts once, because genSig is a set.
func TestMIRGenesisQuorumThroughEraValidation(t *testing.T) {
	t.Parallel()

	var keys [4]ed25519.PrivateKey
	for i := range keys {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(0xa0 + i)
		keys[i] = ed25519.NewKeyFromSeed(seed)
	}
	pub := func(i int) ed25519.PublicKey {
		return keys[i].Public().(ed25519.PublicKey)
	}
	lv := mirQuorumTestView(t, [3]ed25519.PublicKey{pub(0), pub(1), pub(2)})
	delegates, err := lv.GenesisDelegateKeyHashes(0)
	require.NoError(t, err)
	require.Len(t, delegates, 3, "all three genesis delegations are active")
	quorum, err := lv.GenesisUpdateQuorum()
	require.NoError(t, err)
	require.Equal(t, uint(2), quorum)

	reward := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(make([]byte, 28)),
	}
	mir := &lcommon.MoveInstantaneousRewardsCertificate{
		CertType: uint(lcommon.CertificateTypeMoveInstantaneousRewards),
		Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
			Source:  1,
			Rewards: map[*lcommon.Credential]*big.Int{&reward: big.NewInt(1)},
		},
	}
	certs := []lcommon.CertificateWrapper{
		{Type: uint(lcommon.CertificateTypeMoveInstantaneousRewards), Certificate: mir},
	}

	type buildTx func([]lcommon.VkeyWitness) lcommon.Transaction
	eraCases := []struct {
		name     string
		build    buildTx
		pparams  lcommon.ProtocolParameters
		validate func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error
		// quorum is the era's MIR genesis-quorum rule on its own. The full
		// validate path also fails on the fixture's missing inputs, so only
		// this rule can show that a met quorum is accepted.
		quorum func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error
	}{
		{
			name: "shelley",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &shelley.ShelleyTransaction{
					Body: shelley.ShelleyTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &shelley.ShelleyProtocolParameters{},
			validate: eras.ValidateTxShelley,
			quorum:   shelley.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "allegra",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &allegra.AllegraTransaction{
					Body: allegra.AllegraTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &allegra.AllegraProtocolParameters{},
			validate: eras.ValidateTxAllegra,
			quorum:   allegra.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "mary",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &mary.MaryTransaction{
					Body: mary.MaryTransactionBody{TxCertificates: certs},
					WitnessSet: shelley.ShelleyTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &mary.MaryProtocolParameters{},
			validate: eras.ValidateTxMary,
			quorum:   mary.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "alonzo",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &alonzo.AlonzoTransaction{
					TxIsValid: true,
					Body:      alonzo.AlonzoTransactionBody{TxCertificates: certs},
					WitnessSet: alonzo.AlonzoTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &alonzo.AlonzoProtocolParameters{},
			validate: eras.ValidateTxAlonzo,
			quorum:   alonzo.UtxoValidateMIRGenesisQuorum,
		},
		{
			name: "babbage",
			build: func(w []lcommon.VkeyWitness) lcommon.Transaction {
				return &babbage.BabbageTransaction{
					TxIsValid: true,
					Body:      babbage.BabbageTransactionBody{TxCertificates: certs},
					WitnessSet: babbage.BabbageTransactionWitnessSet{
						VkeyWitnesses: w,
					},
				}
			},
			pparams:  &babbage.BabbageProtocolParameters{},
			validate: eras.ValidateTxBabbage,
			quorum:   babbage.UtxoValidateMIRGenesisQuorum,
		},
	}
	signerCases := []struct {
		name         string
		signers      []int
		wantProvided uint
		wantRejected bool
	}{
		{"one delegate is below quorum", []int{0}, 1, true},
		{"a delegate signing twice counts once", []int{0, 0}, 1, true},
		{"a non-delegate signer does not count", []int{0, 3}, 1, true},
		{"two delegates meet quorum", []int{0, 1}, 0, false},
		{"three delegates meet quorum", []int{0, 1, 2}, 0, false},
	}
	for _, era := range eraCases {
		for _, sc := range signerCases {
			t.Run(era.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()
				witnesses := make([]lcommon.VkeyWitness, 0, len(sc.signers))
				for _, i := range sc.signers {
					witnesses = append(witnesses, lcommon.VkeyWitness{
						Vkey:      pub(i),
						Signature: make([]byte, ed25519.SignatureSize),
					})
				}
				tx := era.build(witnesses)
				err := era.validate(tx, 0, lv, era.pparams)
				var insufficient lcommon.MIRInsufficientGenesisSigsError
				if !sc.wantRejected {
					require.False(
						t,
						errors.As(err, &insufficient),
						"quorum met but MIR rejected for genesis signatures: %v",
						err,
					)
					require.NoError(t, era.quorum(tx, 0, lv, era.pparams))
					return
				}
				require.True(
					t,
					errors.As(err, &insufficient),
					"MIR below quorum was not rejected for genesis signatures: %v",
					err,
				)
				require.Equal(t, sc.wantProvided, insufficient.Provided)
				require.Equal(t, uint(2), insufficient.Required)
			})
		}
	}

	// A genesis key delegation certificate applied after genesis moves that
	// genesis key's share of the quorum to the new delegate: its signature
	// counts and the replaced delegate's no longer does.
	const redelegatedSlot = 100_000
	redelegated := mirQuorumTestView(
		t,
		[3]ed25519.PublicKey{pub(0), pub(1), pub(2)},
	)
	seedGenesisDelegation(t, redelegated.ls.db, models.GenesisDelegation{
		GenesisHash:         bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size),
		GenesisDelegateHash: lcommon.Blake2b224Hash(pub(3)).Bytes(),
		VrfKeyHash:          bytes.Repeat([]byte{0xcc}, lcommon.Blake2b256Size),
	})
	delegates, err = redelegated.GenesisDelegateKeyHashes(redelegatedSlot)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b224{
			lcommon.Blake2b224Hash(pub(1)),
			lcommon.Blake2b224Hash(pub(2)),
			lcommon.Blake2b224Hash(pub(3)),
		},
		delegates,
	)
	for _, era := range eraCases {
		for _, sc := range []struct {
			name         string
			signers      []int
			wantRejected bool
		}{
			{"new delegate counts", []int{3, 1}, false},
			{"replaced delegate does not count", []int{0, 1}, true},
		} {
			t.Run(era.name+"/redelegated/"+sc.name, func(t *testing.T) {
				t.Parallel()
				witnesses := make([]lcommon.VkeyWitness, 0, len(sc.signers))
				for _, i := range sc.signers {
					witnesses = append(witnesses, lcommon.VkeyWitness{
						Vkey:      pub(i),
						Signature: make([]byte, ed25519.SignatureSize),
					})
				}
				tx := era.build(witnesses)
				err := era.validate(
					tx,
					redelegatedSlot,
					redelegated,
					era.pparams,
				)
				var insufficient lcommon.MIRInsufficientGenesisSigsError
				require.Equal(
					t,
					sc.wantRejected,
					errors.As(err, &insufficient),
					"genesis quorum verdict after redelegation: %v",
					err,
				)
				quorumErr := era.quorum(
					tx,
					redelegatedSlot,
					redelegated,
					era.pparams,
				)
				if sc.wantRejected {
					require.ErrorAs(t, quorumErr, &insufficient)
					return
				}
				require.NoError(t, quorumErr)
			})
		}
	}
}

// errInjectingMetadataStore wraps a real metadata.MetadataStore and forces
// specific lookups to fail with a caller-supplied error instead of
// delegating to the wrapped store, reproducing a genuine non-not-found
// storage fault (a timeout, a lost connection) without corrupting on-disk
// rows. See blinklabs-io/dingo#1649.
type errInjectingMetadataStore struct {
	metadata.MetadataStore
	getPoolErr                error
	getAccountByCredentialErr error
	getGovernanceProposalErr  error
	getCommitteeMembersErr    error
}

func (s *errInjectingMetadataStore) GetCommitteeMembersIncludeDeleted(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	if s.getCommitteeMembersErr != nil {
		return nil, s.getCommitteeMembersErr
	}
	return s.MetadataStore.GetCommitteeMembersIncludeDeleted(txn)
}

func (s *errInjectingMetadataStore) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn types.Txn,
) (*models.Pool, error) {
	if s.getPoolErr != nil {
		return nil, s.getPoolErr
	}
	return s.MetadataStore.GetPool(pkh, includeInactive, txn)
}

func (s *errInjectingMetadataStore) GetAccountByCredential(
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	if s.getAccountByCredentialErr != nil {
		return nil, s.getAccountByCredentialErr
	}
	return s.MetadataStore.GetAccountByCredential(
		credentialTag,
		stakeKey,
		includeInactive,
		txn,
	)
}

func (s *errInjectingMetadataStore) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	if s.getGovernanceProposalErr != nil {
		return nil, s.getGovernanceProposalErr
	}
	return s.MetadataStore.GetGovernanceProposal(txHash, actionIndex, txn)
}

// newDiscardLoggerLedgerState builds a bare *LedgerState wired only to db,
// with a non-nil discard logger so the boolean predicates' error-path
// logging (lv.ls.config.Logger.Error) does not panic on a zero-value
// LedgerStateConfig.
func newDiscardLoggerLedgerState(db *database.Database) *LedgerState {
	ls := &LedgerState{
		db:     db,
		config: LedgerStateConfig{Logger: slog.New(slog.DiscardHandler)},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls
}

// newStorageFaultTestDB builds a real dingo database (the same badger blob
// and sqlite metadata composition dbtest.NewDatabase uses) with its metadata
// store wrapped by errs, so a specific lookup returns errs's error instead of
// the real store's result.
func newStorageFaultTestDB(
	t *testing.T,
	errs errInjectingMetadataStore,
) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
		func(store metadata.MetadataStore) metadata.MetadataStore {
			errs.MetadataStore = store
			return &errs
		},
	)
	require.NoError(t, err)
	return db
}

// TestLedgerViewPredicatesRecordStorageFaultNotRuleVerdict is the core
// regression test: each boolean LedgerState predicate must record a genuine
// non-not-found storage error on the view instead of only swallowing it into
// a bare false/does-not-exist return with no trace of the fault.
func TestLedgerViewPredicatesRecordStorageFaultNotRuleVerdict(t *testing.T) {
	t.Parallel()

	t.Run("IsStakeCredentialRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic account lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getAccountByCredentialErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}
		cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x01}}

		registered := lv.IsStakeCredentialRegistered(cred)

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run(
		"IsStakeCredentialRegistered not-found is not a fault",
		func(t *testing.T) {
			t.Parallel()
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
			ls := newDiscardLoggerLedgerState(db)
			lv := &LedgerView{ls: ls}
			cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x01}}

			registered := lv.IsStakeCredentialRegistered(cred)

			require.False(t, registered)
			require.NoError(t, lv.StorageErr())
		},
	)

	t.Run("IsRewardAccountRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic account lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getAccountByCredentialErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}
		cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x02}}

		registered := lv.IsRewardAccountRegistered(cred)

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run(
		"IsRewardAccountRegistered not-found is not a fault",
		func(t *testing.T) {
			t.Parallel()
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
			ls := newDiscardLoggerLedgerState(db)
			lv := &LedgerView{ls: ls}
			cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x02}}

			registered := lv.IsRewardAccountRegistered(cred)

			require.False(t, registered)
			require.NoError(t, lv.StorageErr())
		},
	)

	t.Run("IsPoolRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic pool lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getPoolErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		registered := lv.IsPoolRegistered(lcommon.PoolKeyHash{0x03})

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run("IsPoolRegistered not-found is not a fault", func(t *testing.T) {
		t.Parallel()
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		registered := lv.IsPoolRegistered(lcommon.PoolKeyHash{0x03})

		require.False(t, registered)
		require.NoError(t, lv.StorageErr())
	})

	t.Run("GovActionExists", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic governance proposal lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getGovernanceProposalErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		exists := lv.GovActionExists(lcommon.GovActionId{})

		require.False(t, exists)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run("GovActionExists not-found is not a fault", func(t *testing.T) {
		t.Parallel()
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		exists := lv.GovActionExists(lcommon.GovActionId{})

		require.False(t, exists)
		require.NoError(t, lv.StorageErr())
	})
}

// TestStorageFaultOrErrPrefersRecordedFault pins storageFaultOrErr, the
// helper every ValidateTxFunc/EvaluateTxFunc call site uses after invoking
// the era's rule: a recorded fault always wins, including over a nil rule
// verdict (the case where the false negative caused the rule to wrongly
// accept, e.g. "a stake re-registration today yields nil").
func TestStorageFaultOrErrPrefersRecordedFault(t *testing.T) {
	t.Parallel()

	t.Run("no fault recorded, rule error passes through", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		ruleErr := errors.New("rule failure")
		require.Same(t, ruleErr, storageFaultOrErr(lv, ruleErr))
	})

	t.Run(
		"no fault recorded, nil rule verdict passes through",
		func(t *testing.T) {
			t.Parallel()
			lv := &LedgerView{}
			require.NoError(t, storageFaultOrErr(lv, nil))
		},
	)

	t.Run("fault recorded overrides nil rule verdict", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		faultErr := errors.New("storage fault")
		lv.recordStorageErr(faultErr)
		err := storageFaultOrErr(lv, nil)
		require.ErrorIs(t, err, ErrLedgerViewStorageFault)
		require.ErrorIs(t, err, faultErr)
	})

	t.Run("fault recorded overrides rule error", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		faultErr := errors.New("storage fault")
		lv.recordStorageErr(faultErr)
		ruleErr := errors.New("rule failure")
		err := storageFaultOrErr(lv, ruleErr)
		require.ErrorIs(t, err, ErrLedgerViewStorageFault)
		require.ErrorIs(t, err, faultErr)
		require.NotErrorIs(t, err, ruleErr)
	})

	t.Run("only the first fault is sticky", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		firstErr := errors.New("first fault")
		secondErr := errors.New("second fault")
		lv.recordStorageErr(firstErr)
		lv.recordStorageErr(secondErr)
		err := storageFaultOrErr(lv, nil)
		require.ErrorIs(t, err, firstErr)
		require.NotErrorIs(t, err, secondErr)
	})

	t.Run("nil view is a no-op", func(t *testing.T) {
		t.Parallel()
		ruleErr := errors.New("rule failure")
		require.Same(t, ruleErr, storageFaultOrErr(nil, ruleErr))
		require.NoError(t, storageFaultOrErr(nil, nil))
	})
}

// newFakeEraLedgerState builds a minimal LedgerState wired to a single,
// caller-supplied era descriptor so a test can drive the real ValidateTx/
// EvaluateTx call sites without needing a transaction that satisfies every
// gouroboros ledger rule. It mirrors view_governance_test.go's
// governanceTestView, adding the era wiring ValidateTx/EvaluateTx need.
func newFakeEraLedgerState(
	db *database.Database,
	validateTxFunc func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error,
	evaluateTxFunc func(
		lcommon.Transaction,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error),
) *LedgerState {
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:            slog.New(slog.DiscardHandler),
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
		},
		currentEra: eras.EraDesc{
			Id:             conway.EraIdConway,
			ValidateTxFunc: validateTxFunc,
			EvaluateTxFunc: evaluateTxFunc,
		},
		currentPParams: &conway.ConwayProtocolParameters{},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	return ls
}

// TestValidateTxSurfacesStorageFaultOverNilVerdict drives the real
// LedgerState.ValidateTx call site (validateTxCore) with a rule that mimics
// Conway's certificate-deposit rule: IsStakeCredentialRegistered only
// changes whether a deposit is charged, so the rule accepts (returns nil)
// whether or not the credential looks registered. Without the fault check, a
// swallowed storage error is therefore invisible: ValidateTx returns nil.
// After the fix, it returns the storage fault.
func TestValidateTxSurfacesStorageFaultOverNilVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic account lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getAccountByCredentialErr: stubErr,
	})
	cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x04}}
	ls := newFakeEraLedgerState(db, func(
		tx lcommon.Transaction,
		slot uint64,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) error {
		_ = view.IsStakeCredentialRegistered(cred)
		return nil
	}, nil)

	tx := &conway.ConwayTransaction{TxIsValid: true}
	err := ls.ValidateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestValidateTxSurfacesStorageFaultOverPoolRetirementVerdict drives
// LedgerState.ValidateTx with gouroboros's real
// shelley.UtxoValidatePoolCertificates rule validating a pool-retirement
// certificate for a pool the metadata store cannot resolve. Before the fix
// this returns shelley.StakePoolNotRegisteredOnKeyError, indistinguishable
// from a genuinely unregistered pool. After the fix it returns the storage
// fault, and the rule's own verdict is not observable.
func TestValidateTxSurfacesStorageFaultOverPoolRetirementVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic pool lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getPoolErr: stubErr,
	})
	poolKeyHash := lcommon.PoolKeyHash{0x05}
	ls := newFakeEraLedgerState(
		db,
		shelley.UtxoValidatePoolCertificates,
		nil,
	)

	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(lcommon.CertificateTypePoolRetirement),
					Certificate: &lcommon.PoolRetirementCertificate{
						CertType: uint(
							lcommon.CertificateTypePoolRetirement,
						),
						PoolKeyHash: poolKeyHash,
						Epoch:       500,
					},
				},
			},
		},
	}

	err := ls.ValidateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
	var ruleErr shelley.StakePoolNotRegisteredOnKeyError
	require.False(
		t,
		errors.As(err, &ruleErr),
		"the rule's own not-registered verdict must not surface once a storage fault is recorded, got %v",
		err,
	)
}

// TestEvaluateTxSurfacesStorageFaultOverNilVerdict drives the
// LedgerState.EvaluateTx call site (EvaluateTxFunc) the same way
// TestValidateTxSurfacesStorageFaultOverNilVerdict drives ValidateTx.
func TestEvaluateTxSurfacesStorageFaultOverNilVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic pool lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getPoolErr: stubErr,
	})
	poolKeyHash := lcommon.PoolKeyHash{0x06}
	ls := newFakeEraLedgerState(db, nil, func(
		tx lcommon.Transaction,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
		_ = view.IsPoolRegistered(poolKeyHash)
		return 0, lcommon.ExUnits{}, nil, nil
	})

	tx := &conway.ConwayTransaction{TxIsValid: true}
	_, _, _, err := ls.EvaluateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestWithTxValidationSessionSurfacesStorageFaultOverNilVerdict drives the
// WithTxValidationSession call site the same way
// TestValidateTxSurfacesStorageFaultOverNilVerdict drives ValidateTx.
func TestWithTxValidationSessionSurfacesStorageFaultOverNilVerdict(
	t *testing.T,
) {
	t.Parallel()
	stubErr := errors.New("synthetic account lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getAccountByCredentialErr: stubErr,
	})
	cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x07}}
	ls := newFakeEraLedgerState(db, func(
		tx lcommon.Transaction,
		slot uint64,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) error {
		_ = view.IsStakeCredentialRegistered(cred)
		return nil
	}, nil)

	tx := &conway.ConwayTransaction{TxIsValid: true}
	err := ls.WithTxValidationSession(func(
		validate func(
			tx lcommon.Transaction,
			consumedUtxos map[utxoref.Key]struct{},
			createdUtxos map[utxoref.Key]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error {
		return validate(tx, nil, nil)
	})

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestLedgerProcessBlockSurfacesStorageFaultOverRuleVerdict drives the
// chain-sync block-application call site in ledgerProcessBlock. A storage
// fault recorded by a LedgerView predicate must surface as
// ErrLedgerViewStorageFault, not as the rule's verdict: a rejecting rule
// would otherwise classify a canonical block as invalid (txValidationError),
// and an accepting rule would otherwise apply the block on a false negative.
func TestLedgerProcessBlockSurfacesStorageFaultOverRuleVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rejectOnMiss bool
	}{
		{name: "rule rejects on the false negative", rejectOnMiss: true},
		{name: "rule accepts despite the false negative", rejectOnMiss: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stubErr := errors.New("synthetic pool lookup fault")
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{
				getPoolErr: stubErr,
			})
			verdictErr := errors.New("pool not registered verdict")
			called := false
			testEra := eras.ByronEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				view lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				called = true
				if !view.IsPoolRegistered(lcommon.PoolKeyHash{0x08}) &&
					tt.rejectOnMiss {
					return verdictErr
				}
				return nil
			}
			ls := &LedgerState{
				db:         db,
				activeEras: []eras.EraDesc{testEra},
				config: LedgerStateConfig{
					Logger: slog.New(slog.DiscardHandler),
				},
				currentEra: testEra,
			}
			tx := omockledger.NewTransactionBuilder()
			tx.WithId(bytes.Repeat([]byte{0x36}, 32))
			tx.WithType(byron.TxTypeByron)
			tx.WithValid(true)
			block := &validityOutcomeTestBlock{
				header: &byron.ByronMainBlockHeader{},
				txs:    []lcommon.Transaction{tx},
			}

			err := db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(
					txn,
					ocommon.NewPoint(1, block.Hash().Bytes()),
					block,
					true,
					false,
					false,
					nil,
					envelopeParent{origin: true},
					nil,
					testEra,
					nil,
					nil,
					0,
					0,
					false,
				)
				return err
			})

			require.True(t, called, "block validation must run the rule")
			require.ErrorIs(t, err, stubErr)
			require.ErrorIs(t, err, ErrLedgerViewStorageFault)
			require.NotErrorIs(t, err, verdictErr)
			var validationErr *txValidationError
			require.False(
				t,
				errors.As(err, &validationErr),
				"a storage fault must not classify the block as invalid, got %v",
				err,
			)
		})
	}
}

func newTreasuryViewTestLedger(
	t *testing.T,
	dataDir string,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	return &LedgerState{db: db}, db
}

func requireTreasuryValue(
	t *testing.T,
	ls *LedgerState,
	txn *database.Txn,
	want uint64,
) {
	t.Helper()
	got, err := ls.NewView(txn).TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestLedgerViewTreasuryValueReadsValidationTransaction(t *testing.T) {
	t.Parallel()

	ls, db := newTreasuryViewTestLedger(t, "")
	require.NoError(t, db.Metadata().SetNetworkState(100, 900, 10, nil))
	requireTreasuryValue(t, ls, nil, 100)

	rollbackErr := errors.New("test rollback")
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		require.NoError(t, db.Metadata().SetNetworkState(
			250,
			750,
			20,
			txn.Metadata(),
		))
		requireTreasuryValue(t, ls, txn, 250)
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)
	requireTreasuryValue(t, ls, nil, 100)
}

func TestLedgerViewTreasuryValueTracksChainRollback(t *testing.T) {
	t.Parallel()

	ls, db := newTreasuryViewTestLedger(t, "")
	require.NoError(t, db.Metadata().SetNetworkState(100, 900, 10, nil))
	require.NoError(t, db.Metadata().SetNetworkState(60, 900, 20, nil))
	requireTreasuryValue(t, ls, nil, 60)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.DeleteNetworkStateAfterSlot(10, txn); err != nil {
			return err
		}
		value, err := ls.NewView(txn).TreasuryValue()
		if err != nil {
			return err
		}
		if value != 100 {
			return errors.New("rollback transaction exposed the wrong treasury")
		}
		return nil
	}))
	requireTreasuryValue(t, ls, nil, 100)
}

func TestLedgerViewTreasuryValueSurvivesRestart(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ls, db := newTreasuryViewTestLedger(t, dataDir)
	require.NoError(t, db.Metadata().SetNetworkState(777, 223, 42, nil))
	requireTreasuryValue(t, ls, nil, 777)
	require.NoError(t, dbtest.CloseDatabase(db))

	restarted, reopened := newTreasuryViewTestLedger(t, dataDir)
	requireTreasuryValue(t, restarted, nil, 777)
	require.NoError(t, dbtest.CloseDatabase(reopened))
}

func TestLedgerViewTreasuryValueReadsMithrilBootstrapState(t *testing.T) {
	t.Parallel()

	ls, db := newTreasuryViewTestLedger(t, "")
	paramsData, err := cbor.Encode(mithrilRewardConwayPParams())
	require.NoError(t, err)
	nonce := make([]byte, 32)
	const treasury = uint64(87_920_693_660_807)
	require.NoError(t, ledgerstate.ImportLedgerState(
		context.Background(),
		ledgerstate.ImportConfig{
			Database: db,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			State: &ledgerstate.RawLedgerState{
				PParamsData:     paramsData,
				PrevPParamsData: paramsData,
				Epoch:           12,
				EraIndex:        ledgerstate.EraConway,
				EraBounds: make(
					[]ledgerstate.EraBound,
					ledgerstate.EraConway+1,
				),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Treasury:            treasury,
				Reserves:            14_914_270_613_432_674,
				Tip: &ledgerstate.SnapshotTip{
					Slot:      123_456,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 100, nil
			},
		},
	))
	requireTreasuryValue(t, ls, nil, treasury)
}

func TestLedgerViewTreasuryValueFailsClosedWithoutNetworkState(t *testing.T) {
	t.Parallel()

	ls, _ := newTreasuryViewTestLedger(t, "")
	value, err := ls.NewView(nil).TreasuryValue()
	require.ErrorContains(t, err, "network state is unavailable")
	require.Zero(t, value)
}

func TestLedgerViewTreasuryValuePropagatesStorageErrors(t *testing.T) {
	t.Parallel()

	ls, db := newTreasuryViewTestLedger(t, t.TempDir())
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		"INSERT INTO network_state (treasury, reserves, slot) VALUES (?, ?, ?)",
		"not-a-number",
		"0",
		1,
	)
	require.NoError(t, err)

	value, err := ls.NewView(nil).TreasuryValue()
	require.ErrorContains(t, err, "get treasury network state")
	require.ErrorContains(t, err, "invalid syntax")
	require.Zero(t, value)
}

func proposalSetTestPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = lcommon.ProtocolVersionConway + 1
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    rat(67, 100),
		CommitteeNormal:       rat(67, 100),
		CommitteeNoConfidence: rat(60, 100),
		UpdateToConstitution:  rat(75, 100),
		HardForkInitiation:    rat(60, 100),
		PpNetworkGroup:        rat(67, 100),
		PpEconomicGroup:       rat(67, 100),
		PpTechnicalGroup:      rat(67, 100),
		PpGovGroup:            rat(75, 100),
		TreasuryWithdrawal:    rat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    rat(51, 100),
		CommitteeNormal:       rat(51, 100),
		CommitteeNoConfidence: rat(51, 100),
		HardForkInitiation:    rat(51, 100),
		PpSecurityGroup:       rat(51, 100),
	}
	return p
}

func runProposalSetBoundary(
	t *testing.T,
	db *database.Database,
	pparams lcommon.ProtocolParameters,
	newEpoch uint64,
	boundarySlot uint64,
) *governance.EpochOutput {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := governance.ProcessEpoch(&governance.EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: boundarySlot,
		PParams:      pparams,
		UpdateFn: func(
			p lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	return out
}

// Conway GOV resolves votes and parent references against the proposals set
// (Rules/Gov.hs: curGovActionIds = proposalsActionsMap proposals, and
// proposalsAddAction's pGraph membership test). EPOCH removes an expired
// action and its subtree only when it applies the pulser that classified it
// (Rules/Epoch.hs proposalsApplyEnactment), one boundary after RATIFY's
// `gasExpiresAfter < reCurrentEpoch` (Rules/Ratify.hs). In the epoch between,
// the action and its descendants are still members: a child may name it, a
// descendant may still be voted on, and a vote on the action itself fails
// only VotingOnExpiredGovAction (`curEpoch <= gasExpiresAfter`).
func TestProposalSetKeepsExpiredActionUntilItIsDropped(t *testing.T) {
	t.Parallel()

	pparams := proposalSetTestPParams()
	lv, db := governanceTestView(t, pparams)
	for epoch := uint64(10); epoch <= 12; epoch++ {
		require.NoError(t, db.SetEpoch(
			epoch*100, epoch, nil, nil, nil, nil, 0, 1, 100, nil,
		))
	}
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	parentID := governanceTestID(0x41, 0)
	childID := governanceTestID(0x42, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        parentID.TransactionId[:],
		ActionIndex:   parentID.GovActionIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 10,
		ExpiresEpoch:  10,
		Deposit:       5,
		ReturnAddress: returnAddrBytes,
		AddedSlot:     1_010,
	}, hardForkGovernanceTestAction(t, nil, 11, 0))
	parentIdx := parentID.GovActionIdx
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          childID.TransactionId[:],
		ActionIndex:     childID.GovActionIdx,
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch:   10,
		ExpiresEpoch:    16,
		ParentTxHash:    parentID.TransactionId[:],
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       1_020,
	}, hardForkGovernanceTestAction(t, &parentID, 11, 1))

	// The parent's last voting epoch is 10. RATIFY at the boundary into 11
	// classifies it expired; it stays in the proposals set throughout 11.
	out := runProposalSetBoundary(t, db, pparams, 11, 1_100)
	require.Equal(t, 1, out.ExpiredCount)
	const slotInEleven = 1_150

	parentState, err := lv.GovActionById(parentID)
	require.NoError(t, err)
	require.NotNil(
		t,
		parentState,
		"expired parent left the proposals set a boundary early",
	)
	require.True(t, lv.GovActionExists(parentID))
	var expiryErr conway.VotingOnExpiredGovActionError
	require.ErrorAs(t, conway.UtxoValidateVotingOnExpiredGovAction(
		governanceVoteTestTx(parentID, lcommon.VoterTypeDRepKeyHash),
		slotInEleven, lv, pparams,
	), &expiryErr)

	grandchild := governanceProposalTestTx(
		t,
		hardForkGovernanceTestAction(t, &parentID, 11, 2),
	)
	require.NoError(t, conway.UtxoValidateProposalAncestry(
		grandchild, slotInEleven, lv, pparams,
	), "a child naming a member of the proposals set was rejected")

	require.True(
		t,
		lv.GovActionExists(childID),
		"descendant of an expired action left the proposals set a boundary early",
	)
	childVote := governanceVoteTestTx(childID, lcommon.VoterTypeDRepKeyHash)
	require.NoError(t, conway.UtxoValidateUnknownGovActionIds(
		childVote, slotInEleven, lv, pparams,
	))
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		childVote, slotInEleven, lv, pparams,
	))

	// A child proposed during 11 under the expired parent joins its subtree
	// and leaves with it at the boundary into 12.
	lateChildID := governanceTestID(0x43, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          lateChildID.TransactionId[:],
		ActionIndex:     lateChildID.GovActionIdx,
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch:   11,
		ExpiresEpoch:    17,
		ParentTxHash:    parentID.TransactionId[:],
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       slotInEleven,
	}, hardForkGovernanceTestAction(t, &parentID, 11, 2))

	runProposalSetBoundary(t, db, pparams, 12, 1_200)
	for _, id := range []lcommon.GovActionId{parentID, childID, lateChildID} {
		state, err := lv.GovActionById(id)
		require.NoError(t, err)
		require.Nil(t, state, "dropped subtree still resolvable")
		require.False(t, lv.GovActionExists(id))
		proposal, err := db.GetGovernanceProposal(
			id.TransactionId[:], id.GovActionIdx, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, proposal.DroppedEpoch)
		require.Equal(t, uint64(12), *proposal.DroppedEpoch)
	}
	var unknownErr conway.UnknownGovActionIdError
	require.ErrorAs(t, conway.UtxoValidateUnknownGovActionIds(
		childVote, 1_250, lv, pparams,
	), &unknownErr)
}

// preprodVoteOnExpiredParentsChild is preprod transaction
// d7d663ce95ec482418e5678d3ac1b6dfb748cdb5a093ec44385b9050f9351904, in block
// de223e7f0b0628fe158e472a83fef45d6028ff8d637d31d507ae23e9dc3f7163 (height
// 5,152,095, slot 133,175,865, epoch 312). It carries a DRep vote on
// ParameterChange 4f2b214e...7b15#0 (proposed in 311, gasExpiresAfter 317),
// whose parent 78a9aafe...0db3#0 (proposed in 305, gasExpiresAfter 311) RATIFY
// classified expired at the boundary into 312. Both left the proposals set at
// the boundary into 313.
const preprodVoteOnExpiredParentsChild = "" +
	"84a500d90102818258203d5f13a8d355d53eb7e99ce447fe890e6ecdc6c014e3" +
	"da4deef32f268608748100018182583930fe12059162068d4302ae76efd38515" +
	"3a08ee82aed46a0a6f4238ce6ae867f87c9bddadcae49ce60d430d5f65876c72" +
	"a0ab6beab2d8434c0b1b000000011bb1ce7f021a0002e481075820bdaa99eb15" +
	"8414dea0a91d6c727e2268574b23efe6e08ab3b841abe8059a030c13a1820358" +
	"1cfe12059162068d4302ae76efd385153a08ee82aed46a0a6f4238ce6aa18258" +
	"204f2b214e38732ed27cef1006470a7a161577dcb5ca620b594a5626155fc07b" +
	"1500820182785d68747470733a2f2f676174657761792e70696e6174612e636c" +
	"6f75642f697066732f6261666b726569676a62676478736c72356166656c6f78" +
	"666e6d737a34376a33336662636e66377565746f76636b657833627274706477" +
	"743576755820165be521963b57ce3857736ae44f64068addb490183ffe50b4cb" +
	"e6c0b34d380ba200d90102828258204edacc8cba0ff93118666ec81f1aabe085" +
	"221f8e3ca5117371042d1820dce4d55840a911d5db111f7a1ac7bb26fb38bad9" +
	"8703d6b32aea407d9eceab555bfac129b03c0cec9be3b7b43bfa16dd7ea81d4e" +
	"bd2b793a9ed77814f0a0e6f153a0ace804825820de33b1511cac065733086ca2" +
	"9d3b2c75597dafbf3613299f90412ab5ff80526058400edbe15ec0f6cde0fbb0" +
	"6a2cd718e1dc95c806d1f3628ffa04a645e7b0837d88b5f9a7fbb5577d01cdfd" +
	"40e376a8e6fc533ce9375f50d7df50cb3cb2b978510301d90102818303028382" +
	"00581c16c1554c34114687cfe699e548e1799c4a1dc17c76b869a63236186382" +
	"00581c63083bdf144e0976e5ac4dd23cd82f5d0493d7a46365c6c8f1f9899982" +
	"00581c6e23d3de62a0f2820f0c6e9c462e366e9a1448c3d6970a1619f5460bf5" +
	"d90103a0"

func TestPreprodVoteOnDescendantOfExpiredActionIsValid(t *testing.T) {
	t.Parallel()

	const (
		epochLength   = 432_000
		boundary312   = 133_142_400
		voteSlot      = 133_175_865
		lifetime      = 6
		parentEpoch   = 305
		childEpoch    = 311
		boundaryEpoch = 312
	)
	txBytes, err := hex.DecodeString(preprodVoteOnExpiredParentsChild)
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(txBytes)
	require.NoError(t, err)
	require.Equal(
		t,
		"d7d663ce95ec482418e5678d3ac1b6dfb748cdb5a093ec44385b9050f9351904",
		tx.Hash().String(),
	)

	pparams := proposalSetTestPParams()
	lv, db := governanceTestView(t, pparams)
	for epoch := uint64(parentEpoch); epoch <= boundaryEpoch; epoch++ {
		start := boundary312 - (boundaryEpoch-epoch)*epochLength
		require.NoError(t, db.SetEpoch(
			start, epoch, nil, nil, nil, nil, 0, 1, epochLength, nil,
		))
	}
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		make([]byte, lcommon.AddressHashSize),
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	parentHash, err := hex.DecodeString(
		"78a9aafe2e4e14828efa8cd5202fec08c996a9a00c7d56b317b6a95a80510db3",
	)
	require.NoError(t, err)
	childHash, err := hex.DecodeString(
		"4f2b214e38732ed27cef1006470a7a161577dcb5ca620b594a5626155fc07b15",
	)
	require.NoError(t, err)
	var parentID lcommon.GovActionId
	copy(parentID.TransactionId[:], parentHash)
	parentIdx := uint32(0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        parentHash,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: parentEpoch,
		ExpiresEpoch:  parentEpoch + lifetime,
		Deposit:       5,
		ReturnAddress: returnAddrBytes,
		AddedSlot:     boundary312 - 7*epochLength,
	}, &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
	})
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:          childHash,
		ActionType:      uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch:   childEpoch,
		ExpiresEpoch:    childEpoch + lifetime,
		ParentTxHash:    parentHash,
		ParentActionIdx: &parentIdx,
		Deposit:         5,
		ReturnAddress:   returnAddrBytes,
		AddedSlot:       boundary312 - epochLength/2,
	}, &conway.ConwayParameterChangeGovAction{
		Type:     uint(lcommon.GovActionTypeParameterChange),
		ActionId: &parentID,
	})

	out := runProposalSetBoundary(t, db, pparams, boundaryEpoch, boundary312)
	require.Equal(t, 1, out.ExpiredCount)

	require.NoError(t, conway.UtxoValidateUnknownGovActionIds(
		tx, voteSlot, lv, pparams,
	), "preprod accepted this vote on a proposals-set member")
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		tx, voteSlot, lv, pparams,
	))
}
