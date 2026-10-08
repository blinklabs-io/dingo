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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
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
	require.NoError(
		t,
		db.SetGovernanceProposal(context.Background(), proposal, txn),
	)
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{
			{ColdCredentialTag: 0, ColdCredHash: hash[:], ExpiresEpoch: 41},
			{ColdCredentialTag: 1, ColdCredHash: hash[:], ExpiresEpoch: 42},
		}, nil),
	)
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
				require.NoError(
					t,
					db.SetCommitteeMembers(context.Background(), members, nil),
				)

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

	proposal, err := db.GetGovernanceProposal(
		context.Background(),
		governanceTestHash(0x92),
		0,
		nil,
	)
	require.NoError(t, err)
	expired, dropped := uint64(101), uint64(102)
	proposal.ExpiredEpoch = &expired
	proposal.DroppedEpoch = &dropped
	require.NoError(
		t,
		db.SetGovernanceProposal(context.Background(), proposal, nil),
	)
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
	txn := db.MetadataTxn(context.Background(), true)
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), members, txn),
	)
	require.NoError(t, txn.Rollback())
	txn.Release()

	stored, err := db.GetCommitteeMembers(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, stored)

	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), members, nil),
	)
	require.NoError(t, db.SoftDeleteCommitteeMembers(
		context.Background(),
		[]models.CommitteeCredential{{
			CredentialTag: 1,
			Credential:    hash[:],
		}},
		50,
		nil,
	))
	stored, err = db.GetCommitteeMembers(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, uint8(0), stored[0].ColdCredentialTag)

	require.NoError(
		t,
		db.DeleteCommitteeMembersAfterSlot(context.Background(), 49, nil),
	)
	stored, err = db.GetCommitteeMembers(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, stored, 2)
}

func TestCommitteeTermStartPresenceSurvivesStorageRollback(t *testing.T) {
	t.Parallel()

	_, db := committeeTestView(t, &conway.ConwayProtocolParameters{})
	cold := committeeTestCredential(0xa2)
	require.NoError(t, db.SetCommitteeMembers(
		context.Background(),
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
		members, err := db.GetCommitteeMembers(context.Background(), nil)
		require.NoError(t, err)
		require.Len(t, members, 1)
		require.Equal(t, wantStart, members[0].TermStartSlot)
		require.True(t, members[0].TermStartSlotSet)
	}
	assertTermStart(0)

	require.NoError(t, db.SetCommitteeMembers(
		context.Background(),
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

	require.NoError(
		t,
		db.DeleteCommitteeMembersAfterSlot(context.Background(), 15, nil),
	)
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
			context.Background(),
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
				context.Background(),
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
		context.Background(),
		[]*models.CommitteeMember{{
			ColdCredHash: seated.Credential[:],
			ExpiresEpoch: 60,
		}},
		nil,
	))
	proposed := committeeTestCredential(0x62)
	txn := db.MetadataTxn(context.Background(), true)
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
		context.Background(),
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
	require.NoError(
		t,
		db.SoftDeleteAllCommitteeMembers(context.Background(), 10, nil),
	)
	seatedNow, err := db.GetCommitteeMembers(context.Background(), nil)
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
// TermStartSlot-stamping decision.
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
	require.NoError(
		t,
		db.SetGovernanceProposal(context.Background(), proposal, nil),
	)
	_, err = governance.EnactProposal(context.Background(), &governance.EnactmentContext{
		DB:      db,
		Epoch:   0,
		Slot:    slot,
		PParams: pparams,
	}, proposal)
	require.NoError(t, err)
}

// TestLedgerViewCommitteeHotCredentialSurvivesTermRenewal reproduces the
// live Preview halt end-to-end, driving the real
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredentialTag: uint8(elected.CredType),
			ColdCredHash:      elected.Credential[:],
			ExpiresEpoch:      10,
		}}, nil),
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      10,
		}}, nil),
	)
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      10,
		}}, nil),
	)
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredentialTag: uint8(elected.CredType),
			ColdCredHash:      elected.Credential[:],
			ExpiresEpoch:      10,
		}}, nil),
	)
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
					db.SetCommitteeMembers(
						context.Background(),
						[]*models.CommitteeMember{{
							ColdCredentialTag: uint8(cold.CredType),
							ColdCredHash:      cold.Credential[:],
							ExpiresEpoch:      10,
						}},
						nil,
					),
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
			ColdCredentialTag: uint8(cold.CredType),
			ColdCredHash:      cold.Credential[:],
			ExpiresEpoch:      10,
		}}, nil),
	)
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
// is the direct proof for the plural capability: when two
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{
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
		}, nil),
	)
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
// is the end-to-end regression for through dingo's real
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
// that fallback was the singular CommitteeHotCredentialMember,
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
	require.NoError(
		t,
		db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{
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
		}, nil),
	)
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
				require.NoError(
					t,
					db.SetCommitteeMembers(context.Background(), seated, nil),
				)
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
// the Blockfrost adapter's DRep reads, not the
// plain UTxO+reward figure GetDRepVotingPower alone returns.
func TestLedgerViewGetDRepVotingPowerIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	cm, err := chain.NewManager(context.Background(), db, nil)
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

	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: drepStakeCred,
			Drep:       drepCred,
			DrepType:   models.DrepTypeAddrKeyHash,
			AddedSlot:  1,
			Active:     true,
		}),
	)
	require.NoError(t, db.CreateUtxo(context.Background(), nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x44}, 32),
		OutputIdx:  0,
		StakingKey: drepStakeCred,
		AddedSlot:  1,
		Amount:     100,
	}))

	require.NoError(
		t,
		db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey: returnStakeCred,
			Drep:       drepCred,
			DrepType:   models.DrepTypeAddrKeyHash,
			AddedSlot:  1,
			Active:     true,
		}),
	)
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		returnStakeCred,
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	require.NoError(
		t,
		db.SetGovernanceProposal(
			context.Background(),
			&models.GovernanceProposal{
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
			},
			nil,
		),
	)

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
// conservation by exactly the deposit.
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
	require.NoError(
		t,
		db.SetGovernanceProposal(context.Background(), proposal, nil),
	)
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
// rows.
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
// gouroboros ledger rule. It mirrors view_test.go's
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
	err := ls.WithTxValidationSession(context.Background(), func(
		validate func(
			tx lcommon.Transaction,
			consumedUtxos map[utxoref.Key]struct{},
			createdUtxos map[utxoref.Key]lcommon.Utxo,
			accounts *utxoref.StateOverlay,
		) error,
		stillCurrent func() bool,
		_ func(func() error) (bool, error),
	) error {
		return validate(tx, nil, nil, nil)
	})

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

func TestTxValidationCommitExcludesLedgerPublication(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := newFakeEraLedgerState(db, nil, nil)
	before, _ := ls.loadStateSnapshots()

	err = ls.WithTxValidationSession(func(
		_ func(lcommon.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo, *utxoref.StateOverlay) error,
		_ func() bool,
		commitIfCurrent func(func() error) (bool, error),
	) error {
		writerAttempted := make(chan struct{})
		writerDone := make(chan struct{})
		committed, commitErr := commitIfCurrent(func() error {
			go func() {
				ls.Lock()
				close(writerAttempted)
				ls.publishSnapshotsLocked()
				ls.Unlock()
				close(writerDone)
			}()
			<-writerAttempted
			select {
			case <-writerDone:
				t.Fatal("ledger publication completed inside guarded commit")
			default:
			}
			return nil
		})
		require.NoError(t, commitErr)
		require.True(t, committed)
		<-writerDone
		return nil
	})
	require.NoError(t, err)
	after, _ := ls.loadStateSnapshots()
	require.Equal(t, before.generation+1, after.generation)
}

func TestTxValidationCommitDoesNotWaitForLedgerWriteLock(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := newFakeEraLedgerState(db, nil, nil)

	ls.Lock()
	defer ls.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- ls.WithTxValidationSession(func(
			_ func(lcommon.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo, *utxoref.StateOverlay) error,
			_ func() bool,
			commitIfCurrent func(func() error) (bool, error),
		) error {
			committed, commitErr := commitIfCurrent(func() error { return nil })
			if !committed {
				return errors.New("validation generation changed")
			}
			return commitErr
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("validation commit waited for ledger write lock")
	}
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

			err := db.Transaction(context.Background(), true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(
						context.Background(),
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
	txn := db.Transaction(context.Background(), true)
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

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.DeleteNetworkStateAfterSlot(context.Background(), 10, txn); err != nil {
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
	txn := db.MetadataTxn(context.Background(), true)
	defer txn.Release()
	out, err := governance.ProcessEpoch(context.Background(), &governance.EpochInput{
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
			context.Background(),
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

func TestLedgerViewUnimplementedMethodsReturnSentinelError(t *testing.T) {
	t.Parallel()

	lv := &LedgerView{}

	rewards, err := lv.CalculateRewards(
		lcommon.AdaPots{},
		lcommon.RewardSnapshot{},
		lcommon.RewardParameters{},
	)
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Nil(t, rewards)

	snapshot, err := lv.GetRewardSnapshot(0)
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Equal(t, lcommon.RewardSnapshot{}, snapshot)

	adaPots, err := lv.GetAdaPotsWithError()
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Equal(t, lcommon.AdaPots{}, adaPots)

	// GetAdaPots satisfies common.RewardState, which gives it no way to
	// report the sentinel, so it stands in a zero value rather than killing
	// a caller that reaches it through the interface.
	var rewardState lcommon.RewardState = lv
	var interfacePots lcommon.AdaPots
	require.NotPanics(t, func() {
		interfacePots = rewardState.GetAdaPots()
	})
	require.Equal(t, lcommon.AdaPots{}, interfacePots)

	err = lv.UpdateAdaPots(lcommon.AdaPots{})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestLedgerViewRewardAccountBalance(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	for _, tag := range []uint8{0, 1} {
		require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
			StakingKey:    key,
			CredentialTag: tag,
			Reward:        types.Uint64(100 + uint64(tag)),
			Active:        true,
		}))
	}
	inactive := bytes.Repeat([]byte{0xa2}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: inactive,
		Reward:     55,
		Active:     false,
	}))
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}
	credential := func(tag uint, value []byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   tag,
			Credential: lcommon.NewBlake2b224(value),
		}
	}
	for _, tc := range []struct {
		name string
		cred lcommon.Credential
		want *uint64
	}{
		{name: "key credential", cred: credential(0, key), want: new(uint64(100))},
		{name: "script credential", cred: credential(1, key), want: new(uint64(101))},
		{name: "missing credential", cred: credential(0, bytes.Repeat([]byte{0xa3}, 28))},
		{name: "inactive credential", cred: credential(0, inactive)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.RewardAccountBalance(tc.cred)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tc.want, *got)
		})
	}
	zero := bytes.Repeat([]byte{0xa4}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: zero,
		Reward:     0,
		Active:     true,
	}))
	got, err := lv.RewardAccountBalance(credential(0, zero))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Zero(t, *got)

	_, err = lv.RewardAccountBalance(lcommon.Credential{CredType: 2})
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported stake credential tag")

	require.NoError(t, dbtest.CloseDatabase(db))
	_, err = lv.RewardAccountBalance(credential(0, key))
	require.Error(t, err)
}

func TestLedgerViewStakeCredentialDeposit(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	sharedHash := bytes.Repeat([]byte{0xb1}, lcommon.AddressHashSize)
	credential := func(tag uint, value []byte) lcommon.Credential {
		return lcommon.Credential{
			CredType:   tag,
			Credential: lcommon.NewBlake2b224(value),
		}
	}
	keyCredential := credential(lcommon.CredentialTypeAddrKeyHash, sharedHash)
	scriptCredential := credential(lcommon.CredentialTypeScriptHash, sharedHash)
	zeroCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xb6}, lcommon.AddressHashSize),
	)
	importedKeyCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xb8}, lcommon.AddressHashSize),
	)
	importedScriptCredential := credential(
		lcommon.CredentialTypeScriptHash,
		bytes.Repeat([]byte{0xb9}, lcommon.AddressHashSize),
	)
	importedThenRegisteredCredential := credential(
		lcommon.CredentialTypeAddrKeyHash,
		bytes.Repeat([]byte{0xbc}, lcommon.AddressHashSize),
	)
	persistViewStakeRegistration(t, db, keyCredential, 2_000_000, 100, 0xb2)
	persistViewStakeRegistration(t, db, scriptCredential, 3_000_000, 101, 0xb3)
	persistViewStakeRegistration(t, db, zeroCredential, 0, 102, 0xb7)
	persistViewStakeRegistration(
		t,
		db,
		importedKeyCredential,
		1_000_000,
		90,
		0xba,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedKeyCredential,
		4_000_000,
		103,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedScriptCredential,
		5_000_000,
		104,
	)
	persistViewImportedStakeAccount(
		t,
		db,
		importedThenRegisteredCredential,
		4_500_000,
		105,
	)
	persistViewStakeRegistration(
		t,
		db,
		importedThenRegisteredCredential,
		6_000_000,
		106,
		0xbd,
	)

	inactiveHash := bytes.Repeat([]byte{0xb4}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey:    inactiveHash,
		CredentialTag: 0,
		Active:        false,
	}))
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}

	for _, tc := range []struct {
		name string
		cred lcommon.Credential
		want *uint64
	}{
		{name: "key credential", cred: keyCredential, want: new(uint64(2_000_000))},
		{name: "script credential", cred: scriptCredential, want: new(uint64(3_000_000))},
		{name: "zero deposit", cred: zeroCredential, want: new(uint64(0))},
		{
			name: "imported key credential",
			cred: importedKeyCredential,
			want: new(uint64(4_000_000)),
		},
		{
			name: "imported script credential",
			cred: importedScriptCredential,
			want: new(uint64(5_000_000)),
		},
		{
			name: "newer registration supersedes import",
			cred: importedThenRegisteredCredential,
			want: new(uint64(6_000_000)),
		},
		{
			name: "missing credential",
			cred: credential(
				lcommon.CredentialTypeAddrKeyHash,
				bytes.Repeat([]byte{0xb5}, lcommon.AddressHashSize),
			),
		},
		{
			name: "inactive credential",
			cred: credential(lcommon.CredentialTypeAddrKeyHash, inactiveHash),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lv.StakeCredentialDeposit(tc.cred)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tc.want, *got)
		})
	}

	importHistory, err := db.GetAccountRegistrationHistoryByCredential(context.Background(),
		1,
		importedScriptCredential.Credential[:],
		10,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, importHistory)

	require.NoError(t, db.DeleteCertificatesAfterSlot(context.Background(), 105, nil))
	require.NoError(t, db.RestoreAccountStateAtSlot(context.Background(), 105, nil))
	depositAfterRollback, err := lv.StakeCredentialDeposit(
		importedThenRegisteredCredential,
	)
	require.NoError(t, err)
	require.NotNil(t, depositAfterRollback)
	require.Equal(t, uint64(4_500_000), *depositAfterRollback)

	_, err = lv.StakeCredentialDeposit(lcommon.Credential{CredType: 2})
	require.ErrorContains(t, err, "unsupported stake credential tag")

	require.NoError(t, dbtest.CloseDatabase(db))
	_, err = lv.StakeCredentialDeposit(keyCredential)
	require.Error(t, err)
}

func persistViewImportedStakeAccount(
	t *testing.T,
	db *database.Database,
	credential lcommon.Credential,
	deposit uint64,
	slot uint64,
) {
	t.Helper()
	credentialTag, err := models.CredentialTagFromUint(credential.CredType)
	require.NoError(t, err)
	importDeposit := types.Uint64(deposit)
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    credential.Credential[:],
		CredentialTag: credentialTag,
		AddedSlot:     slot,
		Active:        true,
		ImportDeposit: &importDeposit,
	}, nil))
}

func persistViewStakeRegistration(
	t *testing.T,
	db *database.Database,
	credential lcommon.Credential,
	deposit uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{seed}, 32))
	builder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{seed + 1}, 32),
		0,
	)
	require.NoError(t, err)
	builder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(
			"addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd",
		).
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder.WithOutputs(output)
	builder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: credential,
	})
	tx, err := builder.Build()
	require.NoError(t, err)
	require.NoError(t, db.SetTransactionMetadataOnly(context.Background(),
		tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed + 2}, 32)),
		0,
		map[int]uint64{0: deposit},
		nil,
	))
}

// TestLedgerViewPoolCurrentStatePendingRetirement proves PoolCurrentState's
// pending-retirement epoch tracks the pool's latest retirement certificate
// by insertion order (AddedSlot), not the maximum epoch value across every
// retirement row on the pool: a later retirement certificate replaces the
// prior schedule even when it targets an earlier epoch, and a later pool
// registration cancels a pending retirement entirely -- mirroring
// poolIsActive's ordering rule in
// internal/test/conformance/state_provider.go, which this adapter's own
// review caught duplicating this same defect from.
func TestLedgerViewPoolCurrentStatePendingRetirement(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	lv := &LedgerView{
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		},
	}

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xb1}, 28)),
	)

	applyCert := func(slot uint64, txIDSeed byte, cert lcommon.Certificate) {
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
			0,
		)
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
		txBuilder.WithType(gledger.TxTypeDijkstra)
		txBuilder.WithValid(true)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithCertificates(cert)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}
	registrationCert := func(seed byte) *lcommon.PoolRegistrationCertificate {
		return &lcommon.PoolRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypePoolRegistration),
			Operator: poolKeyHash,
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256([]byte{seed, 0x02}),
			),
			Pledge: 1_000_000,
			Cost:   340_000_000,
			Margin: cbor.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224([]byte{seed, 0x03}),
			),
		}
	}
	retirementCert := func(epoch uint64) *lcommon.PoolRetirementCertificate {
		return &lcommon.PoolRetirementCertificate{
			CertType:    uint(lcommon.CertificateTypePoolRetirement),
			PoolKeyHash: poolKeyHash,
			Epoch:       epoch,
		}
	}

	applyCert(1, 0x01, registrationCert(0x01))

	// A retirement targeting epoch 10, then a later retirement targeting an
	// EARLIER epoch (5): the later certificate must win regardless of its
	// epoch value being smaller than the one it replaces.
	applyCert(2, 0x02, retirementCert(10))
	applyCert(3, 0x03, retirementCert(5))

	_, pendingEpoch, err := lv.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.NotNil(t, pendingEpoch)
	require.Equal(
		t,
		uint64(5),
		*pendingEpoch,
		"a later retirement certificate must replace the prior schedule even when it moves the target epoch earlier",
	)

	// A later re-registration cancels the pending retirement entirely.
	applyCert(4, 0x04, registrationCert(0x04))

	_, pendingEpoch, err = lv.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.Nil(
		t,
		pendingEpoch,
		"a later pool registration must cancel a pending retirement",
	)

	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestLedgerViewIsVrfKeyInUseRespectsEpochBoundaryDeferral is the
// LedgerView-level regression test for the VRF reservation across deferred
// re-registration, exercised through the real certificate-application pipeline
// rather than the store layer directly: a pool re-registering with a new VRF
// key mid-epoch must not free its old key before the epoch boundary
// IsVrfKeyInUse is asked about.
func TestLedgerViewIsVrfKeyInUseRespectsEpochBoundaryDeferral(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	lv := &LedgerView{ls: ls}

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xc1}, 28)),
	)
	oldVrfKeyHash := lcommon.NewBlake2b256([]byte{0xc2, 0x01})
	newVrfKeyHash := lcommon.NewBlake2b256([]byte{0xc2, 0x02})

	applyRegistration := func(
		slot uint64,
		txIDSeed byte,
		vrfKeyHash lcommon.VrfKeyHash,
	) {
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
			0,
		)
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		cert := &lcommon.PoolRegistrationCertificate{
			CertType:   uint(lcommon.CertificateTypePoolRegistration),
			Operator:   poolKeyHash,
			VrfKeyHash: vrfKeyHash,
			Pledge:     1_000_000,
			Cost:       340_000_000,
			Margin:     cbor.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224([]byte{txIDSeed, 0x03}),
			),
		}
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
		txBuilder.WithType(gledger.TxTypeDijkstra)
		txBuilder.WithValid(true)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithCertificates(cert)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}

	// A genesis registration predating both keys under test makes the
	// pinned epochStartSlot load-bearing rather than coincidental: without
	// it, GetPoolByVrfKeyHash's earliest-registration fallback (for a
	// pool with no pre-boundary registration) would resolve to this
	// genesis key instead of oldVrfKeyHash whenever epochStartSlot is
	// wrong, giving a visibly different, wrong answer rather than
	// happening to still match.
	applyRegistration(1, 0x09, lcommon.NewBlake2b256([]byte{0xc2, 0x00}))
	// P registers with the old key before the current epoch begins.
	applyRegistration(10, 0x01, oldVrfKeyHash)
	// P re-registers with a new key mid-epoch (slot 50), inside the epoch
	// that starts at slot 30 below. Not yet effective.
	applyRegistration(50, 0x02, newVrfKeyHash)

	// Pinned directly on the view, mirroring how real validation call
	// sites (NewView, ledgerProcessBlock, validateTxCore) pin
	// epochStartSlot at construction time rather than leaving
	// IsVrfKeyInUse to re-read ls.loadConsensusSnapshot() live.
	lv.epochStartSlot = 30

	inUse, owner, err := lv.IsVrfKeyInUse(oldVrfKeyHash)
	require.NoError(t, err)
	assert.True(t, inUse,
		"the pre-boundary key must still be reported as reserved")
	assert.Equal(t, poolKeyHash, owner)

	// The new key is already claimed by P itself, as its same-epoch
	// pending registration -- reported as "in use, by P" rather than
	// free, which is what lets gouroboros's caller compare against
	// PoolCurrentState and still allow P to keep using its own pending
	// key without being rejected as a self-conflict.
	inUse, owner, err = lv.IsVrfKeyInUse(newVrfKeyHash)
	require.NoError(t, err)
	assert.True(t, inUse, "P's own pending key must be reported claimed")
	assert.Equal(t, poolKeyHash, owner)

	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestLedgerViewIsVrfKeyInUseIgnoresConcurrentSnapshotRepublish verifies
// that IsVrfKeyInUse must
// use the epoch boundary pinned on the view at construction time, not
// whatever ls.loadConsensusSnapshot() returns when it happens to be
// called. A validation operation can run long enough that the writer
// publishes a newer snapshot -- and a new epoch boundary -- while it is
// still in progress; if IsVrfKeyInUse re-read that live snapshot, two
// calls against the same view could disagree with each other depending
// on exactly when each one ran, and would disagree with PoolCurrentState
// and every other pinned field (committeeEpoch, pp, syntheticV2CostModel)
// the same view is validating against.
func TestLedgerViewIsVrfKeyInUseIgnoresConcurrentSnapshotRepublish(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	lv := &LedgerView{ls: ls}

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xc9}, 28)),
	)
	keyA := lcommon.NewBlake2b256([]byte{0xca, 0x01})
	keyB := lcommon.NewBlake2b256([]byte{0xca, 0x02})

	applyRegistration := func(slot uint64, txIDSeed byte, vrfKeyHash lcommon.VrfKeyHash) {
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
			0,
		)
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		cert := &lcommon.PoolRegistrationCertificate{
			CertType:   uint(lcommon.CertificateTypePoolRegistration),
			Operator:   poolKeyHash,
			VrfKeyHash: vrfKeyHash,
			Pledge:     1_000_000,
			Cost:       340_000_000,
			Margin:     cbor.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224([]byte{txIDSeed, 0x03}),
			),
		}
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
		txBuilder.WithType(gledger.TxTypeDijkstra)
		txBuilder.WithValid(true)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithCertificates(cert)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}

	applyRegistration(10, 0x01, keyA) // pre-epoch: P's active key
	applyRegistration(50, 0x02, keyB) // this epoch (starts at 30): deferred

	// Pinned once, as if at the start of a long-running validation.
	lv.epochStartSlot = 30

	inUse, owner, err := lv.IsVrfKeyInUse(keyA)
	require.NoError(t, err)
	require.True(t, inUse)
	require.Equal(t, poolKeyHash, owner)

	// The writer advances the published epoch past the deferred
	// re-registration's slot while this view's validation is still
	// notionally in progress -- exactly the race the pinning avoids.
	ls.Lock()
	ls.currentEpoch = models.Epoch{StartSlot: 60}
	ls.publishSnapshotsLocked()
	ls.Unlock()

	// The same view, asked again, must still answer as of its pinned
	// boundary (30): keyA still active, keyB still not yet effective.
	// Reading the live snapshot instead would flip this to keyA freed,
	// keyB active -- the boundary having "already passed" from the
	// writer's perspective, but not from this validation's.
	inUse, owner, err = lv.IsVrfKeyInUse(keyA)
	require.NoError(t, err)
	assert.True(t, inUse,
		"a concurrent snapshot republish must not change this view's answer")
	assert.Equal(t, poolKeyHash, owner)

	inUse, _, err = lv.IsVrfKeyInUse(keyB)
	require.NoError(t, err)
	assert.True(t, inUse,
		"keyB is still claimed by P's own same-epoch pending registration "+
			"per this view's pinned boundary, regardless of the live one")

	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestLedgerViewIsVrfKeyInUseRejectsSameOperatorReuseOfSupersededFutureKey
// is the LedgerView-level regression test for the PV11+ follow-up:
// pool P cycles A -> B -> C within one epoch, then attempts to
// reuse B again. IsVrfKeyInUse must report B as still claimed by P (even
// though B is neither P's effective key, A, nor its current pending key,
// C), which is the signal gouroboros's validatePoolRegistration needs to
// compare against PoolCurrentState and reject the reuse: PoolCurrentState
// returns P's latest registration (C), which does not equal the requested
// key (B).
// TestLedgerViewIsVrfKeyInUseFreesSupersededFutureKey pins release of a
// superseded future VRF key at the LedgerView production entry point: once a
// pool's same-epoch registration is itself superseded by a later same-epoch
// registration, IsVrfKeyInUse must report it free, not still claimed.
func TestLedgerViewIsVrfKeyInUseFreesSupersededFutureKey(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	lv := &LedgerView{ls: ls}

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xd1}, 28)),
	)
	keyA := lcommon.NewBlake2b256([]byte{0xd2, 0x01})
	keyB := lcommon.NewBlake2b256([]byte{0xd2, 0x02})
	keyC := lcommon.NewBlake2b256([]byte{0xd2, 0x03})

	applyRegistration := func(
		slot uint64,
		txIDSeed byte,
		vrfKeyHash lcommon.VrfKeyHash,
	) {
		input, err := mockledger.NewSimpleTransactionInput(
			bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
			0,
		)
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
			WithLovelace(1_000_000).
			Build()
		require.NoError(t, err)
		cert := &lcommon.PoolRegistrationCertificate{
			CertType:   uint(lcommon.CertificateTypePoolRegistration),
			Operator:   poolKeyHash,
			VrfKeyHash: vrfKeyHash,
			Pledge:     1_000_000,
			Cost:       340_000_000,
			Margin:     cbor.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount: lcommon.AddrKeyHash(
				lcommon.NewBlake2b224([]byte{txIDSeed, 0x03}),
			),
		}
		txBuilder := mockledger.NewTransactionBuilder()
		txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
		txBuilder.WithType(gledger.TxTypeDijkstra)
		txBuilder.WithValid(true)
		txBuilder.WithInputs(input)
		txBuilder.WithOutputs(output)
		txBuilder.WithCertificates(cert)
		tx, err := txBuilder.Build()
		require.NoError(t, err)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}

	applyRegistration(10, 0x01, keyA) // pre-epoch: P's active key
	applyRegistration(50, 0x02, keyB) // this epoch: P: A -> B
	applyRegistration(70, 0x03, keyC) // this epoch: P: B -> C, supersedes B

	// Pinned directly on the view, mirroring how real validation call
	// sites (NewView, ledgerProcessBlock, validateTxCore) pin
	// epochStartSlot at construction time rather than leaving
	// IsVrfKeyInUse to re-read ls.loadConsensusSnapshot() live.
	lv.epochStartSlot = 30

	inUse, _, err := lv.IsVrfKeyInUse(keyB)
	require.NoError(t, err)
	assert.False(t, inUse,
		"a superseded same-epoch future key must be freed once a "+
			"later same-epoch registration replaces it")

	// P's current (latest) registration is genuinely C, not B: B was
	// never P's effective key and is no longer its pending one.
	current, _, err := lv.PoolCurrentState(poolKeyHash)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, keyC, current.VrfKeyHash)

	require.NoError(t, dbtest.CloseDatabase(db))
}

// newUnwrittenDijkstraPoolRegistrationTx builds a real
// *gdijkstra-compatible transaction carrying one pool registration
// certificate, WITHOUT writing it to the database -- for passing directly
// to eras.ValidateTxDijkstra so the actual rejection path (not just its
// preconditions) is exercised, the way dijkstra_pool_margin_floor_e2e_test.go
// does for the CIP-23 rule.
func newUnwrittenDijkstraPoolRegistrationTx(
	t *testing.T,
	txIDSeed byte,
	operator lcommon.PoolKeyHash,
	vrfKeyHash lcommon.VrfKeyHash,
) lcommon.Transaction {
	t.Helper()
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	cert := &lcommon.PoolRegistrationCertificate{
		CertType:   uint(lcommon.CertificateTypePoolRegistration),
		Operator:   operator,
		VrfKeyHash: vrfKeyHash,
		Pledge:     1_000_000,
		Cost:       340_000_000,
		Margin:     cbor.Rat{Rat: big.NewRat(1, 20)},
		RewardAccount: lcommon.AddrKeyHash(
			lcommon.NewBlake2b224([]byte{txIDSeed, 0x03}),
		),
	}
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size))
	txBuilder.WithType(gledger.TxTypeDijkstra)
	txBuilder.WithValid(true)
	txBuilder.WithInputs(input)
	txBuilder.WithOutputs(output)
	txBuilder.WithCertificates(cert)
	tx, err := txBuilder.Build()
	require.NoError(t, err)
	return tx
}

// TestValidateTxDijkstraRejectsDifferentPoolClaimingActiveKeyDuringDeferral
// is the full end-to-end proof of the acceptance criterion "reject a
// different pool registering the active key during the deferral window":
// not just that IsVrfKeyInUse reports the right owner, but that the real
// validation entry point actually returns a rejection for it. Protocol
// version 12 (Dijkstra) is required: DuplicateVrfKeysDisallowed only
// applies the check for major > 10.
func TestValidateTxDijkstraRejectsDifferentPoolClaimingActiveKeyDuringDeferral(
	t *testing.T,
) {
	t.Parallel()

	// newRewardCalculationTestLedger, not a bare &LedgerState{}: other
	// Dijkstra/Conway UTxO rules ValidateTxDijkstra also runs (e.g.
	// UtxoValidateProposalNetworkIds) call LedgerView.NetworkId(), which
	// needs a real config.CardanoNodeConfig with loaded genesis.
	ls, db := newRewardCalculationTestLedger(t)
	lv := &LedgerView{ls: ls}

	poolP := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xe1}, 28)),
	)
	poolQ := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xe2}, 28)),
	)
	keyA := lcommon.NewBlake2b256([]byte{0xe3, 0x01})
	keyB := lcommon.NewBlake2b256([]byte{0xe3, 0x02})

	applyRegistration := func(slot uint64, txIDSeed byte, operator lcommon.PoolKeyHash, vrfKeyHash lcommon.VrfKeyHash) {
		tx := newUnwrittenDijkstraPoolRegistrationTx(
			t, txIDSeed, operator, vrfKeyHash,
		)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}

	applyRegistration(10, 0x01, poolP, keyA) // pre-epoch: P's active key
	applyRegistration(50, 0x02, poolP, keyB) // this epoch: P: A -> B, deferred

	// Pinned directly on the view, mirroring how real validation call
	// sites (NewView, ledgerProcessBlock, validateTxCore) pin
	// epochStartSlot at construction time rather than leaving
	// IsVrfKeyInUse to re-read ls.loadConsensusSnapshot() live.
	lv.epochStartSlot = 30

	// Q attempts to claim key A, which is still P's active key during the
	// deferral window. Not written to the database -- this is the
	// certificate under validation, not setup.
	conflictingTx := newUnwrittenDijkstraPoolRegistrationTx(t, 0x03, poolQ, keyA)
	err := eras.ValidateTxDijkstra(
		conflictingTx,
		60,
		lv,
		dijkstraTestProtocolParameters(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

// TestValidateTxDijkstraRejectsSameOperatorReuseOfSupersededFutureKey is
// the full end-to-end proof of the PV11+ follow-up: pool P cycles
// A -> B -> C within one epoch, then attempts to reuse B again. This
// proves the actual rejection fires through the real validation entry
// point, not just that IsVrfKeyInUse and PoolCurrentState individually
// return the values the rejection depends on.
// TestValidateTxDijkstraAllowsReuseOfSupersededFutureKey pins release of a
// superseded future VRF key
// through the full production validation entry point: a key a pool cycled
// through and then superseded within the same epoch is free for any pool
// (including the pool that originally proposed it) to register.
func TestValidateTxDijkstraAllowsReuseOfSupersededFutureKey(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	lv := &LedgerView{ls: ls}

	poolP := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xf1}, 28)),
	)
	keyA := lcommon.NewBlake2b256([]byte{0xf2, 0x01})
	keyB := lcommon.NewBlake2b256([]byte{0xf2, 0x02})
	keyC := lcommon.NewBlake2b256([]byte{0xf2, 0x03})

	applyRegistration := func(slot uint64, txIDSeed byte, vrfKeyHash lcommon.VrfKeyHash) {
		tx := newUnwrittenDijkstraPoolRegistrationTx(
			t, txIDSeed, poolP, vrfKeyHash,
		)
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{txIDSeed}, lcommon.Blake2b256Size),
		}
		require.NoError(
			t,
			db.SetTransactionMetadataOnly(context.Background(),
				tx, point, 0, map[int]uint64{0: 500_000_000}, nil,
			),
		)
	}

	applyRegistration(10, 0x01, keyA) // pre-epoch: P's active key
	applyRegistration(50, 0x02, keyB) // this epoch: P: A -> B
	applyRegistration(70, 0x03, keyC) // this epoch: P: B -> C, supersedes B

	// Pinned directly on the view, mirroring how real validation call
	// sites (NewView, ledgerProcessBlock, validateTxCore) pin
	// epochStartSlot at construction time rather than leaving
	// IsVrfKeyInUse to re-read ls.loadConsensusSnapshot() live.
	lv.epochStartSlot = 30

	// P re-registers with B, a key it itself proposed and then superseded
	// within the same epoch. B is free -- neither P's effective key (still
	// A) nor its current pending key (now C) -- so the VRF-duplicate rule
	// must not fire. This deliberately minimal transaction (unresolvable
	// input, no witnesses, mismatched network) trips other, unrelated
	// Dijkstra UTxO validation rules; ValidateTxDijkstra joins all rule
	// errors, and this test only asserts on the VRF-duplicate substring,
	// the same pattern TestValidateTxDijkstraAcceptsAtOrAboveFloorPoolMarginThroughLedgerView
	// uses for its own companion negative assertion.
	reuseTx := newUnwrittenDijkstraPoolRegistrationTx(t, 0x04, poolP, keyB)
	err := eras.ValidateTxDijkstra(
		reuseTx,
		80,
		lv,
		dijkstraTestProtocolParameters(),
	)
	if err != nil {
		assert.NotContains(t, err.Error(), "already registered by pool")
	}
}

// TestLedgerStateNewViewPinsEpochStartSlot is the regression test: every prior
// epochStartSlot-pinning
// test set the field by hand on a bare &LedgerView{ls: ls}, so a real
// construction site (NewView, ledgerProcessBlock, validateTxCore,
// ValidateTxWithOverlay, EvaluateTx) could drop its own epochStartSlot:
// assignment without any test failing. This calls the real NewView
// directly and asserts the pin happened there.
func TestLedgerStateNewViewPinsEpochStartSlot(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.Lock()
	ls.currentEpoch = models.Epoch{StartSlot: 42}
	ls.publishSnapshotsLocked()
	ls.Unlock()

	view := ls.NewView(nil)
	assert.Equal(t, uint64(42), view.epochStartSlot)
}

// TestLedgerStateValidateTxPinsEpochStartSlot is the companion regression
// test for validateTxCore (reached through the real, public ls.ValidateTx
// entry point): a custom era descriptor's ValidateTxFunc captures the
// *LedgerView it is actually given, so this proves the pin reaches
// IsVrfKeyInUse's caller through the real path, not a hand-set field.
func TestLedgerStateValidateTxPinsEpochStartSlot(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	var capturedEpochStartSlot uint64
	var captured bool
	capturingEra := eras.ShelleyEraDesc
	capturingEra.ValidateTxFunc = func(
		_ lcommon.Transaction,
		_ uint64,
		ls lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) error {
		lv, ok := ls.(*LedgerView)
		require.True(t, ok, "validateTxCore must pass a *LedgerView")
		capturedEpochStartSlot = lv.epochStartSlot
		captured = true
		return nil
	}

	ls := &LedgerState{
		db:             db,
		currentEra:     capturingEra,
		currentPParams: dijkstraTestProtocolParameters(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.Lock()
	ls.currentEpoch = models.Epoch{StartSlot: 77}
	ls.publishSnapshotsLocked()
	ls.Unlock()

	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{0xd9}, lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{0xd9}, lcommon.Blake2b256Size))
	txBuilder.WithType(gledger.TxTypeShelley)
	txBuilder.WithValid(true)
	txBuilder.WithInputs(input)
	txBuilder.WithOutputs(output)
	tx, err := txBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, ls.ValidateTx(tx))
	require.True(t, captured, "ValidateTxFunc must have been called")
	assert.Equal(t, uint64(77), capturedEpochStartSlot)

	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestLedgerStateEvaluateTxPinsEpochStartSlot is the EvaluateTx companion
// of the two tests above.
func TestLedgerStateEvaluateTxPinsEpochStartSlot(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	var capturedEpochStartSlot uint64
	var captured bool
	capturingEra := eras.ShelleyEraDesc
	capturingEra.EvaluateTxFunc = func(
		_ lcommon.Transaction,
		ls lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
		lv, ok := ls.(*LedgerView)
		require.True(t, ok, "EvaluateTx must pass a *LedgerView")
		capturedEpochStartSlot = lv.epochStartSlot
		captured = true
		return 0, lcommon.ExUnits{}, nil, nil
	}

	ls := &LedgerState{
		db:             db,
		currentEra:     capturingEra,
		currentPParams: dijkstraTestProtocolParameters(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.Lock()
	ls.currentEpoch = models.Epoch{StartSlot: 88}
	ls.publishSnapshotsLocked()
	ls.Unlock()

	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{0xda}, lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{0xda}, lcommon.Blake2b256Size))
	txBuilder.WithType(gledger.TxTypeShelley)
	txBuilder.WithValid(true)
	txBuilder.WithInputs(input)
	txBuilder.WithOutputs(output)
	tx, err := txBuilder.Build()
	require.NoError(t, err)

	_, _, _, err = ls.EvaluateTx(tx)
	require.NoError(t, err)
	require.True(t, captured, "EvaluateTxFunc must have been called")
	assert.Equal(t, uint64(88), capturedEpochStartSlot)

	require.NoError(t, dbtest.CloseDatabase(db))
}

func TestLedgerViewSkipPhase2Validation(t *testing.T) {
	t.Parallel()

	lv := &LedgerView{}
	require.False(t, lv.SkipPhase2Validation())

	lv.skipPhase2Validation = true
	require.True(t, lv.SkipPhase2Validation())
}

// TestLedgerViewMinPoolMargin guards the CIP-23 bridge: MinPoolMargin() must
// forward from the LedgerState embedded via the named ls field, since Go does
// not promote methods across a named (non-embedded) field. Without this
// forwarding method, ls.(eras.MinPoolMarginProvider) in ledger/eras always fails
// for the *LedgerView actually passed to ValidateTx*, silently disabling the
// pool-margin-floor certificate rule.
func TestLedgerViewMinPoolMargin(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	lv := &LedgerView{ls: ls}
	require.Nil(t, lv.MinPoolMargin())

	ls.config.MinPoolMargin = 150
	require.Zero(t, big.NewRat(150, 10_000).Cmp(lv.MinPoolMargin()))
}

func TestExtractCostModelsFromPParams_Nil(t *testing.T) {
	t.Parallel()

	result := extractCostModelsFromPParams(nil)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_Alonzo(t *testing.T) {
	t.Parallel()

	pp := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 1)
	_, ok := result[lcommon.PlutusLanguage(1)]
	assert.True(t, ok, "expected PlutusV1 cost model")
}

func TestExtractCostModelsFromPParams_Babbage(t *testing.T) {
	t.Parallel()

	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 2)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
}

func TestExtractCostModelsFromPParams_Conway(t *testing.T) {
	t.Parallel()

	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
			2: {700, 800, 900},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	_, hasV3 := result[lcommon.PlutusLanguage(3)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
	assert.True(t, hasV3, "expected PlutusV3 cost model")
}

func TestExtractCostModelsFromPParams_NilCostModels(t *testing.T) {
	t.Parallel()

	pp := &babbage.BabbageProtocolParameters{
		CostModels: nil,
	}
	result := extractCostModelsFromPParams(pp)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_SkipsUnknownVersions(
	t *testing.T,
) {
	t.Parallel()

	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100},
			1: {200},
			2: {300},
			3: {400}, // unknown version, should be skipped
			9: {500}, // unknown version, should be skipped
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3,
		"should only include versions 0-2")
}

func TestCostModels_WithCurrentPParams(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentPParams: &conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{
				0: {1, 2, 3},
				1: {4, 5, 6},
				2: {7, 8, 9},
			},
		},
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.Len(t, result, 3)
}

func TestCostModels_NilPParams(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentPParams: nil,
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.NotNil(t, result,
		"should return empty map, not nil")
	require.Empty(t, result)
}

func TestIsCommitteeThresholdMet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		yesVotes             int
		totalActiveMembers   int
		thresholdNumerator   uint64
		thresholdDenominator uint64
		expected             bool
	}{
		{
			name:                 "no committee - threshold trivially met",
			yesVotes:             0,
			totalActiveMembers:   0,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             true,
		},
		{
			name:                 "zero threshold - always met",
			yesVotes:             0,
			totalActiveMembers:   5,
			thresholdNumerator:   0,
			thresholdDenominator: 1,
			expected:             true,
		},
		{
			name:                 "zero denominator - not met",
			yesVotes:             5,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 0,
			expected:             false,
		},
		{
			name:                 "2/3 threshold met exactly",
			yesVotes:             4,
			totalActiveMembers:   6,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             true,
		},
		{
			name:                 "2/3 threshold not met",
			yesVotes:             3,
			totalActiveMembers:   6,
			thresholdNumerator:   2,
			thresholdDenominator: 3,
			expected:             false,
		},
		{
			name:                 "simple majority met",
			yesVotes:             3,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 2,
			expected:             true,
		},
		{
			name:                 "simple majority not met",
			yesVotes:             2,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 2,
			expected:             false,
		},
		{
			name:                 "unanimous met",
			yesVotes:             5,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 1,
			expected:             true,
		},
		{
			name:                 "unanimous not met",
			yesVotes:             4,
			totalActiveMembers:   5,
			thresholdNumerator:   1,
			thresholdDenominator: 1,
			expected:             false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsCommitteeThresholdMet(
				tc.yesVotes,
				tc.totalActiveMembers,
				tc.thresholdNumerator,
				tc.thresholdDenominator,
			)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// A member seated by an UpdateCommittee enactment keeps only the committee
// certificates it recorded in the epoch the boundary closes: at the boundary
// before, it was not in the committee, so cardano-ledger dropped its
// csCommitteeCreds entry there (Conway EPOCH, updateCommitteeState). The
// RATIFY tally at the enactment boundary sees the same state: a member with
// no committee entry is not counted, while one with a hot key that did not
// vote counts as No (committeeAcceptedRatio). Here that decides whether a
// treasury withdrawal the incumbent member voted for is ratified.
func TestEpochRolloverSeatsMemberWithOnlyItsClosingEpochAuthorization(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		authSlot uint64
		hasHot   bool
	}{
		{name: "authorized before the closing epoch", authSlot: 499},
		{name: "authorized in the closing epoch", authSlot: 500, hasHot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTreasuryRolloverFixture(t, 100)
			require.Equal(t, uint64(500), f.currentEpoch.StartSlot)
			cold := lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(repeatByte(28, 0xd1)),
			}
			hot := repeatByte(28, 0xd2)
			action, err := lcommon.NewUpdateCommitteeGovAction(
				nil,
				nil,
				map[*lcommon.Credential]uint64{
					&cold: f.currentEpoch.EpochId + 20,
				},
				cbor.Rat{Rat: big.NewRat(1, 1)},
			)
			require.NoError(t, err)
			actionCbor, err := cbor.Encode(action)
			require.NoError(t, err)
			ratifiedEpoch := f.currentEpoch.EpochId
			ratifiedSlot := f.currentEpoch.StartSlot + 50
			update := &models.GovernanceProposal{
				TxHash:        repeatByte(32, 0xd3),
				ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
				ProposedEpoch: f.currentEpoch.EpochId - 2,
				ExpiresEpoch:  f.currentEpoch.EpochId + 20,
				AnchorHash:    repeatByte(32, 0xd4),
				ReturnAddress: repeatByte(29, 0xd5),
				GovActionCbor: actionCbor,
				AddedSlot:     350,
				RatifiedEpoch: &ratifiedEpoch,
				RatifiedSlot:  &ratifiedSlot,
			}
			require.NoError(t, f.db.SetGovernanceProposal(context.Background(), update, nil))
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot, 2, tc.authSlot)
			require.NoError(t, err)
			withdrawAddress, returnAddress, _ := f.rewardAddress(t, 0xd6)
			withdrawal := f.addProposal(
				t,
				0xd7,
				510,
				map[*lcommon.Address]uint64{withdrawAddress: 40},
				returnAddress,
				0,
				false,
			)

			result := f.rollover(t, f.currentEpoch, f.currentPParams)
			enacted := f.proposal(t, update)
			require.NotNil(t, enacted.EnactedSlot)

			state, err := governance.LoadCommitteeVotingState(context.Background(),
				f.db, nil, result.NewCurrentEpoch.EpochId,
			)
			require.NoError(t, err)
			wantActive := 1
			if tc.hasHot {
				wantActive = 2
			}
			require.Equal(t, wantActive, state.ActiveMemberCount)
			ratified := f.proposal(t, withdrawal)
			require.Equal(
				t,
				!tc.hasHot,
				ratified.RatifiedSlot != nil,
				"withdrawal ratification: %s",
				fmt.Sprint(ratified.RatifiedSlot),
			)

			lv := f.ls.NewView(nil)
			lv.epochStartSlot = result.NewCurrentEpoch.StartSlot
			member, err := lv.CommitteeHotCredentialMember(lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(hot),
			})
			require.NoError(t, err)
			require.Equal(t, tc.hasHot, member != nil)

			members, err := f.db.GetCommitteeMembers(context.Background(), nil)
			require.NoError(t, err)
			var seated *models.CommitteeMember
			for _, member := range members {
				if lcommon.NewBlake2b224(
					member.ColdCredHash,
				) == cold.Credential {
					seated = member
				}
			}
			require.NotNil(t, seated)
			require.Equal(t, f.currentEpoch.StartSlot, seated.TermStartSlot)
		})
	}
}

// Committee pruning is applied when committee state is read, not by deleting
// rows, so rolling an enactment back restores the pre-boundary answers
// exactly: the seated member is pending again, and its authorization from
// the restored epoch counts again.
func TestCommitteeEnactmentRollbackRestoresPendingAuthorization(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	cold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe1)),
	}
	hot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe2)),
	}
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint64{&cold: f.currentEpoch.EpochId + 20},
		cbor.Rat{Rat: big.NewRat(1, 1)},
	)
	require.NoError(t, err)
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := f.currentEpoch.EpochId
	ratifiedSlot := f.currentEpoch.StartSlot + 50
	update := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xe3),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: f.currentEpoch.EpochId - 2,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorHash:    repeatByte(32, 0xe4),
		ReturnAddress: repeatByte(29, 0xe5),
		GovActionCbor: actionCbor,
		AddedSlot:     350,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
	}
	require.NoError(t, f.db.SetGovernanceProposal(context.Background(), update, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot.Credential[:], 2, 520)
	require.NoError(t, err)

	type committeeAnswers struct {
		hotKnown bool
		colds    []lcommon.Credential
		elected  bool
		resigned bool
	}
	answers := func(epoch, epochStartSlot uint64) committeeAnswers {
		t.Helper()
		lv := f.ls.NewView(nil).pinCommitteeState(epoch, f.currentPParams)
		lv.epochStartSlot = epochStartSlot
		member, err := lv.CommitteeHotCredentialMember(hot)
		require.NoError(t, err)
		colds, err := lv.CommitteeHotCredentialColdCredentials(hot)
		require.NoError(t, err)
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		coldMember, err := lv.CommitteeCredentialMember(cold)
		require.NoError(t, err)
		require.NotNil(t, coldMember)
		return committeeAnswers{
			hotKnown: member != nil,
			colds:    colds,
			elected:  elected,
			resigned: coldMember.Resigned,
		}
	}
	before := answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
	}, before)

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
		elected:  true,
	}, answers(
		result.NewCurrentEpoch.EpochId,
		result.NewCurrentEpoch.StartSlot,
	))

	boundary := result.NewCurrentEpoch.StartSlot
	require.NoError(t, f.db.DeleteCommitteeMembersAfterSlot(context.Background(), boundary-1, nil))
	require.NoError(t, f.db.DeleteGovernanceProposalsAfterSlot(context.Background(), boundary-1, nil))
	require.Equal(
		t,
		before,
		answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot),
	)
	// Without the rollback, the same authorization would not survive into the
	// next epoch for a credential that stayed pending.
	require.Equal(t, committeeAnswers{
		colds: []lcommon.Credential{},
	}, answers(result.NewCurrentEpoch.EpochId, boundary))
}

// seatExpiredCommitteeMember seats a cold credential whose term ended before
// the view's epoch. cardano-ledger keeps it in committeeMembers until an
// enacted action removes it, so it is still elected.
func seatExpiredCommitteeMember(
	t *testing.T,
	db *database.Database,
	cold lcommon.Credential,
) {
	t.Helper()
	require.NoError(t, db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      1,
	}}, nil))
}

// gOuroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the exact tagged hot
// credential and does not filter by enacted membership or expiry; only a
// resigned cold credential has no authorization to return. The current
// authorizations are the csCommitteeCreds entries that survive the epoch
// boundary (Conway EPOCH updateCommitteeState).
func TestLedgerViewCommitteeHotCredentialColdCredentialsReturnsEveryAuthorization(
	t *testing.T,
) {
	t.Parallel()

	const epochStartSlot = 100
	pparams := committeeVotingConway.pparams(lcommon.ProtocolVersionVanRossem)
	lv, db := committeeTestView(t, pparams)
	lv.pinCommitteeState(5, pparams)
	lv.epochStartSlot = epochStartSlot
	hot := committeeTestCredential(0x11)
	scriptHot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hot.Credential,
	}

	seated := committeeTestCredential(0x21)
	expired := committeeTestCredential(0x22)
	resignedSeated := committeeTestCredential(0x23)
	movedAway := committeeTestCredential(0x24)
	scriptTwin := committeeTestCredential(0x25)
	seatCommitteeMembers(t, db, seated, resignedSeated, movedAway, scriptTwin)
	seatExpiredCommitteeMember(t, db, expired)
	seedCommitteeCredentialAuthorization(t, db, seated, hot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, expired, hot, 2, 1)
	seedCommitteeCredentialAuthorization(t, db, resignedSeated, hot, 3, 1)
	seedCommitteeCredentialResignation(t, db, resignedSeated, 4, 2)
	seedCommitteeCredentialAuthorization(t, db, movedAway, hot, 5, 1)
	seedCommitteeCredentialAuthorization(
		t, db, movedAway, committeeTestCredential(0x12), 6, 2,
	)
	seedCommitteeCredentialAuthorization(t, db, scriptTwin, scriptHot, 7, 1)

	pending := committeeTestCredential(0x31)
	pendingResigned := committeeTestCredential(0x32)
	pendingLastEpoch := committeeTestCredential(0x33)
	for i, cold := range []lcommon.Credential{
		pending, pendingResigned, pendingLastEpoch,
	} {
		storeCommitteeUpdateProposal(t, db, byte(0x41+i), cold, 10)
	}
	seedCommitteeCredentialAuthorization(
		t, db, pending, hot, 8, epochStartSlot,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingResigned, hot, 9, epochStartSlot,
	)
	seedCommitteeCredentialResignation(
		t, db, pendingResigned, 10, epochStartSlot+1,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingLastEpoch, hot, 11, epochStartSlot-1,
	)

	coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(hot)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]lcommon.Credential{seated, expired, pending},
		coldCredentials,
	)
	coldCredentials, err = lv.CommitteeHotCredentialColdCredentials(scriptHot)
	require.NoError(t, err)
	require.Equal(t, []lcommon.Credential{scriptTwin}, coldCredentials)

	for _, cold := range []lcommon.Credential{seated, expired} {
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		require.True(t, elected)
	}
	elected, err := lv.CommitteeCredentialIsElected(pending)
	require.NoError(t, err)
	require.False(t, elected)
}

// Reference verdicts for one committee hot voter at PV9, PV10 and PV11:
// VotersDoNotExist unless a surviving csCommitteeCreds entry authorizes the
// hot credential (at every version), plus UnelectedCommitteeVoters from PV11
// unless that entry's cold credential is in the enacted committee. Expiry is
// not consulted by GOV.
func TestValidateTxCommitteeVoterVerdictsByProtocolVersion(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	type verdict int
	const (
		accept verdict = iota
		unknown
		unelected
	)
	versions := []struct {
		label string
		major uint
	}{
		{label: "PV9", major: lcommon.ProtocolVersionPlomin - 1},
		{label: "PV10", major: lcommon.ProtocolVersionPlomin},
		{label: "PV11", major: lcommon.ProtocolVersionVanRossem},
	}
	members := []struct {
		name string
		seed func(
			t *testing.T,
			db *database.Database,
			cold, hot lcommon.Credential,
		)
		verdicts [3]verdict
	}{
		{
			name: "seated",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated expired",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatExpiredCommitteeMember(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
				seedCommitteeCredentialResignation(t, db, cold, 2, 2)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending authorized this epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
			},
			verdicts: [3]verdict{accept, accept, unelected},
		},
		{
			name: "pending authorized last epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot-1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
				seedCommitteeCredentialResignation(
					t, db, cold, 2, epochStartSlot+1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, member := range members {
			for i, version := range versions {
				name := fmt.Sprintf(
					"%s/%s/%s",
					era.name,
					member.name,
					version.label,
				)
				want := member.verdicts[i]
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(version.major)
					lv, db := committeeTestView(t, pparams)
					lv.pinCommitteeState(5, pparams)
					lv.epochStartSlot = epochStartSlot
					cold := committeeTestCredential(0x51)
					hot, hotKey := committeeTestVotingKey(0x52)
					member.seed(t, db, cold, hot)

					err := committeeVotingValidate(
						t, era, lv, pparams, hotKey,
						lcommon.VotingProcedures{committeeVoter(hot): {}},
						nil,
					)
					switch want {
					case accept:
						require.NoError(t, err)
					case unknown:
						requireUnknownCommitteeVoter(t, err)
					case unelected:
						var unelectedErr conway.UnelectedCommitteeVoterError
						require.ErrorAs(t, err, &unelectedErr)
					}
				})
			}
		}
	}
}

// A seated committee_member row whose cold hash is not 28 bytes is corrupt
// state. Resolving an unseated authorization consults the seated set, so the
// lookup must fail rather than truncate or zero-pad the stored bytes into a
// credential: a 29-byte row whose first 28 bytes are the pending member's
// hash would otherwise hide that member's authorization, and a short row
// would otherwise be silently ignored. Validation fails closed on the error.
func TestLedgerViewCommitteeHotAuthorizationRejectsMalformedSeatedColdHash(
	t *testing.T,
) {
	t.Parallel()

	for _, length := range []int{
		lcommon.Blake2b224Size - 1,
		lcommon.Blake2b224Size + 1,
	} {
		t.Run(fmt.Sprintf("%d bytes", length), func(t *testing.T) {
			t.Parallel()
			era := committeeVotingConway
			pparams := era.pparams(lcommon.ProtocolVersionPlomin)
			lv, db := committeeTestView(t, pparams)
			pending := committeeTestCredential(0x91)
			hot, hotKey := committeeTestVotingKey(0x92)
			malformed := make([]byte, length)
			copy(malformed, pending.Credential[:])
			require.NoError(t, db.SetCommitteeMembers(context.Background(),
				[]*models.CommitteeMember{{
					ColdCredentialTag: uint8(pending.CredType),
					ColdCredHash:      malformed,
					ExpiresEpoch:      10,
				}},
				nil,
			))
			storeCommitteeUpdateProposal(t, db, 0x93, pending, 10)
			seedCommitteeCredentialAuthorization(t, db, pending, hot, 1, 1)

			member, err := lv.CommitteeHotCredentialMember(hot)
			require.Nil(t, member)
			require.ErrorContains(t, err, "invalid blake2b-224 hash")
			coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(
				hot,
			)
			require.Nil(t, coldCredentials)
			require.ErrorContains(t, err, fmt.Sprintf("got %d", length))

			err = committeeVotingValidate(
				t, era, lv, pparams, hotKey,
				lcommon.VotingProcedures{committeeVoter(hot): {}},
				nil,
			)
			var lookup conway.CommitteeMemberLookupError
			require.ErrorAs(t, err, &lookup)
			require.ErrorContains(t, err, "invalid blake2b-224 hash")
		})
	}
}

// incorrectRefundSubstring is the message
// conway.CertificateRefundIncorrectError renders. These tests match on the
// message rather than on a rule index, which is an offset into an upstream
// slice and moves whenever gouroboros inserts or reorders a rule.
const incorrectRefundSubstring = "incorrect refund for certificate type 17"

const (
	// Deliberately different values. drepRefundTestPparamDeposit is what a
	// *registration* certificate must supply, and is the value a refund
	// would be judged against if the deregistration path fell back to
	// protocol parameters; drepRefundTestRecordedDeposit is what the
	// registration row actually recorded. Keeping them apart is what stops
	// these tests passing for the wrong reason.
	drepRefundTestPparamDeposit   = 1_000_000
	drepRefundTestRecordedDeposit = 500_000_000
)

func drepRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           2_000_000,
		DRepDeposit:          drepRefundTestPparamDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func drepRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// drepDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single DRep deregistration, which is the certificate whose refund
// conway.UtxoValidateCertificateDeposits checks against the deposit the
// ledger state reports for the credential.
func drepDeregistrationTx(
	cred lcommon.Credential,
	refund int64,
) *conway.ConwayTransaction {
	cert := &lcommon.DeregistrationDrepCertificate{
		CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: cred,
		Amount:         refund,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeDeregistrationDrep,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedImportedDrep writes the DRep and registration rows the Mithril
// ledger-state import produces: a registration_drep row with no certificate
// behind it, so certificate_id stays 0 while deposit_amount carries the real
// amount owed. On a bootstrapped node this is frequently a DRep's only
// registration row.
func seedImportedDrep(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit uint64,
	slot uint64,
	active bool,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	hash := append([]byte(nil), cred.Credential[:]...)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: tag,
			Credential:    hash,
			AddedSlot:     slot,
			Active:        active,
		},
		&models.RegistrationDrep{
			CredentialTag:  tag,
			DrepCredential: hash,
			AddedSlot:      slot,
			DepositAmount:  types.Uint64(deposit),
		},
		nil,
	))
}

// TestDRepDeregistrationRefundsRecordedDeposit is the regression test for the
// live rejection this fix addresses. LedgerView.DRepRegistration is the
// common.DRepState gouroboros consults for a DRep deregistration's refund;
// it built a DRepRegistration without assigning Deposit, so every refund was
// judged against zero and a certificate supplying the real deposit was
// rejected with "incorrect refund for certificate type 17: supplied
// 500000000, expected 0".
//
// The assertion is on the validation outcome through the production Conway
// rule with a real *LedgerView, not on the helper's return value, because the
// defect was a plausible internal value becoming the wrong consensus
// decision.
func TestDRepDeregistrationRefundsRecordedDeposit(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd1)
	seedImportedDrep(t, db, cred, drepRefundTestRecordedDeposit, 100, true)
	pp := drepRefundTestPparams()

	// Balanced at the recorded deposit: accepted.
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, drepRefundTestRecordedDeposit),
		200,
		lv,
		pp,
	))

	// A refund of zero is what the defect accepted, and it must now be
	// rejected. This is the assertion that cannot hold both before and
	// after the fix.
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, 0),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)

	// And the recorded value must win over the protocol parameter, so the
	// acceptance above is not a fallback to DRepDeposit that happened to
	// balance.
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(t, reg.Deposit)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *reg.Deposit)
}

// TestDRepRegistrationsReportRecordedDeposits covers the plural view, which
// gouroboros declares on common.DRepState alongside the singular form. It
// reads its deposits through the batched query, so this is what executes
// GetDrepLastRegistrationDeposits' derived-table join.
func TestDRepRegistrationsReportRecordedDeposits(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	active := drepRefundTestCredential(0xd2)
	inactive := drepRefundTestCredential(0xd3)
	seedImportedDrep(t, db, active, drepRefundTestRecordedDeposit, 100, true)
	// Registered once, since deregistered. Its registration history must
	// not appear in a listing of active DReps.
	seedImportedDrep(t, db, inactive, 900_000_000, 101, false)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, active, regs[0].Credential)
	require.NotNil(t, regs[0].Deposit)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *regs[0].Deposit)
}

// TestDRepRegistrationReportsNilForUnregisteredCredential pins the absence
// case the batched map leaves out entirely: a DRep row with no
// registration_drep history reports no deposit rather than an error, through
// both views.
func TestDRepRegistrationReportsNilForUnregisteredCredential(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd4)
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: tag,
		Credential:    append([]byte(nil), cred.Credential[:]...),
		AddedSlot:     100,
		Active:        true,
	}))

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(t, reg.Deposit)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Nil(t, regs[0].Deposit)
}

// inconsistentDepositSubstring is the message
// conway.DRepDepositStateInconsistentError renders when a registration
// reports no recorded deposit. Matched on the message rather than on a rule
// index, which moves whenever gouroboros inserts or reorders a rule.
const inconsistentDepositSubstring = "registered DRep credential has no recorded deposit"

// newDrepFallbackTestView returns a *LedgerView whose published consensus
// snapshot is Conway with drepRefundTestPparams, so the unknown-deposit
// fallback resolves through the same era certificate-deposit function the
// certificate write path uses. newStakeRefundTestView leaves the snapshot
// unpublished and the era Shelley, which is the "current era charges no DRep
// deposit" case rather than this one.
func newDrepFallbackTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = drepRefundTestPparams()
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

// seedActiveDrepWithoutRegistration reproduces the state the vote-replay
// recovery path leaves behind. ledger/governance/processing.go calls
// InsertDrepIfAbsent when a valid DRep vote proves the credential exists
// on-chain but the metadata row was lost during recovery or bootstrap; that
// writes an active drep row and no registration_drep row at all, so the
// credential is registered with no recorded deposit.
func seedActiveDrepWithoutRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	slot uint64,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.InsertDrepIfAbsent(context.Background(),
		tag,
		cred.Credential[:],
		slot,
		"",
		nil,
		true,
		nil,
	))
	// The recovery path really does leave no registration row behind; if it
	// ever starts writing one this test is measuring the wrong thing.
	recorded, err := db.GetDrepLastRegistrationDeposit(context.Background(),
		tag,
		cred.Credential[:],
		nil,
	)
	require.NoError(t, err)
	require.Nil(
		t,
		recorded,
		"the recovery path must leave no recorded deposit for this test to exercise the fallback",
	)
}

// TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded is the
// regression test. An active DRep with no registration_drep row reported
// Deposit == nil, and gouroboros fails closed on that
// (DRepDepositStateInconsistentError), so the deregistration was rejected and
// a node reaching that block stopped making progress. The refund is now
// judged against the DRep deposit the current protocol parameters charge,
// which is what the certificate write path would have recorded.
func TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded(
	t *testing.T,
) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe1)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	pp := drepRefundTestPparams()
	tx := drepDeregistrationTx(cred, drepRefundTestPparamDeposit)
	// The rule is invoked directly so a nil error means the refund
	// comparison actually ran and matched, rather than that the substring
	// was absent because an unrelated rule failed first.
	require.NoError(
		t,
		conway.UtxoValidateCertificateDeposits(tx, 200, lv, pp),
	)
	if err := eras.ValidateTxConway(tx, 200, lv, pp); err != nil {
		require.NotContains(t, err.Error(), inconsistentDepositSubstring)
		require.NotContains(t, err.Error(), incorrectRefundSubstring)
	}

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"an unrecorded deposit must be reported as the current parameter, not as absence",
	)
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *reg.Deposit)
}

// TestDrepDeregistrationKeepsRecordedZeroAuthoritative pins the distinction
// the fallback must preserve. A zero dRepDeposit is a legitimate
// configuration, so a recorded zero is a real value: folding it into the
// unrecorded case would refund the current parameter and reject a valid
// deregistration.
func TestDrepDeregistrationKeepsRecordedZeroAuthoritative(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe4)
	seedImportedDrep(t, db, cred, 0, 100, true)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"a recorded zero must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *reg.Deposit)

	pp := drepRefundTestPparams()
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, 0),
		200,
		lv,
		pp,
	))
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)
}

// TestDrepRegistrationsFallBackForUnrecordedDeposit covers the plural view,
// which builds its deposits from a batched map lookup and so has its own
// absence path.
func TestDrepRegistrationsFallBackForUnrecordedDeposit(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	unrecorded := drepRefundTestCredential(0xe5)
	recorded := drepRefundTestCredential(0xe6)
	seedActiveDrepWithoutRegistration(t, db, unrecorded, 100)
	seedImportedDrep(t, db, recorded, drepRefundTestRecordedDeposit, 101, true)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	byCredential := map[string]*uint64{}
	for _, reg := range registrations {
		byCredential[string(reg.Credential.Credential[:])] = reg.Deposit
	}

	got := byCredential[string(unrecorded.Credential[:])]
	require.NotNil(t, got, "the plural view must not report absence either")
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *got)

	got = byCredential[string(recorded.Credential[:])]
	require.NotNil(t, got)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *got)
}

// TestDrepRegistrationReportsAbsenceInAPreConwayEra keeps the fallback from
// inventing a refund where the current era charges no DRep deposit.
//
// The era has to be published for this to mean anything. CertDepositShelley
// through CertDepositBabbage have no *RegistrationDrepCertificate case and
// fall through to "default: return 0, nil", so before the drepDepositParams
// guard this reported a non-nil zero and gouroboros accepted a zero refund
// instead of failing closed.
func TestDrepRegistrationReportsAbsenceInAPreConwayEra(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = &babbage.BabbageProtocolParameters{
		KeyDeposit: 2_000_000,
		MaxTxSize:  16_384,
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	// The era's own deposit function really does answer zero-without-error
	// for a DRep registration, which is what makes the guard load-bearing
	// rather than defensive.
	deposit, err := eras.BabbageEraDesc.CertDepositFunc(
		&lcommon.RegistrationDrepCertificate{},
		ls.currentPParams,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), deposit)

	cred := drepRefundTestCredential(0xe7)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(
		t,
		reg.Deposit,
		"a pre-Conway era has no DRep deposit to fall back to; absence must be reported",
	)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Nil(
		t,
		registrations[0].Deposit,
		"the plural view must report absence in a pre-Conway era too",
	)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// TestDrepRegistrationReportsAbsenceForTypedNilParams covers the other way the
// capability assertion can be satisfied without a usable deposit behind it.
//
// A typed-nil *DijkstraProtocolParameters implements drepDepositParams, so the
// call-site guard admits it; CertDepositDijkstra then asserted the type
// successfully and dereferenced nil, panicking inside DRep view construction.
// It now reports ErrIncompatibleProtocolParams, which currentDRepDeposit turns
// into absence, so gouroboros fails closed as it does for every other
// no-deposit-available case.
//
// The two guards are complementary and both are asserted here: the era helper
// is what stops the panic, and the call-site capability check is what keeps a
// pre-Conway era from reaching it at all.
func TestDrepRegistrationReportsAbsenceForTypedNilParams(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = (*dijkstra.DijkstraProtocolParameters)(nil)
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	// The typed nil really does satisfy the capability the call-site guard
	// tests, so this case reaches the era helper rather than stopping early.
	_, implements := ls.currentPParams.(drepDepositParams)
	require.True(
		t,
		implements,
		"a typed-nil pointer must still satisfy drepDepositParams for this test to exercise the era helper",
	)

	cred := drepRefundTestCredential(0xe9)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	require.NotPanics(t, func() {
		reg, err := lv.DRepRegistration(cred)
		require.NoError(t, err)
		require.NotNil(t, reg)
		require.Nil(
			t,
			reg.Deposit,
			"unusable parameters must report absence, not a fabricated deposit",
		)
	})
	require.NotPanics(t, func() {
		registrations, err := lv.DRepRegistrations()
		require.NoError(t, err)
		require.Len(t, registrations, 1)
		require.Nil(t, registrations[0].Deposit)
	})

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

func epochBoundaryBenchStart(epoch uint64) uint64 {
	return epoch * epochBoundaryBenchEpochLength
}

// TestBoundaryPromotesRewardAfterReregistration pins the other eligibility
// transition: a row made nonspendable by a deregistration before precompute is
// credited when the credential registers and delegates again before boundary.
func TestBoundaryPromotesRewardAfterReregistration(t *testing.T) {
	t.Parallel()
	type outcome struct {
		storedReward uint64
		balance      *uint64
		treasury     uint64
		amount       uint64
		spendable    bool
	}
	run := func(reregister bool) outcome {
		f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
		key := epochBoundaryBenchHash(0x30, 1)
		raw, err := dbtest.RawSQLiteMetadata(t, f.db)
		require.NoError(t, err)
		var pool []byte
		require.NoError(t, raw.QueryRow(
			`SELECT pool FROM account WHERE credential_tag = 0 AND staking_key = ?`,
			key,
		).Scan(&pool))
		deregisterSlot := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch) + 1_000
		_, err = raw.Exec(`
UPDATE account SET active = 0, pool = NULL, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`, deregisterSlot, key)
		require.NoError(t, err)
		_, err = raw.Exec(`
UPDATE reward_live_stake SET registered = 0, pool_key_hash = NULL,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
			deregisterSlot, key,
		)
		require.NoError(t, err)
		_, err = raw.Exec(`
INSERT INTO deregistration (added_slot, staking_key, credential_tag, amount)
VALUES (?, ?, 0, '2000000')`, deregisterSlot, key)
		require.NoError(t, err)
		require.NoError(t, raw.Close())

		require.NoError(t, f.ls.precomputeStakeRewardsAfterEpochTransition(
			epochBoundaryBenchPrecomputeEvent(),
		))
		precomputed, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
		require.NoError(t, err)
		var targetAmount uint64
		for _, output := range precomputed {
			if string(output.StakingKey) == string(key) {
				require.False(t, output.Spendable,
					"precompute observes the deregistered account")
				targetAmount += uint64(output.Amount)
			}
		}
		require.Positive(t, targetAmount)

		if reregister {
			raw, err = dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			registerSlot := deregisterSlot + 1_000
			_, err = raw.Exec(`
UPDATE account SET active = 1, pool = ?, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`, pool, registerSlot, key)
			require.NoError(t, err)
			_, err = raw.Exec(`
UPDATE reward_live_stake SET registered = 1, pool_key_hash = ?,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
				pool, registerSlot, key,
			)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, ?)`, key, registerSlot)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, ?)`, key, pool, registerSlot)
			require.NoError(t, err)
			require.NoError(t, raw.Close())
		}

		f.rollover(t)
		f.ls.waitEpochBoundaryBenchBackground()
		account, err := f.db.GetAccountByCredential(context.Background(), 0, key, true, nil)
		require.NoError(t, err)
		var balance *uint64
		if account.Active {
			balance, err = (&LedgerView{ls: f.ls}).RewardAccountBalance(
				lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						lcommon.NewBlake2b224(key),
					),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, balance)
		}
		outputs, err := f.db.Metadata().GetRewardAccountOutputs(8, nil)
		require.NoError(t, err)
		var spendable bool
		for _, output := range outputs {
			if string(output.StakingKey) == string(key) {
				spendable = output.Spendable
			}
		}
		state, err := f.db.Metadata().GetNetworkState(nil)
		require.NoError(t, err)
		return outcome{
			storedReward: uint64(account.Reward), balance: balance,
			treasury: uint64(state.Treasury),
			amount:   targetAmount, spendable: spendable,
		}
	}

	deregistered := run(false)
	reregistered := run(true)
	require.False(t, deregistered.spendable)
	require.True(t, reregistered.spendable)
	baseReward := stakeRewardSeedReward(string(epochBoundaryBenchHash(0x30, 1)))
	require.Equal(t, baseReward, deregistered.storedReward)
	require.Equal(t, baseReward, reregistered.storedReward,
		"the boundary keeps deferred credits out of account.reward")
	require.Nil(t, deregistered.balance)
	require.NotNil(t, reregistered.balance)
	require.Equal(t, baseReward+reregistered.amount, *reregistered.balance)
	require.Equal(t, deregistered.treasury,
		reregistered.treasury+reregistered.amount,
		"the re-registered reward moves from treasury to the account")
}

const musashiGenesisCommitteeExpiry = 293

func TestGenesisCommitteeStateUnavailableWithoutHistory(t *testing.T) {
	t.Parallel()
	ls, db := genesisConstitutionTestState(t)
	for _, cfg := range []*cardano.CardanoNodeConfig{
		ls.config.CardanoNodeConfig,
		{},
		nil,
	} {
		ls.config.CardanoNodeConfig = cfg
		require.Zero(t, committeeMemberRowCount(t, db))
		available, err := ls.NewView(nil).CommitteeStateAvailable()
		require.NoError(t, err)
		require.False(t, available)
	}
}

func TestEmptyGenesisCommitteeLookupFailure(t *testing.T) {
	t.Parallel()
	ls, _ := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = nil
	wantErr := errors.New("committee storage unavailable")
	ls.db = newStorageFaultTestDB(t, errInjectingMetadataStore{
		getCommitteeMembersErr: wantErr,
	})
	available, err := ls.NewView(nil).CommitteeStateAvailable()
	require.ErrorIs(t, err, wantErr)
	require.False(t, available)
}

func TestEmptyGenesisCommitteeStateAvailable(t *testing.T) {
	t.Parallel()

	for _, members := range []map[string]int{nil, {}} {
		name := "empty map"
		if members == nil {
			name = "nil map"
		}
		t.Run(name, func(t *testing.T) {
			ls, db := genesisConstitutionTestState(t)
			ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = members
			for range 2 {
				require.NoError(t, ls.createGenesisBlock(context.Background()))
				require.Zero(t, committeeMemberRowCount(t, db))
				available, err := ls.NewView(nil).CommitteeStateAvailable()
				require.NoError(t, err)
				require.True(
					t,
					available,
					"empty genesis committee is authoritative",
				)
			}
		})
	}
}

// TestCreateGenesisBlockSeedsCommittee proves a node initialized from Conway
// genesis recognizes every genesis Constitutional Committee member for
// hot-key authorization. Without the seed a
// genesis member never touched by an UpdateCommittee action has no row at
// all, and AuthCommitteeHot/ResignCommitteeCold validation rejects it as
// "not a CC member" even though the real chain has recognized it since the
// hard fork.
func TestCreateGenesisBlockSeedsCommittee(t *testing.T) {
	t.Parallel()

	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	lv := &LedgerView{ls: ls}
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys {
		coldKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		member, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(coldKey),
		})
		require.NoError(t, err)
		require.NotNil(
			t,
			member,
			"genesis committee member %s must resolve",
			coldKeyHex,
		)
		require.False(t, member.Resigned)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			member.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockCommitteeEnactmentWins proves a real UpdateCommittee
// enactment for a genesis cold credential outranks the genesis seed, and
// that a later genesis initialization pass does not revert it back to the
// genesis term -- the hazard a naive unconditional reseed on every startup
// would create.
func TestCreateGenesisBlockCommitteeEnactmentWins(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	coldKey, err := hex.DecodeString(musashiGenesisCommitteeColdKeys[0])
	require.NoError(t, err)
	require.NoError(t, db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{
		{
			ColdCredentialTag: 0,
			ColdCredHash:      coldKey,
			ExpiresEpoch:      999,
			TermStartSlot:     100,
			TermStartSlotSet:  true,
			AddedSlot:         100,
		},
	}, nil))

	ls.currentTip.Point = ocommon.Point{Slot: 200}
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	lv := &LedgerView{ls: ls}
	member, err := lv.CommitteeCredentialMember(lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(coldKey),
	})
	require.NoError(t, err)
	require.NotNil(t, member)
	require.Equal(t, uint64(999), member.ExpiryEpoch)

	// The other two genesis members are untouched and still resolve.
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys[1:] {
		otherKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		other, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(otherKey),
		})
		require.NoError(t, err)
		require.NotNil(t, other)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			other.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockConstitutionEnactmentWins proves an enacted
// NewConstitution action outranks the slot-0 genesis seed, and that a later
// genesis initialization pass does not restore the genesis constitution over
// it.
func TestCreateGenesisBlockConstitutionEnactmentWins(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	enactedAnchor := bytes.Repeat([]byte{0xe1}, lcommon.Blake2b256Size)
	enactedScript := bytes.Repeat([]byte{0xe2}, lcommon.Blake2b224Size)
	require.NoError(t, db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/enacted",
		AnchorHash: enactedAnchor,
		PolicyHash: enactedScript,
		AddedSlot:  100,
	}, nil))

	ls.currentTip.Point = ocommon.Point{Slot: 200}
	require.NoError(t, ls.createGenesisBlock(context.Background()))

	lv := &LedgerView{ls: ls}
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/enacted", got.Anchor.Url)
	require.Equal(t, enactedAnchor, got.Anchor.DataHash[:])
	require.Equal(t, enactedScript, got.ScriptHash)

	require.NoError(t, constitutionTestGuardrails(t, lv, enactedScript))
}

const treasuryRolloverGenesisHash = "0101010101010101010101010101010101010101010101010101010101010101"

type treasuryRolloverFixture struct {
	ls             *LedgerState
	db             *database.Database
	currentEpoch   models.Epoch
	currentPParams *conway.ConwayProtocolParameters
	hotCredential  []byte
}

func newTreasuryRolloverFixture(
	t *testing.T,
	treasury uint64,
) *treasuryRolloverFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	cfg.ShelleyGenesis().MaxLovelaceSupply = 45_000_000_000_000_000
	currentEpoch := newTestEpoch(5, 500, 100, eras.ConwayEraDesc.Id)
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot,
		currentEpoch.EpochId,
		currentEpoch.Nonce,
		currentEpoch.EvolvingNonce,
		currentEpoch.CandidateNonce,
		currentEpoch.LastEpochBlockNonce,
		currentEpoch.EraId,
		currentEpoch.SlotLength,
		currentEpoch.LengthInSlots,
		nil,
	))
	require.NoError(t, db.Metadata().SetNetworkState(treasury, 1_000, 499, nil))

	pparams := donationTestConwayPParams(10)
	pparams.MinCommitteeSize = 1
	pparams.DRepVotingThresholds.TreasuryWithdrawal = cbor.Rat{
		Rat: big.NewRat(0, 1),
	}

	coldCredential := repeatByte(28, 0xc1)
	hotCredential := repeatByte(28, 0xc2)
	require.NoError(t, db.SetCommitteeMembers(context.Background(), []*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: currentEpoch.EpochId + 20,
		AddedSlot:    1,
	}}, nil))
	require.NoError(t, db.SetCommitteeQuorum(context.Background(), big.NewRat(1, 1), 1, nil))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, coldCredential, hotCredential, 1, 1)
	require.NoError(t, err)

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger: slog.New(slog.NewJSONHandler(
				io.Discard,
				nil,
			)),
		},
	}
	return &treasuryRolloverFixture{
		ls:             ls,
		db:             db,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		hotCredential:  hotCredential,
	}
}

func (f *treasuryRolloverFixture) rollover(
	t *testing.T,
	currentEpoch models.Epoch,
	currentPParams lcommon.ProtocolParameters,
) *EpochRolloverResult {
	t.Helper()
	seedEmptyRewardBasisForRollover(t, f.db, currentEpoch, currentPParams)
	var result *EpochRolloverResult
	txn := f.db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		var rolloverErr error
		result, rolloverErr = f.ls.processEpochRollover(context.Background(),
			txn,
			currentEpoch,
			eras.ConwayEraDesc,
			currentPParams,
			false,
		)
		return rolloverErr
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

func (f *treasuryRolloverFixture) proposal(
	t *testing.T,
	proposal *models.GovernanceProposal,
) *models.GovernanceProposal {
	t.Helper()
	loaded, err := f.db.GetGovernanceProposal(context.Background(),
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// donationTestConwayPParams builds Conway pparams with voting thresholds so
// governance.ProcessEpoch's ratification phase has the fields it reads.
func donationTestConwayPParams(major uint) *conway.ConwayProtocolParameters {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = major
	p.NOpt = 500
	p.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	p.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	p.Tau = &cbor.Rat{Rat: big.NewRat(1, 5)}
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

// TestEpochProcessWithdrawalThenDonation drives the real Conway enactment path:
// a ratified treasury withdrawal is enacted by governance.ProcessEpoch and then
// the ending epoch's donation is applied, exactly as processEpochRollover
// sequences them. It proves the withdrawal is checked/applied against the
// pre-donation treasury (the value the ledger uses at the boundary) and the
// donation is added afterwards.
func TestEpochProcessWithdrawalThenDonation(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	const (
		initialTreasury = uint64(1_000)
		initialReserves = uint64(200)
		withdrawal      = uint64(400)
		donation        = uint64(300)
		endedEpoch      = uint64(4)
		boundarySlot    = uint64(500)
	)

	// Registered reward account that the withdrawal pays out to.
	stakeCred := make([]byte, 28)
	for i := range stakeCred {
		stakeCred[i] = 0x42
	}
	withdrawAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	withdrawAddrBytes, err := withdrawAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))

	// A ratified treasury-withdrawal proposal so ProcessEpoch enacts it.
	withdrawalCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: map[*lcommon.Address]uint64{&withdrawAddr: withdrawal},
	})
	require.NoError(t, err)
	ratifiedEpoch := endedEpoch
	ratifiedSlot := uint64(400)
	require.NoError(t, db.SetGovernanceProposal(context.Background(), &models.GovernanceProposal{
		TxHash:        make([]byte, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/withdrawal",
		AnchorHash:    make([]byte, 32),
		Deposit:       0,
		ReturnAddress: withdrawAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     101,
	}, nil))

	// Initial treasury/reserves and the ending epoch's donation.
	require.NoError(t, db.Metadata().SetNetworkState(
		initialTreasury, initialReserves, 1, nil,
	))
	require.NoError(t, db.Metadata().AddNetworkDonation(
		70, endedEpoch, donation, nil,
	))

	runBoundary := func() uint64 {
		t.Helper()
		var providerTreasury uint64
		txn := db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			if _, err := governance.ProcessEpoch(context.Background(), &governance.EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    endedEpoch,
				NewEpoch:     endedEpoch + 1,
				BoundarySlot: boundarySlot,
				PParams:      donationTestConwayPParams(10),
				UpdateFn: func(
					p lcommon.ProtocolParameters, _ any,
				) (lcommon.ProtocolParameters, error) {
					return p, nil
				},
			}); err != nil {
				return err
			}
			if err := ls.applyEpochDonations(
				txn,
				endedEpoch,
				boundarySlot,
			); err != nil {
				return err
			}
			var err error
			providerTreasury, err = ls.NewView(txn).TreasuryValue()
			return err
		}))
		return providerTreasury
	}

	require.Equal(t, uint64(900), runBoundary())

	// Withdrawal (400) was applied against the pre-donation treasury (1000),
	// then the donation (300) was added: 1000 - 400 + 300 = 900.
	treasury, reserves, _ := networkState(t, db)
	assert.Equal(t, uint64(900), treasury,
		"treasury = initial - withdrawal + donation")
	assert.Equal(t, initialReserves, reserves)

	// The withdrawal credited the registered reward account.
	account, err := db.GetAccountByCredential(context.Background(), 0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, withdrawal, uint64(account.Reward),
		"withdrawal paid to the reward account")

	// A crash between the boundary transaction and the tip advance replays
	// the boundary after reward application rewrites the absolute pot row.
	// The enacted proposal and reward credit are replay-idempotent, while the
	// provider must still expose the same post-withdrawal, post-donation value.
	require.NoError(t, db.Metadata().SetNetworkState(
		initialTreasury,
		initialReserves,
		boundarySlot,
		nil,
	))
	require.Equal(t, uint64(900), runBoundary())
	requireTreasuryValue(t, ls, nil, 900)

	account, err = db.GetAccountByCredential(context.Background(), 0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, withdrawal, uint64(account.Reward),
		"boundary replay must not double-credit the withdrawal")

	// Rewinding before both the donation block and boundary restores the
	// earlier pot row. These are the same slot-keyed deletes used by the
	// database rollback path.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(context.Background(), 1, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(context.Background(), 1, nil))
	requireTreasuryValue(t, ls, nil, initialTreasury)
}

// errHorizonProbeDone stops ledgerProcessBlock right after the probe has run,
// so the assertion is about the LedgerView it was handed rather than about
// everything block application does afterwards.
var errHorizonProbeDone = errors.New("horizon probe complete")

// TestLedgerProcessBlockAnchorsValidationHorizonAtParent proves the anchor is
// actually wired from block application, not merely available on LedgerView.
// The reference implementation ticks from the applied block's immediate
// predecessor, so that predecessor — not the published tip, which lags by a
// whole block batch during replay — is what the safe zone must be measured
// from.
func TestLedgerProcessBlockAnchorsValidationHorizonAtParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parentSlot uint64
		wantErr    error
	}{
		{
			// Block 168145's real predecessor on Preview is block 168144 at
			// slot 3516496, so this is the case that has to succeed.
			name:       "applied predecessor",
			parentSlot: previewParentSlot,
		},
		{
			// The published tip trails by one block. Before the fix this was
			// the only anchor available, and it rejected the block.
			name:       "published tip",
			parentSlot: previewPublishedTipSlot,
			wantErr:    hardfork.ErrPastHorizon,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newTestDB(t)
			ls := previewWedgeLedgerState(t)
			ls.db = db

			var probeErr error
			var probed bool
			testEra := eras.BabbageEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				view lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				lv, ok := view.(*LedgerView)
				require.True(t, ok,
					"block application must hand the era validator the "+
						"LedgerView that carries the horizon anchor")
				probed = true
				_, probeErr = lv.SlotToTime(previewTxUpperBound)
				return errHorizonProbeDone
			}
			ls.activeEras = []eras.EraDesc{testEra}

			blocks, err := omockfixtures.GenerateBabbageChain(
				168_145, lcommon.Blake2b256{}, previewBlockSlot, 1, 1,
			)
			require.NoError(t, err)
			block, ok := blocks[0].(*babbage.BabbageBlock)
			require.True(t, ok)
			block.TransactionBodies = []babbage.BabbageTransactionBody{{}}
			block.TransactionWitnessSets = []babbage.BabbageTransactionWitnessSet{
				{},
			}
			pparams := &babbage.BabbageProtocolParameters{
				ProtocolMajor:      8,
				MaxBlockBodySize:   100_000,
				MaxBlockHeaderSize: 100_000,
			}
			processErr := db.Transaction(context.Background(), true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(context.Background(),
						txn,
						ocommon.NewPoint(
							previewBlockSlot,
							block.Hash().Bytes(),
						),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{
							slot:        test.parentSlot,
							blockNumber: 168_144,
						},
						&database.BlockIngestionResult{},
						testEra,
						pparams,
						nil,
						previewEraStartEpoch,
						0,
						false,
					)
					return err
				})
			require.ErrorIs(t, processErr, errHorizonProbeDone)
			require.True(t, probed)
			if test.wantErr != nil {
				require.ErrorIs(t, probeErr, test.wantErr)
				return
			}
			require.NoError(t, probeErr,
				"the block that wedged the Preview replay must convert its "+
					"Plutus validity bound")
		})
	}
}

// newPPUPWindowLedgerState builds a ledger whose Shelley genesis carries the
// given security parameter and active-slot coefficient and whose epoch cache
// holds epochs.
func newPPUPWindowLedgerState(
	t *testing.T,
	securityParam int,
	activeSlotsCoeff *big.Rat,
	epochs []models.Epoch,
) *LedgerState {
	t.Helper()
	cfg := newGenesisDelegateShelleyGenesisCfg(
		t,
		strings.Repeat("aa", lcommon.Blake2b224Size),
		strings.Repeat("bb", lcommon.Blake2b256Size),
	)
	genesis := cfg.ShelleyGenesis()
	genesis.SecurityParam = securityParam
	genesis.ActiveSlotsCoeff = cbor.Rat{Rat: activeSlotsCoeff}
	ls := &LedgerState{}
	ls.config.CardanoNodeConfig = cfg
	ls.consensus.Store(&consensusSnapshot{epochCache: epochs})
	return ls
}

// The reference slot of no return is
// epochInfoFirst (succ e) *- Duration (2 * stabilityWindow), with
// stabilityWindow = computeStabilityWindow k f = ceiling (3k/f)
// (cardano-ledger Cardano.Ledger.Slot.getTheSlotOfNoReturn and
// Cardano.Ledger.Shelley.StabilityWindow). When 3k/f is not an integer,
// 2 * ceiling (3k/f) is larger than floor (6k/f).
func TestProtocolParameterUpdateWindowMatchesReferenceSlotOfNoReturn(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		securityParam    int
		activeSlotsCoeff *big.Rat
		epoch            models.Epoch
		noReturn         uint64
	}{
		{
			// Mainnet and preprod: 2 * 3 * 2160 / 0.05 = 259200.
			name:             "mainnet first Shelley epoch",
			securityParam:    2160,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 208, StartSlot: 4_492_800, LengthInSlots: 432_000},
			noReturn:         4_492_800 + 432_000 - 259_200,
		},
		{
			// Preview: 2 * 3 * 432 / 0.05 = 51840.
			name:             "preview",
			securityParam:    432,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 700, StartSlot: 60_480_000, LengthInSlots: 86_400},
			noReturn:         60_480_000 + 86_400 - 51_840,
		},
		{
			// 3k/f = 30/7: ceiling 5, so 2 * 5 = 10 where floor (60/7) = 8.
			name:             "fractional window below one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 10),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 100},
			noReturn:         600 - 10,
		},
		{
			// 3k/f = 300/7: ceiling 43, so 2 * 43 = 86 where floor (600/7) = 85.
			name:             "fractional window above one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 100),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 200},
			noReturn:         700 - 86,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := newPPUPWindowLedgerState(
				t,
				tc.securityParam,
				tc.activeSlotsCoeff,
				[]models.Epoch{tc.epoch},
			)
			view := &LedgerView{ls: ls}
			for _, slot := range []uint64{
				tc.epoch.StartSlot,
				tc.noReturn - 1,
				tc.noReturn,
				tc.epoch.StartSlot + uint64(tc.epoch.LengthInSlots) - 1,
			} {
				epoch, noReturn, err := view.ProtocolParameterUpdateWindow(slot)
				require.NoError(t, err, "slot %d", slot)
				require.Equal(t, tc.epoch.EpochId, epoch, "slot %d", slot)
				require.Equal(t, tc.noReturn, noReturn, "slot %d", slot)
			}
		})
	}
}

// valueNotConservedSubstring is the message
// shelley.ValueNotConservedUtxoError renders. These tests match on that
// message and never on the rule index: the index is an offset into the
// upstream gouroboros slice and moves whenever upstream inserts or reorders a
// rule. It printed as 32 on v0.202.5 and prints as 33 on the currently pinned
// v0.202.6, which inserted UtxoValidateCurrentTreasuryValue at index 0.
const valueNotConservedSubstring = "value not conserved"

const stakeRefundTestKeyDeposit = 2_000_000

// stakeRefundTestPparams returns Conway protocol parameters whose KeyDeposit
// is the value a legacy stake deregistration falls back to when the ledger
// state cannot report the deposit recorded at registration.
func stakeRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           stakeRefundTestKeyDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// stakeDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single legacy stake deregistration and no inputs or outputs, so value
// conservation reduces to "refund must equal fee". The refund is the only
// consumed value and the fee is the only produced value, which isolates the
// recorded-deposit lookup from every other term in the equation.
func stakeDeregistrationTx(
	cred lcommon.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	cert := &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: cred,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeStakeDeregistration,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedStakeRegistration drives a stake registration through the production
// certificate write path, which is what decides whether the recorded deposit
// lands in the database as a value or as NULL. Passing a nil deposit omits the
// certificate index from the certDeposits map exactly as
// ledger.calculateCertificateDeposit and backfill.calculateCertDeposits do
// when the deposit cannot be computed.
func seedStakeRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit *uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{seed}, 32))
	builder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{seed + 1}, 32),
		0,
	)
	require.NoError(t, err)
	builder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder.WithOutputs(output)
	builder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: cred,
	})
	tx, err := builder.Build()
	require.NoError(t, err)
	certDeposits := map[int]uint64{}
	if deposit != nil {
		certDeposits[0] = *deposit
	}
	require.NoError(t, db.SetTransactionMetadataOnly(context.Background(),
		tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed + 2}, 32)),
		0,
		certDeposits,
		nil,
	))
}

// newStakeRefundTestView returns a *LedgerView over a real database, built
// from the same *LedgerState the other end-to-end validation tests use so the
// Conway rules that read genesis configuration (network ids, slot
// conversion) run rather than panic.
func newStakeRefundTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	return &LedgerView{ls: ls}, db
}

func stakeRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// requireValueConserved asserts the transaction clears value conservation
// through the production Conway validation path. Other rules error on these
// deliberately minimal transactions (the input set is empty by design), so the
// assertion is on the absence of the value-conservation failure specifically,
// which is what the recorded-deposit refund decides.
func requireValueConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	pp := stakeRefundTestPparams()
	// Invoke the rule directly first. This assertion cannot pass vacuously:
	// a nil error means value conservation actually ran and balanced, rather
	// than merely that the substring was absent because some unrelated rule
	// failed first and short-circuited the message.
	require.NoError(
		t,
		conway.UtxoValidateValueNotConservedUtxo(tx, 200, lv, pp),
	)
	// Then assert the same outcome through the production path.
	if err := eras.ValidateTxConway(tx, 200, lv, pp); err != nil {
		require.NotContains(t, err.Error(), valueNotConservedSubstring)
	}
}

func requireValueNotConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	pp := stakeRefundTestPparams()
	// The rule itself must reject, so the rejection is attributable to value
	// conservation rather than to any other rule the production path joins.
	require.ErrorContains(
		t,
		conway.UtxoValidateValueNotConservedUtxo(tx, 200, lv, pp),
		valueNotConservedSubstring,
	)
	err := eras.ValidateTxConway(tx, 200, lv, pp)
	require.Error(t, err)
	require.Contains(t, err.Error(), valueNotConservedSubstring)
}

// TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit is the
// A registration ingested without a computable
// deposit records NULL, LedgerView.StakeCredentialDeposit reports absence, and
// gouroboros' UtxoValidateValueNotConservedUtxo falls back to the current
// KeyDeposit. Before the fix the three zero-reporting sites stored an
// authoritative 0, the rule refunded 0, and this otherwise valid transaction
// failed value conservation.
//
// The assertion is on acceptance through eras.ValidateTxConway with a real
// *LedgerView, not on the helper's return value, because the defect was that
// a plausible internal value became the wrong validation outcome.
func TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit(
	t *testing.T,
) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc1)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc1)

	// The refund falls back to KeyDeposit, so a fee of exactly KeyDeposit
	// conserves value. This acceptance is the assertion that carries the
	// regression: it is the validation outcome, one layer above the recorded
	// value that produces it.
	requireValueConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)

	// Supporting evidence for why the acceptance holds: the recorded deposit
	// is genuinely absent rather than a zero that happened to balance.
	recorded, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	assert.Nil(
		t,
		recorded,
		"an uncomputable registration deposit must be recorded as absent, not zero",
	)
}

// TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit is the second
// mandatory negative case: a correctly recorded non-zero deposit must be
// refunded at its recorded value, never at the current KeyDeposit. The
// recorded 5 ADA deliberately differs from the 2 ADA KeyDeposit, so the two
// possible refunds give opposite outcomes and the test cannot pass by
// accident.
func TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit(
	t *testing.T,
) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc3)
	recordedDeposit := uint64(5_000_000)
	require.NotEqual(
		t,
		uint64(stakeRefundTestKeyDeposit),
		recordedDeposit,
		"the recorded deposit must differ from KeyDeposit for this test to discriminate",
	)
	seedStakeRegistration(t, db, cred, &recordedDeposit, 100, 0xc3)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, recordedDeposit, *got)

	// Balanced at the recorded deposit: accepted.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, recordedDeposit))
	// Balanced at the current KeyDeposit instead: rejected, which is what
	// proves the recorded value won.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}

// TestValueConservationRefundsRecordedZeroStakeDepositAsZero pins the
// distinction the fix must preserve. A recorded zero is reachable and
// authoritative: config/cardano/devnet/shelley-genesis.json sets
// "keyDeposit": 0, so every stake registration on dingo's own devnet records
// a real zero deposit. Folding zero into the unknown case would refund
// KeyDeposit there and break value conservation on the devnet, which is why
// only the uncomputable case reports absence.
func TestValueConservationRefundsRecordedZeroStakeDepositAsZero(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc4)
	recordedZero := uint64(0)
	seedStakeRegistration(t, db, cred, &recordedZero, 100, 0xc4)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(
		t,
		got,
		"a recorded zero deposit must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *got)

	// Refunded as zero, so a zero fee conserves value.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, 0))
	// And the KeyDeposit fallback must not be taken.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}

// TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal covers
// the query-boundary filter: when synthetic is true, the returned value
// omits PlutusV2 while every other key survives, and the original pparams
// (still reachable from internal validation state) is never mutated.
func TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal(
	t *testing.T,
) {
	t.Parallel()

	original := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: {2, 2, 2},
			2: {3, 3, 3},
		},
	}

	filtered := withoutSyntheticV2CostModel(original, true, nil)

	fp, ok := filtered.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, fp.CostModels, uint(1))
	assert.Equal(t, []int64{1, 1, 1}, fp.CostModels[0])
	assert.Equal(t, []int64{3, 3, 3}, fp.CostModels[2])

	// The original, still reachable from ls.currentPParams / the published
	// snapshot for internal script validation, must be untouched.
	assert.Contains(t, original.CostModels, uint(1))
	assert.Equal(t, []int64{2, 2, 2}, original.CostModels[1])
}

// TestWithoutSyntheticV2CostModel_CoversEveryEraType covers
// PR review: the filter's type switch must handle
// every era type ShelleyCurrentProtocolParamsQuery can actually return
// (Alonzo, Babbage, Conway, Dijkstra), not just Conway -- a regression in
// any branch would otherwise pass the suite silently.
func TestWithoutSyntheticV2CostModel_CoversEveryEraType(t *testing.T) {
	t.Parallel()

	costModels := map[uint][]int64{0: {1}, 1: {2}, 2: {3}}

	t.Run("Alonzo", func(t *testing.T) {
		pp := &alonzo.AlonzoProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*alonzo.AlonzoProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Babbage", func(t *testing.T) {
		pp := &babbage.BabbageProtocolParameters{
			CostModels: cloneMap(costModels),
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*babbage.BabbageProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Conway", func(t *testing.T) {
		pp := &conway.ConwayProtocolParameters{CostModels: cloneMap(costModels)}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*conway.ConwayProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
	t.Run("Dijkstra", func(t *testing.T) {
		pp := &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: conway.ConwayProtocolParameters{
				CostModels: cloneMap(costModels),
			},
		}
		got := withoutSyntheticV2CostModel(pp, true, nil)
		fp, ok := got.(*dijkstra.DijkstraProtocolParameters)
		require.True(t, ok)
		assert.NotContains(t, fp.CostModels, uint(1))
		assert.Contains(t, pp.CostModels, uint(1), "original must be untouched")
	})
}

func cloneMap(m map[uint][]int64) map[uint][]int64 {
	out := make(map[uint][]int64, len(m))
	for k, v := range m {
		out[k] = append([]int64(nil), v...)
	}
	return out
}

// TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic covers
// PR review: a concrete-typed nil pointer
// (lcommon.ProtocolParameters holding e.g. a nil *conway.ConwayProtocolParameters)
// still matches its type's case in the switch, so each case must guard
// against nil before dereferencing rather than panicking.
func TestWithoutSyntheticV2CostModel_NilPointerDoesNotPanic(t *testing.T) {
	t.Parallel()

	var nilConway *conway.ConwayProtocolParameters
	var pp lcommon.ProtocolParameters = nilConway

	require.NotPanics(t, func() {
		got := withoutSyntheticV2CostModel(pp, true, nil)
		assert.Equal(t, pp, got)
	})
}

// TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic covers the common
// case: once real data has been observed (or none was ever fabricated),
// the filter must return the value unchanged, identical pointer included,
// so a caller reading it sees the exact same struct internal validation
// uses.
func TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic(t *testing.T) {
	t.Parallel()

	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1}, 1: {2}, 2: {3}},
	}

	got := withoutSyntheticV2CostModel(pp, false, nil)

	assert.Same(t, pp, got)
}

// unknownProtocolParameters is a lcommon.ProtocolParameters implementation
// the withoutSyntheticV2CostModel switch has no case for -- standing in for
// a future era type this switch hasn't been taught yet.
type unknownProtocolParameters struct {
	lcommon.ProtocolParameters
}

// TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered covers
// a protocol-parameters type
// the switch doesn't recognize falls to the default branch, which -- unlike
// every other branch -- returns pp unfiltered even though synthetic is true.
// That silently reintroduces for whatever type this is; the least this
// path can do is log so the gap is observable instead of invisible.
func TestWithoutSyntheticV2CostModel_UnknownTypeLogsAndReturnsUnfiltered(
	t *testing.T,
) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	pp := &unknownProtocolParameters{}

	got := withoutSyntheticV2CostModel(pp, true, logger)

	assert.Same(t, pp, got,
		"an unrecognized type must still be returned, unfiltered")
	assert.Contains(
		t,
		buf.String(),
		"does not recognize this protocol-parameters type",
	)
}

// TestExtractRawCostModels_CoversDijkstra pins a gap
// in extractRawCostModels: its type switch lacked a Dijkstra
// case (falling to its own default: return nil), asymmetric with
// withoutSyntheticV2CostModel, which does handle Dijkstra -- meaning
// injectedSyntheticV2CostModel (built on extractRawCostModels) could never
// detect a Dijkstra-era injection even though the filter it feeds covers
// that era.
func TestExtractRawCostModels_CoversDijkstra(t *testing.T) {
	t.Parallel()

	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{0: {1}, 1: {2}},
		},
	}

	got := extractRawCostModels(pp)

	assert.Equal(t, map[uint][]int64{0: {1}, 1: {2}}, got)
}

// TestExtractRawCostModels_NilPointerDoesNotPanic verifies that a concrete-typed
// nil pointer (lcommon.ProtocolParameters
// holding e.g. a nil *dijkstra.DijkstraProtocolParameters) still matches its
// type's case in the switch, so every case must guard against nil before
// dereferencing rather than panicking -- mirroring the guard
// withoutSyntheticV2CostModel already has for the identical hazard.
func TestExtractRawCostModels_NilPointerDoesNotPanic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		pp   lcommon.ProtocolParameters
	}{
		{"Alonzo", (*alonzo.AlonzoProtocolParameters)(nil)},
		{"Babbage", (*babbage.BabbageProtocolParameters)(nil)},
		{"Conway", (*conway.ConwayProtocolParameters)(nil)},
		{"Dijkstra", (*dijkstra.DijkstraProtocolParameters)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				got := extractRawCostModels(tc.pp)
				assert.Nil(t, got)
			})
		})
	}
}

func repeatByte(length int, b byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestEpochRolloverUsesEraSpecificEnactmentSnapshot(t *testing.T) {
	for _, era := range []eras.EraDesc{eras.ConwayEraDesc, eras.DijkstraEraDesc} {
		t.Run(era.Name, func(t *testing.T) {
			f := newTreasuryRolloverFixture(t, 100)
			address, returnAddress, credential := f.rewardAddress(t, 0xa7)
			f.addProposal(
				t,
				0xa8,
				501,
				map[*lcommon.Address]uint64{address: 40},
				returnAddress,
				0,
				true,
			)
			var params lcommon.ProtocolParameters = f.currentPParams
			if era.Id == eras.DijkstraEraDesc.Id {
				var err error
				params, err = eras.HardForkDijkstra(nil, f.currentPParams)
				require.NoError(t, err)
				f.ls.config.EnableDijkstra = true
				f.ls.activeEras = eras.ErasWithDijkstra
			}
			f.currentEpoch.EraId = era.Id
			f.ls.currentEra = era
			f.ls.currentEpoch = f.currentEpoch
			f.ls.currentPParams = params
			require.NoError(
				t,
				f.db.SetEpoch(
					500,
					5,
					f.currentEpoch.Nonce,
					f.currentEpoch.EvolvingNonce,
					f.currentEpoch.CandidateNonce,
					f.currentEpoch.LastEpochBlockNonce,
					era.Id,
					1,
					100,
					nil,
				),
			)
			seedEmptyRewardBasisForRollover(t, f.db, f.currentEpoch, params)
			var observed []uint64
			f.ls.SetEpochBoundarySnapshotStakeHook(
				func(txn *database.Txn, _ event.EpochTransitionEvent) error {
					account, err := f.db.GetAccountByCredential(
						t.Context(),
						0,
						credential,
						false,
						txn,
					)
					if err != nil {
						return err
					}
					observed = append(observed, uint64(account.Reward))
					return nil
				},
			)
			txn := f.db.Transaction(t.Context(), true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				_, err := f.ls.processEpochRollover(
					t.Context(),
					txn,
					f.currentEpoch,
					era,
					params,
					false,
				)
				return err
			}))
			expected := uint64(0)
			if era.Id == eras.DijkstraEraDesc.Id {
				expected = 40
			}
			require.Equal(t, []uint64{expected}, observed)
			require.Equal(t, uint64(40), f.accountReward(t, credential))
		})
	}
}

// A genesis key delegation certificate is checked against the delegations in
// force and the ones still inside the stability window, through the real
// LedgerView and the Shelley validation entry point.
func TestGenesisKeyDelegationThroughEraValidation(t *testing.T) {
	t.Parallel()

	var keys [5]ed25519.PrivateKey
	for i := range keys {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(0xb0 + i)
		keys[i] = ed25519.NewKeyFromSeed(seed)
	}
	delegateOf := func(i int) lcommon.Blake2b224 {
		return lcommon.Blake2b224Hash(keys[i].Public().(ed25519.PublicKey))
	}
	genesisKey := func(seed byte) []byte {
		return bytes.Repeat([]byte{seed}, lcommon.Blake2b224Size)
	}
	lv := mirQuorumTestView(
		t,
		[3]ed25519.PublicKey{
			keys[0].Public().(ed25519.PublicKey),
			keys[1].Public().(ed25519.PublicKey),
			keys[2].Public().(ed25519.PublicKey),
		},
	)
	// Genesis key 0x11 certified a new delegate at slot 100, which stays
	// pending for the whole stability window.
	seedGenesisDelegation(t, lv.ls.db, models.GenesisDelegation{
		GenesisHash:         genesisKey(0x11),
		GenesisDelegateHash: delegateOf(3).Bytes(),
		VrfKeyHash:          bytes.Repeat([]byte{0xf1}, lcommon.Blake2b256Size),
		AddedSlot:           100,
	})

	validate := func(
		genesis []byte,
		delegate lcommon.Blake2b224,
		vrf byte,
	) error {
		cert := &lcommon.GenesisKeyDelegationCertificate{
			CertType: uint(
				lcommon.CertificateTypeGenesisKeyDelegation,
			),
			GenesisHash:         genesis,
			GenesisDelegateHash: delegate.Bytes(),
		}
		copy(
			cert.VrfKeyHash[:],
			bytes.Repeat([]byte{vrf}, lcommon.Blake2b256Size),
		)
		tx := &shelley.ShelleyTransaction{
			Body: shelley.ShelleyTransactionBody{
				TxCertificates: []lcommon.CertificateWrapper{{
					Type: uint(
						lcommon.CertificateTypeGenesisKeyDelegation,
					),
					Certificate: cert,
				}},
			},
		}
		return eras.ValidateTxShelley(
			tx, 200, lv, &shelley.ShelleyProtocolParameters{},
		)
	}

	var notInMapping eras.GenesisKeyNotInMappingError
	var duplicateDelegate eras.DuplicateGenesisDelegateError
	var duplicateVRF eras.DuplicateGenesisVRFError

	err := validate(genesisKey(0x44), delegateOf(3), 0xf9)
	require.ErrorAs(t, err, &notInMapping)

	err = validate(genesisKey(0x22), delegateOf(2), 0xf9)
	require.ErrorAs(t, err, &duplicateDelegate, "delegate in force")

	err = validate(genesisKey(0x22), delegateOf(3), 0xf9)
	require.ErrorAs(t, err, &duplicateDelegate, "delegate pending")

	err = validate(genesisKey(0x22), delegateOf(4), 0xf1)
	require.ErrorAs(t, err, &duplicateVRF, "VRF key pending")

	// The genesis key that certified the pending delegation may repeat it.
	err = validate(genesisKey(0x11), delegateOf(3), 0xf1)
	require.False(t, errors.As(err, &duplicateDelegate), "%v", err)
	require.False(t, errors.As(err, &duplicateVRF), "%v", err)
}
