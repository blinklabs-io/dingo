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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// The Musashi Conway genesis declares three genesis committee members, all
// key-hash cold credentials, each expiring at epoch 293.
var musashiGenesisCommitteeColdKeys = []string{
	"0fa32e5f69a89afa3f5e1074660b975dde8e5a89c1b8004d49501e33",
	"518a0c96344656d332625e33aa680b6c25bbce6b5972a30adf1dce8d",
	"8feda2412bec6f79bc5996a5055bcff28d230cb9c85fb9d5e8743a46",
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
				require.NoError(t, ls.createGenesisBlock())
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

func TestEmptyGenesisCommitteeValidation(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	require.Zero(t, committeeMemberRowCount(t, db))
	pp := &conway.ConwayProtocolParameters{}
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	lv := ls.NewView(nil)

	for _, tag := range []uint{
		lcommon.CredentialTypeAddrKeyHash,
		lcommon.CredentialTypeScriptHash,
	} {
		cold := committeeTestCredential(0xe1)
		cold.CredType = tag
		hot := committeeTestCredential(0xe2)
		hot.CredType = tag
		certs := []lcommon.Certificate{
			&lcommon.AuthCommitteeHotCertificate{
				CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: cold,
				HotCredential:  hot,
			},
			&lcommon.ResignCommitteeColdCertificate{
				CertType: uint(
					lcommon.CertificateTypeResignCommitteeCold,
				),
				ColdCredential: cold,
			},
		}
		for _, cert := range certs {
			t.Run(
				fmt.Sprintf("tag %d/%s", tag, certificateName(cert)),
				func(t *testing.T) {
					tx := &conway.ConwayTransaction{
						TxIsValid: true,
						Body: conway.ConwayTransactionBody{
							TxCertificates: []lcommon.CertificateWrapper{{
								Type: cert.Type(), Certificate: cert,
							}},
						},
					}
					err := eras.ValidateTxConway(tx, 0, lv, pp)
					var notMember conway.NotCommitteeMemberError
					require.ErrorAs(t, err, &notMember)
					require.Equal(t, cold.Credential, notMember.Credential)
					require.Equal(t, certificateName(cert), notMember.Operation)
				},
			)
		}

		voterType := uint8(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash)
		if tag == lcommon.CredentialTypeScriptHash {
			voterType = lcommon.VoterTypeConstitutionalCommitteeHotScriptHash
		}
		voter := &lcommon.Voter{Type: voterType, Hash: hot.Credential}
		tx := &conway.ConwayTransaction{
			TxIsValid: true,
			Body: conway.ConwayTransactionBody{
				TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
			},
		}
		t.Run(fmt.Sprintf("tag %d/vote", tag), func(t *testing.T) {
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var unknown conway.UnknownVoterError
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, *voter, unknown.Voter)
		})

		// GOVCERT permits a proposed member even when no committee is seated.
		storeCommitteeUpdateProposal(t, db, byte(0xe3+tag), cold, 90)
		for _, cert := range certs {
			tx := &conway.ConwayTransaction{
				TxIsValid: true,
				Body: conway.ConwayTransactionBody{
					TxCertificates: []lcommon.CertificateWrapper{{
						Type: cert.Type(), Certificate: cert,
					}},
				},
			}
			err := eras.ValidateTxConway(tx, 0, lv, pp)
			var notMember conway.NotCommitteeMemberError
			require.False(t, errors.As(err, &notMember), "%v", err)
			var lookup conway.CommitteeMemberLookupError
			require.False(t, errors.As(err, &lookup), "%v", err)
		}
	}
}

func TestEmptyGenesisCommitteeReferenceResignation(t *testing.T) {
	t.Parallel()

	root, err := conformance.ExtractEmbeddedTestdata(t.TempDir())
	require.NoError(t, err)
	vector, err := conformance.DecodeTestVector(filepath.Join(
		root,
		"eras",
		"conway",
		"impl",
		"dump",
		"Conway.Imp.ConwayImpSpec_-_Version_10.GOVCERT.fails_for.resigning_a_nonexistent_CC_member_hotkey",
		"1",
	))
	require.NoError(t, err)
	initial, err := conformance.ParseInitialState(vector.InitialState)
	require.NoError(t, err)
	require.Len(t, vector.Events, 1)
	event := vector.Events[0]
	require.Equal(t, conformance.EventTypeTransaction, event.Type)
	require.False(t, event.Success)
	tx, err := conway.NewConwayTransactionFromCbor(event.TxBytes)
	require.NoError(t, err)
	require.Len(t, tx.Certificates(), 1)
	cert, ok := tx.Certificates()[0].(*lcommon.ResignCommitteeColdCertificate)
	require.True(t, ok)
	for credential := range initial.CommitteeMembersByCredential {
		require.NotEqual(
			t,
			cert.ColdCredential.Credential,
			credential.Credential,
		)
	}
	pp, err := conformance.NewPParamsLoaderFromTestdata(root).
		LoadForVector(vector, initial)
	require.NoError(t, err)

	ls, _ := genesisConstitutionTestState(t)
	ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members = map[string]int{}
	require.NoError(t, ls.createGenesisBlock())
	ls.currentPParams = pp
	ls.currentEpoch = models.Epoch{EpochId: initial.CurrentEpoch}
	ls.publishSnapshotsLocked()
	// The reference rejects this unknown cold credential with a seated
	// committee. Removing that committee cannot make the credential eligible.
	// Check that same GOVCERT error with the production empty-genesis view;
	// this is a rule-level projection, not a replay of the full vector state.
	err = eras.ValidateTxConway(tx, event.Slot, ls.NewView(nil), pp)
	var notMember conway.NotCommitteeMemberError
	require.ErrorAs(t, err, &notMember)
	require.Equal(t, cert.ColdCredential.Credential, notMember.Credential)
	require.Equal(t, "resign", notMember.Operation)
}

// TestCreateGenesisBlockSeedsCommittee proves a node initialized from Conway
// genesis recognizes every genesis Constitutional Committee member for
// hot-key authorization. Without the seed (blinklabs-io/dingo#3785) a
// genesis member never touched by an UpdateCommittee action has no row at
// all, and AuthCommitteeHot/ResignCommitteeCold validation rejects it as
// "not a CC member" even though the real chain has recognized it since the
// hard fork.
func TestCreateGenesisBlockSeedsCommittee(t *testing.T) {
	t.Parallel()

	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

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

// TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase covers the upgrade
// path, which is the population that actually has the bug: a node already
// synced from genesis on a build that never seeded the committee.
//
// Such a database has matching genesis CBOR and a nonzero tip, so
// createGenesisBlock takes its early-return branch and never reaches the
// genesis-creation transaction. Seeding only from that transaction would
// therefore fix new nodes and leave every existing one broken.
func TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// Stand in for a database written by a build with no committee seed:
	// genesis CBOR present and matching, a tip well past zero, and no
	// committee_member rows at all.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(0, genesisHash[:], []byte{0x80}, nil))
	ls.currentTip.Point = ocommon.Point{Slot: 1_000_000}
	require.Equal(t, 0, committeeMemberRowCount(t, db))

	require.NoError(t, ls.createGenesisBlock())

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
			"genesis committee member %s must be backfilled on an existing database",
			coldKeyHex,
		)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			member.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockCommitteeReplayIdempotent proves re-running genesis
// initialization over a store that already holds the genesis committee
// leaves a single row per member rather than a duplicate soft-delete/insert
// pair.
func TestCreateGenesisBlockCommitteeReplayIdempotent(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	require.Equal(
		t,
		len(musashiGenesisCommitteeColdKeys),
		committeeMemberRowCount(t, db),
	)
}

// TestCreateGenesisBlockCommitteeEnactmentWins proves a real UpdateCommittee
// enactment for a genesis cold credential outranks the genesis seed, and
// that a later genesis initialization pass does not revert it back to the
// genesis term -- the hazard a naive unconditional reseed on every startup
// would create.
func TestCreateGenesisBlockCommitteeEnactmentWins(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	coldKey, err := hex.DecodeString(musashiGenesisCommitteeColdKeys[0])
	require.NoError(t, err)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{
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
	require.NoError(t, ls.createGenesisBlock())

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

// TestParseGenesisCommitteeCredential exercises both credential prefixes and
// the rejection paths for malformed genesis committee member keys.
func TestParseGenesisCommitteeCredential(t *testing.T) {
	t.Parallel()

	keyHash := bytes.Repeat([]byte{0xab}, 28)
	tag, hash, err := parseGenesisCommitteeCredential(
		"keyHash-" + hex.EncodeToString(keyHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeAddrKeyHash), tag)
	require.Equal(t, keyHash, hash)

	scriptHash := bytes.Repeat([]byte{0xcd}, 28)
	tag, hash, err = parseGenesisCommitteeCredential(
		"scriptHash-" + hex.EncodeToString(scriptHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeScriptHash), tag)
	require.Equal(t, scriptHash, hash)

	_, _, err = parseGenesisCommitteeCredential("bogus-deadbeef")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-nothex")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-abcd")
	require.Error(t, err)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiry proves a malformed Conway
// genesis fails initialization instead of seating a member with a wrapped
// term.
//
// conway-genesis.json models the committee expiry as a bare JSON number, so it
// decodes into a signed int. Converting a negative value straight to the
// store's unsigned epoch would wrap it to a near-maximum uint64 -- a term no
// epoch boundary would ever expire -- so the seed must refuse it outright.
func TestEnsureGenesisCommitteeRejectsNegativeExpiry(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// The embedded config is parsed fresh on every load, so mutating this
	// state's genesis below cannot leak into the other tests in this file.
	other, _ := genesisConstitutionTestState(t)
	require.NotSame(
		t,
		ls.config.CardanoNodeConfig.ConwayGenesis(),
		other.config.CardanoNodeConfig.ConwayGenesis(),
		"each test state must own its genesis for the mutation below to be safe",
	)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -1

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -1")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		0,
		committeeMemberRowCount(t, db),
		"a malformed genesis committee must seat no members at all",
	)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded proves the
// malformed-genesis check fails closed on the upgrade path too.
//
// Validating the expiry only after the already-seeded check would let the same
// malformed genesis start a node whose database happens to hold rows already
// while refusing a fresh one, even though the genesis file is equally
// malformed in both cases.
func TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded(
	t *testing.T,
) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	seeded := committeeMemberRowCount(t, db)
	require.Equal(t, len(musashiGenesisCommitteeColdKeys), seeded)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -7

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -7")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		seeded,
		committeeMemberRowCount(t, db),
		"the failed check must not add or remove rows",
	)
}

// TestGenesisCommitteeExpiryEpoch covers the signed-to-unsigned conversion
// directly, including the most negative int, which is the value a straight
// conversion wraps furthest.
func TestGenesisCommitteeExpiryEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expiry  int
		want    uint64
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{
			"musashi genesis expiry",
			musashiGenesisCommitteeExpiry,
			musashiGenesisCommitteeExpiry,
			false,
		},
		{"negative one", -1, 0, true},
		{"most negative int", math.MinInt, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := genesisCommitteeExpiryEpoch(tc.expiry)
			if tc.wantErr {
				require.Error(t, err)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// committeeMemberRowCount returns the number of stored committee_member rows.
func committeeMemberRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM committee_member").Scan(&count),
	)
	return count
}
