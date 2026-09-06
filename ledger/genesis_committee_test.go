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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The Musashi Conway genesis declares three genesis committee members, all
// key-hash cold credentials, each expiring at epoch 293.
var musashiGenesisCommitteeColdKeys = []string{
	"4a45ab0e4dc24e2567d282adc3928a69e6b6a8e155a9b14ae7147c21",
	"6bb088d6ada8d97d130a8e3406360fbdf39be38b699a77778d26ae72",
	"c3c5d6f905a91c12ea51b328953ccee359efb2cff6130d9fee8dea05",
}

const musashiGenesisCommitteeExpiry = 293

// TestCreateGenesisBlockSeedsCommittee proves a node initialized from Conway
// genesis recognizes every genesis Constitutional Committee member for
// hot-key authorization. Without the seed (blinklabs-io/dingo#3785) a
// genesis member never touched by an UpdateCommittee action has no row at
// all, and AuthCommitteeHot/ResignCommitteeCold validation rejects it as
// "not a CC member" even though the real chain has recognized it since the
// hard fork.
func TestCreateGenesisBlockSeedsCommittee(t *testing.T) {
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
		require.NotNil(t, member, "genesis committee member %s must resolve", coldKeyHex)
		require.False(t, member.Resigned)
		require.Equal(t, uint64(musashiGenesisCommitteeExpiry), member.ExpiryEpoch)
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
		require.Equal(t, uint64(musashiGenesisCommitteeExpiry), member.ExpiryEpoch)
	}
}

// TestCreateGenesisBlockCommitteeReplayIdempotent proves re-running genesis
// initialization over a store that already holds the genesis committee
// leaves a single row per member rather than a duplicate soft-delete/insert
// pair.
func TestCreateGenesisBlockCommitteeReplayIdempotent(t *testing.T) {
	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	require.Equal(t, len(musashiGenesisCommitteeColdKeys), committeeMemberRowCount(t, db))
}

// TestCreateGenesisBlockCommitteeEnactmentWins proves a real UpdateCommittee
// enactment for a genesis cold credential outranks the genesis seed, and
// that a later genesis initialization pass does not revert it back to the
// genesis term -- the hazard a naive unconditional reseed on every startup
// would create.
func TestCreateGenesisBlockCommitteeEnactmentWins(t *testing.T) {
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
		require.Equal(t, uint64(musashiGenesisCommitteeExpiry), other.ExpiryEpoch)
	}
}

// TestParseGenesisCommitteeCredential exercises both credential prefixes and
// the rejection paths for malformed genesis committee member keys.
func TestParseGenesisCommitteeCredential(t *testing.T) {
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
