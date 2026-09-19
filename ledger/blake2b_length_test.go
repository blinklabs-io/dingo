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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// shortHash returns a credential one byte narrower than a Blake2b224 whose
// zero-padded form is a well-formed hash. An unchecked copy into the
// fixed-size array turns it into that padded value, which then keys a query
// result as if it were a real credential.
func shortHash(fill byte) []byte {
	hash := make([]byte, lcommon.Blake2b224Size-1)
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

func TestQueryStakeSnapshotsRejectsWrongLengthPoolKeyHash(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	poolKeyHash := shortHash(0xA1)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{{
			Epoch:          2,
			SnapshotType:   "mark",
			PoolKeyHash:    poolKeyHash,
			TotalStake:     types.Uint64(1_000_000),
			DelegatorCount: 1,
			CapturedSlot:   200,
		}},
		nil,
	))
	ls := &LedgerState{db: db}
	ls.consensus.Store(
		&consensusSnapshot{currentEpoch: models.Epoch{EpochId: 2}},
	)

	// An empty pool filter takes the unrestricted path, which builds its
	// result keys from the stored pool key hashes.
	result, err := ls.queryShelleyStakeSnapshots(
		&olocalstatequery.ShelleyStakeSnapshotsQuery{},
	)
	require.Error(
		t,
		err,
		"a %d-byte pool key hash must not be padded into a result key",
		len(poolKeyHash),
	)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "blake2b-224")
}

func TestQueryDRepStateRejectsWrongLengthCredential(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	credential := shortHash(0xB1)
	tag, err := models.CredentialTagFromUint(
		uint(lcommon.CredentialTypeAddrKeyHash),
	)
	require.NoError(t, err)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: tag,
			Credential:    credential,
			AddedSlot:     100,
			Active:        true,
		},
		&models.RegistrationDrep{
			CredentialTag:  tag,
			DrepCredential: credential,
			AddedSlot:      100,
			DepositAmount:  types.Uint64(400_000_000),
		},
		nil,
	))
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyDRepState(nil)
	require.Error(
		t,
		err,
		"a %d-byte DRep credential must not be padded into a result key",
		len(credential),
	)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "blake2b-224")
}

// TestQueryDRepStateAcceptsExactLengthCredential is the control for
// TestQueryDRepStateRejectsWrongLengthCredential: it proves the rejected case
// above reaches the same live query path rather than failing earlier.
func TestQueryDRepStateAcceptsExactLengthCredential(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	cred := drepRefundTestCredential(0xc1)
	seedImportedDrep(t, db, cred, 400_000_000, 100, true)
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyDRepState(nil)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	dreps, ok := outer[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok)
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	_, found := dreps[olocalstatequery.StakeCredential{
		Tag:   uint64(tag),
		Bytes: gledger.NewBlake2b224(cred.Credential[:]),
	}]
	require.True(t, found, "DRep missing from GetDRepState result")
}

// TestStakePoolsResultRejectsWrongLengthKeyHash covers GetStakePools, whose
// pool ids come from an unbounded key-hash column. A padded hash is a
// well-formed pool id on the wire and names a pool that does not exist.
func TestStakePoolsResultRejectsWrongLengthKeyHash(t *testing.T) {
	t.Parallel()
	result, err := stakePoolsResult([][]byte{shortHash(0xC1)})
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "blake2b-224")
}

// TestDRepAnchorRejectsWrongLengthHash covers the anchor data hash, which is
// Blake2b-256 and identifies the off-chain metadata a client fetches and
// checks. A truncated stored value becomes an anchor no document can match.
func TestDRepAnchorRejectsWrongLengthHash(t *testing.T) {
	t.Parallel()
	anchor, err := drepAnchor(&models.Drep{
		AnchorURL:  "https://example.invalid/drep.json",
		AnchorHash: make([]byte, lcommon.Blake2b256Size-1),
	})
	require.Error(t, err)
	require.Nil(t, anchor)
	require.Contains(t, err.Error(), "blake2b-256")
}

// TestDRepAnchorAbsentIsNotAnError is the control for the case above: a DRep
// registered without an anchor still yields a nil anchor rather than the new
// length error.
func TestDRepAnchorAbsentIsNotAnError(t *testing.T) {
	t.Parallel()
	anchor, err := drepAnchor(&models.Drep{})
	require.NoError(t, err)
	require.Nil(t, anchor)
}

// TestStakeCredentialFromVoteRejectsWrongLengthCredential covers the
// governance vote credential, which keys the committee and DRep vote maps in
// a GetGovState result.
func TestStakeCredentialFromVoteRejectsWrongLengthCredential(t *testing.T) {
	t.Parallel()
	cred, err := stakeCredentialFromVote(&models.GovernanceVote{
		VoterCredentialTag: uint8(lcommon.CredentialTypeAddrKeyHash),
		VoterCredential:    shortHash(0xD1),
	})
	require.Error(t, err)
	require.Equal(t, olocalstatequery.StakeCredential{}, cred)
	require.Contains(t, err.Error(), "blake2b-224")
}
