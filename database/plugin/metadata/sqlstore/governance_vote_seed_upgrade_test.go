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

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// A vote that predates migration v22 gets one seeded history entry holding
// its value at the upgrade. The vote page reports it as the vote's earliest
// entry, which is what lets the API tell it from a change of value; a later
// change at another slot is not the earliest.
func TestGovernanceVotesPageMarksV22SeededHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	index := slices.IndexFunc(registry, func(m migrations.Migration) bool {
		return m.Name == "governance-vote-history"
	})
	require.GreaterOrEqual(t, index, 0)
	locker := migrations.NewFileLocker(path + ".migrate.lock")
	openStore := func(versions []migrations.Migration) (*Store, *sql.DB) {
		t.Helper()
		db, err := sql.Open(
			"sqlite",
			"file:"+path+"?_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)",
		)
		require.NoError(t, err)
		store, err := New(Config{
			WriteDB:         db,
			Dialect:         SQLiteDialect(),
			Migrations:      versions,
			MigrationLocker: locker,
		})
		require.NoError(t, err)
		require.NoError(t, store.Start(ctx))
		return store, db
	}

	store, db := openStore(registry[:index])
	result, err := db.ExecContext(ctx, `
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    deposit, added_slot
) VALUES (?, 0, 6, 1, 10, 1, 10)`, bytes.Repeat([]byte{0x01}, 32))
	require.NoError(t, err)
	proposalID, err := result.LastInsertId()
	require.NoError(t, err)
	voter := bytes.Repeat([]byte{0xd1}, 28)
	// A pre-v22 revote overwrote the row in place, leaving only the value
	// and slot of the last cast.
	_, err = db.ExecContext(ctx, `
INSERT INTO governance_vote (
    proposal_id, voter_type, voter_credential_tag, voter_credential, vote,
    added_slot, vote_updated_slot
) VALUES (?, ?, 0, ?, ?, 50, 100)`,
		proposalID, models.VoterTypeDRep, voter, models.VoteNo)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	store, _ = openStore(registry)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	updated := uint64(200)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: uint(proposalID), VoterType: models.VoterTypeDRep,
		VoterCredential: voter, Vote: models.VoteYes,
		AddedSlot: 50, VoteUpdatedSlot: &updated,
	}, nil))

	id := uint(proposalID)
	rows, total, err := store.GetGovernanceVotesPage(
		models.GovernanceVoteFilter{ProposalID: &id}, 10, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 2)
	seed, change := rows[0], rows[1]
	require.Equal(t, uint64(100), seed.CastSlot)
	require.Nil(t, seed.CastTxHash)
	require.Equal(t, uint8(models.VoteNo), seed.Vote.Vote)
	require.Equal(t, 0, seed.CastSlotRank)
	require.Equal(t, 1, seed.CastSlotCount)
	require.True(t, seed.CastSlotEarliest)
	require.False(t, seed.Counted)
	require.Equal(t, uint64(200), change.CastSlot)
	require.False(t, change.CastSlotEarliest)
	require.True(t, change.Counted)
}
