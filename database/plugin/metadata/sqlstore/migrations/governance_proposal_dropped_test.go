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

package migrations_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestGovernanceProposalDropBackfillMarksAlreadyRefundedProposals covers
// dingo#4411's upgrade path. Before v17 the epoch tick refunded an expired
// proposal's deposit in the tick that marked it expired, so on an upgraded
// database every expired_epoch row has already been refunded. The drop step
// selects on `dropped_epoch IS NULL`, so without the v17 backfill it would
// match every historically expired proposal and refund each a second time at
// the first boundary after the upgrade.
func TestGovernanceProposalDropBackfillMarksAlreadyRefundedProposals(
	t *testing.T,
) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 22)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	// Pre-v17 schema, holding one proposal already expired (and, under that
	// schema's code, already refunded) and one still active.
	runTo(registry[:16])
	expiredHash := []byte{0xaa, 0xbb}
	activeHash := []byte{0xcc, 0xdd}
	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, "+
			"action_type, proposed_epoch, expires_epoch, deposit, "+
			"expired_epoch, expired_slot, added_slot) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		expiredHash, 0, 6, 670, 673, int64(100000000000), 674, 58100000, 57000000,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, "+
			"action_type, proposed_epoch, expires_epoch, deposit, "+
			"added_slot) VALUES (?, ?, ?, ?, ?, ?, ?)",
		activeHash, 0, 6, 676, 682, int64(100000000000), 58600000,
	)
	require.NoError(t, err)

	runTo(registry)

	var droppedEpoch, droppedSlot uint64
	require.NoError(t, db.QueryRow(
		"SELECT dropped_epoch, dropped_slot FROM governance_proposal_drop "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_drop.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		expiredHash,
	).Scan(&droppedEpoch, &droppedSlot),
		"an already-refunded expired proposal must be recorded as dropped")
	require.Equal(t, uint64(674), droppedEpoch)
	require.Equal(t, uint64(58100000), droppedSlot)

	// A proposal that never expired must not gain a drop row, or its
	// eventual expiry would never refund the deposit at all.
	var activeDropRows int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM governance_proposal_drop "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_drop.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		activeHash,
	).Scan(&activeDropRows))
	require.Zero(t, activeDropRows)
}
