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

package migrations_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestGovernanceProposalOrderBackfillUsesStoredBlockPosition covers the v26
// upgrade: a proposal whose transaction is stored takes that transaction's
// block position, and one without a stored transaction (a Mithril-imported
// proposal) gets no row, so it keeps the transaction-hash order it had
// before the upgrade.
func TestGovernanceProposalOrderBackfillUsesStoredBlockPosition(
	t *testing.T,
) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 26)
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

	runTo(registry[:25])
	storedHash := []byte{0x02, 0x02}
	importedHash := []byte{0x01, 0x01}
	_, err = db.Exec(
		"INSERT INTO \"transaction\" (hash, slot, type, block_index, valid) "+
			"VALUES (?, ?, ?, ?, ?)",
		storedHash, 900, 6, 3, true,
	)
	require.NoError(t, err)
	for _, hash := range [][]byte{storedHash, importedHash} {
		_, err = db.Exec(
			"INSERT INTO governance_proposal (tx_hash, action_index, "+
				"action_type, proposed_epoch, expires_epoch, deposit, "+
				"added_slot) VALUES (?, ?, ?, ?, ?, ?, ?)",
			hash, 0, 2, 9, 15, 1000, 900,
		)
		require.NoError(t, err)
	}

	runTo(registry)
	// Re-running the upgrade is a no-op rather than a duplicate-key failure.
	runTo(registry)

	var txIndex uint32
	require.NoError(t, db.QueryRow(
		"SELECT tx_index FROM governance_proposal_order "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_order.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		storedHash,
	).Scan(&txIndex))
	require.Equal(t, uint32(3), txIndex)

	var importedRows int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM governance_proposal_order "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_order.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		importedHash,
	).Scan(&importedRows))
	require.Zero(t, importedRows)
}
