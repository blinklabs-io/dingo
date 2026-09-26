//go:build dingo_db_integration

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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// The v26 backfill is an INSERT ... SELECT that reads the target table in a
// NOT EXISTS subquery, which MySQL restricts in some statement shapes; run the
// upgrade against rows on each real backend.
func TestPostgresGovernanceProposalOrderUpgradeIntegration(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	exerciseGovernanceProposalOrderUpgrade(
		t, "pgx", dsn, "postgres", schema, registry,
		`INSERT INTO "transaction" (hash, slot, type, block_index, valid)
VALUES ($1, $2, $3, $4, $5)`,
		`INSERT INTO governance_proposal (tx_hash, action_index, action_type,
proposed_epoch, expires_epoch, deposit, added_slot)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
	)
}

func TestMySQLGovernanceProposalOrderUpgradeIntegration(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	exerciseGovernanceProposalOrderUpgrade(
		t, "mysql", dsn, "mysql", database, registry,
		"INSERT INTO `transaction` (hash, slot, type, block_index, valid) "+
			"VALUES (?, ?, ?, ?, ?)",
		"INSERT INTO governance_proposal (tx_hash, action_index, "+
			"action_type, proposed_epoch, expires_epoch, deposit, added_slot) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
}

func exerciseGovernanceProposalOrderUpgrade(
	t *testing.T,
	driver, dsn, dialect, lockNamespace string,
	registry []migrations.Migration,
	insertTransaction, insertProposal string,
) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialect, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.Len(t, registry, 26)
	runTo := func(versions []migrations.Migration) {
		t.Helper()
		runner := migrations.Runner{
			DB:       db,
			Dialect:  dialect,
			Registry: versions,
			Locker:   integrationMigrationLocker(dialect, lockNamespace),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:25])

	stored := make([]byte, 32)
	stored[0] = 0x02
	imported := make([]byte, 32)
	imported[0] = 0x01
	_, err = db.Exec(insertTransaction, stored, 900, 6, 3, true)
	require.NoError(t, err)
	for _, hash := range [][]byte{stored, imported} {
		_, err = db.Exec(insertProposal, hash, 0, 2, 9, 15, 1000, 900)
		require.NoError(t, err)
	}
	runTo(registry)
	runTo(registry)

	rows, err := db.Query(
		"SELECT governance_proposal.tx_hash, governance_proposal_order.tx_index " +
			"FROM governance_proposal_order " +
			"JOIN governance_proposal " +
			"ON governance_proposal.id = governance_proposal_order.proposal_id",
	)
	require.NoError(t, err)
	defer rows.Close()
	type orderRow struct {
		hash    []byte
		txIndex uint32
	}
	var got []orderRow
	for rows.Next() {
		var row orderRow
		require.NoError(t, rows.Scan(&row.hash, &row.txIndex))
		got = append(got, row)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []orderRow{{hash: stored, txIndex: 3}}, got)
}
