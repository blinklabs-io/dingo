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
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// The v38 backfill pages by a (tag, hash) cursor that compares blob columns
// and scans an EXISTS result into a bool, so run it against rows on each real
// backend.
func TestPostgresCommitteeRenewalTermStartUpgradeIntegration(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	exerciseCommitteeRenewalTermStartUpgrade(
		t, "pgx", dsn, PostgresDialect(), schema, registry,
	)
}

func TestMySQLCommitteeRenewalTermStartUpgradeIntegration(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	exerciseCommitteeRenewalTermStartUpgrade(
		t, "mysql", dsn, MySQLDialect(), database, registry,
	)
}

func exerciseCommitteeRenewalTermStartUpgrade(
	t *testing.T,
	driver, dsn string,
	dialect Dialect,
	lockNamespace string,
	registry []migrations.Migration,
) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialect.Name(), false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	migrationIndex := -1
	for i, migration := range registry {
		if migration.Name == "committee-renewal-term-start-repair" {
			migrationIndex = i
			break
		}
	}
	require.Greater(
		t,
		migrationIndex,
		0,
		"committee renewal term start migration must exist",
	)
	registry[migrationIndex].BatchSize = 1
	runTo := func(versions []migrations.Migration) {
		t.Helper()
		runner := migrations.Runner{
			DB:       db,
			Dialect:  dialect.Name(),
			Registry: versions,
			Locker:   integrationMigrationLocker(dialect.Name(), lockNamespace),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:migrationIndex])

	renewed := bytes.Repeat([]byte{0xaa}, 28)
	reelected := bytes.Repeat([]byte{0xbb}, 28)
	noConfidence := bytes.Repeat([]byte{0xcc}, 28)
	imported := bytes.Repeat([]byte{0xee}, 28)
	deleted := func(slot int64) sql.NullInt64 {
		return sql.NullInt64{Int64: slot, Valid: true}
	}
	cbor := []byte{0x80}
	proposals := []struct {
		slot          int64
		actionType    int64
		govActionCbor []byte
	}{
		{1000, 4, cbor},
		{2000, 4, cbor},
		{3000, 4, cbor},
		{4000, 3, cbor},
		{4500, 4, cbor},
		{5000, 4, cbor},
		{8000, 4, nil},
	}
	fixtures := []struct {
		tag           int64
		hash          []byte
		addedSlot     int64
		deletedSlot   sql.NullInt64
		termStartSlot int64
		wantTermStart int64
	}{
		{0, renewed, 0, deleted(1000), 0, 0},
		{0, renewed, 1000, deleted(5000), 900, 0},
		{0, renewed, 5000, sql.NullInt64{}, 4800, 0},
		{1, renewed, 200, deleted(1000), 50, 50},
		{1, renewed, 1000, sql.NullInt64{}, 1000, 50},
		{0, reelected, 100, deleted(2000), 100, 100},
		{0, reelected, 3000, sql.NullInt64{}, 2900, 2900},
		{0, noConfidence, 0, deleted(4000), 0, 0},
		{0, noConfidence, 4500, sql.NullInt64{}, 4400, 4400},
		{0, imported, 0, deleted(8000), 0, 0},
		{0, imported, 8000, sql.NullInt64{}, 8000, 8000},
	}
	for i, proposal := range proposals {
		_, err := db.Exec(dialect.Rebind(`
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    enacted_epoch, enacted_slot, deposit, gov_action_cbor, added_slot
) VALUES (?, 0, ?, 0, 0, 1, ?, 0, ?, 0)`),
			bytes.Repeat([]byte{byte(i + 1)}, 32),
			proposal.actionType, proposal.slot, proposal.govActionCbor,
		)
		require.NoError(t, err)
	}
	for _, fixture := range fixtures {
		_, err := db.Exec(dialect.Rebind(`
INSERT INTO committee_member (
    cold_credential_tag, cold_cred_hash, expires_epoch, term_start_slot,
    term_start_slot_set, added_slot, deleted_slot
) VALUES (?, ?, 100, ?, TRUE, ?, ?)`),
			fixture.tag, fixture.hash, fixture.termStartSlot,
			fixture.addedSlot, fixture.deletedSlot,
		)
		require.NoError(t, err)
	}
	runTo(registry)

	for _, fixture := range fixtures {
		var got int64
		require.NoError(t, db.QueryRow(dialect.Rebind(`
SELECT term_start_slot FROM committee_member
WHERE cold_credential_tag = ? AND cold_cred_hash = ? AND added_slot = ?`),
			fixture.tag, fixture.hash, fixture.addedSlot,
		).Scan(&got))
		require.Equal(
			t,
			fixture.wantTermStart,
			got,
			"tag %d, added_slot %d",
			fixture.tag, fixture.addedSlot,
		)
	}
}
