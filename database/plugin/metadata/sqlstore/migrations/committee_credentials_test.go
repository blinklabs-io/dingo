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
	"errors"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func TestCommitteeCredentialMigrationPreservesExistingRows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 9)
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

	runTo(registry[:6])
	cold := []byte{0x11, 0x22}
	hot := []byte{0x33, 0x44}
	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_cred_hash, expires_epoch, added_slot) VALUES (?, ?, ?)",
		cold,
		41,
		17,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO auth_committee_hot "+
			"(cold_credential, host_credential, added_slot) VALUES (?, ?, ?)",
		cold,
		hot,
		18,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO resign_committee_cold "+
			"(cold_credential, added_slot) VALUES (?, ?)",
		cold,
		19,
	)
	require.NoError(t, err)

	runTo(registry[:7])
	explicitZeroCold := []byte{0x55, 0x66}
	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_credential_tag, cold_cred_hash, expires_epoch, "+
			"term_start_slot, added_slot) VALUES (?, ?, ?, ?, ?)",
		1,
		explicitZeroCold,
		42,
		0,
		23,
	)
	require.NoError(t, err)

	runTo(registry)
	var coldTag, termStart uint64
	var termStartSet bool
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag, term_start_slot, term_start_slot_set "+
			"FROM committee_member WHERE cold_cred_hash = ?",
		cold,
	).Scan(&coldTag, &termStart, &termStartSet))
	require.Zero(t, coldTag)
	require.Equal(t, uint64(17), termStart)
	require.True(t, termStartSet)

	var explicitTermStart uint64
	var explicitTermStartSet bool
	require.NoError(t, db.QueryRow(
		"SELECT term_start_slot, term_start_slot_set "+
			"FROM committee_member WHERE cold_cred_hash = ?",
		explicitZeroCold,
	).Scan(&explicitTermStart, &explicitTermStartSet))
	require.Zero(t, explicitTermStart)
	require.True(t, explicitTermStartSet)

	var authColdTag, authHotTag uint64
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag, hot_credential_tag "+
			"FROM auth_committee_hot WHERE cold_credential = ?",
		cold,
	).Scan(&authColdTag, &authHotTag))
	require.Zero(t, authColdTag)
	require.Zero(t, authHotTag)

	var resignColdTag uint64
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag FROM resign_committee_cold "+
			"WHERE cold_credential = ?",
		cold,
	).Scan(&resignColdTag))
	require.Zero(t, resignColdTag)

	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_credential_tag, cold_cred_hash, expires_epoch, "+
			"term_start_slot, added_slot) VALUES (?, ?, ?, ?, ?)",
		1,
		cold,
		42,
		17,
		17,
	)
	require.NoError(
		t,
		err,
		"the migrated uniqueness constraint must preserve the credential tag",
	)
}

func TestCommitteeTermStartBackfillResumesAfterInterruption(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	run := func(versions []migrations.Migration) error {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		return runner.Run(context.Background())
	}

	require.NoError(t, run(registry[:7]))
	for slot := int64(1); slot <= 3; slot++ {
		_, err = db.Exec(
			"INSERT INTO committee_member (cold_cred_hash, expires_epoch, added_slot) VALUES (?, ?, ?)",
			[]byte{byte(slot)}, 41, slot,
		)
		require.NoError(t, err)
	}

	interrupted := registry[8]
	interrupted.BatchSize = 1
	originalBackfill := interrupted.Backfill
	backfillCalls := 0
	interrupted.Backfill = func(ctx context.Context, batch migrations.Batch) (migrations.BatchResult, error) {
		backfillCalls++
		if backfillCalls == 2 {
			return migrations.BatchResult{}, errors.New("intentional interruption")
		}
		return originalBackfill(ctx, batch)
	}
	interruptedRegistry := append(append([]migrations.Migration{}, registry[:7]...), interrupted)
	require.Error(t, run(interruptedRegistry))
	require.NoError(t, run(registry))

	var incomplete int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM committee_member WHERE NOT term_start_slot_set",
	).Scan(&incomplete))
	require.Zero(t, incomplete)
}
