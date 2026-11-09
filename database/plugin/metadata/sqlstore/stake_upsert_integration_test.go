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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresStakeUpsertsMatchFullUpserts(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testStakeUpsertsMatchFullUpserts(
		t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLStakeUpsertsMatchFullUpserts(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testStakeUpsertsMatchFullUpserts(
		t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

// testStakeUpsertsMatchFullUpserts runs the changed-columns account upsert and
// the pool-preserving live-stake upsert through the dialect's statement
// translation and requires the rows to equal what the full upserts leave.
func testStakeUpsertsMatchFullUpserts(t *testing.T, store *Store) {
	t.Helper()
	for _, query := range append(
		upsertAccountChangedSQLs[:], rewardLiveStakeUpsertKeepPoolQuery,
	) {
		_, ok := store.lookupCachedStmt(query)
		require.True(t, ok, "not prepared by Start:\n%s", query)
	}
	poolA, poolB := []byte{0xa1}, []byte{0xb1}
	drepX := []byte{0xc1}
	var states []certificateAccountStored
	for _, active := range []bool{true, false} {
		for _, pool := range [][]byte{nil, poolA, poolB} {
			for _, drep := range [][]byte{nil, drepX} {
				states = append(states, certificateAccountStored{
					active: active, pool: pool, drep: drep,
					drepType: map[bool]uint64{false: 0, true: 2}[drep != nil],
				})
			}
		}
	}
	accountRow := func(db queryer, ctx context.Context, key []byte) string {
		var pool, drep []byte
		var drepType, added, created int64
		var active bool
		require.NoError(t, db.QueryRowContext(ctx, store.dialect.Rebind(`
SELECT pool, drep, drep_type, active, added_slot, created_slot
FROM account WHERE staking_key = ?`), key).Scan(
			&pool, &drep, &drepType, &active, &added, &created))
		return fmt.Sprintf("%x|%t|%x|%t|%d|%t|%d|%d", pool, pool == nil,
			drep, drep == nil, drepType, active, added, created)
	}
	n := 0
	require.NoError(t, store.withWriteTransaction(nil, func(
		db queryer, ctx context.Context,
	) error {
		for _, stored := range states {
			for _, next := range states {
				n++
				keyNew := []byte(fmt.Sprintf("new-%d", n))
				keyFull := []byte(fmt.Sprintf("full-%d", n))
				for _, key := range [][]byte{keyNew, keyFull} {
					_, err := db.ExecContext(ctx, store.dialect.Rebind(`
INSERT INTO account (staking_key, credential_tag, pool, drep, added_slot,
  created_slot, reward, drep_type, active, expiration_epoch)
VALUES (?, 0, ?, ?, 10, 5, '0', ?, ?, 0)`),
						key, stored.pool, stored.drep, stored.drepType,
						stored.active)
					require.NoError(t, err)
				}
				require.NoError(t, store.updateCertificateAccountRow(
					ctx, db, 0, keyNew, 20, stored, next))
				_, err := db.ExecContext(ctx, upsertAccountSQL,
					keyFull, 0, next.pool, next.drep, 20, 20, next.drepType,
					next.active)
				require.NoError(t, err)
				require.Equal(t, accountRow(db, ctx, keyFull),
					accountRow(db, ctx, keyNew),
					"stored=%+v next=%+v", stored, next)
			}
		}
		return nil
	}))

	liveRow := func(db queryer, ctx context.Context, key []byte) string {
		var pool []byte
		var utxo, reward, total string
		var registered bool
		var delegation, block, cert, updated, version int64
		require.NoError(t, db.QueryRowContext(ctx, store.dialect.Rebind(`
SELECT pool_key_hash, utxo_stake, reward_stake, total_stake, registered,
  pool_delegation_slot, pool_delegation_block_index,
  pool_delegation_cert_index, updated_slot, calculation_version
FROM reward_live_stake WHERE staking_key = ?`), key).Scan(
			&pool, &utxo, &reward, &total, &registered, &delegation, &block,
			&cert, &updated, &version))
		return fmt.Sprintf("%x|%t|%s|%s|%s|%t|%d|%d|%d|%d|%d", pool,
			pool == nil, utxo, reward, total, registered, delegation, block,
			cert, updated, version)
	}
	require.NoError(t, store.withWriteTransaction(nil, func(
		db queryer, ctx context.Context,
	) error {
		for i, pool := range [][]byte{nil, poolA} {
			keyKeep := []byte(fmt.Sprintf("keep-%d", i))
			keyFull := []byte(fmt.Sprintf("full-%d", i))
			for _, key := range [][]byte{keyKeep, keyFull} {
				_, err := db.ExecContext(ctx, store.dialect.Rebind(`
INSERT INTO reward_live_stake (credential_tag, staking_key, pool_key_hash,
  utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
  pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
  calculation_version)
VALUES (0, ?, ?, '1', '2', '3', ?, 4, 7, 7, 5, 2)`), key, pool, true)
				require.NoError(t, err)
			}
			args := func(key []byte) []any {
				return []any{
					0, key, pool, "10", "20", "30", true, int64(40), int64(50),
					int64(3),
				}
			}
			_, err := db.ExecContext(ctx, rewardLiveStakeUpsertKeepPoolQuery,
				args(keyKeep)...)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, rewardLiveStakeUpsertQuery,
				args(keyFull)...)
			require.NoError(t, err)
			require.Equal(t, liveRow(db, ctx, keyFull),
				liveRow(db, ctx, keyKeep))
		}
		return nil
	}))
}
