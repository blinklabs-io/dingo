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
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func accountRowText(t *testing.T, db *sql.DB, key []byte) string {
	t.Helper()
	var text string
	require.NoError(t, db.QueryRow(`
SELECT printf('%s|%s|%s|%s|%s|%s|%s|%s', hex(coalesce(pool, x'')), pool IS NULL,
  hex(coalesce(drep, x'')), drep IS NULL, drep_type, active, added_slot,
  created_slot) FROM account WHERE staking_key = ?`, key).Scan(&text))
	return text
}

// TestUpdateCertificateAccountRowMatchesFullUpsert applies every stored and
// next certificate state to two accounts, one through the changed-columns
// upsert and one through the full upsert, and requires identical rows and the
// same autoincrement reservation.
func TestUpdateCertificateAccountRowMatchesFullUpsert(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	poolA, poolB := []byte{0xa1}, []byte{0xb1}
	drepX, drepY := []byte{0xc1}, []byte{0xd1}
	states := []certificateAccountStored{}
	for _, active := range []bool{true, false} {
		for _, pool := range [][]byte{nil, poolA, poolB} {
			for _, drep := range [][]byte{nil, drepX, drepY} {
				for _, drepType := range []uint64{0, 1, 2} {
					states = append(states, certificateAccountStored{
						active: active, pool: pool, drep: drep, drepType: drepType,
					})
				}
			}
		}
	}
	n := 0
	for _, stored := range states {
		for _, next := range states {
			n++
			keyNew := []byte(fmt.Sprintf("new-%d", n))
			keyFull := []byte(fmt.Sprintf("full-%d", n))
			for _, key := range [][]byte{keyNew, keyFull} {
				_, err := store.writeDB.Exec(
					`
INSERT INTO account (staking_key, credential_tag, pool, drep, added_slot,
  created_slot, reward, drep_type, active, expiration_epoch)
VALUES (?, 0, ?, ?, 10, 5, '0', ?, ?, 0)`,
					key,
					stored.pool,
					stored.drep,
					stored.drepType,
					stored.active,
				)
				require.NoError(t, err)
			}
			require.NoError(t, store.updateCertificateAccountRow(
				ctx, store.writeDB, 0, keyNew, 20, stored, next,
			))
			_, err := store.writeDB.Exec(
				upsertAccountSQL,
				keyFull,
				0,
				next.pool,
				next.drep,
				20,
				20,
				next.drepType,
				next.active,
			)
			require.NoError(t, err)
			require.Equal(t,
				accountRowText(t, store.writeDB, keyFull),
				accountRowText(t, store.writeDB, keyNew),
				"stored=%+v next=%+v", stored, next)
		}
	}
	require.Greater(t, n, 1000)
	var seq int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT seq FROM sqlite_sequence WHERE name = 'account'",
	).Scan(&seq))
	require.Equal(t, int64(4*n), seq,
		"each upsert must reserve an id, as the full upsert does")
}

func liveStakeRowText(t *testing.T, db *sql.DB, key []byte) string {
	t.Helper()
	var text string
	require.NoError(t, db.QueryRow(`
SELECT printf('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', hex(coalesce(pool_key_hash, x'')),
  pool_key_hash IS NULL, utxo_stake, reward_stake, total_stake, registered,
  pool_delegation_slot, pool_delegation_block_index, pool_delegation_cert_index,
  updated_slot || ':' || calculation_version)
FROM reward_live_stake WHERE staking_key = ?`, key).Scan(&text))
	return text
}

// TestLiveStakeKeepPoolUpsertMatchesFullUpsert requires the pool-preserving
// upsert to leave a row exactly as the full upsert does when the pool is
// unchanged, including rows a rebuild left with non-zero delegation order
// columns, and to reserve the same autoincrement id.
func TestLiveStakeKeepPoolUpsertMatchesFullUpsert(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	for i, pool := range [][]byte{nil, {0xa1}} {
		for _, blockIndex := range []int{0, 7} {
			keyKeep := []byte(fmt.Sprintf("keep-%d-%d", i, blockIndex))
			keyFull := []byte(fmt.Sprintf("full-%d-%d", i, blockIndex))
			for _, key := range [][]byte{keyKeep, keyFull} {
				_, err := store.writeDB.Exec(`
INSERT INTO reward_live_stake (credential_tag, staking_key, pool_key_hash,
  utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
  pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
  calculation_version)
VALUES (0, ?, ?, '1', '2', '3', 1, 4, ?, ?, 5, 2)`, key, pool, blockIndex, blockIndex)
				require.NoError(t, err)
			}
			args := func(key []byte) []any {
				return []any{
					0, key, pool, "10", "20", "30", true, int64(40), int64(50),
					int64(3),
				}
			}
			_, err := s2(
				ctx,
				store,
				rewardLiveStakeUpsertKeepPoolQuery,
				args(keyKeep),
			)
			require.NoError(t, err)
			_, err = s2(ctx, store, rewardLiveStakeUpsertQuery, args(keyFull))
			require.NoError(t, err)
			require.Equal(t,
				liveStakeRowText(t, store.writeDB, keyFull),
				liveStakeRowText(t, store.writeDB, keyKeep))
		}
	}
	var seq int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT seq FROM sqlite_sequence WHERE name = 'reward_live_stake'",
	).Scan(&seq))
	require.Equal(t, int64(8+8), seq,
		"each upsert must reserve an id, as the full upsert does")
}

func s2(
	ctx context.Context,
	s *Store,
	q string,
	args []any,
) (sql.Result, error) {
	return s.writeDB.ExecContext(ctx, q, args...)
}
