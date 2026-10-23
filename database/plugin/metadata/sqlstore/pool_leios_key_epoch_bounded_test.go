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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/leioskeyepoch"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// countingLeiosKeyQuerier counts the statements leioskeyepoch issues.
type countingLeiosKeyQuerier struct {
	db      *sql.DB
	queries int
}

func (q *countingLeiosKeyQuerier) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	q.queries++
	return q.db.QueryContext(ctx, query, args...)
}

func (q *countingLeiosKeyQuerier) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	q.queries++
	return q.db.QueryRowContext(ctx, query, args...)
}

func leiosKeyEpochPoolID(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
) int64 {
	t.Helper()
	var id int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM pool WHERE pool_key_hash = ?`,
		pool.Bytes(),
	).Scan(&id))
	return id
}

// leiosKeyEpochQueries derives the registration epoch of a keyed registration
// at slot through a counting querier and returns the statement count.
func leiosKeyEpochQueries(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
) (int, *uint64) {
	t.Helper()
	querier := &countingLeiosKeyQuerier{db: store.writeDB}
	epoch, ageUnknown, err := leioskeyepoch.Registration(
		context.Background(),
		querier,
		leiosKeyEpochPoolID(t, store, pool),
		leioskeyepoch.Position{Slot: slot},
		slot,
		bytes.Repeat([]byte{0x11}, 96),
		bytes.Repeat([]byte{0x21}, 48),
	)
	require.NoError(t, err)
	require.False(t, ageUnknown)
	return querier.queries, epoch
}

// Deriving a registration's epoch reads a fixed number of rows from the
// persisted state, however many registrations and retirements the pool has
// already made, so block application does not grow with a pool's history.
func TestPoolLeiosKeyEpochWorkIsIndependentOfPoolHistory(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	short := depositHeldPoolKey(0xe1)
	long := depositHeldPoolKey(0xe2)

	writeDepositHeldCert(t, store, 2_050, 0, leiosKeyEpochCert(short, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_050, 0, leiosKeyEpochCert(short, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 4_050, 0, leiosKeyEpochCert(short, 0x11, 0x21), 0)

	writeDepositHeldCert(t, store, 1_050, 0, leiosKeyEpochCert(long, 0x11, 0x21), 500)
	for epoch := uint64(1); epoch <= 5; epoch++ {
		for i := range uint64(8) {
			seed := byte(0x11 + i%3) // #nosec G115 -- below three
			writeDepositHeldCert(
				t, store,
				epoch*depositHeldEpochLength+100+i*20, 0,
				leiosKeyEpochCert(long, seed, seed+0x10), 0,
			)
		}
	}
	// Reaped at the boundary into 6, then registered again in 7 and 8.
	writeDepositHeldCert(t, store, 5_900, 0, depositHeldRetirement(long, 6), 0)
	writeDepositHeldCert(t, store, 7_100, 0, leiosKeyEpochCert(long, 0x12, 0x22), 500)
	writeDepositHeldCert(t, store, 8_100, 0, leiosKeyEpochCert(long, 0x11, 0x21), 0)

	shortQueries, shortEpoch := leiosKeyEpochQueries(t, store, short, 9_100)
	longQueries, longEpoch := leiosKeyEpochQueries(t, store, long, 9_100)

	// Both pools hold 0x11 in epoch 9: short since the boundary into 5, where
	// it replaced the key adopted at 4, and long since the boundary into 9.
	require.NotNil(t, shortEpoch)
	require.NotNil(t, longEpoch)
	require.Equal(t, uint64(5), *shortEpoch)
	require.Equal(t, uint64(9), *longEpoch)
	require.Equal(t, shortQueries, longQueries)
	require.LessOrEqual(t, longQueries, 8)
}
