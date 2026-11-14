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
	"math/rand/v2"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestPostgresPoolCurrentState(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testPoolCurrentState(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLPoolCurrentState(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testPoolCurrentState(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

// testPoolCurrentState runs the seeded workload on a server dialect: every
// write path keeps pool_current_state equal to its recomputation, the store
// answers as the history ranking does, and the migration backfill reproduces
// the table.
func testPoolCurrentState(t *testing.T, store *Store) {
	t.Helper()
	depositHeldEpochs(t, store, 12)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewPCG(3, 0x6298)),
		used:  map[[2]uint64]struct{}{},
		slot:  100,
	}
	for i := range 5 {
		w.pools = append(w.pools, depositHeldPoolKey(byte(0xb0+i)))
	}
	for i, pool := range w.pools[:2] {
		require.NoError(t, store.ImportPool(
			&models.Pool{PoolKeyHash: pool.Bytes()},
			&models.PoolRegistration{
				PoolKeyHash: pool.Bytes(),
				AddedSlot:   uint64(40 + i),
			},
			nil,
		))
	}
	compared := 0
	for step := range 40 {
		op := w.step()
		w.check(step, op)
		compared += w.checkStatus(step, op)
	}
	require.Positive(t, compared, "the oracle never covered a pool")
	want := w.tableRows()
	require.NotEmpty(t, want)
	runPoolCurrentBackfill(t, store)
	require.Equal(t, want, w.tableRows())
}
