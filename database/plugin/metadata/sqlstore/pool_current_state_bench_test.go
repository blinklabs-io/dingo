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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

const (
	benchPoolCount   = 6_400
	benchEpochLength = 432_000
	benchEpochs      = 463
	benchEpochSlot   = benchEpochLength * benchEpochs
)

// seedPoolHistory writes pools with a mainnet-shaped certificate history: each
// pool gets one to twelve registrations (about 42,000 rows in all), every one
// with its certificate and transaction rows, and one in four pools a
// retirement, over benchEpochs epochs of mainnet length. One pool in twenty
// registers again in the epoch before the newest and one in fifty schedules a
// retirement in the newest, so the epoch-boundary window holds pending events.
// Rows are inserted directly because the benchmark measures reads.
func seedPoolHistory(b *testing.B, store *Store) []lcommon.PoolKeyHash {
	b.Helper()
	rng := rand.New(rand.NewPCG(6298, 1))
	for epoch := range uint64(benchEpochs) {
		require.NoError(b, store.SetEpoch(
			epoch*benchEpochLength, epoch, nil, nil, nil, nil, 6, 1,
			benchEpochLength, nil,
		))
	}
	tx, err := store.writeDB.Begin()
	require.NoError(b, err)
	prep := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(b, err)
		b.Cleanup(func() { stmt.Close() })
		return stmt
	}
	insertPool := prep(
		`INSERT INTO pool (id, pool_key_hash, pledge, cost, latest_op_cert_sequence) VALUES (?, ?, '0', '0', 0)`,
	)
	insertTx := prep(`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`)
	insertApplied := prep(
		`INSERT INTO applied_transaction (tx_id, slot, block_index, valid) VALUES (?, ?, ?, 1)`,
	)
	insertCert := prep(
		`INSERT INTO certs (id, transaction_id, certificate_id, slot, cert_index, cert_type) VALUES (?, ?, 0, ?, 0, 3)`,
	)
	insertReg := prep(
		`INSERT INTO pool_registration (id, pool_key_hash, certificate_id, pool_id, added_slot, pledge, cost, registration_index) VALUES (?, ?, ?, ?, ?, '0', '0', 0)`,
	)
	insertRet := prep(
		`INSERT INTO pool_retirement (id, pool_key_hash, certificate_id, pool_id, epoch, added_slot) VALUES (?, ?, ?, ?, ?, ?)`,
	)
	var next int64
	cert := func(slot int64) int64 {
		next++
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, uint64(next))
		_, err := insertTx.Exec(next, hash)
		require.NoError(b, err)
		_, err = insertApplied.Exec(next, slot, rng.IntN(40))
		require.NoError(b, err)
		_, err = insertCert.Exec(next, next, slot)
		require.NoError(b, err)
		return next
	}
	pools := make([]lcommon.PoolKeyHash, benchPoolCount)
	for i := range pools {
		var hash [28]byte
		binary.BigEndian.PutUint64(hash[:], uint64(i+1))
		pools[i] = lcommon.PoolKeyHash(lcommon.NewBlake2b224(hash[:]))
		id := int64(i + 1)
		_, err := insertPool.Exec(id, hash[:])
		require.NoError(b, err)
		for range 1 + rng.IntN(12) {
			slot := 1 + rng.Int64N(benchEpochSlot-1)
			certID := cert(slot)
			_, err := insertReg.Exec(certID, hash[:], certID, id, slot)
			require.NoError(b, err)
		}
		if i%20 == 0 {
			slot := int64(benchEpochs-2)*benchEpochLength + 1 +
				rng.Int64N(benchEpochLength-1)
			certID := cert(slot)
			_, err := insertReg.Exec(certID, hash[:], certID, id, slot)
			require.NoError(b, err)
		}
		if i%50 == 1 {
			slot := int64(benchEpochs-1)*benchEpochLength + 1 +
				rng.Int64N(benchEpochLength-1)
			certID := cert(slot)
			_, err := insertRet.Exec(
				certID, hash[:], certID, id, benchEpochs+9, slot,
			)
			require.NoError(b, err)
		}
		if i%4 == 0 {
			slot := 1 + rng.Int64N(benchEpochSlot-1)
			certID := cert(slot)
			_, err := insertRet.Exec(
				certID,
				hash[:],
				certID,
				id,
				rng.Int64N(500),
				slot,
			)
			require.NoError(b, err)
		}
	}
	require.NoError(b, tx.Commit())
	return pools
}

// materializePoolCurrentState fills pool_current_state for the seeded pools.
func materializePoolCurrentState(
	b *testing.B,
	store *Store,
	pools []lcommon.PoolKeyHash,
) {
	b.Helper()
	for start := 0; start < len(pools); start += 1000 {
		hashes := make([][]byte, 0, 1000)
		for _, pool := range pools[start:min(start+1000, len(pools))] {
			hashes = append(hashes, pool.Bytes())
		}
		require.NoError(b, refreshPoolCurrentState(
			context.Background(), store.writeDB, hashes,
		))
	}
}

func newBenchStore(b *testing.B) *Store {
	b.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(b, err)
	seedMigratedSQLite(b, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(b, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(b, err)
	require.NoError(b, store.Start(context.Background()))
	b.Cleanup(func() { require.NoError(b, store.Close()) })
	return store
}

// poolReadShape is one read a caller makes, as it ran before the stored state
// (history) and as it runs now (current).
type poolReadShape struct {
	name    string
	history func(i int) error
	current func(i int) error
}

// BenchmarkPoolCurrentStateReads times each read shape on the history path and
// on the pool_current_state path in alternation inside one process, so both see
// the same cache and machine state, and reports the median of the rounds.
// Shapes: PoolCurrentState, the per-block read (the pool's status and its
// epoch-effective registration); EpochBoundary, every pool's epoch-effective
// registration; AtSlot, every pool's latest registration at the tip slot.
func BenchmarkPoolCurrentStateReads(b *testing.B) {
	// Not t.Parallel: testing.B timing measures the machine.
	store := newBenchStore(b)
	pools := seedPoolHistory(b, store)
	materializePoolCurrentState(b, store, pools)
	ids := func(regs []models.PoolRegistration) []uint {
		out := make([]uint, len(regs))
		for i, reg := range regs {
			out[i] = reg.ID
		}
		slices.Sort(out)
		return out
	}
	boundaryStart := uint64(benchEpochs-2) * benchEpochLength
	tipStart := uint64(benchEpochs-1) * benchEpochLength
	tipSlot := tipStart + benchEpochLength/2
	oldCurrentState := func(i int) error {
		pool := pools[i%len(pools)]
		stored, err := store.GetPool(pool, false, nil)
		if err != nil {
			return err
		}
		if stored == nil {
			return nil
		}
		_, err = store.getPoolRegistrationsEffectiveForEpochFromHistory(
			[]lcommon.PoolKeyHash{pool}, tipStart, benchEpochs-1,
			math.MaxInt64, nil,
		)
		return err
	}
	newCurrentState := func(i int) error {
		pool := pools[i%len(pools)]
		status, err := store.GetPoolStatus(pool, nil)
		if err != nil || !status.Active {
			return err
		}
		_, err = store.GetPoolRegistrationsEffectiveForEpoch(
			[]lcommon.PoolKeyHash{pool}, tipStart, benchEpochs-1,
			math.MaxInt64, nil,
		)
		return err
	}
	for _, check := range []struct {
		name          string
		history, rows func() ([]models.PoolRegistration, error)
	}{
		{
			"EpochBoundary",
			func() ([]models.PoolRegistration, error) {
				return store.getPoolRegistrationsEffectiveForEpochFromHistory(
					pools, boundaryStart, benchEpochs-2, tipStart-1, nil,
				)
			},
			func() ([]models.PoolRegistration, error) {
				return store.GetPoolRegistrationsEffectiveForEpoch(
					pools, boundaryStart, benchEpochs-2, tipStart-1, nil,
				)
			},
		},
		{
			"AtSlot",
			func() ([]models.PoolRegistration, error) {
				return store.getPoolRegistrationsAtSlotFromHistory(
					pools, tipSlot, nil,
				)
			},
			func() ([]models.PoolRegistration, error) {
				return store.GetPoolRegistrationsAtSlot(pools, tipSlot, nil)
			},
		},
	} {
		want, err := check.history()
		require.NoError(b, err)
		got, err := check.rows()
		require.NoError(b, err)
		require.NotEmpty(b, want)
		require.Equal(b, ids(want), ids(got), check.name)
	}
	for i := range pools {
		status, err := store.GetPoolStatus(pools[i], nil)
		require.NoError(b, err)
		stored, err := store.GetPool(pools[i], false, nil)
		require.NoError(b, err)
		require.Equal(b, stored != nil, status.Active)
	}
	shapes := []poolReadShape{
		{"PoolCurrentState", oldCurrentState, newCurrentState},
		{
			"EpochBoundary",
			func(int) error {
				_, err := store.getPoolRegistrationsEffectiveForEpochFromHistory(
					pools, boundaryStart, benchEpochs-2, tipStart-1, nil,
				)
				return err
			},
			func(int) error {
				_, err := store.GetPoolRegistrationsEffectiveForEpoch(
					pools, boundaryStart, benchEpochs-2, tipStart-1, nil,
				)
				return err
			},
		},
		{
			"AtSlot",
			func(int) error {
				_, err := store.getPoolRegistrationsAtSlotFromHistory(
					pools, tipSlot, nil,
				)
				return err
			},
			func(int) error {
				_, err := store.GetPoolRegistrationsAtSlot(pools, tipSlot, nil)
				return err
			},
		},
	}
	const rounds = 9
	for _, shape := range shapes {
		perRound := 1
		if shape.name == "PoolCurrentState" {
			perRound = 200
		}
		var history, current []float64
		for range rounds {
			for _, side := range []struct {
				run func(int) error
				out *[]float64
			}{{shape.history, &history}, {shape.current, &current}} {
				start := time.Now()
				for i := range perRound {
					require.NoError(b, side.run(i))
				}
				*side.out = append(
					*side.out,
					float64(time.Since(start).Nanoseconds())/float64(perRound),
				)
			}
		}
		slices.Sort(history)
		slices.Sort(current)
		b.Logf(
			"%s median ns/op history=%.0f current=%.0f (%d rounds x %d calls)",
			shape.name, history[rounds/2], current[rounds/2], rounds, perRound,
		)
		b.ReportMetric(history[rounds/2], shape.name+"-history-ns/op")
		b.ReportMetric(current[rounds/2], shape.name+"-current-ns/op")
	}
}
