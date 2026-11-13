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
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// newPoolCurrentStore is newDepositHeldStore with foreign keys enforced, as in
// production: the pool_current_state rows of a pool a rollback deletes go
// with it through the foreign key.
func newPoolCurrentStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared&_pragma=foreign_keys(1)",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	seedMigratedSQLite(t, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// poolCurrentWorkload drives the production write paths over a seeded random
// certificate history and checks, after every step, that
// GetPoolRegistrationsEffectiveForEpoch returns what ranking the history
// returns and that pool_current_state holds what recomputing it from history
// would write.
type poolCurrentWorkload struct {
	t      *testing.T
	store  *Store
	rng    *rand.Rand
	pools  []lcommon.PoolKeyHash
	slot   uint64
	used   map[[2]uint64]struct{}
	placed [][2]uint64
}

func (w *poolCurrentWorkload) pool() lcommon.PoolKeyHash {
	return w.pools[w.rng.IntN(len(w.pools))]
}

func (w *poolCurrentWorkload) freshPosition(maxStep uint64) (uint64, uint32) {
	for {
		slot := w.slot + w.rng.Uint64N(maxStep)
		blockIndex := uint32(w.rng.IntN(3))
		key := [2]uint64{slot, uint64(blockIndex)}
		if _, ok := w.used[key]; ok {
			continue
		}
		w.used[key] = struct{}{}
		w.placed = append(w.placed, key)
		w.slot = slot
		return slot, blockIndex
	}
}

func poolCurrentTxHash(slot uint64, blockIndex uint32) []byte {
	hash := make([]byte, 32)
	hash[0] = byte(slot)
	hash[1] = byte(slot >> 8)
	hash[2] = byte(slot >> 16)
	hash[3] = byte(blockIndex)
	return hash
}

func (w *poolCurrentWorkload) apply(
	slot uint64,
	blockIndex uint32,
	cert lcommon.Certificate,
	contextSlot *uint64,
) {
	w.t.Helper()
	hash := poolCurrentTxHash(slot, blockIndex)
	tx := mockledger.NewTransactionBuilder().WithCertificates(cert)
	tx.WithId(hash)
	tx.WithValid(true)
	point := ocommon.Point{Slot: slot, Hash: hash}
	deposits := map[int]uint64{0: 500}
	if contextSlot != nil {
		require.NoError(w.t, w.store.SetTransactionLeiosClosureInContext(
			tx, point, blockIndex, deposits, true, *contextSlot, nil, 0,
		))
		return
	}
	require.NoError(w.t, w.store.SetTransaction(
		tx, point, blockIndex, deposits, false, nil, 0,
	))
}

func (w *poolCurrentWorkload) cert(
	pool lcommon.PoolKeyHash,
) lcommon.Certificate {
	if w.rng.IntN(5) < 3 {
		return depositHeldRegistration(pool)
	}
	return depositHeldRetirement(pool, uint64(w.rng.IntN(60)))
}

func (w *poolCurrentWorkload) step() string {
	switch roll := w.rng.IntN(25); {
	case roll < 10:
		slot, index := w.freshPosition(300)
		w.apply(slot, index, w.cert(w.pool()), nil)
		return "certificate"
	case roll < 13:
		slot, index := w.freshPosition(300)
		contextSlot := slot - 1 - w.rng.Uint64N(min(slot, 400))
		if slot == 0 {
			return "skip"
		}
		w.apply(slot, index, w.cert(w.pool()), &contextSlot)
		return "leios certificate"
	case roll < 15 && len(w.placed) > 0:
		// Replay of an applied position with a different certificate.
		key := w.placed[w.rng.IntN(len(w.placed))]
		if key[0] > w.slot {
			return "skip"
		}
		w.apply(key[0], uint32(key[1]), w.cert(w.pool()), nil)
		return "replay"
	case roll < 18 && w.slot > 0:
		target := w.rng.Uint64N(w.slot + 1)
		require.NoError(w.t, w.store.RestorePoolStateAtSlot(target, nil))
		require.NoError(w.t, w.store.DeleteCertificatesAfterSlot(target, nil))
		require.NoError(w.t, w.store.DeleteTransactionsAfterSlot(target, nil))
		w.slot = target
		for key := range w.used {
			if key[0] > target {
				delete(w.used, key)
			}
		}
		w.placed = slices.DeleteFunc(
			w.placed, func(key [2]uint64) bool { return key[0] > target },
		)
		return "rollback"
	case roll >= 23 && w.slot > 0:
		// Transaction rows alone: their certificate and context rows go and
		// the pool rows stay, so the pools' ranking keys change with nothing
		// else touching them.
		target := w.rng.Uint64N(w.slot + 1)
		require.NoError(w.t, w.store.DeleteTransactionsAfterSlot(target, nil))
		w.slot = target
		for key := range w.used {
			if key[0] > target {
				delete(w.used, key)
			}
		}
		w.placed = slices.DeleteFunc(
			w.placed, func(key [2]uint64) bool { return key[0] > target },
		)
		return "transaction rollback"
	case roll >= 20 && roll < 23 && len(w.placed) > 0:
		// A closure context recorded for a transaction already applied, with
		// no certificates to re-apply: the only effect on the pool rows is the
		// move of the effective slot its certificates rank by.
		key := w.placed[w.rng.IntN(len(w.placed))]
		if key[0] == 0 {
			return "skip"
		}
		contextSlot := key[0] - 1 - w.rng.Uint64N(min(key[0], 400))
		hash := poolCurrentTxHash(key[0], uint32(key[1]))
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(hash)
		tx.WithValid(true)
		require.NoError(w.t, w.store.SetTransactionLeiosClosureInContext(
			tx, ocommon.Point{Slot: key[0], Hash: hash}, uint32(key[1]),
			nil, true, contextSlot, nil, 0,
		))
		return "context write"
	default:
		pool := w.pool()
		require.NoError(w.t, w.store.RetirePools(
			nil, [][]byte{pool.Bytes()}, uint64(w.rng.IntN(60)), w.slot,
		))
		return "synthetic retirement"
	}
}

func (w *poolCurrentWorkload) tableRows() []string {
	var out []string
	collect := func(query string, columns int) {
		rows, err := w.store.writeDB.Query(query)
		require.NoError(w.t, err)
		defer rows.Close()
		for rows.Next() {
			v := make([]int64, columns)
			dest := make([]any, columns)
			for i := range v {
				dest[i] = &v[i]
			}
			require.NoError(w.t, rows.Scan(dest...))
			out = append(out, fmt.Sprint(v))
		}
		require.NoError(w.t, rows.Err())
	}
	collect(`
SELECT pool_id, horizon_slot
FROM pool_current_state ORDER BY pool_id`, 2)
	collect(`
SELECT pool_id, kind, event_rank, event_id, effective_slot, added_slot,
       synthetic, block_index, cert_index, registration_index, epoch
FROM pool_current_event ORDER BY pool_id, kind, event_rank`, 11)
	return out
}

func (w *poolCurrentWorkload) hashes() [][]byte {
	out := make([][]byte, len(w.pools))
	for i, pool := range w.pools {
		out[i] = pool.Bytes()
	}
	return out
}

func (w *poolCurrentWorkload) effective(
	fromHistory bool,
	start, ended, snapshot uint64,
) map[string]uint {
	w.t.Helper()
	fn := w.store.GetPoolRegistrationsEffectiveForEpoch
	if fromHistory {
		fn = w.store.getPoolRegistrationsEffectiveForEpochFromHistory
	}
	regs, err := fn(w.pools, start, ended, snapshot, nil)
	require.NoError(w.t, err)
	out := map[string]uint{}
	for _, reg := range regs {
		_, dup := out[string(reg.PoolKeyHash)]
		require.False(w.t, dup, "pool returned twice")
		out[string(reg.PoolKeyHash)] = reg.ID
	}
	return out
}

func (w *poolCurrentWorkload) check(step int, op string) {
	w.t.Helper()
	before := w.tableRows()
	db, ctx, err := w.store.dbFromTxn(nil)
	require.NoError(w.t, err)
	require.NoError(w.t, refreshPoolCurrentState(
		ctx, db, w.hashes(),
	), "step %d (%s)", step, op)
	require.Equal(
		w.t, w.tableRows(), before,
		"step %d (%s): pool_current_state differs from its recomputation"+
			" from certificate history", step, op,
	)
	starts := []uint64{0, 1, w.slot + 1, w.slot + 5000}
	starts = append(starts, w.rng.Uint64N(w.slot+400))
	if len(w.placed) > 0 {
		key := w.placed[w.rng.IntN(len(w.placed))]
		starts = append(starts, key[0], key[0]+1)
	}
	endeds := func(start uint64) []uint64 {
		return []uint64{0, start / 1000, start/1000 + 3}
	}
	snapshots := func(start uint64) []uint64 {
		return []uint64{start + 700, math.MaxInt64}
	}
	if poolCurrentRaceBuild {
		// The detector multiplies this SQL-only comparison about eighteen
		// times and finds nothing in it: -race keeps the ended-epoch and
		// snapshot bounds that decide a retirement and the window, one value
		// of each, and the un-instrumented check job runs the full matrix.
		endeds = func(start uint64) []uint64 { return []uint64{start/1000 + 3} }
		snapshots = func(uint64) []uint64 { return []uint64{math.MaxInt64} }
	}
	for _, start := range starts {
		for _, ended := range endeds(start) {
			for _, snapshot := range snapshots(start) {
				require.Equal(
					w.t,
					w.effective(true, start, ended, snapshot),
					w.effective(false, start, ended, snapshot),
					"step %d (%s): epochStart=%d ended=%d snapshot=%d",
					step, op, start, ended, snapshot,
				)
			}
		}
	}
}

func TestPoolCurrentStateMatchesCertificateHistory(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			if seed > 1 {
				// Each seed is single-goroutine SQLite work over its own
				// store, so the detector has nothing to observe and multiplies
				// its runtime (about 500s per seed under -race on a loaded
				// machine, against 30s without). Seed 1 still runs under
				// -race; all three run un-instrumented in the check job.
				testutil.SkipUnderRace(t, "seeded SQL equivalence workload; seed 1 covers -race")
			}
			store := newPoolCurrentStore(t)
			depositHeldEpochs(t, store, 12)
			w := &poolCurrentWorkload{
				t:     t,
				store: store,
				rng:   rand.New(rand.NewPCG(seed, 0x6298)),
				used:  map[[2]uint64]struct{}{},
			}
			for i := range 6 {
				w.pools = append(w.pools, depositHeldPoolKey(byte(0xc0+i)))
			}
			// An import baseline whose pools have history only after it.
			for i, pool := range w.pools[:3] {
				require.NoError(t, store.ImportPool(
					&models.Pool{PoolKeyHash: pool.Bytes()},
					&models.PoolRegistration{
						PoolKeyHash: pool.Bytes(),
						AddedSlot:   uint64(40 + i),
					},
					nil,
				))
			}
			require.NoError(t, store.RetirePools(
				nil, [][]byte{w.pools[2].Bytes()}, 1, 41,
			))
			w.slot = 100
			ops := map[string]int{}
			compared := 0
			for step := range 45 {
				op := w.step()
				ops[op]++
				w.check(step, op)
				compared += w.checkStatus(step, op)
			}
			require.Positive(t, compared, "the oracle never covered a pool")
			for _, op := range []string{
				"certificate", "leios certificate", "replay", "rollback",
				"synthetic retirement",
			} {
				require.Positive(t, ops[op], "workload never ran %q", op)
			}
			require.Positive(t, store.poolCurrentFastPools.Load())
			require.Positive(t, store.poolCurrentHistoryPools.Load())
		})
	}
}

// The migration backfill and the store's recomputation are separate copies of
// one ranking; they must agree on a history with every kind of event.
func TestPoolCurrentStateBackfillMatchesStore(t *testing.T) {
	t.Parallel()
	store := newPoolCurrentStore(t)
	depositHeldEpochs(t, store, 12)
	w := &poolCurrentWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewPCG(7, 0x6298)),
		used:  map[[2]uint64]struct{}{},
		slot:  100,
	}
	for i := range 8 {
		w.pools = append(w.pools, depositHeldPoolKey(byte(0xe0+i)))
	}
	for range 120 {
		w.step()
	}
	// A rollback late in the workload can leave no history; end with every
	// pool registered so the comparison is never of two empty tables.
	for _, pool := range w.pools {
		slot, index := w.freshPosition(300)
		w.apply(slot, index, depositHeldRegistration(pool), nil)
	}
	want := w.tableRows()
	require.NotEmpty(t, want)

	runPoolCurrentBackfill(t, store)
	require.Equal(t, want, w.tableRows())
}

// runPoolCurrentBackfill clears pool_current_state and refills it with the
// migration's backfill.
func runPoolCurrentBackfill(t *testing.T, store *Store) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	if store.dialect.Name() == "postgres" {
		registry, err = migrations.PostgresRegistry()
	} else if store.dialect.Name() == "mysql" {
		registry, err = migrations.MySQLRegistry()
	}
	require.NoError(t, err)
	var backfill migrations.Backfill
	for _, migration := range registry {
		if migration.Name == "pool-current-state" {
			backfill = migration.Backfill
		}
	}
	require.NotNil(t, backfill)
	_, err = store.writeDB.Exec("DELETE FROM pool_current_state")
	require.NoError(t, err)
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	cursor := ""
	for {
		result, err := backfill(context.Background(), migrations.Batch{
			Tx:     tx,
			Cursor: cursor,
			Limit:  3,
			Rebind: store.dialect.Rebind,
		})
		require.NoError(t, err)
		if result.Done {
			break
		}
		cursor = result.Cursor
	}
	require.NoError(t, tx.Commit())
}

func newPoolCurrentScenario(t *testing.T, seed byte) *poolCurrentWorkload {
	t.Helper()
	store := newPoolCurrentStore(t)
	depositHeldEpochs(t, store, 12)
	return &poolCurrentWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewPCG(uint64(seed), 1)),
		pools: []lcommon.PoolKeyHash{depositHeldPoolKey(seed)},
		used:  map[[2]uint64]struct{}{},
	}
}

// A closure context recorded for an applied transaction moves the effective
// slot of its pool certificates even when the replay carries no certificates
// to re-apply.
func TestPoolCurrentStateFollowsContextWrite(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xf1)
	pool := w.pools[0]
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(950, 0, depositHeldRegistration(pool), nil)
	w.slot = 1_000
	w.check(0, "applied")

	hash := poolCurrentTxHash(1_000, 0)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(hash)
	tx.WithValid(true)
	contextSlot := uint64(900)
	require.NoError(t, w.store.SetTransactionLeiosClosureInContext(
		tx, ocommon.Point{Slot: 1_000, Hash: hash}, 0, nil, true,
		contextSlot, nil, 0,
	))
	w.check(1, "context moved earlier than the other registration")
	regs, err := w.store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, 5_000, 4, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, uint64(950), regs[0].AddedSlot)
}

// DeleteTransactionsAfterSlot alone removes a transaction's certificate and
// context rows and leaves its pool rows, whose ranking keys change with them.
func TestPoolCurrentStateFollowsTransactionOnlyRollback(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentScenario(t, 0xf2)
	pool := w.pools[0]
	w.apply(1_800, 0, depositHeldRegistration(pool), nil)
	contextSlot := uint64(1_500)
	w.apply(2_000, 0, depositHeldRegistration(pool), &contextSlot)
	w.slot = 2_000
	w.check(0, "applied")
	regs, err := w.store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, 5_000, 4, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, uint64(1_800), regs[0].AddedSlot)

	require.NoError(t, w.store.DeleteTransactionsAfterSlot(1_900, nil))
	w.check(1, "transaction-only rollback")
	regs, err = w.store.GetPoolRegistrationsEffectiveForEpoch(
		w.pools, 5_000, 4, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, uint64(2_000), regs[0].AddedSlot)
}
