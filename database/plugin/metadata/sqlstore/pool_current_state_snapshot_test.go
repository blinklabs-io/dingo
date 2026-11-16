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
	"math"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newPoolCurrentSnapshotScenario is newPoolCurrentScenario on a file-backed
// WAL database with separate read and write pools, so a rollback can commit
// while a read transaction holds its snapshot.
func newPoolCurrentSnapshotScenario(
	t *testing.T,
	seed byte,
) *poolCurrentWorkload {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "pool_current.db") +
		"?_pragma=synchronous(OFF)&_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	writeDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	writeDB.SetMaxOpenConns(1)
	readDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	readDB.SetMaxOpenConns(2)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         writeDB,
		ReadDB:          readDB,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	depositHeldEpochs(t, store, 12)
	return &poolCurrentWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewPCG(uint64(seed), 1)),
		pools: []lcommon.PoolKeyHash{depositHeldPoolKey(seed)},
		used:  map[[2]uint64]struct{}{},
	}
}

// rollbackOnceAfterLookup rolls the chain back to slot, then runs reapply,
// the first time a pool_current_state read has looked up its rows.
func (w *poolCurrentWorkload) rollbackOnceAfterLookup(
	slot uint64,
	reapply func(),
) *int {
	fired := new(int)
	w.store.poolCurrentStatesRead = func() {
		*fired++
		if *fired > 1 {
			return
		}
		require.NoError(w.t, w.store.RestorePoolStateAtSlot(slot, nil))
		require.NoError(w.t, w.store.DeleteCertificatesAfterSlot(slot, nil))
		require.NoError(w.t, w.store.DeleteTransactionsAfterSlot(slot, nil))
		reapply()
	}
	return fired
}

// Without a caller transaction the registration reads look up the stored
// rows and load the registrations they name in separate statements. A
// rollback that replaces the registration between them must not remove the
// pool from the result: both statements read one snapshot.
func TestPoolRegistrationReadsHoldOneSnapshotWithoutTxn(t *testing.T) {
	t.Parallel()
	for _, read := range []struct {
		name string
		call func(*Store, []lcommon.PoolKeyHash) (
			[]models.PoolRegistration, error,
		)
	}{
		{
			name: "at_slot",
			call: func(
				s *Store, pools []lcommon.PoolKeyHash,
			) ([]models.PoolRegistration, error) {
				return s.GetPoolRegistrationsAtSlot(pools, 5_000, nil)
			},
		},
		{
			name: "effective_for_epoch",
			call: func(
				s *Store, pools []lcommon.PoolKeyHash,
			) ([]models.PoolRegistration, error) {
				return s.GetPoolRegistrationsEffectiveForEpoch(
					pools, 5_000, 4, math.MaxInt64, nil,
				)
			},
		},
	} {
		t.Run(read.name, func(t *testing.T) {
			t.Parallel()
			w := newPoolCurrentSnapshotScenario(t, 0xd1)
			pool := w.pools[0]
			w.apply(1_000, 0, depositHeldRegistration(pool), nil)
			fired := w.rollbackOnceAfterLookup(500, func() {
				w.apply(600, 0, depositHeldRegistration(pool), nil)
			})

			regs, err := read.call(w.store, w.pools)
			require.NoError(t, err)
			require.Equal(t, 1, *fired, "the lookup hook must have run")
			require.Len(t, regs, 1)
			require.Equal(t, uint64(1_000), regs[0].AddedSlot,
				"the registration the snapshot holds")
		})
	}
}

// GetPoolStatus reads the pool's events and then the current epoch. A
// rollback between them that removes the retirement and moves the tip back
// must not combine the old retirement with the new epoch.
func TestPoolStatusHoldsOneSnapshotWithoutTxn(t *testing.T) {
	t.Parallel()
	w := newPoolCurrentSnapshotScenario(t, 0xd2)
	pool := w.pools[0]
	w.apply(1_000, 0, depositHeldRegistration(pool), nil)
	w.apply(1_500, 0, depositHeldRetirement(pool, 3), nil)
	require.NoError(t, w.store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 3_100, Hash: make([]byte, 32)},
	}, nil))
	fired := w.rollbackOnceAfterLookup(1_200, func() {
		require.NoError(t, w.store.SetTip(ochainsync.Tip{
			Point: ocommon.Point{Slot: 1_200, Hash: make([]byte, 32)},
		}, nil))
	})

	status, err := w.store.GetPoolStatus(pool, nil)
	require.NoError(t, err)
	require.Equal(t, 1, *fired, "the lookup hook must have run")
	require.Equal(t, models.PoolStatus{Exists: true}, status,
		"retired, as the snapshot holds it")
}
