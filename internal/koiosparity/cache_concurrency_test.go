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

package koiosparity

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sqlite "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// busyTimeoutMargin exceeds OpenCache's hardcoded 5s busy_timeout pragma,
// so a connection that only frees the WAL writer slot after this long
// forces any contending connection's own busy-retry loop to exhaust before
// the slot is released — the specific condition dingo #4091 hits, as
// opposed to ordinary contention a shorter hold resolves on its own well
// within busy_timeout: a 100ms hold fails neither before nor after the fix,
// because busy_timeout's retry genuinely covers short waits.
const busyTimeoutMargin = 5500 * time.Millisecond

// TestSaveAccountFetchChunkProgressWaitsOutSlowConcurrentWriter is dingo
// #4091's minimal, deterministic reproduction.
//
// SaveAccountFetchChunkProgress's transaction (cache.go) always opens with
// two DELETEs targeting a brand-new chunk_hash nothing has written before.
// Both match zero rows on a fresh chunk, but SQLite still treats the
// statements as write-intending (a DELETE always opens a write cursor, even
// if it ultimately touches nothing) and, under c.db.Begin()'s default
// DEFERRED mode, grabs SQLite's single per-database WAL writer slot right
// there — and holds it for the rest of the transaction, not just its final
// COMMIT. Before this fix OpenCache left the connection pool unbounded, so
// accountFetchConcurrency's five chunk workers really did run on five
// distinct connections that then contended for that one slot.
//
// This test drives exactly that: connection A opens a transaction and runs
// SaveAccountFetchChunkProgress's own two opening DELETE statements
// verbatim (there is no seam to pause mid-transaction inside the real
// function, so this reproduces its exact SQL rather than calling it), which
// already claims the writer slot, and holds it for busyTimeoutMargin —
// standing in for five real chunk workers' large-chunk Prepare/Exec work
// collectively outlasting busy_timeout. While A holds it, a second
// goroutine calls the real, unmodified SaveAccountFetchChunkProgress for a
// different chunk of the same (network, epoch) — exactly fetch_accounts.go's
// concurrent-worker shape.
//
// Pre-fix, B's connection retries against busy_timeout (5s), exhausts it
// before A releases, and fails with SQLITE_BUSY / "database is locked" —
// matching the issue's exact log line. Post-fix (a serialized connection
// pool), B's Begin() instead blocks at the Go connection-pool level with no
// fixed budget, so it simply waits for A to finish and then succeeds.
func TestSaveAccountFetchChunkProgressWaitsOutSlowConcurrentWriter(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const network = "preview"
	const epoch = uint64(4091)
	now := time.Now().UTC()

	// Connection A: open a deferred transaction and run
	// SaveAccountFetchChunkProgress's own opening statements verbatim
	// against a chunk hash nothing has ever written, claiming the WAL
	// writer slot exactly as the real function would.
	txA, err := cache.db.Begin()
	require.NoError(t, err)
	_, err = txA.Exec(
		`DELETE FROM koios_account_fetch_staged_rows WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
		network,
		epoch,
		"chunk-A",
	)
	require.NoError(t, err)
	_, err = txA.Exec(
		`DELETE FROM koios_account_checked WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
		network,
		epoch,
		"chunk-A",
	)
	require.NoError(t, err)

	// Connection B: a second, genuinely concurrent chunk worker — the real
	// production entry point — races to complete its own chunk for the same
	// (network, epoch) while A still holds the writer slot open.
	bErr := make(chan error, 1)
	go func() {
		bErr <- cache.SaveAccountFetchChunkProgress(
			network, epoch, "chunk-B",
			[]KoiosAccountRewards{{StakeAddress: "addr-B", RewardType: "member", Earned: "1"}},
			[]string{"addr-B"},
			now,
		)
	}()

	// Release A only after busyTimeoutMargin, so a pre-fix B has already
	// exhausted its own busy_timeout wait by the time A lets go.
	go func() {
		time.Sleep(busyTimeoutMargin)
		_ = txA.Commit()
	}()

	select {
	case err := <-bErr:
		if err != nil {
			if strings.Contains(err.Error(), "locked") ||
				strings.Contains(err.Error(), "SQLITE_BUSY") {
				t.Fatalf(
					"SaveAccountFetchChunkProgress for a concurrent chunk "+
						"hit SQLITE_BUSY because another chunk worker held "+
						"the WAL writer slot for %s, longer than "+
						"busy_timeout: %v",
					busyTimeoutMargin, err,
				)
			}
			require.NoError(t, err)
		}
	case <-time.After(busyTimeoutMargin + 4*time.Second):
		t.Fatalf(
			"SaveAccountFetchChunkProgress for a concurrent chunk did not "+
				"return within %s of a %s writer-slot hold — it is either "+
				"stuck in SQLite's busy retry loop or deadlocked",
			busyTimeoutMargin+4*time.Second, busyTimeoutMargin,
		)
	}
}

// TestSaveAccountFetchChunkProgressConcurrentWritersDoNotHitSQLiteBusy
// drives fetch_accounts.go's actual dispatch shape — accountFetchConcurrency
// (5) chunk workers calling SaveAccountFetchChunkProgress in parallel via
// the unmodified public API, repeated over many rounds — and fails if any
// call returns a SQLITE_BUSY-shaped error.
//
// This is a best-effort companion to the deterministic reproduction above,
// not the primary regression guard: at the production concurrency of 5
// workers this defect is timing-dependent and was not observed at all
// within a unit test's timeframe: measured here, 5 workers produced no
// SQLITE_BUSY across thousands of attempts, and it took roughly 60
// concurrent workers sustained for several seconds to see any at all
// (29 of 71,468 attempts). It is kept because it exercises the real
// dispatch width and the real public API, and costs little to run even
// when it does not reproduce the failure on its own.
func TestSaveAccountFetchChunkProgressConcurrentWritersDoNotHitSQLiteBusy(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const (
		workers = accountFetchConcurrency // 5, matches fetch_accounts.go's dispatch width
		rounds  = 40
	)
	network := "preview"
	now := time.Now().UTC()

	for round := range rounds {
		epoch := uint64(round)
		var wg sync.WaitGroup
		errs := make([]error, workers)
		start := make(chan struct{})
		for w := range workers {
			wg.Go(func() {
				<-start
				chunkHash := "chunk-" + strconv.Itoa(w)
				addr := "addr-" + strconv.Itoa(w)
				errs[w] = cache.SaveAccountFetchChunkProgress(
					network,
					epoch,
					chunkHash,
					[]KoiosAccountRewards{
						{StakeAddress: addr, RewardType: "member", Earned: "1"},
					},
					[]string{addr},
					now,
				)
			})
		}
		close(start)
		wg.Wait()

		for w, err := range errs {
			if err == nil {
				continue
			}
			if strings.Contains(err.Error(), "SQLITE_BUSY") ||
				strings.Contains(err.Error(), "database is locked") {
				t.Fatalf(
					"round %d worker %d: SaveAccountFetchChunkProgress hit "+
						"SQLITE_BUSY under its own designed concurrency: %v",
					round, w, err,
				)
			}
			require.NoError(t, err)
		}
	}
}

// TestOpenCacheLegacyColumnMigrationDoesNotDeadlock covers the hazard the
// single-connection pool above creates for the rest of the package: with
// SetMaxOpenConns(1), any code path that asks for a second connection while
// still holding the first blocks forever, because database/sql queues the
// request on an unbuffered channel with no deadline (OpenCache's write paths
// use the context-free Exec/Begin, so there is nothing to cancel it).
//
// createCacheSchema's legacy-column migration was exactly that shape: it held
// an open *sql.Rows from its pragma_table_info probe — closed only by a
// deferred Close at function exit — while issuing ALTER TABLE ... DROP COLUMN
// on the same *sql.DB. It reaches that Exec only when the legacy column is
// actually present, i.e. against a cache.db written by an older dingo, so a
// fresh-file test never exercised it and the node would simply hang inside
// OpenCache with no error and no timeout. The probe now lives in
// columnExists, whose rows are closed before it returns.
//
// The sibling probe in addColumnIfMissing was safe by contrast: it Execs only
// when rows.Next() returned false, and an exhausted *sql.Rows has already
// released its connection. It shares columnExists so the invariant holds in
// one place rather than by accident in two.
func TestOpenCacheLegacyColumnMigrationDoesNotDeadlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(path, nil)
	require.NoError(t, err)

	// Recreate an older cache file: the three columns createCacheSchema
	// migrates away, as written by a dingo that still had them.
	legacy := [][2]string{
		{"koios_epoch_info", "pool_cnt"},
		{"koios_epoch_info", "delegator_cnt"},
		{"koios_totals", "deposits_d_rep"},
	}
	for _, col := range legacy {
		_, err = cache.db.Exec(
			"ALTER TABLE " + col[0] + " ADD COLUMN " + col[1] + " INTEGER",
		)
		require.NoError(t, err)
	}
	require.NoError(t, cache.Close())

	reopened := make(chan *Cache, 1)
	reopenErr := make(chan error, 1)
	go func() {
		c, err := OpenCache(path, nil)
		if err != nil {
			reopenErr <- err
			return
		}
		reopened <- c
	}()

	select {
	case err := <-reopenErr:
		require.NoError(t, err)
	case c := <-reopened:
		defer c.Close() //nolint:errcheck
		for _, col := range legacy {
			var n int
			require.NoError(t, c.db.QueryRow(
				"SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?",
				col[0], col[1],
			).Scan(&n))
			require.Zerof(
				t, n,
				"%s.%s should have been dropped by the migration",
				col[0], col[1],
			)
		}
	case <-time.After(30 * time.Second):
		t.Fatal(
			"OpenCache did not return within 30s against a cache.db carrying " +
				"legacy columns: createCacheSchema's DROP COLUMN Exec is " +
				"waiting for a second connection while its pragma_table_info " +
				"rows still hold the only one",
		)
	}
}

// sqliteBusySnapshot is SQLITE_BUSY_SNAPSHOT, SQLite's extended result code
// for a deferred transaction that took a read snapshot and then could not
// upgrade to a writer because another connection committed in between
// (SQLITE_BUSY | 2<<8).
const sqliteBusySnapshot = 517

// TestCacheWritesNeverUpgradeAReadSnapshot guards the statement order inside
// every Cache transaction that writes.
//
// c.db.Begin() is DEFERRED, so a transaction whose first statement is a
// SELECT opens as a reader and only upgrades to a writer when a write
// statement runs. Under WAL, any other connection that commits in that window
// makes the upgrade fail with SQLITE_BUSY_SNAPSHOT — a stale snapshot rather
// than lock contention, so busy_timeout's retry cannot wait it out: no amount
// of waiting makes an already-taken snapshot current, only a new transaction
// does. Issuing a write first takes the writer slot immediately, so the
// transaction never holds a snapshot it has to upgrade, and any read that
// follows runs under a lock no other connection can commit against.
//
// The window is microseconds wide and the real functions expose no seam to
// pause inside, so this drives it by volume rather than by interleaving: one
// connection committing continuously while another performs 3000 writes.
//
// The assertion is on SQLITE_BUSY_SNAPSHOT specifically rather than on any
// error, because only that code is structurally impossible once a write runs
// first. A plain SQLITE_BUSY stays reachable — it is ordinary writer-slot
// contention against busy_timeout, the dingo #4091 class — and a starved
// machine can produce one without the ordering having regressed: measured
// under six concurrent copies of this package's suite, the fixed order
// produced 1 plain SQLITE_BUSY in 3000 on three runs and zero
// SQLITE_BUSY_SNAPSHOT anywhere.
//
// This is the shape TestObserverBackfillsParamsForAPreExistingCache hit on
// CI, where the test's own require.Eventually polling supplied the second
// connection by reopening the same cache file every 10ms while the observer
// backfilled.
func TestCacheWritesNeverUpgradeAReadSnapshot(t *testing.T) {
	const network = "preview"
	const sourceURL = "https://first.example/api/v1"
	now := time.Now().UTC()

	cases := []struct {
		name string
		// write performs one call of the path under test. Pre-fix
		// measurements are the SQLITE_BUSY_SNAPSHOT count per 3000 calls on
		// an unloaded machine.
		write func(*testing.T, *Cache, int)
	}{
		{
			// assertClaimedSource's SELECT used to run before the caller's
			// write. Pre-fix: 891 to 1321 of 3000 on 8 of 8 runs.
			name: "gated write via withClaimedSource",
			write: func(t *testing.T, c *Cache, i int) {
				t.Helper()
				recordSnapshotBusy(t, c.UpsertEpochParams(KoiosEpochParams{
					Network:   network,
					Epoch:     uint64(i),
					Era:       "alonzo",
					FetchedAt: now,
				}))
			},
		},
		{
			// RecordKoiosSource has to read the previous root before it can
			// decide what to discard, so its read cannot move after its
			// writes; it claims the writer slot with a no-op UPDATE instead.
			// Pre-fix: 30 to 55 of 3000 on 4 of 4 runs.
			name: "RecordKoiosSource",
			write: func(t *testing.T, c *Cache, _ int) {
				t.Helper()
				_, err := c.RecordKoiosSource(network, sourceURL, now)
				recordSnapshotBusy(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const iterations = 3000
			path := filepath.Join(t.TempDir(), "cache.db")

			writer, err := OpenCache(path, nil)
			require.NoError(t, err)
			defer writer.Close() //nolint:errcheck
			_, err = writer.RecordKoiosSource(network, sourceURL, now)
			require.NoError(t, err)

			// A second connection to the same file, committing continuously
			// so the WAL keeps advancing under the writer's transactions. It
			// writes another network, so nothing it does can legitimately
			// trip the claimed-source gate.
			other, err := OpenCache(path, nil)
			require.NoError(t, err)
			defer other.Close() //nolint:errcheck
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for epoch := uint64(0); ; epoch++ {
					select {
					case <-stop:
						return
					default:
					}
					_ = other.UpsertEpochInfo(KoiosEpochInfo{
						Network:      "wal-churn",
						Epoch:        epoch % 8,
						ActiveStake:  "1",
						Fees:         "1",
						TotalRewards: "1",
						EpochEndTime: now,
						FetchedAt:    now,
					})
				}
			}()

			for i := range iterations {
				tc.write(t, writer, i)
			}
			close(stop)
			<-done
		})
	}
}

// recordSnapshotBusy fails the test when err is SQLITE_BUSY_SNAPSHOT, and
// tolerates every other error: only the snapshot code says a transaction
// read before it wrote. See TestCacheWritesNeverUpgradeAReadSnapshot.
func recordSnapshotBusy(t *testing.T, err error) {
	t.Helper()
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) &&
		sqliteErr.Code() == sqliteBusySnapshot {
		t.Fatalf(
			"write failed with SQLITE_BUSY_SNAPSHOT against a concurrently "+
				"committing connection, so the transaction is taking a read "+
				"snapshot before its first write again: %v",
			err,
		)
	}
}
