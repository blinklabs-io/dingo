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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// busyTimeoutMargin exceeds OpenCache's hardcoded 5s busy_timeout pragma,
// so a connection that only frees the WAL writer slot after this long
// forces any contending connection's own busy-retry loop to exhaust before
// the slot is released — the specific condition dingo #4091 hits, as
// opposed to ordinary contention a shorter hold resolves on its own well
// within busy_timeout (verified separately: a 100ms hold never fails either
// before or after the fix, because busy_timeout's retry genuinely covers
// short waits — see the handoff for that finding).
const busyTimeoutMargin = 5500 * time.Millisecond

// TestSaveAccountFetchChunkProgressDeferredWriteAfterConcurrentCommit is
// dingo #4091's minimal, deterministic reproduction.
//
// SaveAccountFetchChunkProgress's transaction (cache.go) always opens with
// two DELETEs targeting a brand-new chunk_hash nothing has written before.
// Both match zero rows on a fresh chunk, but SQLite still treats the
// statements as write-intending (a DELETE always opens a write cursor, even
// if it ultimately touches nothing) and, under c.db.Begin()'s default
// DEFERRED mode, grabs SQLite's single per-database WAL writer slot right
// there — and holds it for the rest of the transaction, not just its final
// COMMIT. OpenCache never bounds the connection pool, so
// accountFetchConcurrency's five chunk workers really do run on five
// distinct connections that then contend for that one slot.
//
// This test drives exactly that: connection A opens a transaction and runs
// SaveAccountFetchChunkProgress's own two opening DELETE statements
// verbatim (there is no seam to pause mid-transaction inside the real
// function, so this reproduces its exact SQL rather than calling it), which
// already claims the writer slot, and holds it for busyTimeoutMargin —
// standing in for five real chunk workers' large-chunk Prepare/Exec work
// colllectively outlasting busy_timeout. While A holds it, a second
// goroutine calls the real, unmodified SaveAccountFetchChunkProgress for a
// different chunk of the same (network, epoch) — exactly fetch_accounts.go's
// concurrent-worker shape.
//
// Pre-fix, B's connection retries against busy_timeout (5s), exhausts it
// before A releases, and fails with SQLITE_BUSY / "database is locked" —
// matching the issue's exact log line. Post-fix (a serialized connection
// pool), B's Begin() instead blocks at the Go connection-pool level with no
// fixed budget, so it simply waits for A to finish and then succeeds.
func TestSaveAccountFetchChunkProgressDeferredWriteAfterConcurrentCommit(
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
		network, epoch, "chunk-A",
	)
	require.NoError(t, err)
	_, err = txA.Exec(
		`DELETE FROM koios_account_checked WHERE network = ? AND epoch = ? AND chunk_hash = ?`,
		network, epoch, "chunk-A",
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
// within a unit test's timeframe (see the handoff for measured reproduction
// rates — it took roughly 60 concurrent workers sustained for several
// seconds to observe any SQLITE_BUSY at all, versus production's 5). It is
// kept because it exercises the real dispatch width and the real public
// API, and costs little to run even when it does not reproduce the
// failure on its own.
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

	for round := 0; round < rounds; round++ {
		epoch := uint64(round)
		var wg sync.WaitGroup
		errs := make([]error, workers)
		start := make(chan struct{})
		for w := 0; w < workers; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
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
			}()
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
