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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// BenchmarkTipSnapshotReadOnly and BenchmarkTipSnapshotReadUnderWriter are the
// regression sentinel for issue #2601: LedgerState.Tip, GetCurrentPParams,
// CurrentEpoch, and IsAtTip are read constantly from API handlers, chainsync,
// forging, and block validation, and used to take the embedded RWMutex's read
// lock. A concurrent writer stalled every reader (RWMutex with 1% concurrent
// writer measured ~591ns at 16 cores in #2601's prototype, versus ~133ns
// read-only), because each writer Lock blocks the shared reader counter. The
// fix (already landed, see LedgerState.consensus/tip atomic.Pointer fields
// and publishSnapshotsLocked) moved these fields behind immutable
// copy-on-write snapshots so reads never block on a concurrent writer.
//
// Comparing these two benchmarks' ns/op across -cpu=1,4,8,16 is the
// regression signal: on the current atomic.Pointer implementation both should
// stay flat and close to each other as core count rises. A reintroduced
// RWMutex (or any lock) on this read path would show
// BenchmarkTipSnapshotReadUnderWriter degrading sharply relative to
// BenchmarkTipSnapshotReadOnly as cores increase, exactly the negative
// scaling issue #1895 asks this framework to catch.
//
// BenchmarkConcurrentQueries (see benchmark_test.go) exercises database query
// load under concurrency; it is not a substitute for this benchmark, which
// targets the specific in-memory snapshot read/publish path #2601 describes.

func benchmarkTipSnapshotReaders(b *testing.B, ledgerState *LedgerState) {
	b.Helper()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ledgerState.Tip()
			_ = ledgerState.GetCurrentPParams()
			_ = ledgerState.CurrentEpoch()
			_ = ledgerState.IsAtTip()
		}
	})
}

// BenchmarkTipSnapshotReadOnly is the baseline: readers only, no concurrent
// writer. Run with -cpu=1,4,8,16 to see the scaling curve.
func BenchmarkTipSnapshotReadOnly(b *testing.B) {
	db, ledgerState := newBatchBenchmarkLedgerState(b, nil)
	defer dbtest.CloseDatabase(db)

	benchmarkTipSnapshotReaders(b, ledgerState)
}

// BenchmarkTipSnapshotReadUnderWriter adds a background writer that
// continuously republishes the consensus/tip snapshots (the same
// publishSnapshotsLocked call a real per-block writer makes), while readers
// run concurrently. Run with -cpu=1,4,8,16 to see the scaling curve; per
// #2601's regression, an implementation using a plain RWMutex here would
// degrade sharply at higher core counts, while the atomic.Pointer
// implementation should stay close to BenchmarkTipSnapshotReadOnly.
//
// The writer runs as fast as possible (deliberately more aggressive than a
// real per-block cadence) so a reintroduced lock's contention shows up
// clearly rather than being diluted by a realistic, much lower write rate.
func BenchmarkTipSnapshotReadUnderWriter(b *testing.B) {
	db, ledgerState := newBatchBenchmarkLedgerState(b, nil)
	defer dbtest.CloseDatabase(db)

	done := make(chan struct{})
	writerStopped := make(chan struct{})
	go func() {
		defer close(writerStopped)
		for {
			select {
			case <-done:
				return
			default:
				ledgerState.Lock()
				ledgerState.publishSnapshotsLocked()
				ledgerState.Unlock()
			}
		}
	}()
	defer func() {
		close(done)
		<-writerStopped
	}()

	benchmarkTipSnapshotReaders(b, ledgerState)
}
