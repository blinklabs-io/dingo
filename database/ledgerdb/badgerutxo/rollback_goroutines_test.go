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

package badgerutxo_test

import (
	"encoding/binary"
	"runtime/metrics"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func goroutinesCreated() uint64 {
	s := []metrics.Sample{{Name: "/sched/goroutines-created:goroutines"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// A rollback reads one journal record and copies each state journal value
// itself, so it has no use for Badger's value prefetch, which starts a
// goroutine per prefetched item. Undoing n records must start a number of
// goroutines that does not grow with the prefetch size times n.
func TestRollbackStartsNoPrefetchGoroutines(t *testing.T) {
	// Not t.Parallel: the assertion is a delta of the process-wide
	// goroutines-created counter, which a concurrent test would move.
	const n = 300
	s := smallRollbackStore(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	for i := range n {
		p := point(uint64(i+1), byte(i%250+1)) //nolint:gosec // a test index
		_, err := s.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
			Changes: []badgerutxo.Change{{Point: p, Added: []badgerutxo.Output{
				out(byte(i%250+1), uint32(i)), //nolint:gosec // a test index
			}}},
			Rows: journaledRows(p.Slot, 7, 3, 1),
		})
		require.NoError(t, err)
	}
	before := goroutinesCreated()
	_, err := s.Rollback(start)
	require.NoError(t, err)
	created := goroutinesCreated() - before
	requireTip(t, s, start)
	// Prefetch costs up to 100 goroutines per undone record, so ~n*100.
	// Commits and the rollback's own bookkeeping stay far below that.
	require.Less(
		t, created, uint64(5*n),
		"rollback of %d records started %d goroutines", n, created,
	)
}

// Each undone record or state journal batch deletes the newest entries, and a
// reverse scan from the top of the prefix walks every entry deleted since the
// last compaction. A rollback of n records must start a bounded number of
// such scans, whether it undoes the rows record by record or drains them
// first in transactions of their own, and still restore each row as of the
// target.
func TestRollbackDoesNotRescanFromTheTop(t *testing.T) {
	// Not t.Parallel: the assertion is a delta of a process-wide counter,
	// which a concurrent test would move.
	// rows per record make the drained case, about 50 rows a transaction,
	// take 20 transactions.
	const n, rows = 200, 10
	for _, tc := range []struct {
		name string
		open func(t *testing.T, dir string) *badgerutxo.Store
	}{
		{"record by record", open},
		{"drained", smallRollbackStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.open(t, t.TempDir())
			defer s.Close()
			start := point(0, 0)
			bootstrap(t, s, start)
			value := func(i int) byte {
				return byte(i%250 + 1) //nolint:gosec // a test index
			}
			for i := range n {
				p := point(uint64(i+1), value(i))
				o := out(value(i), uint32(i)) //nolint:gosec // a test index
				_, err := s.ApplyLedgerBatch(&p, badgerutxo.LedgerBatch{
					Changes: []badgerutxo.Change{{
						Point: p,
						Added: []badgerutxo.Output{o},
					}},
					Rows: journaledRows(p.Slot, 7, rows, value(i)),
				})
				require.NoError(t, err)
			}
			mid := n / 2
			target := point(uint64(mid), value(mid-1))
			before := badgerutxo.TopSeeksForTest()
			_, err := s.Rollback(target)
			require.NoError(t, err)
			seeks := badgerutxo.TopSeeksForTest() - before
			requireTip(t, s, target)
			for k := range rows {
				v, ok, err := s.Row(binary.BigEndian.AppendUint32(
					[]byte{'Z', 7}, uint32(k), //nolint:gosec // a test index
				))
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, []byte{value(mid - 1)}, v)
			}
			require.LessOrEqual(
				t, seeks, uint64(4),
				"rollback of %d records started %d scans at the top",
				n-mid, seeks,
			)
		})
	}
}
