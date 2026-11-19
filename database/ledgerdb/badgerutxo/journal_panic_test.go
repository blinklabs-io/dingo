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

package badgerutxo

import (
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestAsOfPanicReleasesSnapshotBorrow(t *testing.T) {
	t.Parallel()
	s, err := Open(t.TempDir(), WithMemTableSize(8<<20))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(tipKey, []byte{1})
	}))
	snap, err := s.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() {
		snap.snap.mu.Lock()
		active := snap.snap.active
		snap.snap.mu.Unlock()
		// A failed borrow-count assertion must not hang parent cleanup.
		for range active {
			snap.snap.done()
		}
		require.NoError(t, snap.Close())
	})
	require.Panics(t, func() {
		_, _ = snap.AsOf(ocommon.Point{Slot: 1})
	})
	snap.snap.mu.Lock()
	active := snap.snap.active
	snap.snap.mu.Unlock()
	require.Zero(
		t,
		active,
		"historical construction retained the parent borrow",
	)
}
