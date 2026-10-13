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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// testPoolOpCertSequencesExistAtSlot checks the exact-slot probe the ledger
// reads at a Mithril trust boundary: rows at neighbouring slots, from any
// pool, must not count, and one row at the slot must.
func testPoolOpCertSequencesExistAtSlot(t *testing.T, store *Store) {
	t.Helper()
	const boundary = uint64(100)
	exists, err := store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.False(t, exists, "an empty table has no row at any slot")

	poolA := lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte("opcert-slot-a")))
	poolB := lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte("opcert-slot-b")))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolA, 3, boundary-1, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 4, boundary+1, nil))
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.False(t, exists, "rows either side of the slot must not count")

	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 2, boundary, nil))
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.True(t, exists)

	txn := store.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, txn)
	require.NoError(t, err)
	require.True(t, exists, "the probe must read through a caller transaction")
}

func TestSQLitePoolOpCertSequencesExistAtSlot(t *testing.T) {
	t.Parallel()
	testPoolOpCertSequencesExistAtSlot(t, newMigratedTestStore(t))
}
