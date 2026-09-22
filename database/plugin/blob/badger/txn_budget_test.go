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

package badger

import (
	"encoding/binary"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// newBudgetTestStore opens a store whose memtable is small enough that a
// transaction's entry budget is reachable in a unit test.
func newBudgetTestStore(
	t *testing.T,
	valueThreshold int64,
) *BlobStoreBadger {
	t.Helper()
	store, err := New(
		WithDataDir(t.TempDir()),
		WithGc(false),
		WithValueThreshold(valueThreshold),
		WithValueLogFileSize(1<<24),
		WithMemTableSize(1<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func budgetTestKey(i int) []byte {
	key := make([]byte, 33)
	key[0] = 'u'
	//nolint:gosec // loop counter, always in range
	binary.BigEndian.PutUint32(key[1:5], uint32(i))
	return key
}

// TestRemainingTxnEntriesIsExact pins the contract the staged bulk deletes in
// the database package rely on: RemainingTxnEntries reports exactly how many
// further mutations the transaction accepts -- no more, so a caller that
// stages that many is full, and no fewer, so a caller that reserves headroom
// really has it.
func TestRemainingTxnEntriesIsExact(t *testing.T) {
	store := newBudgetTestStore(t, 1024)

	txn := store.NewTransaction(true)
	defer txn.Rollback() //nolint:errcheck

	remaining, ok := store.RemainingTxnEntries(txn, len(budgetTestKey(0)))
	require.True(t, ok, "badger must report its transaction budget")
	require.Positive(t, remaining)

	for i := range remaining {
		require.NoError(
			t,
			store.Delete(txn, budgetTestKey(i)),
			"the store must accept every delete it said it had room for",
		)
	}
	left, ok := store.RemainingTxnEntries(txn, len(budgetTestKey(0)))
	require.True(t, ok)
	require.Zero(t, left, "the reported budget must now be spent")
	require.ErrorIs(
		t,
		store.Delete(txn, budgetTestKey(remaining)),
		badger.ErrTxnTooBig,
		"one past the reported budget must be exactly where badger stops",
	)
}

// TestRemainingTxnEntriesLeavesRoomForCommitTimestamp is the same contract
// seen from the caller's side: stop one short of the reported budget and the
// commit timestamp Txn.Commit writes into the same transaction still fits, so
// the transaction commits. Running the budget to zero instead is what wedges
// a startup rollback (blinklabs-io/dingo#4657).
func TestRemainingTxnEntriesLeavesRoomForCommitTimestamp(t *testing.T) {
	store := newBudgetTestStore(t, 1024)

	txn := store.NewTransaction(true)
	defer txn.Rollback() //nolint:errcheck

	remaining, ok := store.RemainingTxnEntries(txn, len(budgetTestKey(0)))
	require.True(t, ok)
	require.Positive(t, remaining)

	for i := range remaining - 1 {
		require.NoError(t, store.Delete(txn, budgetTestKey(i)))
	}
	require.NoError(
		t,
		store.SetCommitTimestamp(1, txn),
		"the reserved entry must still be there for the commit timestamp",
	)
	require.NoError(t, txn.Commit())
}

// TestRemainingTxnEntriesRejectsForeignTxn reports "cannot answer" rather
// than a number for a handle it does not own, so a caller falls back to
// staging unbounded instead of trusting a budget from the wrong store.
func TestRemainingTxnEntriesRejectsForeignTxn(t *testing.T) {
	store := newBudgetTestStore(t, 1024)
	other := newBudgetTestStore(t, 1024)

	foreign := other.NewTransaction(true)
	defer foreign.Rollback() //nolint:errcheck

	_, ok := store.RemainingTxnEntries(foreign, 33)
	require.False(t, ok)

	_, ok = store.RemainingTxnEntries(nil, 33)
	require.False(t, ok)
}

func TestRemainingTxnEntriesIsExactWithPointerChargedDeletes(t *testing.T) {
	store := newBudgetTestStore(t, 0)

	txn := store.NewTransaction(true)
	defer txn.Rollback() //nolint:errcheck

	remaining, ok := store.RemainingTxnEntries(txn, len(budgetTestKey(0)))
	require.True(t, ok, "badger must report its transaction budget")
	require.Positive(t, remaining)

	for i := range remaining {
		require.NoError(
			t,
			store.Delete(txn, budgetTestKey(i)),
			"the store must accept every delete it said it had room for",
		)
	}
	left, ok := store.RemainingTxnEntries(txn, len(budgetTestKey(0)))
	require.True(t, ok)
	require.Zero(t, left, "the reported budget must now be spent")
	require.ErrorIs(
		t,
		store.Delete(txn, budgetTestKey(remaining)),
		badger.ErrTxnTooBig,
		"one past the reported budget must be exactly where badger stops",
	)
}
