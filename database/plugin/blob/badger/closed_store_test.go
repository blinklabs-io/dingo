package badger

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestNewTransactionOnClosedStoreFailsFast pins that a closed store hands back
// an unusable transaction instead of calling into a closed Badger.
//
// Regression test for #3609. badger.DB.NewTransaction takes a read timestamp
// via oracle.readTs, which waits on the commit watermark with
// context.Background(). Closing the DB stops the watermark's process
// goroutine, and a Done mark still queued in markCh at that moment is dropped
// (y/watermark.go selects randomly between the close signal and the mark), so
// doneUntil can stay behind nextTxnTs-1 permanently. A read transaction taken
// afterwards then blocks forever with no context to cancel it.
func TestNewTransactionOnClosedStoreFailsFast(t *testing.T) {
	store, err := New()
	require.NoError(t, err)

	// Issue at least one commit timestamp, so the watermark has a mark to
	// drop; this is the state the hang needs.
	txn := store.NewTransaction(true)
	require.NoError(t, store.Set(txn, []byte("key"), []byte("value")))
	require.NoError(t, txn.Commit())

	require.NoError(t, store.Close())

	got := make(chan types.Txn, 1)
	go func() { got <- store.NewTransaction(false) }()

	var closedTxn types.Txn
	select {
	case closedTxn = <-got:
	case <-time.After(30 * time.Second):
		t.Fatal(
			"NewTransaction blocked on a closed store (see #3609)",
		)
	}
	require.NotNil(t, closedTxn)

	_, err = store.Get(closedTxn, []byte("key"))
	require.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
	require.ErrorIs(
		t,
		store.Set(closedTxn, []byte("key"), []byte("value")),
		types.ErrBlobStoreUnavailable,
	)

	// Commit and Rollback stay nil-safe so deferred cleanup cannot panic.
	require.NoError(t, closedTxn.Rollback())
	require.NoError(t, closedTxn.Commit())
}
