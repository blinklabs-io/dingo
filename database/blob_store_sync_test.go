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

package database

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// passthroughBlobStore forwards every call to the store it wraps. Installing
// one changes the identity of the database's blob store without changing what
// any operation observes, which is the shape of the real replacement in
// node.go and node_lifecycle.go: bark.NewBarkBlobStore wraps the store already
// installed, and the wrapper is then handed to SetBlobStore.
type passthroughBlobStore struct {
	blob.BlobStore
}

// noReceiveWindow is how long the drain tests wait before concluding that a
// drain that must not have returned has in fact not returned. It only bounds a
// negative assertion, so it trades test runtime against confidence, not
// correctness — a drain that is genuinely stuck stays stuck.
const noReceiveWindow = 100 * time.Millisecond

// drainAsync runs drain on its own goroutine and returns a channel closed when
// it returns.
func drainAsync(drain func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		drain()
		close(done)
	}()
	return done
}

// TestSetBlobStoreConcurrentWithReaders is the regression test for the
// unsynchronized Database.blob field. Readers call the accessor, open
// transactions (which read the installed store to open the underlying blob
// transaction), and perform real blob reads while another goroutine replaces
// the store. Against the unsynchronized field this reports a DATA RACE between
// SetBlobStore's write and every one of those reads.
//
// It also covers the "preserve correct behavior" criterion: every read must
// still return the value written before the replacement started, because each
// installed store resolves to the same underlying badger store.
func TestSetBlobStoreConcurrentWithReaders(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)
	// Restore the original store before the test database is torn down, so
	// cleanup never runs against a wrapper left installed by the writer.
	t.Cleanup(func() { db.SetBlobStore(base) })

	key := []byte("blob-store-sync-test-key")
	want := []byte("blob-store-sync-test-value")
	writeTxn := db.BlobTxn(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(t, base.Set(writeTxn.Blob(), key, want))
	require.NoError(t, writeTxn.Commit())

	const iterations = 100
	const readers = 4
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				// Bare accessor read.
				if store := db.Blob(); store == nil {
					t.Error("Blob() returned nil during replacement")
					return
				}
				// Transaction construction reads the installed store to
				// open the underlying blob transaction, and BlobStore
				// hands back exactly that store.
				blobTxn := db.BlobTxn(false)
				store := blobTxn.BlobStore()
				if store == nil {
					t.Error("BlobStore() returned nil during replacement")
					blobTxn.Release()
					return
				}
				got, getErr := store.Get(blobTxn.Blob(), key)
				if getErr != nil {
					t.Errorf("blob get during replacement: %v", getErr)
					blobTxn.Release()
					return
				}
				if !bytes.Equal(got, want) {
					t.Errorf(
						"blob get during replacement: got %q, want %q",
						got,
						want,
					)
					blobTxn.Release()
					return
				}
				blobTxn.Release()
				// A combined transaction reads the installed store too.
				txn := db.Transaction(false)
				txn.Release()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := range iterations {
			if i%2 == 0 {
				db.SetBlobStore(&passthroughBlobStore{BlobStore: base})
			} else {
				db.SetBlobStore(base)
			}
		}
	}()

	close(start)
	wg.Wait()
}

// TestSetBlobStoreDrainWaitsForOpenTransaction pins down the close policy: the
// replaced store may be closed once drain returns, and not before. A
// transaction opened before the replacement is still using the store it opened
// on, so drain must not return while it is open — and a transaction opened
// after the replacement runs against the new store and must not hold the old
// one open.
func TestSetBlobStoreDrainWaitsForOpenTransaction(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	txn := db.BlobTxn(false)
	released := false
	release := func() {
		if !released {
			released = true
			txn.Release()
		}
	}
	t.Cleanup(release)

	replacement := &passthroughBlobStore{BlobStore: base}
	prev, drain := db.SetBlobStore(replacement)
	t.Cleanup(func() { db.SetBlobStore(base) })
	require.NotNil(t, drain)
	require.True(
		t,
		prev == base,
		"SetBlobStore must return the store it replaced",
	)
	require.True(
		t,
		db.Blob() == blob.BlobStore(replacement),
		"the new store must be installed immediately",
	)

	drained := drainAsync(drain)
	testutil.RequireNoReceive(
		t,
		drained,
		noReceiveWindow,
		"drain returned while a transaction on the previous store was open",
	)

	// A transaction opened after the replacement uses the new store, so it
	// cannot be what keeps drain waiting.
	afterTxn := db.BlobTxn(false)
	// Release is idempotent (Rollback returns early once the transaction is
	// finished), so registering it here as well as calling it below keeps
	// the pin from outliving a failure of the require in between.
	t.Cleanup(afterTxn.Release)
	require.True(
		t,
		afterTxn.BlobStore() == blob.BlobStore(replacement),
		"a transaction opened after the replacement must use the new store",
	)
	afterTxn.Release()
	testutil.RequireNoReceive(
		t,
		drained,
		noReceiveWindow,
		"drain returned before the transaction on the previous store ended",
	)

	release()
	testutil.RequireReceive(
		t,
		drained,
		5*time.Second,
		"drain did not return once the previous store had no users left",
	)
}

// TestSetBlobStoreDrainWaitsForPinBlob is the same contract for blob work that
// runs outside a transaction, which is what PinBlob exists for.
func TestSetBlobStoreDrainWaitsForPinBlob(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	pinned, releasePin := db.PinBlob()
	require.True(t, pinned == base)

	_, drain := db.SetBlobStore(&passthroughBlobStore{BlobStore: base})
	t.Cleanup(func() { db.SetBlobStore(base) })

	drained := drainAsync(drain)
	testutil.RequireNoReceive(
		t,
		drained,
		noReceiveWindow,
		"drain returned while a PinBlob pin on the previous store was held",
	)

	releasePin()
	testutil.RequireReceive(
		t,
		drained,
		5*time.Second,
		"drain did not return after the pin was released",
	)
}

// TestSetBlobStoreDrainReturnsWhenIdle is the negative case for the two tests
// above: with nothing pinning the replaced store, drain must not block, so a
// caller that wants to close the previous store is never made to wait for work
// that is not there.
func TestSetBlobStoreDrainReturnsWhenIdle(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	_, drain := db.SetBlobStore(&passthroughBlobStore{BlobStore: base})
	t.Cleanup(func() { db.SetBlobStore(base) })

	testutil.RequireReceive(
		t,
		drainAsync(drain),
		5*time.Second,
		"drain blocked with nothing pinning the previous store",
	)
}

// TestTxnKeepsBlobStoreAcrossReplacement covers the pairing half of the
// ownership rule. A transaction's types.Txn handle belongs to the store it was
// opened on; re-reading the database's installed store mid-transaction could
// pair that handle with a different store. BlobStore must keep naming the
// original, and the transaction must still commit correctly through it.
func TestTxnKeepsBlobStoreAcrossReplacement(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	txn := db.Transaction(true)
	defer txn.Release()
	require.True(t, txn.BlobStore() == base)

	replacement := &passthroughBlobStore{BlobStore: base}
	_, drain := db.SetBlobStore(replacement)
	t.Cleanup(func() { db.SetBlobStore(base) })

	require.True(
		t,
		txn.BlobStore() == base,
		"an open transaction must keep the store it opened on",
	)

	key := []byte("blob-store-pairing-test-key")
	want := []byte("blob-store-pairing-test-value")
	require.NoError(t, txn.BlobStore().Set(txn.Blob(), key, want))
	require.NoError(t, txn.Commit())

	// Committing is what releases the transaction's pin, so drain must now
	// complete.
	testutil.RequireReceive(
		t,
		drainAsync(drain),
		5*time.Second,
		"drain did not return after the transaction committed",
	)

	readTxn := db.BlobTxn(false)
	defer readTxn.Release()
	got, err := readTxn.BlobStore().Get(readTxn.Blob(), key)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// pairingProbeBlobStore is a passthrough that counts the reads which reach it.
// A read that lands here came from the store installed *now*, so a test that
// opens a transaction on one store and then installs this one can tell whether
// an operation ran the transaction's handle through the wrong store.
type pairingProbeBlobStore struct {
	blob.BlobStore
	getTx   atomic.Int64
	getUtxo atomic.Int64
}

func (s *pairingProbeBlobStore) GetTx(
	txn types.Txn,
	txHash []byte,
) ([]byte, error) {
	s.getTx.Add(1)
	return s.BlobStore.GetTx(txn, txHash)
}

func (s *pairingProbeBlobStore) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	s.getUtxo.Add(1)
	return s.BlobStore.GetUtxo(txn, txId, outputIdx)
}

// TestResolveTxCborUsesTransactionStore covers the cold path of the CBOR cache
// against the same pairing rule the rest of the package follows. ResolveTxCbor
// takes the caller's transaction so uncommitted writes are visible, and that
// transaction's handle belongs to the store it was opened on. Pinning the
// installed store instead would send that handle into a store replaced in
// between, which is what this test forbids.
func TestResolveTxCborUsesTransactionStore(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	var txHash [32]byte
	copy(txHash[:], []byte("resolve-tx-pairing-hash"))
	// Raw CBOR rather than an offset record, so the resolve returns after
	// the single GetTx this test is measuring.
	want := []byte{0x82, 0x01, 0x02}
	writeTxn := db.BlobTxn(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(t, base.SetTx(writeTxn.Blob(), txHash[:], want))
	require.NoError(t, writeTxn.Commit())

	txn := db.BlobTxn(false)
	t.Cleanup(txn.Release)
	require.True(t, txn.BlobStore() == base)

	replacement := &pairingProbeBlobStore{BlobStore: base}
	db.SetBlobStore(replacement)
	t.Cleanup(func() { db.SetBlobStore(base) })

	got, err := db.CborCache().ResolveTxCbor(txn, txHash[:])
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(
		t,
		replacement.getTx.Load(),
		"ResolveTxCbor ran the transaction's handle through the store installed after it was opened",
	)
}

// TestResolveUtxoCborUsesTransactionStore is the same contract for the UTxO
// entry point, which loadCbor reaches with the transaction validation is
// running under.
func TestResolveUtxoCborUsesTransactionStore(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	base := db.Blob()
	require.NotNil(t, base)

	txId := make([]byte, 32)
	copy(txId, []byte("resolve-utxo-pairing-txid"))
	want := []byte{0x82, 0x03, 0x04}
	writeTxn := db.BlobTxn(true)
	t.Cleanup(writeTxn.Release)
	require.NoError(t, base.SetUtxo(writeTxn.Blob(), txId, 0, want))
	require.NoError(t, writeTxn.Commit())

	txn := db.BlobTxn(false)
	t.Cleanup(txn.Release)
	require.True(t, txn.BlobStore() == base)

	replacement := &pairingProbeBlobStore{BlobStore: base}
	db.SetBlobStore(replacement)
	t.Cleanup(func() { db.SetBlobStore(base) })

	got, err := db.CborCache().ResolveUtxoCbor(txId, 0, txn)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(
		t,
		replacement.getUtxo.Load(),
		"ResolveUtxoCbor ran the transaction's handle through the store installed after it was opened",
	)
}
