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
	"bytes"
	"crypto/rand"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestCloseReleasesDirectoryLockDuringConcurrentExec pins that an immediate
// reopen after Close succeeds while other goroutines in the process are
// starting child processes.
//
// Badger releases its flock by closing the directory descriptor. A child
// forked while that descriptor is open holds a copy of it until the child
// execs, so unless Close waits for that lock an immediate in-process reopen of
// the same directory fails with "Cannot acquire directory lock".
func TestCloseReleasesDirectoryLockDuringConcurrentExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows children do not inherit the parent's lock handle")
	}
	require.NotEmpty(t, os.Args)
	dataDir := t.TempDir()

	stop := make(chan struct{})
	var spawners sync.WaitGroup
	for range 4 {
		spawners.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// -test.run=^$ makes the child exit without running tests.
				cmd := exec.Command(os.Args[0], "-test.run=^$")
				_ = cmd.Run()
			}
		})
	}
	t.Cleanup(func() {
		close(stop)
		spawners.Wait()
	})

	for i := range 40 {
		store, err := New(
			WithDataDir(dataDir),
			WithGc(false),
			WithValueLogFileSize(16*1024*1024),
			WithMemTableSize(8*1024*1024),
		)
		require.NoError(t, err, "reopen %d after Close", i)
		require.NoError(t, store.Close())
	}
}

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

func TestBlobStoreConformance(t *testing.T) {
	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		t.Helper()
		store, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
		})
		return store
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, err := New(WithDataDir(t.TempDir()))
		require.NoError(t, err)
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Stop())
	})
}

// BenchmarkValueLogGC measures GC against a fixed-size dataset with rotated
// value-log files and both overwritten and deleted values. Setup is outside
// the timed region, so the benchmark compares the GC rewrite itself.
func BenchmarkValueLogGC(b *testing.B) {
	for _, ratio := range []float64{0.25, 0.5, 0.75} {
		b.Run(strconv.FormatFloat(ratio, 'f', 2, 64), func(b *testing.B) {
			store, err := New(
				WithDataDir(b.TempDir()),
				WithGc(false),
				WithValueThreshold(1),
				WithValueLogFileSize(1<<20),
				WithMemTableSize(1<<20),
			)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for pass := range 2 {
					for batch := range 5 {
						txn := store.DB().NewTransaction(true)
						for j := range 20 {
							key := batch*20 + j
							value := make([]byte, 32<<10)
							_, err = rand.Read(value)
							require.NoError(b, err)
							entry := badgerdb.NewEntry(
								[]byte("benchmark-key-"+strconv.Itoa(key)),
								value,
							)
							if pass == 0 {
								entry.ExpiresAt = 1
							}
							require.NoError(b, txn.SetEntry(entry))
						}
						require.NoError(b, txn.Commit())
					}
				}
				for batch := range 100 {
					txn := store.DB().NewTransaction(true)
					for j := range 1000 {
						key := batch*1000 + j
						require.NoError(
							b,
							txn.SetEntry(
								badgerdb.NewEntry(
									[]byte(
										"benchmark-filler-"+strconv.Itoa(key),
									),
									[]byte{1},
								),
							),
						)
					}
					require.NoError(b, txn.Commit())
				}
				for batch := range 3 {
					txn := store.DB().NewTransaction(true)
					for j := range 20 {
						key := batch*20 + j
						if key >= 45 {
							continue
						}
						require.NoError(
							b,
							txn.Delete(
								[]byte("benchmark-key-"+strconv.Itoa(key)),
							),
						)
					}
					require.NoError(b, txn.Commit())
				}
				require.NoError(b, store.DB().Flatten(10))
				require.NoError(b, store.DB().Sync())
				b.StartTimer()
				successes := 0
				reclaimed := int64(0)
				for range 32 {
					passBefore, sizeErr := store.DiskSize()
					require.NoError(b, sizeErr)
					err = store.DB().RunValueLogGC(ratio)
					if errors.Is(err, badgerdb.ErrNoRewrite) {
						continue
					}
					require.NoError(b, err)
					successes++
					passAfter, sizeErr := store.DiskSize()
					require.NoError(b, sizeErr)
					if passBefore > passAfter &&
						passBefore-passAfter > reclaimed {
						reclaimed = passBefore - passAfter
					}
				}
				b.StopTimer()
				require.Greater(
					b,
					successes,
					0,
					"GC did not perform a successful rewrite",
				)
				if reclaimed > 0 {
					b.ReportMetric(float64(reclaimed), "bytes_reclaimed")
				}
			}
		})
	}
}

func FuzzCompactBlockMetadataRoundTrip(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(0), []byte(nil))
	f.Add(uint64(42), uint64(7), uint64(99), bytes.Repeat([]byte{0xab}, 32))
	f.Add(uint64(1), uint64(2), uint64(3), bytes.Repeat([]byte{0xcd}, 33))

	f.Fuzz(
		func(t *testing.T, id uint64, typeValue uint64, height uint64, prevHash []byte) {
			if len(prevHash) > types.BlockMetadataPrevHashMaxLen {
				return
			}

			metadata := types.BlockMetadata{
				ID:       id,
				Type:     uint(typeValue),
				Height:   height,
				PrevHash: append([]byte(nil), prevHash...),
			}
			dst := make([]byte, 32+len(prevHash))
			err := marshalBlockMetadataInto(dst, metadata)
			if err != nil {
				t.Fatalf("marshalBlockMetadataInto: %v", err)
			}

			decoded, err := unmarshalBlockMetadata(dst)
			if err != nil {
				t.Fatalf("unmarshalBlockMetadata(compact): %v", err)
			}
			if decoded.ID != metadata.ID ||
				decoded.Type != metadata.Type ||
				decoded.Height != metadata.Height ||
				!bytes.Equal(decoded.PrevHash, metadata.PrevHash) {
				t.Fatalf("decoded metadata = %#v, want %#v", decoded, metadata)
			}
		},
	)
}

func FuzzUnmarshalBlockMetadata(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("DBM1"))
	seed := make([]byte, 32)
	if err := marshalBlockMetadataInto(seed, types.BlockMetadata{ID: 1}); err != nil {
		f.Fatalf("marshal compact metadata seed: %v", err)
	}
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64*1024 {
			t.Skip("metadata input is too large for fast fuzzing")
		}

		metadata, err := unmarshalBlockMetadata(data)
		if err != nil {
			return
		}
		if len(metadata.PrevHash) > types.BlockMetadataPrevHashMaxLen {
			t.Fatalf("metadata prev hash length = %d, want <= %d",
				len(metadata.PrevHash),
				types.BlockMetadataPrevHashMaxLen,
			)
		}
	})
}
