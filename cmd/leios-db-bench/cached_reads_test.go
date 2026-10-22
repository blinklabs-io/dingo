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

package main

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
)

type countedEBReader struct {
	reads atomic.Int64
	err   error
}

func (r *countedEBReader) GetLeiosEBManifest(
	hash []byte,
	slot uint64,
) ([]byte, error) {
	r.reads.Add(1)
	return []byte{hash[0], byte(slot)}, r.err
}

func (r *countedEBReader) GetLeiosEBTxs(
	hash []byte,
	slot uint64,
) ([]cbor.RawMessage, error) {
	r.reads.Add(1)
	return []cbor.RawMessage{{hash[0]}, {byte(slot)}, {0x82, 1, 2}}, r.err
}

func TestCachedReadsShareConcurrentStorageLoads(t *testing.T) {
	t.Parallel()
	reader := &countedEBReader{}
	hash := []byte{42}
	cache := newEBReadCache(reader, hash, 7)
	hash[0] = 99
	eb := &benchEb{readCache: cache}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			manifest, err := cache.manifest()
			if err != nil || !bytes.Equal(manifest, []byte{42, 7}) {
				t.Errorf(
					"manifest = %v, %v; want original occurrence",
					manifest,
					err,
				)
			}
			txs, err := readEBTxs(nil, eb, 7, txRange{1, 3})
			if err != nil || len(txs) != 2 || !bytes.Equal(txs[0], []byte{7}) {
				t.Errorf("selected transactions = %v, %v", txs, err)
			}
		})
	}
	wg.Wait()
	if got := reader.reads.Load(); got != 2 {
		t.Fatalf(
			"concurrent cache readers issued %d storage reads, want one manifest and one closure read",
			got,
		)
	}
}

func TestCachedReadsPreserveReturnedOwnershipAndOccurrence(t *testing.T) {
	t.Parallel()
	reader := &countedEBReader{}
	eb := &benchEb{readCache: newEBReadCache(reader, []byte{42}, 7)}
	txs, err := readEBTxs(nil, eb, 7, txRange{1, 3})
	if err != nil || len(txs) != 2 {
		t.Fatalf("selected closure = %v, %v", txs, err)
	}
	txs[0][0] = 99
	txs[1] = nil
	again, err := readEBTxs(nil, eb, 7, txRange{end: -1})
	if err != nil || len(again) != 3 || !bytes.Equal(again[1], []byte{7}) ||
		!bytes.Equal(again[2], []byte{0x82, 1, 2}) {
		t.Fatalf("returned mutation changed cached closure: %v, %v", again, err)
	}
	other := &benchEb{readCache: newEBReadCache(reader, []byte{42}, 8)}
	txs, err = readEBTxs(nil, other, 8, txRange{1, 2})
	if err != nil || len(txs) != 1 || !bytes.Equal(txs[0], []byte{8}) {
		t.Fatalf("different occurrence reused old closure: %v, %v", txs, err)
	}
}

func TestCachedReadsDoNotHideStorageFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("storage read failed")
	eb := &benchEb{
		readCache: newEBReadCache(&countedEBReader{err: want}, []byte{42}, 7),
	}
	if _, err := readEBTxs(nil, eb, 7, txRange{0, 1}); !errors.Is(err, want) {
		t.Fatalf("closure error = %v, want storage error", err)
	}
	if _, err := eb.readCache.manifest(); !errors.Is(err, want) {
		t.Fatalf("manifest error = %v, want storage error", err)
	}
}

func TestCachedReadPackedTransactionsKeepIndependentCapacity(t *testing.T) {
	t.Parallel()
	eb := &benchEb{readCache: &ebReadCache{
		txs: func() ([]cbor.RawMessage, error) {
			return []cbor.RawMessage{nil, {}, {1}, {2}}, nil
		},
	}}
	txs, err := readEBTxs(nil, eb, 0, txRange{end: -1})
	if err != nil || len(txs) != 4 {
		t.Fatalf("packed transactions = %v, %v", txs, err)
	}
	if txs[0] != nil || txs[1] == nil || len(txs[1]) != 0 {
		t.Fatalf("nil and empty transaction distinction lost: %v", txs)
	}
	appended := append(txs[2], 99)
	if !bytes.Equal(appended, []byte{1, 99}) ||
		!bytes.Equal(txs[3], []byte{2}) {
		t.Fatalf("append overwrote adjacent transaction: %v", txs)
	}
}

type countingBlobReads struct {
	blob.BlobStore
	reads atomic.Int64
}

func (b *countingBlobReads) Get(txn types.Txn, key []byte) ([]byte, error) {
	b.reads.Add(1)
	return b.BlobStore.Get(txn, key)
}

func TestRunBenchCachedReadsReduceActualStorageRequests(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "database", true: "cached"}[cached],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(
					t,
					&database.Config{DataDir: t.TempDir()},
				)
				if err != nil {
					t.Fatal(err)
				}
				counter := &countingBlobReads{BlobStore: db.Blob()}
				db.SetBlobStore(counter)
				config := validTestConfig()
				config.downstreamPeers = 100
				config.cachedReads = cached
				if err := runBench(db, config); err != nil {
					t.Fatal(err)
				}
				want := int64(
					202,
				) // one manifest and closure per peer, vote and apply
				if cached {
					want = 2
				}
				if got := counter.reads.Load(); got != want {
					t.Fatalf(
						"cached=%v issued %d blob reads, want %d",
						cached,
						got,
						want,
					)
				}
			},
		)
	}
}
