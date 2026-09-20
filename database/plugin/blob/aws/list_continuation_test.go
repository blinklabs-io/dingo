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

//go:build dingo_extra_plugins

package aws

import (
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/database/types"
)

// listBatchSize mirrors database.blobIteratorBatchSize, the number of block
// keys the block iterator collects before it closes the blob iterator and
// reopens one seeked past the last key it took.
const listBatchSize = 1000

// blockBlobKey builds a key in the block blob layout, "bp" + big-endian slot +
// 32-byte hash, so the fake bucket sorts the way a real one does.
func blockBlobKey(slot uint64) string {
	key := make([]byte, 0, len(types.BlockBlobKeyPrefix)+8+32)
	key = append(key, types.BlockBlobKeyPrefix...)
	var slotBytes [8]byte
	binary.BigEndian.PutUint64(slotBytes[:], slot)
	key = append(key, slotBytes[:]...)
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[:8], slot)
	return string(append(key, hash...))
}

// storeOnFake wires a store to srv without Start(), which would need a real
// bucket. The iterator only needs the client.
func storeOnFake(t *testing.T, srv *httptest.Server) *BlobStoreS3 {
	t.Helper()
	store, err := NewWithOptions(
		WithBucket("test-bucket"),
		WithEndpoint(srv.URL),
		WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	store.client = s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(
			"test", "test", "",
		),
	})
	return store
}

// fakeBlockBucket fills a fake bucket with blocks worth of block keys, each
// followed by its metadata key, in the order S3 would return them.
func fakeBlockBucket(blocks int) *fakeS3List {
	fake := &fakeS3List{}
	for i := range blocks {
		key := blockBlobKey(uint64(i) * 20)
		fake.keys = append(fake.keys, hex.EncodeToString([]byte(key)))
		fake.keys = append(
			fake.keys,
			hex.EncodeToString(
				[]byte(key+types.BlockBlobMetadataKeySuffix),
			),
		)
	}
	sort.Strings(fake.keys)
	return fake
}

// batchOpener records, per batch, the start-after the batch's opening request
// carried.
type batchOpener struct {
	batch      int
	startAfter string
}

// runBatchedScan drives the blob iterator the way database's block iterator
// does: reopen the iterator every listBatchSize block keys, seeked past the
// last key taken, and take the keys in order. It returns the block keys yielded
// and the opening request of each batch.
func runBatchedScan(
	t *testing.T,
	store *BlobStoreS3,
	fake *fakeS3List,
) ([]string, []batchOpener) {
	t.Helper()
	prefix := []byte(types.BlockBlobKeyPrefix)
	metaSuffix := []byte(types.BlockBlobMetadataKeySuffix)
	seek := make([]byte, 0, len(prefix)+8)
	seek = append(seek, prefix...)
	seek = append(seek, make([]byte, 8)...)

	var yielded []string
	var openers []batchOpener
	var resume []byte
	for batch := 0; ; batch++ {
		fake.mu.Lock()
		requestsBefore := len(fake.startAfter)
		fake.mu.Unlock()

		txn := store.NewTransaction(false)
		it := store.NewIterator(
			txn,
			types.BlobIteratorOptions{Prefix: prefix},
		)
		if it == nil {
			t.Fatal("NewIterator returned nil")
		}
		taken := 0
		resuming := resume != nil
		if resume != nil {
			seek = resume
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) >= len(metaSuffix) &&
				string(key[len(key)-len(metaSuffix):]) ==
					string(metaSuffix) {
				continue
			}
			if resuming {
				resuming = false
				if string(key) == string(resume) {
					continue
				}
			}
			resume = append([]byte(nil), key...)
			yielded = append(yielded, string(key))
			taken++
			if taken >= listBatchSize {
				break
			}
		}
		if err := it.Err(); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
		it.Close()
		_ = txn.Rollback()

		fake.mu.Lock()
		if len(fake.startAfter) > requestsBefore {
			openers = append(openers, batchOpener{
				batch:      batch,
				startAfter: fake.startAfter[requestsBefore],
			})
		}
		fake.mu.Unlock()

		if taken < listBatchSize {
			return yielded, openers
		}
	}
}

// TestS3BatchedScanIssuesOnlyBoundedListings pins that reopening the iterator
// for the next batch costs one listing that continues from the last key taken,
// not two -- an unbounded one from the start of the prefix, then the seeked
// one.
//
// NewIterator rewinds the iterator it returns, and a rewind used to issue the
// listing immediately. The caller's Seek then replaced it, so every batch
// fetched and discarded a full page from the head of the prefix. Every blob
// iterator in database/ seeks straight after NewIterator, so all of them paid
// it; the block iterator paid it once per listBatchSize blocks.
func TestS3BatchedScanIssuesOnlyBoundedListings(t *testing.T) {
	const blocks = 5000
	fake := fakeBlockBucket(blocks)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	store := storeOnFake(t, srv)

	yielded, openers := runBatchedScan(t, store, fake)
	if len(yielded) != blocks {
		t.Fatalf("scan yielded %d block keys, want %d", len(yielded), blocks)
	}

	fake.mu.Lock()
	startAfter := append([]string(nil), fake.startAfter...)
	continued := append([]string(nil), fake.continued...)
	returned := fake.returned
	fake.mu.Unlock()

	var unbounded int
	for i := range startAfter {
		if startAfter[i] == "" && continued[i] == "" {
			unbounded++
		}
	}
	t.Logf(
		"%d blocks (%d objects) in %d batches: %d ListObjectsV2 request(s), "+
			"%d unbounded, %d object(s) returned",
		blocks,
		len(fake.keys),
		len(openers),
		len(startAfter),
		unbounded,
		returned,
	)
	if unbounded != 0 {
		t.Fatalf(
			"%d of %d ListObjectsV2 request(s) carried neither start-after "+
				"nor continuation-token: each lists the prefix from its "+
				"start and the seek that follows discards the page",
			unbounded,
			len(startAfter),
		)
	}

	// Forward-only: each batch opens its listing at or after the previous
	// batch's, so no batch re-lists ground an earlier batch already covered.
	if len(openers) != len(yielded)/listBatchSize+1 {
		t.Fatalf(
			"recorded %d batch opening request(s) for %d block keys",
			len(openers),
			len(yielded),
		)
	}
	for i := 1; i < len(openers); i++ {
		if openers[i].startAfter <= openers[i-1].startAfter {
			t.Fatalf(
				"batch %d opened at start-after %q, not past batch %d's %q",
				openers[i].batch,
				openers[i].startAfter,
				openers[i-1].batch,
				openers[i-1].startAfter,
			)
		}
	}
}

// TestS3BatchedScanYieldsEveryKeyOnceInOrder holds the iteration contract the
// bounded listing has to preserve: ascending key order, every block key once,
// none dropped or repeated at a batch boundary.
func TestS3BatchedScanYieldsEveryKeyOnceInOrder(t *testing.T) {
	// Not a multiple of listBatchSize, so the last batch is partial and the
	// scan crosses two batch boundaries.
	const blocks = 2500
	fake := fakeBlockBucket(blocks)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	store := storeOnFake(t, srv)

	yielded, _ := runBatchedScan(t, store, fake)

	want := make([]string, 0, blocks)
	for i := range blocks {
		want = append(want, blockBlobKey(uint64(i)*20))
	}
	sort.Strings(want)
	if len(yielded) != len(want) {
		t.Fatalf("scan yielded %d block keys, want %d", len(yielded), len(want))
	}
	seen := make(map[string]int, len(yielded))
	for i, key := range yielded {
		seen[key]++
		if seen[key] > 1 {
			t.Fatalf("block key at index %d yielded %d times", i, seen[key])
		}
		if key != want[i] {
			t.Fatalf(
				"block key at index %d = %x, want %x",
				i,
				key,
				want[i],
			)
		}
	}
}

// TestS3IteratorCloseStopsListing pins that a closed iterator stays closed. The
// deferred listing is issued on the first read, so Valid or Err after Close
// must not open one.
func TestS3IteratorCloseStopsListing(t *testing.T) {
	fake := fakeBlockBucket(10)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	store := storeOnFake(t, srv)

	txn := store.NewTransaction(false)
	it := store.NewIterator(
		txn,
		types.BlobIteratorOptions{Prefix: []byte(types.BlockBlobKeyPrefix)},
	)
	it.Close()

	fake.mu.Lock()
	before := len(fake.startAfter)
	fake.mu.Unlock()
	if it.Valid() {
		t.Fatal("closed iterator reports a valid position")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("closed iterator: %v", err)
	}
	it.Next()
	fake.mu.Lock()
	after := len(fake.startAfter)
	fake.mu.Unlock()
	if after != before {
		t.Fatalf(
			"reading a closed iterator issued %d ListObjectsV2 request(s)",
			after-before,
		)
	}
	_ = txn.Rollback()
}
