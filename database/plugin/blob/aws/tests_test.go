//go:build dingo_extra_plugins

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

package aws

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/blockverify"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/dingo/plugin"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// hasS3Credentials is defined in backup_test.go and shared across this
// package's test files.

func TestBlobStoreConformance(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
		t.Helper()
		opts := []BlobStoreS3OptionFunc{
			WithBucket(bucket),
			WithRegion(region),
			// Isolate this run's keys from any other test/CI run sharing
			// the bucket. t.Name() alone is not enough here -- it is
			// always "TestBlobStoreConformance" on this shared outer test,
			// so every invocation would otherwise write and delete under
			// the exact same prefix; a run interrupted before its own
			// cleanup runs (a killed CI job, a panic, a machine restart)
			// would leave objects the next run then reads, corrupting
			// assertions that expect to start from an empty prefix (e.g.
			// IteratorEnumeratesWrittenKeys). time.Now().UnixNano()
			// matches the per-run suffix already used for the
			// GetBlockURL tests below and benchmark_test.go.
			WithPrefix(fmt.Sprintf(
				"storagetest-conformance-%d/",
				time.Now().UnixNano(),
			)),
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			opts = append(opts, WithEndpoint(endpoint))
		}
		store, err := NewWithOptions(opts...)
		require.NoError(t, err)
		require.NoError(t, store.Start())
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
		})
		// Registered after (so it runs before, via t.Cleanup's LIFO order,
		// while the store is still started) Stop's own cleanup, matching
		// newTestS3Store's convention in backup_test.go: this suite commits
		// real data under the prefix above and would otherwise accumulate
		// every run's objects in a real, persistent bucket.
		t.Cleanup(func() { cleanupTestS3Store(t, store) })
		return store
	})
}

func TestBlobStoreResourceCleanup(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}

	storagetest.AssertRepeatedLifecycleIsSafe(t, 5, func(t *testing.T) {
		// t.Name() is always "TestBlobStoreResourceCleanup" on this shared
		// outer test, not a per-cycle or per-run value -- see the identical
		// fix's comment on TestBlobStoreConformance's prefix above for why
		// that fails to isolate concurrent/interrupted runs.
		opts := []BlobStoreS3OptionFunc{
			WithBucket(bucket),
			WithPrefix(fmt.Sprintf(
				"storagetest-resource-cleanup-%d/",
				time.Now().UnixNano(),
			)),
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			opts = append(opts, WithEndpoint(endpoint))
		}
		store, err := NewWithOptions(opts...)
		require.NoError(t, err)
		require.NoError(t, store.Start())
		txn := store.NewTransaction(true)
		require.NoError(t, store.Set(txn, []byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		// Delete what this cycle committed while the store is still
		// started, so 5 cycles against a real, persistent bucket don't
		// accumulate 5 copies of the same key under 5 different prefixes.
		cleanupTestS3Store(t, store)
		require.NoError(t, store.Stop())
	})
}

// TestBlobStoreUnreachableEndpointFailsWithoutHanging needs no credentials
// or running server: it points at a closed local port so the operation
// fails with a connection error rather than reaching any real endpoint.
// Start itself does not probe connectivity (the SDK only builds a client),
// so the failure surfaces on first use; this asserts it surfaces quickly
// and as an error, not a hang or a panic.
func TestBlobStoreUnreachableEndpointFailsWithoutHanging(t *testing.T) {
	store, err := NewWithOptions(
		WithBucket("dingo-test"),
		WithRegion("us-east-1"),
		WithEndpoint("http://127.0.0.1:1/"),
		// Bounds the SDK's own retry/backoff so the test fails fast instead
		// of waiting out the 60s default.
		WithTimeout(3*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
	})

	start := time.Now()
	txn := store.NewTransaction(false)
	_, err = store.Get(txn, []byte("k"))
	require.Error(t, err)
	require.NoError(t, txn.Rollback())
	require.Less(
		t,
		time.Since(start),
		10*time.Second,
		"an unreachable endpoint should fail within the configured "+
			"operation timeout, not hang",
	)
}

// TestBlobStoreBadCredentialsFailsCleanly is gated on a real, reachable
// endpoint being configured (the same convention as every other test in this
// file) because it needs a server that actually rejects the credentials --
// pointing at nothing would just repeat
// TestBlobStoreUnreachableEndpointFailsWithoutHanging. t.Setenv scopes the
// deliberately wrong credentials to this test only.
func TestBlobStoreBadCredentialsFailsCleanly(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "dingo-test-bucket"
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "storagetest-invalid-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "storagetest-invalid-secret-key")

	opts := []BlobStoreS3OptionFunc{
		WithBucket(bucket),
		WithRegion("us-east-1"),
		WithTimeout(10 * time.Second),
	}
	if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}
	store, err := NewWithOptions(opts...)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
	})

	txn := store.NewTransaction(false)
	_, err = store.Get(txn, []byte("k"))
	require.Error(t, err)
	require.NoError(t, txn.Rollback())
}

// newTestS3Store is defined in backup_test.go and shared across this
// package's test files.

// TestBlobStoreGetBlockURLSignsCommittedBlock exercises the happy path no
// existing test in this package covers: a committed block's GetBlockURL
// returns a usable, parseable, non-expired presigned URL and the block's
// metadata.
func TestBlobStoreGetBlockURLSignsCommittedBlock(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("block-url-signs-%d/", time.Now().UnixNano()),
	)
	slot := uint64(500)
	hash := []byte("block-url-committed")

	writeTxn := store.NewTransaction(true)
	require.NoError(t, store.SetBlock(
		writeTxn, slot, hash, []byte{0x82, 0x01, 0x02}, 1, 0, 7, nil,
	))
	require.NoError(t, writeTxn.Commit())

	readTxn := store.NewTransaction(false)
	defer func() { require.NoError(t, readTxn.Rollback()) }()
	signed, meta, err := store.GetBlockURL(
		t.Context(),
		readTxn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.NoError(t, err)
	require.NotEmpty(t, signed.URL.String())
	require.True(t, signed.Expires.After(time.Now()))
	require.Equal(t, uint64(7), meta.Height)
}

// TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock exercises the
// documented contract in DATABASE.md's Cross-Store Durability Contract: "a
// block staged but not yet committed is reported as not found rather than
// signed into a URL that would 404." The staging check itself runs before
// any network call, but constructing a real S3 client still needs a
// reachable endpoint, so this is gated the same as every other test here.
func TestBlobStoreGetBlockURLRejectsStagedUncommittedBlock(t *testing.T) {
	store := newTestS3Store(
		t,
		fmt.Sprintf("block-url-staged-%d/", time.Now().UnixNano()),
	)
	slot := uint64(600)
	hash := []byte("block-url-staged")

	txn := store.NewTransaction(true)
	defer func() { require.NoError(t, txn.Rollback()) }()
	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x82, 0x03, 0x04}, 2, 0, 8, nil,
	))

	_, _, err := store.GetBlockURL(
		t.Context(),
		txn,
		ocommon.Point{Slot: slot, Hash: hash},
	)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

// TestGetBlockVerifiesContent proves GetBlock re-derives the block's hash
// from the returned bytes rather than trusting the (slot, hash) key alone:
// a genuine block round-trips, but content stored under a hash it does not
// actually hash to -- as a corrupted object, an eventual-consistency stale
// read, or a misdirected request could produce -- is rejected instead of
// being handed back to the caller as though it were the requested block.
func TestGetBlockVerifiesContent(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		1, lcommon.Blake2b256{}, 1000, 10, 2,
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	realBlock, otherBlock := blocks[0], blocks[1]

	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(s3.Client)

	t.Run("genuine content round-trips", func(t *testing.T) {
		hash := realBlock.Hash()
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 1000, hash[:], realBlock.Cbor(),
			1, uint(gledger.BlockTypeConway), 1, nil,
		))

		gotCbor, meta, err := store.GetBlock(txn, 1000, hash[:])
		require.NoError(t, err)
		require.Equal(t, realBlock.Cbor(), gotCbor)
		require.Equal(t, uint(gledger.BlockTypeConway), meta.Type)
	})

	t.Run("content for the wrong block is rejected", func(t *testing.T) {
		requestedHash := realBlock.Hash()
		txn := store.NewTransaction(true)
		// Stage otherBlock's bytes under realBlock's key, simulating a
		// remote store handing back the wrong object for the key.
		require.NoError(t, store.SetBlock(
			txn, 1010, requestedHash[:], otherBlock.Cbor(),
			2, uint(gledger.BlockTypeConway), 1, nil,
		))

		_, _, err := store.GetBlock(txn, 1010, requestedHash[:])
		require.ErrorIs(t, err, blockverify.ErrHashMismatch)
	})

	t.Run("synthetic ID=0 entries skip verification", func(t *testing.T) {
		// Mirrors Database.SetGenesisCbor: genesis UTxO CBOR and Leios
		// endorser-block manifests share this same bp/bp..._metadata key
		// layout with ID=0, but are not decodable ledger blocks, so
		// GetBlock must not attempt to verify them.
		notABlock := []byte("genesis UTxO CBOR, not a block")
		key := []byte("synthetic-key-hash")
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 2000, key, notABlock,
			0, 0, 0, nil,
		))

		gotCbor, meta, err := store.GetBlock(txn, 2000, key)
		require.NoError(t, err)
		require.Equal(t, notABlock, gotCbor)
		require.Equal(t, uint64(0), meta.ID)
	})

	t.Run("ID=0 with a nonzero type is still verified", func(t *testing.T) {
		// Only the exact (ID, Type) == (0, 0) synthetic marker skips
		// verification. A real chain block never has both zero, so this
		// proves an ID==0 row with a nonzero type -- however it got that
		// way -- still gets its content checked rather than silently
		// passing through as though it were a synthetic entry.
		requestedHash := realBlock.Hash()
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(
			txn, 3000, requestedHash[:], otherBlock.Cbor(),
			0, uint(gledger.BlockTypeConway), 1, nil,
		))

		_, _, err := store.GetBlock(txn, 3000, requestedHash[:])
		require.ErrorIs(t, err, blockverify.ErrHashMismatch)
	})
}

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

func TestS3IteratorSeekAfterCloseReopensListing(t *testing.T) {
	fake := fakeBlockBucket(10)
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()
	store := storeOnFake(t, srv)
	txn := store.NewTransaction(false)
	defer txn.Rollback()
	it := store.NewIterator(
		txn,
		types.BlobIteratorOptions{Prefix: []byte(types.BlockBlobKeyPrefix)},
	)
	it.Close()
	seek := []byte(blockBlobKey(60))
	it.Seek(seek)
	if !it.Valid() {
		t.Fatalf("Seek after Close did not reopen listing: %v", it.Err())
	}
	if got := it.Item().Key(); string(got) != string(seek) {
		t.Fatalf("first key after Seek = %x, want %x", got, seek)
	}
}

// TestIsS3NotFoundRecognizesHeadObjectAndGetObjectVariants guards a real
// bug found via a live MinIO run: GetObject and HeadObject disagree on
// which error type/code they report for the identical "key does not
// exist" condition -- GetObject returns NoSuchKey, but HeadObject (which
// objectExists uses, and both Delete and Commit's per-key existence probe
// depend on) returns the differently-coded NotFound instead, since a HEAD
// response has no body to parse a specific error out of. Missing the
// NotFound case made every existence probe against a genuinely-absent key
// fail with a hard error instead of correctly reporting "not found".
func TestIsS3NotFoundRecognizesHeadObjectAndGetObjectVariants(t *testing.T) {
	require.True(t, isS3NotFound(&s3types.NoSuchKey{}))
	require.True(t, isS3NotFound(&s3types.NotFound{}))
	require.False(t, isS3NotFound(errors.New("some other failure")))
	require.False(t, isS3NotFound(nil))
}

func TestRegisterProvider(t *testing.T) {
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	require.Contains(t, host.Providers(), plugin.Descriptor{
		Capability: plugin.CapabilityStorageBlob,
		Name:       "s3", Description: "AWS S3 blob store",
	})
}

func TestReadBlobBodyWithLimit(t *testing.T) {
	data, err := readBlobBodyWithLimit(strings.NewReader("123"), 3)
	if err != nil || string(data) != "123" {
		t.Fatalf("read within limit = %q, %v", data, err)
	}
	if _, err := readBlobBodyWithLimit(strings.NewReader("1234"), 3); err == nil {
		t.Fatal("oversized object should be rejected")
	}
}

func TestS3TransactionStagesAndRollsBack(t *testing.T) {
	txn := &s3Txn{pending: make(map[string]s3PendingChange)}
	txn.stageSet([]byte("key"), []byte("value"))
	value, deleted, staged := txn.stagedValue([]byte("key"))
	if !staged || deleted || string(value) != "value" {
		t.Fatalf(
			"staged value = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	txn.stageDelete([]byte("key"))
	value, deleted, staged = txn.stagedValue([]byte("key"))
	if !staged || !deleted || value != nil {
		t.Fatalf(
			"staged delete = %q, deleted=%v, staged=%v",
			value,
			deleted,
			staged,
		)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if txn.pending != nil {
		t.Fatal("rollback should discard pending changes")
	}
}

func TestReverseKeyFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	f := &reverseKeyFile{file: file}
	for _, key := range []string{"a", "b", "c"} {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"c", "b", "a"} {
		got, valid, err := f.nextReverse()
		if err != nil || !valid || got != want {
			t.Fatalf("reverse key = %q, %v, %v", got, valid, err)
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf(
			"reverse iterator should be exhausted: valid=%v err=%v",
			valid,
			err,
		)
	}
	file.Close()
}

// Corrupt local spool framing must never yield a key to the cloud iterator.
func TestReverseKeyFileRejectsMalformedRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"short header", []byte{0, 0, 0}},
		{"missing prefix", []byte{0, 0, 0, 4}},
		{"truncated payload", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 2}},
		{"mismatched lengths", []byte{0, 0, 0, 2, 'a', 0, 0, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "reverse-")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if _, err := file.Write(tc.data); err != nil {
				t.Fatal(err)
			}
			f := &reverseKeyFile{file: file}
			key, valid, err := f.nextReverse()
			if err == nil || valid || key != "" {
				t.Fatalf(
					"malformed spool yielded key %q, valid=%v, err=%v",
					key,
					valid,
					err,
				)
			}
		})
	}
}

func TestReverseKeyFileEmptyAndBinaryKeys(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	keys := []string{
		"",
		string(binary.BigEndian.AppendUint32(nil, 0xffffffff)),
		"last",
	}
	for _, key := range keys {
		if err := writeReverseKey(file, key); err != nil {
			t.Fatal(err)
		}
	}
	f := &reverseKeyFile{file: file}
	for i := len(keys) - 1; i >= 0; i-- {
		key, valid, err := f.nextReverse()
		if err != nil || !valid || key != keys[i] {
			t.Fatalf(
				"reverse key = %q, valid=%v, err=%v; want %q",
				key,
				valid,
				err,
				keys[i],
			)
		}
	}
	if _, valid, err := f.nextReverse(); err != nil || valid {
		t.Fatalf("exhausted iterator: valid=%v err=%v", valid, err)
	}
}

func TestReverseIteratorPropagatesSpoolCorruption(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "reverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := writeReverseKey(file, "a"); err != nil {
		t.Fatal(err)
	}
	// The trailer still declares one byte, but the prefix declares two.
	if _, err := file.WriteAt([]byte{0, 0, 0, 2}, 0); err != nil {
		t.Fatal(err)
	}
	it := &s3ReverseIterator{keys: &reverseKeyFile{file: file}}
	it.Rewind()
	if it.Err() == nil || it.Valid() || it.Item() != nil {
		t.Fatalf(
			"corrupted spool iterator: valid=%v err=%v",
			it.Valid(),
			it.Err(),
		)
	}
}

// fakeS3List serves just enough ListObjectsV2 to drive s3StreamIterator, and
// records the start-after parameter of every request it answers.
type fakeS3List struct {
	mu         sync.Mutex
	keys       []string // full object keys, sorted
	startAfter []string // one entry per ListObjectsV2 request
	continued  []string // continuation-token, positionally matching startAfter
	returned   int      // total keys returned across all requests
}

func (f *fakeS3List) handler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = tok
	}
	f.mu.Lock()
	f.startAfter = append(f.startAfter, q.Get("start-after"))
	f.continued = append(f.continued, q.Get("continuation-token"))
	f.mu.Unlock()

	var out []string
	for _, k := range f.keys {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if after != "" && k <= after {
			continue
		}
		out = append(out, k)
	}
	const page = 1000
	truncated := false
	if len(out) > page {
		out = out[:page]
		truncated = true
	}
	f.mu.Lock()
	f.returned += len(out)
	f.mu.Unlock()

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
	)
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", truncated)
	for _, k := range out {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>1</Size></Contents>", k)
	}
	if truncated && len(out) > 0 {
		fmt.Fprintf(
			&b,
			"<NextContinuationToken>%s</NextContinuationToken>",
			out[len(out)-1],
		)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

// TestS3SeekBoundsListServerSide pins that seeking a forward iterator asks the
// server to start near the seek key instead of listing the whole prefix and
// discarding keys client-side.
//
// bark's ArchiveService is unauthenticated, and height-only block references
// drive a binary search whose every probe seeks into the full "bi" prefix. With
// no server-side bound each probe lists the prefix from the start, so one
// anonymous request costs order N keys per probe.
func TestS3SeekBoundsListServerSide(t *testing.T) {
	const total = 20000
	fake := &fakeS3List{}
	for i := range total {
		key := fmt.Sprintf("bi%06d", i)
		fake.keys = append(fake.keys, hex.EncodeToString([]byte(key)))
	}
	sort.Strings(fake.keys)

	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	store, err := NewWithOptions(
		WithBucket("test-bucket"),
		WithEndpoint(srv.URL),
		WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// Start() would need a real bucket; the iterator only needs the client.
	store.client = s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(
			"test", "test", "",
		),
	})

	seek := []byte(fmt.Sprintf("bi%06d", total/2))
	it := &s3StreamIterator{store: store, prefix: []byte("bi")}
	it.Seek(seek)
	// The listing is issued on the first read, not by Seek itself.
	if !it.Valid() {
		if it.err != nil {
			t.Fatalf("seek: %v", it.err)
		}
		t.Fatal("seek landed on no key")
	}
	if got := it.key; got != string(seek) {
		t.Fatalf("seek key = %q, want %q", got, string(seek))
	}

	fake.mu.Lock()
	reqs := append([]string(nil), fake.startAfter...)
	returned := fake.returned
	fake.mu.Unlock()

	t.Logf(
		"seek to mid-point of %d keys: %d ListObjectsV2 request(s), "+
			"%d key(s) returned",
		total,
		len(reqs),
		returned,
	)

	bounded := 0
	for _, s := range reqs {
		if s != "" {
			bounded++
		}
	}
	if bounded == 0 {
		t.Fatalf(
			"seek to the mid-point issued %d ListObjectsV2 request(s) and "+
				"returned %d key(s), none carrying start-after: the whole "+
				"prefix is listed and discarded client-side",
			len(reqs),
			returned,
		)
	}
	// The bound is a strict prefix of the seek key, so a handful of keys just
	// below it can still come back -- but not half the bucket.
	if returned > total/10 {
		t.Fatalf(
			"seek returned %d of %d keys; the server-side bound is not "+
				"restricting the listing",
			returned,
			total,
		)
	}
}

// resolveKey is the single read path shared by Get and every typed getter. A
// value staged by this transaction has to win over the bucket, otherwise a
// read-after-write inside one transaction returns pre-transaction state and the
// plugin diverges from badger. The bucket is never consulted for a staged key,
// so a nil client is proof the read was served from the staging map.
func TestResolveKeyServesStagedWrite(t *testing.T) {
	store := &BlobStoreS3{}
	txn := &s3Txn{store: store, pending: make(map[string]s3PendingChange)}
	txn.stageSet([]byte("key"), []byte("staged"))

	value, err := store.resolveKey(context.Background(), txn, []byte("key"))
	if err != nil {
		t.Fatalf("resolveKey on a staged write: %v", err)
	}
	if string(value) != "staged" {
		t.Fatalf("resolveKey = %q, want %q", value, "staged")
	}
}

// A staged delete has to read as missing for the rest of the transaction.
func TestResolveKeyReportsStagedDeleteAsMissing(t *testing.T) {
	store := &BlobStoreS3{}
	txn := &s3Txn{store: store, pending: make(map[string]s3PendingChange)}
	txn.stageDelete([]byte("key"))

	_, err := store.resolveKey(context.Background(), txn, []byte("key"))
	if !errors.Is(err, types.ErrBlobKeyNotFound) {
		t.Fatalf(
			"resolveKey on a staged delete = %v, want ErrBlobKeyNotFound",
			err,
		)
	}
}

// Iterators must not list a key this transaction has staged for deletion: the
// value path resolves staged changes, so listing it would surface a key whose
// value immediately reads back as missing.
func TestStagedDeletedFiltersIteratorKeys(t *testing.T) {
	store := &BlobStoreS3{}
	txn := &s3Txn{store: store, pending: make(map[string]s3PendingChange)}
	txn.stageDelete([]byte("gone"))
	txn.stageSet([]byte("kept"), []byte("value"))

	if !stagedDeleted(txn, "gone") {
		t.Fatal("a staged delete should be filtered from listings")
	}
	if stagedDeleted(txn, "kept") {
		t.Fatal("a staged write must not be filtered from listings")
	}
	if stagedDeleted(txn, "untouched") {
		t.Fatal("an unstaged key must not be filtered from listings")
	}

	// A finished transaction has no staged state to honor, and a foreign txn
	// type must not panic the iterator.
	txn.finished = true
	if stagedDeleted(txn, "gone") {
		t.Fatal("a finished transaction should not filter listings")
	}
	if stagedDeleted(nil, "gone") {
		t.Fatal("a nil transaction should not filter listings")
	}
}

// A zero-length write is a real value, not a deletion. Collapsing the two would
// make Set of an empty blob read back as ErrBlobKeyNotFound until commit.
func TestResolveKeyServesStagedEmptyValue(t *testing.T) {
	st := &BlobStoreS3{}
	txn := &s3Txn{store: st, pending: make(map[string]s3PendingChange)}
	txn.stageSet([]byte("key"), []byte{})

	value, deleted, staged := txn.stagedValue([]byte("key"))
	if !staged || deleted {
		t.Fatalf(
			"empty write should be staged and not deleted, got deleted=%v staged=%v",
			deleted,
			staged,
		)
	}
	if value == nil || len(value) != 0 {
		t.Fatalf("staged empty value = %v, want an empty non-nil slice", value)
	}

	got, err := st.resolveKey(context.Background(), txn, []byte("key"))
	if err != nil {
		t.Fatalf("resolveKey on a staged empty value: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("resolveKey = %q, want empty", got)
	}

	// Iterators must still list it: it is a write, not a delete.
	if stagedDeleted(txn, "key") {
		t.Fatal("a staged empty value must not be filtered from listings")
	}
}

// RollbackIsNoop must report false now that mutations are staged and applied
// only in Commit: Rollback discards the staged work without issuing any S3
// request. It reported true when Set/Delete wrote through immediately, and
// database/lifecycle/blob_bulk_delete.go reads this flag to decide whether a
// failed batch's deletes are permanent — reporting true made a truncate count
// blocks it never removed.
func TestRollbackIsNoopReportsFalse(t *testing.T) {
	txn := &s3Txn{pending: make(map[string]s3PendingChange)}
	if txn.RollbackIsNoop() {
		t.Fatal(
			"staged transactions are reversible: Rollback issues no requests",
		)
	}

	// Rollback really does discard staged work rather than applying it.
	txn.stageSet([]byte("key"), []byte("value"))
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	if txn.pending != nil {
		t.Fatal("rollback should discard pending changes")
	}
}

func TestTombstoneBlockRetainsMetadata(t *testing.T) {
	store, err := NewWithOptions()
	require.NoError(t, err)
	store.client = new(s3.Client)
	txn := store.NewTransaction(true)
	slot := uint64(42)
	hash := []byte("s3-tombstone-metadata")

	require.NoError(t, store.SetBlock(
		txn, slot, hash, []byte{0x80}, 7, 1, 6, nil,
	))
	require.NoError(t, store.TombstoneBlock(txn, slot, hash))

	_, metadata, err := store.GetBlock(txn, slot, hash)
	require.ErrorIs(t, err, types.ErrHistoryExpired)
	require.Equal(t, uint64(7), metadata.ID)
}
