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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

const (
	reclaimBodySize = 8 << 10
	reclaimBodies   = 2000
)

func reclaimKey(i int) []byte {
	return []byte(fmt.Sprintf("bp%08d", i))
}

// supersededBytes sums every version of a "bp" key that is larger than a
// marker, which is what a replaced body still occupies.
func supersededBytes(t *testing.T, store *BlobStoreBadger) int {
	t.Helper()
	total := 0
	require.NoError(t, store.DB().View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if item := it.Item(); item.ValueSize() > 64 {
				total += int(item.ValueSize())
			}
		}
		return nil
	}))
	return total
}

func newReclaimStore(t *testing.T) *BlobStoreBadger {
	t.Helper()
	// The defaults reserve a 2 GiB value log per store; these tests write
	// about 16 MiB, in transactions of 100 bodies the memtable must hold.
	store, err := New(
		WithDataDir(t.TempDir()),
		WithGc(false),
		WithValueLogFileSize(1<<24),
		WithMemTableSize(32<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// replaceBodies stores bodies and then overwrites each with a short value,
// the shape of block promotion.
func replaceBodies(t *testing.T, store *BlobStoreBadger) {
	t.Helper()
	body := make([]byte, reclaimBodySize)
	for start := 0; start < reclaimBodies; start += 100 {
		txn := store.NewTransaction(true)
		for i := start; i < start+100; i++ {
			require.NoError(t, store.Set(txn, reclaimKey(i), body))
		}
		require.NoError(t, txn.Commit())
	}
	// A memtable flush keeps only the newest version of each key, so the
	// bodies must reach the bottom level before the markers are written, as they do
	// when promotion trails the chain tip.
	flush := []byte("\xff\xfftest-flush")
	require.NoError(t, store.DB().Update(func(txn *badgerdb.Txn) error {
		return txn.Set(flush, []byte{1})
	}))
	require.NoError(t, store.DB().DropPrefix(flush))
	require.NoError(t, store.DB().Flatten(1))
	for start := 0; start < reclaimBodies; start += 100 {
		txn := store.NewTransaction(true)
		for i := start; i < start+100; i++ {
			require.NoError(t, store.Set(txn, reclaimKey(i), []byte("marker")))
		}
		require.NoError(t, txn.Commit())
	}
}

func TestReclaimSpaceDropsSupersededValues(t *testing.T) {
	t.Parallel()
	store := newReclaimStore(t)
	replaceBodies(t, store)
	// Without a compaction both versions of every key are still held.
	require.Equal(
		t, reclaimBodies*reclaimBodySize, supersededBytes(t, store),
		"setup must leave every replaced body in place",
	)

	require.NoError(t, store.ReclaimSpace())

	require.Zero(
		t, supersededBytes(t, store),
		"replaced bodies still occupy the store after ReclaimSpace",
	)
	for _, i := range []int{0, reclaimBodies / 2, reclaimBodies - 1} {
		txn := store.NewTransaction(false)
		v, err := store.Get(txn, reclaimKey(i))
		require.NoError(t, err)
		require.Equal(t, []byte("marker"), v)
		require.NoError(t, txn.Rollback())
	}
	txn := store.NewTransaction(false)
	_, err := store.Get(txn, []byte(reclaimSentinelPrefix))
	require.Error(t, err, "the flush sentinel must not survive")
	require.NoError(t, txn.Rollback())
}

func TestReclaimSpaceNeverFailsConcurrentCommits(t *testing.T) {
	t.Parallel()
	store := newReclaimStore(t)
	replaceBodies(t, store)

	var failed atomic.Value
	var commits atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			txn := store.NewTransaction(true)
			if err := store.Set(txn, fmt.Appendf(nil, "wr%08d", i), []byte("v")); err != nil {
				failed.Store(err)
				return
			}
			if err := txn.Commit(); err != nil {
				failed.Store(err)
				return
			}
			commits.Add(1)
		}
	})
	for range 5 {
		require.NoError(t, store.ReclaimSpace())
	}
	close(stop)
	wg.Wait()
	require.Nil(t, failed.Load(), "a commit raced a reclaim")
	require.Positive(t, commits.Load())
}

// staleBytes sums every version of every key except the newest, which is what
// a compaction that covers all of a key's versions would free.
func staleBytes(t *testing.T, store *BlobStoreBadger) int {
	t.Helper()
	total := 0
	require.NoError(t, store.DB().View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var last []byte
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if last != nil && bytes.Equal(item.Key(), last) {
				total += int(item.ValueSize())
				continue
			}
			last = item.KeyCopy(last[:0])
		}
		return nil
	}))
	return total
}

// bottomTables returns the IDs of the tables in the store's last level.
func bottomTables(t *testing.T, store *BlobStoreBadger) map[uint64]struct{} {
	t.Helper()
	db := store.DB()
	last := db.Opts().MaxLevels - 1
	ids := make(map[uint64]struct{})
	for _, ti := range db.Tables() {
		if ti.Level == last {
			ids[ti.ID] = struct{}{}
		}
	}
	return ids
}

// flushMemtable flushes the store's memtable the way ReclaimSpace does,
// which also compacts L0 into the base level.
func flushMemtable(t *testing.T, store *BlobStoreBadger) {
	t.Helper()
	flush := []byte("\xff\xfftest-flush")
	require.NoError(t, store.DB().Update(func(txn *badgerdb.Txn) error {
		return txn.Set(flush, []byte{1})
	}))
	require.NoError(t, store.DB().DropPrefix(flush))
}

// bottomBodies stores reclaimBodies bodies and compacts them to the bottom
// level, as on a node that has run for a while, and returns the body. The
// body does not compress, so the bottom level outgrows badger's base level
// size and later memtable flushes land in an upper level rather than in it.
func bottomBodies(t *testing.T, store *BlobStoreBadger) []byte {
	t.Helper()
	body := make([]byte, reclaimBodySize)
	_, err := rand.Read(body)
	require.NoError(t, err)
	for start := 0; start < reclaimBodies; start += 100 {
		txn := store.NewTransaction(true)
		for i := start; i < start+100; i++ {
			require.NoError(t, store.Set(txn, reclaimKey(i), body))
		}
		require.NoError(t, txn.Commit())
	}
	flushMemtable(t, store)
	require.NoError(t, store.DB().Flatten(1))
	return body
}

// TestReclaimSpaceLeavesBottomLevelAtTip covers a reclaim at the chain tip:
// the replaced bodies are recent, so they sit in an upper level, and the same
// window wrote keys across the whole key space. The reclaim must drop the
// replaced bodies without rewriting the bottom level, whose tables hold
// nothing superseded. Rewriting it replaces every bottom table under the
// level's write lock, which blocks every read of the store.
func TestReclaimSpaceLeavesBottomLevelAtTip(t *testing.T) {
	t.Parallel()
	store := newReclaimStore(t)
	body := bottomBodies(t, store)

	// Recent bodies, flushed out of the memtable into an upper level.
	const recent = 50
	recentKey := func(i int) []byte {
		return []byte(fmt.Sprintf("bq%08d", i))
	}
	txn := store.NewTransaction(true)
	for i := range recent {
		require.NoError(t, store.Set(txn, recentKey(i), body))
	}
	require.NoError(t, txn.Commit())
	flushMemtable(t, store)

	before := bottomTables(t, store)
	require.Greater(t, len(before), 1, "setup must leave several bottom tables")

	// Promotion replaces the recent bodies with markers while the node keeps
	// writing keys at both ends of the key space.
	txn = store.NewTransaction(true)
	for i := range recent {
		require.NoError(t, store.Set(txn, recentKey(i), []byte("marker")))
	}
	require.NoError(t, store.Set(txn, []byte("a-tip"), []byte{1}))
	require.NoError(t, store.Set(txn, []byte("u-tip"), []byte{1}))
	require.NoError(t, txn.Commit())
	require.Equal(
		t, recent*reclaimBodySize, staleBytes(t, store),
		"setup must leave every replaced body in place",
	)

	require.NoError(t, store.ReclaimSpace())

	require.Zero(
		t, staleBytes(t, store),
		"replaced bodies still occupy the store after ReclaimSpace",
	)
	after := bottomTables(t, store)
	for id := range before {
		_, ok := after[id]
		require.True(
			t, ok,
			"ReclaimSpace rewrote bottom table %d, which held nothing superseded", id,
		)
	}
	rtxn := store.NewTransaction(false)
	defer func() { require.NoError(t, rtxn.Rollback()) }()
	v, err := store.Get(rtxn, reclaimKey(reclaimBodies/2))
	require.NoError(t, err)
	require.Equal(t, body, v)
	v, err = store.Get(rtxn, recentKey(recent/2))
	require.NoError(t, err)
	require.Equal(t, []byte("marker"), v)
}

// TestReclaimSpaceCompactsBottomLevelBacklog covers a whole-history
// promotion: every body already sits in the bottom level when it is
// replaced, so only a rewrite of that level frees it, and the replaced bodies
// are most of the store.
func TestReclaimSpaceCompactsBottomLevelBacklog(t *testing.T) {
	t.Parallel()
	store := newReclaimStore(t)
	bottomBodies(t, store)
	for start := 0; start < reclaimBodies; start += 100 {
		txn := store.NewTransaction(true)
		for i := start; i < start+100; i++ {
			require.NoError(t, store.Set(txn, reclaimKey(i), []byte("marker")))
		}
		require.NoError(t, txn.Commit())
	}
	require.Equal(
		t, reclaimBodies*reclaimBodySize, staleBytes(t, store),
		"setup must leave every replaced body in place",
	)

	require.NoError(t, store.ReclaimSpace())

	require.Zero(
		t, staleBytes(t, store),
		"replaced bodies still occupy the store after ReclaimSpace",
	)
	txn := store.NewTransaction(false)
	defer func() { require.NoError(t, txn.Rollback()) }()
	v, err := store.Get(txn, reclaimKey(reclaimBodies/2))
	require.NoError(t, err)
	require.Equal(t, []byte("marker"), v)
}
