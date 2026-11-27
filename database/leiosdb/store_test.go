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

package leiosdb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

type storeFactory struct {
	name string
	open func(t *testing.T, dir string, hook func(string) error) leiosdb.Store
}

func factories() []storeFactory {
	return []storeFactory{
		{
			name: "memory",
			open: func(t *testing.T, _ string, hook func(string) error) leiosdb.Store {
				t.Helper()
				s, err := leiosdb.Open(leiosdb.Options{Hook: hook})
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
		{
			name: "badger",
			open: func(t *testing.T, dir string, hook func(string) error) leiosdb.Store {
				t.Helper()
				s, err := leiosdb.Open(leiosdb.Options{
					DataDir:          dir,
					Hook:             hook,
					ValueLogFileSize: 16 << 20,
					MemTableSize:     8 << 20,
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				return s
			},
		},
	}
}

func eb(slot uint64, fill byte) leiosdb.Ref {
	return leiosdb.Ref{Slot: slot, Hash: bytes.Repeat([]byte{fill}, 32)}
}

func content(tag string, txs ...string) leiosdb.Content {
	c := leiosdb.Content{Manifest: []byte("manifest-" + tag)}
	if txs != nil {
		c.Txs = make([]cbor.RawMessage, len(txs))
		for i, tx := range txs {
			raw, err := cbor.Encode([]byte(tx))
			if err != nil {
				panic(err)
			}
			c.Txs[i] = raw
		}
	}
	return c
}

func forEachStore(t *testing.T, fn func(t *testing.T, f storeFactory)) {
	t.Helper()
	for _, f := range factories() {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			fn(t, f)
		})
	}
}

func TestStorePutGetRoundTrip(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ref := eb(10, 1)
		_, err := s.Get(ref)
		require.ErrorIs(t, err, leiosdb.ErrNotFound)

		require.NoError(t, s.PutVolatile(ref, content("a")))
		got, err := s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, []byte("manifest-a"), got.Manifest)
		require.False(t, got.Complete(), "manifest-only write has no tx list")

		full := content("a", "tx1", "tx2")
		require.NoError(t, s.PutVolatile(ref, full))
		got, err = s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full.Txs, got.Txs)

		empty := content("b")
		empty.Txs = []cbor.RawMessage{}
		other := eb(10, 2)
		require.NoError(t, s.PutVolatile(other, empty))
		got, err = s.Get(other)
		require.NoError(t, err)
		require.True(t, got.Complete(), "an empty tx list is a stored list")
		require.Empty(t, got.Txs)

		// The same hash at another slot is its own occurrence.
		_, err = s.Get(leiosdb.Ref{Slot: 11, Hash: ref.Hash})
		require.ErrorIs(t, err, leiosdb.ErrNotFound)
	})
}

func TestStoreClosureRoundTrip(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ref := eb(10, 1)
		_, err := s.GetClosure(ref)
		require.ErrorIs(t, err, leiosdb.ErrNotFound)
		require.NoError(t, s.PutClosure(ref, []byte("closure")))
		got, err := s.GetClosure(ref)
		require.NoError(t, err)
		require.Equal(t, []byte("closure"), got)
		// A closure alone is not endorser-block content.
		_, err = s.Get(ref)
		require.ErrorIs(t, err, leiosdb.ErrNotFound)
	})
}

func TestStorePinMovesContentAndIsIdempotent(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ref, cert := eb(10, 1), eb(11, 9)
		full := content("a", "tx1")
		require.NoError(t, s.PutVolatile(ref, full))
		require.NoError(t, s.PutClosure(ref, []byte("closure")))

		pinned, err := s.IsPinned(ref)
		require.NoError(t, err)
		require.False(t, pinned)

		res, err := s.Pin(ref, cert)
		require.NoError(t, err)
		require.Equal(t, leiosdb.PinResult{}, res)
		pinned, err = s.IsPinned(ref)
		require.NoError(t, err)
		require.True(t, pinned)

		got, err := s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got)
		closure, err := s.GetClosure(ref)
		require.NoError(t, err)
		require.Equal(t, []byte("closure"), closure)

		res, err = s.Pin(ref, cert)
		require.NoError(t, err)
		require.True(t, res.AlreadyPinned)

		// The volatile partition no longer holds it: collecting everything
		// leaves the pinned block readable.
		_, err = s.GarbageCollect(context.Background(), 1000)
		require.NoError(t, err)
		got, err = s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got)
	})
}

func TestStoreGarbageCollectIsVolatileOnlyAndStrictlyBelow(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		old, edge, newer, pinnedRef := eb(5, 1), eb(10, 2), eb(11, 3), eb(6, 4)
		for _, r := range []leiosdb.Ref{old, edge, newer, pinnedRef} {
			require.NoError(t, s.PutVolatile(r, content("x", "tx")))
		}
		_, err := s.Pin(pinnedRef, eb(7, 9))
		require.NoError(t, err)

		deleted, err := s.GarbageCollect(context.Background(), 10)
		require.NoError(t, err)
		require.Equal(t, 3, deleted, "marker, manifest, and txs")

		_, err = s.Get(old)
		require.ErrorIs(t, err, leiosdb.ErrNotFound)
		_, err = s.Get(edge)
		require.NoError(t, err, "a block at the boundary slot is kept")
		_, err = s.Get(newer)
		require.NoError(t, err)
		_, err = s.Get(pinnedRef)
		require.NoError(t, err, "a pinned block survives collection")

		wm, err := s.Watermarks()
		require.NoError(t, err)
		require.Equal(t, uint64(10), wm.GCBefore)
	})
}

func TestStoreRefusesVolatileWriteBelowCollectionWatermark(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		_, err := s.GarbageCollect(context.Background(), 10)
		require.NoError(t, err)

		err = s.PutVolatile(eb(9, 1), content("a"))
		require.ErrorIs(t, err, leiosdb.ErrBehindImmutableTip)
		_, err = s.Get(eb(9, 1))
		require.ErrorIs(
			t,
			err,
			leiosdb.ErrNotFound,
			"a refused write stores nothing",
		)
		require.NoError(t, s.PutVolatile(eb(10, 1), content("a")))

		// A pinned block below the watermark still accepts its content: that
		// is how a content-pending pin is repaired.
		pinnedRef := eb(3, 2)
		_, err = s.Pin(pinnedRef, eb(4, 9))
		require.NoError(t, err)
		require.NoError(t, s.PutVolatile(pinnedRef, content("late", "tx")))
		got, err := s.Get(pinnedRef)
		require.NoError(t, err)
		require.True(t, got.Complete())
	})
}

func TestStoreGarbageCollectWatermarkNeverMovesBack(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		_, err := s.GarbageCollect(context.Background(), 20)
		require.NoError(t, err)
		_, err = s.GarbageCollect(context.Background(), 5)
		require.NoError(t, err)
		wm, err := s.Watermarks()
		require.NoError(t, err)
		require.Equal(t, uint64(20), wm.GCBefore)
	})
}

func TestStoreContentPendingPinAndRepair(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ref, cert := eb(10, 1), eb(11, 9)

		res, err := s.Pin(ref, cert)
		require.NoError(t, err)
		require.True(t, res.ContentPending)
		pending, err := s.PendingPins()
		require.NoError(t, err)
		require.Equal(
			t,
			[]leiosdb.PendingPin{{EB: ref, Certifier: cert}},
			pending,
		)

		// Manifest alone does not settle the pin.
		require.NoError(t, s.PutVolatile(ref, content("a")))
		pending, err = s.PendingPins()
		require.NoError(t, err)
		require.Len(t, pending, 1)

		require.NoError(t, s.PutVolatile(ref, content("a", "tx")))
		pending, err = s.PendingPins()
		require.NoError(t, err)
		require.Empty(t, pending)
		got, err := s.Get(ref)
		require.NoError(t, err)
		require.True(t, got.Complete())
	})
}

func TestStoreMaxSlot(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		_, found, err := s.MaxSlot()
		require.NoError(t, err)
		require.False(t, found)

		require.NoError(t, s.PutVolatile(eb(10, 1), content("a", "tx")))
		require.NoError(t, s.PutVolatile(eb(30, 2), content("b", "tx")))
		require.NoError(t, s.PutClosure(eb(99, 3), []byte("closure only")))
		slot, found, err := s.MaxSlot()
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(
			t,
			uint64(30),
			slot,
			"a closure alone is not a stored endorser block",
		)

		_, err = s.Pin(eb(30, 2), eb(31, 9))
		require.NoError(t, err)
		require.NoError(t, s.PutVolatile(eb(20, 4), content("c")))
		slot, _, err = s.MaxSlot()
		require.NoError(t, err)
		require.Equal(t, uint64(30), slot, "pinned blocks count")

		_, err = s.GarbageCollect(context.Background(), 25)
		require.NoError(t, err)
		slot, _, err = s.MaxSlot()
		require.NoError(t, err)
		require.Equal(t, uint64(30), slot)
	})
}

func TestStoreRollbackPins(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		keep, drop, legacy := eb(10, 1), eb(20, 2), eb(30, 3)
		for _, r := range []leiosdb.Ref{keep, drop, legacy} {
			require.NoError(t, s.PutVolatile(r, content("x", "tx")))
		}
		require.NoError(t, s.PutClosure(drop, []byte("closure-drop")))
		_, err := s.Pin(keep, eb(11, 9))
		require.NoError(t, err)
		_, err = s.Pin(drop, eb(21, 9))
		require.NoError(t, err)
		_, err = s.Pin(legacy, leiosdb.Ref{})
		require.NoError(t, err)
		through := eb(40, 8)
		require.NoError(t, s.AdvancePinnedThrough(through))
		_, err = s.GarbageCollect(context.Background(), 35)
		require.NoError(t, err)

		target := eb(15, 7)
		removed, err := s.RollbackPins(target)
		require.NoError(t, err)
		require.Equal(t, 1, removed)

		_, err = s.Get(keep)
		require.NoError(t, err)
		// The rolled-back block keeps its records, now volatile, because the
		// chain may still reference them.
		got, err := s.Get(drop)
		require.NoError(t, err)
		require.Equal(t, content("x", "tx"), got)
		closure, err := s.GetClosure(drop)
		require.NoError(t, err)
		require.Equal(t, []byte("closure-drop"), closure)
		pinned, err := s.IsPinned(drop)
		require.NoError(t, err)
		require.False(t, pinned)
		_, err = s.Get(legacy)
		require.NoError(t, err, "a pin of unknown certifier is kept")

		wm, err := s.Watermarks()
		require.NoError(t, err)
		require.Equal(t, target, wm.PinnedThrough)
		require.Equal(t, uint64(15), wm.GCBefore)
		// The re-applied chain can store the rolled-back slot range again.
		require.NoError(t, s.PutVolatile(eb(16, 5), content("again", "tx")))
	})
}

func TestStoreAdvancePinnedThroughNeverMovesBack(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		wm, err := s.Watermarks()
		require.NoError(t, err)
		require.False(t, wm.PinnedThroughSet)
		require.NoError(t, s.AdvancePinnedThrough(eb(20, 1)))
		require.NoError(t, s.AdvancePinnedThrough(eb(10, 2)))
		wm, err = s.Watermarks()
		require.NoError(t, err)
		require.True(t, wm.PinnedThroughSet)
		require.Equal(t, eb(20, 1), wm.PinnedThrough)
	})
}

func TestStoreMigrationState(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		state, err := s.MigrationState()
		require.NoError(t, err)
		require.Equal(t, leiosdb.MigrationNone, state)
		require.NoError(t, s.SetMigrationState(leiosdb.MigrationCopied))
		state, err = s.MigrationState()
		require.NoError(t, err)
		require.Equal(t, leiosdb.MigrationCopied, state)
	})
}

func TestStoreClosedReturnsErrClosed(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		require.NoError(t, s.Close())
		require.NoError(t, s.Close(), "close is idempotent")
		require.ErrorIs(
			t,
			s.PutVolatile(eb(1, 1), content("a")),
			leiosdb.ErrClosed,
		)
		_, err := s.Get(eb(1, 1))
		require.ErrorIs(t, err, leiosdb.ErrClosed)
	})
}

func TestStorePersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() leiosdb.Store {
		s, err := leiosdb.Open(leiosdb.Options{
			DataDir:          dir,
			ValueLogFileSize: 16 << 20,
			MemTableSize:     8 << 20,
		})
		require.NoError(t, err)
		return s
	}
	s := open()
	ref := eb(10, 1)
	require.NoError(t, s.PutVolatile(ref, content("a", "tx")))
	_, err := s.Pin(ref, eb(11, 9))
	require.NoError(t, err)
	require.NoError(t, s.AdvancePinnedThrough(eb(11, 9)))
	_, err = s.GarbageCollect(context.Background(), 5)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s = open()
	defer s.Close()
	got, err := s.Get(ref)
	require.NoError(t, err)
	require.True(t, got.Complete())
	wm, err := s.Watermarks()
	require.NoError(t, err)
	require.Equal(t, eb(11, 9), wm.PinnedThrough)
	require.Equal(t, uint64(5), wm.GCBefore)
}

var errCrash = errors.New("simulated crash")

func crashAt(point string) func(string) error {
	return func(p string) error {
		if p == point {
			return errCrash
		}
		return nil
	}
}

// A crash between the copy into the immutable partition and the pin record
// leaves the block unpinned and fully readable; the retry completes the pin.
func TestStoreCrashAfterPinCopyBeforePinRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func(hook func(string) error) leiosdb.Store {
		s, err := leiosdb.Open(leiosdb.Options{
			DataDir:          dir,
			Hook:             hook,
			ValueLogFileSize: 16 << 20,
			MemTableSize:     8 << 20,
		})
		require.NoError(t, err)
		return s
	}
	s := open(crashAt(leiosdb.HookPinCopied))
	ref, cert := eb(10, 1), eb(11, 9)
	full := content("a", "tx")
	require.NoError(t, s.PutVolatile(ref, full))
	_, err := s.Pin(ref, cert)
	require.ErrorIs(t, err, errCrash)
	require.NoError(t, s.Close())

	s = open(nil)
	defer s.Close()
	pinned, err := s.IsPinned(ref)
	require.NoError(t, err)
	require.False(t, pinned)
	got, err := s.Get(ref)
	require.NoError(t, err)
	require.Equal(t, full, got)

	res, err := s.Pin(ref, cert)
	require.NoError(t, err)
	require.False(t, res.AlreadyPinned)
	require.False(t, res.ContentPending)
	got, err = s.Get(ref)
	require.NoError(t, err)
	require.Equal(t, full, got)
}

// A crash after the watermark is persisted and before the deletes leaves the
// records in place; the next collection removes them and never resurrects the
// refusal below the watermark.
func TestStoreCrashMidGarbageCollect(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func(hook func(string) error) leiosdb.Store {
		s, err := leiosdb.Open(leiosdb.Options{
			DataDir:          dir,
			Hook:             hook,
			ValueLogFileSize: 16 << 20,
			MemTableSize:     8 << 20,
		})
		require.NoError(t, err)
		return s
	}
	s := open(crashAt(leiosdb.HookGCWatermark))
	require.NoError(t, s.PutVolatile(eb(5, 1), content("a", "tx")))
	_, err := s.GarbageCollect(context.Background(), 10)
	require.ErrorIs(t, err, errCrash)
	require.NoError(t, s.Close())

	s = open(nil)
	defer s.Close()
	wm, err := s.Watermarks()
	require.NoError(t, err)
	require.Equal(t, uint64(10), wm.GCBefore)
	_, err = s.Get(eb(5, 1))
	require.NoError(t, err, "the interrupted collection deleted nothing yet")

	// A retry with a lower bound still finishes the interrupted collection.
	deleted, err := s.GarbageCollect(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 3, deleted)
	_, err = s.Get(eb(5, 1))
	require.ErrorIs(t, err, leiosdb.ErrNotFound)
}

func TestStoreGarbageCollectHonoursCancellation(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := s.GarbageCollect(ctx, 10)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// syncCountingStore wraps a blob store to count the flushes a caller asks for.
type syncCountingStore struct {
	blob.BlobStore
	syncs      atomic.Int64
	writes     atomic.Int64
	valueBytes atomic.Int64
}

func (s *syncCountingStore) Sync() error {
	s.syncs.Add(1)
	return s.BlobStore.Sync()
}

func (s *syncCountingStore) Set(
	txn types.Txn,
	key []byte,
	value []byte,
) error {
	s.writes.Add(1)
	s.valueBytes.Add(int64(len(value)))
	return s.BlobStore.Set(txn, key, value)
}

func TestStoreFlushesWritesCrossStoreCommitsDependOn(t *testing.T) {
	t.Parallel()
	inner, err := leiosdb.Open(leiosdb.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner.Close() })
	counting := &syncCountingStore{BlobStore: inner.BlobStore()}
	s := leiosdb.Wrap(counting)
	ref, cert := eb(10, 1), eb(11, 9)

	require.NoError(t, s.PutVolatile(ref, content("a", "tx")))
	require.Equal(t, int64(1), counting.syncs.Load(), "PutVolatile")
	require.NoError(t, s.PutClosure(ref, []byte("closure")))
	require.Equal(t, int64(2), counting.syncs.Load(), "PutClosure")
	_, err = s.Pin(ref, cert)
	require.NoError(t, err)
	require.Equal(
		t,
		int64(3),
		counting.syncs.Load(),
		"one flush for the whole pin, not one per copied record",
	)
	require.NoError(t, s.SetMigrationState(leiosdb.MigrationCopied))
	require.Equal(t, int64(4), counting.syncs.Load(), "SetMigrationState")
}

func TestStoreReadsPinsAndDemotesLegacyPayload(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		ref, cert := eb(10, 1), eb(11, 9)
		full := content("a", "tx")
		txs, err := cbor.Encode(full.Txs)
		require.NoError(t, err)
		legacy := s.(*leiosdb.BadgerStore).BlobStore()
		txn := legacy.NewTransaction(true)
		for kind, value := range map[byte][]byte{
			'm': full.Manifest,
			't': txs,
			'c': []byte("closure"),
		} {
			require.NoError(t, legacy.Set(txn, legacyRecordKey('v', ref, kind), value))
		}
		require.NoError(t, txn.Commit())

		got, err := s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got)
		closure, err := s.GetClosure(ref)
		require.NoError(t, err)
		require.Equal(t, []byte("closure"), closure)

		res, err := s.Pin(ref, cert)
		require.NoError(t, err)
		require.False(t, res.ContentPending)
		_, err = s.GarbageCollect(context.Background(), 100)
		require.NoError(t, err)
		got, err = s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got)
		require.NoError(t, s.PutClosure(ref, []byte("current closure")))
		require.NoError(t, s.PutVolatile(ref, content("partial current")))
		got, err = s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got, "complete legacy content wins over partial current content")

		rolledBack, err := s.RollbackPins(eb(9, 8))
		require.NoError(t, err)
		require.Equal(t, 1, rolledBack)
		got, err = s.Get(ref)
		require.NoError(t, err)
		require.Equal(t, full, got)
		closure, err = s.GetClosure(ref)
		require.NoError(t, err)
		require.Equal(t, []byte("current closure"), closure)
	})
}

func legacyRecordKey(part byte, ref leiosdb.Ref, kind byte) []byte {
	key := []byte{part}
	key = binary.BigEndian.AppendUint64(key, ref.Slot)
	key = append(key, ref.Hash...)
	return append(key, kind)
}

func TestStorePinDoesNotRewriteStablePayload(t *testing.T) {
	t.Parallel()
	inner, err := leiosdb.Open(leiosdb.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner.Close() })
	counting := &syncCountingStore{BlobStore: inner.BlobStore()}
	s := leiosdb.Wrap(counting)
	ref := eb(10, 1)
	large, err := cbor.Encode(bytes.Repeat([]byte{0x42}, 512))
	require.NoError(t, err)
	require.NoError(t, s.PutVolatile(ref, leiosdb.Content{
		Manifest: bytes.Repeat([]byte{0x24}, 512),
		Txs:      []cbor.RawMessage{large},
	}))
	counting.writes.Store(0)
	counting.valueBytes.Store(0)

	res, err := s.Pin(ref, eb(11, 9))
	require.NoError(t, err)
	require.False(t, res.ContentPending)
	require.Less(t, counting.valueBytes.Load(), int64(128))
}

func TestStoreTransactionBackingReusesTransactionRecord(t *testing.T) {
	t.Parallel()
	inner, err := leiosdb.Open(leiosdb.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner.Close() })
	counting := &syncCountingStore{BlobStore: inner.BlobStore()}
	s := leiosdb.Wrap(counting)
	ref := eb(10, 1)
	raw, err := cbor.Encode(bytes.Repeat([]byte{0x42}, 512))
	require.NoError(t, err)
	content := leiosdb.Content{
		Manifest: []byte("manifest"),
		Txs:      []cbor.RawMessage{raw},
	}
	require.NoError(t, s.PutVolatile(ref, content))
	backing, err := cbor.Encode(content.Txs)
	require.NoError(t, err)
	counting.writes.Store(0)
	counting.valueBytes.Store(0)

	require.NoError(t, s.PutTransactionBacking(ref, backing))
	require.Less(t, counting.valueBytes.Load(), int64(len(backing)))
	got, err := s.GetClosure(ref)
	require.NoError(t, err)
	require.Equal(t, backing, got)
}

func TestStoreGarbageCollectIsCappedAtCurrentPinWatermark(t *testing.T) {
	t.Parallel()
	forEachStore(t, func(t *testing.T, f storeFactory) {
		s := f.open(t, t.TempDir(), nil)
		kept, above := eb(10, 1), eb(20, 2)
		require.NoError(t, s.PutVolatile(kept, content("a", "tx")))
		require.NoError(t, s.PutVolatile(above, content("b", "tx")))
		require.NoError(t, s.AdvancePinnedThrough(eb(40, 8)))

		// A rollback lowers the watermark after the caller read it.
		_, err := s.RollbackPins(eb(15, 7))
		require.NoError(t, err)
		_, err = s.GarbageCollect(context.Background(), 100)
		require.NoError(t, err)

		_, err = s.Get(above)
		require.NoError(t, err, "a record above the pin watermark is kept")
		wm, err := s.Watermarks()
		require.NoError(t, err)
		require.Equal(t, uint64(15), wm.GCBefore)
		_, err = s.Get(kept)
		require.ErrorIs(
			t,
			err,
			leiosdb.ErrNotFound,
			"a record below it is still collected",
		)
	})
}
