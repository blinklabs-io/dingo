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
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// reclaimSentinelPrefix is a key prefix no blob key uses. DropPrefix returns
// early when nothing carries the prefix, so a sentinel under it is written
// first to make the call run its memtable flush.
const reclaimSentinelPrefix = "\xff\xffdingo-reclaim"

// flattenShare sets when ReclaimSpace rewrites the whole tree: only once the
// superseded versions a memtable flush left behind make up at least
// 1/flattenShare of the LSM tree, so each rewrite frees a share of what it
// copies.
const flattenShare = 4

var _ interface{ ReclaimSpace() error } = (*BlobStoreBadger)(nil)

// ReclaimSpace flushes the memtable so superseded versions committed before
// the call can be dropped, and rewrites the whole tree only when what is left
// is a large share of it.
//
// Block bodies are far below the value threshold, so they live in the LSM
// tree and the value log holds nothing for GC to rewrite. A body replaced by
// a marker stays on disk until a compaction covers both versions. That needs
// the marker flushed out of the memtable, which holds every write until it
// fills (128 MiB by default), and then five L0 tables or a level over its
// size target; a node at the chain tip writes a few MiB an hour, so neither
// happens for weeks.
//
// badger exposes no memtable flush. DropPrefix flushes it and compacts L0 into
// the base level, but fails any commit that arrives meanwhile, so commits are
// held back around the call. At the chain tip the replaced bodies are recent
// and still in the levels that compaction covers, so it drops them. A body
// already in a lower level is dropped when badger's own leveled compaction
// carries its marker down.
//
// Flatten instead moves every level into the bottom one. A table in a sparse
// upper level spans most of the key space, so each Flatten step rewrites
// most of the bottom level and then swaps its tables under the level's write
// lock, which every read of the store waits on. It therefore runs only when
// the superseded versions written since the last reclaim, as left after the
// flush, are at least 1/flattenShare of the tree: a whole-history promotion,
// not the few blocks promoted at the tip.
func (d *BlobStoreBadger) ReclaimSpace() error {
	db := d.DB()
	if db == nil || d.dataDir == "" {
		return nil
	}
	sentinel := []byte(reclaimSentinelPrefix)
	d.commitGate.Lock()
	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(sentinel, []byte{1})
	})
	if err == nil {
		err = db.DropPrefix(sentinel)
	}
	d.commitGate.Unlock()
	if err != nil {
		return fmt.Errorf("flush memtable: %w", err)
	}
	since := d.reclaimSince.Load()
	superseded, lsm, readTs, err := supersededSince(db, since)
	if err != nil {
		return fmt.Errorf("measure superseded versions: %w", err)
	}
	if superseded > 0 && superseded*flattenShare >= lsm {
		if err := db.Flatten(1); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
	}
	d.reclaimSince.Store(readTs)
	return nil
}

// supersededSince sums the bytes of the older versions of every key written
// after since, and returns them with the uncompressed size of the LSM tree
// and the read timestamp the sum was taken at. It stops early once the sum
// reaches the share that makes a Flatten worthwhile.
func supersededSince(
	db *badger.DB,
	since uint64,
) (superseded, lsm int64, readTs uint64, err error) {
	// Value sizes are uncompressed, so the tree is measured the same way.
	for _, ti := range db.Tables() {
		lsm += int64(ti.UncompressedSize)
	}
	err = db.View(func(txn *badger.Txn) error {
		readTs = txn.ReadTs()
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		opts.PrefetchValues = false
		opts.SinceTs = since
		it := txn.NewIterator(opts)
		defer it.Close()
		var key []byte
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if key != nil && bytes.Equal(item.Key(), key) {
				continue
			}
			key = item.KeyCopy(key[:0])
			superseded += olderVersionBytes(txn, key)
			if superseded > 0 && superseded*flattenShare >= lsm {
				return nil
			}
		}
		return nil
	})
	return superseded, lsm, readTs, err
}

// olderVersionBytes sums the value sizes of every version of key but the
// newest.
func olderVersionBytes(txn *badger.Txn, key []byte) int64 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewKeyIterator(key, opts)
	defer it.Close()
	var total int64
	newest := true
	for it.Rewind(); it.Valid(); it.Next() {
		if newest {
			newest = false
			continue
		}
		total += it.Item().ValueSize()
	}
	return total
}
