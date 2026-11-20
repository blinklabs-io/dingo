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
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// reclaimSentinelPrefix is a key prefix no blob key uses. DropPrefix returns
// early when nothing carries the prefix, so a sentinel under it is written
// first to make the call run its memtable flush.
const reclaimSentinelPrefix = "\xff\xffdingo-reclaim"

var _ interface{ ReclaimSpace() error } = (*BlobStoreBadger)(nil)

// ReclaimSpace flushes the memtable and compacts the LSM tree so superseded
// versions committed before the call are dropped.
//
// Block bodies are far below the value threshold, so they live in the LSM
// tree and the value log holds nothing for GC to rewrite. A body replaced by
// a marker stays on disk until a compaction covers both versions. That needs
// the marker flushed out of the memtable, which holds every write until it
// fills (128 MiB by default), and then five L0 tables or a level over its
// size target; a node at the chain tip writes a few MiB an hour, so neither
// happens for weeks.
//
// badger exposes no memtable flush. DropPrefix flushes it, but fails any
// commit that arrives meanwhile, so commits are held back around the call.
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
	if err := db.Flatten(1); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}
