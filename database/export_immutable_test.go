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

import "github.com/blinklabs-io/dingo/database/plugin/blob"

// SetImmutablePromoteFault installs fn on db's ImmutableDB, which calls it at
// the "append", "sync", "commit" and "state" steps of every promotion batch. A
// non-nil result aborts the batch at that step.
func SetImmutablePromoteFault(db *Database, fn func(step string) error) {
	db.immutable.fault = fn
}

// ImmutableStoreOpen reports whether db holds an open chunk writer.
func ImmutableStoreOpen(db *Database) bool {
	rt := db.immutable
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.store != nil
}

// RawBlobStoreForTest exposes rawBlobStore.
func RawBlobStoreForTest(store blob.BlobStore) blob.BlobStore {
	return rawBlobStore(store)
}

// TombstoneBlockForTest runs the history-expiry tombstone path on one block.
func TombstoneBlockForTest(db *Database, slot uint64, hash []byte) error {
	txn := db.BlockBlobTxn()
	defer txn.Rollback() //nolint:errcheck
	if err := db.tombstoneBlockTxn(txn, slot, hash); err != nil {
		return err
	}
	return txn.Commit()
}
