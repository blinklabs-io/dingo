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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// unsyncedBlobStore drops the durability barrier so the benchmarks below can
// price it against an otherwise identical on-disk store.
type unsyncedBlobStore struct {
	blob.BlobStore
}

func (unsyncedBlobStore) Sync() error { return nil }

func benchCombinedCommitDB(b *testing.B, sync bool) *Database {
	b.Helper()
	db, err := newTestDatabase(b, &Config{
		DataDir: b.TempDir(),
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatalf("new test database: %v", err)
	}
	if !sync {
		db.SetBlobStore(unsyncedBlobStore{db.Blob()})
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Fatalf("close database: %v", err)
		}
	})
	return db
}

// benchmarkCombinedCommit measures a blob+metadata commit carrying a
// mainnet-sized block body plus a tip advance, which is the shape of every
// applied block. Compare WithSync against WithoutSync to price the cross-store
// durability barrier: SQLite runs synchronous=NORMAL and so does not fsync per
// commit, making the Badger sync the only fsync on this path.
func benchmarkCombinedCommit(b *testing.B, sync bool) {
	db := benchCombinedCommitDB(b, sync)
	blobStore := db.Blob()
	// Roughly a mainnet block body, so the sync has representative dirty data
	// to flush rather than a bare tip row.
	body := bytes.Repeat([]byte{0x5a}, 16*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		slot := uint64(i) + 1
		hash := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 16)
		txn := db.Transaction(true)
		if err := blobStore.SetBlock(
			txn.Blob(), slot, hash, body, slot, 6, slot, hash,
		); err != nil {
			b.Fatalf("set block: %v", err)
		}
		if err := db.SetTip(ochainsync.Tip{
			Point:       ocommon.Point{Slot: slot, Hash: hash},
			BlockNumber: slot,
		}, txn); err != nil {
			b.Fatalf("set tip: %v", err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkCombinedCommitWithSync(b *testing.B) {
	benchmarkCombinedCommit(b, true)
}

func BenchmarkCombinedCommitWithoutSync(b *testing.B) {
	benchmarkCombinedCommit(b, false)
}

// BenchmarkBlobSync isolates the barrier itself on an on-disk Badger store.
func BenchmarkBlobSync(b *testing.B) {
	db := benchCombinedCommitDB(b, true)
	blobStore := db.Blob()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := blobStore.Sync(); err != nil {
			b.Fatalf("sync: %v", err)
		}
	}
}
