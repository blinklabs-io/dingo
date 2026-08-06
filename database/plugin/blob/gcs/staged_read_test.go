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

package gcs

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

// resolveKey is the single read path shared by Get and every typed getter. A
// value staged by this transaction has to win over the bucket, otherwise a
// read-after-write inside one transaction returns pre-transaction state and the
// plugin diverges from badger. The bucket is never consulted for a staged key,
// so a nil client is proof the read was served from the staging map.
func TestResolveKeyServesStagedWrite(t *testing.T) {
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
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
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
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
	store := &BlobStoreGCS{}
	txn := &gcsTxn{store: store, pending: make(map[string]gcsPendingChange)}
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
	st := &BlobStoreGCS{}
	txn := &gcsTxn{store: st, pending: make(map[string]gcsPendingChange)}
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
// only in Commit: Rollback discards the staged work without issuing any GCS
// request. It reported true when Set/Delete wrote through immediately, and
// database/lifecycle/blob_bulk_delete.go reads this flag to decide whether a
// failed batch's deletes are permanent — reporting true made a truncate count
// blocks it never removed.
func TestRollbackIsNoopReportsFalse(t *testing.T) {
	txn := &gcsTxn{pending: make(map[string]gcsPendingChange)}
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
