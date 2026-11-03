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
	"context"
	"errors"
	"testing"
)

func TestGetContextLimited(t *testing.T) {
	t.Parallel()
	store, err := New()
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	key := []byte("bounded-read")
	want := []byte("value")
	writeTxn := store.NewTransaction(true)
	if err := store.Set(writeTxn, key, want); err != nil {
		t.Fatalf("set value: %v", err)
	}
	if err := writeTxn.Commit(); err != nil {
		t.Fatalf("commit value: %v", err)
	}

	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	got, err := store.GetContextLimited(
		context.Background(), txn, key, int64(len(want)),
	)
	if err != nil {
		t.Fatalf("read at limit: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("read at limit = %q, want %q", got, want)
	}

	if _, err := store.GetContextLimited(
		context.Background(), txn, key, int64(len(want)-1),
	); err == nil {
		t.Fatal("oversized value was accepted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.GetContextLimited(ctx, txn, key, int64(len(want))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error = %v, want context.Canceled", err)
	}

	got, err = store.Get(txn, key)
	if err != nil {
		t.Fatalf("generic read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("generic read = %q, want %q", got, want)
	}
}
