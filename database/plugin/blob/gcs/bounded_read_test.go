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

package gcs

import (
	"context"
	"errors"
	"testing"
)

func TestGetContextLimited(t *testing.T) {
	t.Parallel()
	store, objects := storeOnFakeCloud(t)
	key := []byte("bounded-read")
	want := []byte("value")
	objects.Put(fakeBucket, store.fullKey(string(key)), want)

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
