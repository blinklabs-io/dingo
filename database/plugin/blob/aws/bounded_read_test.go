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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
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
	); !errors.Is(err, types.ErrBlobTooLarge) {
		t.Fatalf("oversized read error = %v, want ErrBlobTooLarge", err)
	}

	// A value staged in the transaction is bounded the same way.
	write := store.NewTransaction(true)
	defer write.Rollback() //nolint:errcheck
	if err := store.Set(write, []byte("staged-read"), want); err != nil {
		t.Fatalf("stage value: %v", err)
	}
	if _, err := store.GetContextLimited(
		context.Background(), write, []byte("staged-read"),
		int64(len(want)-1),
	); !errors.Is(err, types.ErrBlobTooLarge) {
		t.Fatalf("oversized staged read error = %v, want ErrBlobTooLarge", err)
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
