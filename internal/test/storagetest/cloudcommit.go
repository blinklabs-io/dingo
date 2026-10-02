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

package storagetest

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/fakecloud"
	"github.com/stretchr/testify/require"
)

// failMutation arms fc so that the n-th mutation (0-based) fails. With
// afterApply the mutation takes effect and the response is lost, which is how
// an object store reports an operation whose outcome the caller cannot know;
// otherwise it is rejected unapplied. With rejectRest every later mutation,
// which includes the commit's own compensation, is rejected unapplied.
func failMutation(
	fc *fakecloud.Store,
	n int32,
	afterApply, rejectRest bool,
) {
	var seen atomic.Int32
	fc.SetHooks(fakecloud.Hooks{
		Before: func(op fakecloud.Op) error {
			if !op.Mutates {
				return nil
			}
			idx := seen.Add(1) - 1
			if (!afterApply && idx == n) || (rejectRest && idx > n) {
				return fakecloud.ErrInjected
			}
			return nil
		},
		After: func(op fakecloud.Op) error {
			// seen already counted this mutation in Before.
			if afterApply && seen.Load()-1 == n {
				return fakecloud.ErrInjected
			}
			return nil
		},
	})
}

func readKey(t *testing.T, store blob.BlobStore, key []byte) ([]byte, bool) {
	t.Helper()
	txn := store.NewTransaction(false)
	defer func() { require.NoError(t, txn.Rollback()) }()
	val, err := store.Get(txn, key)
	if errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, false
	}
	require.NoError(t, err)
	return val, true
}

// RunCloudCommitUncertainOutcome checks a cloud store's Commit when one of its
// object mutations fails. The failed mutation may have taken effect, so it
// must be compensated like the ones before it, and a commit whose final remote
// state cannot be proven must report types.ErrPartialCommit.
//
// store must be backed by fc, which must start empty.
func RunCloudCommitUncertainOutcome(
	t *testing.T,
	fc *fakecloud.Store,
	store blob.BlobStore,
) {
	t.Helper()
	keys := [][]byte{[]byte("k1-existing"), []byte("k2-new"), []byte("k3-new")}
	seed := store.NewTransaction(true)
	require.NoError(t, store.Set(seed, keys[0], []byte("old")))
	require.NoError(t, seed.Commit())

	commit := func() error {
		txn := store.NewTransaction(true)
		for _, k := range keys {
			require.NoError(t, store.Set(txn, k, []byte("new")))
		}
		return txn.Commit()
	}
	requireRestored := func(t *testing.T) {
		t.Helper()
		got, ok := readKey(t, store, keys[0])
		require.True(t, ok)
		require.Equal(t, []byte("old"), got, "overwritten object not restored")
		for _, k := range keys[1:] {
			_, ok := readKey(t, store, k)
			require.False(t, ok, "object %q written by the failed commit remains", k)
		}
	}
	t.Cleanup(func() { fc.SetHooks(fakecloud.Hooks{}) })

	for n := range int32(len(keys)) {
		t.Run(fmt.Sprintf("applied-then-error-mutation-%d", n), func(t *testing.T) {
			failMutation(fc, n, true, false)
			err := commit()
			require.Error(t, err)
			require.NotErrorIs(t, err, types.ErrPartialCommit)
			fc.SetHooks(fakecloud.Hooks{})
			requireRestored(t)
		})
	}
	for _, n := range []int32{0, int32(len(keys)) - 1} {
		t.Run(fmt.Sprintf("uncompensable-mutation-%d", n), func(t *testing.T) {
			// The failed mutation took effect and compensation is rejected,
			// so the remote state is unknown.
			failMutation(fc, n, true, true)
			err := commit()
			fc.SetHooks(fakecloud.Hooks{})
			require.ErrorIs(t, err, types.ErrPartialCommit)

			// Leave a clean slate for later subtests.
			for _, k := range keys[1:] {
				cleanup := store.NewTransaction(true)
				if err := store.Delete(cleanup, k); err == nil {
					require.NoError(t, cleanup.Commit())
				} else {
					require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
					require.NoError(t, cleanup.Rollback())
				}
			}
			reset := store.NewTransaction(true)
			require.NoError(t, store.Set(reset, keys[0], []byte("old")))
			require.NoError(t, reset.Commit())
		})
	}
}

// RunCloudPruneCommitVisibility checks that a commit which materializes UTxOs
// and tombstones their block never exposes the tombstone before the UTxOs. A
// reader (a point read and an iterator, which is what a snapshot walks) looks
// at the store after every object mutation, and again after a commit that fails
// at any mutation, with and without a working compensation.
//
// store must be backed by fc, which must start empty.
func RunCloudPruneCommitVisibility(
	t *testing.T,
	fc *fakecloud.Store,
	store blob.BlobStore,
) {
	t.Helper()
	const (
		slot     = 42
		utxoRefs = 3
	)
	hash := make([]byte, 32)
	hash[0] = 0x01
	blockKey := types.BlockBlobKey(slot, hash)
	offsetForm, materialized := []byte("offset-ref"), []byte("raw-cbor")
	utxoKey := func(i int) []byte {
		return types.UtxoBlobKey([]byte{byte(i), 0x0a}, uint32(i))
	}

	// requireConsistent fails when the block reads as expired while any UTxO
	// still holds its offset reference into it.
	requireConsistent := func(t *testing.T, when string) {
		t.Helper()
		block, ok := readKey(t, store, blockKey)
		require.True(t, ok)
		if !types.IsBlockTombstone(block) {
			return
		}
		snapshot := map[string][]byte{}
		txn := store.NewTransaction(false)
		it := store.NewIterator(txn, types.BlobIteratorOptions{
			Prefix: []byte(types.UtxoBlobKeyPrefix),
		})
		for it.Rewind(); it.ValidForPrefix([]byte(types.UtxoBlobKeyPrefix)); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			require.NoError(t, err)
			snapshot[string(it.Item().Key())] = val
		}
		require.NoError(t, it.Err())
		it.Close()
		require.NoError(t, txn.Rollback())
		require.Len(t, snapshot, utxoRefs)
		for i := range utxoRefs {
			got, ok := readKey(t, store, utxoKey(i))
			require.True(t, ok)
			require.Equalf(t, materialized, got,
				"%s: block is expired but UTxO %d still references it (point read)", when, i)
			require.Equalf(t, materialized, snapshot[string(utxoKey(i))],
				"%s: block is expired but UTxO %d still references it (snapshot)", when, i)
		}
	}

	seed := func(t *testing.T) {
		t.Helper()
		fc.SetHooks(fakecloud.Hooks{})
		txn := store.NewTransaction(true)
		require.NoError(t, store.SetBlock(txn, slot, hash, []byte("block"), 1, 1, 1, nil))
		for i := range utxoRefs {
			require.NoError(t, store.SetUtxo(txn, []byte{byte(i), 0x0a}, uint32(i), offsetForm))
		}
		require.NoError(t, txn.Commit())
	}
	prune := func() error {
		txn := store.NewTransaction(true)
		for i := range utxoRefs {
			if err := store.SetUtxo(txn, []byte{byte(i), 0x0a}, uint32(i), materialized); err != nil {
				return err
			}
		}
		if err := store.TombstoneBlock(txn, slot, hash); err != nil {
			return err
		}
		return txn.Commit()
	}
	t.Cleanup(func() { fc.SetHooks(fakecloud.Hooks{}) })

	t.Run("reader-between-mutations", func(t *testing.T) {
		seed(t)
		fc.SetHooks(fakecloud.Hooks{
			After: func(op fakecloud.Op) error {
				requireConsistent(t, "after "+op.Key)
				return nil
			},
		})
		require.NoError(t, prune())
		fc.SetHooks(fakecloud.Hooks{})
		block, _ := readKey(t, store, blockKey)
		require.True(t, types.IsBlockTombstone(block))
	})

	// utxoRefs UTxO writes plus the tombstone.
	for n := range int32(utxoRefs + 1) {
		for _, rejectRest := range []bool{false, true} {
			t.Run(fmt.Sprintf("failure-at-%d-compensation-rejected=%t", n, rejectRest), func(t *testing.T) {
				seed(t)
				failMutation(fc, n, true, rejectRest)
				err := prune()
				fc.SetHooks(fakecloud.Hooks{})
				require.Error(t, err)
				requireConsistent(t, "after failed commit")
			})
		}
	}
}
