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

package sqlstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func delegatorPool(seed byte) lcommon.PoolKeyHash {
	return lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{seed}, 28)),
	)
}

func seedDelegator(
	t *testing.T,
	store *Store,
	seed byte,
	tag uint8,
	pool []byte,
	active bool,
) {
	t.Helper()
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    bytes.Repeat([]byte{seed}, 28),
		CredentialTag: tag,
		Pool:          pool,
		Active:        active,
		AddedSlot:     2,
	}, nil))
}

// The count covers only active accounts delegated to a requested pool: an
// inactive account, an account with no pool and an account of an unrequested
// pool are all left out.
func TestGetActiveDelegatorCountsByPool(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	a, b, c, none := delegatorPool(0x11), delegatorPool(0x12), delegatorPool(0x13),
		delegatorPool(0x14)
	seedDelegator(t, store, 0x61, 0, a.Bytes(), true)
	seedDelegator(t, store, 0x62, 1, a.Bytes(), true)
	seedDelegator(t, store, 0x63, 0, a.Bytes(), false)
	seedDelegator(t, store, 0x64, 0, b.Bytes(), true)
	seedDelegator(t, store, 0x65, 0, c.Bytes(), true)
	seedDelegator(t, store, 0x66, 0, nil, true)

	got, err := store.GetActiveDelegatorCountsByPool(
		[]lcommon.PoolKeyHash{a, b, none},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(a.Bytes()): 2,
		string(b.Bytes()): 1,
	}, got)

	empty, err := store.GetActiveDelegatorCountsByPool(nil, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}

// A request larger than one statement's chunk is answered from every chunk.
func TestGetActiveDelegatorCountsByPoolSpansChunks(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	pools := make([]lcommon.PoolKeyHash, 0, activeDelegatorChunkSize*2+50)
	for i := range activeDelegatorChunkSize*2 + 50 {
		var hash lcommon.Blake2b224
		hash[0] = byte(i >> 8)
		hash[1] = byte(i)
		pools = append(pools, lcommon.PoolKeyHash(hash))
	}
	first, last := pools[0], pools[len(pools)-1]
	seedDelegator(t, store, 0x61, 0, first.Bytes(), true)
	seedDelegator(t, store, 0x62, 0, last.Bytes(), true)
	seedDelegator(t, store, 0x63, 0, last.Bytes(), true)

	got, err := store.GetActiveDelegatorCountsByPool(pools, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{
		string(first.Bytes()): 1,
		string(last.Bytes()):  2,
	}, got)
}

// The credentials come back per requested pool in the ledger's order, script
// credentials before key credentials and then by hash.
func TestGetActiveDelegatorCredentialsByPool(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	a, b, none := delegatorPool(0x11), delegatorPool(0x12), delegatorPool(0x14)
	seedDelegator(t, store, 0x62, 0, a.Bytes(), true)
	seedDelegator(t, store, 0x61, 1, a.Bytes(), true)
	seedDelegator(t, store, 0x60, 0, a.Bytes(), true)
	seedDelegator(t, store, 0x70, 1, a.Bytes(), true)
	seedDelegator(t, store, 0x63, 0, a.Bytes(), false)
	seedDelegator(t, store, 0x64, 0, b.Bytes(), true)
	seedDelegator(t, store, 0x65, 0, nil, true)

	got, err := store.GetActiveDelegatorCredentialsByPool(
		[]lcommon.PoolKeyHash{a, none},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1, "only the requested pool with delegators")
	want := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, bytes.Repeat([]byte{0x61}, 28)),
		models.NewStakeCredentialRef(1, bytes.Repeat([]byte{0x70}, 28)),
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x60}, 28)),
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x62}, 28)),
	}
	require.Equal(t, want, got[string(a.Bytes())])
}

// A cancelled transaction context stops the read instead of walking on.
func TestGetActiveDelegatorQueriesHonourContext(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)
	pool := delegatorPool(0x11)
	seedDelegator(t, store, 0x61, 0, pool.Bytes(), true)

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.ReadTransaction(ctx)
	defer txn.Rollback() //nolint:errcheck
	cancel()

	_, err := store.GetActiveDelegatorCountsByPool(
		[]lcommon.PoolKeyHash{pool}, txn,
	)
	require.ErrorIs(t, err, context.Canceled)
	_, err = store.GetActiveDelegatorCredentialsByPool(
		[]lcommon.PoolKeyHash{pool}, txn,
	)
	require.ErrorIs(t, err, context.Canceled)
}
