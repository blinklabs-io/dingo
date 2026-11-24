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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

func TestLiveUtxoDigestSelectsTheSetAtASlotAndPagesWithoutLosingRows(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	txs := make([]int64, 3)
	for i := range txs {
		txs[i] = fixtureIdentityRow(
			t,
			store,
			0,
			bytes.Repeat([]byte{byte(i + 1)}, 32),
		)
	}
	insert := func(tx int64, idx int, added, deleted int64, amount string) {
		t.Helper()
		_, err := store.writeDB.ExecContext(ctx, store.dialect.Rebind(`
INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, ?, ?, ?)`), tx, idx, added, deleted, amount)
		require.NoError(t, err)
	}
	// Live at slot 50: (0,0) (0,1) (0,2) (1,0) and (2,0) spent later; not
	// live: (1,1) created after, (2,1) spent at or before.
	insert(txs[0], 0, 10, 0, "1")
	insert(txs[0], 1, 10, 0, "2")
	insert(txs[0], 2, 10, 0, "3")
	insert(txs[1], 0, 20, 0, "4")
	insert(txs[1], 1, 51, 0, "5")
	insert(txs[2], 0, 30, 90, "6")
	insert(txs[2], 1, 30, 50, "7")

	want, err := store.liveUtxoDigest(ctx, 50, nil, nil, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(5), want.Count)
	for _, page := range []int{1, 2, 3, 5, 6} {
		got, err := store.liveUtxoDigest(ctx, 50, nil, nil, page)
		require.NoError(t, err)
		require.Equal(t, want, got, "page size %d", page)
	}

	later, err := store.LiveUtxoDigestWithOutputs(ctx, 51, nil, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(6),
		later.Count,
		"the output created at 51 is live at 51",
	)
	require.NotEqual(t, want.Digest, later.Digest)

	insert(txs[0], 3, 10, 0, "8")
	changed, err := store.LiveUtxoDigestWithOutputs(ctx, 50, nil, nil)
	require.NoError(t, err)
	require.NotEqual(
		t,
		want.Digest,
		changed.Digest,
		"an added output changes the digest",
	)
}

func TestLiveUtxoDigestCoversResolvedOutputBytes(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	tx := fixtureIdentityRow(t, store, 0, bytes.Repeat([]byte{1}, 32))
	_, err := store.writeDB.ExecContext(ctx, store.dialect.Rebind(`
INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, 0, 10, 0, '1')`), tx)
	require.NoError(t, err)

	resolver := func(out []byte) metadata.UtxoOutputResolver {
		return func([]byte, uint32) ([]byte, error) { return out, nil }
	}
	base, err := store.LiveUtxoDigestWithOutputs(ctx, 50, nil, nil)
	require.NoError(t, err)
	first, err := store.LiveUtxoDigestWithOutputs(
		ctx, 50, resolver([]byte{0x82, 1}), nil,
	)
	require.NoError(t, err)
	again, err := store.LiveUtxoDigestWithOutputs(
		ctx, 50, resolver([]byte{0x82, 1}), nil,
	)
	require.NoError(t, err)
	other, err := store.LiveUtxoDigestWithOutputs(
		ctx, 50, resolver([]byte{0x82, 2}), nil,
	)
	require.NoError(t, err)

	require.Equal(t, first, again)
	require.NotEqual(t, base.Digest, first.Digest,
		"the output bytes enter the digest")
	require.NotEqual(t, first.Digest, other.Digest,
		"an output differing only in its bytes digests differently")
	require.Equal(t, base.Count, first.Count)

	_, err = store.LiveUtxoDigestWithOutputs(
		ctx, 50,
		func([]byte, uint32) ([]byte, error) {
			return nil, errors.New("no bytes")
		}, nil,
	)
	require.ErrorContains(t, err, "no bytes")
}
