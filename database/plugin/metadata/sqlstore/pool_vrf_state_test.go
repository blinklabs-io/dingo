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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestGetPoolVrfStatePreservesRegistrationOrderWithoutAssociations(
	t *testing.T,
) {
	t.Parallel()
	store, recorder := newRecordingSQLiteStore(t)
	pkh := lcommon.PoolKeyHash{0x61}
	rowHash := bytes.Repeat([]byte{0x11}, 32)
	_, err := store.writeDB.Exec(
		`INSERT INTO pool (id,pool_key_hash,vrf_key_hash) VALUES (1,?,?)`,
		pkh.Bytes(),
		rowHash,
	)
	require.NoError(t, err)
	for _, row := range []struct{ id, slot, block, cert, registration int }{
		{50, 100, 1, 1, 1}, {20, 100, 0, 0, 0}, {60, 100, 1, 1, 1},
		{10, 99, 99, 99, 99}, {40, 100, 1, 1, 0}, {30, 100, 1, 0, 0},
	} {
		_, err = store.writeDB.Exec(
			`INSERT INTO "transaction" (id,hash,slot,block_index) VALUES (?,?,?,?)`,
			row.id,
			bytes.Repeat([]byte{byte(row.id)}, 32),
			row.slot,
			row.block,
		)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(
			`INSERT INTO certs (id,transaction_id,slot,cert_index) VALUES (?,?,?,?)`,
			row.id,
			row.id,
			row.slot,
			row.cert,
		)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(
			`INSERT INTO pool_registration (id,pool_id,pool_key_hash,certificate_id,added_slot,registration_index,vrf_key_hash) VALUES (?,1,?,?,?,?,?)`,
			row.id,
			pkh.Bytes(),
			row.id,
			row.slot,
			row.registration,
			bytes.Repeat([]byte{byte(row.id)}, 32),
		)
		require.NoError(t, err)
	}
	_, err = store.writeDB.Exec(
		`INSERT INTO pool_registration_owner (key_hash,pool_registration_id,pool_id) VALUES (?,60,1)`,
		bytes.Repeat([]byte{0x41}, 28),
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO pool_registration_relay (relay_type,hostname,pool_registration_id,pool_id,port) VALUES (1,'relay.example',60,1,3001)`,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO pool_retirement (pool_id,pool_key_hash,epoch,added_slot) VALUES (1,?,1,100)`,
		pkh.Bytes(),
	)
	require.NoError(t, err)
	for _, id := range []int{60, 50, 40, 30, 20, 10} {
		full, err := store.GetPool(pkh, true, nil)
		require.NoError(t, err)
		require.NotNil(t, full)
		require.Len(t, full.Registration, 1)
		require.Equal(
			t,
			bytes.Repeat([]byte{byte(id)}, 32),
			full.Registration[0].VrfKeyHash,
		)
		if id == 60 {
			require.Len(t, full.Registration[0].Owners, 1)
			require.Len(t, full.Registration[0].Relays, 1)
			require.Len(t, full.Retirement, 1)
		}
		before := len(recorder.snapshot())
		projected, err := store.GetPoolVrfState(pkh, nil)
		require.NoError(t, err)
		require.NotNil(t, projected)
		require.Equal(t, full.PoolKeyHash, projected.PoolKeyHash)
		require.Equal(t, full.VrfKeyHash, projected.VrfKeyHash)
		require.Len(t, projected.Registration, 1)
		require.Equal(
			t,
			full.Registration[0].VrfKeyHash,
			projected.Registration[0].VrfKeyHash,
		)
		statements := recorder.snapshot()[before:]
		require.Len(
			t,
			statements,
			1,
			"current VRF resolution must not hydrate pool associations",
		)
		require.Contains(t, statements[0], "-- name: GetPoolVrfState :one")
		_, err = store.writeDB.Exec(
			`DELETE FROM pool_registration WHERE id=?`,
			id,
		)
		require.NoError(t, err)
	}
	pool, err := store.GetPoolVrfState(pkh, nil)
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.Empty(
		t,
		pool.Registration,
		"no registration must not become a fallback-key registration",
	)
	require.Equal(t, rowHash, pool.VrfKeyHash)
	missing, err := store.GetPoolVrfState(lcommon.PoolKeyHash{0xff}, nil)
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestGetPoolVrfStatePreservesHashBytesAndCallerTransaction(t *testing.T) {
	t.Parallel()
	store, _ := newRecordingSQLiteStore(t)
	pkh := lcommon.PoolKeyHash{0x62}
	_, err := store.writeDB.Exec(
		`INSERT INTO pool (id,pool_key_hash) VALUES (1,?)`,
		pkh.Bytes(),
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		`INSERT INTO pool_registration (id,pool_id,pool_key_hash,added_slot) VALUES (1,1,?,10)`,
		pkh.Bytes(),
	)
	require.NoError(t, err)
	for _, hashes := range []struct{ row, registration []byte }{
		{bytes.Repeat([]byte{1}, 32), nil},
		{bytes.Repeat([]byte{2}, 32), []byte{}},
		{bytes.Repeat([]byte{3}, 32), []byte{4}},
		{[]byte{5}, nil},
		{nil, nil},
	} {
		_, err = store.writeDB.Exec(
			`UPDATE pool SET vrf_key_hash=? WHERE id=1`,
			hashes.row,
		)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(
			`UPDATE pool_registration SET vrf_key_hash=? WHERE id=1`,
			hashes.registration,
		)
		require.NoError(t, err)
		projected, err := store.GetPoolVrfState(pkh, nil)
		require.NoError(t, err)
		full, err := store.GetPool(pkh, true, nil)
		require.NoError(t, err)
		require.Equal(t, full.VrfKeyHash, projected.VrfKeyHash)
		require.Len(t, projected.Registration, 1)
		require.Equal(
			t,
			full.Registration[0].VrfKeyHash,
			projected.Registration[0].VrfKeyHash,
		)
	}
	txn := store.Transaction(t.Context())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	pendingHash := bytes.Repeat([]byte{0x71}, 32)
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO pool_registration (pool_id,pool_key_hash,added_slot,vrf_key_hash) VALUES (1,?,20,?)`,
		pkh.Bytes(),
		pendingHash,
	)
	require.NoError(t, err)
	projected, err := store.GetPoolVrfState(pkh, txn)
	require.NoError(t, err)
	require.Equal(t, pendingHash, projected.Registration[0].VrfKeyHash)
	full, err := store.GetPool(pkh, true, txn)
	require.NoError(t, err)
	require.Equal(
		t,
		full.Registration[0].VrfKeyHash,
		projected.Registration[0].VrfKeyHash,
	)
	require.NoError(t, txn.Rollback())
	projected, err = store.GetPoolVrfState(pkh, nil)
	require.NoError(t, err)
	require.Empty(t, projected.Registration[0].VrfKeyHash)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	read := store.ReadTransaction(canceled)
	defer read.Rollback()
	_, err = store.GetPoolVrfState(pkh, read)
	require.ErrorIs(t, err, context.Canceled)
}
