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
	"errors"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// bodyStore resolves payload offsets from the block bodies it holds, and
// fails for a block whose body was released.
type bodyStore struct {
	mu     sync.Mutex
	bodies map[[32]byte][]byte
}

func (b *bodyStore) put(hash []byte, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bodies[[32]byte(hash)] = body
}

func (b *bodyStore) release(hash []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.bodies, [32]byte(hash))
}

func (b *bodyStore) ResolvePayload(encoded []byte) ([]byte, error) {
	offset, err := types.DecodeCborOffset(encoded)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.bodies[offset.BlockHash]
	if !ok {
		return nil, errors.New("block body released")
	}
	return bytes.Clone(
		body[offset.ByteOffset : offset.ByteOffset+offset.ByteLength],
	), nil
}

// A datum or script first carried by a block that a rollback abandons is
// removed with it, so the same transaction applied in the replacement block
// names the replacement block's bytes and stays readable once the abandoned
// block's body is released.
func TestRollbackReplacesDatumAndScriptRowsOfAbandonedBlock(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	bodies := &bodyStore{bodies: map[[32]byte][]byte{}}
	store.SetPayloadResolver(bodies)

	datumRaw := []byte{0x82, 0x01, 0x02}
	var datum lcommon.Datum
	datum.SetCbor(datumRaw)
	script := lcommon.PlutusV1Script{0x41, 0x06}
	txID := bytes.Repeat([]byte{0x31}, 32)
	build := func() lcommon.Transaction {
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(txID)
		tx.WithValid(true)
		tx.WithWitnesses(
			mockledger.NewMockTransactionWitnessSet().
				WithPlutusV1Scripts(script).
				WithPlutusData(datum),
		)
		return tx
	}
	apply := func(slot uint64, hash byte, prefix string) []byte {
		t.Helper()
		blockHash := bytes.Repeat([]byte{hash}, 32)
		body := bytes.Join([][]byte{
			[]byte(prefix), datumRaw, script.RawScriptBytes(),
		}, []byte{'|'})
		locator, err := types.NewBlockPayloadLocator(slot, blockHash, body)
		require.NoError(t, err)
		bodies.put(blockHash, body)
		txn := store.Transaction(t.Context())
		txn.(*sqlTxn).SetPayloadLocator(locator)
		require.NoError(t, store.SetTransaction(
			build(), ocommon.Point{Slot: slot, Hash: blockHash},
			0, nil, true, txn, 0,
		))
		require.NoError(t, txn.Commit())
		return blockHash
	}

	blockA := apply(300, 0xa1, "block a")
	require.NoError(t, store.DeleteTransactionsAfterSlot(299, nil))
	blockB := apply(301, 0xb1, "the replacement block b")
	bodies.release(blockA)

	got, err := store.GetDatum(lcommon.Blake2b256Hash(datumRaw), nil)
	require.NoError(t, err, "the datum names the abandoned block")
	require.Equal(t, datumRaw, got.RawDatum)
	require.EqualValues(t, 301, got.AddedSlot)
	gotScript, err := store.GetScript(script.Hash(), nil)
	require.NoError(t, err, "the script names the abandoned block")
	require.Equal(t, script.RawScriptBytes(), gotScript.Content)
	require.EqualValues(t, 301, gotScript.CreatedSlot)

	held, err := store.BlockHoldsActivePayloads(t.Context(), 301, blockB, 0)
	require.NoError(t, err)
	require.False(t, held, "no live output names the rows")
}

// A datum or script first carried by a block at or below the rollback point
// stays.
func TestRollbackKeepsDatumAndScriptRowsAtOrBelowThePoint(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	bindTestPayloads(t, store)
	datumHash := bytes.Repeat([]byte{0xd1}, 32)
	scriptHash := bytes.Repeat([]byte{0xd2}, 28)
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 50)", []any{datumHash, []byte{0x01}}},
		{"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 50, 1)", []any{scriptHash, []byte{0x02}}},
		{"INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, 51)", []any{bytes.Repeat([]byte{0xd3}, 32), []byte{0x03}}},
		{"INSERT INTO script (hash, content, created_slot, type) VALUES (?, ?, 51, 1)", []any{bytes.Repeat([]byte{0xd4}, 28), []byte{0x04}}},
	} {
		_, err := store.writeDB.Exec(stmt.query, stmt.args...)
		require.NoError(t, err)
	}
	require.NoError(t, store.DeleteTransactionsAfterSlot(50, nil))
	var datums, scripts int
	require.NoError(t, store.writeDB.QueryRow("SELECT COUNT(*) FROM datum").Scan(&datums))
	require.NoError(t, store.writeDB.QueryRow("SELECT COUNT(*) FROM script").Scan(&scripts))
	require.Equal(t, 1, datums)
	require.Equal(t, 1, scripts)
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM datum WHERE hash = ? AND added_slot = 50", datumHash,
	).Scan(&datums))
	require.Equal(t, 1, datums)
}
