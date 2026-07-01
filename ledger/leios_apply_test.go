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

package ledger

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newLeiosApplyTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *gorm.DB) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "expected sqlite metadata store")
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db, store.DB()
}

func leiosApplyTestTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, []byte, lcommon.Transaction) {
	t.Helper()
	bodyCbor, err := cbor.Encode(map[uint]any{
		2: 200_000 + uint64(seed),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), bodyCbor, tx
}

func leiosApplyTestEbHash(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size)
}

func leiosApplyTestRankingPoint(seed byte) ocommon.Point {
	return ocommon.Point{
		Slot: 10_000 + uint64(seed),
		Hash: bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size),
	}
}

func requireLeiosApplyTestTxCount(
	t *testing.T,
	gdb *gorm.DB,
	want int64,
) {
	t.Helper()
	var got int64
	require.NoError(t, gdb.Model(&models.Transaction{}).Count(&got).Error)
	require.Equal(t, want, got)
}

func requireLeiosApplyTestNoEndorserBlob(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hash []byte,
) {
	t.Helper()
	require.False(t, db.HasGenesisCbor(slot, hash))
}

func requireLeiosApplyTestEndorserBlob(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hash []byte,
	want []byte,
) {
	t.Helper()
	txn := db.BlobTxn(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		got, _, err := db.Blob().GetBlock(txn.Blob(), slot, hash)
		if err != nil {
			return err
		}
		require.Equal(t, want, got)
		return nil
	}))
}

func TestApplyEndorserBlockDedupSkipsCommittedTransaction(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	rawTx, _, tx := leiosApplyTestTx(t, 0x01)
	require.NoError(t, gdb.Create(&models.Transaction{
		Hash:  tx.Hash().Bytes(),
		Type:  tx.Type(),
		Slot:  99,
		Valid: true,
	}).Error)

	const ebSlot = uint64(200)
	ebHash := leiosApplyTestEbHash(0x22)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x33),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	require.Equal(t, 0, applied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	requireLeiosApplyTestNoEndorserBlob(t, db, ebSlot, ebHash)
}

func TestApplyEndorserBlockDedupSeesEarlierEndorserBlockInBatch(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	rawTx, bodyCbor, _ := leiosApplyTestTx(t, 0x02)
	firstEbHash := leiosApplyTestEbHash(0x44)
	secondEbHash := leiosApplyTestEbHash(0x55)
	firstApplied := -1
	secondApplied := -1

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		firstApplied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x66),
			1,
			300,
			firstEbHash,
			[]cbor.RawMessage{rawTx},
		)
		if err != nil {
			return err
		}
		secondApplied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x77),
			2,
			301,
			secondEbHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	require.Equal(t, 1, firstApplied)
	require.Equal(t, 0, secondApplied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	requireLeiosApplyTestEndorserBlob(t, db, 300, firstEbHash, bodyCbor)
	requireLeiosApplyTestNoEndorserBlob(t, db, 301, secondEbHash)
}

func TestApplyEndorserBlockDedupSkipsRepeatedTransactionInSameEndorserBlock(
	t *testing.T,
) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	rawTx, bodyCbor, _ := leiosApplyTestTx(t, 0x03)
	ebHash := leiosApplyTestEbHash(0x88)
	applied := -1

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x99),
			1,
			400,
			ebHash,
			[]cbor.RawMessage{rawTx, rawTx},
		)
		return err
	}))

	require.Equal(t, 1, applied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	requireLeiosApplyTestEndorserBlob(t, db, 400, ebHash, bodyCbor)
}
