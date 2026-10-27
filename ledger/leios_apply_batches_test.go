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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// leiosDenseOutputsPerTx is the number of produced outputs in each dense
// endorser transaction. Each output stages one UTxO offset and each
// transaction one transaction offset.
const leiosDenseOutputsPerTx = 8

// leiosDenseOffsetEntryBytes is the key-plus-value size charged per staged
// offset entry when sizing the dense endorser block from the store budget.
const leiosDenseOffsetEntryBytes = 96

// leiosApplyDenseTx builds the idx-th Dijkstra endorser transaction of a
// dense endorser block: no inputs, leiosDenseOutputsPerTx distinct outputs and
// a distinct fee, so every transaction hash and produced reference is unique.
func leiosApplyDenseTx(
	t *testing.T,
	idx int,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	outputs := make([]any, 0, leiosDenseOutputsPerTx)
	for j := range leiosDenseOutputsPerTx {
		addr := make([]byte, 29)
		addr[0] = 0x60
		binary.BigEndian.PutUint32(addr[1:], uint32(idx)) //nolint:gosec
		binary.BigEndian.PutUint32(addr[5:], uint32(j)+1) //nolint:gosec
		outputs = append(outputs, map[uint]any{
			0: addr,
			1: uint64(1_000_000),
		})
	}
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{}},
		1: outputs,
		2: 200_000 + uint64(idx), //nolint:gosec
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		nil,
		true,
	})
	require.NoError(t, err)
	tx, err := safedecode.DijkstraBlockTransaction(txCbor)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), tx
}

// newLeiosApplyDenseLedger opens a ledger over a Badger store whose memtable
// is small enough that one dense endorser block exceeds a single write
// transaction. It returns the number of transactions that is about three
// times what one blob transaction accepts.
func newLeiosApplyDenseLedger(
	t *testing.T,
) (*LedgerState, *database.Database, int) {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
		Blob: dbtest.StorageProvider{
			Config: map[string]any{
				"memTableSize":   uint64(1 << 20),
				"valueThreshold": uint64(1024),
			},
		},
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	initializeLeiosApplyTestState(ls)

	budget, ok := db.Blob().(interface {
		RemainingTxnEntries(types.Txn, int) (int, bool)
	})
	require.True(t, ok, "badger must report its transaction budget")
	probe := db.BlobTxn(true)
	remaining, ok := budget.RemainingTxnEntries(
		probe.Blob(),
		leiosDenseOffsetEntryBytes,
	)
	require.NoError(t, probe.Rollback())
	require.True(t, ok)
	require.Positive(t, remaining)
	entriesPerTx := leiosDenseOutputsPerTx + 1
	return ls, db, (3*remaining)/entriesPerTx + 1
}

func leiosApplyDenseBlock(
	t *testing.T,
	count int,
) ([]cbor.RawMessage, []lcommon.Transaction) {
	t.Helper()
	raws := make([]cbor.RawMessage, count)
	txs := make([]lcommon.Transaction, count)
	for i := range count {
		raws[i], txs[i] = leiosApplyDenseTx(t, i)
	}
	return raws, txs
}

// An endorser block whose offset writes exceed one Badger write transaction
// still applies inside the shared transaction, and every produced output
// resolves both inside that transaction and after it commits.
func TestApplyEndorserBlockDenseOffsetsExceedBlobTransaction(t *testing.T) {
	t.Parallel()

	ls, db, count := newLeiosApplyDenseLedger(t)
	raws, txs := leiosApplyDenseBlock(t, count)
	rbPoint := leiosApplyTestRankingPoint(0x51)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		applied, _, err := ls.applyEndorserBlock(
			context.Background(),
			txn,
			rbPoint,
			1,
			700,
			leiosApplyTestEbHash(0x52),
			raws,
		)
		if err != nil {
			return err
		}
		require.Equal(t, count, applied)
		last := txs[count-1]
		got, err := db.CborCache().ResolveUtxoCbor(
			last.Hash().Bytes(),
			uint32(leiosDenseOutputsPerTx-1),
			txn,
		)
		require.NoError(t, err)
		require.Equal(t, last.Produced()[leiosDenseOutputsPerTx-1].Output.Cbor(), got)
		return nil
	}))

	read := db.Transaction(context.Background(), false)
	defer read.Release()
	for _, tx := range txs {
		for idx, utxo := range tx.Produced() {
			got, err := db.CborCache().ResolveUtxoCbor(
				tx.Hash().Bytes(),
				uint32(idx), //nolint:gosec
				read,
			)
			require.NoError(t, err)
			require.Equal(t, utxo.Output.Cbor(), got)
		}
	}
}

func requireLeiosDenseOutputsResolve(
	t *testing.T,
	db *database.Database,
	txs []lcommon.Transaction,
) {
	t.Helper()
	read := db.Transaction(context.Background(), false)
	defer read.Release()
	for _, tx := range txs {
		for idx, utxo := range tx.Produced() {
			got, err := db.CborCache().ResolveUtxoCbor(
				tx.Hash().Bytes(),
				uint32(idx), //nolint:gosec
				read,
			)
			require.NoError(t, err)
			require.Equal(t, utxo.Output.Cbor(), got)
		}
	}
}

func applyLeiosDenseBlock(
	ls *LedgerState,
	db *database.Database,
	rbPoint ocommon.Point,
	ebSlot uint64,
	ebHash []byte,
	raws []cbor.RawMessage,
) error {
	txn := db.Transaction(context.Background(), true)
	return txn.Do(func(txn *database.Txn) error {
		_, _, err := ls.applyEndorserBlock(
			context.Background(),
			txn,
			rbPoint,
			1,
			ebSlot,
			ebHash,
			raws,
		)
		return err
	})
}

// A later endorser block in the same shared transaction can spend an output
// of an earlier dense one. The earlier block's offsets were committed outside
// the shared transaction, so the spend resolves the produced output through a
// fresh snapshot, and the shared transaction still commits.
func TestApplyEndorserBlockDenseOffsetsReadableByLaterBlock(t *testing.T) {
	t.Parallel()

	ls, db, count := newLeiosApplyDenseLedger(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	raws, txs := leiosApplyDenseBlock(t, count)
	rbPoint := leiosApplyTestRankingPoint(0x53)
	spent := txs[count/2]
	spendRaw, _ := leiosApplyTestSpendingTx(t, 0x54, spent.Hash().Bytes(), 3)

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if _, _, err := ls.applyEndorserBlock(
			context.Background(), txn, rbPoint, 1, 710,
			leiosApplyTestEbHash(0x55), raws,
		); err != nil {
			return err
		}
		_, _, err := ls.applyEndorserBlock(
			context.Background(), txn, rbPoint, 1, 711,
			leiosApplyTestEbHash(0x56), []cbor.RawMessage{spendRaw},
		)
		return err
	}))
	spender, _ := leiosApplyTestUtxoState(t, raw, spent.Hash().Bytes(), 3)
	require.NotEmpty(t, spender, "the later block must spend the dense output")
	requireLeiosDenseOutputsResolve(t, db, txs)
}

// A failure between offset batches leaves the committed batches unreferenced:
// the aborted transaction recorded no transaction, and a fresh apply of the
// same endorser block records every transaction and resolves every output.
func TestApplyEndorserBlockDenseOffsetsCrashBetweenBatches(t *testing.T) {
	t.Parallel()

	ls, db, count := newLeiosApplyDenseLedger(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	raws, txs := leiosApplyDenseBlock(t, count)
	rbPoint := leiosApplyTestRankingPoint(0x57)
	ebHash := leiosApplyTestEbHash(0x58)

	injected := errors.New("injected crash between offset batches")
	batches := 0
	ls.afterEndorserOffsetBatch = func() error {
		batches++
		return injected
	}
	err = applyLeiosDenseBlock(ls, db, rbPoint, 720, ebHash, raws)
	require.ErrorIs(t, err, injected)
	require.Equal(t, 1, batches, "the crash must follow the first committed batch")
	requireLeiosApplyTestTxCount(t, raw, 0)

	ls.afterEndorserOffsetBatch = nil
	require.NoError(t, applyLeiosDenseBlock(ls, db, rbPoint, 720, ebHash, raws))
	requireLeiosApplyTestTxCount(t, raw, int64(count))
	requireLeiosDenseOutputsResolve(t, db, txs)
}

// Rolling back the ranking block that admitted a dense endorser block removes
// its transactions, and applying the block again afterwards is correct even
// though the rollback cannot stage every offset delete in one transaction.
func TestApplyEndorserBlockDenseOffsetsRollbackThenReapply(t *testing.T) {
	t.Parallel()

	ls, db, count := newLeiosApplyDenseLedger(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	raws, txs := leiosApplyDenseBlock(t, count)
	rbPoint := leiosApplyTestRankingPoint(0x59)
	ebHash := leiosApplyTestEbHash(0x5a)

	require.NoError(t, applyLeiosDenseBlock(ls, db, rbPoint, 730, ebHash, raws))
	requireLeiosApplyTestTxCount(t, raw, int64(count))

	rollback := db.Transaction(context.Background(), true)
	require.NoError(t, rollback.Do(func(txn *database.Txn) error {
		if err := db.UtxosDeleteRolledback(
			context.Background(), rbPoint.Slot-1, txn,
		); err != nil {
			return err
		}
		return db.TransactionsDeleteRolledback(
			context.Background(), rbPoint.Slot-1, txn,
		)
	}))
	requireLeiosApplyTestTxCount(t, raw, 0)

	require.NoError(t, applyLeiosDenseBlock(ls, db, rbPoint, 730, ebHash, raws))
	requireLeiosApplyTestTxCount(t, raw, int64(count))
	requireLeiosDenseOutputsResolve(t, db, txs)
}
