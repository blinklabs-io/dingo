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

package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	metadataSqlite "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	dbtestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

type mockBlobStore struct {
	deleteTxErrs     map[string]error
	deleteUtxoErrs   map[string]error
	commitErrs       []error
	deleteTxnIDs     []int
	deleteUtxoTxnIDs []int
	iterator         types.BlobIterator
	txns             []*mockBlobTxn
	utxoData         map[string][]byte
	// syncErr is returned by Sync. syncCount counts calls, and
	// syncAtBlobCommitCount snapshots the blob transaction's commit count as
	// observed from inside Sync, so tests can assert the durability barrier
	// fires after the blob commit rather than before it.
	syncErr               error
	syncCount             int
	syncAtBlobCommitCount int
	// getErr, when set, is returned by Get instead of the default
	// types.ErrBlobKeyNotFound, so a test can simulate a genuine read
	// failure (e.g. a corrupted or unreadable store) distinct from the
	// ordinary "key was never written" case.
	getErr error
}

func (m *mockBlobStore) Sync() error {
	m.syncCount++
	if len(m.txns) > 0 {
		m.syncAtBlobCommitCount = m.txns[0].commitCount
	}
	return m.syncErr
}

type mockBlobTxn struct {
	id            int
	commitErr     error
	commitCount   int
	rollbackCount int
}

func (m *mockBlobStore) Close() error {
	return nil
}

func (m *mockBlobStore) DiskSize() (int64, error) {
	return 0, nil
}

func (m *mockBlobStore) NewTransaction(bool) types.Txn {
	txn := &mockBlobTxn{id: len(m.txns) + 1}
	if idx := len(m.txns); idx < len(m.commitErrs) {
		txn.commitErr = m.commitErrs[idx]
	}
	m.txns = append(m.txns, txn)
	return txn
}

func (m *mockBlobStore) Get(types.Txn, []byte) ([]byte, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) Set(types.Txn, []byte, []byte) error {
	return nil
}

func (m *mockBlobStore) Delete(types.Txn, []byte) error {
	return nil
}

func (m *mockBlobStore) NewIterator(
	types.Txn,
	types.BlobIteratorOptions,
) types.BlobIterator {
	return m.iterator
}

func (m *mockBlobStore) GetCommitTimestamp() (int64, error) {
	return 0, nil
}

func (m *mockBlobStore) SetCommitTimestamp(int64, types.Txn) error {
	return nil
}

func (m *mockBlobStore) SetBlock(
	types.Txn,
	uint64,
	[]byte,
	[]byte,
	uint64,
	uint,
	uint64,
	[]byte,
) error {
	return nil
}

func (m *mockBlobStore) GetBlock(
	types.Txn,
	uint64,
	[]byte,
) ([]byte, types.BlockMetadata, error) {
	return nil, types.BlockMetadata{}, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteBlock(types.Txn, uint64, []byte, uint64) error {
	return nil
}

func (m *mockBlobStore) TombstoneBlock(types.Txn, uint64, []byte) error {
	return nil
}

func (m *mockBlobStore) GetBlockURL(
	context.Context,
	types.Txn,
	ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	return types.SignedURL{}, types.BlockMetadata{}, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) SetUtxo(types.Txn, []byte, uint32, []byte) error {
	return nil
}

func (m *mockBlobStore) GetUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	if m.utxoData != nil {
		if data, ok := m.utxoData[fmt.Sprintf("%x:%d", txId, outputIdx)]; ok {
			return data, nil
		}
	}
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteUtxo(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	mockTxn, ok := txn.(*mockBlobTxn)
	if !ok {
		return types.ErrTxnWrongType
	}
	m.deleteUtxoTxnIDs = append(m.deleteUtxoTxnIDs, mockTxn.id)
	if err, ok := m.deleteUtxoErrs[fmt.Sprintf("%x:%d", txId, outputIdx)]; ok {
		return err
	}
	return nil
}

func (m *mockBlobStore) SetTx(types.Txn, []byte, []byte) error {
	return nil
}

func (m *mockBlobStore) GetTx(types.Txn, []byte) ([]byte, error) {
	return nil, types.ErrBlobKeyNotFound
}

func (m *mockBlobStore) DeleteTx(txn types.Txn, txHash []byte) error {
	mockTxn, ok := txn.(*mockBlobTxn)
	if !ok {
		return types.ErrTxnWrongType
	}
	m.deleteTxnIDs = append(m.deleteTxnIDs, mockTxn.id)
	if err, ok := m.deleteTxErrs[string(txHash)]; ok {
		return err
	}
	return nil
}

func (m *mockBlobTxn) Commit() error {
	m.commitCount++
	return m.commitErr
}

func (m *mockBlobTxn) Rollback() error {
	m.rollbackCount++
	return nil
}

// TestMithrilTrustBoundarySlotStrictPropagatesReadError guards against
// a real bug: database/lifecycle.Truncate's safety check
// against the Mithril trust boundary used MithrilTrustBoundarySlot, which
// silently treats a failed sync-state read the same as "no boundary
// recorded" (returns 0) — bypassing the safety check entirely on a
// transient storage error instead of refusing the truncate. The strict
// variant must instead propagate the read error so a caller enforcing a
// safety check can fail closed.
func TestMithrilTrustBoundarySlotStrictPropagatesReadError(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, closeTestDatabase(db))

	_, err := db.MithrilTrustBoundarySlotStrict(nil)
	require.Error(t, err)
}

// TestMithrilTrustBoundarySlotSwallowsReadError confirms
// MithrilTrustBoundarySlot's existing fail-open contract is unchanged for
// its other caller (the consumed-UTxO recovery heuristic in this same
// file): a failed read still returns 0, not an error.
func TestMithrilTrustBoundarySlotSwallowsReadError(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, closeTestDatabase(db))

	require.Zero(t, db.MithrilTrustBoundarySlot(nil))
}

// TestMithrilTrustBoundarySlotStrictPropagatesParseError guards against a
// corrupted persisted boundary being silently treated as "no Mithril
// snapshot was ever imported": database/lifecycle.Truncate relies on
// MithrilTrustBoundarySlotStrict to fail closed, so a malformed stored
// value (not just a read error) must also come back as an error rather
// than (0, nil), or a truncate could proceed past a boundary that is
// actually corrupt/unreadable.
func TestMithrilTrustBoundarySlotStrictPropagatesParseError(t *testing.T) {
	db := openTestDB(t)
	require.NoError(
		t,
		db.SetSyncState(mithrilLedgerSlotSyncKey, "not-a-slot", nil),
	)

	slot, err := db.MithrilTrustBoundarySlotStrict(nil)
	require.Error(t, err)
	require.Zero(t, slot)
}

// TestMithrilTrustBoundarySlotSwallowsParseError confirms
// MithrilTrustBoundarySlot's existing fail-open contract is unchanged for a
// malformed persisted boundary: it must still return 0 with no error, only
// via its logged fail-open path now that the strict variant propagates the
// parse error.
func TestMithrilTrustBoundarySlotSwallowsParseError(t *testing.T) {
	db := openTestDB(t)
	require.NoError(
		t,
		db.SetSyncState(mithrilLedgerSlotSyncKey, "not-a-slot", nil),
	)

	require.Zero(t, db.MithrilTrustBoundarySlot(nil))
}

func TestDeleteTxBlobsUsesCallerBlobTxn(t *testing.T) {
	t.Parallel()

	store := &mockBlobStore{}
	db := &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				io.Discard,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}
	txn := db.Transaction(true)

	txHashes := [][]byte{{0x01}, {0x02}, {0x03}}
	require.NoError(t, deleteTxBlobs(db, txHashes, txn))
	require.Len(t, store.txns, 1)
	require.Equal(t, []int{1, 1, 1}, store.deleteTxnIDs)
	require.Zero(t, store.txns[0].commitCount)
	require.Zero(t, store.txns[0].rollbackCount)

	require.NoError(t, txn.Rollback())
}

func TestDeleteTxBlobsCountsFailedBatchCommit(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	store := &mockBlobStore{commitErrs: []error{errors.New("commit failed")}}
	db := &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				&logs,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}

	txHashes := [][]byte{{0x01}, {0x02}, {0x03}}
	require.NoError(t, deleteTxBlobs(db, txHashes, nil))
	require.Len(t, store.txns, 1)
	require.Equal(t, 1, store.txns[0].commitCount)
	require.Contains(t, logs.String(), "\"failed\":3")
	require.Contains(t, logs.String(), "\"total\":3")
}

// TestDeleteUtxoBlobsCountsFailedBatchCommit injects a UTxO batch commit failure.
// It verifies every uncommitted deletion is included in the aggregate error count.
func TestDeleteUtxoBlobsCountsFailedBatchCommit(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	store := &mockBlobStore{commitErrs: []error{errors.New("commit failed")}}
	db := &Database{
		blob: store,
		logger: slog.New(
			slog.NewJSONHandler(
				&logs,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	}

	utxos := []models.Utxo{
		{TxId: []byte{0x01}, OutputIdx: 0},
		{TxId: []byte{0x02}, OutputIdx: 1},
		{TxId: []byte{0x03}, OutputIdx: 2},
	}
	require.NoError(t, deleteUtxoBlobs(db, utxos, nil))
	require.Len(t, store.txns, 1)
	require.Equal(t, 1, store.txns[0].commitCount)
	require.Contains(t, logs.String(), "\"failed\":3")
	require.Contains(t, logs.String(), "\"total\":3")
}

// TestTransactionsDeleteRolledbackLogsBlobFailureAndDeletesMetadata injects a transaction blob deletion failure.
// It verifies the failure is logged while rollback metadata cleanup still succeeds.
func TestTransactionsDeleteRolledbackLogsBlobFailureAndDeletesMetadata(
	t *testing.T,
) {
	var logs bytes.Buffer
	logger := slog.New(
		slog.NewJSONHandler(
			&logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	txHash := bytes.Repeat([]byte{0x11}, 32)
	dataDir := t.TempDir()
	sqliteStore, err := metadataSqlite.NewSQLStore(
		metadataSqlite.Config{DataDir: dataDir},
		metadata.ProviderDependencies{Logger: logger},
	)
	require.NoError(t, err)
	require.NoError(t, sqliteStore.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, sqliteStore.Close())
	})
	store := &mockBlobStore{
		deleteTxErrs: map[string]error{
			string(txHash): errors.New("delete tx blob failed"),
		},
	}
	db := &Database{
		blob:     store,
		metadata: sqliteStore,
		logger:   logger,
		config:   &Config{DataDir: dataDir, Logger: logger},
	}
	defer func() {
		require.NoError(t, db.Close())
	}()
	raw := rawSQLiteMetadataFixture(t, db)
	_, err = raw.Exec(
		`INSERT INTO "transaction" (hash, slot, valid) VALUES (?, ?, ?)`,
		txHash,
		200,
		true,
	)
	require.NoError(t, err)

	require.NoError(t, db.TransactionsDeleteRolledback(100, nil))

	var count int64
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		txHash,
	).Scan(&count))
	require.Zero(t, count)
	require.Contains(t, logs.String(), "\"level\":\"WARN\"")
	require.Contains(t, logs.String(), "failed to delete TX blob data")
	require.Contains(t, logs.String(), "\"txHash\"")
	require.Contains(t, logs.String(), "\"total\":1")
}

// TestUtxosDeleteRolledbackLogsBlobFailureAndDeletesMetadata injects a UTxO blob deletion failure.
// It verifies the failure is logged while rollback metadata cleanup still succeeds.
func TestUtxosDeleteRolledbackLogsBlobFailureAndDeletesMetadata(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(
		slog.NewJSONHandler(
			&logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	txID := bytes.Repeat([]byte{0x22}, 32)
	dataDir := t.TempDir()
	sqliteStore, err := metadataSqlite.NewSQLStore(
		metadataSqlite.Config{DataDir: dataDir},
		metadata.ProviderDependencies{Logger: logger},
	)
	require.NoError(t, err)
	require.NoError(t, sqliteStore.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, sqliteStore.Close())
	})
	store := &mockBlobStore{
		deleteUtxoErrs: map[string]error{
			fmt.Sprintf("%x:%d", txID, 0): errors.New(
				"delete utxo blob failed",
			),
		},
	}
	db := &Database{
		blob:     store,
		metadata: sqliteStore,
		logger:   logger,
		config:   &Config{DataDir: dataDir, Logger: logger},
	}
	defer func() {
		require.NoError(t, db.Close())
	}()
	raw := rawSQLiteMetadataFixture(t, db)
	_, err = raw.Exec(`
INSERT INTO utxo (tx_id, output_idx, added_slot, amount)
VALUES (?, ?, ?, ?)`,
		txID,
		0,
		200,
		"1",
	)
	require.NoError(t, err)

	require.NoError(t, db.UtxosDeleteRolledback(100, nil))

	var count int64
	require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM utxo WHERE tx_id = ? AND output_idx = ?`,
		txID,
		0,
	).Scan(&count))
	require.Zero(t, count)
	require.Contains(t, logs.String(), "\"level\":\"WARN\"")
	require.Contains(t, logs.String(), "failed to delete UTxO blob data")
	require.Contains(t, logs.String(), "\"added_slot\":200")
	require.Contains(t, logs.String(), "\"total\":1")
}

func TestRecoverConsumedUtxoLegacyRawCborWithoutProducerBlockFails(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	origBlob := db.Blob()
	store := &mockBlobStore{utxoData: make(map[string][]byte)}
	db.SetBlobStore(store)
	if origBlob != nil {
		require.NoError(t, origBlob.Close())
	}

	txId := bytes.Repeat([]byte{0xAB}, 32)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	rawOutput, err := cbor.Encode(output)
	require.NoError(t, err)
	store.utxoData[fmt.Sprintf("%x:%d", txId, 0)] = rawOutput

	txn := db.Transaction(true)
	defer txn.Release()

	_, err = db.recoverConsumedUtxo(
		dbtestutil.NewMockInput(txId, 0),
		txn,
		false,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUtxoNotFound)
}

// TestRecoveredProducerOnPrimaryChain verifies the membership check that gates
// blob-recovery of consumed inputs (issue #3005 Mode B cross-fork splice). A
// producer block that is present in the append-only blob store but is not the
// block indexed on the applied primary chain at its height (an abandoned fork)
// must be reported off-chain, so recoverConsumedUtxo refuses to resurrect it
// for a validated block past the Mithril boundary.
func TestRecoveredProducerOnPrimaryChain(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir:              t.TempDir(),
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		StrictUtxoValidation: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const height = uint64(100)
	canonHash := bytes.Repeat([]byte{0x11}, 32)
	forkHash := bytes.Repeat([]byte{0x22}, 32)
	prevHash := bytes.Repeat([]byte{0x01}, 32)

	create := func(slot uint64, hash []byte) {
		t.Helper()
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(itxn *Txn) error {
			return db.BlockCreate(models.Block{
				ID:       height,
				Slot:     slot,
				Hash:     hash,
				PrevHash: prevHash,
				Number:   height,
				Cbor:     []byte{0x80},
			}, itxn)
		}))
		txn.Release()
	}

	// The producer's block ID is supplied by the caller (every recovery path
	// has already loaded the producer), so the check is by ID and hash.
	check := func(producerID uint64, hash []byte) bool {
		t.Helper()
		txn := db.Transaction(true)
		defer txn.Release()
		onChain, cErr := db.recoveredProducerOnPrimaryChain(
			txn, producerID, hash,
		)
		require.NoError(t, cErr)
		return onChain
	}

	// The block currently indexed at this height is on the primary chain.
	create(1000, canonHash)
	require.True(t, check(height, canonHash),
		"canonical producer must be reported on-chain")

	// Index the same height to a different block: the earlier block remains
	// retrievable by point (append-only blob) but is no longer the canonical
	// block at its height, i.e. an abandoned fork.
	create(2000, forkHash)
	require.False(t, check(height, canonHash),
		"abandoned-fork producer (not indexed at its height) must be off-chain")
	require.True(t, check(height, forkHash),
		"the newly indexed block is now the canonical producer")

	// A producer at a height the chain never indexed is off-chain.
	require.False(t, check(height+1, canonHash),
		"producer at an unindexed height must be reported off-chain")
}

// TestSetTransactionRecoveryPopulatesProducerFK verifies that when
// ensureTransactionConsumedUtxos has to recover a missing UTxO row for
// a consumed input, the recovered row carries the producer transaction
// FK. Without this FK, SetUtxosNotDeletedAfterSlot would reanimate the
// row during a rollback, but joins on utxo.transaction_id would silently drop
// it from producer-transaction output lookups.
func TestSetTransactionRecoveryPopulatesProducerFK(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)

	// Persist each producer half: the block + blob offsets, plus a
	// metadata Transaction row with its produced UTxOs. We bypass the
	// db.SetTransaction wrapper because the producer's own inputs are
	// not part of this fixture and would force a recovery cascade. We
	// only need the producer's Transaction row and its produced UTxO
	// rows to exist so the consumer's recovery has a real FK target.
	for _, p := range candidate.producers {
		storeBlockOffsetsOnly(t, db, p.block)
		metaTxn := db.MetadataTxn(true)
		producer := p
		require.NoError(
			t,
			metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					txn.Metadata(),
				)
			}),
		)
		metaTxn.Release()
	}

	// Snapshot each producer transaction's primary key so we can
	// later assert the recovered UTxOs' FK matches.
	producerIDByHash := make(map[string]uint, len(candidate.producers))
	for _, p := range candidate.producers {
		producerHash := p.tx.Hash().Bytes()
		got, err := db.Metadata().GetTransactionByHash(producerHash, nil)
		require.NoError(t, err)
		require.NotNil(
			t,
			got,
			"producer transaction row must be persisted",
		)
		require.NotZero(t, got.ID)
		producerIDByHash[fmt.Sprintf("%x", producerHash)] = got.ID
	}

	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

	// Force the recovery path by deleting the metadata Utxo rows for
	// the inputs the consumer is about to spend. The blob store still
	// has their offset references, and the metadata Transaction row
	// for each producer remains intact.
	refs := make([]models.UtxoId, 0, len(candidate.consumerTx.Consumed()))
	for _, input := range candidate.consumerTx.Consumed() {
		refs = append(refs, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	metaTxn := db.MetadataTxn(true)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteUtxos(refs, txn.Metadata())
		}),
	)
	metaTxn.Release()
	for _, input := range candidate.consumerTx.Consumed() {
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			input.Id().Bytes(),
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.Nil(
			t,
			utxo,
			"setup: utxo %s must be deleted to exercise recovery",
			input.String(),
		)
	}

	// SetTransaction on the consumer triggers
	// ensureTransactionConsumedUtxos -> recoverConsumedUtxo for each
	// missing input.
	require.NoError(
		t,
		db.SetTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		),
	)

	// Each recovered UTxO row must carry the producer transaction FK
	// pointing at the right Transaction.ID.
	for _, input := range candidate.consumerTx.Consumed() {
		producerHash := input.Id().Bytes()
		expectedID, ok := producerIDByHash[fmt.Sprintf("%x", producerHash)]
		require.True(
			t,
			ok,
			"setup: producer hash %x missing from snapshot",
			producerHash,
		)
		utxo, err := db.Metadata().GetUtxoIncludingSpent(
			producerHash,
			input.Index(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(
			t,
			utxo,
			"recovered UTxO for %s must be present",
			input.String(),
		)
		require.NotNil(
			t,
			utxo.TransactionID,
			"recovered UTxO for %s must carry producer FK so that "+
				"rollback reanimation keeps it visible to joins on "+
				"utxo.transaction_id and Preload(\"Outputs\")",
			input.String(),
		)
		require.Equal(t, expectedID, *utxo.TransactionID)
	}

	// Stronger end-to-end check: rollback past the consumer slot and
	// confirm the producer Transaction's preloaded Outputs include
	// each reanimated row.
	rollbackTxn := db.MetadataTxn(true)
	require.NoError(
		t,
		rollbackTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteTransactionsAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	rollbackTxn.Release()
	rollbackTxn = db.MetadataTxn(true)
	require.NoError(
		t,
		rollbackTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetUtxosNotDeletedAfterSlot(
				candidate.consumerPoint.Slot-1,
				txn.Metadata(),
			)
		}),
	)
	rollbackTxn.Release()
	for _, input := range candidate.consumerTx.Consumed() {
		producer, err := db.Metadata().GetTransactionByHash(
			input.Id().Bytes(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, producer)
		found := false
		for _, out := range producer.Outputs {
			if out.OutputIdx == input.Index() {
				found = true
				break
			}
		}
		require.True(
			t,
			found,
			"after rollback, recovered UTxO for %s must be "+
				"reachable from producer Transaction.Outputs preload",
			input.String(),
		)
	}
}

// TestEnsureTransactionConsumedUtxosStrictAppliedInputConservation covers
// strict at-tip recovery after a consumed UTxO row was pruned. A canonical
// producer may be reconstructed from the retained block/blob data (issue
// #3170), while the primary-chain check still rejects an abandoned-fork
// producer (issue #3005).
//
// The fixture stages the producers so that recovery from the blob WOULD
// otherwise succeed (blob offsets present, metadata Transaction rows present,
// only the Utxo rows deleted), isolating the new refusal from the pre-existing
// unrecoverable-input behavior in the sibling test below.
func TestEnsureTransactionConsumedUtxosStrictAppliedInputConservation(
	t *testing.T,
) {
	candidate := findGapConsumeCandidateWithoutCertificates(t)

	// newRecoverableDB stages the fixture in a recovery-ready state: producer
	// blocks and their metadata Transaction rows exist, the consumer block's
	// offsets exist, and the producers' Utxo rows are deleted so the consumer's
	// consumed inputs are absent from the metadata store but reconstructable
	// from the blob.
	newRecoverableDB := func(t *testing.T) *Database {
		t.Helper()
		db, err := newTestDatabase(t, &Config{
			DataDir: t.TempDir(),
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			StrictUtxoValidation: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		for _, p := range candidate.producers {
			storeBlockOffsetsOnly(t, db, p.block)
			metaTxn := db.MetadataTxn(true)
			producer := p
			require.NoError(t, metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					txn.Metadata(),
				)
			}))
			metaTxn.Release()
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)

		refs := make([]models.UtxoId, 0, len(candidate.consumerTx.Consumed()))
		for _, input := range candidate.consumerTx.Consumed() {
			refs = append(refs, models.UtxoId{
				Hash: input.Id().Bytes(),
				Idx:  input.Index(),
			})
		}
		metaTxn := db.MetadataTxn(true)
		require.NoError(t, metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteUtxos(refs, txn.Metadata())
		}))
		metaTxn.Release()
		return db
	}

	setConsumer := func(t *testing.T, db *Database, opts BatchedTxIngestOpts) error {
		t.Helper()
		return db.SetTransactionWithOpts(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
			opts,
		)
	}

	t.Run("flag off recovers from blob", func(t *testing.T) {
		db := newRecoverableDB(t)
		require.NoError(t, setConsumer(t, db, BatchedTxIngestOpts{}))
	})

	t.Run(
		"flag on recovers canonical producer past boundary",
		func(t *testing.T) {
			db := newRecoverableDB(t)
			// No mithril_ledger_slot recorded (0): every slot is past the boundary.
			require.NoError(t, setConsumer(t, db, BatchedTxIngestOpts{
				StrictAppliedInputConservation: true,
			}))
			// The block's transaction must persist the recovered row and mark it
			// spent by the consumer, just as if rollback restoration had preserved
			// the row in the first place.
			for _, input := range candidate.consumerTx.Consumed() {
				utxo, err := db.Metadata().GetUtxoIncludingSpent(
					input.Id().Bytes(),
					input.Index(),
					nil,
				)
				require.NoError(t, err)
				require.NotNil(
					t,
					utxo,
					"input %s must be recovered/persisted when its producer is canonical",
					input.String(),
				)
				require.Equal(t, candidate.consumerPoint.Slot, utxo.DeletedSlot)
			}
		},
	)

	t.Run("flag on tolerated at or below boundary", func(t *testing.T) {
		db := newRecoverableDB(t)
		require.NoError(t, db.SetSyncState(
			mithrilLedgerSlotSyncKey,
			fmt.Sprintf("%d", candidate.consumerPoint.Slot),
			nil,
		))
		require.NoError(t, setConsumer(t, db, BatchedTxIngestOpts{
			StrictAppliedInputConservation: true,
		}))
	})

	t.Run("flag on inert without StrictUtxoValidation", func(t *testing.T) {
		db, err := newTestDatabase(t, &Config{
			DataDir: t.TempDir(),
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			StrictUtxoValidation: false,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		for _, p := range candidate.producers {
			storeBlockOffsetsOnly(t, db, p.block)
			metaTxn := db.MetadataTxn(true)
			producer := p
			require.NoError(t, metaTxn.Do(func(txn *Txn) error {
				return db.Metadata().SetGapBlockTransaction(
					producer.tx,
					producer.point,
					0,
					txn.Metadata(),
				)
			}))
			metaTxn.Release()
		}
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		refs := make([]models.UtxoId, 0, len(candidate.consumerTx.Consumed()))
		for _, input := range candidate.consumerTx.Consumed() {
			refs = append(refs, models.UtxoId{
				Hash: input.Id().Bytes(),
				Idx:  input.Index(),
			})
		}
		metaTxn := db.MetadataTxn(true)
		require.NoError(t, metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().DeleteUtxos(refs, txn.Metadata())
		}))
		metaTxn.Release()
		require.NoError(t, setConsumer(t, db, BatchedTxIngestOpts{
			StrictAppliedInputConservation: true,
		}))
	})
}

// TestEnsureTransactionConsumedUtxosStrictValidation covers issue #396:
// when a consumed UTxO cannot be recovered from either the metadata store
// or the blob store, StrictUtxoValidation controls whether that is a hard
// error or a silently skipped condition, gated by the recorded Mithril
// trust boundary (blocks past the boundary should have complete producer
// history; blocks at or below it legitimately may not).
func TestEnsureTransactionConsumedUtxosStrictValidation(t *testing.T) {
	candidate := findGapConsumeCandidateWithoutCertificates(t)

	newTestDB := func(t *testing.T, strict bool) *Database {
		t.Helper()
		db, err := newTestDatabase(t, &Config{
			DataDir: t.TempDir(),
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			StrictUtxoValidation: strict,
		})

		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		// Intentionally do NOT persist candidate.producers, so the
		// consumer's inputs exist in neither the metadata store nor the
		// blob store and recovery is guaranteed to fail.
		storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
		return db
	}

	setTransaction := func(t *testing.T, db *Database) error {
		t.Helper()
		return db.SetTransaction(
			candidate.consumerTx,
			candidate.consumerPoint,
			0,
			0,
			nil,
			nil,
			mustBlockOffsets(t, candidate.consumerBlock),
			nil,
		)
	}

	t.Run("disabled preserves silent skip", func(t *testing.T) {
		db := newTestDB(t, false)
		require.NoError(t, setTransaction(t, db))
	})

	t.Run("enabled errors past an unset boundary", func(t *testing.T) {
		db := newTestDB(t, true)
		// No mithril_ledger_slot recorded (0): every slot above it is
		// past the boundary, so the missing input must be a hard error.
		err := setTransaction(t, db)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrUtxoNotFound)
	})

	t.Run(
		"enabled skips at or below the recorded boundary",
		func(t *testing.T) {
			db := newTestDB(t, true)
			require.NoError(
				t,
				db.SetSyncState(
					mithrilLedgerSlotSyncKey,
					fmt.Sprintf("%d", candidate.consumerPoint.Slot),
					nil,
				),
			)
			require.NoError(t, setTransaction(t, db))
		},
	)
}

// TestRecoverConsumedUtxoRefusesOffPrimaryChainProducer is the end-to-end guard
// for the Mode B cross-fork splice (issue #3005): it drives recoverConsumedUtxo
// itself, with a real offset-format blob entry, rather than only the membership
// helper. The append-only blob store keeps abandoned-fork blocks, so an
// offset-format UTxO can still resolve to a producer the applied chain
// abandoned; recovering it for a validated block past the Mithril boundary would
// splice in a UTxO the chain never produced.
//
// The three cases pin that the gate is what decides: a canonical producer gets
// past it, an abandoned one is refused with ErrUtxoNotFound, and with the gate
// off the same abandoned producer gets past it again. "Past the gate" is
// observed as the later, unrelated output-decode failure, so the test needs no
// fabricated ledger CBOR.
func TestRecoverConsumedUtxoRefusesOffPrimaryChainProducer(t *testing.T) {
	db, err := newTestDatabase(t, &Config{
		DataDir:              t.TempDir(),
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		StrictUtxoValidation: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const (
		producerID   = uint64(500)
		producerSlot = uint64(1_000)
	)
	txId := randomHash(t)
	producerHash := randomHash(t)
	// Block CBOR whose bytes at [1,4) stand in for the output payload. The
	// content only has to be sliceable; the refusal happens before any decode.
	blockCbor := []byte{0x80, 0xa1, 0xb2, 0xc3, 0xd4}
	payloadOffset, payloadLength := uint32(1), uint32(3)

	createBlock := func(hash []byte, slot uint64) {
		t.Helper()
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(itxn *Txn) error {
			return db.BlockCreate(models.Block{
				ID:       producerID,
				Slot:     slot,
				Hash:     hash,
				PrevHash: randomHash(t),
				Number:   producerID,
				Cbor:     blockCbor,
				Type:     1,
			}, itxn)
		}))
		txn.Release()
	}

	// The producer block, canonical at its height, plus an offset-format blob
	// entry for the consumed input pointing into it.
	createBlock(producerHash, producerSlot)
	var producerHashArr [32]byte
	copy(producerHashArr[:], producerHash)
	blobTxn := db.BlobTxn(true)
	require.NoError(t, blobTxn.Do(func(itxn *Txn) error {
		return db.Blob().SetUtxo(
			itxn.Blob(), txId, 0,
			EncodeUtxoOffset(&CborOffset{
				BlockSlot:  producerSlot,
				BlockHash:  producerHashArr,
				ByteOffset: payloadOffset,
				ByteLength: payloadLength,
			}),
		)
	}))

	recover := func(enforcePrimaryChain bool) error {
		t.Helper()
		txn := db.Transaction(true)
		defer txn.Release()
		_, rErr := db.recoverConsumedUtxo(
			dbtestutil.NewMockInput(txId, 0), txn, enforcePrimaryChain,
		)
		return rErr
	}

	// Canonical producer: the gate allows it through, and recovery proceeds to
	// the output decode.
	err = recover(true)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUtxoNotFound,
		"a canonical producer must not be refused by the primary-chain gate")
	require.ErrorContains(t, err, "decode transaction output",
		"recovery should reach the decode step for a canonical producer")

	// Index a different block at the producer's height: the producer is still
	// in the blob and still resolvable by the offset, but is now an abandoned
	// fork.
	createBlock(randomHash(t), producerSlot+1)

	err = recover(true)
	require.ErrorIs(t, err, ErrUtxoNotFound,
		"an abandoned-fork producer must be refused past the trust boundary")
	require.ErrorContains(t, err, "not on the applied primary chain")

	// With the gate off (below the boundary, or the Mithril gap-closure path)
	// the same producer is recovered as before.
	err = recover(false)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUtxoNotFound,
		"the gate must be the only thing refusing this producer")
	require.ErrorContains(t, err, "decode transaction output")
}
