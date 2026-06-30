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

package sqlite

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// TestBulkInsertColumnsMatchSchema guards the hand-written column lists in
// bulk_sql.go against the GORM-migrated schema. If a model gains/renames a
// column (or GORM names one differently than expected, e.g. ex_units_cpu),
// this fails instead of silently writing the wrong columns.
func TestBulkInsertColumnsMatchSchema(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	ns := store.DB().NamingStrategy

	cases := []struct {
		name  string
		model any
		cols  []string
	}{
		{"key_witness", &models.KeyWitness{}, keyWitnessCols},
		{"witness_scripts", &models.WitnessScripts{}, witnessScriptCols},
		{"script", &models.Script{}, scriptCols},
		{"plutus_data", &models.PlutusData{}, plutusDataCols},
		{"redeemer", &models.Redeemer{}, redeemerCols},
		{"address_transaction", &models.AddressTransaction{}, addressTxCols},
		{"utxo", &models.Utxo{}, utxoCols},
		{"asset", &models.Asset{}, assetCols},
		{"transaction", &models.Transaction{}, transactionCols},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schema.Parse(tc.model, &sync.Map{}, ns)
			require.NoError(t, err)
			// Expected = every real column except the autoincrement PK,
			// which the bulk INSERT lets SQLite assign.
			want := map[string]struct{}{}
			for _, name := range s.DBNames {
				f := s.FieldsByDBName[name]
				if f.AutoIncrement && f.PrimaryKey {
					continue
				}
				want[name] = struct{}{}
			}
			got := map[string]struct{}{}
			for _, c := range tc.cols {
				got[c] = struct{}{}
			}
			assert.Equal(
				t, want, got,
				"bulk insert columns for %s drifted from the schema",
				tc.name,
			)
		})
	}
}

// TestExecBulkInsert_PartialChunkWritesAllRows exercises the remainder path:
// a row count that is not a multiple of batchChunkRows must still write every
// row (full chunks via the stable statement, the tail via a one-off statement).
func TestExecBulkInsert_PartialChunkWritesAllRows(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	// 2 full chunks of batchChunkRows + a partial remainder.
	n := 2*batchChunkRows + batchChunkRows/2
	items := make([]models.KeyWitness, n)
	for i := range items {
		items[i] = models.KeyWitness{
			Vkey:          []byte{byte(i), byte(i >> 8)},
			Signature:     []byte{0xFF},
			TransactionID: uint(i + 1),
			Type:          models.KeyWitnessTypeVkey,
		}
	}
	require.NoError(t, createSequentialTestTransactions(store.DB(), n, 1))
	require.NoError(t, insertKeyWitnesses(store.DB(), items))

	var count int64
	require.NoError(
		t,
		store.DB().Model(&models.KeyWitness{}).Count(&count).Error,
	)
	assert.Equal(t, int64(n), count)

	// Spot-check a row from the trailing remainder chunk round-trips intact.
	last := n - 1
	var row models.KeyWitness
	require.NoError(
		t,
		store.DB().
			Where("transaction_id = ?", uint(last+1)).
			First(&row).Error,
	)
	assert.Equal(t, []byte{byte(last), byte(last >> 8)}, row.Vkey)
}

// TestInsertScripts_ConflictDoNothing confirms the ON CONFLICT(hash) DO NOTHING
// semantics survive the GORM->fixed-shape conversion: re-inserting an existing
// hash is a no-op that keeps the original row.
func TestInsertScripts_ConflictDoNothing(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	hash := bytes.Repeat([]byte{0xAB}, 28)

	require.NoError(t, insertScripts(store.DB(), []models.Script{{
		Hash: hash, Content: []byte{0x01}, CreatedSlot: 10, Type: 1,
	}}))
	// Same hash, different content: must be ignored, not error or overwrite.
	require.NoError(t, insertScripts(store.DB(), []models.Script{{
		Hash: hash, Content: []byte{0x02}, CreatedSlot: 20, Type: 2,
	}}))

	var got []models.Script
	require.NoError(t, store.DB().Where("hash = ?", hash).Find(&got).Error)
	require.Len(t, got, 1)
	assert.Equal(t, []byte{0x01}, got[0].Content)
}

// TestImportUtxos_FixedShapeRoundTrip inserts a fully-populated UTxO with an
// asset through the fixed-shape path and reads every column back. A swapped
// column in utxoCols/assetCols (which the set-based schema guard would miss)
// corrupts a value here. It also exercises asset linking, which depends on
// BatchRefetchUtxoIDs recovering the utxo ID after a raw INSERT.
func TestImportUtxos_FixedShapeRoundTrip(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	txid := bytes.Repeat([]byte{0x11}, 32)
	in := models.Utxo{
		TxId:       txid,
		OutputIdx:  2,
		PaymentKey: bytes.Repeat([]byte{0x22}, 28),
		StakingKey: bytes.Repeat([]byte{0x33}, 28),
		AddedSlot:  42,
		Amount:     types.Uint64(1234567),
		Assets: []models.Asset{{
			Name:        []byte("TOKEN"),
			NameHex:     []byte("544f4b454e"),
			PolicyId:    bytes.Repeat([]byte{0x44}, 28),
			Fingerprint: []byte("asset1xyz"),
			Amount:      types.Uint64(999),
		}},
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{in}, nil))

	var got models.Utxo
	require.NoError(t, store.DB().
		Where("tx_id = ? AND output_idx = ?", txid, uint32(2)).
		First(&got).Error)
	assert.Equal(t, in.PaymentKey, got.PaymentKey)
	assert.Equal(t, in.StakingKey, got.StakingKey)
	assert.Equal(t, uint64(42), got.AddedSlot)
	assert.Equal(t, types.Uint64(1234567), got.Amount)
	assert.Equal(t, uint32(2), got.OutputIdx)

	var assets []models.Asset
	require.NoError(t, store.DB().Where("utxo_id = ?", got.ID).Find(&assets).Error)
	require.Len(t, assets, 1)
	assert.Equal(t, []byte("TOKEN"), assets[0].Name)
	assert.Equal(t, bytes.Repeat([]byte{0x44}, 28), assets[0].PolicyId)
	assert.Equal(t, []byte("asset1xyz"), assets[0].Fingerprint)
	assert.Equal(t, types.Uint64(999), assets[0].Amount)
}

// TestImportUtxos_ConflictDoNothingIdempotent confirms the
// ON CONFLICT(tx_id,output_idx) DO NOTHING semantics survive the conversion:
// re-importing the same ref keeps the original row.
func TestImportUtxos_ConflictDoNothingIdempotent(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	txid := bytes.Repeat([]byte{0xAA}, 32)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txid, OutputIdx: 0, AddedSlot: 1, Amount: types.Uint64(10),
	}}, nil))
	// Same ref, different payload: must be ignored, not overwritten or errored.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txid, OutputIdx: 0, AddedSlot: 2, Amount: types.Uint64(20),
	}}, nil))

	var got []models.Utxo
	require.NoError(t, store.DB().Where("tx_id = ?", txid).Find(&got).Error)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(1), got[0].AddedSlot)
	assert.Equal(t, types.Uint64(10), got[0].Amount)
}

// TestBatchSpendUtxos_GuardSkipsAlreadySpent confirms the per-row UPDATE keeps
// the deleted_slot = 0 idempotency guard: a second spend of an already-spent
// row (e.g. on retry, or a different tx) is a no-op and leaves the first
// spender's slot/hash intact.
func TestBatchSpendUtxos_GuardSkipsAlreadySpent(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	txid := bytes.Repeat([]byte{0x55}, 32)
	spender1 := bytes.Repeat([]byte{0x66}, 32)
	spender2 := bytes.Repeat([]byte{0x77}, 32)

	// spent_at_tx_id has an FK to transactions.hash; create the spenders.
	require.NoError(t, store.DB().Create(
		&models.Transaction{Hash: spender1, Slot: 100, Valid: true},
	).Error)
	require.NoError(t, store.DB().Create(
		&models.Transaction{Hash: spender2, Slot: 200, Valid: true},
	).Error)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txid, OutputIdx: 0, AddedSlot: 1, Amount: types.Uint64(5),
	}}, nil))

	require.NoError(t, batchSpendUtxos(store.DB(), []utxoSpend{{
		TxId: txid, OutputIdx: 0, Slot: 100, SpentByTxHash: spender1,
	}}))
	// Second spend must be skipped (deleted_slot != 0).
	require.NoError(t, batchSpendUtxos(store.DB(), []utxoSpend{{
		TxId: txid, OutputIdx: 0, Slot: 200, SpentByTxHash: spender2,
	}}))

	var got models.Utxo
	require.NoError(t, store.DB().
		Where("tx_id = ? AND output_idx = ?", txid, uint32(0)).
		First(&got).Error)
	assert.Equal(t, uint64(100), got.DeletedSlot)
	assert.Equal(t, spender1, []byte(got.SpentAtTxId))
}

func newBenchStore(b *testing.B) *MetadataStoreSqlite {
	b.Helper()
	store, err := NewWithOptions(WithStorageMode("api"))
	require.NoError(b, err)
	require.NoError(b, store.Start())
	b.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

func benchKeyWitnessRows(n int) []models.KeyWitness {
	items := make([]models.KeyWitness, n)
	for i := range items {
		items[i] = models.KeyWitness{
			Vkey:          []byte{byte(i), byte(i >> 8), byte(i >> 16)},
			Signature:     []byte{0x01, 0x02, 0x03, 0x04},
			TransactionID: uint(i + 1),
			Type:          models.KeyWitnessTypeVkey,
		}
	}
	return items
}

// BenchmarkInsertKeyWitnesses measures the fixed-shape writer; pair with
// BenchmarkInsertKeyWitnessesGORM to see the delta vs the prior CreateInBatches
// path. key_witness has no unique constraint, so repeated inserts just append.
func BenchmarkInsertKeyWitnesses(b *testing.B) {
	store := newBenchStore(b)
	if err := createSequentialTestTransactions(store.DB(), 1000, 1); err != nil {
		b.Fatal(err)
	}
	rows := benchKeyWitnessRows(1000)
	b.ResetTimer()
	for range b.N {
		if err := insertKeyWitnesses(store.DB(), rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertKeyWitnessesGORM(b *testing.B) {
	store := newBenchStore(b)
	if err := createSequentialTestTransactions(store.DB(), 1000, 1); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		// GORM mutates the slice's PKs on Create, so rebuild per iteration
		// (untimed) to avoid re-inserting populated IDs.
		b.StopTimer()
		rows := benchKeyWitnessRows(1000)
		b.StartTimer()
		if err := store.DB().
			CreateInBatches(rows, batchChunkRows).Error; err != nil {
			b.Fatal(err)
		}
	}
}

// benchHash builds a deterministic 32-byte key unique to (prefix, a, b) so a
// benchmark's repeated flushes insert fresh rows instead of hitting
// ON CONFLICT DO NOTHING.
func benchHash(prefix byte, a, b int) []byte {
	h := make([]byte, 32)
	h[0] = prefix
	binary.BigEndian.PutUint32(h[1:5], uint32(a))
	binary.BigEndian.PutUint32(h[5:9], uint32(b))
	return h
}

func benchUtxos(iter, n int) []models.Utxo {
	utxos := make([]models.Utxo, n)
	for i := range utxos {
		utxos[i] = models.Utxo{
			TxId:       benchHash(0x01, iter, i),
			OutputIdx:  0,
			PaymentKey: benchHash(0x02, iter, i)[:28],
			StakingKey: benchHash(0x03, iter, i)[:28],
			AddedSlot:  uint64(iter),
			Amount:     types.Uint64(1_000_000),
		}
	}
	return utxos
}

// BenchmarkImportUtxos and BenchmarkImportUtxosGORM compare the fixed-shape
// utxo writer against the prior GORM CreateInBatches path on the heaviest flush
// table. Both rebuild rows per iteration (untimed) with keys unique to the
// iteration, so each insert is fresh; utxos carry no assets and a nil
// TransactionID, so neither path touches the asset refetch or any FK.
func BenchmarkImportUtxos(b *testing.B) {
	store := newBenchStore(b)
	for n := range b.N {
		b.StopTimer()
		utxos := benchUtxos(n, 500)
		b.StartTimer()
		if err := importUtxosWithDB(store.DB(), utxos); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkImportUtxosGORM(b *testing.B) {
	store := newBenchStore(b)
	for n := range b.N {
		b.StopTimer()
		utxos := benchUtxos(n, 500)
		b.StartTimer()
		if err := store.DB().Omit("Assets").Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tx_id"},
				{Name: "output_idx"},
			},
			DoNothing: true,
		}).CreateInBatches(utxos, importUtxoBatchSize).Error; err != nil {
			b.Fatal(err)
		}
	}
}
