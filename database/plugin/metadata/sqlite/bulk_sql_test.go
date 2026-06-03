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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/schema"
)

// TestBulkInsertColumnsMatchSchema guards the hand-written column lists in
// bulk_sql.go against the GORM-migrated schema. If a model gains/renames a
// column (or GORM names one differently than expected, e.g. ex_units_cpu),
// this fails instead of silently writing the wrong columns.
func TestBulkInsertColumnsMatchSchema(t *testing.T) {
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
