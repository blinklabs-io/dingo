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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestImportPoolRejectsTransactionWithoutMetadataWriteHandle(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)

	newPool := func() (*models.Pool, *models.PoolRegistration) {
		poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
		return &models.Pool{PoolKeyHash: poolKeyHash},
			&models.PoolRegistration{
				PoolKeyHash: poolKeyHash,
				AddedSlot:   1,
			}
	}

	t.Run("blob-only transaction", func(t *testing.T) {
		txn := db.BlobTxn(true)
		t.Cleanup(func() {
			require.NoError(t, txn.Rollback())
		})
		pool, reg := newPool()

		err := db.ImportPool(txn, pool, reg)

		require.ErrorIs(t, err, types.ErrNilTxn)
	})

	t.Run("read-only metadata transaction", func(t *testing.T) {
		txn := db.MetadataTxn(false)
		t.Cleanup(func() {
			require.NoError(t, txn.Rollback())
		})
		pool, reg := newPool()

		err := db.ImportPool(txn, pool, reg)

		require.ErrorIs(t, err, types.ErrTxnWrongType)
	})
}
