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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetBlockNonceBatchedQueuesUntilFlush(t *testing.T) {
	t.Parallel()
	db := openIndexerTestDB(t)
	acc := db.NewBatchAccumulator()
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	defer txn.Rollback() //nolint:errcheck

	require.NoError(t, db.SetBlockNonceBatched(
		[]byte{0x01}, 10, []byte{0xa1}, true, acc, txn,
	))
	require.NoError(t, db.SetBlockNonceBatched(
		[]byte{0x02}, 20, []byte{0xa2}, false, acc, txn,
	))
	rows, err := db.GetBlockNoncesInSlotRange(0, 100, txn)
	require.NoError(t, err)
	require.Empty(t, rows, "queued nonces must not be written before flush")

	require.NoError(t, db.FlushBatch(acc, txn))
	rows, err = db.GetBlockNoncesInSlotRange(0, 100, txn)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, []byte{0x01}, rows[0].Hash)
	require.True(t, rows[0].IsCheckpoint)
	require.Equal(t, []byte{0xa2}, rows[1].Nonce)
	require.NoError(t, txn.Commit())
}
