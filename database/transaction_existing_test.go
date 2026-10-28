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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func existingTxTestHash(i int) []byte {
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(i)) //nolint:gosec
	return hash
}

// The request spans more hashes than SQLite binds in one statement, so the
// lookup must chunk and still report every stored hash and no missing one.
func TestGetExistingTransactionHashesAcrossChunks(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)
	const total = 1200
	tx, err := raw.Begin()
	require.NoError(t, err)
	stored := make(map[string]bool, total/2)
	hashes := make([][]byte, 0, total)
	for i := range total {
		hash := existingTxTestHash(i)
		hashes = append(hashes, hash)
		if i%2 == 1 {
			continue
		}
		stored[string(hash)] = true
		_, err := tx.Exec(`
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid
) VALUES (?, ?, 1, 1, '0', '0', '0', ?, TRUE)`,
			hash, []byte("block"), i,
		)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	got, err := db.GetExistingTransactionHashes(context.Background(), hashes)
	require.NoError(t, err)
	require.Len(t, got, len(stored))
	for _, hash := range got {
		require.True(t, stored[string(hash)], "reported a missing hash %x", hash)
	}
}

func TestGetExistingTransactionHashesCanceledContext(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.GetExistingTransactionHashes(
		ctx,
		[][]byte{existingTxTestHash(1)},
	)
	require.ErrorIs(t, err, context.Canceled)
}
