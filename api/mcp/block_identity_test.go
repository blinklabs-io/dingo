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

package mcp

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestBlockToolsReadBlockIdentity runs the tip and block tools against the
// migrated schema, where an applied transaction names its block through
// block_identity instead of carrying the hash itself.
func TestBlockToolsReadBlockIdentity(t *testing.T) {
	t.Parallel()
	consensus, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	nodeDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)
	db, err := dbtest.RawSQLiteMetadata(t, nodeDB)
	require.NoError(t, err)
	blockHash := bytes.Repeat([]byte{0xab}, 32)
	for i := range 2 {
		_, err = testutil.ExecTransactionInsert(db,
			`INSERT INTO "transaction" (hash, block_hash, slot, block_index, fee)
VALUES (?, ?, 123, ?, '170001')`,
			bytes.Repeat([]byte{byte(i + 1)}, 32), blockHash, i,
		)
		require.NoError(t, err)
	}
	want := "`" + hex.EncodeToString(blockHash) + "`"

	cs := newToolSession(t, db, 100)
	tip := callTool(t, cs, "get_cardano_tip", map[string]any{}, false)
	require.Contains(t, tip, "**Block Hash**: "+want)
	for _, id := range []string{"123", hex.EncodeToString(blockHash)} {
		block := callTool(t, cs, "get_block",
			map[string]any{"hash_or_slot": id}, false)
		require.Contains(t, block, "**Hash**: "+want, id)
		require.Contains(t, block, "**Transactions**: 2", id)
		require.Contains(t, block, "**Total Fees (lovelace)**: 340002", id)
	}
}
