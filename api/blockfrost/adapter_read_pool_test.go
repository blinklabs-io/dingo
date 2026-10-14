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

package blockfrost

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// newSingleReadConnAdapter is newSlotTimeDBBackedAdapter over a metadata
// store whose read pool holds one connection, so a listing that keeps its
// snapshot open while issuing another read waits on itself.
func newSingleReadConnAdapter(
	t *testing.T,
) (*NodeAdapter, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		MaxConnections: 1,
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)
	adapter.slotTime = testSlotTime
	return adapter, db
}

// requireCompletes runs fn and fails if it has not returned within the
// timeout, which is how a listing blocked on its own pooled connection shows.
func requireCompletes(t *testing.T, msg string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	testutil.RequireReceive(t, done, 10*time.Second, msg)
}

func TestNodeAdapterListingsReleaseSnapshotBeforeNestedReads(t *testing.T) {
	t.Parallel()
	adapter, db := newSingleReadConnAdapter(t)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	addr, _, _ := testKeyAddress(t, 0xa2, 0)
	policy := bytes.Repeat([]byte{0xa3}, lcommon.AddressHashSize)
	script := bytes.Repeat([]byte{0xa4}, lcommon.AddressHashSize)

	tx := seedAssetTransaction(t, raw, db, fill32(0x01), 1, 10, 0)
	insertAdapterUtxo(t, raw, &models.Utxo{
		TransactionID: &tx.ID,
		TxId:          tx.Hash,
		PaymentKey:    addr.PaymentKeyHash().Bytes(),
		AddedSlot:     tx.Slot,
		Amount:        types.Uint64(1_000_000),
		Assets: []models.Asset{
			{PolicyId: policy, Name: []byte("T"), Amount: 1},
		},
	})
	storePointerOutputCbor(t, db, tx.Hash, 0, addr, 1_000_000)
	_, err = raw.Exec(
		`INSERT INTO utxo_reference_script (utxo_id, script_hash)
SELECT id, ? FROM utxo WHERE tx_id = ?`,
		script,
		tx.Hash,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetTip(ochainsync.Tip{BlockNumber: 1}, nil))

	ctx := context.Background()
	page := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}
	var (
		total int
		count int
		err2  error
	)
	requireCompletes(t, "ScriptUTXOs", func() {
		var rows []ScriptUTXOInfo
		rows, total, err2 = adapter.ScriptUTXOs(ctx, script, page)
		count = len(rows)
	})
	require.NoError(t, err2)
	require.Equal(t, 1, total)
	require.Equal(t, 1, count)
	requireCompletes(t, "AssetUTXOs", func() {
		var rows []AssetUTXOInfo
		rows, total, err2 = adapter.AssetUTXOs(
			ctx,
			hex.EncodeToString(policy),
			[]byte("T"),
			TransactionRangeParams{Pagination: page},
		)
		count = len(rows)
	})
	require.NoError(t, err2)
	require.Equal(t, 1, total)
	require.Equal(t, 1, count)
}
