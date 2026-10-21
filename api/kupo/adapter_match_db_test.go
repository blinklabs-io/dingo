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

package kupo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// historyFixtureSlots seeds two transactions per slot with two outputs each,
// so the stored ordering has ties on slot and on transaction index and spans
// more than two match pages.
const historyFixtureSlots = 275

func newHistoryFixtureAdapter(t *testing.T) (*NodeAdapter, []string) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)

	payment := bytes.Repeat([]byte{0x5a}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)

	type ref struct {
		txID   []byte
		output uint32
	}
	var refs []ref
	tx, err := raw.Begin()
	require.NoError(t, err)
	txRowID := 0
	for slot := 1; slot <= historyFixtureSlots; slot++ {
		for blockIndex := range 2 {
			txRowID++
			// Reverse the hash order against the block index so the
			// transaction id cannot stand in for the transaction index.
			txID := make([]byte, 32)
			binary.BigEndian.PutUint64(txID, uint64(1_000_000-txRowID)) // #nosec G115 -- small fixture index
			blockHash := bytes.Repeat([]byte{byte(slot)}, 32)
			_, err := tx.Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, block_index, type, fee, collateral_fee,
    ttl, valid
) VALUES (?, ?, ?, ?, ?, 0, '0', '0', '0', TRUE)`,
				txRowID, txID, blockHash, slot, blockIndex,
			)
			require.NoError(t, err)
			for output := range uint32(2) {
				_, err := tx.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, added_slot, deleted_slot, amount,
    output_idx, payment_script
) VALUES (?, ?, ?, ?, 0, '1000000', ?, FALSE)`,
					txRowID, txID, payment, slot, output,
				)
				require.NoError(t, err)
				refs = append(refs, ref{txID: txID, output: output})
			}
		}
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		for _, r := range refs {
			if err := db.Blob().SetUtxo(
				txn.Blob(), r.txID, r.output, encoded,
			); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: historyFixtureSlots,
			Hash: bytes.Repeat([]byte{0xee}, 32),
		},
		BlockNumber: historyFixtureSlots,
	}, nil))

	chainManager, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
	})
	require.NoError(t, err)
	adapter, err := NewNodeAdapter(ls)
	require.NoError(t, err)

	keys := make([]string, len(refs))
	for i, r := range refs {
		keys[i] = fmt.Sprintf("%x#%d", r.txID, r.output)
	}
	return adapter, keys
}

func drainMatches(
	t *testing.T,
	adapter *NodeAdapter,
	query MatchQuery,
	limit int,
) ([]string, *MatchCursor, bool) {
	t.Helper()
	iter, err := adapter.Matches(context.Background(), query)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for limit <= 0 || len(got) < limit {
		match, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(
			got,
			fmt.Sprintf("%s#%d", match.TransactionID, match.OutputIndex),
		)
	}
	cursor, truncated := iter.Resume()
	return got, cursor, truncated
}

// TestNodeMatchesScanBudgetResumesStoredOrderInBothDirections drives the
// storage-backed iterator through the scan budget and the cursor in both
// result orders, over rows that tie on slot and on transaction index.
func TestNodeMatchesScanBudgetResumesStoredOrderInBothDirections(
	t *testing.T,
) {
	t.Parallel()

	adapter, keys := newHistoryFixtureAdapter(t)
	for _, oldestFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("oldest_first=%t", oldestFirst), func(t *testing.T) {
			want := slices.Clone(keys)
			if !oldestFirst {
				slices.Reverse(want)
			}
			query := MatchQuery{
				Pattern:     "*",
				OldestFirst: oldestFirst,
				MaxScan:     matchPageSize,
			}
			var all []string
			var sizes []int
			for range 5 {
				got, cursor, truncated := drainMatches(t, adapter, query, 0)
				all = append(all, got...)
				sizes = append(sizes, len(got))
				if !truncated {
					break
				}
				require.NotNil(t, cursor)
				query.Cursor = cursor
			}
			require.Equal(t, []int{512, 512, len(keys) - 1024}, sizes)
			require.Equal(t, want, all)
		})
	}
}

// TestNodeMatchesCursorResumesAfterTheLastReturnedRow pins the position a
// response cut short by its row or byte budget resumes from: the row after
// the last one returned, even in the middle of a storage page.
func TestNodeMatchesCursorResumesAfterTheLastReturnedRow(t *testing.T) {
	t.Parallel()

	adapter, keys := newHistoryFixtureAdapter(t)
	for _, oldestFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("oldest_first=%t", oldestFirst), func(t *testing.T) {
			want := slices.Clone(keys)
			if !oldestFirst {
				slices.Reverse(want)
			}
			query := MatchQuery{Pattern: "*", OldestFirst: oldestFirst}
			head, cursor, truncated := drainMatches(t, adapter, query, 101)
			require.False(t, truncated)
			require.Equal(t, want[:101], head)
			query.Cursor = cursor
			rest, _, truncated := drainMatches(t, adapter, query, 0)
			require.False(t, truncated)
			require.Equal(t, want[101:], rest)
		})
	}
}
