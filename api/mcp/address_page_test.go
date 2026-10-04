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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestExactAddressCursorBudgetAndResume(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	payment := bytes.Repeat([]byte{0x45}, 28)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		payment,
		nil,
	)
	require.NoError(t, err)
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		0,
		payment,
		payment,
	)
	require.NoError(t, err)
	require.NoError(t, db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
		for i := range addressCandidateBudget + 103 {
			address := base
			stake := payment
			if i >= addressCandidateBudget {
				address = enterprise
				stake = nil
			}
			raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
				OutputAddress: address, OutputAmount: 1000000,
			})
			if err != nil {
				return err
			}
			txID := make([]byte, 32)
			binary.BigEndian.PutUint64(txID[24:], uint64(i+1))
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId: txID, PaymentKey: payment, StakingKey: stake,
				AddedSlot: uint64(i + 1), Amount: 1000000,
			}); err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(txn.Blob(), txID, 0, raw); err != nil {
				return err
			}
		}
		return nil
	}))
	cs := newToolSessionWithTimeout(t, nil, 100, time.Minute, db)
	type pageResult struct {
		Utxos []struct {
			TxID string `json:"tx_id"`
		} `json:"utxos"`
		Next     string `json:"next_cursor"`
		Complete bool   `json:"complete"`
		Scanned  int    `json:"candidates_scanned"`
	}
	read := func(cursor string, limit int) pageResult {
		t.Helper()
		result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
			Name: "get_utxos", Arguments: map[string]any{
				"address_or_credential": enterprise.String(), "cursor": cursor, "limit": limit,
			},
		})
		require.NoError(t, err)
		require.False(t, result.IsError)
		raw, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		var page pageResult
		require.NoError(t, json.Unmarshal(raw, &page))
		return page
	}
	first := read("", 1)
	require.Empty(t, first.Utxos)
	require.Equal(t, addressCandidateBudget, first.Scanned)
	require.NotEmpty(t, first.Next)
	require.False(t, first.Complete)
	second := read(first.Next, 1)
	require.Len(t, second.Utxos, 1)
	require.Equal(t, 1, second.Scanned)
	require.NotEmpty(t, second.Next)
	third := read(second.Next, 100)
	require.Len(t, third.Utxos, 100)
	require.Equal(t, 100, third.Scanned)
	require.False(t, third.Complete)
	require.NotEmpty(t, third.Next)
	fourth := read(third.Next, 100)
	require.Len(t, fourth.Utxos, 2)
	require.True(t, fourth.Complete)
	require.Empty(t, fourth.Next)
	seen := map[string]bool{}
	for _, page := range []pageResult{second, third, fourth} {
		for _, u := range page.Utxos {
			require.False(t, seen[u.TxID], "duplicate result %s", u.TxID)
			seen[u.TxID] = true
		}
	}
	require.Len(t, seen, 103)
	require.NotEqual(t, second.Utxos[0].TxID, third.Utxos[0].TxID)
	require.NotEqual(t, third.Utxos[0].TxID, third.Utxos[1].TxID)
	callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": base.String(), "cursor": first.Next,
	}, true)
	callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": enterprise.String(), "cursor": "invalid",
	}, true)
	callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": enterprise.String(), "offset": 1,
	}, true)
	pattern, err := models.ExactUtxoAddressPattern(enterprise)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled, err := db.UtxosByAddressPage(ctx, &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern}, Limit: 100,
	}, addressCandidateBudget)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, canceled.Scanned)
}

// pauseAddressBlob delegates to the real store, pausing the second candidate
// until cancellation so the cursor must retain the last fully processed row.
type pauseAddressBlob struct {
	blob.BlobStore
	onSecond func()
	reads    int
}

func (b *pauseAddressBlob) GetUtxo(
	txn types.Txn,
	id []byte,
	idx uint32,
) ([]byte, error) {
	b.reads++
	if b.reads == 2 {
		b.onSecond()
	}
	return b.BlobStore.GetUtxo(txn, id, idx)
}

func TestExactAddressInterruptedPageResumes(t *testing.T) {
	t.Parallel()
	for _, deadline := range []bool{false, true} {
		name := "request_cancel"
		if deadline {
			name = "query_deadline"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(
				t,
				&database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			payment := bytes.Repeat([]byte{0x67}, 28)
			addr, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				0,
				payment,
				nil,
			)
			require.NoError(t, err)
			raw, err := cbor.Encode(
				&shelley.ShelleyTransactionOutput{
					OutputAddress: addr,
					OutputAmount:  1000000,
				},
			)
			require.NoError(t, err)
			require.NoError(
				t,
				db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
					for i := range 3 {
						id := bytes.Repeat([]byte{byte(i + 1)}, 32)
						if err := db.CreateUtxo(txn, &models.Utxo{TxId: id, PaymentKey: payment, AddedSlot: uint64(i + 1), Amount: 1000000}); err != nil {
							return err
						}
						if err := db.Blob().SetUtxo(txn.Blob(), id, 0, raw); err != nil {
							return err
						}
					}
					return nil
				}),
			)
			pattern, err := models.ExactUtxoAddressPattern(addr)
			require.NoError(t, err)
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			defer cancelRequest()
			queryCtx := requestCtx
			if deadline {
				var cancel context.CancelFunc
				queryCtx, cancel = context.WithTimeout(requestCtx, time.Second)
				defer cancel()
			}
			original := db.Blob()
			paused := &pauseAddressBlob{BlobStore: original, onSecond: func() {
				if !deadline {
					cancelRequest()
				}
				<-queryCtx.Done()
			}}
			db.SetBlobStore(paused)
			t.Cleanup(func() { db.SetBlobStore(original) })
			var partial models.UtxoAddressPage
			var lookupErr error
			lookup := func(ctx context.Context, q *models.UtxoWithOrderingQuery, budget int) (models.UtxoAddressPage, error) {
				partial, lookupErr = db.UtxosByAddressPage(ctx, q, budget)
				return partial, lookupErr
			}
			result, _, err := exactAddressPage(
				queryCtx,
				requestCtx,
				lookup,
				pattern,
				"preview",
				addr.String(),
				"",
				100,
				0,
			)
			require.Equal(t, 1, partial.Scanned)
			require.Len(t, partial.Utxos, 1)
			require.Equal(t, uint64(1), partial.Next.Slot)
			if deadline {
				require.NoError(t, err)
				require.ErrorIs(t, lookupErr, context.DeadlineExceeded)
				content := result.StructuredContent.(map[string]any)
				require.Equal(t, "deadline", content["stop_reason"])
				require.Equal(t, false, content["complete"])
				require.NotEmpty(t, content["next_cursor"])
			} else {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			}
			db.SetBlobStore(original)
			rest, err := db.UtxosByAddressPage(
				t.Context(),
				&models.UtxoWithOrderingQuery{
					AddressPatterns: []models.UtxoAddressPattern{pattern},
					After:           partial.Next,
					Limit:           100,
				},
				addressCandidateBudget,
			)
			require.NoError(t, err)
			require.Len(t, rest.Utxos, 2)
			require.Equal(t, uint64(2), rest.Utxos[0].TxSlot)
			require.Equal(t, uint64(3), rest.Utxos[1].TxSlot)
			require.Nil(t, rest.Next)
		})
	}
}

func TestExactAddressDeadlineInterruptsSQL(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	payment := bytes.Repeat([]byte{0x32}, 28)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		payment,
		nil,
	)
	require.NoError(t, err)
	// A fixture view makes candidate sorting expensive inside SQLite itself.
	// Cancellation must reach that statement, before any blob is loaded.
	_, err = raw.Exec(`ALTER TABLE utxo RENAME TO stored_utxo;
 CREATE VIEW utxo AS
 WITH RECURSIVE candidates(n) AS (
  VALUES(1) UNION ALL SELECT n+1 FROM candidates WHERE n<1000000000
 )
 SELECT stored_utxo.*, candidates.n AS candidate FROM stored_utxo CROSS JOIN candidates;`)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO stored_utxo(tx_id, output_idx, payment_key, payment_script, added_slot, deleted_slot, amount)
 VALUES(zeroblob(32),0,?,0,1,0,'1000000')`,
		payment,
	)
	require.NoError(t, err)
	pattern, err := models.ExactUtxoAddressPattern(address)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	page, err := db.UtxosByAddressPage(ctx, &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern}, Limit: 100,
	}, addressCandidateBudget)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, page.Scanned)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestExactAddressFiltersBeforePagination(t *testing.T) {
	t.Parallel()
	nodeDB, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	hash := bytes.Repeat([]byte{0x44}, 28)
	var enterprise lcommon.Address
	for i, kind := range []uint8{lcommon.AddressTypeKeyNone, lcommon.AddressTypeKeyKey, lcommon.AddressTypeScriptNone, lcommon.AddressTypeKeyPointer} {
		stake := hash
		if kind == lcommon.AddressTypeKeyNone ||
			kind == lcommon.AddressTypeScriptNone {
			stake = nil
		}
		if kind == lcommon.AddressTypeKeyPointer {
			stake = []byte{1, 2, 3}
		}
		addr, err := lcommon.NewAddressFromParts(kind, 0, hash, stake)
		require.NoError(t, err)
		if i == 0 {
			enterprise = addr
		}
		txHash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		output := &shelley.ShelleyTransactionOutput{
			OutputAddress: addr,
			OutputAmount:  1000000,
		}
		raw, err := cbor.Encode(output)
		require.NoError(t, err)
		model := models.Utxo{
			TxId:          txHash,
			PaymentKey:    hash,
			PaymentScript: kind == lcommon.AddressTypeScriptNone,
			AddedSlot:     uint64(i + 1),
			Amount:        1000000,
		}
		if kind == lcommon.AddressTypeKeyKey {
			model.StakingKey = hash
		}
		require.NoError(t, nodeDB.CreateUtxo(nil, &model))
		require.NoError(
			t,
			nodeDB.BlobTxn(true).
				Do(func(txn *database.Txn) error { return nodeDB.Blob().SetUtxo(txn.Blob(), txHash, 0, raw) }),
		)
	}
	// SQL tools and full-address lookup share the same active store.
	_, ro, err := NewMCPServer(
		DefaultProviderConfig(),
		ProviderDependencies{Database: nodeDB},
	)
	require.NoError(t, err)
	require.NotNil(t, ro)
	defer ro.Close()
	cs := newToolSession(t, ro, 100, nodeDB)
	text := callTool(
		t,
		cs,
		"get_utxos",
		map[string]any{"address_or_credential": enterprise.String()},
		false,
	)
	require.Contains(t, text, "Showing 1 results")
	require.Contains(t, text, strings.Repeat("01", 32))
	for _, other := range []string{"02", "03", "04"} {
		require.NotContains(t, text, strings.Repeat(other, 32))
	}
	text = callTool(
		t,
		cs,
		"get_utxos",
		map[string]any{"address_or_credential": hex.EncodeToString(hash)},
		false,
	)
	require.Contains(t, text, "Showing 4 results")

	// More than one SQL candidate page is required when a payment credential
	// is shared with many outputs at another full address.
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, 0, hash, hash,
	)
	require.NoError(t, err)
	raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: base, OutputAmount: 1000000,
	})
	require.NoError(t, err)
	for i := range 130 {
		txHash := bytes.Repeat([]byte{byte(i + 5)}, 32)
		require.NoError(t, nodeDB.CreateUtxo(nil, &models.Utxo{
			TxId: txHash, PaymentKey: hash, StakingKey: hash,
			AddedSlot: uint64(i + 5), Amount: 1000000,
		}))
		require.NoError(
			t,
			nodeDB.BlobTxn(true).Do(func(txn *database.Txn) error {
				return nodeDB.Blob().SetUtxo(txn.Blob(), txHash, 0, raw)
			}),
		)
	}
	text = callTool(t, cs, "get_utxos", map[string]any{
		"address_or_credential": enterprise.String(),
	}, false)
	require.Contains(t, text, "Showing 1 results")
	require.Contains(t, text, strings.Repeat("01", 32))
}
