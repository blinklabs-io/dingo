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

package database_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type scanFixture struct {
	db       *database.Database
	payment  []byte
	exact    lcommon.Address
	other    lcommon.Address
	nextTxID uint64
}

func newScanFixture(t *testing.T) *scanFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	payment := bytes.Repeat([]byte{0x46}, lcommon.AddressHashSize)
	exact, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone, 0, payment, nil,
	)
	require.NoError(t, err)
	// Shares the payment credential, so it is a coarse candidate the exact
	// address check must reject.
	other, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, 0, payment, payment,
	)
	require.NoError(t, err)
	return &scanFixture{db: db, payment: payment, exact: exact, other: other}
}

// add stores one output at slot with the given number of native assets and
// returns its coin key.
func (f *scanFixture) add(
	t *testing.T,
	addr lcommon.Address,
	slot uint64,
	outputIdx uint32,
	assets int,
) string {
	t.Helper()
	f.nextTxID++
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], f.nextTxID)
	value := mary.MaryTransactionOutputValue{Amount: 1_000_000}
	if assets > 0 {
		names := make(map[cbor.ByteString]*big.Int, assets)
		for i := range assets {
			names[cbor.NewByteString([]byte{byte(i)})] = big.NewInt(1)
		}
		multi := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
			map[lcommon.Blake2b224]map[cbor.ByteString]*big.Int{
				lcommon.NewBlake2b224(bytes.Repeat([]byte{0x01}, 28)): names,
			},
		)
		value.Assets = &multi
	}
	raw, err := cbor.Encode(&mary.MaryTransactionOutput{
		OutputAddress: addr, OutputAmount: value,
	})
	require.NoError(t, err)
	var stake []byte
	if addr.Type() == lcommon.AddressTypeKeyKey {
		stake = f.payment
	}
	require.NoError(t, f.db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			if err := f.db.CreateUtxo(t.Context(), txn, &models.Utxo{
				TxId: txID, OutputIdx: outputIdx, PaymentKey: f.payment,
				StakingKey: stake, AddedSlot: slot, Amount: 1_000_000,
			}); err != nil {
				return err
			}
			return f.db.Blob().SetUtxo(txn.Blob(), txID, outputIdx, raw)
		},
	))
	return fmt.Sprintf("%x#%d", txID, outputIdx)
}

func (f *scanFixture) setTip(t *testing.T, blockNumber uint64) ochainsync.Tip {
	t.Helper()
	tip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			blockNumber*10, bytes.Repeat([]byte{byte(blockNumber)}, 32),
		),
		BlockNumber: blockNumber,
	}
	require.NoError(t, f.db.SetTip(tip, nil))
	return tip
}

func (f *scanFixture) query(t *testing.T, pageSize int) *models.UtxoWithOrderingQuery {
	t.Helper()
	pattern, err := models.ExactUtxoAddressPattern(f.exact)
	require.NoError(t, err)
	return &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern},
		Limit:           pageSize,
	}
}

func coinKey(u *models.UtxoWithOrdering) string {
	return fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)
}

// TestScanUtxosByAddressVisitsEveryOutputOnceInOrder pages through matches
// interleaved with coarse candidates, so pages end on candidate and size
// bounds alike, and checks every match arrives exactly once in ledger order
// with its output CBOR, including the native assets the coin list derives
// from it.
func TestScanUtxosByAddressVisitsEveryOutputOnceInOrder(t *testing.T) {
	t.Parallel()
	f := newScanFixture(t)
	var want []string
	for i := range 9 {
		slot := uint64(10 + i)
		if i%3 == 0 {
			f.add(t, f.other, slot, 0, 0)
		}
		want = append(want, f.add(t, f.exact, slot, 1, i%2*3))
	}
	// Two outputs of one slot order by output index.
	want = append(want, f.add(t, f.exact, 30, 0, 0))
	want = append(want, f.add(t, f.exact, 30, 2, 2))
	tip := f.setTip(t, 7)

	var got []string
	gotAssets := 0
	pages := 0
	gotTip, err := f.db.ScanUtxosByAddress(
		t.Context(), f.query(t, 2), 3, 1<<20,
		func(page []models.UtxoWithOrdering) error {
			pages++
			require.LessOrEqual(t, len(page), 2)
			for i := range page {
				got = append(got, coinKey(&page[i]))
				require.Empty(t, page[i].Assets)
				output, err := page[i].Decode()
				require.NoError(t, err)
				assets, err := models.ConvertMultiAssetToModels(
					output.Assets(),
				)
				require.NoError(t, err)
				gotAssets += len(assets)
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, 4*3+2, gotAssets)
	require.Greater(t, pages, len(want)/2)
	require.Equal(t, tip, gotTip)
}

// TestScanUtxosByAddressReadsOneSnapshot applies a block's worth of changes
// while the scan is between pages: a new output that sorts after the cursor,
// a spend of an output not yet visited, and a new tip. A scan that read each
// page from a fresh transaction would return the new output, drop the spent
// one, and name the new tip.
func TestScanUtxosByAddressReadsOneSnapshot(t *testing.T) {
	t.Parallel()
	f := newScanFixture(t)
	var want []string
	for i := range 4 {
		want = append(want, f.add(t, f.exact, uint64(10+i), 0, 0))
	}
	tip := f.setTip(t, 3)
	lastTxID := make([]byte, 32)
	binary.BigEndian.PutUint64(lastTxID[24:], f.nextTxID)

	var got []string
	mutated := false
	gotTip, err := f.db.ScanUtxosByAddress(
		t.Context(), f.query(t, 1), 10, 1<<20,
		func(page []models.UtxoWithOrdering) error {
			for i := range page {
				got = append(got, coinKey(&page[i]))
			}
			if !mutated {
				mutated = true
				f.add(t, f.exact, 50, 0, 0)
				require.NoError(t, f.db.MarkUtxosDeletedAtSlot(
					t.Context(), nil,
					[]types.UtxoKey{{TxId: lastTxID, OutputIdx: 0}},
					50,
				))
				f.setTip(t, 4)
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, tip, gotTip)
}
