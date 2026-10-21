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

var historyFixturePayment = bytes.Repeat([]byte{0x5a}, lcommon.AddressHashSize)

func historyFixtureEnterprise(t *testing.T) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		historyFixturePayment,
		nil,
	)
	require.NoError(t, err)
	return addr
}

// newHistoryFixtureAdapter seeds one output per row in stored order and pays
// row i to addrFor(i).
func newHistoryFixtureAdapter(
	t *testing.T,
	addrFor func(row int) lcommon.Address,
) (*NodeAdapter, []string) {
	t.Helper()
	adapter, _, keys := newHistoryFixture(t, addrFor, 0)
	return adapter, keys
}

// newHistoryFixture is newHistoryFixtureAdapter with the metadata read pool
// sized to maxConnections (zero keeps the default) and the database exposed.
func newHistoryFixture(
	t *testing.T,
	addrFor func(row int) lcommon.Address,
	maxConnections int,
) (*NodeAdapter, *database.Database, []string) {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		MaxConnections: maxConnections,
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)

	type ref struct {
		txID    []byte
		output  uint32
		encoded []byte
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
				encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
					OutputAddress: addrFor(len(refs)),
					OutputAmount:  1_000_000,
				})
				require.NoError(t, err)
				_, err = tx.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, added_slot, deleted_slot, amount,
    output_idx, payment_script
) VALUES (?, ?, ?, ?, 0, '1000000', ?, FALSE)`,
					txRowID, txID, historyFixturePayment, slot, output,
				)
				require.NoError(t, err)
				refs = append(
					refs,
					ref{txID: txID, output: output, encoded: encoded},
				)
			}
		}
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		for _, r := range refs {
			if err := db.Blob().SetUtxo(
				txn.Blob(), r.txID, r.output, r.encoded,
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
	return adapter, db, keys
}
