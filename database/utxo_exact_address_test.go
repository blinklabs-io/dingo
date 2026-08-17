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

package database

import (
	"bytes"
	"database/sql"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func exactAddressTestPointer(
	t *testing.T,
	payment []byte,
	pointer byte,
) lcommon.Address {
	t.Helper()
	raw := []byte{
		(lcommon.AddressTypeKeyPointer << 4) |
			lcommon.AddressNetworkTestnet,
	}
	raw = append(raw, payment...)
	raw = append(raw, pointer, 0x00, 0x00)
	addr, err := lcommon.NewAddressFromBytes(raw)
	require.NoError(t, err)
	return addr
}

func seedExactAddressUtxo(
	t *testing.T,
	db *Database,
	raw *sql.DB,
	addr lcommon.Address,
	slot uint64,
	hashByte byte,
) models.Utxo {
	t.Helper()
	txID := uint(slot)
	txHash := bytes.Repeat([]byte{hashByte}, 32)
	_, err := raw.Exec(`
INSERT INTO "transaction" (
    id, hash, slot, block_index, type, fee, collateral_fee, ttl, valid
) VALUES (?, ?, ?, 0, 0, '0', '0', '0', TRUE)`,
		txID,
		txHash,
		slot,
	)
	require.NoError(t, err)
	row := models.Utxo{
		TransactionID: &txID,
		TxId:          txHash,
		OutputIdx:     0,
		PaymentKey:    addr.PaymentKeyHash().Bytes(),
		AddedSlot:     slot,
		Amount:        types.Uint64(slot * 1_000_000),
	}
	if stake := addr.StakeKeyHash(); stake != lcommon.NewBlake2b224(nil) {
		row.StakingKey = stake.Bytes()
	}
	_, err = raw.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, FALSE)`,
		txID,
		row.TxId,
		row.PaymentKey,
		row.StakingKey,
		row.CredentialTag,
		row.AddedSlot,
		strconv.FormatUint(uint64(row.Amount), 10),
		row.OutputIdx,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO address_transaction (
    payment_key, staking_key, credential_tag, transaction_id, slot, tx_index
) VALUES (?, ?, ?, ?, ?, 0)`,
		row.PaymentKey,
		row.StakingKey,
		row.CredentialTag,
		txID,
		slot,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  uint64(row.Amount),
	})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(
			txn.Blob(),
			row.TxId,
			row.OutputIdx,
			encoded,
		)
	}))
	return row
}

func TestUtxoAddressQueriesPreserveExactIdentityAndPagination(t *testing.T) {
	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	stake := bytes.Repeat([]byte{0xcd}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	base, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		payment,
		stake,
	)
	require.NoError(t, err)
	pointerOne := exactAddressTestPointer(t, payment, 0x01)
	pointerTwo := exactAddressTestPointer(t, payment, 0x02)

	rows := []struct {
		addr lcommon.Address
		slot uint64
		hash byte
	}{
		{pointerOne, 1, 0x01},
		{enterprise, 2, 0x02},
		{pointerTwo, 3, 0x03},
		{enterprise, 4, 0x04},
		{base, 5, 0x05},
		{enterprise, 6, 0x06},
	}
	seeded := make([]models.Utxo, len(rows))
	for i := range rows {
		seeded[i] = seedExactAddressUtxo(
			t,
			db,
			raw,
			rows[i].addr,
			rows[i].slot,
			rows[i].hash,
		)
	}

	for _, tc := range []struct {
		name string
		addr lcommon.Address
		want [][]byte
	}{
		{
			name: "enterprise",
			addr: enterprise,
			want: [][]byte{seeded[1].TxId, seeded[3].TxId, seeded[5].TxId},
		},
		{name: "base", addr: base, want: [][]byte{seeded[4].TxId}},
		{name: "pointer one", addr: pointerOne, want: [][]byte{seeded[0].TxId}},
		{name: "pointer two", addr: pointerTwo, want: [][]byte{seeded[2].TxId}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := db.UtxosByAddress(tc.addr, nil)
			require.NoError(t, err)
			gotIDs := make([][]byte, len(got))
			for i := range got {
				gotIDs[i] = got[i].TxId
			}
			assert.ElementsMatch(t, tc.want, gotIDs)
		})
	}

	pattern, err := models.ExactUtxoAddressPattern(enterprise)
	require.NoError(t, err)
	first, err := db.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, first, 2)
	assert.Equal(t, []byte{0x02, 0x04}, []byte{
		first[0].TxId[0], first[1].TxId[0],
	})

	second, err := db.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			After: &models.UtxoOrderingCursor{
				Slot:       first[1].TxSlot,
				BlockIndex: first[1].TxBlockIndex,
				OutputIdx:  first[1].OutputIdx,
			},
			Limit: 2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, byte(0x06), second[0].TxId[0])

	credentialRows, err := db.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{{
				PaymentPart: payment,
			}},
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, credentialRows, len(rows))

	enterpriseBytes, err := enterprise.Bytes()
	require.NoError(t, err)
	andMatch, err := db.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{{
				ExactAddress: enterpriseBytes,
				PaymentPart:  payment,
			}},
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, andMatch, 3)

	andMismatch, err := db.UtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{{
				ExactAddress: enterpriseBytes,
				PaymentPart: bytes.Repeat(
					[]byte{0xee},
					lcommon.AddressHashSize,
				),
			}},
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, andMismatch)

	atSlot, err := db.UtxosByAddressAtSlot(enterprise, 6, nil)
	require.NoError(t, err)
	require.Len(t, atSlot, 3)

	enterpriseTxs, err := db.GetTransactionsByAddressWithOrder(
		enterprise,
		2,
		1,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, enterpriseTxs, 2)
	assert.Equal(t, []byte{0x04, 0x06}, []byte{
		enterpriseTxs[0].Hash[0],
		enterpriseTxs[1].Hash[0],
	})
	enterpriseTxCount, err := db.CountTransactionsByAddress(enterprise, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, enterpriseTxCount)
	hasEnterpriseTx, err := db.HasTransactionsByAddress(enterprise, nil)
	require.NoError(t, err)
	assert.True(t, hasEnterpriseTx)
}
