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
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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
	return seedExactAddressUtxoWithHash(
		t, db, raw, addr, slot, bytes.Repeat([]byte{hashByte}, 32),
	)
}

// seedExactAddressUtxoWithHash is seedExactAddressUtxo with an explicit
// 32-byte transaction hash, for callers seeding more than 256 rows: a
// single repeated hashByte collides past that count, since it is the
// transaction table's hash.
func seedExactAddressUtxoWithHash(
	t *testing.T,
	db *Database,
	raw *sql.DB,
	addr lcommon.Address,
	slot uint64,
	txHash []byte,
) models.Utxo {
	t.Helper()
	txID := uint(slot)
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
	paymentKey := any(row.PaymentKey)
	if addr.PaymentKeyHash() == lcommon.NewBlake2b224(nil) {
		paymentKey = nil
	}
	_, err = raw.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, FALSE)`,
		txID,
		row.TxId,
		paymentKey,
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
	t.Parallel()

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
			got, err := db.UtxosByAddress(
				context.Background(),
				[]lcommon.Address{tc.addr},
				MaxUtxosByAddressResults,
				nil,
			)
			require.NoError(t, err)
			gotIDs := make([][]byte, len(got))
			for i := range got {
				gotIDs[i] = got[i].TxId
			}
			assert.ElementsMatch(t, tc.want, gotIDs)
		})
	}

	t.Run("multiple addresses", func(t *testing.T) {
		got, err := db.UtxosByAddress(
			context.Background(),
			[]lcommon.Address{enterprise, base},
			MaxUtxosByAddressResults,
			nil,
		)
		require.NoError(t, err)
		gotIDs := make([][]byte, len(got))
		for i := range got {
			gotIDs[i] = got[i].TxId
		}
		assert.ElementsMatch(
			t,
			[][]byte{
				seeded[1].TxId, seeded[3].TxId, seeded[5].TxId,
				seeded[4].TxId,
			},
			gotIDs,
		)
	})

	pattern, err := models.ExactUtxoAddressPattern(enterprise)
	require.NoError(t, err)
	first, err := db.UtxosByAddressWithOrdering(
		context.Background(),
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
		context.Background(),
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
		context.Background(),
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
		context.Background(),
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
		context.Background(),
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

	atSlot, err := db.UtxosByAddressAtSlot(context.Background(), enterprise, 6, nil)
	require.NoError(t, err)
	require.Len(t, atSlot, 3)

	enterpriseTxs, err := db.GetTransactionsByAddressWithOrder(
		context.Background(),
		enterprise,
		2,
		1,
		"asc",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, enterpriseTxs, 2)
	assert.Equal(t, []byte{0x04, 0x06}, []byte{
		enterpriseTxs[0].Hash[0],
		enterpriseTxs[1].Hash[0],
	})
	enterpriseTxCount, err := db.CountTransactionsByAddress(
		context.Background(),
		enterprise,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 3, enterpriseTxCount)
	hasEnterpriseTx, err := db.HasTransactionsByAddress(context.Background(), enterprise, nil)
	require.NoError(t, err)
	assert.True(t, hasEnterpriseTx)
}

func TestUtxosWithHistoryLoadsCborAndPreservesExactAddressIdentity(
	t *testing.T,
) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0xbc}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	pointer := exactAddressTestPointer(t, payment, 0x01)
	enterpriseUtxo := seedExactAddressUtxo(
		t,
		db,
		raw,
		enterprise,
		10,
		0x71,
	)
	seedExactAddressUtxo(t, db, raw, pointer, 20, 0x72)

	spenderHash := bytes.Repeat([]byte{0x73}, 32)
	spenderBlockHash := bytes.Repeat([]byte{0x74}, 32)
	_, err = raw.Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, block_index, type, fee, collateral_fee,
    ttl, valid
) VALUES (30, ?, ?, 30, 0, 0, '0', '0', '0', TRUE)`,
		spenderHash,
		spenderBlockHash,
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = 30 WHERE tx_id = ?`,
		spenderHash,
		enterpriseUtxo.TxId,
	)
	require.NoError(t, err)

	pattern, err := models.ExactUtxoAddressPattern(enterprise)
	require.NoError(t, err)
	got, err := db.UtxosWithHistory(&models.UtxoHistoryQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern},
	}, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, enterpriseUtxo.TxId, got[0].TxId)
	require.NotEmpty(t, got[0].Cbor)
	require.Equal(t, uint64(30), got[0].DeletedSlot)
	require.Equal(t, spenderBlockHash, got[0].SpentBlockHash)
}

// TestUtxosWithHistoryExactAddressLimitFillsPage proves Limit applies after
// exact-address matching. The coarse SQL predicate intentionally admits 130
// pointer-address siblings sharing the enterprise address's payment
// credential, enough to cross the coordinated scan's 128-row batch boundary.
// The bounded query must continue through those candidates to fill a two-row
// exact page, then preserve the caller's keyset order when retrieving the
// remaining exact match.
func TestUtxosWithHistoryExactAddressLimitFillsPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		descending  bool
		targetSlots map[uint64]bool
		wantSlots   []uint64
	}{
		{
			name:        "ascending",
			targetSlots: map[uint64]bool{131: true, 132: true, 133: true},
			wantSlots:   []uint64{131, 132, 133},
		},
		{
			name:        "descending",
			descending:  true,
			targetSlots: map[uint64]bool{1: true, 2: true, 3: true},
			wantSlots:   []uint64{3, 2, 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			raw := rawSQLiteMetadataFixture(t, db)

			payment := bytes.Repeat(
				[]byte{0xbd},
				lcommon.AddressHashSize,
			)
			target, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				lcommon.AddressNetworkTestnet,
				payment,
				nil,
			)
			require.NoError(t, err)
			sibling := exactAddressTestPointer(t, payment, 0x01)

			for slot := uint64(1); slot <= 133; slot++ {
				addr := sibling
				if test.targetSlots[slot] {
					addr = target
				}
				seedExactAddressUtxo(
					t,
					db,
					raw,
					addr,
					slot,
					byte(slot),
				)
			}

			pattern, err := models.ExactUtxoAddressPattern(target)
			require.NoError(t, err)
			query := &models.UtxoHistoryQuery{
				AddressPatterns: []models.UtxoAddressPattern{pattern},
				Descending:      test.descending,
				Limit:           2,
			}
			first, err := db.UtxosWithHistory(query, nil)
			require.NoError(t, err)
			require.Len(t, first, 2)
			last := first[len(first)-1]
			query.After = &models.UtxoOrderingCursor{
				Slot:       last.TxSlot,
				BlockIndex: last.TxBlockIndex,
				OutputIdx:  last.OutputIdx,
				TxId:       last.TxId,
			}
			second, err := db.UtxosWithHistory(query, nil)
			require.NoError(t, err)
			require.Len(t, second, 1)

			got := append(first, second...)
			for i := range got {
				assert.Equal(t, test.wantSlots[i], got[i].TxSlot)
			}
		})
	}
}

// TestUtxosWithHistoryExactAddressHasNoTotalCandidateCap proves a bounded
// exact-address page is not failed merely because more than 10,000 coarse
// credential-sharing candidates precede its first match. The old coordinated
// scan returned errExactAddressCandidateScanLimit before reaching targetSlot;
// fixed-size keyset batches bound per-query work without hiding that match.
func TestUtxosWithHistoryExactAddressHasNoTotalCandidateCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0xbe}, lcommon.AddressHashSize)
	target, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	sibling := exactAddressTestPointer(t, payment, 0x01)
	siblingCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: sibling,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)
	targetCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: target,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)

	const siblingCount = 10_001
	const targetSlot = siblingCount + 1
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		metadataTxn, err := raw.Begin()
		if err != nil {
			return err
		}
		defer metadataTxn.Rollback() //nolint:errcheck
		for slot := uint64(1); slot <= targetSlot; slot++ {
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint64(txHash[24:], slot)
			outputCbor := siblingCbor
			if slot == targetSlot {
				outputCbor = targetCbor
			}
			if _, err := metadataTxn.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (NULL, ?, ?, NULL, 0, ?, 0, '1000000', 0, FALSE)`,
				txHash,
				payment,
				slot,
			); err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(
				txn.Blob(),
				txHash,
				0,
				outputCbor,
			); err != nil {
				return err
			}
		}
		return metadataTxn.Commit()
	}))

	pattern, err := models.ExactUtxoAddressPattern(target)
	require.NoError(t, err)
	got, err := db.UtxosWithHistory(&models.UtxoHistoryQuery{
		AddressPatterns: []models.UtxoAddressPattern{pattern},
		Limit:           1,
	}, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(targetSlot), got[0].TxSlot)
}

// TestCountAndPageUtxosByAddressWithOrderingCoarseMatch seeds a stake
// credential with a large number of live UTxOs (standing in for a "large
// address", the scenario dingo/3520 flags for unbounded pagination work) and
// proves CountUtxosByAddressWithOrdering and Offset/Descending pagination on
// GetUtxosByAddressWithOrdering return correct, tightly bounded windows
// without loading the full result set: the shared credential pattern here
// never needs CBOR-based exact-address filtering, so a cheap SQL COUNT and a
// LIMIT/OFFSET fetch are the entire answer, matching how NodeAdapter.AccountUTXOs
// (api/blockfrost) now queries a stake credential's UTxOs.
func TestCountAndPageUtxosByAddressWithOrderingCoarseMatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	const total = 250
	stake := bytes.Repeat([]byte{0xfe}, lcommon.AddressHashSize)
	for i := range total {
		payment := make([]byte, lcommon.AddressHashSize)
		binary.BigEndian.PutUint32(payment, uint32(i)+1)
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			payment,
			stake,
		)
		require.NoError(t, err)
		slot := uint64(i) + 1
		seedExactAddressUtxo(t, db, raw, addr, slot, byte(i))
	}

	query := &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{{DelegationPart: stake}},
	}

	count, err := db.CountUtxosByAddressWithOrdering(context.Background(), query, nil)
	require.NoError(t, err)
	assert.Equal(t, total, count)

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		page, err := db.UtxosByAddressWithOrdering(
			context.Background(),
			&models.UtxoWithOrderingQuery{
				AddressPatterns: query.AddressPatterns,
				Limit:           10,
				Offset:          20,
			},
			nil,
		)
		require.NoError(t, err)
		require.Len(t, page, 10)
		// Ascending order by producing slot: offset 20 lands on the 21st
		// seeded row (slot 21), not the 1st.
		assert.Equal(t, uint64(21), page[0].TxSlot)
		assert.Equal(t, uint64(30), page[len(page)-1].TxSlot)
	})

	t.Run(
		"descending page returns newest first without a full reverse",
		func(t *testing.T) {
			page, err := db.UtxosByAddressWithOrdering(
				context.Background(),
				&models.UtxoWithOrderingQuery{
					AddressPatterns: query.AddressPatterns,
					Limit:           10,
					Descending:      true,
				},
				nil,
			)
			require.NoError(t, err)
			require.Len(t, page, 10)
			assert.Equal(t, uint64(total), page[0].TxSlot)
			assert.Equal(t, uint64(total-9), page[len(page)-1].TxSlot)
		},
	)

	t.Run("offset past the end returns an empty page", func(t *testing.T) {
		page, err := db.UtxosByAddressWithOrdering(
			context.Background(),
			&models.UtxoWithOrderingQuery{
				AddressPatterns: query.AddressPatterns,
				Limit:           10,
				Offset:          total,
			},
			nil,
		)
		require.NoError(t, err)
		assert.Empty(t, page)
	})
}

// TestCountUtxosByAddressWithOrderingRejectsExactAddress proves
// CountUtxosByAddressWithOrdering refuses to compute a count for
// exact-address patterns: the coarse SQL predicate over-matches address
// forms that share a payment/delegation credential (pointer addresses being
// the concrete case), so a plain COUNT(*) against it would silently report
// too many UTxOs instead of failing loudly.
func TestCountUtxosByAddressWithOrderingRejectsExactAddress(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	payment := bytes.Repeat([]byte{0x11}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)

	_, err = db.CountUtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		nil,
	)
	require.ErrorIs(t, err, models.ErrExactAddressRequiresCbor)
}

// TestUtxosByAddressWithOrderingRejectsOffsetOnExactAddress proves Offset
// pagination is refused for exact-address patterns, for the same reason
// CountUtxosByAddressWithOrdering is: SQL OFFSET would skip coarse
// candidates, not exact matches, so the skipped count would not equal
// Offset for an address whose coarse predicate over-matches.
func TestUtxosByAddressWithOrderingRejectsOffsetOnExactAddress(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	payment := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)

	_, err = db.UtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           10,
			Offset:          10,
		},
		nil,
	)
	require.ErrorIs(t, err, models.ErrOffsetRequiresCoarseMatch)
}

// TestUtxosByAddressWithOrderingRejectsDescendingKeyset proves Descending
// cannot be combined with keyset (After) pagination: the After predicate's
// comparison operators assume ascending order, so silently accepting both
// would return rows in the wrong direction from the cursor instead of
// failing loudly.
func TestUtxosByAddressWithOrderingRejectsDescendingKeyset(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	_, err := db.UtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			MatchAllAddresses: true,
			Descending:        true,
			After:             &models.UtxoOrderingCursor{Slot: 1},
			Limit:             10,
		},
		nil,
	)
	require.ErrorIs(t, err, models.ErrDescendingKeysetUnsupported)
}

// TestUtxosByAddressWithOrderingRejectsOffsetKeyset proves Offset cannot be
// combined with keyset (After) pagination: applying both would filter to
// rows after the cursor and then additionally skip Offset rows within that
// filtered set, silently returning a page shifted by both controls instead
// of the single, well-defined page either one alone describes.
func TestUtxosByAddressWithOrderingRejectsOffsetKeyset(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	_, err := db.UtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			MatchAllAddresses: true,
			Offset:            10,
			After:             &models.UtxoOrderingCursor{Slot: 1},
			Limit:             10,
		},
		nil,
	)
	require.ErrorIs(t, err, models.ErrOffsetKeysetUnsupported)
}

// TestCountAndFetchShareSnapshotWithinOneTxn proves the fix for a review
// finding on AccountUTXOs/AddressUTXOs: a count and a subsequent page fetch
// against a nil Txn each open their own transaction, so a commit landing
// between them could make the reported total and the returned page
// describe different UTxO sets. Passing one Txn to both calls instead
// (what the adapter now does) must keep them on the same snapshot even
// when a conflicting write commits in between.
func TestCountAndFetchShareSnapshotWithinOneTxn(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	stakeKey := bytes.Repeat([]byte{0x81}, lcommon.AddressHashSize)
	payment := bytes.Repeat([]byte{0x82}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		payment,
		stakeKey,
	)
	require.NoError(t, err)
	for i := range 5 {
		seedExactAddressUtxo(t, db, raw, addr, uint64(i)+1, byte(i)+1)
	}

	query := &models.UtxoWithOrderingQuery{
		AddressPatterns: []models.UtxoAddressPattern{
			{DelegationPart: stakeKey},
		},
	}

	// Opens its transaction eagerly (sqlstore.Store.transaction calls
	// BeginTx before returning), fixing its snapshot right here, before
	// the extra rows below exist.
	txn := db.Transaction(context.Background(), false)
	defer txn.Release()

	before, err := db.CountUtxosByAddressWithOrdering(context.Background(), query, txn)
	require.NoError(t, err)
	require.Equal(t, 5, before)

	// A conflicting write commits out-of-band, as a live node's chain
	// processing would between an API request's two calls.
	for i := range 3 {
		seedExactAddressUtxo(t, db, raw, addr, uint64(100+i), byte(100+i))
	}

	// A fresh (nil-txn) read observes the new rows...
	after, err := db.CountUtxosByAddressWithOrdering(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, 8, after)

	// ...but reusing the original txn still sees only the original
	// snapshot: the count and the page fetch below cannot disagree about
	// how many rows exist.
	stillBefore, err := db.CountUtxosByAddressWithOrdering(context.Background(), query, txn)
	require.NoError(t, err)
	require.Equal(t, 5, stillBefore)

	rows, err := db.UtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: query.AddressPatterns,
			Limit:           100,
		},
		txn,
	)
	require.NoError(t, err)
	require.Len(t, rows, 5)
}

// TestMatchingUtxoRefsByAddressWithOrderingExcludesPointerSiblings proves
// MatchingUtxoRefsByAddressWithOrdering applies the same CBOR-based exact
// match as UtxosByAddressWithOrdering: it returns exactly the enterprise
// address's own UTxOs, in ascending order, excluding pointer-address
// siblings that share its payment credential.
func TestMatchingUtxoRefsByAddressWithOrderingExcludesPointerSiblings(
	t *testing.T,
) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	pointerOne := exactAddressTestPointer(t, payment, 0x01)

	rows := []struct {
		addr lcommon.Address
		slot uint64
		hash byte
	}{
		{pointerOne, 1, 0x01},
		{enterprise, 2, 0x02},
		{enterprise, 4, 0x04},
		{enterprise, 6, 0x06},
	}
	seeded := make([]models.Utxo, len(rows))
	for i := range rows {
		seeded[i] = seedExactAddressUtxo(
			t, db, raw, rows[i].addr, rows[i].slot, rows[i].hash,
		)
	}

	pattern, err := models.ExactUtxoAddressPattern(enterprise)
	require.NoError(t, err)
	refs, err := db.MatchingUtxoRefsByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	assert.Equal(t, seeded[1].TxId, refs[0].Hash)
	assert.Equal(t, seeded[2].TxId, refs[1].Hash)
	assert.Equal(t, seeded[3].TxId, refs[2].Hash)
}

// TestMatchingUtxoRefsByAddressWithOrderingCrossesBatchBoundary seeds more
// exact-address UTxOs than the internal scan's per-batch size (1024) and
// proves the keyset-cursor continuation across batches neither drops nor
// duplicates a match -- the risk a batched scan carries that a single
// unbounded fetch does not.
func TestMatchingUtxoRefsByAddressWithOrderingCrossesBatchBoundary(
	t *testing.T,
) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0x71}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	const total = 1100
	wantHashes := make([][]byte, total)
	for i := range total {
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
		row := seedExactAddressUtxoWithHash(
			t, db, raw, addr, uint64(i)+1, txHash,
		)
		wantHashes[i] = row.TxId
	}

	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)
	refs, err := db.MatchingUtxoRefsByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, refs, total)
	gotHashes := make([][]byte, len(refs))
	for i := range refs {
		gotHashes[i] = refs[i].Hash
	}
	assert.Equal(t, wantHashes, gotHashes)
}

// seedExactAddressImportedUtxo seeds an exact-address UTxO with no producing
// transaction row, so GetUtxosByAddressWithOrdering's COALESCE falls back to
// added_slot and block index zero -- the snapshot-import case described on
// UtxoOrderingCursor. Real snapshot imports can share slot, block index, and
// output index across many rows, differing only by tx_id.
func seedExactAddressImportedUtxo(
	t *testing.T,
	raw *sql.DB,
	addr lcommon.Address,
	slot uint64,
	outputIdx uint32,
	txHash []byte,
) {
	t.Helper()
	var paymentKey any = addr.PaymentKeyHash().Bytes()
	if addr.PaymentKeyHash() == lcommon.NewBlake2b224(nil) {
		paymentKey = nil
	}
	var stakingKey any
	if stake := addr.StakeKeyHash(); stake != lcommon.NewBlake2b224(nil) {
		stakingKey = stake.Bytes()
	}
	_, err := raw.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (NULL, ?, ?, ?, 0, ?, 0, ?, ?, FALSE)`,
		txHash,
		paymentKey,
		stakingKey,
		slot,
		strconv.FormatUint(slot*1_000_000, 10),
		outputIdx,
	)
	require.NoError(t, err)
}

// TestMatchingUtxoRefsByAddressWithOrderingSnapshotTieBreak seeds more
// snapshot-imported exact-address UTxOs sharing one slot, block index, and
// output index than the internal scan's per-batch size (1024), so the
// keyset cursor can only resume correctly past the batch boundary by
// carrying the last row's tx_id. Without it, the next batch's predicate
// re-matches (duplicating) or excludes (dropping) every tied row instead of
// resuming after the one already returned.
func TestMatchingUtxoRefsByAddressWithOrderingSnapshotTieBreak(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0x9a}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	const total = 1200
	const importSlot = 42
	wantHashes := make(map[string]bool, total)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		for i := range total {
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
			seedExactAddressImportedUtxo(t, raw, addr, importSlot, 0, txHash)
			wantHashes[string(txHash)] = true
			encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  1_000_000,
			})
			if err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded); err != nil {
				return err
			}
		}
		return nil
	}))

	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)
	refs, err := db.MatchingUtxoRefsByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(
		t, refs, total,
		"tied snapshot-imported rows must not be dropped or duplicated "+
			"across the scan's batch boundary",
	)
	got := make(map[string]bool, len(refs))
	for _, ref := range refs {
		require.False(
			t,
			got[string(ref.Hash)],
			"duplicate ref for tx %x",
			ref.Hash,
		)
		got[string(ref.Hash)] = true
	}
	assert.Equal(t, wantHashes, got)
}

// TestMatchingUtxoRefsByAddressWithOrderingExceedsOldCandidateScanLimit
// seeds more exact-address UTxOs than the page-fill scan's
// exactAddressCandidateScanLimit (10,000). AddressUTXOs relies on this scan
// for an exact-address total, which -- unlike a page fetch -- cannot stop
// once a page is full; applying that same cap here previously turned a
// valid, merely large, address listing into a hard error instead of
// completing it.
func TestMatchingUtxoRefsByAddressWithOrderingExceedsOldCandidateScanLimit(
	t *testing.T,
) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0x5c}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	const total = 10_050
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		tx, err := raw.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck
		for i := range total {
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
			txID := uint(i + 1)
			slot := uint64(i) + 1
			amount := slot * 1_000_000
			if _, err := tx.Exec(`
INSERT INTO "transaction" (
    id, hash, slot, block_index, type, fee, collateral_fee, ttl, valid
) VALUES (?, ?, ?, 0, 0, '0', '0', '0', TRUE)`,
				txID, txHash, slot,
			); err != nil {
				return err
			}
			if _, err := tx.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (?, ?, ?, NULL, 0, ?, 0, ?, 0, FALSE)`,
				txID,
				txHash,
				addr.PaymentKeyHash().Bytes(),
				slot,
				strconv.FormatUint(amount, 10),
			); err != nil {
				return err
			}
			encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  amount,
			})
			if err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded); err != nil {
				return err
			}
		}
		return tx.Commit()
	}))

	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)
	refs, err := db.MatchingUtxoRefsByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, refs, total)
}

// TestUtxosByAddressWithOrderingSnapshotTieBreak seeds enough
// snapshot-imported candidates sharing one slot, block index, and output
// index -- 118 pointer-address siblings followed by 12 exact-address
// matches, all tied and ordered only by tx_id -- that UtxosByAddressWithOrdering's
// exact-address page-fill scan (batch size 128) splits the 12 matches
// across its batch boundary: the first batch's last row and the second
// batch's first candidates all tie on slot/block index/output index. The
// keyset cursor can only resume past that boundary without revisiting it by
// carrying the last row's tx_id.
func TestUtxosByAddressWithOrderingSnapshotTieBreak(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0x4d}, lcommon.AddressHashSize)
	target, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	sibling := exactAddressTestPointer(t, payment, 0x01)

	const (
		importSlot   = 7
		siblingCount = 118
		matchCount   = 12
	)
	wantHashes := make(map[string]bool, matchCount)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		seedRow := func(addr lcommon.Address, txID uint32) error {
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint32(txHash[28:], txID)
			seedExactAddressImportedUtxo(t, raw, addr, importSlot, 0, txHash)
			encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  1_000_000,
			})
			if err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded)
		}
		txID := uint32(1)
		for range siblingCount {
			if err := seedRow(sibling, txID); err != nil {
				return err
			}
			txID++
		}
		for range matchCount {
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint32(txHash[28:], txID)
			wantHashes[string(txHash)] = true
			if err := seedRow(target, txID); err != nil {
				return err
			}
			txID++
		}
		return nil
	}))

	pattern, err := models.ExactUtxoAddressPattern(target)
	require.NoError(t, err)
	got, err := db.UtxosByAddressWithOrdering(
		context.Background(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           matchCount,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(
		t, got, matchCount,
		"tied snapshot-imported matches must not be dropped across the "+
			"scan's batch boundary",
	)
	seen := make(map[string]bool, len(got))
	for _, u := range got {
		require.False(
			t, seen[string(u.TxId)],
			"duplicate UTxO for tx %x", u.TxId,
		)
		seen[string(u.TxId)] = true
		require.True(t, wantHashes[string(u.TxId)], "unexpected tx %x", u.TxId)
	}
}

// TestUtxosByAddressLoadsAssets proves GetUtxosByAddress still attaches
// native assets to its results after candidate selection was split from
// asset loading (assets are now loaded once on the deduplicated result set
// instead of once per chunk -- see GetUtxosByAddress).
func TestUtxosByAddressLoadsAssets(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	payment := bytes.Repeat([]byte{0x55}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	txHash := bytes.Repeat([]byte{0x99}, 32)
	policyID := bytes.Repeat([]byte{0x33}, 28)
	assetName := []byte("asset")
	require.NoError(t, db.CreateUtxo(context.Background(), nil, &models.Utxo{
		TxId:       txHash,
		OutputIdx:  0,
		PaymentKey: addr.PaymentKeyHash().Bytes(),
		AddedSlot:  1,
		Amount:     types.Uint64(1_000_000),
		Assets: []models.Asset{{
			Name:        assetName,
			PolicyId:    policyID,
			Fingerprint: []byte("fingerprint"),
			Amount:      5,
		}},
	}))

	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded)
	}))

	got, err := db.UtxosByAddress(
		context.Background(),
		[]lcommon.Address{addr},
		MaxUtxosByAddressResults,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Assets, 1)
	assert.Equal(t, assetName, got[0].Assets[0].Name)
	assert.Equal(t, policyID, got[0].Assets[0].PolicyId)
	assert.Equal(t, types.Uint64(5), got[0].Assets[0].Amount)
}

// seedManyUtxoAddressesAndAssertRoundTrip builds numAddrs addresses via
// newAddr, seeds a distinct transaction, UTxO row, and blob CBOR for each,
// then queries GetUtxosByAddress with the full address set in one call and
// asserts every address's UTxO comes back exactly once. Shared by the
// chunking-boundary tests below, which differ only in how the address (and
// therefore its stored payment/staking key columns, which are NULL when the
// address's credential hash is the zero hash) is constructed.
func seedManyUtxoAddressesAndAssertRoundTrip(
	t *testing.T,
	numAddrs int,
	newAddr func(i int) lcommon.Address,
) {
	t.Helper()
	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	zeroHash := lcommon.NewBlake2b224(nil)
	addrs := make([]lcommon.Address, numAddrs)
	txIds := make([][]byte, numAddrs)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *Txn) error {
		for i := range addrs {
			addr := newAddr(i)
			addrs[i] = addr

			txID := uint(i + 1)
			txHash := make([]byte, 32)
			binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
			txIds[i] = txHash
			amount := uint64(i+1) * 1_000_000

			if _, err := raw.Exec(`
INSERT INTO "transaction" (
    id, hash, slot, block_index, type, fee, collateral_fee, ttl, valid
) VALUES (?, ?, ?, 0, 0, '0', '0', '0', TRUE)`,
				txID, txHash, uint64(i+1),
			); err != nil {
				return err
			}

			var paymentKey, stakingKey any
			if pk := addr.PaymentKeyHash(); pk != zeroHash {
				paymentKey = pk.Bytes()
			}
			if sk := addr.StakeKeyHash(); sk != zeroHash {
				stakingKey = sk.Bytes()
			}
			if _, err := raw.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, 0, ?, 0, ?, 0, FALSE)`,
				txID, txHash, paymentKey, stakingKey, uint64(i+1),
				strconv.FormatUint(amount, 10),
			); err != nil {
				return err
			}

			encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  amount,
			})
			if err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded); err != nil {
				return err
			}
		}
		return nil
	}))

	got, err := db.UtxosByAddress(
		context.Background(),
		addrs,
		MaxUtxosByAddressResults,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, numAddrs)

	gotIDs := make([][]byte, len(got))
	for i := range got {
		gotIDs[i] = got[i].TxId
	}
	assert.ElementsMatch(t, txIds, gotIDs)
}

// TestUtxosByAddressExceedsSQLiteParameterLimit proves GetUtxosByAddress
// chunks patterns instead of building one statement that can overflow
// SQLite's limits as the address count grows: without chunking, this test's
// address count fails with "SQL logic error: Expression tree is too large
// (maximum depth 1000)" (a single WHERE built from that many OR-branches),
// and a larger count fails on the 999 bound-parameter limit instead. Every
// address's UTxO must still come back exactly once from the chunked query.
func TestUtxosByAddressExceedsSQLiteParameterLimit(t *testing.T) {
	t.Parallel()

	seedManyUtxoAddressesAndAssertRoundTrip(
		t, 2000, func(i int) lcommon.Address {
			payment := make([]byte, lcommon.AddressHashSize)
			binary.BigEndian.PutUint32(payment, uint32(i)+1)
			stake := make([]byte, lcommon.AddressHashSize)
			binary.BigEndian.PutUint32(stake, uint32(i)+1_000_000)
			addr, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyKey,
				lcommon.AddressNetworkTestnet,
				payment,
				stake,
			)
			require.NoError(t, err)
			return addr
		},
	)
}

// TestUtxosByAddressManyZeroArgBranches covers patterns whose coarse SQL
// branch carries no bind arguments at all: a Byron address whose payment
// hash bytes happen to be all-zero decodes with both PaymentKeyHash and
// StakeKeyHash reading as the zero hash, so AppendUtxoAddressPatternOrBranch
// falls back to a fixed "(payment_key IS NULL...) AND (staking_key IS
// NULL...)" branch with zero args (see AppendUtxoAddressOrBranchMode).
// GetUtxosByAddress's chunking must not rely on bind-argument count alone
// to decide when to flush a chunk, or a long run of these zero-arg branches
// would never trigger a flush and would overflow SQLite's OR-expression
// tree depth. The coarse branch returns the one NULL-credential candidate from
// every chunk, which must be deduplicated before its CBOR is exactly matched
// against the requested addresses.
func TestUtxosByAddressManyZeroArgBranches(t *testing.T) {
	t.Parallel()

	const patternCount = 1_000

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)
	zeroPayment := bytes.Repeat([]byte{0x00}, lcommon.AddressHashSize)
	zeroHash := lcommon.NewBlake2b224(nil)
	addrs := make([]lcommon.Address, patternCount)
	for i := range addrs {
		payload := make([]byte, 4)
		binary.BigEndian.PutUint32(payload, uint32(i)+1)
		derivationPath, err := cbor.Encode(payload)
		require.NoError(t, err)
		addr, err := lcommon.NewByronAddressFromParts(
			0,
			zeroPayment,
			lcommon.ByronAddressAttributes{Payload: derivationPath},
		)
		require.NoError(t, err)
		require.Equal(
			t, zeroHash, addr.PaymentKeyHash(),
			"fixture invariant: payment hash must be zero",
		)
		require.Equal(
			t, zeroHash, addr.StakeKeyHash(),
			"fixture invariant: staking hash must be zero",
		)
		addrs[i] = addr
	}

	want := seedExactAddressUtxo(t, db, raw, addrs[0], 1, 0x42)
	got, err := db.UtxosByAddress(
		context.Background(),
		addrs,
		MaxUtxosByAddressResults,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, want.TxId, got[0].TxId)
	assert.Equal(t, want.OutputIdx, got[0].OutputIdx)
}

// TestResolveUtxoCborWithRecoveryReconstructsMissingBlob is a regression
// test for a bot-review finding on worker-pool
// fix: ledger.queryShelleyUtxoWhole switched from
// IterateLiveUtxos' inline loadCbor (which recovers a missing blob from the
// producing block via recoverUtxoCbor) to a bare CborCache().ResolveUtxoCbor
// call that silently dropped the row on ErrBlobKeyNotFound instead. This
// proves the replacement, ResolveUtxoCborWithRecovery, actually performs the
// same recovery loadCbor did rather than only wrapping the bare resolve.
func TestResolveUtxoCborWithRecoveryReconstructsMissingBlob(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	// Write the block, blob offsets, and metadata rows directly -- bypassing
	// Database.SetTransaction/SetGapBlockTransaction -- the same bypass
	// TestSetTransactionRecoveryPopulatesProducerFK uses, so the produced
	// UTxO is live with an offset reference before its blob entry is
	// deleted below.
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	// Simulate a blob gone missing for an otherwise still-live UTxO (the
	// scenario recoverUtxoCbor exists for): delete just this one output's
	// blob entry while its metadata row (written by seedLiveProducerForWarmTest
	// via SetGapBlockTransaction) stays live.
	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(t, writeTxn.Commit())

	// Confirm the scenario actually exercises the fallback: the bare
	// tiered-cache resolve must miss first.
	_, err = db.CborCache().ResolveUtxoCbor(txId, outputIdx, nil)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"test setup must reproduce a genuinely missing blob")

	recovered, err := db.ResolveUtxoCborWithRecovery(context.Background(), txId, outputIdx, nil)
	require.NoError(t, err, "a missing-but-reconstructable blob must recover")
	require.NotEmpty(t, recovered)

	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryUpgradesBlobOnlyTxnForRecovery covers a
// human-review finding on a caller resolving many refs
// concurrently (queryShelleyUtxoWhole's worker pool) passes a blob-only
// *Txn (BlobTxn, Metadata() == nil) so the resolve hot path never holds a
// metadata connection from the shared read pool. This proves recovery's
// metadata-based fallback (utxoRecoveryBlockForTx, once the blob-based tx
// lookup misses) still succeeds correctly end-to-end against that
// blob-only txn, rather than failing or panicking for lack of a metadata
// handle -- ResolveUtxoCborWithRecovery's own explicit on-demand upgrade
// makes this guarantee independent of the metadata store's
// GetTransactionByHash also separately tolerating a nil types.Txn
// (confirmed: this test still passes with that explicit upgrade removed,
// since GetTransactionByHash falls back to its own ad-hoc pooled
// connection either way -- see ResolveUtxoCborWithRecovery's doc comment
// for why the explicit upgrade is kept anyway).
func TestResolveUtxoCborWithRecoveryUpgradesBlobOnlyTxnForRecovery(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	// Also delete the tx-offset blob entry storeBlockOffsetsOnly wrote:
	// utxoRecoveryBlockForTx tries the blob-based tx lookup
	// (fetchTxBlobSlotAndHash) first, and it would otherwise satisfy
	// recovery without ever reaching the metadata-based fallback this test
	// means to exercise.
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	blobOnlyTxn := db.BlobTxn(false)
	defer blobOnlyTxn.Release()
	require.Nil(
		t, blobOnlyTxn.Metadata(),
		"test setup must reproduce a genuinely blob-only txn",
	)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		context.Background(),
		txId, outputIdx, blobOnlyTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed even when the caller's txn has no "+
			"metadata handle",
	)
	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryUpgradesMetadataOnlyTxnForRecovery verifies
// the mirror image
// of the blob-only case above. A metadata-only Txn (Blob() == nil) hitting
// a missing blob was passed straight into recoverUtxoCbor with no blob
// handle at all, so utxoRecoveryBlockForTx's block lookup
// (BlockByPointTxn) returned ErrNilTxn instead of reconstructing the CBOR
// -- recovery failed even though the metadata needed to locate the
// producing block was right there.
func TestResolveUtxoCborWithRecoveryUpgradesMetadataOnlyTxnForRecovery(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	// Also delete the tx-offset blob entry so the blob-based lookup misses
	// and utxoRecoveryBlockForTx falls to the metadata-based path -- the
	// one this test means to exercise. See the sibling blob-only test's
	// identical comment.
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	metadataOnlyTxn := db.MetadataTxn(context.Background(), false)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		context.Background(),
		txId, outputIdx, metadataOnlyTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed even when the caller's txn has no blob "+
			"handle",
	)
	wantCbor := utxo.Output.Cbor()
	require.NotEmpty(t, wantCbor, "fixture output must carry its own CBOR")
	require.Equal(t, []byte(wantCbor), recovered)
}

// TestResolveUtxoCborWithRecoveryMetadataOnlyWriteCapableCallerPersistsRepair
// verifies that
// withBlobForRecovery copied t.readWrite into aug.readWrite, so a
// write-capable metadata-only caller made aug's freshly-opened blobTxn
// write-capable too. repairUtxoBlob then took its "use the caller's own
// blob txn" branch (txn.IsReadWrite() true) and wrote the recovered offset
// into aug.blobTxn expecting the caller to eventually commit it -- but
// aug.blobTxn is never the caller's own handle and is always torn down by
// ResolveUtxoCborWithRecovery's deferred cleanup (aug.Release, which only
// ever rolls back), discarding the repair every time regardless of
// whether the caller's own txn ever committed.
//
// Proves the fix: even with a write-capable metadata-only caller, the
// repaired offset is durably persisted in the blob store once
// ResolveUtxoCborWithRecovery returns -- because withBlobForRecovery now
// forces aug.readWrite false, routing the write through repairUtxoBlob's
// independent-writer branch, which commits on its own.
func TestResolveUtxoCborWithRecoveryMetadataOnlyWriteCapableCallerPersistsRepair(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	// Write-capable, unlike the sibling upgrade test above -- this is the
	// caller shape the finding is about.
	metadataOnlyTxn := db.MetadataTxn(context.Background(), true)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)
	require.True(
		t, metadataOnlyTxn.IsReadWrite(),
		"test setup must reproduce a genuinely write-capable caller",
	)

	_, err = db.ResolveUtxoCborWithRecovery(context.Background(), txId, outputIdx, metadataOnlyTxn)
	require.NoError(t, err, "UTxO must recover successfully")

	checkTxn := blob.NewTransaction(false)
	defer checkTxn.Rollback() //nolint:errcheck
	repaired, err := blob.GetUtxo(checkTxn, txId, outputIdx)
	require.NoError(
		t, err,
		"repair must be durably committed to the blob store, not "+
			"discarded by aug's deferred rollback",
	)
	require.NotEmpty(t, repaired)
}

// TestResolveUtxoCborWithRecoverySharedBlobRollbackDoesNotFinishCallersTxn
// is the regression test for a chrisguiney review finding on the
// !t.sharedBlob guard added to Txn.rollback() (see withMetadataForRecovery)
// was load-bearing but untested -- every existing recovery test resolves
// only one row per caller txn, so removing the guard still left
// go test ./database/... ./ledger/... green.
//
// queryShelleyUtxoWhole's worker pool reuses one BlobTxn(false) across every
// job it handles. Without the guard, recovering the first row's missing
// blob calls withMetadataForRecovery, which shares the caller's blobTxn;
// releasing that augmented Txn afterward would then roll back -- and so
// finish -- the underlying provider transaction the caller's own txn still
// points at, failing every resolve attempted through it afterward.
//
// Reproduces that shape directly: two independent UTxOs resolved through
// the same blob-only Txn, the first needing recovery and the second not.
func TestResolveUtxoCborWithRecoverySharedBlobRollbackDoesNotFinishCallersTxn(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	// UTxO A: recoverable -- blob and tx-offset entries deleted below, so
	// resolving it requires reconstructing from the producing block.
	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	producedA := producer.tx.Produced()
	require.NotEmpty(t, producedA)
	utxoA := producedA[0]
	txIdA := utxoA.Id.Id().Bytes()
	outputIdxA := utxoA.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txIdA, outputIdxA),
	)
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txIdA),
	)

	// UTxO B: independent and blob-intact -- resolves via the bare
	// tiered-cache path, no recovery involved. Seeded through the same
	// write txn as A's deletions above, committed together below.
	txIdB := bytes.Repeat([]byte{0xB2}, 32)
	const outputIdxB = uint32(0)
	require.NoError(t, db.CreateUtxo(context.Background(), writeTxn, &models.Utxo{
		TxId:      txIdB,
		OutputIdx: outputIdxB,
		AddedSlot: 100,
	}))
	wantCborB := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	require.NoError(
		t,
		blob.SetUtxo(writeTxn.Blob(), txIdB, outputIdxB, wantCborB),
	)
	require.NoError(t, writeTxn.Commit())

	callerTxn := db.BlobTxn(false)
	defer callerTxn.Release()
	require.Nil(
		t, callerTxn.Metadata(),
		"test setup must reproduce a genuinely blob-only txn",
	)

	_, err = db.ResolveUtxoCborWithRecovery(context.Background(), txIdA, outputIdxA, callerTxn)
	require.NoError(t, err, "UTxO A must recover successfully")

	recoveredB, err := db.ResolveUtxoCborWithRecovery(
		context.Background(),
		txIdB, outputIdxB, callerTxn,
	)
	require.NoError(
		t, err,
		"resolving B through the same caller txn afterward must still "+
			"succeed -- recovering A must not have finished the shared "+
			"underlying blob transaction",
	)
	require.Equal(t, wantCborB, recoveredB)
}

// TestResolveUtxoCborWithRecoverySharedMetadataRollbackDoesNotFinishCallersTxn
// verifies that
// withBlobForRecovery's aug borrows the caller's metadataTxn (the mirror
// image of withMetadataForRecovery's borrowed blobTxn) but an earlier
// version left sharedMetadata unset. Releasing aug after recovery then
// rolled back -- and so finished -- the caller's own metadata transaction,
// a side effect of a call that only meant to add blob access for one
// recovery. A write-capable caller reaching this path would lose any
// uncommitted metadata writes to that premature rollback, but this
// test's caller (metadataOnlyTxn) is read-only and writes nothing through
// it, so it does not exercise that loss -- only the narrower property
// below.
//
// Proves the caller's own metadata handle is still usable after recovery
// completes: a plain read through metadataOnlyTxn.Metadata() must not fail
// with "transaction already finished".
func TestResolveUtxoCborWithRecoverySharedMetadataRollbackDoesNotFinishCallersTxn(
	t *testing.T,
) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	blob := db.Blob()
	require.NotNil(t, blob)
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		blob.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(
		t,
		blob.DeleteTx(writeTxn.Blob(), txId),
	)
	require.NoError(t, writeTxn.Commit())

	metadataOnlyTxn := db.MetadataTxn(context.Background(), false)
	defer metadataOnlyTxn.Release()
	require.Nil(
		t, metadataOnlyTxn.Blob(),
		"test setup must reproduce a genuinely metadata-only txn",
	)

	_, err = db.ResolveUtxoCborWithRecovery(context.Background(), txId, outputIdx, metadataOnlyTxn)
	require.NoError(t, err, "UTxO must recover successfully")

	_, err = db.Metadata().GetTransactionByHash(
		txId, metadataOnlyTxn.Metadata(),
	)
	require.NoError(
		t, err,
		"using the caller's own metadata handle after recovery must "+
			"still succeed -- recovering must not have finished the "+
			"shared underlying metadata transaction",
	)
}

// TestResolveUtxoCborWithRecoveryPropagatesUnrecoverable proves a UTxO whose
// producing block cannot be located at all (recovery itself fails) surfaces
// ErrUtxoCborUnavailable rather than being silently treated as resolved --
// this is the sentinel ledger.queryShelleyUtxoWhole's worker loop checks to
// decide whether to still omit a row after actually trying recovery, versus
// the pre-fix behavior of omitting on the bare ErrBlobKeyNotFound alone.
func TestResolveUtxoCborWithRecoveryPropagatesUnrecoverable(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	txId := make([]byte, 32)
	txId[0] = 0xEE
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(itxn *Txn) error {
		return db.CreateUtxo(context.Background(), itxn, &models.Utxo{
			TxId:      txId,
			OutputIdx: 0,
			AddedSlot: 1,
		})
	}))
	txn.Release()

	_, err = db.ResolveUtxoCborWithRecovery(context.Background(), txId, 0, nil)
	require.True(t, errors.Is(err, ErrUtxoCborUnavailable),
		"a UTxO with no indexed producer block must report unavailable, not succeed silently")
}

// TestRepairUtxoBlobWritesThroughCallersPinnedStore verifies recovery reads
// and repairs the caller's pinned blob store. A store swap between the block
// read and repair must not redirect the write to the replacement store.
// The test installs an empty store after the read and checks the repair stays
// in the originally pinned store.
func TestRepairUtxoBlobWritesThroughCallersPinnedStore(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	originalStore := db.Blob()
	require.NotNil(t, originalStore)

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	// Written against the original store, before any swap below.
	storeBlockOffsetsOnly(t, db, producer.block)
	metaTxn := db.MetadataTxn(context.Background(), true)
	t.Cleanup(metaTxn.Release)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				producer.tx, producer.point, 0, nil, txn.Metadata(), 0,
			)
		}),
	)
	metaTxn.Release()

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	txId := utxo.Id.Id().Bytes()
	outputIdx := utxo.Id.Index()

	// Delete just this output's blob entry so the resolve misses and
	// recovery kicks in, same setup as the sibling recovery tests.
	writeTxn := db.Transaction(context.Background(), true)
	t.Cleanup(writeTxn.Release)
	require.NoError(
		t,
		originalStore.DeleteUtxo(writeTxn.Blob(), txId, outputIdx),
	)
	require.NoError(t, writeTxn.Commit())

	// Open a read-only Txn against the original store *before* the swap --
	// this is what pins ResolveUtxoCborWithRecovery to the original store
	// for the whole call, the same as a real caller already holding a txn
	// opened earlier than a concurrent SetBlobStore.
	callerTxn := db.BlobTxn(false)
	require.True(
		t, originalStore == callerTxn.BlobStore(),
		"test setup must pin the caller's txn to the original store",
	)

	// Registered now, before newStore/drain exist below, via forward
	// references closed over by the cleanup funcs -- so a require failure
	// anywhere below (e.g. the pinned-store fix regressing) still releases
	// callerTxn's pin and drains/closes both stores instead of leaking
	// them past FailNow, which would otherwise leave callerTxn's pin held
	// and mask the real failure behind a confusing hang or close error.
	// t.Cleanup runs strictly after this function's own defers (checkTxn/
	// newCheckTxn's Rollback below), and in the reverse of registration
	// order, so registering newStore/originalStore's Close before drain's
	// call before callerTxn's Release here gives the needed teardown
	// order: release the pin, then drain, then close each store.
	var (
		newStore blob.BlobStore
		drain    func()
	)
	t.Cleanup(func() {
		if newStore != nil {
			require.NoError(t, newStore.Close())
		}
	})
	t.Cleanup(func() {
		require.NoError(t, originalStore.Close())
	})
	t.Cleanup(func() {
		if drain != nil {
			drain()
		}
	})
	t.Cleanup(callerTxn.Release)

	// Confirm the scenario actually exercises recovery (and so the repair
	// write-back this test asserts on): the bare tiered-cache resolve must
	// miss first, same premise check as the sibling recovery tests. Without
	// this, a tiered-cache entry surviving from setup would let
	// ResolveUtxoCborWithRecovery return early on the cache hit, and this
	// test would instead fail confusingly at the originalStore.GetUtxo
	// check below rather than testing the pinned-store repair path.
	_, err = db.CborCache().ResolveUtxoCbor(txId, outputIdx, callerTxn)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound,
		"test setup must reproduce a genuinely missing blob")

	// Install a second, empty store -- simulating a blob-store rotation
	// (e.g. bark) happening concurrently with this in-flight recovery.
	// Sized down from badger's defaults (see TestBadgerValueLogFileSize's
	// doc comment): the default reserves 2GiB per store on Windows, which
	// a CI runner opening several of these concurrently can exhaust.
	newStore, err = badger.New(
		badger.WithDataDir(t.TempDir()),
		badger.WithValueLogFileSize(testutil.TestBadgerValueLogFileSize),
		badger.WithMemTableSize(testutil.TestBadgerMemTableSize),
	)
	require.NoError(t, err)
	var prev blob.BlobStore
	prev, drain = db.SetBlobStore(newStore)
	require.True(t, originalStore == prev)

	recovered, err := db.ResolveUtxoCborWithRecovery(
		context.Background(),
		txId, outputIdx, callerTxn,
	)
	require.NoError(
		t, err,
		"recovery must succeed by reading the caller's pinned (original) "+
			"store, even though a different store is now installed",
	)
	require.NotEmpty(t, recovered)

	// The repair write-back must have landed in the *original* store...
	checkTxn := originalStore.NewTransaction(false)
	defer checkTxn.Rollback() //nolint:errcheck
	repaired, err := originalStore.GetUtxo(checkTxn, txId, outputIdx)
	require.NoError(
		t, err,
		"repair must have written the offset back into the original store",
	)
	require.NotEmpty(t, repaired)

	// ...and must not have landed in the newly-installed store, which would
	// only happen if repairUtxoBlob re-pinned the current store instead of
	// reusing the caller's.
	newCheckTxn := newStore.NewTransaction(false)
	defer newCheckTxn.Rollback() //nolint:errcheck
	_, err = newStore.GetUtxo(newCheckTxn, txId, outputIdx)
	require.ErrorIs(
		t, err, types.ErrBlobKeyNotFound,
		"repair must not write into the newly-installed store",
	)
}

func TestTransactionsByAddressHonorSlotRange(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)

	payment := bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)
	enterprise, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	for slot := uint64(1); slot <= 6; slot++ {
		seedExactAddressUtxo(t, db, raw, enterprise, slot, byte(slot))
	}

	from := &models.AddressTransactionPosition{Slot: 3}
	to := &models.AddressTransactionPosition{Slot: 5, TxIndex: 0}
	txs, err := db.GetTransactionsByAddressWithOrder(
		context.Background(), enterprise, 10, 0, "asc", from, to, nil,
	)
	require.NoError(t, err)
	require.Len(t, txs, 3)
	assert.Equal(t, []byte{0x03, 0x04, 0x05}, []byte{
		txs[0].Hash[0], txs[1].Hash[0], txs[2].Hash[0],
	})
	count, err := db.CountTransactionsByAddress(
		context.Background(), enterprise, from, to, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Bounds apply before the page window.
	page, err := db.GetTransactionsByAddressWithOrder(
		context.Background(), enterprise, 1, 1, "desc", from, to, nil,
	)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, byte(0x04), page[0].Hash[0])
}
