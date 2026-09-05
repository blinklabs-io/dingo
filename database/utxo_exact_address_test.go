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
	"encoding/binary"
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

	count, err := db.CountUtxosByAddressWithOrdering(query, nil)
	require.NoError(t, err)
	assert.Equal(t, total, count)

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		page, err := db.UtxosByAddressWithOrdering(
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
	db := openTestDB(t)

	_, err := db.UtxosByAddressWithOrdering(
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
	db := openTestDB(t)

	_, err := db.UtxosByAddressWithOrdering(
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
	txn := db.Transaction(false)
	defer txn.Release()

	before, err := db.CountUtxosByAddressWithOrdering(query, txn)
	require.NoError(t, err)
	require.Equal(t, 5, before)

	// A conflicting write commits out-of-band, as a live node's chain
	// processing would between an API request's two calls.
	for i := range 3 {
		seedExactAddressUtxo(t, db, raw, addr, uint64(100+i), byte(100+i))
	}

	// A fresh (nil-txn) read observes the new rows...
	after, err := db.CountUtxosByAddressWithOrdering(query, nil)
	require.NoError(t, err)
	require.Equal(t, 8, after)

	// ...but reusing the original txn still sees only the original
	// snapshot: the count and the page fetch below cannot disagree about
	// how many rows exist.
	stillBefore, err := db.CountUtxosByAddressWithOrdering(query, txn)
	require.NoError(t, err)
	require.Equal(t, 5, stillBefore)

	rows, err := db.UtxosByAddressWithOrdering(
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
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txHash,
		OutputIdx:  0,
		PaymentKey: addr.PaymentKeyHash().Bytes(),
		AddedSlot:  1,
		Amount:     types.Uint64(1_000_000),
		Assets: []models.Asset{{
			Name:        assetName,
			NameHex:     []byte("6173736574"),
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
	const patternCount = 1_000

	db := openTestDB(t)
	raw := rawSQLiteMetadataFixture(t, db)
	zeroPayment := bytes.Repeat([]byte{0x00}, lcommon.AddressHashSize)
	zeroHash := lcommon.NewBlake2b224(nil)
	addrs := make([]lcommon.Address, patternCount)
	for i := range addrs {
		payload := make([]byte, 4)
		binary.BigEndian.PutUint32(payload, uint32(i)+1)
		addr, err := lcommon.NewByronAddressFromParts(
			0,
			zeroPayment,
			lcommon.ByronAddressAttributes{Payload: payload},
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
		addrs,
		MaxUtxosByAddressResults,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, want.TxId, got[0].TxId)
	assert.Equal(t, want.OutputIdx, got[0].OutputIdx)
}
