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

package sqlstore

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func dictionaryHash(b byte) []byte {
	return bytes.Repeat([]byte{b}, 28)
}

func importCredentialUtxos(
	t *testing.T,
	store *Store,
	utxos ...models.Utxo,
) {
	t.Helper()
	require.NoError(t, store.ImportUtxos(utxos, nil))
}

func credentialUtxo(
	txByte byte,
	payment, staking, datum []byte,
	tag uint8,
) models.Utxo {
	return models.Utxo{
		TxId:          bytes.Repeat([]byte{txByte}, 32),
		PaymentKey:    payment,
		StakingKey:    staking,
		DatumHash:     datum,
		CredentialTag: tag,
		AddedSlot:     uint64(txByte),
		Amount:        types.Uint64(1_000_000),
	}
}

func dictionaryRows(t *testing.T, store *Store, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, store.writeDB.QueryRow(query, args...).Scan(&n))
	return n
}

func TestUtxoAndAddressTransactionReferenceDictionariesByID(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	utxoColumns := tableColumnNames(t, store, "utxo")
	for _, column := range []string{"payment_key", "staking_key", "datum_hash"} {
		require.NotContains(t, utxoColumns, column,
			"utxo repeats %s by value on every row", column)
	}
	require.Contains(t, utxoColumns, "payment_key_id")
	require.Contains(t, utxoColumns, "staking_key_id")
	require.Contains(t, utxoColumns, "datum_id")
	require.Contains(t, utxoColumns, "credential_tag",
		"the credential kind stays on the row")
	addressColumns := tableColumnNames(t, store, "address_transaction")
	require.NotContains(t, addressColumns, "payment_key")
	require.NotContains(t, addressColumns, "staking_key")
	require.Contains(t, addressColumns, "payment_key_id")
	require.Contains(t, addressColumns, "staking_key_id")

	require.Equal(t,
		[]string{"credential_tag", "staking_key_id", "deleted_slot", "amount"},
		indexColumnNames(t, store, "idx_utxo_staking_deleted_amount"))
	require.Equal(t,
		[]string{"deleted_slot", "credential_tag", "staking_key_id", "amount"},
		indexColumnNames(t, store, "idx_utxo_deleted_staking_amount"))
	require.Equal(t, []string{"payment_key_id"},
		indexColumnNames(t, store, "idx_utxo_payment_key"))
	require.Equal(t, []string{"staking_key_id"},
		indexColumnNames(t, store, "idx_utxo_staking_key"))
	require.Equal(t,
		[]string{
			"credential_tag", "staking_key_id", "slot", "tx_index",
			"payment_key_id",
		},
		indexColumnNames(t, store, "idx_addr_tx_stake_position"))
	require.Equal(t, []string{"payment_key_id"},
		indexColumnNames(t, store, "idx_addr_tx_payment"))
}

// TestUtxoWritesHoldEachCredentialAndDatumOnce writes outputs that repeat the
// same hashes under both credential kinds and checks the dictionaries hold
// each hash once while every read returns the hashes written.
func TestUtxoWritesHoldEachCredentialAndDatumOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	payment, stake, datum := dictionaryHash(
		0xa1,
	), dictionaryHash(
		0xb1,
	), bytes.Repeat(
		[]byte{0xd1},
		32,
	)
	importCredentialUtxos(t, store,
		credentialUtxo(1, payment, stake, datum, 0),
		credentialUtxo(2, payment, stake, datum, 0),
		// The same stake hash under the script kind is one dictionary row.
		credentialUtxo(3, dictionaryHash(0xa2), stake, nil, 1),
		credentialUtxo(4, nil, nil, nil, 0),
	)

	require.Equal(
		t,
		3,
		dictionaryRows(t, store, `SELECT COUNT(*) FROM credential`),
		"payment a1, payment a2 and stake b1 once each",
	)
	require.Equal(t, 1, dictionaryRows(t, store, `SELECT COUNT(*) FROM datum`))
	require.Equal(t, 1, dictionaryRows(t, store,
		`SELECT COUNT(*) FROM datum WHERE LENGTH(raw_datum) = 0`),
		"a datum hash no transaction revealed is an unrevealed row")
	require.Equal(t, 3, dictionaryRows(t, store,
		`SELECT COUNT(*) FROM utxo WHERE staking_key_id = (
SELECT id FROM credential WHERE hash = ?)`, stake),
		"outputs under both kinds share the stake credential id")

	for _, want := range []struct {
		tx      byte
		payment []byte
		staking []byte
		datum   []byte
		tag     uint8
	}{
		{1, payment, stake, datum, 0},
		{3, dictionaryHash(0xa2), stake, nil, 1},
		{4, nil, nil, nil, 0},
	} {
		got, err := store.GetUtxo(bytes.Repeat([]byte{want.tx}, 32), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, string(want.payment), string(got.PaymentKey))
		require.Equal(t, string(want.staking), string(got.StakingKey))
		require.Equal(t, string(want.datum), string(got.DatumHash))
		require.Equal(t, want.tag, got.CredentialTag)
	}
}

// TestRolledBackUtxoWriteLeavesNoDictionaryRows pins the rollback rule: ids
// are resolved inside the writing transaction and never cached across it, so
// a rolled back write leaves no dictionary row, and a later write of the same
// hash creates it again.
func TestRolledBackUtxoWriteLeavesNoDictionaryRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	payment, stake, datum := dictionaryHash(
		0xa1,
	), dictionaryHash(
		0xb1,
	), bytes.Repeat(
		[]byte{0xd1},
		32,
	)
	utxo := credentialUtxo(1, payment, stake, datum, 0)
	txn := store.Transaction(t.Context())
	require.NoError(t, store.ImportUtxos([]models.Utxo{utxo}, txn))
	require.NoError(t, txn.Rollback())
	require.Zero(t, dictionaryRows(t, store, `SELECT COUNT(*) FROM credential`))
	require.Zero(t, dictionaryRows(t, store, `SELECT COUNT(*) FROM datum`))

	require.NoError(t, store.ImportUtxos([]models.Utxo{utxo}, nil))
	got, err := store.GetUtxo(utxo.TxId, 0, nil)
	require.NoError(t, err)
	require.Equal(t, payment, got.PaymentKey)
	require.Equal(t, stake, got.StakingKey)
	require.Equal(t, datum, got.DatumHash)
	require.Equal(
		t,
		2,
		dictionaryRows(t, store, `SELECT COUNT(*) FROM credential`),
	)
}

// TestUnrevealedDatumRowIsHiddenUntilRevealed pins the stub-row contract: an
// output naming a datum hash adds a datum row with an empty raw_datum, readers
// of datum see no such row, and the reveal fills the same row in without
// changing the id the output holds. A revealed datum is never replaced by a
// later write of the same hash.
func TestUnrevealedDatumRowIsHiddenUntilRevealed(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := bytes.Repeat([]byte{0xd1}, 32)
	var datumHash [32]byte
	copy(datumHash[:], hash)
	importCredentialUtxos(t, store,
		credentialUtxo(1, dictionaryHash(0xa1), nil, hash, 0))
	var stubID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM datum WHERE hash = ?`, hash).Scan(&stubID))

	got, err := store.GetDatum(datumHash, nil)
	require.NoError(t, err)
	require.Nil(t, got, "an unrevealed datum is not a stored datum")

	// The batch writer reveals it, repeating the hash within one statement.
	var rows rowBatch
	raw := []byte{0x82, 0x01, 0x02}
	rows.add(datumShape, hash, raw, int64(50))
	rows.add(datumShape, hash, []byte{0x01}, int64(51))
	other := bytes.Repeat([]byte{0xd2}, 32)
	rows.add(datumShape, other, []byte{0x03}, int64(52))
	require.NoError(t, rows.flush(
		context.Background(), store.writeDB, store,
		store.dialect.ParameterLimit(),
	))

	var gotRaw []byte
	var gotSlot, gotID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT raw_datum, added_slot, id FROM datum WHERE hash = ?`, hash,
	).Scan(&gotRaw, &gotSlot, &gotID))
	require.Equal(t, raw, gotRaw, "the first queued reveal fills the row")
	require.Equal(t, int64(50), gotSlot)
	require.Equal(t, stubID, gotID, "the reveal keeps the id the output holds")
	revealed, err := store.GetDatum(datumHash, nil)
	require.NoError(t, err)
	require.NotNil(t, revealed)
	require.Equal(t, raw, revealed.RawDatum)
	require.Equal(t, uint64(50), revealed.AddedSlot)
	require.Equal(t, 2, dictionaryRows(t, store, `SELECT COUNT(*) FROM datum`))

	// A later write of the same hash leaves the revealed datum alone.
	var again rowBatch
	again.add(datumShape, hash, []byte{0xff}, int64(99))
	require.NoError(t, again.flush(
		context.Background(), store.writeDB, store,
		store.dialect.ParameterLimit(),
	))
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT raw_datum, added_slot FROM datum WHERE hash = ?`, hash,
	).Scan(&gotRaw, &gotSlot))
	require.Equal(t, raw, gotRaw)
	require.Equal(t, int64(50), gotSlot)

	utxo, err := store.GetUtxo(bytes.Repeat([]byte{1}, 32), 0, nil)
	require.NoError(t, err)
	require.Equal(t, hash, utxo.DatumHash)
}

func TestSetDatumFillsAnUnrevealedRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := bytes.Repeat([]byte{0xd3}, 32)
	var datumHash [32]byte
	copy(datumHash[:], hash)
	importCredentialUtxos(t, store, credentialUtxo(1, nil, nil, hash, 0))
	require.NoError(t, store.SetDatum(datumHash, []byte{0x05}, 77, nil))
	got, err := store.GetDatum(datumHash, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, []byte{0x05}, got.RawDatum)
	require.Equal(t, uint64(77), got.AddedSlot)
	require.Equal(t, 1, dictionaryRows(t, store, `SELECT COUNT(*) FROM datum`))
}

// seedLiveStakeCredentials inserts credentials in descending hash order, so
// dictionary ids run opposite to hash order, then one live output under each
// (tag, hash) of keys.
func seedLiveStakeCredentials(
	t *testing.T,
	store *Store,
	keys []stakeKeyBound,
) {
	t.Helper()
	hashes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		hashes = append(hashes, key.key)
	}
	slices.SortFunc(
		hashes,
		func(a, b []byte) int { return bytes.Compare(b, a) },
	)
	for _, hash := range hashes {
		testutil.CredentialID(t, store.writeDB, hash)
	}
	for i, key := range keys {
		_, err := store.writeDB.Exec(`
INSERT INTO utxo (tx_id, output_idx, staking_key_id, credential_tag,
    added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, 1, 0, ?)`,
			testutil.TransactionID(t, store.writeDB, []byte{byte(i + 1)}),
			testutil.CredentialID(t, store.writeDB, key.key),
			int64(key.tag), fmt.Sprint(10*(i+1)))
		require.NoError(t, err)
	}
}

// TestLiveStakeKeysAreReadInHashOrderNotDictionaryOrder pins the contract of
// the credential-driven live stake key source: batches are ranges of
// (credential_tag, hash), however the ids were issued. The credentials are
// issued in descending hash order, so a reader that followed ids would split
// the key space differently.
func TestLiveStakeKeysAreReadInHashOrderNotDictionaryOrder(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	keys := []stakeKeyBound{
		{0, dictionaryHash(0x30)}, {0, dictionaryHash(0x10)},
		{1, dictionaryHash(0x20)}, {0, dictionaryHash(0x20)},
		{1, dictionaryHash(0x40)}, {1, dictionaryHash(0x10)},
	}
	seedLiveStakeCredentials(t, store, keys)
	// A spent output holds no live stake.
	_, err := store.writeDB.Exec(`
INSERT INTO utxo (tx_id, output_idx, staking_key_id, credential_tag,
    added_slot, deleted_slot, amount)
VALUES (?, 0, ?, 0, 1, 9, '5')`,
		testutil.TransactionID(t, store.writeDB, []byte{0x77}),
		testutil.CredentialID(t, store.writeDB, dictionaryHash(0x05)))
	require.NoError(t, err)

	want := slices.Clone(keys)
	slices.SortFunc(want, func(a, b stakeKeyBound) int {
		if a.tag != b.tag {
			return int(a.tag) - int(b.tag)
		}
		return bytes.Compare(a.key, b.key)
	})
	ctx := context.Background()

	count, err := countRewardLiveStakeKeys(ctx, store.writeDB)
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), count)

	// Walking the batch ends of every size visits the keys in order.
	for batch := 1; batch <= len(want)+1; batch++ {
		var ends []stakeKeyBound
		var lo *stakeKeyBound
		// A reader that does not advance past its lower bound must fail the
		// test rather than loop.
		for range len(want) + 1 {
			end, err := nextRewardLiveStakeBatchEnd(
				ctx,
				store.writeDB,
				lo,
				batch,
			)
			require.NoError(t, err)
			if end == nil {
				break
			}
			ends = append(ends, *end)
			lo = end
		}
		require.LessOrEqual(t, len(ends), len(want), "batch size %d", batch)
		var wantEnds []stakeKeyBound
		for i := batch - 1; i < len(want); i += batch {
			wantEnds = append(wantEnds, want[i])
		}
		require.Equal(t, wantEnds, ends, "batch size %d", batch)
	}

	// Each batch range counts exactly its own keys.
	lo := want[1]
	hi := want[3]
	inRange, err := countRewardLiveStakeKeysInRange(
		ctx, store.writeDB, stakeKeyRange{lo: &lo, hi: &hi})
	require.NoError(t, err)
	require.Equal(t, int64(2), inRange, "(lo, hi] holds want[2] and want[3]")

	sums, err := sumRewardLiveStakeUtxos(
		ctx, store.writeDB, stakeKeyRange{lo: &lo, hi: &hi})
	require.NoError(t, err)
	got := sums
	wantSums := map[string]uint64{}
	for i, key := range keys {
		if bytes.Compare(
			append([]byte{key.tag}, key.key...),
			append([]byte{lo.tag}, lo.key...),
		) > 0 &&
			bytes.Compare(append([]byte{key.tag}, key.key...), append([]byte{hi.tag}, hi.key...)) <= 0 {
			wantSums[rewardLiveStakeKey(key.tag, key.key)] = uint64(
				10 * (i + 1),
			)
		}
	}
	require.Equal(t, wantSums, got)
}

// TestStakeKeyRangeBoundsApplyPerCredentialTag pins how a (tag, hash) range
// splits over the per-tag dictionary scans.
func TestStakeKeyRangeBoundsApplyPerCredentialTag(t *testing.T) {
	t.Parallel()
	lo := &stakeKeyBound{tag: 0, key: []byte{0x10}}
	hi := &stakeKeyBound{tag: 1, key: []byte{0x20}}
	for _, tc := range []struct {
		name      string
		keys      stakeKeyRange
		tag       uint8
		ok        bool
		condition string
		args      int
	}{
		{"unbounded", stakeKeyRange{}, 0, true, "1 = 1", 0},
		{"lower bound on its own tag", stakeKeyRange{lo: lo}, 0, true, "c.hash > ?", 1},
		{"lower bound below later tag", stakeKeyRange{lo: lo}, 1, true, "1 = 1", 0},
		{"lower bound above earlier tag", stakeKeyRange{lo: hi}, 0, false, "", 0},
		{"upper bound on its own tag", stakeKeyRange{hi: hi}, 1, true, "c.hash <= ?", 1},
		{"upper bound above earlier tag", stakeKeyRange{hi: hi}, 0, true, "1 = 1", 0},
		{"upper bound below later tag", stakeKeyRange{hi: lo}, 1, false, "", 0},
		{"both bounds", stakeKeyRange{lo: lo, hi: &stakeKeyBound{tag: 0, key: []byte{0x30}}}, 0, true, "c.hash > ? AND c.hash <= ?", 2},
	} {
		condition, args, ok := tc.keys.hashBounds(tc.tag, "c.hash")
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.condition, condition, tc.name)
		require.Len(t, args, tc.args, tc.name)
	}
}

// TestCredentialQueriesUseDictionaryIndexes pins the plans of the readers that
// reach utxo and address_transaction through the credential dictionary: the
// hash is resolved once through the dictionary's unique index and the rows are
// read through the id indexes.
func TestCredentialQueriesUseDictionaryIndexes(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := dictionaryHash(0x11)
	for name, tc := range map[string]struct {
		query string
		args  []any
		want  []string
	}{
		"live stake sum": {
			sumCredentialUtxoStakeQuery, []any{0, hash},
			[]string{"idx_utxo_staking_deleted_amount", "staking_key_id=?"},
		},
		"payment credential lookup": {
			"SELECT COUNT(*) FROM utxo WHERE payment_key_id = " +
				credentialIDSubquery, []any{hash},
			[]string{"idx_utxo_payment_key"},
		},
		"address history by stake credential": {
			func() string {
				query, _ := addressTransactionRangeQuery(0, hash, nil, nil)
				return query + " ORDER BY at.slot DESC, at.tx_index DESC, at.payment_key_id DESC"
			}(), []any{0, hash},
			[]string{"idx_addr_tx_stake_position"},
		},
		"payment credential transaction count": {
			countTransactionsByPaymentCredSQL, []any{hash, false},
			[]string{"idx_addr_tx_payment"},
		},
	} {
		plan := queryPlan(t, store.writeDB, tc.query, tc.args...)
		for _, fragment := range tc.want {
			require.Contains(t, plan, fragment, "%s:\n%s", name, plan)
		}
		require.NotContains(t, plan, "SCAN utxo", "%s:\n%s", name, plan)
		require.NotContains(
			t,
			plan,
			"SCAN address_transaction",
			"%s:\n%s",
			name,
			plan,
		)
	}

	// The live key source reads a range of the dictionary's unique hash index
	// and probes utxo by the credential id.
	arms, armArgs := stakeKeyRange{lo: &stakeKeyBound{tag: 0, key: hash}}.
		liveUtxoArms("{tag} AS credential_tag, c.hash", false)
	require.Len(t, arms, 2)
	for i, arm := range arms {
		args := armArgs[:strings.Count(arm, "?")]
		armArgs = armArgs[len(args):]
		plan := queryPlan(t, store.writeDB, arm, args...)
		if i == 0 {
			require.Contains(t, plan, "sqlite_autoindex_credential_1", plan)
		}
		require.Contains(t, plan, "idx_utxo_staking_deleted_amount", plan)
		require.NotContains(t, plan, "SCAN utxo", plan)
	}
}
