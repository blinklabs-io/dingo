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

package sqlite

import (
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestDeleteAccountRewardJournalForCredentialsAfterSlotScope checks that the
// scoped journal delete removes exactly the named credentials' rows above the
// slot from both the reward journal and account_withdrawal_witness, across
// more credentials than one parameter chunk holds, and leaves rows at
// or below the slot (including rows at exactly the slot), rows of unnamed
// credentials, and rows sharing a named staking key under the other
// credential tag untouched.
func TestDeleteAccountRewardJournalForCredentialsAfterSlotScope(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStoreWithParameterLimit(t, 5)

	const (
		slot      = uint64(100)
		belowSlot = int64(50)
		aboveSlot = int64(150)
		// More than the parameter limit minus the two reserved parameters,
		// so the key-hash credentials span several chunks.
		coveredKeyHashes = 8
		coveredScripts   = 5
		uncovered        = 7
	)
	key := func(prefix byte, i int) []byte {
		k := make([]byte, 28)
		k[0] = prefix
		binary.BigEndian.PutUint32(k[1:], uint32(i))
		return k
	}
	seed := func(tag uint8, stakingKey []byte) {
		for _, s := range []int64{belowSlot, int64(slot), aboveSlot} {
			txHash := append([]byte{byte(s)}, stakingKey...)
			require.NoError(t, insertRewardJournalRow(
				raw, stakingKey, tag, txHash, "10", "", s, false, false,
			))
			_, err := raw.Exec(
				`INSERT INTO account_withdrawal_witness (
    staking_key, credential_tag, tx_hash, added_slot
) VALUES (?, ?, ?, ?)`,
				stakingKey, tag, txHash, s,
			)
			require.NoError(t, err)
		}
	}

	refs := make([]models.StakeCredentialRef, 0, coveredKeyHashes+coveredScripts)
	for i := range coveredKeyHashes {
		k := key(0xa0, i)
		seed(0, k)
		refs = append(refs, models.NewStakeCredentialRef(0, k))
	}
	for i := range coveredScripts {
		k := key(0xb0, i)
		seed(1, k)
		refs = append(refs, models.NewStakeCredentialRef(1, k))
	}
	for i := range uncovered {
		seed(0, key(0xc0, i))
	}
	// The same staking key as a covered key-hash credential, under the
	// script tag: a different credential that must survive.
	seed(1, key(0xa0, 0))
	// The same staking key as a covered script credential, under the
	// key-hash tag.
	seed(0, key(0xb0, 0))

	require.NoError(t, store.DeleteAccountRewardJournalForCredentialsAfterSlot(
		slot, refs, nil,
	))

	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, raw.QueryRow(query, args...).Scan(&n))
		return n
	}
	survivorsAbove := uncovered + 2
	totalCredentials := coveredKeyHashes + coveredScripts + uncovered + 2
	for _, journal := range []struct{ table, slotColumn string }{
		{"reward_event_credit", "slot"},
		{"account_withdrawal_witness", "added_slot"},
	} {
		table := journal.table
		column := journal.slotColumn
		require.Equal(t, survivorsAbove, count(
			"SELECT COUNT(*) FROM "+table+" WHERE "+column+" > ?", slot,
		), "%s: only unnamed credentials keep rows above the slot", table)
		require.Equal(t, 2*totalCredentials, count(
			"SELECT COUNT(*) FROM "+table+" WHERE "+column+" <= ?", slot,
		), "%s: rows at or below the slot are never deleted", table)
		require.Equal(t, totalCredentials, count(
			"SELECT COUNT(*) FROM "+table+" WHERE "+column+" = ?", slot,
		), "%s: rows at exactly the slot are never deleted", table)
		require.Equal(t, 1, count(
			journalCountForCredential(table, column, 1),
			slot, key(0xa0, 0),
		), "%s: same key under the other tag must survive", table)
		require.Equal(t, 1, count(
			journalCountForCredential(table, column, 0),
			slot, key(0xb0, 0),
		), "%s: same key under the other tag must survive", table)
	}

	// An empty credential list deletes nothing.
	require.NoError(t, store.DeleteAccountRewardJournalForCredentialsAfterSlot(
		slot, nil, nil,
	))
	require.Equal(t, survivorsAbove, count(
		"SELECT COUNT(*) FROM reward_event_credit WHERE slot > ?", slot,
	))
}

// journalCountForCredential counts a table's rows above a slot for one
// credential. The event table keys its rows by account id; the witness table
// carries the credential itself.
func journalCountForCredential(table, column string, tag int) string {
	if table == "reward_event_credit" {
		return "SELECT COUNT(*) FROM reward_event_credit e" +
			" JOIN account a ON a.id = e.account_id" +
			" WHERE e." + column + " > ? AND a.credential_tag = " +
			string(rune('0'+tag)) + " AND a.staking_key = ?"
	}
	return "SELECT COUNT(*) FROM " + table +
		" WHERE " + column + " > ? AND credential_tag = " +
		string(rune('0'+tag)) + " AND staking_key = ?"
}
