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

package utxoset_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// A set that first loads state from a metadata store holding a reward journal
// takes the journal's credits and withdrawals, in the order the store wrote
// them, with the amounts and flags it recorded.
func TestSetLoadsTheMetadataStoresRewardJournal(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 8)
	db := ledgerAt(t, slotPoint(8))
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, '30', 20, 0, 0), (?, 0, ?, '10', 5, 1, 0), (?, 0, ?, '6', 20, 0, 1)`,
		[]byte{1}, []byte{0xa1}, []byte{1}, []byte{0xa2}, []byte{1}, []byte{0xa3},
	)
	require.NoError(t, err)

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))

	lost, err := set.RewardRecordsLost()
	require.NoError(t, err)
	require.False(t, lost)
	recs, err := set.RewardRecordsIn(0, 100)
	require.NoError(t, err)
	require.Len(t, recs, 3)
	require.Equal(t, []byte{0xa1}, recs[0].Source)
	require.Equal(t, uint64(30), recs[0].Amount)
	require.Equal(t, []byte{0xa2}, recs[1].Source)
	require.True(t, recs[1].Withdrawal)
	require.Equal(t, uint64(10), recs[1].Amount)
	require.Equal(t, []byte{0xa3}, recs[2].Source)
	require.True(t, recs[2].PostSnapshot)
	high, err := set.RewardRecordHigh()
	require.NoError(t, err)
	require.Equal(t, uint64(3), high)
}
