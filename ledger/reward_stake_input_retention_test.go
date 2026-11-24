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

package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPendingRewardInputWriteRejectsMalformedRecord(t *testing.T) {
	t.Parallel()
	ls, _ := newRewardCalculationTestLedger(t)
	ldb := ls.ledgerDB()
	valid, err := FormatRewardStakeInputsPending(
		[]RewardStakeInputsPendingEntry{{Epoch: 1, BoundarySlot: 100}},
	)
	require.NoError(t, err)
	for _, invalid := range []string{"{", "null", "{}", `{"entries":{}}`, `{"entries":[],"unknown":1}`} {
		t.Run(invalid, func(t *testing.T) {
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(
				t,
				ldb.SetSyncState(rewardStakeInputsPendingKey, valid, txn),
			)
			require.Error(
				t,
				ldb.SetSyncState(rewardStakeInputsPendingKey, invalid, txn),
			)
			got, err := ldb.GetSyncState(rewardStakeInputsPendingKey, txn)
			require.NoError(t, err)
			require.Equal(
				t,
				valid,
				got,
				"rejected pending record must not replace the staged one",
			)
		})
	}
}
