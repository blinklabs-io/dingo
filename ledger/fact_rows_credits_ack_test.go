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

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// retainerSet is a live UTxO set stand-in that only keeps the reward
// acknowledgement a reader gives it.
type retainerSet struct {
	UtxoSetWriter
	high      uint64
	ackedSeq  uint64
	ackedSlot uint64
	acks      int
}

func (r *retainerSet) RewardRecordHigh() (uint64, error) { return r.high, nil }

func (r *retainerSet) AckRewardRecords(seq, slot uint64) error {
	r.acks++
	r.ackedSeq, r.ackedSlot = seq, slot
	return nil
}

func (r *retainerSet) RewardConsumedSlot() (uint64, bool, error) {
	return r.ackedSlot, true, nil
}

func (r *retainerSet) RewardRecordsLost() (bool, error) { return false, nil }

// A consumer registered on the set is acknowledged whether or not the accounts
// family is drained: the set retains records for every registered consumer,
// and an acknowledgement that never reached it would keep them for good.
func TestRewardJournalAcknowledgementReachesTheSetWithUndrainedAccounts(
	t *testing.T,
) {
	t.Parallel()
	for name, drained := range map[string]bool{
		"accounts undrained": false,
		"accounts drained":   true,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			set := &retainerSet{high: 42}
			ls := &LedgerState{config: LedgerStateConfig{UtxoSet: set}}
			if drained {
				ls.SetDrainedFamilies(
					nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
				)
			}
			high, err := ls.RewardJournalHigh()
			require.NoError(t, err)
			require.Equal(t, uint64(42), high)
			require.NoError(t, ls.AckRewardJournal(high, 900))
			require.Equal(t, 1, set.acks, "the set was acknowledged")
			require.Equal(t, uint64(42), set.ackedSeq)
			require.Equal(t, uint64(900), set.ackedSlot)
		})
	}
}
