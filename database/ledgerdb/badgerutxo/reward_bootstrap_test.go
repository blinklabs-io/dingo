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

package badgerutxo_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// A full bootstrap restarts the reward record numbering, so it must also
// restart the consumer's acknowledged position: a mark left from before would
// make the new, lower-numbered records look copied and let a prune drop them.
func TestFullBootstrapRestartsTheRewardConsumerPosition(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	require.NoError(t, s.SetRewardConsumer(true))
	tip := point(100, 0x64)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: []badgerutxo.RewardRecord{
			{Slot: 10, Source: []byte{1}, Key: []byte{1}, Amount: 1},
			{Slot: 20, Source: []byte{2}, Key: []byte{1}, Amount: 2},
		},
	})
	require.NoError(t, err)
	require.NoError(t, s.AckRewardRecords(2, 1000))

	bootstrap(t, s, point(200, 0xc8))
	tip = point(300, 0x2c)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: []badgerutxo.RewardRecord{
			{Slot: 210, Source: []byte{3}, Key: []byte{1}, Amount: 3},
		},
	})
	require.NoError(t, err)
	require.NoError(t, s.PruneJournal(250))

	recs, err := s.RewardRecordsIn(0, 1000)
	require.NoError(t, err)
	require.Len(t, recs, 1,
		"the record after the bootstrap is not yet copied and stays")
	_, held, err := s.RewardConsumedSlot()
	require.NoError(t, err)
	require.True(t, held, "the consumer stays registered")
}
