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

func creditsAt(slot uint64, n int, firstSource int) []badgerutxo.RewardRecord {
	out := make([]badgerutxo.RewardRecord, 0, n)
	for i := range n {
		src := firstSource + i
		out = append(out, badgerutxo.RewardRecord{
			Slot: slot, Source: []byte{byte(src >> 8), byte(src)},
			Key: []byte{1}, Amount: uint64(src + 1), //nolint:gosec // small test value
		})
	}
	return out
}

// A page of credits costs the entries it returns, not the records before its
// cursor: resuming deep into a large boundary visits about one page of the
// sequence index.
func TestRewardCreditPagesSeekBySequence(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	tip := point(100, 0x64)
	const total, limit = 3000, 100
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: creditsAt(50, total, 0),
	})
	require.NoError(t, err)
	// A withdrawal in the same range is never paged.
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: []badgerutxo.RewardRecord{
			{Slot: 50, Withdrawal: true, Source: []byte{9}, Key: []byte{1}},
		},
	})
	require.NoError(t, err)

	var got []uint64
	cursor := uint64(0)
	var deepVisited uint64
	for {
		before := s.RewardIndexVisited()
		recs, more, err := s.RewardCreditsAfter(0, 1000, cursor, limit)
		require.NoError(t, err)
		if cursor >= total-2*limit {
			deepVisited = max(deepVisited, s.RewardIndexVisited()-before)
		}
		for _, r := range recs {
			got = append(got, r.Seq)
		}
		if !more {
			break
		}
		cursor = recs[len(recs)-1].Seq
	}
	require.Len(t, got, total)
	for i, seq := range got {
		require.Equal(t, uint64(i), seq) //nolint:gosec // index
	}
	require.LessOrEqual(t, deepVisited, uint64(limit+1),
		"a page late in the sequence visits one page of the index")
}

// A store from before sequences keeps working: opening it numbers each record
// in slot order and indexes it, and the numbering continues from there.
func TestOpeningALegacyStoreNumbersAndIndexesItsRewardRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir)
	bootstrap(t, s, point(1, 0x01))
	require.NoError(t, s.WriteLegacyRewardRecordsForTest(
		append(creditsAt(10, 3, 0), creditsAt(20, 2, 10)...),
	))
	require.NoError(t, s.Close())

	s = open(t, dir)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	recs, more, err := s.RewardCreditsAfter(0, 100, 0, 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, recs, 5)
	for i, r := range recs {
		require.Equal(t, uint64(i), r.Seq) //nolint:gosec // index
	}
	high, err := s.RewardRecordHigh()
	require.NoError(t, err)
	require.Equal(t, uint64(5), high)

	tip := point(100, 0x64)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		RewardRecords: creditsAt(30, 1, 20),
	})
	require.NoError(t, err)
	recs, _, err = s.RewardCreditsAfter(0, 100, 4, 100)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	require.Equal(t, uint64(5), recs[0].Seq)
}

// Pruned and rolled back records leave no index entries behind to be skipped.
func TestRewardSequenceIndexFollowsPruneAndRollback(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	for _, slot := range []uint64{10, 20, 30} {
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			RewardRecords: creditsAt(slot, 2, int(slot)),
		})
		require.NoError(t, err)
	}
	visited := func() uint64 {
		before := s.RewardIndexVisited()
		_, _, err := s.RewardCreditsAfter(0, 1000, 0, 1000)
		require.NoError(t, err)
		return s.RewardIndexVisited() - before
	}
	require.Equal(t, uint64(6), visited())
	require.NoError(t, s.PruneJournal(15))
	require.Equal(t, uint64(4), visited(), "the pruned records' entries go")
	_, err := s.Rollback(point(20, 20))
	require.NoError(t, err)
	require.Equal(t, uint64(2), visited(), "the rolled back records' entries go")
}
