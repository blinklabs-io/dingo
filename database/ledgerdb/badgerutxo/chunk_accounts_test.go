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
	"encoding/binary"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func manyAccounts(slot uint64, n int, reward uint64) []badgerutxo.AccountPut {
	puts := make([]badgerutxo.AccountPut, 0, n)
	for i := range n {
		key := binary.BigEndian.AppendUint32(make([]byte, 24), uint32(i)) //nolint:gosec // a test index
		puts = append(puts, badgerutxo.AccountPut{Slot: slot, Row: badgerutxo.AccountRow{
			Tag: 0, Key: key, Pool: []byte{1}, Active: true, Reward: reward,
		}})
	}
	return puts
}

func manyRewardRecords(slot uint64, n int) []badgerutxo.RewardRecord {
	recs := make([]badgerutxo.RewardRecord, 0, n)
	for i := range n {
		recs = append(recs, badgerutxo.RewardRecord{
			Slot: slot, Source: []byte{1},
			Key: binary.BigEndian.AppendUint32(make([]byte, 24), uint32(i)), //nolint:gosec // a test index
		})
	}
	return recs
}

func countAccounts(t *testing.T, s *badgerutxo.Store) int {
	t.Helper()
	n := 0
	require.NoError(t, s.Accounts(func(badgerutxo.AccountRow) error {
		n++
		return nil
	}))
	return n
}

// A batch with more account puts than one transaction holds, as the one-time
// expiration stamp of every active account or the reap of a large pool
// stages, applies in as many as it takes, and one rollback undoes them all.
func TestApplyTakesMoreAccountPutsThanOneTransactionHolds(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	start := point(0, 0)
	bootstrap(t, s, start)
	tip := point(20, 0x14)
	const n = 120_000
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Accounts:      manyAccounts(tip.Slot, n, 1),
		RewardRecords: manyRewardRecords(tip.Slot, n),
	})
	require.NoError(t, err)
	requireTip(t, s, tip)
	require.Equal(t, n, countAccounts(t, s))
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed, "the last transaction clears the mark")

	_, err = s.Rollback(start)
	require.NoError(t, err)
	require.Zero(t, countAccounts(t, s))
}

// A chunked batch of account puts and reward records that stops between its
// transactions leaves the failure mark and a tip behind it, and the rollback
// the mark triggers removes what landed.
func TestAccountChunksStoppedBetweenTransactionsAreUndoneByTheMark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := smallRollbackStore(t, dir)
	mid, tip := point(10, 0x0a), point(20, 0x14)
	bootstrap(t, s, point(0, 0))
	_, err := s.ApplyLedgerBatch(&mid, badgerutxo.LedgerBatch{
		Accounts: manyAccounts(mid.Slot, 30, 7),
	})
	require.NoError(t, err)
	var chunks int
	stop := errors.New("stopped between chunks")
	s.SetAfterChunkForTest(func(done int) error {
		chunks = done
		if done == 3 {
			return stop
		}
		return nil
	})
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Accounts:      manyAccounts(tip.Slot, 400, 9),
		RewardRecords: manyRewardRecords(tip.Slot, 100),
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 3, chunks)
	require.NoError(t, s.Close())

	s = open(t, dir)
	defer s.Close()
	requireTip(t, s, mid)
	slot, failed, err := s.Failed()
	require.NoError(t, err)
	require.True(t, failed)
	require.Equal(t, tip.Slot, slot)
	require.Greater(t, countAccounts(t, s), 30, "chunks landed")

	_, err = s.Rollback(mid)
	require.NoError(t, err)
	require.Equal(t, 30, countAccounts(t, s))
	a, ok, err := s.Account(0, binary.BigEndian.AppendUint32(make([]byte, 24), 3))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(7), a.Reward)
}
