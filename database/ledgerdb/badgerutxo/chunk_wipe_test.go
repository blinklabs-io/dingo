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

const wipeOutputs = 12000

func wipeRow(epoch uint64) badgerutxo.RowPut {
	return badgerutxo.RowPut{
		WipeRewardEpoch: true, Key: binary.BigEndian.AppendUint64(nil, epoch),
	}
}

// seedRewardEpoch writes n outputs of epoch through batches that fit one
// transaction, at slots from slot up, and returns the next free slot.
func seedRewardEpoch(
	t *testing.T,
	s *badgerutxo.Store,
	epoch uint64,
	n int,
	slot uint64,
) uint64 {
	t.Helper()
	const perBatch = 500
	for first := 0; first < n; first += perBatch {
		var refs []badgerutxo.RewardOutputKey
		for i := first; i < min(first+perBatch, n); i++ {
			ref := rewardRef(epoch, 0x11, "member")
			ref.Key = binary.BigEndian.AppendUint32(ref.Key[:24], uint32(i)) //nolint:gosec // a test index
			refs = append(refs, ref)
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
			Rows: rewardPuts(slot, refs...),
		})
		require.NoError(t, err)
		slot++
	}
	return slot
}

func countRewardRows(t *testing.T, s *badgerutxo.Store, epoch uint64) (primary, index int) {
	t.Helper()
	require.NoError(t, s.Rows(
		badgerutxo.RewardEpochPrefix(epoch), func(_, _ []byte) error {
			index++
			return nil
		},
	))
	require.NoError(t, s.Rows([]byte{badgerutxo.RewardOutputTag}, func(k, _ []byte) error {
		ref, err := badgerutxo.DecodeRewardOutputRowKey(k)
		require.NoError(t, err)
		if ref.Epoch == epoch {
			primary++
		}
		return nil
	}))
	return primary, index
}

// A reward epoch wipe deletes the output and index rows of every output the
// epoch holds, which is more than one transaction takes. A batch of nothing
// else has no tip, no change and no slot to mark, and still applies.
func TestRewardEpochWipeBeyondOneTransactionCommits(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := seedRewardEpoch(t, s, 5, wipeOutputs, 2)
	slot = seedRewardEpoch(t, s, 6, 10, slot)
	primary, index := countRewardRows(t, s, 5)
	require.Equal(t, wipeOutputs, primary)
	require.Equal(t, wipeOutputs, index)

	_, err := s.ApplyLedgerBatch(nil, badgerutxo.LedgerBatch{
		Rows: []badgerutxo.RowPut{wipeRow(5)},
	})
	require.NoError(t, err)
	primary, index = countRewardRows(t, s, 5)
	require.Zero(t, primary)
	require.Zero(t, index)
	primary, index = countRewardRows(t, s, 6)
	require.Equal(t, 10, primary, "another epoch's rows stay")
	require.Equal(t, 10, index)
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed)
}

// A wipe that stopped between its transactions leaves some of the epoch's
// rows; the batch applied again removes the rest and writes the puts that
// follow it.
func TestRewardEpochWipeStoppedBetweenTransactionsIsFinishedByTheReplay(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := seedRewardEpoch(t, s, 5, wipeOutputs, 2)
	puts := rewardRef(5, 0x77, "leader")
	batch := func() badgerutxo.LedgerBatch {
		return badgerutxo.LedgerBatch{
			Rows: append([]badgerutxo.RowPut{wipeRow(5)}, rewardPuts(slot, puts)...),
		}
	}
	stop := errors.New("stopped between transactions")
	// The first transaction holds only the batch's rollback, so the stop
	// comes after the second, the first that deletes rows.
	s.SetAfterChunkForTest(func(done int) error {
		if done >= 2 {
			return stop
		}
		return nil
	})
	tip := point(slot, byte(slot))
	_, err := s.ApplyLedgerBatch(&tip, batch())
	require.ErrorIs(t, err, stop)
	partial, partialIndex := countRewardRows(t, s, 5)
	require.Positive(t, partial)
	require.Less(t, partial, wipeOutputs, "part of the wipe committed")
	require.Equal(t, partial, partialIndex)

	s.SetAfterChunkForTest(nil)
	require.NoError(t, s.ClearFailed())
	_, err = s.ApplyLedgerBatch(&tip, batch())
	require.NoError(t, err)
	primary, index := countRewardRows(t, s, 5)
	require.Equal(t, 1, primary, "only the batch's own put is left")
	require.Equal(t, 1, index)
	requireTip(t, s, tip)
}

// A batch's rollback runs ahead of its wipe, so an output row the rollback
// restores to its earlier value is removed with the rest of the epoch.
func TestRewardEpochWipeFollowsTheBatchsRollback(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := seedRewardEpoch(t, s, 5, wipeOutputs, 2)
	first := rewardRef(5, 0x11, "member")
	first.Key = binary.BigEndian.AppendUint32(first.Key[:24], 0)
	tip := point(slot+100, 0x64)
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: []badgerutxo.RowPut{
			{
				Slot: tip.Slot, Key: badgerutxo.RewardOutputRowKey(first),
				Value: badgerutxo.RewardOutputValue(9, 3, 4),
			},
		},
	})
	require.NoError(t, err)

	to := slot + 50
	back := point(to, 0x32)
	_, err = s.ApplyLedgerBatch(&back, badgerutxo.LedgerBatch{
		DeleteNoncesAfter: &to,
		Rows:              []badgerutxo.RowPut{wipeRow(5)},
	})
	require.NoError(t, err)
	primary, index := countRewardRows(t, s, 5)
	require.Zero(t, primary, "the row the rollback restored is wiped")
	require.Zero(t, index)
}

// A wipe's key is the epoch as 8 bytes; any other key is rejected before the
// batch writes anything.
func TestRewardEpochWipeWithoutAnEightByteEpochIsRejected(t *testing.T) {
	t.Parallel()
	s := smallTxnStore(t)
	bootstrap(t, s, point(1, 0x01))
	slot := seedRewardEpoch(t, s, 5, 10, 2)
	tip := point(slot, byte(slot))
	_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Rows: []badgerutxo.RowPut{
			{WipeRewardEpoch: true, Key: []byte{0, 0, 0, 5}},
		},
	})
	require.Error(t, err)
	primary, index := countRewardRows(t, s, 5)
	require.Equal(t, 10, primary)
	require.Equal(t, 10, index)
	_, failed, err := s.Failed()
	require.NoError(t, err)
	require.False(t, failed)
}
