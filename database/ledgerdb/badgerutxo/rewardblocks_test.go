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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

type blockRows struct {
	total, next uint64
	hasTotal    bool
	fees        uint64
	hasFees     bool
	pools       map[string]uint64
}

func readBlockRows(
	t *testing.T,
	s *badgerutxo.Store,
	epoch uint64,
	asOf *uint64,
) blockRows {
	t.Helper()
	out := blockRows{pools: map[string]uint64{}}
	fn := func(k, v []byte) error {
		_, kind, pool, err := badgerutxo.DecodeRewardBlockKey(k)
		require.NoError(t, err)
		switch kind {
		case badgerutxo.RewardBlockPoolCount:
			n, err := badgerutxo.DecodeRewardBlockCount(v)
			require.NoError(t, err)
			out.pools[string(pool)] = n
		case badgerutxo.RewardBlockEpochTotal:
			out.hasTotal = true
			out.total, out.next, err = badgerutxo.DecodeRewardBlockTotal(v)
			require.NoError(t, err)
		case badgerutxo.RewardBlockEpochFees:
			out.hasFees = true
			out.fees, err = badgerutxo.DecodeRewardBlockCount(v)
			require.NoError(t, err)
		}
		return nil
	}
	prefix := badgerutxo.RewardBlocksEpochPrefix(epoch)
	if asOf == nil {
		require.NoError(t, s.Rows(prefix, fn))
	} else {
		require.NoError(t, s.RowsAt(prefix, *asOf, fn))
	}
	return out
}

func applyBlockOps(
	t *testing.T,
	s *badgerutxo.Store,
	slot uint64,
	ops ...badgerutxo.RewardBlockOp,
) {
	t.Helper()
	tip := point(slot, byte(slot))
	_, err := s.ApplyLedgerBatch(
		&tip, badgerutxo.LedgerBatch{RewardBlocks: ops},
	)
	require.NoError(t, err)
}

// Blocks count once per slot, an epoch without a total ignores them, fees
// need a fees row, and a rollback past a block undoes its count, total and
// fees in the order they were made, on both rollback paths.
func TestRewardBlockOpsCountOncePerSlotAndRollBack(t *testing.T) {
	t.Parallel()
	poolA := bytes.Repeat([]byte{0xa1}, 28)
	poolB := bytes.Repeat([]byte{0xb2}, 28)
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		// An unknown epoch takes neither blocks nor fees.
		applyBlockOps(t, s, 5,
			badgerutxo.RewardBlockOp{Slot: 5, Epoch: 3, Pool: poolA},
			badgerutxo.RewardBlockOp{Slot: 5, Epoch: 3, Fee: 7},
		)
		require.Equal(t, blockRows{pools: map[string]uint64{}},
			readBlockRows(t, s, 3, nil))

		applyBlockOps(t, s, 10,
			badgerutxo.RewardBlockOp{Slot: 10, Epoch: 3, Open: true})
		applyBlockOps(t, s, 11,
			badgerutxo.RewardBlockOp{Slot: 11, Epoch: 3, Pool: poolA},
			badgerutxo.RewardBlockOp{Slot: 11, Epoch: 3, Fee: 5},
			badgerutxo.RewardBlockOp{Slot: 11, Epoch: 3, Fee: 6},
			// The same slot again counts nothing.
			badgerutxo.RewardBlockOp{Slot: 11, Epoch: 3, Pool: poolB},
		)
		applyBlockOps(t, s, 20,
			badgerutxo.RewardBlockOp{Slot: 20, Epoch: 3, Pool: poolB},
			badgerutxo.RewardBlockOp{Slot: 20, Epoch: 3, Pool: poolA},
		)
		// Opening an epoch that has rows leaves them alone.
		applyBlockOps(t, s, 21,
			badgerutxo.RewardBlockOp{Slot: 21, Epoch: 3, Open: true})
		got := readBlockRows(t, s, 3, nil)
		require.Equal(t, blockRows{
			total: 2, next: 21, hasTotal: true, fees: 11, hasFees: true,
			pools: map[string]uint64{string(poolA): 1, string(poolB): 1},
		}, got, "the second block of slot 20 is below the next countable slot")

		// A block at a later slot counts.
		applyBlockOps(t, s, 30,
			badgerutxo.RewardBlockOp{Slot: 30, Epoch: 3, Pool: poolB})
		got = readBlockRows(t, s, 3, nil)
		require.Equal(t, uint64(3), got.total)
		require.Equal(t, uint64(31), got.next)
		require.Equal(t, map[string]uint64{
			string(poolA): 1, string(poolB): 2,
		}, got.pools)

		slot15 := uint64(15)
		asOf := readBlockRows(t, s, 3, &slot15)
		require.Equal(t, uint64(1), asOf.total, "as of slot 15")
		require.Equal(t, uint64(11), asOf.fees)

		if viaJournal {
			// The journal rolls back only to a point the set held; nothing
			// was applied between slots 11 and 15.
			_, err := s.Rollback(point(11, 0x0b))
			require.NoError(t, err)
		} else {
			tip := point(15, 0x0f)
			_, err := s.ApplyLedgerBatch(
				&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &slot15},
			)
			require.NoError(t, err)
		}
		require.Equal(t, asOf, readBlockRows(t, s, 3, nil),
			"journal=%t", viaJournal)

		slot5 := uint64(5)
		if viaJournal {
			_, err := s.Rollback(point(1, 0x01))
			require.NoError(t, err)
		} else {
			tip := point(5, 0x05)
			_, err := s.ApplyLedgerBatch(
				&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &slot5},
			)
			require.NoError(t, err)
		}
		require.Equal(t, blockRows{pools: map[string]uint64{}},
			readBlockRows(t, s, 3, nil),
			"the epoch opened after slot 5 is gone, journal=%t", viaJournal)
	}
}

// Pruning deletes the rows of every epoch below the bound and only those. A
// rollback that keeps the prune keeps them deleted; one below the prune's
// slot restores them as they stood at the rollback target.
func TestRewardBlockPruneDropsOlderEpochsUntilARollbackBelowIt(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0xa1}, 28)
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	for epoch := uint64(1); epoch <= 4; epoch++ {
		slot := 10 * epoch
		applyBlockOps(t, s, slot,
			badgerutxo.RewardBlockOp{Slot: slot, Epoch: epoch, Open: true})
		applyBlockOps(t, s, slot+1,
			badgerutxo.RewardBlockOp{Slot: slot + 1, Epoch: epoch, Pool: pool},
			badgerutxo.RewardBlockOp{Slot: slot + 1, Epoch: epoch, Fee: epoch},
		)
	}
	applyBlockOps(t, s, 45,
		badgerutxo.RewardBlockOp{Slot: 45, PruneBelow: 3})
	applyBlockOps(t, s, 46,
		badgerutxo.RewardBlockOp{Slot: 46, Epoch: 4, Fee: 1})
	for epoch := uint64(1); epoch <= 2; epoch++ {
		require.Equal(t, blockRows{pools: map[string]uint64{}},
			readBlockRows(t, s, epoch, nil), "epoch %d", epoch)
	}
	for epoch := uint64(3); epoch <= 4; epoch++ {
		got := readBlockRows(t, s, epoch, nil)
		require.True(t, got.hasTotal, "epoch %d", epoch)
	}
	_, err := s.Rollback(point(45, 45))
	require.NoError(t, err)
	for epoch := uint64(1); epoch <= 2; epoch++ {
		require.False(t, readBlockRows(t, s, epoch, nil).hasTotal,
			"epoch %d stays pruned by a rollback that keeps the prune", epoch)
	}
	_, err = s.Rollback(point(11, 0x0b))
	require.NoError(t, err)
	got := readBlockRows(t, s, 1, nil)
	require.True(t, got.hasTotal, "epoch 1 is restored below the prune")
	require.Equal(t, uint64(1), got.total)
	require.Equal(t, uint64(1), got.fees)
	require.False(t, readBlockRows(t, s, 2, nil).hasTotal,
		"epoch 2 opened after the rollback target")
	require.False(t, readBlockRows(t, s, 3, nil).hasTotal,
		"epoch 3 opened after the rollback target")
}

// The window keeps the performance epoch of the next boundary's reward round,
// so a rollback that crosses two boundaries, as replay recovery's descent
// does on a chain whose journal reaches that far, must leave that epoch's
// counts known when the boundary is re-crossed. Pruning without a journal
// left them unknown, and the re-run reward round halted the node.
func TestRewardBlockRollbackAcrossTwoBoundariesKeepsThePerformanceEpoch(
	t *testing.T,
) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0xa2}, 28)
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	// Each epoch opens as the ledger stages it: the open, then the prune of
	// the epochs two below it.
	for epoch := uint64(1); epoch <= 4; epoch++ {
		slot := 10 * epoch
		ops := []badgerutxo.RewardBlockOp{
			{Slot: slot, Epoch: epoch, Open: true},
		}
		if epoch > 2 {
			ops = append(ops,
				badgerutxo.RewardBlockOp{Slot: slot, PruneBelow: epoch - 2})
		}
		applyBlockOps(t, s, slot, ops...)
		applyBlockOps(t, s, slot+1,
			badgerutxo.RewardBlockOp{Slot: slot + 1, Epoch: epoch, Pool: pool},
		)
	}
	require.False(t, readBlockRows(t, s, 1, nil).hasTotal,
		"opening epoch 4 prunes epoch 1")

	asOf := uint64(21)
	viewed := readBlockRows(t, s, 1, &asOf)
	require.True(t, viewed.hasTotal,
		"a read as of epoch 2 sees epoch 1's counts")
	require.Equal(t, uint64(1), viewed.pools[string(pool)])

	_, err := s.Rollback(point(21, 21))
	require.NoError(t, err)
	got := readBlockRows(t, s, 1, nil)
	require.True(t, got.hasTotal,
		"the 2-to-3 reward round reads epoch 1's counts after the rollback")
	require.Equal(t, uint64(1), got.total)
	require.Equal(t, uint64(1), got.pools[string(pool)])
}

// The loaded marker and the rows a load supplies come back, a key outside
// the family is refused, and a bootstrap clears both.
func TestRewardBlocksLoadAndBootstrapClearsThem(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0xa1}, 28)
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.Error(t, s.LoadRewardBlocks(
		func(put func(k, v []byte) error) error {
			return put([]byte("ga"), []byte{1})
		}))
	require.NoError(t, s.LoadRewardBlocks(
		func(put func(k, v []byte) error) error {
			if err := put(
				badgerutxo.RewardBlockTotalKey(9),
				badgerutxo.EncodeRewardBlockTotal(4, 0),
			); err != nil {
				return err
			}
			return put(
				badgerutxo.RewardBlockCountKey(9, pool),
				badgerutxo.EncodeRewardBlockCount(4),
			)
		}))
	loaded, err = s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	got := readBlockRows(t, s, 9, nil)
	require.Equal(t, uint64(4), got.total)
	require.Equal(t, map[string]uint64{string(pool): 4}, got.pools)

	bootstrap(t, s, point(2, 0x02))
	loaded, err = s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.False(t, readBlockRows(t, s, 9, nil).hasTotal)
}

// A rollback changes the durable counter, and a store that has seen none
// reads zero.
func TestRollbackSeqChangesWithEveryRollback(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	seq, err := s.RollbackSeq()
	require.NoError(t, err)
	require.Zero(t, seq)
	applyBlockOps(t, s, 10,
		badgerutxo.RewardBlockOp{Slot: 10, Epoch: 1, Open: true})
	seq, err = s.RollbackSeq()
	require.NoError(t, err)
	require.Zero(t, seq, "a write is not a rollback")
	prev := seq
	for range 2 {
		_, err = s.Rollback(point(1, 0x01))
		require.NoError(t, err)
		seq, err = s.RollbackSeq()
		require.NoError(t, err)
		require.Greater(t, seq, prev)
		prev = seq
	}
	to := uint64(3)
	tip := point(3, 0x03)
	_, err = s.ApplyLedgerBatch(
		&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to},
	)
	require.NoError(t, err)
	seq, err = s.RollbackSeq()
	require.NoError(t, err)
	require.Greater(t, seq, prev, "the batch rollback path counts too")
}

// A range tombstone stands for every row later written under its prefix: a
// rollback past it deletes them all, a rollback short of it keeps them, and
// RowAt and RowsAt as of an earlier slot find none of them, while a row
// outside the prefix is untouched and a wipe leaves nothing to roll back.
func TestRangeTombstoneDropsAPrefixOnRollback(t *testing.T) {
	t.Parallel()
	for _, viaJournal := range []bool{true, false} {
		s := open(t, t.TempDir())
		t.Cleanup(func() { require.NoError(t, s.Close()) })
		bootstrap(t, s, point(1, 0x01))
		put := func(slot uint64, rows ...badgerutxo.RowPut) {
			for i := range rows {
				rows[i].Slot = slot
			}
			tip := point(slot, byte(slot))
			_, err := s.ApplyLedgerBatch(
				&tip,
				badgerutxo.LedgerBatch{Rows: rows},
			)
			require.NoError(t, err)
		}
		prefix := []byte("Z\x00\x07")
		put(10, badgerutxo.RowPut{Key: []byte("Zkeep"), Value: []byte{9}})
		put(20,
			badgerutxo.RowPut{Key: prefix, TombstonePrefix: true},
			badgerutxo.RowPut{
				Key: append(bytes.Clone(prefix), 'a'), Value: []byte{1},
				SkipJournal: true,
			},
			badgerutxo.RowPut{
				Key: append(bytes.Clone(prefix), 'b'), Value: []byte{2},
				SkipJournal: true,
			},
		)
		put(30, badgerutxo.RowPut{
			Key: append(bytes.Clone(prefix), 'a'), Value: []byte{3},
		})

		v, ok, err := s.RowAt(append(bytes.Clone(prefix), 'a'), 15)
		require.NoError(t, err)
		require.False(
			t,
			ok,
			"journal=%t: absent before the tombstone",
			viaJournal,
		)
		require.Nil(t, v)
		_, ok, err = s.RowAt([]byte("Zkeep"), 15)
		require.NoError(t, err)
		require.True(t, ok, "a row outside the prefix is unaffected")
		v, ok, err = s.RowAt(append(bytes.Clone(prefix), 'a'), 25)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte{1}, v)

		var asOf15, asOf25 []string
		require.NoError(t, s.RowsAt([]byte("Z"), 15, func(k, _ []byte) error {
			asOf15 = append(asOf15, string(k))
			return nil
		}))
		require.Equal(t, []string{"Zkeep"}, asOf15)
		require.NoError(t, s.RowsAt(prefix, 25, func(k, _ []byte) error {
			asOf25 = append(asOf25, string(k))
			return nil
		}))
		require.Len(t, asOf25, 2)
		changed, err := s.RowKeysChangedAfter([]byte("Z"), 15)
		require.NoError(t, err)
		require.Equal(t, [][]byte{
			append(bytes.Clone(prefix), 'a'), append(bytes.Clone(prefix), 'b'),
		}, changed, "every row under the tombstone changed, journaled or not")

		to := uint64(25)
		if viaJournal {
			_, err = s.Rollback(point(20, 0x14))
		} else {
			tip := point(25, 0x19)
			_, err = s.ApplyLedgerBatch(
				&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to},
			)
		}
		require.NoError(t, err)
		v, ok, err = s.Row(append(bytes.Clone(prefix), 'a'))
		require.NoError(t, err)
		require.True(t, ok, "the tombstone is at slot 20, before the target")
		require.Equal(t, []byte{1}, v, "the later change is undone")

		to = 15
		if viaJournal {
			_, err = s.Rollback(point(10, 0x0a))
		} else {
			tip := point(15, 0x0f)
			_, err = s.ApplyLedgerBatch(
				&tip, badgerutxo.LedgerBatch{DeleteNoncesAfter: &to},
			)
		}
		require.NoError(t, err)
		for _, suffix := range []byte{'a', 'b'} {
			_, ok, err = s.Row(append(bytes.Clone(prefix), suffix))
			require.NoError(t, err)
			require.False(t, ok, "journal=%t: row %c", viaJournal, suffix)
		}
		_, ok, err = s.Row([]byte("Zkeep"))
		require.NoError(t, err)
		require.True(t, ok)
	}
}

// A prefix wipe deletes rows without leaving a journal entry, so a rollback
// does not bring them back.
func TestPrefixWipeIsNotJournaled(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	apply := func(slot uint64, rows ...badgerutxo.RowPut) {
		for i := range rows {
			rows[i].Slot = slot
		}
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: rows})
		require.NoError(t, err)
	}
	apply(10, badgerutxo.RowPut{Key: []byte("Za1"), Value: []byte{1}},
		badgerutxo.RowPut{Key: []byte("Za2"), Value: []byte{2}},
		badgerutxo.RowPut{Key: []byte("Zb1"), Value: []byte{3}})
	apply(20, badgerutxo.RowPut{Key: []byte("Za"), WipePrefix: true})
	_, err := s.Rollback(point(10, 0x0a))
	require.NoError(t, err)
	for _, k := range []string{"Za1", "Za2"} {
		_, ok, err := s.Row([]byte(k))
		require.NoError(t, err)
		require.False(t, ok, "%s stays wiped", k)
	}
	_, ok, err := s.Row([]byte("Zb1"))
	require.NoError(t, err)
	require.True(t, ok, "a row outside the wiped prefix is untouched")
}

// A set whose reward block rows were loaded before the overlay-inclusive
// counts existed has them unknown, so the earlier marker no longer counts as
// loaded: the rows are reloaded from the metadata store, which replaces the
// old ones and clears the earlier marker.
func TestRewardBlocksReloadWhenTheMarkerIsFromBeforeOverlayCounts(
	t *testing.T,
) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0xa1}, 28)
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	load := func(total uint64, all bool) {
		require.NoError(t, s.LoadRewardBlocks(
			func(put func(k, v []byte) error) error {
				if err := put(
					badgerutxo.RewardBlockTotalKey(9),
					badgerutxo.EncodeRewardBlockTotal(total, 0),
				); err != nil {
					return err
				}
				if !all {
					return nil
				}
				return put(
					badgerutxo.RewardBlockAllCountKey(9, pool),
					badgerutxo.EncodeRewardBlockCount(total),
				)
			}))
	}
	load(1, false)
	loaded, err := s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	require.NoError(t, s.UseLegacyRewardBlocksMarkerForTest())
	loaded, err = s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.False(t, loaded, "rows without overlay counts must be reloaded")

	load(2, true)
	loaded, err = s.RewardBlocksLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	legacy, err := s.HasLegacyRewardBlocksMarkerForTest()
	require.NoError(t, err)
	require.False(t, legacy)
	require.Equal(t, uint64(2), readBlockRows(t, s, 9, nil).total)
}

// A set whose op-cert counters were loaded before they carried the sequence of
// the latest observation reloads them: the earlier marker no longer counts as
// loaded, and a load clears it.
func TestOpCertsReloadWhenTheMarkerIsFromBeforeTheLastSequence(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	load := func() {
		require.NoError(t, s.LoadOpCerts(
			func(put func(k, v []byte) error) error {
				return put(
					append(
						[]byte{badgerutxo.OpCertTag},
						0x01,
					),
					make([]byte, 24),
				)
			}))
	}
	load()
	loaded, err := s.OpCertsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	require.NoError(t, s.UseLegacyOpCertsMarkerForTest())
	loaded, err = s.OpCertsLoaded()
	require.NoError(t, err)
	require.False(
		t,
		loaded,
		"counters without a last sequence must be reloaded",
	)

	load()
	loaded, err = s.OpCertsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	legacy, err := s.HasLegacyOpCertsMarkerForTest()
	require.NoError(t, err)
	require.False(t, legacy)
}

// A set whose pool history was loaded before its events carried an effective
// slot reloads it: the earlier marker no longer counts as loaded, and a load
// clears it.
func TestPoolHistoryReloadsWhenTheMarkerIsFromBeforeEffectiveSlots(
	t *testing.T,
) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	load := func() {
		require.NoError(t, s.LoadPoolHistory(
			func(put func(k, v []byte) error) error {
				return put(
					[]byte{badgerutxo.PoolHistoryTag, 1, 0x01},
					[]byte{0x80},
				)
			}))
	}
	load()
	loaded, err := s.PoolHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)

	require.NoError(t, s.UseLegacyPoolHistoryMarkerForTest())
	loaded, err = s.PoolHistoryLoaded()
	require.NoError(t, err)
	require.False(
		t,
		loaded,
		"events without an effective slot must be reloaded",
	)

	load()
	loaded, err = s.PoolHistoryLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	legacy, err := s.HasLegacyPoolHistoryMarkerForTest()
	require.NoError(t, err)
	require.False(t, legacy)
}

// A set whose stake events or witness rows were loaded before they carried an
// effective slot reloads them: the earlier marker no longer counts as loaded,
// and a load clears it.
func TestStakeFamiliesReloadWhenTheMarkerIsFromBeforeEffectiveSlots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		legacy    []byte
		useLegacy func(*badgerutxo.Store) error
		load      func(*badgerutxo.Store) error
		loaded    func(*badgerutxo.Store) (bool, error)
	}{
		{
			"registration history", []byte("mreghist"),
			(*badgerutxo.Store).UseLegacyRegistrationHistoryMarkerForTest,
			func(s *badgerutxo.Store) error {
				return s.LoadRegistrationHistory(
					func(put func(k, v []byte) error) error {
						return put([]byte{badgerutxo.StakeHistoryTag, 0, 1, 0x01}, []byte{0x80})
					})
			},
			(*badgerutxo.Store).RegistrationHistoryLoaded,
		},
		{
			"witness slots", []byte("mwitness"),
			(*badgerutxo.Store).UseLegacyWitnessSlotsMarkerForTest,
			func(s *badgerutxo.Store) error {
				return s.LoadWitnessSlots(
					func(put func(k, v []byte) error) error {
						return put([]byte{badgerutxo.WitnessTag, 0, 1, 0x01}, []byte{})
					})
			},
			(*badgerutxo.Store).WitnessSlotsLoaded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := open(t, t.TempDir())
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			bootstrap(t, s, point(1, 0x01))
			require.NoError(t, tc.load(s))
			loaded, err := tc.loaded(s)
			require.NoError(t, err)
			require.True(t, loaded)

			require.NoError(t, tc.useLegacy(s))
			loaded, err = tc.loaded(s)
			require.NoError(t, err)
			require.False(t, loaded, "rows without an effective slot must be reloaded")

			require.NoError(t, tc.load(s))
			loaded, err = tc.loaded(s)
			require.NoError(t, err)
			require.True(t, loaded)
			legacy, err := s.HasKeyForTest(tc.legacy)
			require.NoError(t, err)
			require.False(t, legacy)
		})
	}
}

// Closure output effects are pruned with the journal: the ones applied before
// the floor go and the rest stay, a load marks them loaded, and a bootstrap
// clears both.
func TestClosureUtxoEffectsPruneWithTheJournalAndBootstrapClearsThem(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.ClosureUtxoEffectsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	ref := badgerutxo.NewRef(bytes.Repeat([]byte{0x07}, 32), 1)
	require.NoError(t, s.LoadClosureUtxoEffects(
		func(put func(k, v []byte) error) error {
			for _, applied := range []uint64{5, 20} {
				if err := put(
					badgerutxo.ClosureUtxoKey(applied, false, ref),
					make([]byte, 17),
				); err != nil {
					return err
				}
			}
			return nil
		}))
	loaded, err = s.ClosureUtxoEffectsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	rows := func() []uint64 {
		var out []uint64
		require.NoError(t, s.Rows(
			[]byte{badgerutxo.ClosureUtxoTag},
			func(k, _ []byte) error {
				applied, _, _, err := badgerutxo.DecodeClosureUtxoKey(k)
				out = append(out, applied)
				return err
			}))
		return out
	}
	require.Equal(t, []uint64{5, 20}, rows())

	require.NoError(t, s.PruneJournal(10))
	require.Equal(t, []uint64{20}, rows())

	bootstrap(t, s, point(2, 0x02))
	loaded, err = s.ClosureUtxoEffectsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.Empty(t, rows())
}

// A store without the delegation marker reads as not loaded, a load marks it
// loaded and a bootstrap clears both the marker and the rows.
func TestDelegationsLoadAndBootstrapClearsThem(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.DelegationsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	key := []byte{badgerutxo.DelegationTag, 0, 1, 0x01}
	require.NoError(t, s.LoadDelegations(
		func(put func(k, v []byte) error) error {
			return put(key, make([]byte, 9))
		}))
	loaded, err = s.DelegationsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := s.Row(key)
	require.NoError(t, err)
	require.True(t, ok)

	bootstrap(t, s, point(2, 0x02))
	loaded, err = s.DelegationsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	_, ok, err = s.Row(key)
	require.NoError(t, err)
	require.False(t, ok)
}
