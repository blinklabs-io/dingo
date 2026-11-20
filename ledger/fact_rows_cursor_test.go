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
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// prunableInputs is a reward input store of epochs 1 to 4, each with two pools
// holding one stake row, that can lose its oldest epochs.
type prunableInputs struct {
	RewardInputsReader
	epochs []uint64
}

func (p *prunableInputs) RewardPoolRows(
	epoch uint64, _ *uint64,
) ([]*models.RewardPoolInput, error) {
	if !slices.Contains(p.epochs, epoch) {
		return nil, nil
	}
	return []*models.RewardPoolInput{
		{Epoch: epoch, PoolKeyHash: []byte{1}},
		{Epoch: epoch, PoolKeyHash: []byte{2}},
	}, nil
}

func (p *prunableInputs) RewardStakeRows(
	epoch uint64, lo, _ []byte, _ *uint64,
) ([]*models.RewardStakeInput, error) {
	if !slices.Contains(p.epochs, epoch) {
		return nil, nil
	}
	return []*models.RewardStakeInput{{
		Epoch: epoch, PoolKeyHash: lo, StakingKey: []byte{byte(epoch)},
		CapturedSlot: epoch,
	}}, nil
}

// A stake input cursor names an epoch, not a position in the list of epochs
// the set holds. A boundary prune that removes the oldest epochs between two
// pages must not make the next page skip rows.
func TestStakeInputPagesSurviveAPruneBetweenPages(t *testing.T) {
	t.Parallel()
	inputs := &prunableInputs{epochs: []uint64{1, 2, 3, 4}}
	export := func() rewardInputsExport {
		return rewardInputsExport{
			view:   rewardInputsView{r: inputs},
			epochs: slices.Clone(inputs.epochs),
		}
	}
	served := func(rows [][]any) (out [][2]any) {
		for _, r := range rows {
			out = append(out, [2]any{r[2], r[0]})
		}
		return out
	}

	first, cursor, err := stakeInputPageOver(export(), 0, 100, 0, 3)
	require.NoError(t, err)
	require.NotZero(t, cursor)
	// Epoch 1 and the first pool of epoch 2 are served; epochs 1 and 2 are
	// then pruned.
	inputs.epochs = []uint64{3, 4}
	rest, cursor, err := stakeInputPageOver(export(), 0, 100, cursor, 100)
	require.NoError(t, err)
	require.Zero(t, cursor)

	got := append(served(first.Rows), served(rest.Rows)...)
	for _, epoch := range []int64{3, 4} {
		for _, pool := range [][]byte{{1}, {2}} {
			require.Contains(t, got, [2]any{epoch, pool},
				"epoch %d pool %x was skipped", epoch, pool)
		}
	}
}

// prunableOutputs serves one account output row per epoch it still holds.
type prunableOutputs struct {
	RewardDerivedReader
	epochs []uint64
}

func (p *prunableOutputs) RewardAccountOutputRows(
	epoch uint64, _ *uint64,
) ([]*models.RewardAccountOutput, error) {
	if !slices.Contains(p.epochs, epoch) {
		return nil, nil
	}
	return []*models.RewardAccountOutput{{
		Epoch: epoch, StakingKey: []byte{byte(epoch)}, CapturedSlot: epoch,
	}}, nil
}

// An account output cursor names an epoch for the same reason.
func TestAccountOutputPagesSurviveAPruneBetweenPages(t *testing.T) {
	t.Parallel()
	outputs := &prunableOutputs{epochs: []uint64{1, 2, 3, 4}}
	first, cursor, err := accountOutputPageOver(
		outputs, slices.Clone(outputs.epochs), 0, 100, 0, 2,
	)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	require.NotZero(t, cursor)

	outputs.epochs = []uint64{3, 4}
	rest, cursor, err := accountOutputPageOver(
		outputs, slices.Clone(outputs.epochs), 0, 100, cursor, 100,
	)
	require.NoError(t, err)
	require.Zero(t, cursor)
	var got []int64
	for _, r := range rest.Rows {
		got = append(got, r[3].(int64))
	}
	require.Equal(t, []int64{3, 4}, got, "epoch 3 was skipped")
}

// A fact page cursor stands for the key of the last row served. A row that
// leaves the range between two pages is not served, and it does not shift the
// rows behind the cursor: the page after it still starts at the next key.
func TestSetFactPagesResumeAfterTheLastKeyServed(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{}
	table := metadata.FactTable{
		Name: "account", Key: []string{"k"}, Paged: true,
	}
	row := func(k string) []any { return []any{[]byte(k), int64(1)} }
	cols := []string{"k", "added_slot"}
	reads := 0
	read := func() (metadata.FactRows, error) {
		reads++
		if reads == 1 {
			return metadata.FactRows{Columns: cols, Rows: [][]any{
				row("e"), row("a"), row("c"), row("b"), row("d"),
			}}, nil
		}
		// "a", already served, has left the range.
		return metadata.FactRows{Columns: cols, Rows: [][]any{
			row("e"), row("c"), row("b"), row("d"),
		}}, nil
	}
	keys := func(rows [][]any) (out []string) {
		for _, r := range rows {
			out = append(out, string(r[0].([]byte)))
		}
		return out
	}

	var got []string
	cursor := int64(0)
	for {
		page, next, err := ls.pageSetFactRows(table, 0, 100, cursor, 2, read, nil)
		require.NoError(t, err)
		got = append(got, keys(page.Rows)...)
		if next == 0 {
			break
		}
		cursor = next
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, got,
		"every row is served once, in key order")

	// A cursor of an export that is not in progress is refused rather than
	// restarted from a guessed position.
	_, _, err := ls.pageSetFactRows(table, 0, 100, 99, 2, read, nil)
	require.ErrorIs(t, err, ErrFactCursorLost)
}

// slotBoundedInputs serves epochs 1 to 3, each with one pool and stake rows
// captured at slots[epoch], and counts the stake rows it was asked for.
type slotBoundedInputs struct {
	RewardInputsReader
	slots map[uint64][]uint64
	reads []uint64
}

func (p *slotBoundedInputs) RewardPoolRows(
	epoch uint64, _ *uint64,
) ([]*models.RewardPoolInput, error) {
	return []*models.RewardPoolInput{{Epoch: epoch, PoolKeyHash: []byte{1}}}, nil
}

func (p *slotBoundedInputs) RewardStakeRows(
	epoch uint64, lo, _ []byte, _ *uint64,
) ([]*models.RewardStakeInput, error) {
	p.reads = append(p.reads, epoch)
	var out []*models.RewardStakeInput
	for i, slot := range p.slots[epoch] {
		out = append(out, &models.RewardStakeInput{
			Epoch: epoch, PoolKeyHash: lo, StakingKey: []byte{byte(i)},
			CapturedSlot: slot,
		})
	}
	return out, nil
}

func (p *slotBoundedInputs) RewardStakeSlotBounds(
	epoch uint64,
) (first, last uint64, found bool, err error) {
	s := p.slots[epoch]
	if len(s) == 0 {
		return 0, 0, false, nil
	}
	return s[0], s[len(s)-1], true, nil
}

// A stake input page for a slot range reads the stake rows of the epochs
// captured inside the range only; an epoch whose rows are not all captured at
// one slot is read in full, and a row outside the range is still dropped.
func TestStakeInputPageReadsOnlyEpochsCapturedInTheSlotRange(t *testing.T) {
	t.Parallel()
	inputs := &slotBoundedInputs{slots: map[uint64][]uint64{
		1: {10, 10, 10},
		2: {50, 50, 50},
		3: {10, 60, 60},
	}}
	ex := rewardInputsExport{
		view:   rewardInputsView{r: inputs},
		epochs: []uint64{1, 2, 3},
	}

	got, cursor, err := stakeInputPageOver(ex, 40, 100, 0, 100)
	require.NoError(t, err)
	require.Zero(t, cursor)
	require.Equal(t, []uint64{2, 3}, inputs.reads,
		"epoch 1 is captured outside the range and must not be read")
	require.Len(t, got.Rows, 5, "epoch 2's three rows and epoch 3's two")
}
