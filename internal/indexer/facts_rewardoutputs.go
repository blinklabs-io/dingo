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

package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// CopiedRewardOutputsPrefix prefixes the IndexerDB sync_state keys recording,
// per snapshot epoch, the finished reward round whose outputs were copied.
const CopiedRewardOutputsPrefix = "dingo:indexer:reward-outputs-copied:"

// The ledger finishes a reward round's outputs after the boundary commits and
// dates them at the slot it captured the round at, which is the ledger's tip
// when the epoch transition was processed. A writer that follows the tip is
// already past that slot by the time the round is finished, so the slot range
// copy never reads the rows. A finished round is read by epoch instead, at
// the slot the round's cursor records, once per finished round.

// finishedRewardRound is a reward precompute cursor whose last chunk has
// committed.
type finishedRewardRound struct {
	Epoch        uint64
	CapturedSlot uint64
	Fingerprint  string
}

type rewardCursorRecord struct {
	FormatVersion     int    `json:"format_version"`
	SnapshotEpoch     uint64 `json:"snapshot_epoch"`
	Done              bool   `json:"done"`
	OutputFingerprint string `json:"output_fingerprint"`
	CapturedSlot      uint64 `json:"captured_slot"`
}

// copiedRewardOutputs is the IndexerDB's record of a round it copied.
type copiedRewardOutputs struct {
	Fingerprint  string `json:"fingerprint"`
	CapturedSlot uint64 `json:"captured_slot"`
}

func (f *MetadataFacts) ledgerRewardCursors() (map[uint64]string, error) {
	if ro, ok := f.srcRows.(readOnlyFactRows); ok {
		if r, ok := ro.FactRowReader.(metadata.RewardPrecomputeCursorReader); ok {
			return r.RewardPrecomputeCursors()
		}
	}
	keys, err := f.src.ListSyncStateKeysByPrefix(
		models.RewardPrecomputeCursorPrefix, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("indexer: list reward cursors: %w", err)
	}
	out := make(map[uint64]string, len(keys))
	for _, k := range keys {
		epoch, err := strconv.ParseUint(
			strings.TrimPrefix(k, models.RewardPrecomputeCursorPrefix), 10, 64,
		)
		if err != nil {
			continue
		}
		raw, err := f.src.Metadata().GetSyncState(k, nil)
		if err != nil {
			return nil, fmt.Errorf("indexer: read reward cursor: %w", err)
		}
		out[epoch] = raw
	}
	return out, nil
}

// finishedRewardRounds returns the rounds whose outputs the ledger has
// finished writing, ascending by epoch. It is read before the slot range
// copy: a round listed here was complete when the rows are read afterwards.
func (f *MetadataFacts) finishedRewardRounds() ([]finishedRewardRound, error) {
	cursors, err := f.ledgerRewardCursors()
	if err != nil {
		return nil, err
	}
	var out []finishedRewardRound
	for epoch, raw := range cursors {
		var c rewardCursorRecord
		if json.Unmarshal([]byte(raw), &c) != nil || !c.Done ||
			c.SnapshotEpoch != epoch {
			continue
		}
		out = append(out, finishedRewardRound{
			Epoch: epoch, CapturedSlot: c.CapturedSlot,
			Fingerprint: c.OutputFingerprint,
		})
	}
	slices.SortFunc(out, func(a, b finishedRewardRound) int {
		return int(a.Epoch) - int(b.Epoch) //nolint:gosec // epochs are small
	})
	return out, nil
}

func copiedRewardOutputsKey(epoch uint64) string {
	return CopiedRewardOutputsPrefix + strconv.FormatUint(epoch, 10)
}

func (f *MetadataFacts) loadCopiedRewardOutputs(
	dst *database.Txn,
	epoch uint64,
) (copiedRewardOutputs, error) {
	raw, err := f.dst.GetSyncState(copiedRewardOutputsKey(epoch), dst)
	if err != nil || raw == "" {
		return copiedRewardOutputs{}, err
	}
	var c copiedRewardOutputs
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		// An unreadable record owes a copy, which only repeats one.
		return copiedRewardOutputs{}, nil //nolint:nilerr
	}
	return c, nil
}

func (f *MetadataFacts) saveCopiedRewardOutputs(
	dst *database.Txn,
	r finishedRewardRound,
) error {
	raw, err := json.Marshal(copiedRewardOutputs{
		Fingerprint: r.Fingerprint, CapturedSlot: r.CapturedSlot,
	})
	if err != nil {
		return err
	}
	return f.dst.SetSyncState(copiedRewardOutputsKey(r.Epoch), string(raw), dst)
}

// RefreshFacts copies completed reward outputs even while the chain is idle.
func (f *MetadataFacts) RefreshFacts(
	ctx context.Context,
	dst *database.Txn,
	through uint64,
) error {
	if err := f.copyDeferredStakeInputs(ctx, dst); err != nil {
		return err
	}
	rounds, err := f.finishedRewardRounds()
	if err != nil {
		return err
	}
	for _, r := range rounds {
		if r.CapturedSlot > through {
			continue
		}
		have, err := f.loadCopiedRewardOutputs(dst, r.Epoch)
		if err != nil {
			return err
		}
		if have == (copiedRewardOutputs{
			Fingerprint: r.Fingerprint, CapturedSlot: r.CapturedSlot,
		}) {
			continue
		}
		if err := f.copyRewardRound(ctx, dst, r); err != nil {
			return err
		}
		if err := f.saveCopiedRewardOutputs(dst, r); err != nil {
			return err
		}
	}
	return nil
}

// copyLateRewardOutputs copies, by epoch, the outputs of each round the ledger
// finished after the writer passed the slot they are dated at. rounds was read
// before the slot range copy of [from, through].
//
// A round dated inside the range was read by that copy, after its cursor was
// done, so it only needs its record. A round dated before the range is copied
// when its finished state differs from the recorded one, and again by the
// segment that carries the boundary crediting it: the boundary rechecks the
// eligibility of its outputs in place, after the writer copied them.
func (f *MetadataFacts) copyLateRewardOutputs(
	ctx context.Context,
	dst *database.Txn,
	rounds []finishedRewardRound,
	from, through uint64,
) error {
	var credited map[uint64]struct{}
	for _, r := range rounds {
		if r.CapturedSlot > through {
			continue
		}
		want := copiedRewardOutputs{
			Fingerprint: r.Fingerprint, CapturedSlot: r.CapturedSlot,
		}
		have, err := f.loadCopiedRewardOutputs(dst, r.Epoch)
		if err != nil {
			return err
		}
		if r.CapturedSlot >= from {
			if have == want {
				continue
			}
			if err := f.saveCopiedRewardOutputs(dst, r); err != nil {
				return err
			}
			continue
		}
		if credited == nil {
			credited, err = f.creditedRewardEpochs(ctx, from, through)
			if err != nil {
				return err
			}
		}
		if _, ok := credited[r.Epoch]; ok || have != want {
			if err := f.copyRewardRound(ctx, dst, r); err != nil {
				return err
			}
			if err := f.saveCopiedRewardOutputs(dst, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// creditedRewardEpochs returns the snapshot epochs of the rounds credited at a
// boundary in [from, through].
func (f *MetadataFacts) creditedRewardEpochs(
	ctx context.Context,
	from, through uint64,
) (map[uint64]struct{}, error) {
	table, err := factTableNamed("reward_credit_round")
	if err != nil {
		return nil, err
	}
	rows, _, err := f.srcRows.ReadFactRows(
		ctx, table, from, through, 0, factPageRows, nil,
	)
	if err != nil {
		return nil, err
	}
	col := slices.Index(rows.Columns, "snapshot_epoch")
	if col < 0 {
		return nil, errors.New("indexer: credit rounds lack snapshot_epoch")
	}
	out := make(map[uint64]struct{}, len(rows.Rows))
	for _, r := range rows.Rows {
		epoch, err := asUint64(r[col])
		if err != nil {
			return nil, err
		}
		out[epoch] = struct{}{}
	}
	return out, nil
}

func factTableNamed(name string) (metadata.FactTable, error) {
	for _, t := range metadata.FactTables {
		if t.Name == name {
			return t, nil
		}
	}
	return metadata.FactTable{}, fmt.Errorf("indexer: no fact table %s", name)
}

// copyRewardRound copies a finished round's pool and account outputs at the
// slot they are dated, and the ADA pots rows, whose rewards column the round
// fills after the pots row's own slot.
func (f *MetadataFacts) copyRewardRound(
	ctx context.Context,
	dst *database.Txn,
	r finishedRewardRound,
) error {
	for _, name := range []string{
		"reward_pool_output", "reward_account_output",
	} {
		table, err := factTableNamed(name)
		if err != nil {
			return err
		}
		if err := f.copyEpochRows(
			ctx, dst, table, r.CapturedSlot, r.CapturedSlot, &r.Epoch,
		); err != nil {
			return err
		}
	}
	pots, err := factTableNamed("reward_ada_pots")
	if err != nil {
		return err
	}
	return f.copyEpochRows(ctx, dst, pots, 0, r.CapturedSlot, nil)
}

// dropCopiedRewardOutputsAfter forgets the rounds copied at a slot after a
// rollback point: the rows they describe were removed with the rollback.
func (f *MetadataFacts) dropCopiedRewardOutputsAfter(
	dst *database.Txn,
	slot uint64,
) error {
	keys, err := f.dst.ListSyncStateKeysByPrefix(CopiedRewardOutputsPrefix, dst)
	if err != nil {
		return err
	}
	for _, k := range keys {
		raw, err := f.dst.GetSyncState(k, dst)
		if err != nil {
			return err
		}
		var c copiedRewardOutputs
		if json.Unmarshal([]byte(raw), &c) == nil && c.CapturedSlot <= slot {
			continue
		}
		if err := f.dst.DeleteSyncState(k, dst); err != nil {
			return err
		}
	}
	return nil
}
