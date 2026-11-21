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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
)

// DeferredStakeInputsKey is the IndexerDB sync_state key listing the
// snapshots whose stake inputs were still being written by the ledger when
// their slot was copied, and have not been copied since.
const DeferredStakeInputsKey = "dingo:indexer:stake-inputs-deferred"

// stakeInputTable is the fact table the ledger fills after the boundary
// commit.
var stakeInputTable = metadata.FactTable{Name: "reward_stake_input"}

// pendingStakeInput is one snapshot in the ledger's pending list and in the
// IndexerDB's deferred list: the epoch and the slot its rows are dated.
type pendingStakeInput struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
}

type pendingStakeInputs struct {
	Entries []pendingStakeInput `json:"entries"`
}

func decodePendingStakeInputs(raw string) ([]pendingStakeInput, error) {
	if raw == "" {
		return nil, nil
	}
	var p pendingStakeInputs
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, fmt.Errorf("indexer: decode pending stake inputs: %w", err)
	}
	return p.Entries, nil
}

// ledgerPendingStakeInputs returns the snapshots the ledger has not finished
// writing stake inputs for.
func (f *MetadataFacts) ledgerPendingStakeInputs() ([]pendingStakeInput, error) {
	var raw string
	var err error
	if ro, ok := f.srcRows.(readOnlyFactRows); ok {
		if r, ok := ro.pending(); ok {
			raw, err = r.RewardStakeInputsPendingState()
		} else {
			raw, err = f.src.Metadata().GetSyncState(
				models.RewardStakeInputsPendingKey, nil,
			)
		}
	} else {
		raw, err = f.src.Metadata().GetSyncState(
			models.RewardStakeInputsPendingKey, nil,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("indexer: read pending stake inputs: %w", err)
	}
	return decodePendingStakeInputs(raw)
}

func (f *MetadataFacts) pendingStakeInputEpochs() (map[uint64]struct{}, error) {
	entries, err := f.ledgerPendingStakeInputs()
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]struct{}, len(entries))
	for _, e := range entries {
		out[e.Epoch] = struct{}{}
	}
	return out, nil
}

func (f *MetadataFacts) loadDeferred(
	dst *database.Txn,
	key string,
) ([]pendingStakeInput, error) {
	raw, err := f.dst.Metadata().GetSyncState(key, dst.Metadata())
	if err != nil {
		return nil, fmt.Errorf("indexer: read deferred snapshots: %w", err)
	}
	return decodePendingStakeInputs(raw)
}

func (f *MetadataFacts) saveDeferred(
	dst *database.Txn,
	key string,
	entries []pendingStakeInput,
) error {
	raw, err := json.Marshal(pendingStakeInputs{Entries: entries})
	if err != nil {
		return err
	}
	return f.dst.Metadata().SetSyncState(key, string(raw), dst.Metadata())
}

// dropPendingEpochs removes from rows those of an epoch in skip and returns
// the epoch and slot, read from slotColumn, of each removed snapshot.
func dropPendingEpochs(
	rows *metadata.FactRows,
	skip map[uint64]struct{},
	slotColumn string,
) ([]pendingStakeInput, error) {
	epochCol := slices.Index(rows.Columns, "epoch")
	slotCol := slices.Index(rows.Columns, slotColumn)
	if epochCol < 0 || slotCol < 0 {
		return nil, fmt.Errorf(
			"indexer: snapshot rows lack epoch or %s", slotColumn,
		)
	}
	kept := rows.Rows[:0]
	var deferred []pendingStakeInput
	for _, r := range rows.Rows {
		epoch, err := asUint64(r[epochCol])
		if err != nil {
			return nil, err
		}
		if _, pending := skip[epoch]; !pending {
			kept = append(kept, r)
			continue
		}
		slot, err := asUint64(r[slotCol])
		if err != nil {
			return nil, err
		}
		deferred = append(
			deferred, pendingStakeInput{Epoch: epoch, BoundarySlot: slot},
		)
	}
	rows.Rows = kept
	return deferred, nil
}

// deferSnapshots records, under key, snapshots whose rows were not copied.
func (f *MetadataFacts) deferSnapshots(
	dst *database.Txn,
	key string,
	deferred []pendingStakeInput,
) error {
	if len(deferred) == 0 {
		return nil
	}
	entries, err := f.loadDeferred(dst, key)
	if err != nil {
		return err
	}
	changed := false
	for _, d := range deferred {
		if !slices.Contains(entries, d) {
			entries = append(entries, d)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return f.saveDeferred(dst, key, entries)
}

// copyDeferredStakeInputs copies the stake inputs of each deferred snapshot
// the ledger has since finished writing, at the slot they are dated.
func (f *MetadataFacts) copyDeferredStakeInputs(
	ctx context.Context,
	dst *database.Txn,
) error {
	entries, err := f.loadDeferred(dst, DeferredStakeInputsKey)
	if err != nil || len(entries) == 0 {
		return err
	}
	stillPending, err := f.pendingStakeInputEpochs()
	if err != nil {
		return err
	}
	var kept []pendingStakeInput
	for _, e := range entries {
		if _, pending := stillPending[e.Epoch]; pending {
			kept = append(kept, e)
			continue
		}
		if err := f.copyStakeInputEpoch(ctx, dst, e); err != nil {
			return err
		}
	}
	if len(kept) == len(entries) {
		return nil
	}
	return f.saveDeferred(dst, DeferredStakeInputsKey, kept)
}

func (f *MetadataFacts) copyStakeInputEpoch(
	ctx context.Context,
	dst *database.Txn,
	e pendingStakeInput,
) error {
	return f.copyEpochRows(
		ctx, dst, stakeInputTable, e.BoundarySlot, e.BoundarySlot, &e.Epoch,
	)
}

// copyEpochRows copies the rows of table dated in [from, through], only those
// of epoch when one is given, by natural key.
func (f *MetadataFacts) copyEpochRows(
	ctx context.Context,
	dst *database.Txn,
	table metadata.FactTable,
	from, through uint64,
	epoch *uint64,
) error {
	var cursor int64
	restarts := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, next, err := f.srcRows.ReadFactRows(
			ctx, table, from, through, cursor, factPageRows, nil,
		)
		if errors.Is(err, metadata.ErrFactCursorLost) && restarts < maxCursorRestarts {
			restarts++
			cursor = 0
			continue
		}
		if err != nil {
			return err
		}
		if epoch != nil {
			epochCol := slices.Index(rows.Columns, "epoch")
			if epochCol < 0 {
				return fmt.Errorf("indexer: %s rows lack epoch", table.Name)
			}
			kept := rows.Rows[:0]
			for _, r := range rows.Rows {
				got, err := asUint64(r[epochCol])
				if err != nil {
					return err
				}
				if got == *epoch {
					kept = append(kept, r)
				}
			}
			rows.Rows = kept
		}
		if err := f.dstRows.WriteFactRows(
			table, rows, dst.Metadata(),
		); err != nil {
			return err
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// dropDeferredStakeInputsAfter forgets the deferred snapshots a rollback to
// slot has rewound; the rows they describe are gone from the ledger.
func (f *MetadataFacts) dropDeferredStakeInputsAfter(
	dst *database.Txn,
	slot uint64,
) error {
	return f.dropDeferredAfter(dst, DeferredStakeInputsKey, slot)
}

func (f *MetadataFacts) dropDeferredAfter(
	dst *database.Txn,
	key string,
	slot uint64,
) error {
	entries, err := f.loadDeferred(dst, key)
	if err != nil || len(entries) == 0 {
		return err
	}
	kept := slices.DeleteFunc(
		slices.Clone(entries),
		func(e pendingStakeInput) bool { return e.BoundarySlot > slot },
	)
	if len(kept) == len(entries) {
		return nil
	}
	return f.saveDeferred(dst, key, kept)
}
