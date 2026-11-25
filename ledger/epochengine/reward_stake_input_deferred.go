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

package epochengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// RewardStakeInputsPendingKey names the record of the mark snapshots whose
// per-credential reward basis (reward_stake_input) is held only in part: a
// store migrated from one that wrote those rows after the boundary committed
// and stopped before the last of them. The set keeps one row per such
// snapshot; every reader of reward_stake_input goes through
// EnsureRewardStakeInputsReady first, which rebuilds the rows.
const RewardStakeInputsPendingKey = models.RewardStakeInputsPendingKey

// RewardStakeInputsPendingEntry names a snapshot whose stake rows are not
// all written.
type RewardStakeInputsPendingEntry struct {
	Epoch        uint64 `json:"epoch"`
	BoundarySlot uint64 `json:"boundary_slot"`
}

type rewardStakeInputsPending struct {
	Entries []RewardStakeInputsPendingEntry `json:"entries"`
}

// ErrRewardStakeInputsNotReady reports that a snapshot's reward basis is
// incomplete and the reader's transaction cannot rebuild it.
var ErrRewardStakeInputsNotReady = errors.New(
	"reward stake inputs are incomplete",
)

var ErrRewardStakeInputsUnrecoverable = errors.New(
	"reward stake inputs cannot be reconstructed",
)

func LoadRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
) (*rewardStakeInputsPending, error) {
	raw, err := meta.GetSyncState(RewardStakeInputsPendingKey, metaTxn)
	if err != nil {
		return nil, fmt.Errorf("load pending reward stake inputs: %w", err)
	}
	return decodeRewardStakeInputsPending(raw)
}

func decodeRewardStakeInputsPending(
	raw string,
) (*rewardStakeInputsPending, error) {
	pending := &rewardStakeInputsPending{}
	if raw == "" {
		return pending, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return nil, fmt.Errorf("decode pending reward stake inputs: %w", err)
	}
	entries, ok := envelope["entries"]
	if !ok || len(envelope) != 1 {
		return nil, errors.New(
			"decode pending reward stake inputs: unknown state format",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(entries))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&pending.Entries); err != nil {
		return nil, fmt.Errorf("decode pending reward stake inputs: %w", err)
	}
	return pending, nil
}

// ParseRewardStakeInputsPending decodes the value of the pending sync_state
// key, rejecting a format it does not know.
func ParseRewardStakeInputsPending(
	raw string,
) ([]RewardStakeInputsPendingEntry, error) {
	pending, err := decodeRewardStakeInputsPending(raw)
	if err != nil {
		return nil, err
	}
	return pending.Entries, nil
}

// FormatRewardStakeInputsPending is the pending sync_state value naming
// entries, empty when there are none.
func FormatRewardStakeInputsPending(
	entries []RewardStakeInputsPendingEntry,
) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(&rewardStakeInputsPending{Entries: entries})
	if err != nil {
		return "", fmt.Errorf("encode pending reward stake inputs: %w", err)
	}
	return string(raw), nil
}

func SaveRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	pending *rewardStakeInputsPending,
) error {
	raw, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("encode pending reward stake inputs: %w", err)
	}
	if err := meta.SetSyncState(
		RewardStakeInputsPendingKey, string(raw), metaTxn,
	); err != nil {
		return fmt.Errorf("save pending reward stake inputs: %w", err)
	}
	return nil
}

func (p *rewardStakeInputsPending) Find(epoch uint64) int {
	return slices.IndexFunc(
		p.Entries,
		func(e RewardStakeInputsPendingEntry) bool {
			return e.Epoch == epoch
		},
	)
}

func clearRewardStakeInputsPending(
	meta ledgerdb.LedgerDB,
	metaTxn ledgerdb.Txn,
	epoch uint64,
) error {
	pending, err := LoadRewardStakeInputsPending(meta, metaTxn)
	if err != nil {
		return err
	}
	i := pending.Find(epoch)
	if i < 0 {
		return nil
	}
	pending.Entries = slices.Delete(pending.Entries, i, i+1)
	return SaveRewardStakeInputsPending(meta, metaTxn, pending)
}

// EnsureRewardStakeInputsReady makes a snapshot's reward_stake_input rows
// complete before a reader uses them. A pending snapshot is completed here
// from the historical reconstruction the retention path already uses,
// checked against the snapshot's persisted pool inputs, in txn; a read-only
// txn gets ErrRewardStakeInputsNotReady.
func (e *Engine) EnsureRewardStakeInputsReady(
	txn ledgerdb.Txn,
	epoch uint64,
) error {
	return e.EnsureRewardStakeInputsReadyIn(e.ledgerDB(), txn, epoch)
}

// EnsureRewardStakeInputsReadyIn is EnsureRewardStakeInputsReady against an
// explicit store, so the metadata store can be read directly while the ledger
// store would serve the set.
func (e *Engine) EnsureRewardStakeInputsReadyIn(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	epoch uint64,
) error {
	metaTxn := txn
	pending, err := LoadRewardStakeInputsPending(ldb, metaTxn)
	if err != nil {
		return err
	}
	i := pending.Find(epoch)
	if i < 0 {
		return nil
	}
	if !txn.IsReadWrite() {
		return ErrRewardStakeInputsNotReady
	}
	snapshot, err := ldb.GetRewardSnapshot(epoch, "mark", metaTxn)
	if err != nil {
		return fmt.Errorf("get reward snapshot for epoch %d: %w", epoch, err)
	}
	if snapshot == nil ||
		snapshot.BoundarySlot != pending.Entries[i].BoundarySlot {
		// The snapshot the entry describes was rolled back.
		return clearRewardStakeInputsPending(ldb, metaTxn, epoch)
	}
	poolInputs, err := ldb.GetRewardPoolInputs(epoch, metaTxn)
	if err != nil {
		return fmt.Errorf(
			"get reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	rebuilt, rejected, err := e.rebuildPrunedRewardStakeInputsDetailed(
		ldb, metaTxn, epoch, snapshot, poolInputs,
	)
	if err != nil {
		return fmt.Errorf(
			"rebuild pending reward stake inputs for epoch %d: %w",
			epoch, err,
		)
	}
	if rejected && snapshot.TotalDelegators > 0 {
		return fmt.Errorf(
			"%w: pending epoch %d reconstruction disagrees with the "+
				"retained reward_pool_input totals",
			ErrRewardStakeInputsUnrecoverable,
			epoch,
		)
	}
	if len(rebuilt) == 0 && snapshot.TotalDelegators > 0 {
		return fmt.Errorf(
			"%w: pending epoch %d returned no rows for %d snapshot delegators",
			ErrRewardStakeInputsUnrecoverable,
			epoch,
			snapshot.TotalDelegators,
		)
	}
	if err := ldb.DeleteRewardInputsForEpoch(epoch, metaTxn); err != nil {
		return fmt.Errorf(
			"clear partial reward inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := ldb.SaveRewardPoolInputs(poolInputs, metaTxn); err != nil {
		return fmt.Errorf(
			"restore reward pool inputs for epoch %d: %w", epoch, err,
		)
	}
	if err := ldb.SaveRewardStakeInputs(rebuilt, metaTxn); err != nil {
		return fmt.Errorf(
			"save rebuilt reward stake inputs for epoch %d: %w", epoch, err,
		)
	}
	e.config.Logger.Info(
		"rebuilt incomplete reward stake inputs",
		"component", "ledger",
		"epoch", epoch,
		"rows", len(rebuilt),
	)
	return clearRewardStakeInputsPending(ldb, metaTxn, epoch)
}
