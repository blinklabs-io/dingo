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

package database

import (
	"fmt"
	"strconv"
)

// SyntheticV2CostModelSyncKey is the durable sync_state marker key backing
// LedgerState.syntheticV2CostModel (blinklabs-io/dingo#3825): whether the
// PlutusV2 cost model currently in force is still HardForkBabbage's
// fabricated default rather than real governance/protocol-update data.
// Shared between ledger (which writes it during forward processing) and
// this package (which reads/resets it during rollback/truncate) so both
// agree on the exact same marker regardless of which caller performs the
// truncation.
const SyntheticV2CostModelSyncKey = "synthetic_v2_cost_model"

// SyntheticV2CostModelClearedEpochSyncKey is the durable sync_state marker
// key recording the epoch at which real (non-synthetic) PlutusV2 cost-model
// data was last confirmed written -- either via CIP-1694 governance
// enactment or a pre-Conway protocol-parameter update. Its value is that
// epoch, stored as a decimal string; an empty value means no real write has
// been confirmed since the marker was last reset. This is the provenance
// signal RecomputeSyntheticV2CostModelMarkerAfterTruncate uses to decide
// whether a rollback or truncate crossed back before the confirmation, and
// so must undo it.
const SyntheticV2CostModelClearedEpochSyncKey = "synthetic_v2_cost_model_cleared_epoch"

// SyntheticV2CostModelClearedEpoch reads the durable marker and reports the
// epoch at which real PlutusV2 cost-model data was last confirmed, whether
// it has been confirmed at all, and any read/parse error. An empty marker
// means no real write has been confirmed (cleared == false, epoch 0).
func SyntheticV2CostModelClearedEpoch(
	d *Database,
	txn *Txn,
) (epoch uint64, cleared bool, err error) {
	marker, err := d.GetSyncState(SyntheticV2CostModelClearedEpochSyncKey, txn)
	if err != nil {
		return 0, false, err
	}
	if marker == "" {
		return 0, false, nil
	}
	epoch, err = strconv.ParseUint(marker, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parse synthetic PlutusV2 cost model cleared-epoch marker %q: %w",
			marker,
			err,
		)
	}
	return epoch, true, nil
}

// SetSyntheticV2CostModelClearedEpoch durably records that real PlutusV2
// cost-model data was confirmed written as of epoch. Callers pair this with
// setting SyntheticV2CostModelSyncKey to "false" in the same transaction --
// see ledger.LedgerState's persistSyntheticV2CostModel caller.
func SetSyntheticV2CostModelClearedEpoch(
	d *Database,
	txn *Txn,
	epoch uint64,
) error {
	if err := d.SetSyncState(
		SyntheticV2CostModelClearedEpochSyncKey,
		strconv.FormatUint(epoch, 10),
		txn,
	); err != nil {
		return fmt.Errorf(
			"persist synthetic PlutusV2 cost model cleared-epoch marker: %w",
			err,
		)
	}
	return nil
}

// RecomputeSyntheticV2CostModelMarkerAfterTruncate restores
// SyntheticV2CostModelSyncKey when a rollback or truncate to rollbackSlot
// crosses back before the epoch SyntheticV2CostModelClearedEpochSyncKey
// recorded.
//
// Without this, a rollback past the enactment or protocol-parameter update
// that confirmed real PlutusV2 cost-model data restores the fabricated
// default into the surviving pparams (via the normal pparams-row reload
// truncate already performs correctly) while the marker stays "false" --
// GetCurrentProtocolParams then reports the fabricated model as real,
// permanently after a restart, and the same wrong answer survives a re-sync
// onto a fork that never re-enacts the confirming write. Mirrors the
// CIP-0163 delegator-inactivity-activation precedent
// (RecomputeAccountExpirationsAfterTruncate,
// DelegatorInactivityActivationEpoch): shared by
// ledger.LedgerState.rollback (bounded rollback during normal sync) and
// database/lifecycle.Truncate (offline/live disaster-recovery truncate, which
// may go far deeper), so both apply the exact same bookkeeping regardless of
// which path performs the truncation. See blinklabs-io/dingo#3825's PR
// review.
func RecomputeSyntheticV2CostModelMarkerAfterTruncate(
	d *Database,
	txn *Txn,
	rollbackSlot uint64,
) error {
	clearedEpoch, cleared, err := SyntheticV2CostModelClearedEpoch(d, txn)
	if err != nil {
		return fmt.Errorf(
			"read synthetic PlutusV2 cost model cleared-epoch marker: %w",
			err,
		)
	}
	if !cleared {
		return nil
	}
	rollbackEpoch, err := EpochBySlot(d, rollbackSlot, txn)
	if err != nil {
		return fmt.Errorf(
			"map rollback slot %d to epoch: %w",
			rollbackSlot,
			err,
		)
	}
	if clearedEpoch <= rollbackEpoch.EpochId {
		// The rollback doesn't cross the epoch that confirmed real data --
		// still valid for the surviving chain.
		return nil
	}
	// Rolling back to before the confirming epoch: the confirmation no
	// longer applies to the surviving chain (which may resync onto a fork
	// that never re-enacts it), so undo both the epoch marker and the
	// boolean it drove.
	if err := d.DeleteSyncState(
		SyntheticV2CostModelClearedEpochSyncKey, txn,
	); err != nil {
		return fmt.Errorf(
			"clear synthetic PlutusV2 cost model cleared-epoch marker: %w",
			err,
		)
	}
	if err := d.SetSyncState(
		SyntheticV2CostModelSyncKey, "true", txn,
	); err != nil {
		return fmt.Errorf(
			"restore synthetic PlutusV2 cost model marker: %w",
			err,
		)
	}
	return nil
}
