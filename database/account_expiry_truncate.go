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

	"github.com/blinklabs-io/dingo/database/models"
)

// DelegatorInactivityActivatedSyncKey is the durable sync_state marker key
// guarding the one-time CIP-0163 activation stamp. Its value is the
// activation epoch A (the epoch entered at the boundary activation ran
// on), stored as a decimal string; a non-empty value means activation has
// already run. Shared between ledger (which writes it during forward
// processing, see ledger.LedgerState.activateDelegatorInactivityIfNeeded)
// and this package (which reads/clears it during rollback/truncate) so
// both agree on the exact same marker regardless of which caller performs
// the delegator-inactivity bookkeeping around a given database mutation.
const DelegatorInactivityActivatedSyncKey = "delegator_inactivity_activated"

// DelegatorInactivityActivationEpoch reads the durable CIP-0163 activation
// marker and reports the activation epoch A, whether activation has
// occurred, and any read/parse error. An empty marker means activation
// has not run yet (activated == false, epoch 0).
func DelegatorInactivityActivationEpoch(
	d *Database,
	txn *Txn,
) (epoch uint64, activated bool, err error) {
	marker, err := d.GetSyncState(DelegatorInactivityActivatedSyncKey, txn)
	if err != nil {
		return 0, false, err
	}
	if marker == "" {
		return 0, false, nil
	}
	epoch, err = strconv.ParseUint(marker, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf(
			"parse delegator-inactivity activation marker %q: %w",
			marker,
			err,
		)
	}
	return epoch, true, nil
}

// RecomputeAccountExpirationsAfterTruncate restores the CIP-0163
// expiration_epoch of the reward accounts affected by a rollback or
// truncate to point.Slot.
//
// expiration_epoch is an epoch quantity, but the metadata layer that
// restores the rest of the account state on rollback only knows slots.
// This recomputes it here: for every affected credential (one whose
// expiration may have been renewed by a now-orphaned witness, gathered by
// AccountsWitnessedAfterSlot before the rolled-away certificate/
// withdrawal rows were deleted) it finds the greatest surviving
// witnessing slot <= rollbackSlot and stamps expiration = that slot's
// epoch + delegatorInactivity. A credential with no surviving witness has
// its expiration reset to 0 (unset): its only renewals were orphaned.
// Doing nothing would leave a stale, too-high expiration and exclude the
// account later than the surviving chain would -- a consensus divergence.
//
// Activation floor: the one-time activation stamp sets expiration = A + W
// for every account active at activation epoch A WITHOUT leaving any
// witness. A pure witness-history recompute therefore mis-restores an
// account that was active and stamped at activation but whose only
// post-activation witness is orphaned: it would drop to a far-past
// registration epoch (or reset to 0), below the activation floor the
// surviving chain still carries. So when activation actually ran at or
// before the rollback point (activated && A <= epoch(rollbackSlot)), the
// recomputed expiration is clamped up to the activation floor A + W for
// every account in the durable activation-membership set. Existence
// alone is not enough because a deregistered account present in the
// table was not stamped.
//
// affectedRefs are deduped with any rows reset while crossing back before
// the activation boundary, so each credential is stamped exactly once.
// Refs are grouped by target expiration so identical epochs share a
// single RenewAccountExpirations batch.
//
// It is a no-op when delegatorInactivityEnabled is false, and must run
// inside the same write transaction as the rest of the rollback/truncate
// sweep, after the metadata layer has restored the remaining account
// fields (e.g. via TruncateAfterSlot).
//
// Shared by ledger.LedgerState.rollback (bounded rollback during normal
// sync, security-parameter-limited) and database/lifecycle.Truncate
// (offline and live CIP-0135 disaster-recovery truncate, which may go far
// deeper) so both apply the exact same CIP-0163 bookkeeping regardless of
// which path performs the truncation.
func RecomputeAccountExpirationsAfterTruncate(
	d *Database,
	txn *Txn,
	delegatorInactivityEnabled bool,
	delegatorInactivity uint64,
	rollbackSlot uint64,
	affectedRefs []models.StakeCredentialRef,
) error {
	if !delegatorInactivityEnabled {
		return nil
	}
	rollbackEpoch, err := EpochBySlot(d, rollbackSlot, txn)
	if err != nil {
		return fmt.Errorf("map rollback slot %d to epoch: %w", rollbackSlot, err)
	}
	activationEpoch, activated, err := DelegatorInactivityActivationEpoch(d, txn)
	if err != nil {
		return fmt.Errorf("read delegator-inactivity activation epoch: %w", err)
	}
	// Rolling back to before the activation boundary: undo the one-time
	// activation stamp so a later re-sync re-runs it.
	// ResetAccountExpirationActivation returns every reset credential. Unioning
	// those credentials into affectedRefs is required because activation
	// overwrites an earlier witness expiration: an account with no orphaned
	// post-activation witness would otherwise be absent from affectedRefs and
	// remain reset to 0 instead of being reconstructed from its surviving
	// pre-activation witness.
	if activated && activationEpoch > rollbackEpoch.EpochId {
		resetRefs, resetErr := d.ResetAccountExpirationActivation(txn)
		if resetErr != nil {
			return fmt.Errorf(
				"reset delegator-inactivity activation stamp: %w",
				resetErr,
			)
		}
		affectedRefs = mergeStakeCredentialRefs(affectedRefs, resetRefs)
		if err := d.DeleteSyncState(DelegatorInactivityActivatedSyncKey, txn); err != nil {
			return fmt.Errorf("clear delegator-inactivity activation marker: %w", err)
		}
		activated = false
	}
	if len(affectedRefs) == 0 {
		return nil
	}
	lastWitness, err := d.AccountLastWitnessSlots(
		affectedRefs,
		rollbackSlot,
		txn,
	)
	if err != nil {
		return fmt.Errorf("query last witness slots: %w", err)
	}
	// Resolve the activation floor. The floor applies only when activation
	// actually ran at or before the rollback point; otherwise the surviving
	// chain carries no activation stamp and there is nothing to clamp to.
	clampApplies := false
	if activated {
		clampApplies = activationEpoch <= rollbackEpoch.EpochId
	}
	// Load exact activation membership only when the floor is in play. Existence
	// at activation is insufficient: accounts that were deregistered at the
	// boundary were not stamped and must not receive the floor.
	var activationMembership map[string]struct{}
	if clampApplies {
		activationMembership, err = d.AccountInactivityActivationMembership(
			affectedRefs,
			txn,
		)
		if err != nil {
			return fmt.Errorf("load activation membership: %w", err)
		}
	}
	// Group affected credentials by the expiration epoch to stamp. A credential
	// with neither a surviving witness nor an applicable activation floor is
	// reset to expiration 0.
	byExpiration := make(map[uint64][]models.StakeCredentialRef)
	for _, ref := range affectedRefs {
		var (
			witnessEpoch   uint64
			witnessPresent bool
		)
		if slot, ok := lastWitness[ref.MapKey()]; ok {
			epoch, epochErr := EpochBySlot(d, slot, txn)
			if epochErr != nil {
				return fmt.Errorf(
					"map witness slot %d to epoch: %w",
					slot,
					epochErr,
				)
			}
			witnessEpoch = epoch.EpochId
			witnessPresent = true
		}
		floorEpoch, floorPresent := RollbackActivationFloor(
			ref,
			clampApplies,
			activationEpoch,
			activationMembership,
		)
		var expiration uint64
		switch {
		case witnessPresent && floorPresent:
			expiration = max(witnessEpoch, floorEpoch) + delegatorInactivity
		case witnessPresent:
			expiration = witnessEpoch + delegatorInactivity
		case floorPresent:
			expiration = floorEpoch + delegatorInactivity
		default:
			expiration = 0
		}
		byExpiration[expiration] = append(byExpiration[expiration], ref)
	}
	for expiration, refs := range byExpiration {
		if err := d.RenewAccountExpirations(refs, expiration, txn); err != nil {
			return fmt.Errorf(
				"stamp rollback expiration %d: %w",
				expiration,
				err,
			)
		}
	}
	return nil
}

func mergeStakeCredentialRefs(
	refs ...[]models.StakeCredentialRef,
) []models.StakeCredentialRef {
	total := 0
	for _, group := range refs {
		total += len(group)
	}
	ret := make([]models.StakeCredentialRef, 0, total)
	seen := make(map[string]struct{}, total)
	for _, group := range refs {
		for _, ref := range group {
			key := ref.MapKey()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ret = append(ret, ref)
		}
	}
	return ret
}

// RollbackActivationFloor reports the CIP-0163 activation-floor epoch for
// one affected credential and whether the floor applies to it. The floor
// is the activation epoch A, and it applies only when the clamp is active
// for this rollback AND the account was actually stamped at activation.
// Durable activation membership distinguishes those accounts from
// credentials that existed but were deregistered at the activation
// boundary.
func RollbackActivationFloor(
	ref models.StakeCredentialRef,
	clampApplies bool,
	activationEpoch uint64,
	activationMembership map[string]struct{},
) (uint64, bool) {
	if !clampApplies {
		return 0, false
	}
	_, ok := activationMembership[ref.MapKey()]
	if !ok {
		return 0, false
	}
	return activationEpoch, true
}
