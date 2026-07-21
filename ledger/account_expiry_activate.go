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
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
)

// delegatorInactivityActivatedSyncKey is the durable sync_state marker key
// guarding the one-time CIP-0163 activation stamp. Its value is the activation
// epoch A (the epoch entered at the boundary activation ran on), stored as a
// decimal string; a NON-EMPTY value means activation has already run. Once set,
// activateDelegatorInactivityIfNeeded never re-stamps, even across restarts
// (the marker is stored in the same metadata store as the account rows and
// committed atomically with the stamp). The stored epoch is the activation
// floor the rollback recompute clamps to (see
// recomputeAccountExpirationsAfterRollback): activation stamps A + W without
// leaving a witness, so witness history alone cannot reconstruct it.
const delegatorInactivityActivatedSyncKey = "delegator_inactivity_activated"

// activateDelegatorInactivityIfNeeded performs the one-time CIP-0163
// activation stamp: on the first epoch boundary processed with the
// delegator-inactivity gate on, it sets every active reward account's
// expiration_epoch to currentEpoch + DelegatorInactivity, including rows
// already renewed by a pre-activation witness, giving every pre-existing
// account a full inactivity window starting at this boundary. It is guarded by
// a durable sync_state marker so it runs exactly once regardless of how many
// epoch boundaries are processed afterward or how many times the node
// restarts. A no-op when the gate is off (no stamp, no marker write, so turning
// the gate on later still triggers activation on the next boundary).
//
// currentEpoch is the epoch being entered at this boundary
// (processEpochRollover passes the ending epoch plus one), not the epoch that
// just ended: pre-existing accounts get a full DelegatorInactivity-epoch window
// measured from the boundary they are activated at, matching the monotonic
// currentEpoch+DelegatorInactivity convention used by
// renewWitnessedAccountExpirations for ordinary per-block renewals.
//
// Must run inside the same database transaction as the epoch rollover
// (txn) so the marker commits atomically with the stamp: a crash between the
// two would otherwise either re-run the stamp or, worse, record activation
// without ever having stamped anything. Passing the rollover's txn rules out
// both partial states.
func (ls *LedgerState) activateDelegatorInactivityIfNeeded(
	txn *database.Txn,
	currentEpoch uint64,
) error {
	if !ls.config.DelegatorInactivityEnabled {
		return nil
	}
	done, err := ls.db.GetSyncState(delegatorInactivityActivatedSyncKey, txn)
	if err != nil {
		return err
	}
	if done != "" {
		return nil
	}
	expiration := currentEpoch + ls.config.DelegatorInactivity
	stamped, err := ls.db.StampAllActiveAccountExpirations(expiration, txn)
	if err != nil {
		return err
	}
	// Record the activation epoch A (not a bare flag) so the rollback recompute
	// can clamp orphaned accounts back up to the activation floor A + W.
	if err := ls.db.SetSyncState(
		delegatorInactivityActivatedSyncKey,
		strconv.FormatUint(currentEpoch, 10),
		txn,
	); err != nil {
		return err
	}
	// This one-time, consensus-affecting event starts the inactivity clock for
	// every pre-existing active account; log it so operators can see when and
	// how broadly CIP-0163 activation fired.
	if ls.config.Logger != nil {
		ls.config.Logger.Info(
			"CIP-0163 delegator inactivity activated",
			"component", "ledger",
			"activation_epoch", currentEpoch,
			"expiration_epoch", expiration,
			"accounts_stamped", stamped,
		)
	}
	return nil
}

// delegatorInactivityActivationEpoch reads the durable CIP-0163 activation
// marker and reports the activation epoch A, whether activation has occurred,
// and any read/parse error. An empty marker means activation has not run yet
// (activated == false, epoch 0). It is the read-back counterpart of the marker
// write in activateDelegatorInactivityIfNeeded and is consumed by
// recomputeAccountExpirationsAfterRollback.
func (ls *LedgerState) delegatorInactivityActivationEpoch(
	txn *database.Txn,
) (epoch uint64, activated bool, err error) {
	marker, err := ls.db.GetSyncState(delegatorInactivityActivatedSyncKey, txn)
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
